"""Stage 4 — For each school in `schools`, add up to 3 degree programs into `programs`.

Uses Tavily search (optionally with raw page excerpts from Tavily), HTTP fetches of
same-domain official URLs for longer text, then Claude to extract evidence-backed rows.
Skips schools that already have three or more programs.

Requires `programs.school_id` to be uuid FK to `schools.id`. If inserts fail with a
type error, apply db/fix_programs_school_id.sql in the Supabase SQL editor first.
"""
from __future__ import annotations

import hashlib
import json
import sys
import time
from typing import Any
from urllib.parse import urlparse

import anthropic
import requests
from bs4 import BeautifulSoup

from config.settings import Settings
from db.supabase_client import get_client
from utils.logger import get_logger
from utils.retry import retry

log = get_logger("stage4_programs")

TAVILY_URL = "https://api.tavily.com/search"
PROGRAMS_TABLE = "programs"
SCHOOLS_TABLE = "schools"

# Evidence budget: richer than short snippets alone, still bounded for API cost/latency.
TAVILY_SNIPPET_MAX = 2500
MAX_TAVILY_BLOCKS = 18
MAX_DEEP_FETCH_PAGES = 5
PAGE_EXTRACT_MAX = 10000
EVIDENCE_TOTAL_SOFT_MAX = 120_000

FETCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

SYSTEM_PROMPT = """You are a research assistant for university art/design admissions data.
You receive web search snippets, optional Tavily raw excerpts, and optional extracted text
from official-site pages. Use ONLY that evidence — do not invent facts.
Return ONLY valid JSON (no markdown). If the evidence does not support a field, use null.
Use English for all text fields. Booleans must be true/false, not strings."""


USER_TEMPLATE = """School (English): {name_en}
School (Chinese, if any): {name_zh}
Country/region: {country}
Official website: {website}

Task: list exactly {need} DISTINCT, real degree programs this institution currently offers
(undergraduate or postgraduate level) that are relevant to fine art, design, architecture,
film, visual communication, digital media, or closely related creative fields.
Prefer flagship or well-documented programs found in the evidence.

Web evidence:
{evidence}

Return a JSON object with a single key "programs" whose value is an array of exactly {need}
objects. Each object MUST have these keys (use null when unknown):
- program_name: string
- degree_type: string — short label, e.g. "BA", "BFA", "MA", "MFA", "MArch"
- degree_full_name: string or null
- program_category: string or null — e.g. "Fine Art", "Graphic Design"
- program_code: string or null — internal course code if stated
- ucas_code: string or null — only for UK UCAS-listed courses when stated
- duration_text: string or null — e.g. "3 years full-time"
- duration_months: integer or null — approximate total calendar months
- study_mode: string or null — e.g. "full-time", "part-time", "online", "hybrid"
- intake_months: array of strings or null — e.g. ["September"] or ["September","January"]
- requires_portfolio: boolean or null
- requires_interview: boolean or null
- requires_personal_statement: boolean or null
- minimum_education: string or null
- program_overview: string — 2–4 sentences from evidence
- program_highlights: string or null — short bullet-style sentence listing 1–3 evidence-based perks
- accreditation_info: string or null
- core_courses: array of strings or null — module/course titles if evidenced
- career_paths: array of strings or null
- admission_summary: string or null — one short paragraph of main thresholds (language/tests/portfolio)
- cover_image_url: string or null — only if a direct image URL appears in evidence
- status: string — "active" if evidence clearly describes a live intake; otherwise "draft"
- is_recommended: boolean — false unless evidence shows it is a flagship / highly cited program

Return ONLY the JSON object."""


def _programs_school_id_is_uuid(settings: Settings) -> bool:
    url = settings.supabase_url.rstrip("/") + "/rest/v1/"
    headers = {
        "apikey": settings.supabase_service_key,
        "Authorization": f"Bearer {settings.supabase_service_key}",
    }
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    spec = resp.json()
    prop = (
        spec.get("definitions", {})
        .get(PROGRAMS_TABLE, {})
        .get("properties", {})
        .get("school_id", {})
    )
    return prop.get("format") == "uuid"


def _fetch_all_schools(client, columns: str) -> list[dict]:
    page_size = 500
    start = 0
    out: list[dict] = []
    while True:
        end = start + page_size - 1
        resp = (
            client.table(SCHOOLS_TABLE)
            .select(columns)
            .order("name_en")
            .range(start, end)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        out.extend(batch)
        if len(batch) < page_size:
            break
        start += page_size
    return out


def _program_count_for_school(client, school_id: str) -> int:
    resp = (
        client.table(PROGRAMS_TABLE)
        .select("id", count="exact")
        .eq("school_id", school_id)
        .limit(0)
        .execute()
    )
    return int(resp.count or 0)


def _domain_from_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url)
    return parsed.netloc.replace("www.", "").lower()


def _netloc_key(url: str) -> str:
    if not url:
        return ""
    try:
        return urlparse(url).netloc.replace("www.", "").lower()
    except Exception:
        return ""


def _url_on_domain(url: str, school_domain: str) -> bool:
    if not school_domain or not url:
        return False
    nl = _netloc_key(url)
    return nl == school_domain or nl.endswith("." + school_domain)


def _tavily_search(api_key: str, query: str, *, include_raw_content: bool = True) -> list[dict]:
    resp = requests.post(
        TAVILY_URL,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "query": query,
            "search_depth": "advanced",
            "topic": "general",
            "max_results": 5,
            "include_raw_content": include_raw_content,
            "include_answer": False,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("results") or []


def _extract_visible_text(html: str, max_chars: int) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    out = "\n".join(lines)
    if len(out) > max_chars:
        out = out[:max_chars] + "\n...[truncated]"
    return out


def _fetch_official_page_text(url: str, max_chars: int) -> str | None:
    if not url or not url.startswith(("http://", "https://")):
        return None
    try:
        resp = requests.get(url, headers=FETCH_HEADERS, timeout=25, allow_redirects=True)
        resp.raise_for_status()
    except Exception as exc:
        log.warning(f"  fetch failed {url!r}: {exc}")
        return None
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "pdf" in ctype or "msword" in ctype or "wordprocessingml" in ctype:
        log.warning(f"  skip non-html {url!r} ({ctype})")
        return None
    if "html" not in ctype and "text/plain" not in ctype and "xml" not in ctype:
        # Many servers omit charset; still try if body looks like HTML.
        if "<html" not in resp.text[:2000].lower() and "<!doctype html" not in resp.text[:200].lower():
            log.warning(f"  skip likely non-html {url!r} ({ctype})")
            return None
    try:
        return _extract_visible_text(resp.text, max_chars)
    except Exception as exc:
        log.warning(f"  parse failed {url!r}: {exc}")
        return None


def _trim_evidence_total(text: str, soft_max: int) -> str:
    if len(text) <= soft_max:
        return text
    return text[:soft_max] + "\n\n...[evidence truncated for size]"


def _build_evidence(settings: Settings, school: dict) -> str:
    name_en = school.get("name_en") or ""
    name_zh = school.get("name_zh") or ""
    website = (school.get("official_website") or "").strip()
    country = school.get("country") or ""
    domain = _domain_from_url(website)

    queries = [
        f"{name_en} undergraduate postgraduate degree programs fine art design {country}",
        f"{name_en} BA MA MFA BFA course portfolio admission",
        f"{name_en} official programs courses catalogue",
    ]
    if name_zh:
        queries.append(f"{name_en} {name_zh} 专业 课程 学位")
    if domain:
        queries.append(f"{name_en} site:{domain} programs courses admissions")
        queries.append(f"site:{domain} admissions portfolio requirements degree duration")

    blocks: list[str] = []
    seen_urls: set[str] = set()
    for query in queries:
        for result in _tavily_search(settings.tavily_api_key, query):
            url = (result.get("url") or "").strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            title = (result.get("title") or "").strip()
            raw = (result.get("raw_content") or "").strip()
            content = (result.get("content") or "").strip()
            body = raw if raw else content
            if len(body) > TAVILY_SNIPPET_MAX:
                body = body[: TAVILY_SNIPPET_MAX] + "..."
            label = "Excerpt" if raw else "Snippet"
            blocks.append(f"Title: {title}\nURL: {url}\n{label}: {body}")
            if len(blocks) >= MAX_TAVILY_BLOCKS:
                break
        if len(blocks) >= MAX_TAVILY_BLOCKS:
            break

    # Deep-fetch a few same-domain pages (homepage from sheet + Tavily hits on that domain).
    deep_urls: list[str] = []
    seen_deep: set[str] = set()
    if website.startswith(("http://", "https://")):
        deep_urls.append(website)
        seen_deep.add(website)
    if domain:
        for u in sorted(seen_urls):
            if len(deep_urls) >= MAX_DEEP_FETCH_PAGES:
                break
            if u in seen_deep or not _url_on_domain(u, domain):
                continue
            deep_urls.append(u)
            seen_deep.add(u)
    deep_urls = deep_urls[:MAX_DEEP_FETCH_PAGES]

    for i, page_url in enumerate(deep_urls):
        if i > 0:
            time.sleep(0.35)
        extracted = _fetch_official_page_text(page_url, PAGE_EXTRACT_MAX)
        if not extracted:
            continue
        blocks.append(
            f"Source: official-site page extract\nURL: {page_url}\nText:\n{extracted}"
        )

    if not blocks:
        return "No web evidence found."
    joined = "\n\n".join(blocks)
    return _trim_evidence_total(joined, EVIDENCE_TOTAL_SOFT_MAX)


def _parse_json_object(text: str, context: str) -> dict:
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    start = text.find("{")
    end = text.rfind("}") + 1
    if start == -1 or end == 0:
        log.error(f"No JSON object for {context}: {text[:240]}")
        return {}
    try:
        return json.loads(text[start:end])
    except json.JSONDecodeError as exc:
        log.error(f"JSON error for {context}: {exc}")
        return {}


@retry(max_attempts=3, base_delay=3.0)
def _claude_programs(
    client: anthropic.Anthropic,
    school: dict,
    need: int,
    evidence: str,
) -> list[dict]:
    prompt = USER_TEMPLATE.format(
        name_en=school.get("name_en") or "",
        name_zh=school.get("name_zh") or "",
        country=school.get("country") or "",
        website=school.get("official_website") or "unknown",
        need=need,
        evidence=evidence,
    )
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=8192,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    text_blocks = [
        b.text for b in response.content if b.type == "text" and getattr(b, "text", None)
    ]
    final_text = "\n".join(text_blocks).strip()
    if not final_text:
        return []
    data = _parse_json_object(final_text, school.get("name_en", ""))
    programs = data.get("programs")
    if not isinstance(programs, list):
        return []
    return [p for p in programs if isinstance(p, dict)]


def _normalize_intake_months(val: Any) -> list[str] | None:
    if val is None:
        return None
    if isinstance(val, str):
        s = val.strip()
        return [s] if s else None
    if isinstance(val, list):
        out = [str(x).strip() for x in val if str(x).strip()]
        return out or None
    return None


def _normalize_str_list(val: Any, max_items: int = 12) -> list[str] | None:
    if val is None:
        return None
    if not isinstance(val, list):
        return None
    out = [str(x).strip() for x in val if str(x).strip()]
    out = out[:max_items]
    return out or None


def _normalize_bool(val: Any) -> bool | None:
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    return None


def _nullable_str(val: Any) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return s or None


def _nullable_int(val: Any) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _row_for_insert(raw: dict, school: dict, evidence: str) -> dict | None:
    name = (raw.get("program_name") or "").strip()
    if not name:
        return None

    payload_for_hash = {
        "school_id": school["id"],
        "program_name": name,
        "degree_type": raw.get("degree_type"),
        "degree_full_name": raw.get("degree_full_name"),
        "program_category": raw.get("program_category"),
        "program_code": raw.get("program_code"),
        "ucas_code": raw.get("ucas_code"),
        "duration_text": raw.get("duration_text"),
        "duration_months": raw.get("duration_months"),
        "study_mode": raw.get("study_mode"),
        "intake_months": raw.get("intake_months"),
        "requires_portfolio": raw.get("requires_portfolio"),
        "requires_interview": raw.get("requires_interview"),
        "requires_personal_statement": raw.get("requires_personal_statement"),
        "minimum_education": raw.get("minimum_education"),
        "program_overview": raw.get("program_overview"),
        "program_highlights": raw.get("program_highlights"),
        "accreditation_info": raw.get("accreditation_info"),
        "core_courses": raw.get("core_courses"),
        "career_paths": raw.get("career_paths"),
        "admission_summary": raw.get("admission_summary"),
        "cover_image_url": raw.get("cover_image_url"),
        "status": raw.get("status"),
        "is_recommended": raw.get("is_recommended"),
    }
    canonical = json.dumps(payload_for_hash, sort_keys=True, ensure_ascii=False, default=str)
    source_hash = hashlib.sha256(
        (canonical + "\n" + evidence[:2000]).encode("utf-8")
    ).hexdigest()

    row = {
        "school_id": school["id"],
        "school_name_en": school.get("name_en"),
        "school_name_zh": school.get("name_zh"),
        "program_name": name,
        "degree_type": _nullable_str(raw.get("degree_type")),
        "degree_full_name": _nullable_str(raw.get("degree_full_name")),
        "program_category": _nullable_str(raw.get("program_category")),
        "program_code": _nullable_str(raw.get("program_code")),
        "ucas_code": _nullable_str(raw.get("ucas_code")),
        "duration_text": _nullable_str(raw.get("duration_text")),
        "duration_months": _nullable_int(raw.get("duration_months")),
        "study_mode": _nullable_str(raw.get("study_mode")),
        "intake_months": _normalize_intake_months(raw.get("intake_months")),
        "requires_portfolio": _normalize_bool(raw.get("requires_portfolio")),
        "requires_interview": _normalize_bool(raw.get("requires_interview")),
        "requires_personal_statement": _normalize_bool(raw.get("requires_personal_statement")),
        "minimum_education": _nullable_str(raw.get("minimum_education")),
        "program_overview": _nullable_str(raw.get("program_overview")) or "No overview available from evidence.",
        "program_highlights": _nullable_str(raw.get("program_highlights")),
        "accreditation_info": _nullable_str(raw.get("accreditation_info")),
        "core_courses": _normalize_str_list(raw.get("core_courses")),
        "career_paths": _normalize_str_list(raw.get("career_paths")),
        "admission_summary": _nullable_str(raw.get("admission_summary")),
        "cover_image_url": _nullable_str(raw.get("cover_image_url")),
        "status": _nullable_str(raw.get("status")) or "draft",
        "is_recommended": bool(raw.get("is_recommended"))
        if isinstance(raw.get("is_recommended"), bool)
        else False,
        "source_file": "pipeline/stage4_programs.py",
        "source_hash": source_hash,
    }
    return row


def _already_has_program(client, school_id: str, program_name: str) -> bool:
    resp = (
        client.table(PROGRAMS_TABLE)
        .select("id")
        .eq("school_id", school_id)
        .eq("program_name", program_name)
        .limit(1)
        .execute()
    )
    return bool(resp.data)


def run(settings: Settings, batch_size: int) -> None:
    if not _programs_school_id_is_uuid(settings):
        log.error(
            "programs.school_id is not uuid in the PostgREST schema. "
            "Apply db/fix_programs_school_id.sql in the Supabase SQL editor, then re-run."
        )
        sys.exit(1)

    client = get_client(settings)
    claude = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    schools = _fetch_all_schools(
        client,
        "id,name_en,name_zh,official_website,country",
    )
    log.info(f"Loaded {len(schools)} schools from {SCHOOLS_TABLE}")

    processed = 0
    inserted_total = 0

    for school in schools:
        if processed >= batch_size:
            break

        sid = school["id"]
        existing = _program_count_for_school(client, sid)
        if existing >= 3:
            continue

        need = 3 - existing
        name_en = school.get("name_en") or ""
        log.info(f"→ {name_en} (need {need} programs)")

        evidence = _build_evidence(settings, school)
        programs_raw = _claude_programs(claude, school, need, evidence)

        inserted_here = 0
        for raw in programs_raw:
            if inserted_here >= need:
                break
            row = _row_for_insert(raw, school, evidence)
            if not row:
                continue
            if _already_has_program(client, sid, row["program_name"]):
                continue
            try:
                client.table(PROGRAMS_TABLE).insert(row).execute()
                inserted_here += 1
                inserted_total += 1
            except Exception as exc:
                log.error(f"  insert failed for {row['program_name']!r}: {exc}")

        processed += 1
        log.info(f"  ✓ inserted {inserted_here} program(s) for this school")
        time.sleep(1.0)

    log.info(f"Stage 4 complete: touched {processed} schools, inserted {inserted_total} programs")
