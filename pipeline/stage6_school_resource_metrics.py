"""Stage 6 — Fill `school_resource_metrics` via Tavily + Claude (school-level facilities / ratio / scholarships).

Runs after baseline school rows exist (Stage 1+) and complements program-level Stage 5.
Skips rows that already have substantive resource data unless `--force-resources` mode is toggled via `force_refresh=True`.

See also: `pipeline/stage7_school_comparison_rollups.py`.
"""
from __future__ import annotations

import json
import time
from typing import Any

import anthropic

from config.settings import Settings
from db.supabase_client import TABLE as SCHOOLS_TABLE, get_client
from pipeline import evidence
from utils.logger import get_logger
from utils.retry import retry

log = get_logger("stage6_resources")

RESOURCE_TABLE = "school_resource_metrics"


SYSTEM_PROMPT = """You are a research assistant for university resource and campus indicators.
You receive web search excerpts and extracts. Use ONLY that evidence — do not invent facts.
Return ONLY valid JSON (no markdown). Use null when evidence does not support a field.
Scholarship ratio must be numeric 0-100 only when a clear percentage refers to undergraduates,\
 international students receiving aid, or similar—otherwise null."""

USER_TEMPLATE = """Institution (English): {name_en}
Institution (Chinese, if any): {name_zh}
Country/region: {country}
Official website: {website}

Evidence:
{ev_text}

Return a single JSON object with exactly these keys:
- student_faculty_ratio_text (string|null) — cite as published, e.g. "1:12" or "~15 staff per student"
- scholarship_ratio_pct (number|null) — 0-100 ONLY if evidenced as ONE percentage covering aid/scholarships
- campus_facilities_summary (string|null) — 3-8 sentences summarising evidenced studios/workshops/spaces/equipment
- resource_notes (string|null)
- data_source (string|null) — prose naming evidence types (factsheet, gov data, FAQ)
- source_url (string|null) — one primary URL string if evidenced

Use null generously when facts are ambiguous or missing."""


@retry(max_attempts=3, base_delay=3.0)
def _claude_resources(client: anthropic.Anthropic, user_prompt: str) -> dict:
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )
    blocks = [
        b.text for b in response.content if b.type == "text" and getattr(b, "text", None)
    ]
    final_text = "\n".join(blocks).strip()
    return _parse_json_object(final_text, "school_resources")


def _parse_json_object(text: str, context: str) -> dict:
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    start = text.find("{")
    end = text.rfind("}") + 1
    if start == -1 or end == 0:
        log.error("No JSON object for %s: %s", context, text[:260])
        return {}
    try:
        return json.loads(text[start:end])
    except json.JSONDecodeError as exc:
        log.error("JSON error for %s: %s", context, exc)
        return {}


def _nullable_str(val: Any) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return s or None


def _nullable_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _row_needs_fill(row: dict | None) -> bool:
    if row is None:
        return True
    if _nullable_str(row.get("student_faculty_ratio_text")):
        return False
    if row.get("scholarship_ratio_pct") is not None:
        return False
    if _nullable_str(row.get("campus_facilities_summary")):
        return False
    return True


def _fetch_school_metrics_map(client: Any, school_ids: list[str]) -> dict[str, dict]:
    if not school_ids:
        return {}
    resp = (
        client.table(RESOURCE_TABLE)
        .select(
            "school_id,student_faculty_ratio_text,scholarship_ratio_pct,campus_facilities_summary",
        )
        .in_("school_id", school_ids)
        .execute()
    )
    out: dict[str, dict] = {}
    for r in resp.data or []:
        sid = r.get("school_id")
        if sid:
            out[sid] = r
    return out


def _build_payload_from_claude(school_id: str, parsed: dict) -> dict:
    pct_raw = parsed.get("scholarship_ratio_pct")
    pct = _nullable_float(pct_raw)
    if pct is not None and not (0 <= pct <= 100):
        pct = None
    return {
        "school_id": school_id,
        "student_faculty_ratio_text": _nullable_str(parsed.get("student_faculty_ratio_text")),
        "scholarship_ratio_pct": pct,
        "campus_facilities_summary": _nullable_str(parsed.get("campus_facilities_summary")),
        "resource_notes": _nullable_str(parsed.get("resource_notes")),
        "data_source": _nullable_str(parsed.get("data_source")),
        "source_url": _nullable_str(parsed.get("source_url")),
        "raw_evidence_json": {"stage6_extract": parsed},
    }


def run(settings: Settings, batch_size: int, *, force_refresh: bool = False) -> None:
    client = get_client(settings)
    claude = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    page_size = 50
    start = 0
    touched = 0

    while touched < batch_size:
        rng_end = start + page_size - 1
        resp = (
            client.table(SCHOOLS_TABLE)
            .select(
                "id,name_en,name_zh,official_website,raw_country",
            )
            .range(start, rng_end)
            .order("name_en")
            .execute()
        )
        schools_chunk = resp.data or []
        if not schools_chunk:
            break

        ids = [s["id"] for s in schools_chunk if s.get("id")]
        metrics_by_school = _fetch_school_metrics_map(client, ids)

        for school in schools_chunk:
            if touched >= batch_size:
                break
            sid = school.get("id")
            name_en = school.get("name_en") or ""
            if not sid or not name_en:
                continue
            prev = metrics_by_school.get(sid)
            if not force_refresh and not _row_needs_fill(prev):
                continue

            log.info("→ resources: %s", name_en)
            try:
                ev_text = evidence.build_evidence_for_school_resources(settings, school)
                prompt = USER_TEMPLATE.format(
                    name_en=name_en,
                    name_zh=school.get("name_zh") or "",
                    country=(school.get("raw_country") or "").strip() or "unknown",
                    website=(school.get("official_website") or "").strip() or "unknown",
                    ev_text=ev_text,
                )
                parsed = _claude_resources(claude, prompt)
            except Exception as exc:
                log.error("  evidence/claude failed, skip: %s", exc)
                touched += 1
                time.sleep(1.0)
                continue

            payload = _build_payload_from_claude(sid, parsed)
            try:
                client.table(RESOURCE_TABLE).upsert(payload).execute()
            except Exception as exc:
                log.error("  upsert school_resource_metrics failed: %s", exc)

            touched += 1
            time.sleep(1.0)

        start += page_size

    log.info("Stage 6 complete: processed %s school resource job(s).", touched)
