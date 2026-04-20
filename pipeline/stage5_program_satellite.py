"""Stage 5 — Fill satellite tables for each `programs` row: fees, admissions, evaluations;
optionally `program_art_categories` when `fill_art_categories=True`.

Uses the same Tavily + official-page evidence pattern as Stage 4 (`pipeline.evidence`), then one
Claude call per program to extract structured rows. Skips tables that already have a row for
that `program_id` (re-runs complete partial fills).

Field semantics: optional glossary in `FIELD_GLOSSARY_APPEND` (edit this file) is appended to the
user prompt so Tavily/Claude align with your product meanings — add definitions when ready.

`schools` enrichment for new institutions remains Stages 1–3; this stage only enriches program-level
satellite data after `programs` rows exist (typically from Stage 4).
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

log = get_logger("stage5_satellite")

PROGRAMS_TABLE = "programs"
FEES_TABLE = "program_fees"
ADM_TABLE = "program_admissions"
EVAL_TABLE = "program_evaluations"
ART_CAT_LINK_TABLE = "program_art_categories"
ART_CATEGORIES_TABLE = "art_categories"

# Optional: paste short per-field definitions (English) to steer extraction; appended to the prompt.
FIELD_GLOSSARY_APPEND = ""

SYSTEM_PROMPT = """You are a research assistant for university program-level admissions and fees.
You receive web search snippets and page extracts. Use ONLY that evidence — do not invent facts.
Return ONLY valid JSON (no markdown). Use null when evidence does not support a field.
Dates as ISO strings YYYY-MM-DD when a calendar date is evidenced, else null.
Numbers must be JSON numbers, not strings."""


def _user_template_satellite(
    school_en: str,
    school_zh: str,
    country: str,
    website: str,
    program_name: str,
    degree_type: str,
    category_catalog: str,
    evidence_text: str,
    *,
    fill_art_categories: bool,
    field_glossary: str,
) -> str:
    glossary_block = ""
    if field_glossary.strip():
        glossary_block = f"\nField glossary (product definitions):\n{field_glossary.strip()}\n"

    core = f"""Institution (English): {school_en}
Institution (Chinese, if any): {school_zh}
Country/region: {country}
Official website: {website}

Program name: {program_name}
Degree type (if any): {degree_type}
{glossary_block}
Web evidence:
{evidence_text}

Return a single JSON object with exactly these keys:
- fees: object with keys: currency_code (3-letter ISO, e.g. GBP), domestic_tuition_fee (number|null),
  international_tuition_fee (number|null), additional_fees_note (string|null)
- admissions: object with keys: academic_requirements (string|null), deadline_notes (string|null),
  ielts_overall (number|null), ielts_subscores (object|null), interview_format (object|null),
  other_language_tests (string|null), portfolio_deadline (string|null), portfolio_format (object|null),
  portfolio_requirements (string|null), priority_deadline (string|null), reference_count (integer|null),
  regular_deadline (string|null), toefl_ibt (integer|null)
- evaluation: object with keys: acceptance_rate (number|null, 0-1 or percentage as 0.xx if evidence),
  application_difficulty_score (string|null, max 20 chars), competition_level (string|null, max 50 chars),
  data_source (string|null, max 100 chars), evidence_note (string|null), source_url (string|null)"""

    if not fill_art_categories:
        return core + "\n\nUse null for any field not supported by evidence."

    return (
        core
        + f"""

Valid art category ids (choose zero or more ids that best match this program; ids are integers):
{category_catalog}

Also include key:
- art_category_ids: array of integers — subset of the valid ids listed above; empty array if none fit

Use null for any field not supported by evidence."""
    )


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
def _claude_satellite(
    client: anthropic.Anthropic,
    user_prompt: str,
) -> dict:
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=8192,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )
    text_blocks = [
        b.text for b in response.content if b.type == "text" and getattr(b, "text", None)
    ]
    final_text = "\n".join(text_blocks).strip()
    if not final_text:
        return {}
    return _parse_json_object(final_text, "satellite")


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


def _nullable_int(val: Any) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _jsonb(val: Any) -> dict | list | None:
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return val
    return None


def _has_row(client, table: str, program_id: str) -> bool:
    resp = (
        client.table(table).select("id").eq("program_id", program_id).limit(1).execute()
    )
    return bool(resp.data)


def _art_link_count(client, program_id: str) -> int:
    resp = (
        client.table(ART_CAT_LINK_TABLE)
        .select("id", count="exact")
        .eq("program_id", program_id)
        .limit(0)
        .execute()
    )
    return int(resp.count or 0)


def _needs_satellite(client, program_id: str) -> bool:
    """True if fees, admissions, or evaluations row is missing.

    `program_art_categories` is best-effort in the same Claude pass; avoiding a hard
    requirement prevents infinite retries when the model returns no category ids.
    """
    if not _has_row(client, FEES_TABLE, program_id):
        return True
    if not _has_row(client, ADM_TABLE, program_id):
        return True
    if not _has_row(client, EVAL_TABLE, program_id):
        return True
    return False


def _load_art_categories(client) -> tuple[str, set[int]]:
    """Return only level-2 categories; programs should not be linked to level-1 faculties.

    Tolerates pre-hierarchy databases that lack the `level` column by falling back to all rows.
    """
    try:
        resp = (
            client.table(ART_CATEGORIES_TABLE)
            .select("id,name_en,name_zh,level,is_active")
            .eq("level", 2)
            .eq("is_active", True)
            .order("id")
            .execute()
        )
    except Exception:
        resp = (
            client.table(ART_CATEGORIES_TABLE)
            .select("id,name_en,name_zh")
            .order("id")
            .execute()
        )
    valid: set[int] = set()
    lines: list[str] = []
    for r in resp.data or []:
        rid = r.get("id")
        if rid is None:
            continue
        try:
            iid = int(rid)
        except (TypeError, ValueError):
            continue
        valid.add(iid)
        ne = r.get("name_en") or ""
        nz = r.get("name_zh") or ""
        lines.append(f"  {iid}: {ne} / {nz}")
    catalog = "\n".join(lines) if lines else "(no categories in DB)"
    return catalog, valid


def _fallback_art_ids_from_program_category(
    client,
    program: dict,
    valid: set[int],
) -> list[int]:
    """Weak match: program.program_category substring vs art_categories.name_en."""
    pc = (program.get("program_category") or "").strip().lower()
    if len(pc) < 2:
        return []
    resp = (
        client.table(ART_CATEGORIES_TABLE)
        .select("id,name_en")
        .execute()
    )
    out: list[int] = []
    for r in resp.data or []:
        rid = r.get("id")
        ne = (r.get("name_en") or "").strip().lower()
        if rid is None or not ne:
            continue
        try:
            iid = int(rid)
        except (TypeError, ValueError):
            continue
        if iid not in valid:
            continue
        if ne in pc or pc in ne:
            out.append(iid)
            if len(out) >= 3:
                break
    return out


def _fetch_schools_map(client, school_ids: list[str]) -> dict[str, dict]:
    if not school_ids:
        return {}
    resp = (
        client.table(SCHOOLS_TABLE)
        .select("id,name_en,name_zh,official_website,country")
        .in_("id", list(dict.fromkeys(school_ids)))
        .execute()
    )
    return {r["id"]: r for r in (resp.data or [])}


def _fetch_programs_page(client, start: int, page_size: int) -> list[dict]:
    end = start + page_size - 1
    resp = (
        client.table(PROGRAMS_TABLE)
        .select(
            "id,program_name,degree_type,program_category,school_id,school_name_en,school_name_zh"
        )
        .order("program_name")
        .range(start, end)
        .execute()
    )
    return resp.data or []


def _insert_fees(client, program_id: str, fees: dict) -> None:
    row = {
        "program_id": program_id,
        "currency_code": _nullable_str(fees.get("currency_code")) or "GBP",
        "domestic_tuition_fee": _nullable_float(fees.get("domestic_tuition_fee")),
        "international_tuition_fee": _nullable_float(fees.get("international_tuition_fee")),
        "additional_fees_note": _nullable_str(fees.get("additional_fees_note")),
    }
    client.table(FEES_TABLE).insert(row).execute()


def _insert_admissions(client, program_id: str, adm: dict) -> None:
    row = {
        "program_id": program_id,
        "academic_requirements": _nullable_str(adm.get("academic_requirements")),
        "deadline_notes": _nullable_str(adm.get("deadline_notes")),
        "ielts_overall": _nullable_float(adm.get("ielts_overall")),
        "ielts_subscores": _jsonb(adm.get("ielts_subscores")),
        "interview_format": _jsonb(adm.get("interview_format")),
        "other_language_tests": _nullable_str(adm.get("other_language_tests")),
        "portfolio_deadline": _nullable_str(adm.get("portfolio_deadline")),
        "portfolio_format": _jsonb(adm.get("portfolio_format")),
        "portfolio_requirements": _nullable_str(adm.get("portfolio_requirements")),
        "priority_deadline": _nullable_str(adm.get("priority_deadline")),
        "reference_count": _nullable_int(adm.get("reference_count")),
        "regular_deadline": _nullable_str(adm.get("regular_deadline")),
        "toefl_ibt": _nullable_int(adm.get("toefl_ibt")),
    }
    client.table(ADM_TABLE).insert(row).execute()


def _insert_evaluation(client, program_id: str, ev: dict) -> None:
    row = {
        "program_id": program_id,
        "acceptance_rate": _nullable_float(ev.get("acceptance_rate")),
        "application_difficulty_score": (_nullable_str(ev.get("application_difficulty_score")) or "")[
            :20
        ]
        or None,
        "competition_level": (_nullable_str(ev.get("competition_level")) or "")[:50] or None,
        "data_source": (_nullable_str(ev.get("data_source")) or "")[:100] or None,
        "evidence_note": _nullable_str(ev.get("evidence_note")),
        "source_url": _nullable_str(ev.get("source_url")),
        "updated_by": "pipeline/stage5_program_satellite.py",
    }
    client.table(EVAL_TABLE).insert(row).execute()


def _replace_art_links(client, program_id: str, category_ids: list[int], valid: set[int]) -> None:
    client.table(ART_CAT_LINK_TABLE).delete().eq("program_id", program_id).execute()
    clean = [cid for cid in category_ids if cid in valid]
    clean = list(dict.fromkeys(clean))
    for cid in clean:
        client.table(ART_CAT_LINK_TABLE).insert(
            {"program_id": program_id, "category_id": cid}
        ).execute()


def run(
    settings: Settings,
    batch_size: int,
    *,
    fill_art_categories: bool = False,
) -> None:
    client = get_client(settings)
    claude = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    if fill_art_categories:
        category_catalog, valid_ids = _load_art_categories(client)
    else:
        category_catalog, valid_ids = "(art categorization disabled for this run)", set()
        log.info("Stage 5: skipping program_art_categories (use --fill-art-categories to enable)")

    processed = 0
    inserted_fees = inserted_adm = inserted_eval = art_runs = 0
    start = 0
    page_size = 80

    while processed < batch_size:
        programs = _fetch_programs_page(client, start, page_size)
        if not programs:
            break
        school_ids = [p["school_id"] for p in programs if p.get("school_id")]
        schools_map = _fetch_schools_map(client, school_ids)

        for prog in programs:
            if processed >= batch_size:
                break
            pid = prog["id"]
            if not _needs_satellite(client, pid):
                continue

            school = schools_map.get(prog.get("school_id")) or {
                "name_en": prog.get("school_name_en"),
                "name_zh": prog.get("school_name_zh"),
                "official_website": None,
                "country": None,
            }
            name_en = school.get("name_en") or prog.get("school_name_en") or ""
            name_zh = school.get("name_zh") or prog.get("school_name_zh") or ""
            website = (school.get("official_website") or "").strip()
            country = school.get("country") or ""

            log.info(
                "→ %s / %s",
                name_en,
                (prog.get("program_name") or "")[:80],
            )

            ev_text = evidence.build_evidence_for_program_detail(settings, school, prog)
            user_prompt = _user_template_satellite(
                name_en,
                name_zh or "",
                country or "",
                website or "unknown",
                prog.get("program_name") or "",
                prog.get("degree_type") or "",
                category_catalog,
                ev_text,
                fill_art_categories=fill_art_categories,
                field_glossary=FIELD_GLOSSARY_APPEND,
            )
            data = _claude_satellite(claude, user_prompt)
            fees = data.get("fees") if isinstance(data.get("fees"), dict) else {}
            adm = data.get("admissions") if isinstance(data.get("admissions"), dict) else {}
            eva = data.get("evaluation") if isinstance(data.get("evaluation"), dict) else {}
            raw_ids = data.get("art_category_ids")
            art_ids: list[int] = []
            if isinstance(raw_ids, list):
                for x in raw_ids:
                    try:
                        art_ids.append(int(x))
                    except (TypeError, ValueError):
                        pass

            try:
                if not _has_row(client, FEES_TABLE, pid):
                    _insert_fees(client, pid, fees)
                    inserted_fees += 1
                if not _has_row(client, ADM_TABLE, pid):
                    _insert_admissions(client, pid, adm)
                    inserted_adm += 1
                if not _has_row(client, EVAL_TABLE, pid):
                    _insert_evaluation(client, pid, eva)
                    inserted_eval += 1
                if fill_art_categories and _art_link_count(client, pid) < 1:
                    link_ids = art_ids
                    if not link_ids:
                        link_ids = _fallback_art_ids_from_program_category(
                            client, prog, valid_ids
                        )
                    if link_ids:
                        _replace_art_links(client, pid, link_ids, valid_ids)
                        art_runs += 1
            except Exception as exc:
                log.error("  satellite insert failed: %s", exc)

            processed += 1
            time.sleep(1.0)

        start += page_size

    log.info(
        "Stage 5 complete: processed %s program(s); inserts fees=%s admissions=%s evaluations=%s art_batches=%s",
        processed,
        inserted_fees,
        inserted_adm,
        inserted_eval,
        art_runs,
    )
