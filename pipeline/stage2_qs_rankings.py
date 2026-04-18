"""Stage 2 — QS Rankings lookup via multi-stage name matching + LLM safety net."""
import json
import time
from pathlib import Path
import os
import requests
import anthropic
import pandas as pd
from config.settings import Settings
from db.supabase_client import get_client, fetch_by_status, upsert_school
from utils.logger import get_logger
from pipeline.qs_matcher import QSIndex, MatchResult

log = get_logger("stage2")

_DATA_DIR = Path(__file__).parent.parent / "data"
SUBJECT_EXCEL = _DATA_DIR / "qs_data_subject.xlsx"
OVERALL_CSV   = _DATA_DIR / "qs_data_metrics.csv"
TAVILY_URL    = "https://api.tavily.com/search"

# Supabase column → Excel sheet name (art-domain subjects only)
ART_SUBJECTS = {
    "qs_art_humanities_rank":                 "Arts & Humanities",
    "qs_architecture_built_environment_rank": "Architecture _ Built Environmen",
    "qs_art_design_rank":                     "Art & Design",
    "qs_history_of_art_rank":                 "History of Art",
}

_LLM_SYSTEM = """You are a data extraction assistant. Extract QS World University Rankings 2026 data from web search snippets.
Return ONLY a valid JSON object — no markdown, no explanation.
Use null for any rank not explicitly stated in the evidence.
Do NOT infer or guess ranks. Only extract numbers you can see in the evidence.
For range ranks like "51-100", return the lower bound integer (51)."""

_LLM_USER = """Find QS 2026 rankings for this school from the web evidence below.

School: {name_en}
Country: {country}

Web evidence:
{evidence}

Return ONLY this JSON (all values integer or null):
{{
  "qs_overall_rank": null,
  "qs_art_humanities_rank": null,
  "qs_architecture_built_environment_rank": null,
  "qs_art_design_rank": null,
  "qs_history_of_art_rank": null
}}

Rules:
- Only fill a field if the evidence explicitly states a QS 2026 rank number for that category.
- If the school is a department of a larger university, you may use the parent university's QS rank for qs_overall_rank only — but only if the evidence clearly connects them.
- Return null for any field without clear evidence."""


def _load_subject_indices() -> dict[str, QSIndex]:
    """Build one QSIndex per art-subject sheet."""
    if not os.path.exists(SUBJECT_EXCEL):
        log.error(f"Subject Excel not found: {SUBJECT_EXCEL}")
        return {}
    xl = pd.ExcelFile(SUBJECT_EXCEL)
    indices = {}
    for db_col, sheet_name in ART_SUBJECTS.items():
        if sheet_name not in xl.sheet_names:
            log.warning(f"Sheet '{sheet_name}' not found — skipping")
            continue
        df = xl.parse(sheet_name, header=3)
        df.columns = [str(c).strip() for c in df.columns]
        df = df[df["INSTITUTION"].notna()].reset_index(drop=True)
        indices[db_col] = QSIndex(df, inst_col="INSTITUTION", rank_col="2026",
                                  country_col="COUNTRY/TERRITORY")
        log.info(f"Indexed {len(df)} rows from sheet '{sheet_name}'")
    return indices


def _load_overall_index() -> QSIndex | None:
    """Build QSIndex for overall QS 2026 world rankings."""
    if not os.path.exists(OVERALL_CSV):
        log.warning(f"Overall CSV not found: {OVERALL_CSV}")
        return None
    df = pd.read_csv(OVERALL_CSV)
    log.info(f"Indexed {len(df)} rows from {OVERALL_CSV}")
    return QSIndex(df, inst_col="Institution Name", rank_col="2026 Rank",
                   country_col="Country/Territory")


def _assign_tier(subject_ranks: dict, qs_overall: int | None) -> int:
    """Tier 1: best art rank ≤ 50 or overall ≤ 100.
    Tier 2: best art rank ≤ 200 or overall ≤ 300. Else Tier 3."""
    art_ranks = [v for v in subject_ranks.values() if v is not None]
    best = min(art_ranks) if art_ranks else None
    if (best and best <= 50) or (qs_overall and qs_overall <= 100):
        return 1
    if (best and best <= 200) or (qs_overall and qs_overall <= 300):
        return 2
    return 3


def _fmt_result(r: MatchResult | None) -> str:
    if r is None:
        return "n/a"
    if r.band == "auto_match":
        return str(r.rank)
    return f"~{r.band}({r.confidence:.2f})"


# ---------------------------------------------------------------------------
# LLM safety net
# ---------------------------------------------------------------------------

def _tavily_search(api_key: str, query: str, max_results: int = 5) -> list[dict]:
    try:
        resp = requests.post(
            TAVILY_URL,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"query": query, "search_depth": "basic", "max_results": max_results,
                  "include_raw_content": False, "include_answer": False},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("results") or []
    except Exception as e:
        log.warning(f"  [LLM] Tavily search failed: {e}")
        return []


def _build_qs_evidence(api_key: str, name_en: str) -> str:
    """Two targeted searches: overall rank + art/design subject ranks."""
    queries = [
        f'"{name_en}" QS World University Rankings 2026',
        f'"{name_en}" QS subject rankings 2026 art design architecture humanities',
    ]
    blocks: list[str] = []
    seen: set[str] = set()
    for query in queries:
        for r in _tavily_search(api_key, query, max_results=4):
            url = r.get("url") or ""
            if url in seen:
                continue
            seen.add(url)
            title   = (r.get("title") or "").strip()
            content = (r.get("content") or "").strip()[:600]
            blocks.append(f"[{title}]\n{url}\n{content}")
            if len(blocks) >= 8:
                break
        if len(blocks) >= 8:
            break
    return "\n\n---\n\n".join(blocks) if blocks else ""


def _parse_llm_ranks(text: str, name_en: str) -> dict[str, int | None]:
    """Extract the JSON dict from Claude's response."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    start, end = text.find("{"), text.rfind("}") + 1
    if start == -1 or end == 0:
        log.warning(f"  [LLM] no JSON in response for {name_en!r}")
        return {}
    try:
        raw = json.loads(text[start:end])
    except json.JSONDecodeError as e:
        log.warning(f"  [LLM] JSON parse error for {name_en!r}: {e}")
        return {}

    result: dict[str, int | None] = {}
    for field in ["qs_overall_rank", "qs_art_humanities_rank",
                  "qs_architecture_built_environment_rank",
                  "qs_art_design_rank", "qs_history_of_art_rank"]:
        val = raw.get(field)
        if val is None:
            result[field] = None
        else:
            try:
                result[field] = int(val)
            except (TypeError, ValueError):
                result[field] = None
    return result


def _llm_rank_lookup(
    name_en: str,
    country: str,
    settings: Settings,
    claude: anthropic.Anthropic,
) -> dict[str, int | None]:
    """Use Tavily + Claude Haiku to find QS 2026 ranks when the matcher fails."""
    evidence = _build_qs_evidence(settings.tavily_api_key, name_en)
    if not evidence:
        log.info(f"  [LLM] no web evidence found for {name_en!r}")
        return {}

    prompt = _LLM_USER.format(name_en=name_en, country=country or "unknown", evidence=evidence)
    try:
        resp = claude.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=256,
            system=_LLM_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as e:
        log.warning(f"  [LLM] Claude call failed for {name_en!r}: {e}")
        return {}

    text = "".join(b.text for b in resp.content if b.type == "text")
    ranks = _parse_llm_ranks(text, name_en)

    found = {k: v for k, v in ranks.items() if v is not None}
    if found:
        log.info(f"  [LLM] found for {name_en!r}: {found}")
    else:
        log.info(f"  [LLM] nothing found for {name_en!r}")
    return ranks


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _report(unmatched: list[dict], manual_review: list[dict]) -> None:
    if manual_review:
        log.warning(f"\n{'='*60}")
        log.warning(f"MANUAL REVIEW NEEDED ({len(manual_review)} schools) — "
                    f"confidence in [45%, 80%):")
        log.warning(f"  Add confirmed matches to data/qs_aliases.json and re-run.")
        log.warning(f"{'='*60}")
        for item in sorted(manual_review, key=lambda x: -x["best_conf"]):
            log.warning(
                f"  DB:        {item['name_en']!r}\n"
                f"  Best QS:   {item['best_qs_name']!r}  "
                f"(conf={item['best_conf']:.3f})\n"
                f"  Features:  {item['features']}"
            )
        log.warning(f"{'='*60}\n")

    if unmatched:
        log.warning(f"\n{'='*60}")
        log.warning(f"NOT IN QS DATA ({len(unmatched)} schools) — "
                    f"all fields NULL after matcher + LLM:")
        log.warning(f"{'='*60}")
        for item in sorted(unmatched, key=lambda x: x["name_en"]):
            log.warning(f"  {item['name_en']!r}")
        log.warning(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run(settings: Settings, batch_size: int) -> None:
    supabase = get_client(settings)
    claude   = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    subject_indices = _load_subject_indices()
    overall_index   = _load_overall_index()

    schools = fetch_by_status(supabase, "enriched", batch_size)
    log.info(f"Looking up QS rankings for {len(schools)} schools")

    manual_review: list[dict] = []
    unmatched:     list[dict] = []

    for school in schools:
        name_en = school["name_en"]
        country = school.get("country", "") or ""

        # --- Pass 1: local file matcher ---
        subject_ranks: dict[str, int | None] = {}
        subject_results: dict[str, MatchResult] = {}
        for db_col, index in subject_indices.items():
            result = index.match(name_en, country)
            subject_ranks[db_col] = result.rank
            subject_results[db_col] = result

        overall_result: MatchResult | None = None
        qs_overall: int | None = None
        if overall_index is not None:
            overall_result = overall_index.match(name_en, country)
            qs_overall = overall_result.rank

        # --- Pass 2: LLM safety net for any school missing qs_overall_rank ---
        llm_ranks: dict[str, int | None] = {}
        if qs_overall is None:
            llm_ranks = _llm_rank_lookup(name_en, country, settings, claude)
            time.sleep(0.5)   # respect rate limits

            # Merge: LLM fills only fields that are still NULL from matcher
            if llm_ranks.get("qs_overall_rank") is not None:
                qs_overall = llm_ranks["qs_overall_rank"]
            for field in subject_ranks:
                if subject_ranks[field] is None and llm_ranks.get(field) is not None:
                    subject_ranks[field] = llm_ranks[field]

        tier = _assign_tier(subject_ranks, qs_overall)

        upsert_school(supabase, {
            **school,
            **subject_ranks,
            "qs_overall_rank": qs_overall,
            "school_tier": tier,
            "status": "qs_done",
        })

        llm_tag = " [LLM]" if any(v is not None for v in llm_ranks.values()) else ""
        log.info(
            f"  {name_en}{llm_tag}: "
            f"art_hum={subject_ranks.get('qs_art_humanities_rank')}, "
            f"arch={subject_ranks.get('qs_architecture_built_environment_rank')}, "
            f"art_design={subject_ranks.get('qs_art_design_rank')}, "
            f"hist_art={subject_ranks.get('qs_history_of_art_rank')}, "
            f"overall={qs_overall}, tier={tier}"
        )

        # --- Collect for end-of-run reports ---
        all_results = list(subject_results.values()) + (
            [overall_result] if overall_result else []
        )
        best_result = max(all_results, key=lambda r: r.confidence, default=None)
        report_item = {
            "name_en":      name_en,
            "best_conf":    best_result.confidence if best_result else 0.0,
            "best_qs_name": best_result.qs_name    if best_result else None,
            "band":         best_result.band        if best_result else "auto_reject",
            "features":     best_result.features    if best_result else {},
        }

        all_null = all(v is None for v in subject_ranks.values()) and qs_overall is None
        any_manual = any(r.band == "manual_review" for r in all_results)

        if all_null:
            unmatched.append(report_item)
        elif any_manual and qs_overall is None:
            # Only flag manual review if LLM also found nothing
            manual_review.append(report_item)

    _report(unmatched, manual_review)
