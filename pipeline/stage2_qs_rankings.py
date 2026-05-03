"""Stage 2 — QS Rankings lookup via multi-stage name matching + LLM safety net."""
import time
from pathlib import Path
import os
import anthropic
import pandas as pd
from config.settings import Settings
from db.supabase_client import get_client, fetch_by_status, upsert_school
from utils.logger import get_logger
from pipeline.qs_matcher import QSIndex, MatchResult
from pipeline.qs_global_rank import (
    load_overall_index,
    resolve_qs_overall_rank_with_llm,
    format_qs_overall_rank_value,
)

log = get_logger("stage2")

_DATA_DIR = Path(__file__).parent.parent / "data"
SUBJECT_EXCEL = _DATA_DIR / "qs_data_subject.xlsx"

# Supabase column → Excel sheet name (art-domain subjects only)
ART_SUBJECTS = {
    "qs_art_humanities_rank":                 "Arts & Humanities",
    "qs_architecture_built_environment_rank": "Architecture _ Built Environmen",
    "qs_art_design_rank":                     "Art & Design",
    "qs_history_of_art_rank":                 "History of Art",
}


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
    overall_index   = load_overall_index()

    schools = fetch_by_status(supabase, "enriched", batch_size)
    log.info(f"Looking up QS rankings for {len(schools)} schools")

    manual_review: list[dict] = []
    unmatched:     list[dict] = []

    for school in schools:
        name_en = school["name_en"]
        country = school.get("raw_country") or school.get("country") or ""

        # --- Pass 1: local file matcher ---
        subject_ranks: dict[str, int | None] = {}
        subject_results: dict[str, MatchResult] = {}
        for db_col, index in subject_indices.items():
            result = index.match(name_en, country)
            subject_ranks[db_col] = result.rank
            subject_results[db_col] = result

        overall_result: MatchResult | None = None
        if overall_index is not None:
            overall_result = overall_index.match(name_en, country)

        qs_overall, llm_ranks, used_llm = resolve_qs_overall_rank_with_llm(
            name_en, country, overall_index, settings, claude
        )
        if used_llm:
            time.sleep(0.5)  # respect rate limits
            for field in subject_ranks:
                if subject_ranks[field] is None and llm_ranks.get(field) is not None:
                    subject_ranks[field] = llm_ranks[field]

        tier = _assign_tier(subject_ranks, qs_overall)

        upsert_school(supabase, {
            **school,
            **subject_ranks,
            "qs_overall_rank": format_qs_overall_rank_value(qs_overall),
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
            f"overall={qs_overall}, qs_overall_rank={format_qs_overall_rank_value(qs_overall)}, tier={tier}"
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
