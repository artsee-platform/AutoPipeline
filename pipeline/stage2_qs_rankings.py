"""Stage 2 — QS Rankings lookup via fuzzy matching against local CSVs."""
import os
import pandas as pd
from rapidfuzz import process, fuzz
from config.settings import Settings
from db.supabase_client import get_client, fetch_by_status, update_status, upsert_school
from utils.logger import get_logger

log = get_logger("stage2")

QS_ART_CSV = "data/qs_art_design.csv"
QS_ARCH_CSV = "data/qs_architecture.csv"
QS_OVERALL_CSV = "data/qs_overall.csv"

FUZZY_THRESHOLD = 85


def _load_qs(path: str, rank_col: str = "Rank") -> pd.DataFrame | None:
    if not os.path.exists(path):
        log.warning(f"QS CSV not found: {path} — skipping this ranking")
        return None
    df = pd.read_csv(path)
    # Normalise column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    log.info(f"Loaded {len(df)} rows from {path}")
    return df


def _fuzzy_rank(df: pd.DataFrame, name_en: str, institution_col: str = "institution") -> int | None:
    if df is None or institution_col not in df.columns:
        return None
    choices = df[institution_col].tolist()
    match = process.extractOne(name_en, choices, scorer=fuzz.token_sort_ratio)
    if not match or match[1] < FUZZY_THRESHOLD:
        return None
    matched_name = match[0]
    row = df[df[institution_col] == matched_name].iloc[0]
    # Rank column may be named 'rank', '2025_rank', etc.
    rank_cols = [c for c in df.columns if "rank" in c]
    if not rank_cols:
        return None
    raw_rank = str(row[rank_cols[0]]).strip()
    # Handle ranges like "51-100"
    raw_rank = raw_rank.split("-")[0].replace("=", "").strip()
    try:
        return int(raw_rank)
    except ValueError:
        return None


def _assign_tier(qs_art: int | None, qs_overall: int | None) -> int:
    """Tier 1: QS art ≤ 50 or overall ≤ 100; Tier 2: art ≤ 200 or overall ≤ 300; else Tier 3."""
    if (qs_art and qs_art <= 50) or (qs_overall and qs_overall <= 100):
        return 1
    if (qs_art and qs_art <= 200) or (qs_overall and qs_overall <= 300):
        return 2
    return 3


def run(settings: Settings, batch_size: int) -> None:
    supabase = get_client(settings)

    df_art = _load_qs(QS_ART_CSV)
    df_arch = _load_qs(QS_ARCH_CSV)
    df_overall = _load_qs(QS_OVERALL_CSV)

    schools = fetch_by_status(supabase, "enriched", batch_size)
    log.info(f"Looking up QS rankings for {len(schools)} schools")

    for school in schools:
        name_en = school["name_en"]
        qs_art = _fuzzy_rank(df_art, name_en)
        qs_arch = _fuzzy_rank(df_arch, name_en)
        qs_overall = _fuzzy_rank(df_overall, name_en)
        tier = _assign_tier(qs_art, qs_overall)

        updates = {
            "qs_art_rank": qs_art,
            "qs_architecture_rank": qs_arch,
            "qs_overall_rank": qs_overall,
            "school_tier": tier,
            "status": "qs_done",
        }
        upsert_school(supabase, {**school, **updates})
        log.info(
            f"  {name_en}: art={qs_art}, arch={qs_arch}, overall={qs_overall}, tier={tier}"
        )
