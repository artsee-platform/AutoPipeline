"""Insert schools from data/schools.xlsx into public `schools` when not already present.

Excel uses wrapped/continuation rows: a row with empty name_zh but non-empty name_en
continues the previous school's English name. Same normalization as Stage 0 for newlines.

Overlap detection:
- Primary: normalized Chinese name (all whitespace removed) — matches DB despite spaces in cells.
- Fallback: normalized English (whitespace-collapsed, case-insensitive).

`schools.id` is uuid with default gen_random_uuid(); no need to compute or backfill UUIDs manually.

Usage:
  python -m pipeline.sync_public_schools
  python -m pipeline.sync_public_schools --xlsx /path/to/schools.xlsx
  python -m pipeline.sync_public_schools --dry-run
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd

from config.settings import Settings, load_settings
from db.supabase_client import TABLE as SCHOOLS_TABLE, get_client
from utils.logger import get_logger

log = get_logger("sync_public_schools")

XLSX_PATH = Path(__file__).parent.parent / "data" / "schools.xlsx"


def clean_str(val) -> str | None:
    if pd.isna(val):
        return None
    t = str(val).strip()
    if not t:
        return None
    t = re.sub(r"\s+", " ", t.replace("\n", " "))
    return t or None


def norm_zh(s: str | None) -> str:
    if not s:
        return ""
    c = clean_str(s)
    if not c:
        return ""
    return re.sub(r"\s+", "", c)


def norm_en(s: str | None) -> str:
    if not s:
        return ""
    c = clean_str(s)
    if not c:
        return ""
    return c.lower()


def load_merged_schools_from_xlsx(path: Path) -> list[dict]:
    """Merge continuation rows (blank name_zh, non-empty name_en) into previous school."""
    df = pd.read_excel(path, dtype=str)
    schools: list[dict] = []
    current: dict | None = None
    for _, row in df.iterrows():
        zh = clean_str(row.get("name_zh"))
        en = clean_str(row.get("name_en"))
        if not zh and not en:
            continue
        if zh:
            if current:
                schools.append(current)
            current = {"name_zh": zh, "name_en": en or ""}
        else:
            if en and current:
                current["name_en"] = (current["name_en"] + " " + en).strip()
    if current:
        schools.append(current)
    return schools


def _fetch_all_schools_name_columns(client, columns: str) -> list[dict]:
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


def compute_inserts(settings: Settings, *, xlsx_path: Path | None = None) -> list[dict]:
    client = get_client(settings)
    rows = _fetch_all_schools_name_columns(client, "name_en,name_zh")

    existing_zh = set()
    existing_en = set()
    for r in rows:
        zh = clean_str(r.get("name_zh"))
        if zh:
            existing_zh.add(norm_zh(zh))
        en = clean_str(r.get("name_en"))
        if en:
            existing_en.add(norm_en(en))

    path = xlsx_path or XLSX_PATH
    xlsx_schools = load_merged_schools_from_xlsx(path)
    to_insert: list[dict] = []
    seen_zh: set[str] = set()
    seen_en: set[str] = set()

    for s in xlsx_schools:
        name_zh = s.get("name_zh")
        name_en = (s.get("name_en") or "").strip()
        if not name_en:
            continue
        nz = norm_zh(name_zh)
        ne = norm_en(name_en)
        dup = False
        if nz and nz in existing_zh:
            dup = True
        elif ne and ne in existing_en:
            dup = True
        elif nz and nz in seen_zh:
            dup = True
        elif ne and ne in seen_en:
            dup = True
        if dup:
            continue
        if nz:
            seen_zh.add(nz)
        seen_en.add(ne)
        to_insert.append({"name_en": name_en, "name_zh": name_zh, "status": "pending"})

    return to_insert


def run(
    settings: Settings,
    *,
    dry_run: bool = False,
    xlsx_path: Path | None = None,
) -> int:
    path = xlsx_path or XLSX_PATH
    to_insert = compute_inserts(settings, xlsx_path=path)
    log.info(
        "From %s: %d school(s) not yet in %s",
        path,
        len(to_insert),
        SCHOOLS_TABLE,
    )
    for row in to_insert:
        log.info("  insert: %r / %r", row.get("name_en"), row.get("name_zh"))

    if dry_run:
        log.info("Dry run — no rows inserted.")
        return len(to_insert)

    if not to_insert:
        return 0

    client = get_client(settings)
    batch_size = 100
    for i in range(0, len(to_insert), batch_size):
        chunk = to_insert[i : i + batch_size]
        client.table(SCHOOLS_TABLE).insert(chunk).execute()
    log.info("Inserted %d row(s) into %s.", len(to_insert), SCHOOLS_TABLE)
    return len(to_insert)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--xlsx",
        type=Path,
        default=None,
        help=f"Excel path (default: {XLSX_PATH})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List rows that would be inserted without writing",
    )
    args = parser.parse_args()
    settings = load_settings()
    run(settings, dry_run=args.dry_run, xlsx_path=args.xlsx)


if __name__ == "__main__":
    main()
