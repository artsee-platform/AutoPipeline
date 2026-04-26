"""Backfill `normalized_degree_type` and `honours_flag` on public.programs.

Run after editing `pipeline/degree_normalizer.py` (e.g. adding new aliases or
combined-degree patterns) to bring existing rows up to date. Idempotent: rows
whose values already match what the normalizer would produce are skipped.

Usage:
    python -m scripts.backfill_degree_normalization
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import load_settings
from db.supabase_client import get_client
from pipeline.degree_normalizer import normalize_degree
from utils.logger import get_logger

log = get_logger("backfill_degree_normalization")

TABLE = "programs"
PAGE = 500


def _unchanged(current: dict, computed: dict) -> bool:
    return (
        current.get("normalized_degree_type") == computed["normalized_degree_type"]
        and bool(current.get("honours_flag")) == computed["honours_flag"]
    )


def main() -> int:
    settings = load_settings()
    client = get_client(settings)

    start = 0
    seen = 0
    updated = 0

    while True:
        resp = (
            client.table(TABLE)
            .select("id,raw_degree_type,normalized_degree_type,honours_flag")
            .order("id")
            .range(start, start + PAGE - 1)
            .execute()
        )
        rows = resp.data or []
        if not rows:
            break

        for row in rows:
            seen += 1
            computed = normalize_degree(row.get("raw_degree_type"))
            if _unchanged(row, computed):
                continue
            client.table(TABLE).update(computed).eq("id", row["id"]).execute()
            updated += 1

        log.info("scanned %d / updated %d", seen, updated)

        if len(rows) < PAGE:
            break
        start += PAGE

    log.info("backfill complete — scanned %d rows, updated %d", seen, updated)
    return 0


if __name__ == "__main__":
    sys.exit(main())
