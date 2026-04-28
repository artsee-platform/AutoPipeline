"""Sync `public.currencies` from `pipeline/currency_catalog.py`.

    python -m scripts.sync_currencies

Idempotent. Does not delete DB codes absent from the catalog — remove manually
after confirming no `program_fees` row references them (FK RESTRICT).
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import load_settings
from db.supabase_client import get_client
from pipeline.currency_catalog import iter_currency_catalog
from utils.logger import get_logger

log = get_logger("sync_currencies")

TABLE = "currencies"


def main() -> int:
    settings = load_settings()
    client = get_client(settings)

    catalog = list(iter_currency_catalog())
    expected = {r["code"] for r in catalog}

    existing = (
        client.table(TABLE)
        .select("code,name_en,name_zh,sort_order")
        .execute()
        .data
        or []
    )
    by_code = {row["code"]: row for row in existing}

    inserted = updated = 0
    for entry in catalog:
        current = by_code.get(entry["code"])
        payload = {
            "code": entry["code"],
            "name_en": entry["name_en"],
            "name_zh": entry["name_zh"],
            "sort_order": entry["sort_order"],
        }
        if current is None:
            client.table(TABLE).insert(payload).execute()
            inserted += 1
            continue
        if (
            current.get("name_en") == payload["name_en"]
            and current.get("name_zh") == payload["name_zh"]
            and current.get("sort_order") == payload["sort_order"]
        ):
            continue
        client.table(TABLE).update(payload).eq("code", entry["code"]).execute()
        updated += 1

    stale = sorted(set(by_code) - expected)
    log.info("currencies sync — catalog=%d, inserted=%d, updated=%d, stale_in_db=%d",
             len(catalog), inserted, updated, len(stale))
    if stale:
        log.warning("codes in DB but not in catalog (review): %s", ", ".join(stale))
    return 0


if __name__ == "__main__":
    sys.exit(main())
