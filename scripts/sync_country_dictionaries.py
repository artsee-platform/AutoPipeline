"""Sync the `countries` and `region_tags` dictionary tables from
`pipeline/country_normalizer.py`.

The normalizer module is the single source of truth for both controlled
vocabularies. This script reads the in-memory catalogs and upserts them into
the dictionary tables so the FKs on `schools` always have the codes they need.

Run after editing `_COUNTRY_DATA` or `_REGION_TAGS` in the normalizer:

    python -m scripts.sync_country_dictionaries

Idempotent. Will not delete codes that exist in the table but are no longer
in the normalizer — those need to be removed manually after confirming no
`schools` row references them (FK is RESTRICT).
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import load_settings
from db.supabase_client import get_client
from pipeline.country_normalizer import (
    iter_country_catalog,
    iter_region_tag_catalog,
)
from utils.logger import get_logger

log = get_logger("sync_country_dictionaries")


def _sync_countries(client) -> tuple[int, int, list[str]]:
    table = "countries"
    catalog = list(iter_country_catalog())
    expected = {entry["code"] for entry in catalog}

    existing = (
        client.table(table)
        .select("code,name_en,name_zh,region_continent,sort_order")
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
            "region_continent": entry["region_continent"],
            "sort_order": entry["sort_order"],
        }
        if current is None:
            client.table(table).insert(payload).execute()
            inserted += 1
            continue
        if (
            current.get("name_en") == payload["name_en"]
            and current.get("name_zh") == payload["name_zh"]
            and current.get("region_continent") == payload["region_continent"]
            and current.get("sort_order") == payload["sort_order"]
        ):
            continue
        client.table(table).update(payload).eq("code", entry["code"]).execute()
        updated += 1

    stale = sorted(set(by_code) - expected)
    return inserted, updated, stale


def _sync_region_tags(client) -> tuple[int, int, list[str]]:
    table = "region_tags"
    catalog = list(iter_region_tag_catalog())
    expected = {entry["code"] for entry in catalog}

    existing = (
        client.table(table)
        .select("code,name_en,name_zh,scope,implied_country_code,sort_order")
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
            "scope": entry["scope"],
            "implied_country_code": entry["implied_country_code"],
            "sort_order": entry["sort_order"],
        }
        if current is None:
            client.table(table).insert(payload).execute()
            inserted += 1
            continue
        if (
            current.get("name_en") == payload["name_en"]
            and current.get("name_zh") == payload["name_zh"]
            and current.get("scope") == payload["scope"]
            and current.get("implied_country_code") == payload["implied_country_code"]
            and current.get("sort_order") == payload["sort_order"]
        ):
            continue
        client.table(table).update(payload).eq("code", entry["code"]).execute()
        updated += 1

    stale = sorted(set(by_code) - expected)
    return inserted, updated, stale


def main() -> int:
    settings = load_settings()
    client = get_client(settings)

    c_inserted, c_updated, c_stale = _sync_countries(client)
    log.info(
        "countries sync — inserted=%d, updated=%d, stale_in_db=%d",
        c_inserted, c_updated, len(c_stale),
    )
    if c_stale:
        log.warning(
            "countries codes present in DB but not in normalizer "
            "(review/remove manually): %s",
            ", ".join(c_stale),
        )

    r_inserted, r_updated, r_stale = _sync_region_tags(client)
    log.info(
        "region_tags sync — inserted=%d, updated=%d, stale_in_db=%d",
        r_inserted, r_updated, len(r_stale),
    )
    if r_stale:
        log.warning(
            "region_tags codes present in DB but not in normalizer "
            "(review/remove manually): %s",
            ", ".join(r_stale),
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
