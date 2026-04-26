"""Sync the `degree_labels` dictionary table from `pipeline/degree_normalizer.py`.

The normalizer module is the single source of truth for the controlled
vocabulary used by `programs.normalized_degree_type`. This script reads its
in-memory catalog (singles + known combined patterns) and upserts it into the
dictionary table so the FK on `programs` always has the codes it needs.

Run after editing `_CANONICAL_FAMILY`, `_COMBINED_CATALOG`, or `_ALIASES` in
the normalizer:

    python -m scripts.sync_degree_labels

Idempotent. Will not delete codes that exist in the table but are no longer
in the normalizer — those need to be removed manually after confirming no
`programs` row references them.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import load_settings
from db.supabase_client import get_client
from pipeline.degree_normalizer import iter_label_catalog
from utils.logger import get_logger

log = get_logger("sync_degree_labels")

TABLE = "degree_labels"


def main() -> int:
    settings = load_settings()
    client = get_client(settings)

    catalog = list(iter_label_catalog())
    expected_codes = {entry["code"] for entry in catalog}

    existing = (
        client.table(TABLE)
        .select("code,family,is_combined,parts")
        .execute()
        .data
        or []
    )
    existing_by_code = {row["code"]: row for row in existing}

    inserted = updated = 0
    for entry in catalog:
        current = existing_by_code.get(entry["code"])
        payload = {
            "code": entry["code"],
            "family": entry["family"],
            "is_combined": entry["is_combined"],
            "parts": entry["parts"],
        }
        if current is None:
            client.table(TABLE).insert(payload).execute()
            inserted += 1
            continue
        # Skip no-op updates so we don't bump updated_at unnecessarily.
        if (
            current.get("family") == payload["family"]
            and bool(current.get("is_combined")) == payload["is_combined"]
            and (current.get("parts") or None) == payload["parts"]
        ):
            continue
        client.table(TABLE).update(payload).eq("code", entry["code"]).execute()
        updated += 1

    stale = sorted(set(existing_by_code) - expected_codes)
    log.info(
        "sync complete — catalog=%d, inserted=%d, updated=%d, stale_in_db=%d",
        len(catalog),
        inserted,
        updated,
        len(stale),
    )
    if stale:
        log.warning(
            "codes present in degree_labels but not in normalizer (review/remove manually): %s",
            ", ".join(stale),
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
