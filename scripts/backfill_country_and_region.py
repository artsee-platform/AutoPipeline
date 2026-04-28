"""Backfill `schools.country_code` and `schools.region_tag` from the legacy
geography text column (`country` before P2, `raw_country` after
`db/migrate_p2_schools_raw_country_rename.sql`).

Run after `db/migrate_p1_country_region.sql` is applied. Idempotent — rows
that already match what the resolver would produce are skipped.

For every row:
  * If the legacy label is one of the 4 US-internal flagship buckets (e.g. "加州旗舰"),
    sets country_code='US' and region_tag='us_..._flagship'.
  * If the legacy label is one of the 5 multi-country buckets (e.g. "北欧",
    "其他亚洲国家"), sets region_tag only and leaves country_code=NULL for
    manual entry.
  * Otherwise treats the legacy label as a real country label and maps it to an
    ISO 3166-1 alpha-2 code.

At the end the script prints every row whose country_code is still NULL so
the operator can manually resolve them with plain SQL UPDATEs.

Usage:
    python -m scripts.backfill_country_and_region
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import load_settings
from db.supabase_client import get_client
from pipeline.country_normalizer import resolve_country
from utils.logger import get_logger

try:
    from postgrest.exceptions import APIError
except ImportError:  # pragma: no cover
    APIError = Exception  # type: ignore[misc, assignment]

log = get_logger("backfill_country_and_region")

TABLE = "schools"
PAGE = 500


def _is_missing_country_columns_error(exc: BaseException) -> bool:
    """postgrest.APIError exposes `.message`; fall back to str(exc)."""
    msg = getattr(exc, "message", None) or str(exc)
    return "country_code" in msg and "does not exist" in msg


def _ensure_country_columns(client) -> bool:
    """Return False if migrate_p1_country_region.sql was not applied."""
    try:
        client.table(TABLE).select("id,country_code,region_tag").limit(1).execute()
    except APIError as exc:
        if _is_missing_country_columns_error(exc):
            print(
                "\nError: `schools.country_code` (and likely `region_tag`) do not exist yet.\n"
                "Apply the migration first in the Supabase SQL Editor:\n"
                "  db/migrate_p1_country_region.sql\n"
                "Then run this script again.\n"
            )
            log.error("schema missing country columns — run db/migrate_p1_country_region.sql first")
            return False
        raise
    return True


def _legacy_geography_column(client) -> str | None:
    """`raw_country` after P2 rename; `country` on older schemas."""
    for col in ("raw_country", "country"):
        try:
            client.table(TABLE).select(f"id,{col}").limit(1).execute()
            return col
        except APIError as exc:
            msg = getattr(exc, "message", None) or str(exc)
            if "does not exist" in msg:
                continue
            raise
    print(
        "\nError: neither `schools.raw_country` nor `schools.country` exists.\n"
        "If P1 is applied, add/rename the legacy text column (see migrate_p2) "
        "or restore the column that holds the Chinese / bucket labels.\n"
    )
    log.error("no legacy geography text column on schools")
    return None


def _unchanged(current: dict, computed: dict) -> bool:
    return (
        current.get("country_code") == computed["country_code"]
        and current.get("region_tag") == computed["region_tag"]
    )


def main() -> int:
    settings = load_settings()
    client = get_client(settings)
    if not _ensure_country_columns(client):
        return 2

    label_col = _legacy_geography_column(client)
    if label_col is None:
        return 2

    start = 0
    seen = 0
    updated = 0
    unrecognised: list[dict] = []     # raw country could not be mapped at all
    needs_manual: list[dict] = []     # multi-country bucket: region_tag only

    while True:
        resp = (
            client.table(TABLE)
            .select(
                f"id,name_en,name_zh,{label_col},country_code,region_tag",
            )
            .order("id")
            .range(start, start + PAGE - 1)
            .execute()
        )
        rows = resp.data or []
        if not rows:
            break

        for row in rows:
            seen += 1
            raw = row.get(label_col)
            computed = resolve_country(raw)

            if computed["country_code"] is None and computed["region_tag"] is None and raw:
                unrecognised.append({
                    "id": row["id"],
                    "name_en": row.get("name_en"),
                    "name_zh": row.get("name_zh"),
                    "legacy_label": raw,
                })

            if computed["country_code"] is None and computed["region_tag"] is not None:
                needs_manual.append({
                    "id": row["id"],
                    "name_en": row.get("name_en"),
                    "name_zh": row.get("name_zh"),
                    "legacy_label": raw,
                    "region_tag": computed["region_tag"],
                })

            if _unchanged(row, computed):
                continue

            client.table(TABLE).update(computed).eq("id", row["id"]).execute()
            updated += 1

        log.info("scanned %d / updated %d", seen, updated)

        if len(rows) < PAGE:
            break
        start += PAGE

    log.info("backfill complete — scanned %d rows, updated %d", seen, updated)

    if unrecognised:
        log.warning(
            "%d rows have a legacy geography label that the normalizer could not map "
            "to any ISO code or region_tag. Add the missing alias to "
            "pipeline/country_normalizer.py and re-run.",
            len(unrecognised),
        )
        for r in unrecognised:
            log.warning(
                "  unrecognised: id=%s  legacy_label=%r  name_en=%s  name_zh=%s",
                r["id"], r["legacy_label"], r["name_en"], r["name_zh"],
            )

    if needs_manual:
        log.info(
            "%d rows are tagged with a multi-country bucket and need a "
            "manual country_code. List + ready-to-run UPDATE templates below:",
            len(needs_manual),
        )
        for r in needs_manual:
            log.info(
                "  manual: id=%s  region_tag=%s  legacy_label=%r  name_en=%s  name_zh=%s",
                r["id"], r["region_tag"], r["legacy_label"], r["name_en"], r["name_zh"],
            )
        print("\n-- ============================================================")
        print("-- Manually resolve the rows below by replacing 'XX' with the")
        print("-- correct ISO 3166-1 alpha-2 country code and running in Supabase.")
        print("-- ============================================================")
        for r in needs_manual:
            label = r["name_en"] or r["name_zh"] or "(no name)"
            print(
                f"UPDATE public.schools SET country_code = 'XX' "
                f"WHERE id = '{r['id']}';  -- {label} | bucket={r['region_tag']} | raw={r['legacy_label']!r}"
            )

    return 0


if __name__ == "__main__":
    sys.exit(main())
