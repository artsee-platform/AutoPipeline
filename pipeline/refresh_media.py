"""Standalone refresher for school logo_url and campus_image_urls only.

Purpose: re-run just the headless + Claude-vision media scraper for schools
that have already been enriched, without touching any other field. This lets
you iterate on the image-picking logic without re-paying the cost of Tavily
search, Claude research, QS ranking lookup, etc.

By default, only processes schools whose logo_url or campus_image_urls is
missing. Pass `all_schools=True` (CLI: --force-all) to reprocess every row.
"""
import time
from typing import Optional

import anthropic

from config.settings import Settings
from db.supabase_client import TABLE, get_client
from scrapers.website_scraper import scrape_school_website_smart
from utils.logger import get_logger

log = get_logger("refresh_media")


def _fetch_targets(
    client,
    batch_size: int,
    all_schools: bool,
    names: Optional[list[str]],
) -> list[dict]:
    """Pull the rows we want to re-scrape media for."""
    cols = "id, name_en, name_zh, official_website, logo_url, campus_image_urls, status"
    q = client.table(TABLE).select(cols)

    if names:
        q = q.in_("name_en", names)
    else:
        if not all_schools:
            # Missing logo OR missing/empty campus_image_urls.
            # PostgREST can't express `array_length = 0` directly via Supabase SDK,
            # so we over-select (is null OR eq empty-list), then filter client-side.
            q = q.or_("logo_url.is.null,campus_image_urls.is.null,campus_image_urls.eq.{}")

        q = q.limit(batch_size)

    resp = q.execute()
    rows = resp.data or []

    if not names and not all_schools:
        # Belt-and-suspenders: drop rows that already have both fields populated.
        rows = [
            r for r in rows
            if not r.get("logo_url") or not (r.get("campus_image_urls") or [])
        ]
    if names:
        rows = rows[:batch_size]

    return rows


def run(
    settings: Settings,
    batch_size: int,
    all_schools: bool = False,
    names: Optional[list[str]] = None,
    sleep_between: float = 1.0,
) -> None:
    """Refresh logo_url + campus_image_urls for matching schools.

    Args:
        settings: loaded Settings.
        batch_size: max number of schools to process.
        all_schools: if True, ignore the "missing media" filter.
        names: optional list of name_en to limit to (takes priority over the filter).
        sleep_between: polite delay between schools.
    """
    client = get_client(settings)
    claude = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    targets = _fetch_targets(client, batch_size, all_schools, names)
    log.info(f"Refreshing media for {len(targets)} school(s) (batch_size={batch_size}, "
             f"all={all_schools}, names={'yes' if names else 'no'})")

    ok = 0
    skipped = 0
    errors = 0

    for row in targets:
        name_en = row.get("name_en")
        website = row.get("official_website") or ""
        if not website:
            log.info(f"  - {name_en}: no official_website, skipping")
            skipped += 1
            continue

        log.info(f"→ {name_en}  ({website})")
        try:
            media = scrape_school_website_smart(website, school_name=name_en, claude=claude)
            payload = {
                "logo_url": media.get("logo_url"),
                "campus_image_urls": media.get("campus_image_urls") or [],
            }
            # Don't overwrite existing good data with None — only replace when we have something new
            if not payload["logo_url"] and row.get("logo_url"):
                payload.pop("logo_url")
            if not payload["campus_image_urls"] and (row.get("campus_image_urls") or []):
                payload.pop("campus_image_urls")

            if not payload:
                log.info(f"  = {name_en}: classifier returned nothing; keeping existing values")
                skipped += 1
            else:
                client.table(TABLE).update(payload).eq("id", row["id"]).execute()
                log.info(
                    f"  ✓ {name_en}: "
                    f"logo={'✓' if payload.get('logo_url') else '-'} "
                    f"campus={'✓' if payload.get('campus_image_urls') else '-'}"
                )
                ok += 1
        except Exception as exc:
            log.error(f"  ✗ {name_en}: {exc}")
            errors += 1

        if sleep_between:
            time.sleep(sleep_between)

    log.info(f"Refresh complete: {ok} updated, {skipped} skipped, {errors} errors")
