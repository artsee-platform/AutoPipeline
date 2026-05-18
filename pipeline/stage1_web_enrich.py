"""Stage 1 — Web enrichment via Claude API web_search + official website scraping."""
import time
import anthropic
from config.settings import Settings
from db.media_status import upsert_media_status
from db.supabase_client import get_client, fetch_by_status, update_status, upsert_school
from pipeline.media_storage import store_school_media
from scrapers.claude_researcher import research_school
from scrapers.website_scraper import scrape_school_website_smart
from utils.logger import get_logger

log = get_logger("stage1")


def run(settings: Settings, batch_size: int) -> None:
    supabase = get_client(settings)
    claude = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    schools = fetch_by_status(supabase, "pending", batch_size)
    log.info(f"Processing {len(schools)} schools (batch_size={batch_size})")

    for school in schools:
        name_en = school["name_en"]
        log.info(f"→ {name_en}")

        # Mark as processing immediately to prevent duplicate runs
        update_status(supabase, name_en, "processing")

        try:
            # 1. Claude web research
            web_data = research_school(
                claude,
                settings,
                name_en=name_en,
                name_zh=school.get("name_zh") or "",
                website=school.get("official_website") or "",
            )

            # 2. Website scraping for logo + images (headless browser + Claude vision,
            #    with static-HTML fallback)
            site_data = scrape_school_website_smart(
                school.get("official_website") or "",
                school_name=name_en,
                claude=claude,
                image_search_api_key=settings.tavily_api_key,
            )
            site_media = {
                "logo_url": site_data.get("logo_url"),
                "campus_image_urls": site_data.get("campus_image_urls") or [],
            }
            stored_logo = store_school_media(
                supabase,
                bucket=settings.school_media_bucket,
                school_id=school["id"],
                kind="logo",
                source_url=site_media["logo_url"],
            )
            if stored_logo:
                site_media["logo_url"] = stored_logo.public_url

            campus_url = site_media["campus_image_urls"][0] if site_media["campus_image_urls"] else None
            stored_campus = store_school_media(
                supabase,
                bucket=settings.school_media_bucket,
                school_id=school["id"],
                kind="campus-1",
                source_url=campus_url,
            )
            if stored_campus:
                site_media["campus_image_urls"] = [stored_campus.public_url]

            # 3. Merge: web_data wins for text fields; site_data for media
            merged = {**school, **web_data, **site_media, "status": "enriched"}
            # Preserve known fields from xlsx that Claude might overwrite
            merged["name_en"] = web_data.get("name_en") or name_en
            merged["name_zh"] = school.get("name_zh") or merged.get("name_zh")
            merged["raw_country"] = (
                school.get("raw_country")
                or school.get("country")
                or merged.get("raw_country")
                or merged.get("country")
            )
            merged["official_website"] = school.get("official_website") or merged.get("official_website")

            upsert_school(supabase, merged)
            upsert_media_status(
                supabase,
                merged["id"],
                logo_status=site_data.get("logo_status"),
                campus_image_status=site_data.get("campus_image_status"),
            )
            log.info(f"  ✓ enriched: {name_en}")

        except Exception as exc:
            log.error(f"  ✗ {name_en}: {exc}")
            update_status(supabase, name_en, "error")

        # Brief pause between schools to respect Claude rate limits
        time.sleep(1.5)
