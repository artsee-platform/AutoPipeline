"""Stage 1 — Web enrichment via Claude API web_search + official website scraping."""
import time
import anthropic
from config.settings import Settings
from db.supabase_client import get_client, fetch_by_status, update_status, upsert_school
from scrapers.claude_researcher import research_school
from scrapers.website_scraper import scrape_school_website
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

            # 2. Website scraping for logo + images
            site_data = scrape_school_website(school.get("official_website") or "")

            # 3. Merge: web_data wins for text fields; site_data for media
            merged = {**school, **web_data, **site_data, "status": "enriched"}
            # Preserve known fields from xlsx that Claude might overwrite
            merged["name_en"] = web_data.get("name_en") or name_en
            merged["name_zh"] = school.get("name_zh") or merged.get("name_zh")
            merged["country"] = school.get("country") or merged.get("country")
            merged["official_website"] = school.get("official_website") or merged.get("official_website")

            upsert_school(supabase, merged)
            log.info(f"  ✓ enriched: {name_en}")

        except Exception as exc:
            log.error(f"  ✗ {name_en}: {exc}")
            update_status(supabase, name_en, "error")

        # Brief pause between schools to respect Claude rate limits
        time.sleep(1.5)
