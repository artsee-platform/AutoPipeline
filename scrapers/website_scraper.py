from typing import Optional

import anthropic
import requests
import requests_cache
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from utils.logger import get_logger
from utils.retry import retry

log = get_logger("website_scraper")

# Use a simple SQLite cache so repeated runs don't re-fetch
requests_cache.install_cache("data/cache/http_cache", expire_after=86400 * 7)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}


@retry(max_attempts=3, base_delay=2.0)
def scrape_school_website(url: str) -> dict:
    """Scrape a school's official website for logo and campus images.

    Returns a dict with keys: logo_url, campus_image_urls
    """
    result = {"logo_url": None, "campus_image_urls": []}

    if not url:
        return result

    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        log.warning(f"Failed to fetch {url}: {e}")
        return result

    soup = BeautifulSoup(resp.text, "html.parser")

    # --- Logo: try og:image first, then apple-touch-icon, then first header img ---
    og_image = soup.find("meta", property="og:image")
    if og_image and og_image.get("content"):
        result["logo_url"] = _abs(url, og_image["content"])
    else:
        touch_icon = soup.find("link", rel=lambda r: r and "apple-touch-icon" in r)
        if touch_icon and touch_icon.get("href"):
            result["logo_url"] = _abs(url, touch_icon["href"])
        else:
            # Try a logo img in header/nav
            for tag in soup.select("header img, nav img, .logo img, #logo img"):
                src = tag.get("src") or tag.get("data-src")
                if src:
                    result["logo_url"] = _abs(url, src)
                    break

    # --- Campus images: collect up to 5 large images from the page ---
    campus_imgs = []
    for img in soup.find_all("img", src=True):
        src = img["src"]
        # Skip tiny icons / tracking pixels
        w = img.get("width") or img.get("data-width") or "0"
        h = img.get("height") or img.get("data-height") or "0"
        try:
            if int(str(w).replace("px", "") or 0) < 100:
                continue
        except ValueError:
            pass
        if any(kw in src.lower() for kw in ["logo", "icon", "favicon", "pixel", "tracking"]):
            continue
        abs_src = _abs(url, src)
        if abs_src and abs_src not in campus_imgs:
            campus_imgs.append(abs_src)
        if len(campus_imgs) >= 5:
            break

    result["campus_image_urls"] = campus_imgs
    log.info(f"Scraped {url}: logo={'yes' if result['logo_url'] else 'no'}, {len(campus_imgs)} campus imgs")
    return result


def _abs(base: str, href: str) -> str | None:
    if not href:
        return None
    if href.startswith("data:"):
        return None
    return urljoin(base, href)


# -----------------------------------------------------------------------------
# Smart scraper: headless browser + multimodal classifier, with static fallback
# -----------------------------------------------------------------------------

def scrape_school_website_smart(
    url: str,
    school_name: str,
    claude: Optional[anthropic.Anthropic] = None,
) -> dict:
    """Headless + Claude-vision scraper with graceful static-HTML fallback.

    Strategy:
      1. Use a headless Chromium to render the page and collect every plausible
         image URL (including CSS background-image and lazy-loaded <img>).
      2. Heuristically pre-rank candidates into logo-likely and campus-likely sets.
      3. Ask Claude (vision) to pick the single best one from each set.
      4. For any field Claude can't resolve, fill from the static-HTML fallback.

    Returns the same shape as scrape_school_website: {"logo_url", "campus_image_urls"}.
    campus_image_urls will contain at most one element (the single best cover photo).
    """
    empty = {"logo_url": None, "campus_image_urls": []}
    if not url:
        return empty

    # Without a Claude client we can't do the multimodal pick, so just fall back.
    if claude is None:
        log.info(f"smart scrape: no Claude client provided, using static fallback for {url}")
        return scrape_school_website(url)

    try:
        # Import lazily so missing Playwright / circular-import risks don't break
        # the static fallback path.
        from scrapers.headless_image_scraper import (
            collect_candidates,
            select_campus_candidates,
            select_logo_candidates,
        )
        from pipeline.image_classifier import pick_best_campus, pick_best_logo

        raw_candidates = collect_candidates(url)
        if not raw_candidates:
            log.info(f"smart scrape: no candidates from headless, falling back ({url})")
            return scrape_school_website(url)

        logo_cands = select_logo_candidates(raw_candidates)
        campus_cands = select_campus_candidates(raw_candidates)

        if logo_cands:
            logo_url, logo_status = pick_best_logo(claude, school_name, logo_cands)
        else:
            logo_url, logo_status = None, "error"

        if campus_cands:
            campus_url, campus_status = pick_best_campus(claude, school_name, campus_cands)
        else:
            campus_url, campus_status = None, "error"

        # Fall back to static HTML ONLY when the classifier couldn't actually run
        # (no candidates, download/API failure). If Claude explicitly said "none match",
        # we trust that verdict and leave the field empty rather than serving garbage.
        need_fallback = (logo_status == "error") or (campus_status == "error")
        if need_fallback:
            fb = scrape_school_website(url)
            if logo_status == "error" and not logo_url:
                logo_url = fb.get("logo_url")
            if campus_status == "error" and not campus_url:
                fb_campus = fb.get("campus_image_urls") or []
                campus_url = fb_campus[0] if fb_campus else None

        return {
            "logo_url": logo_url,
            "campus_image_urls": [campus_url] if campus_url else [],
        }
    except Exception as exc:
        log.exception(f"smart scrape failed for {url}: {exc}; using static fallback")
        return scrape_school_website(url)
