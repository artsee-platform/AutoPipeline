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
