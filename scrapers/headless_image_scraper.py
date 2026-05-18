"""Headless-browser based image candidate collector.

Renders the target page (so JS / lazy-loaded images show up), then extracts
every plausible image URL along with metadata (source type, rendered size,
alt text, DOM context). Heuristic pre-filters then narrow the raw candidate
list down to ~top-N for each purpose (logo vs campus), which keeps the
downstream multimodal-LLM call cheap and focused.

Only URLs are returned — no bytes are downloaded here.
"""

from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urljoin

from utils.logger import get_logger

log = get_logger("headless_image_scraper")


# JS snippet evaluated in page context. Returns a dict of candidate groups.
# Kept in one string so we can iterate without touching Python each time.
_COLLECT_JS = r"""
() => {
    const out = { meta_images: [], icons: [], imgs: [], bgs: [] };

    // <meta property="og:image"> / twitter:image
    for (const m of document.querySelectorAll('meta')) {
        const key = (m.getAttribute('property') || m.getAttribute('name') || '').toLowerCase();
        if (key.includes('og:image') || key.includes('twitter:image')) {
            const c = m.getAttribute('content');
            if (c) out.meta_images.push({ url: c, key });
        }
    }

    // <link rel="icon|apple-touch-icon|mask-icon|shortcut icon">
    for (const link of document.querySelectorAll('link[rel]')) {
        const rel = (link.getAttribute('rel') || '').toLowerCase();
        if (rel.includes('icon') || rel.includes('mask-icon')) {
            const href = link.getAttribute('href');
            if (href) out.icons.push({ url: href, rel, sizes: link.getAttribute('sizes') || '' });
        }
    }

    // All <img>
    for (const img of document.querySelectorAll('img')) {
        const src = img.currentSrc || img.src || img.getAttribute('data-src') || '';
        if (!src) continue;
        const rect = img.getBoundingClientRect();
        const w = img.naturalWidth || rect.width || 0;
        const h = img.naturalHeight || rect.height || 0;

        let context = '';
        if (img.closest('header')) context = 'header';
        else if (img.closest('nav')) context = 'nav';
        else if (img.closest('footer')) context = 'footer';
        else if (img.closest('.hero,.banner,.slider,.carousel,.swiper,#hero,.cover')) context = 'hero';
        else if (img.closest('main')) context = 'main';

        const parent = img.closest('a, header, nav, [id*="logo" i], [class*="logo" i], [class*="brand" i]');
        out.imgs.push({
            url: src,
            w, h,
            x: rect.x,
            y: rect.y,
            alt: img.alt || '',
            context,
            cls: img.className || '',
            parentText: parent ? (parent.innerText || parent.getAttribute('aria-label') || parent.getAttribute('title') || '') : '',
            parentCls: parent ? String(parent.className || '') : '',
            parentId: parent ? String(parent.id || '') : ''
        });
    }

    // CSS background-image on sizeable elements (often where the real hero image lives)
    const bgTargets = document.querySelectorAll(
        'section, div, header, main, a, span, article, figure'
    );
    for (const el of bgTargets) {
        const style = window.getComputedStyle(el);
        const bg = style.backgroundImage;
        if (!bg || bg === 'none') continue;
        const m = bg.match(/url\(['"]?([^'")]+)['"]?\)/);
        if (!m) continue;
        const rect = el.getBoundingClientRect();
        const label = [
            el.id || '',
            String(el.className || ''),
            el.getAttribute('aria-label') || '',
            el.getAttribute('title') || '',
            el.innerText || ''
        ].join(' ').toLowerCase();
        const isLogoish = label.includes('logo') || label.includes('brand') || label.includes('university') || label.includes('college');
        const isTop = rect.top >= -20 && rect.top <= 300;
        if (!isLogoish && !isTop && (rect.width < 200 || rect.height < 150)) continue;
        if (rect.width < 24 || rect.height < 18) continue;
        out.bgs.push({
            url: m[1], w: rect.width, h: rect.height, x: rect.x, y: rect.y,
            cls: String(el.className || ''), id: String(el.id || ''), text: el.innerText || ''
        });
    }

    return out;
}
"""


@dataclass
class ImageCandidate:
    url: str
    source: str                        # og_image | favicon | img_tag | bg_image | image_search
    width: int = 0
    height: int = 0
    left: int = 0
    top: int = 0
    alt: str = ""
    context: str = ""                  # header | nav | footer | hero | main | ""
    cls: str = ""
    extra: dict = field(default_factory=dict)


_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def collect_candidates(
    url: str,
    goto_timeout_ms: int = 20000,
    scroll: bool = True,
) -> list[ImageCandidate]:
    """Launch a headless Chromium, render `url`, and return unique image candidates.

    Returns an empty list on any failure — caller should treat that as a signal to
    fall back to a simpler scraper.
    """
    if not url:
        return []

    # Import locally so projects that never call this path don't need Playwright installed.
    from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout

    raw = None
    final_url = url

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                ctx = browser.new_context(user_agent=_USER_AGENT, viewport={"width": 1440, "height": 900})
                page = ctx.new_page()
                try:
                    page.goto(url, wait_until="networkidle", timeout=goto_timeout_ms)
                except PwTimeout:
                    # networkidle can be slow on ad/tracker-heavy sites; domcontentloaded is enough
                    log.info(f"networkidle timed out for {url}, retry with domcontentloaded")
                    page.goto(url, wait_until="domcontentloaded", timeout=goto_timeout_ms)

                if not _wait_past_verification(page):
                    log.warning(f"headless collect skipped verification/interstitial page for {url}")
                    return []

                # Trigger lazy-loading
                if scroll:
                    try:
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        page.wait_for_timeout(1500)
                        page.evaluate("window.scrollTo(0, 0)")
                        page.wait_for_timeout(500)
                    except Exception:
                        pass

                final_url = page.url
                raw = page.evaluate(_COLLECT_JS)
            finally:
                browser.close()
    except Exception as e:
        log.warning(f"headless collect failed for {url}: {e}")
        return []

    if not raw:
        return []

    seen: set[str] = set()
    candidates: list[ImageCandidate] = []

    def _add(cand: ImageCandidate) -> None:
        if not cand.url or cand.url.startswith("data:"):
            return
        if cand.url in seen:
            return
        seen.add(cand.url)
        candidates.append(cand)

    for item in raw.get("meta_images", []) or []:
        abs_u = _abs(final_url, item.get("url"))
        if abs_u:
            _add(ImageCandidate(url=abs_u, source="og_image", extra={"key": item.get("key")}))

    for item in raw.get("icons", []) or []:
        abs_u = _abs(final_url, item.get("url"))
        if abs_u:
            _add(ImageCandidate(
                url=abs_u, source="favicon",
                extra={"rel": item.get("rel"), "sizes": item.get("sizes")},
            ))

    for item in raw.get("imgs", []) or []:
        abs_u = _abs(final_url, item.get("url"))
        if abs_u:
            _add(ImageCandidate(
                url=abs_u,
                source="img_tag",
                width=int(item.get("w") or 0),
                height=int(item.get("h") or 0),
                left=int(item.get("x") or 0),
                top=int(item.get("y") or 0),
                alt=item.get("alt") or "",
                context=item.get("context") or "",
                cls=item.get("cls") or "",
                extra={
                    "parent_text": item.get("parentText") or "",
                    "parent_cls": item.get("parentCls") or "",
                    "parent_id": item.get("parentId") or "",
                },
            ))

    for item in raw.get("bgs", []) or []:
        abs_u = _abs(final_url, item.get("url"))
        if abs_u:
            _add(ImageCandidate(
                url=abs_u, source="bg_image",
                width=int(item.get("w") or 0),
                height=int(item.get("h") or 0),
                left=int(item.get("x") or 0),
                top=int(item.get("y") or 0),
                cls=item.get("cls") or "",
                alt=item.get("text") or "",
                extra={"id": item.get("id") or ""},
            ))

    log.info(f"{url}: collected {len(candidates)} raw candidates")
    return candidates


def _wait_past_verification(page, timeout_ms: int = 9000) -> bool:
    blocked_markers = (
        "verify you are human",
        "checking your browser",
        "just a moment",
        "enable javascript and cookies",
        "attention required",
        "cloudflare",
        "ddos-guard",
        "please wait while we check",
    )
    elapsed = 0
    while elapsed <= timeout_ms:
        try:
            text = page.evaluate(
                "() => ((document.title || '') + ' ' + (document.body ? document.body.innerText : '')).toLowerCase().slice(0, 4000)"
            )
        except Exception:
            text = ""
        if text and not any(marker in text for marker in blocked_markers):
            page.wait_for_timeout(900)
            return True
        page.wait_for_timeout(1000)
        elapsed += 1000
    return False


# -----------------------------------------------------------------------------
# Heuristic pre-filters
# -----------------------------------------------------------------------------

# Third-party domains whose images are never the school's own media
# (cookie banners, consent managers, ad networks, analytics pixels, etc.)
_THIRD_PARTY_DOMAINS = (
    "onetrust.com", "cookielaw.org", "cookiepedia.co.uk", "trustarc.com",
    "cookiebot.com", "quantcast.com", "usercentrics.eu", "termly.io",
    "googletagmanager.com", "google-analytics.com", "doubleclick.net",
    "facebook.com/tr", "connect.facebook.net", "hotjar.com",
)

_LOGO_URL_POSITIVE = ("logo", "crest", "emblem", "brand", "seal", "shield", "wordmark", "badge")
_LOGO_URL_NEGATIVE = ("banner", "hero", "campus", "building", "aerial", "photo", "news",
                      "student", "event", "cover", "background", "bg-",
                      "cookie", "consent", "gdpr", "onetrust", "powered_by",
                      "partner", "sponsor")
_CAMPUS_URL_POSITIVE = ("campus", "building", "aerial", "hero", "banner", "landmark",
                        "architecture", "view", "exterior", "quad", "gate", "tower", "cover")
_CAMPUS_URL_NEGATIVE = ("logo", "icon", "favicon", "sprite", "avatar", "profile",
                        "emblem", "crest", "seal", "badge", "brand",
                        "social", "facebook", "twitter", "instagram", "linkedin", "weibo",
                        "wechat", "youtube", "tiktok", "bilibili")


def _is_third_party(u: str) -> bool:
    return any(dom in u for dom in _THIRD_PARTY_DOMAINS)


def select_logo_candidates(cands: list[ImageCandidate], limit: int = 8) -> list[ImageCandidate]:
    """Rank and return top-N candidates most likely to be a logo / crest / emblem."""
    scored: list[tuple[int, ImageCandidate]] = []
    for c in cands:
        u = (c.url or "").lower()
        # Hard-exclude third-party cookie/consent/tracker images
        if _is_third_party(u):
            continue
        score = 0
        alt = (c.alt or "").lower()
        cls = (c.cls or "").lower()
        parent_text = ((c.extra or {}).get("parent_text") or "").lower()
        parent_cls = ((c.extra or {}).get("parent_cls") or "").lower()
        parent_id = ((c.extra or {}).get("parent_id") or (c.extra or {}).get("id") or "").lower()
        all_labels = " ".join([alt, cls, parent_text, parent_cls, parent_id])

        if any(k in u for k in _LOGO_URL_POSITIVE):
            score += 5
        if any(k in all_labels for k in ("logo", "crest", "emblem", "seal", "校徽", "university", "college", "academy")):
            score += 4
        if any(k in all_labels for k in ("logo", "brand", "navbar-brand", "header-logo", "site-logo", "uni-logo")):
            score += 3

        if c.source == "favicon":
            # apple-touch-icon is often a decent stand-in for the crest
            score += 3
            rel = (c.extra.get("rel") or "").lower() if c.extra else ""
            if "apple-touch-icon" in rel:
                score += 2
        if c.source == "og_image":
            # Social-share images are often posters, photos, or banners. Keep
            # them only when other evidence says logo/crest/brand.
            score -= 2

        if c.context in ("header", "nav"):
            score += 3

        # Many university headers use a plain banner image for the institution
        # lockup (crest + name) with unhelpful filenames like banner_0.jpg and
        # alt text like "1". Prefer visible top-page images with logo-like
        # proportions even when their filename has no semantic hint.
        if c.source in ("img_tag", "bg_image") and 0 <= c.top <= 300:
            score += 3
            if c.left <= 1000:
                score += 2
            if 120 <= c.width <= 950 and 32 <= c.height <= 260:
                score += 2
            if c.height and 1.5 <= (c.width / c.height) <= 8.0:
                score += 2

        if u.endswith(".svg"):
            score += 2

        if any(k in u for k in _LOGO_URL_NEGATIVE):
            score -= 3

        # Logos are usually small-to-medium
        if 0 < c.width <= 400 and 0 < c.height <= 400:
            score += 1
        elif c.width > 1200 or c.height > 1200:
            score -= 2

        if score > 0:
            scored.append((score, c))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [c for _, c in scored[:limit]]


def select_campus_candidates(cands: list[ImageCandidate], limit: int = 8) -> list[ImageCandidate]:
    """Rank and return top-N candidates most likely to be campus scenery / landmark."""
    scored: list[tuple[int, ImageCandidate]] = []
    for c in cands:
        u = (c.url or "").lower()
        alt = (c.alt or "").lower()

        # Hard exclusions: favicons, vector-only, obviously non-photo, third-party images
        if c.source == "favicon":
            continue
        if _is_third_party(u):
            continue
        if any(k in u for k in _CAMPUS_URL_NEGATIVE):
            continue
        if u.endswith(".svg") or u.endswith(".gif"):
            continue

        score = 0
        if any(k in u for k in _CAMPUS_URL_POSITIVE):
            score += 4
        if any(k in alt for k in ("campus", "building", "view", "exterior", "quad",
                                  "校园", "校区", "楼")):
            score += 3

        if c.source == "og_image":
            score += 2
        if c.source == "bg_image":
            score += 1

        if c.context == "hero":
            score += 3
        elif c.context in ("main", "header"):
            score += 1

        # Bigger is better for scenery
        if c.width >= 1200 or c.height >= 800:
            score += 3
        elif c.width >= 800:
            score += 2
        elif 0 < c.width < 400 and 0 < c.height < 300:
            # Too small to be a nice cover photo
            continue

        scored.append((score, c))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [c for _, c in scored[:limit]]


def _abs(base: str, href: Optional[str]) -> Optional[str]:
    if not href:
        return None
    if href.startswith("data:"):
        return None
    return urljoin(base, href)
