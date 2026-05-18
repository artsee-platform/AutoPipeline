"""Small Tavily image-search helper for campus cover fallbacks."""

from urllib.parse import urlparse

import requests

from scrapers.headless_image_scraper import ImageCandidate
from utils.logger import get_logger

log = get_logger("image_search")

TAVILY_URL = "https://api.tavily.com/search"


def search_campus_image_candidates(
    api_key: str,
    school_name: str,
    *,
    official_website: str = "",
    limit: int = 8,
) -> list[ImageCandidate]:
    """Return image candidates from search when the official homepage has none.

    Tavily returns image URLs directly, so we still run the same validation and
    Claude vision selection before accepting any result.
    """
    if not api_key or not school_name:
        return []

    query = f"{school_name} campus building exterior"
    domain = _domain_from_url(official_website)

    try:
        resp = requests.post(
            TAVILY_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "query": query,
                "search_depth": "basic",
                "topic": "general",
                "max_results": 5,
                "include_images": True,
                "include_raw_content": False,
                "include_answer": False,
            },
            timeout=20,
        )
        resp.raise_for_status()
    except Exception as exc:
        log.warning(f"campus image search failed for {school_name}: {exc}")
        return []

    data = resp.json()
    urls = _extract_image_urls(data)
    scored: list[tuple[int, str]] = []
    for url in urls:
        u = url.lower()
        score = 0
        if domain and domain in u:
            score += 4
        if any(k in u for k in ("campus", "building", "exterior", "architecture", "quad")):
            score += 2
        if any(k in u for k in ("logo", "icon", "crest", "seal", "badge", "avatar")):
            score -= 4
        scored.append((score, url))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [
        ImageCandidate(url=url, source="image_search")
        for _, url in scored[:limit]
    ]


def _extract_image_urls(data: dict) -> list[str]:
    seen: set[str] = set()
    urls: list[str] = []

    def add(value) -> None:
        if isinstance(value, str) and value.startswith(("http://", "https://")) and value not in seen:
            seen.add(value)
            urls.append(value)

    for item in data.get("images") or []:
        if isinstance(item, str):
            add(item)
        elif isinstance(item, dict):
            add(item.get("url"))

    for result in data.get("results") or []:
        for item in result.get("images") or []:
            if isinstance(item, str):
                add(item)
            elif isinstance(item, dict):
                add(item.get("url"))

    return urls


def _domain_from_url(url: str) -> str:
    if not url:
        return ""
    return urlparse(url).netloc.replace("www.", "")
