"""Shared Tavily + official-page evidence gathering for program-related pipeline stages."""

from __future__ import annotations

import time
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from config.settings import Settings
from utils.logger import get_logger
from utils.retry import retry

log = get_logger("evidence")

TAVILY_URL = "https://api.tavily.com/search"

TAVILY_SNIPPET_MAX = 2500
MAX_TAVILY_BLOCKS = 18
MAX_DEEP_FETCH_PAGES = 5
PAGE_EXTRACT_MAX = 10000
EVIDENCE_TOTAL_SOFT_MAX = 120_000

FETCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def domain_from_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url)
    return parsed.netloc.replace("www.", "").lower()


def netloc_key(url: str) -> str:
    if not url:
        return ""
    try:
        return urlparse(url).netloc.replace("www.", "").lower()
    except Exception:
        return ""


def url_on_domain(url: str, school_domain: str) -> bool:
    if not school_domain or not url:
        return False
    nl = netloc_key(url)
    return nl == school_domain or nl.endswith("." + school_domain)


@retry(max_attempts=4, base_delay=3.0, max_delay=45.0)
def tavily_search(api_key: str, query: str, *, include_raw_content: bool = True) -> list[dict]:
    resp = requests.post(
        TAVILY_URL,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "query": query,
            "search_depth": "advanced",
            "topic": "general",
            "max_results": 5,
            "include_raw_content": include_raw_content,
            "include_answer": False,
        },
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("results") or []


def extract_visible_text(html: str, max_chars: int) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    out = "\n".join(lines)
    if len(out) > max_chars:
        out = out[:max_chars] + "\n...[truncated]"
    return out


def fetch_official_page_text(url: str, max_chars: int) -> str | None:
    if not url or not url.startswith(("http://", "https://")):
        return None
    try:
        resp = requests.get(url, headers=FETCH_HEADERS, timeout=25, allow_redirects=True)
        resp.raise_for_status()
    except Exception as exc:
        log.warning(f"  fetch failed {url!r}: {exc}")
        return None
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "pdf" in ctype or "msword" in ctype or "wordprocessingml" in ctype:
        log.warning(f"  skip non-html {url!r} ({ctype})")
        return None
    if "html" not in ctype and "text/plain" not in ctype and "xml" not in ctype:
        if "<html" not in resp.text[:2000].lower() and "<!doctype html" not in resp.text[:200].lower():
            log.warning(f"  skip likely non-html {url!r} ({ctype})")
            return None
    try:
        return extract_visible_text(resp.text, max_chars)
    except Exception as exc:
        log.warning(f"  parse failed {url!r}: {exc}")
        return None


def trim_evidence_total(text: str, soft_max: int) -> str:
    if len(text) <= soft_max:
        return text
    return text[:soft_max] + "\n\n...[evidence truncated for size]"


def _gather_blocks_from_queries(
    settings: Settings,
    queries: list[str],
    website: str,
    domain: str,
) -> list[str]:
    blocks: list[str] = []
    seen_urls: set[str] = set()
    for query in queries:
        for result in tavily_search(settings.tavily_api_key, query):
            url = (result.get("url") or "").strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            title = (result.get("title") or "").strip()
            raw = (result.get("raw_content") or "").strip()
            content = (result.get("content") or "").strip()
            body = raw if raw else content
            if len(body) > TAVILY_SNIPPET_MAX:
                body = body[:TAVILY_SNIPPET_MAX] + "..."
            label = "Excerpt" if raw else "Snippet"
            blocks.append(f"Title: {title}\nURL: {url}\n{label}: {body}")
            if len(blocks) >= MAX_TAVILY_BLOCKS:
                break
        if len(blocks) >= MAX_TAVILY_BLOCKS:
            break

    deep_urls: list[str] = []
    seen_deep: set[str] = set()
    if website.startswith(("http://", "https://")):
        deep_urls.append(website)
        seen_deep.add(website)
    if domain:
        for u in sorted(seen_urls):
            if len(deep_urls) >= MAX_DEEP_FETCH_PAGES:
                break
            if u in seen_deep or not url_on_domain(u, domain):
                continue
            deep_urls.append(u)
            seen_deep.add(u)
    deep_urls = deep_urls[:MAX_DEEP_FETCH_PAGES]

    for i, page_url in enumerate(deep_urls):
        if i > 0:
            time.sleep(0.35)
        extracted = fetch_official_page_text(page_url, PAGE_EXTRACT_MAX)
        if not extracted:
            continue
        blocks.append(
            f"Source: official-site page extract\nURL: {page_url}\nText:\n{extracted}"
        )

    return blocks


def build_evidence_for_school_programs(settings: Settings, school: dict) -> str:
    """Evidence for listing degree programs at an institution (Stage 4)."""
    name_en = school.get("name_en") or ""
    name_zh = school.get("name_zh") or ""
    website = (school.get("official_website") or "").strip()
    country = school.get("raw_country") or school.get("country") or ""
    domain = domain_from_url(website)

    queries = [
        f"{name_en} undergraduate postgraduate degree programs fine art design {country}",
        f"{name_en} BA MA MFA BFA course portfolio admission",
        f"{name_en} official programs courses catalogue",
    ]
    if name_zh:
        queries.append(f"{name_en} {name_zh} 专业 课程 学位")
    if domain:
        queries.append(f"{name_en} site:{domain} programs courses admissions")
        queries.append(f"site:{domain} admissions portfolio requirements degree duration")

    blocks = _gather_blocks_from_queries(settings, queries, website, domain)
    if not blocks:
        return "No web evidence found."
    return trim_evidence_total("\n\n".join(blocks), EVIDENCE_TOTAL_SOFT_MAX)


def build_evidence_for_program_detail(
    settings: Settings,
    school: dict,
    program: dict,
) -> str:
    """Evidence for fees, admissions, evaluation, and art categories for one program row."""
    name_en = school.get("name_en") or ""
    name_zh = school.get("name_zh") or ""
    website = (school.get("official_website") or "").strip()
    country = school.get("raw_country") or school.get("country") or ""
    domain = domain_from_url(website)
    pname = (program.get("program_name") or "").strip()
    deg = (
        program.get("normalized_degree_type")
        or program.get("raw_degree_type")
        or ""
    ).strip()

    queries = [
        f'"{pname}" {name_en} tuition fee international overseas home domestic {country}',
        f'"{pname}" {name_en} {deg} application deadline IELTS TOEFL portfolio interview',
        f'"{pname}" {name_en} acceptance rate admission statistics entry requirements',
        f"{name_en} {pname} fees funding scholarships currency GBP EUR USD",
    ]
    if name_zh:
        queries.append(f"{name_zh} {pname} 学费 申请 雅思 作品集")
    if domain:
        queries.append(f'site:{domain} "{pname}" fees tuition admissions')

    blocks = _gather_blocks_from_queries(settings, queries, website, domain)
    if not blocks:
        return "No web evidence found."
    return trim_evidence_total("\n\n".join(blocks), EVIDENCE_TOTAL_SOFT_MAX)
