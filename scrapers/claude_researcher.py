import json
from urllib.parse import urlparse

import anthropic
import requests

from config.settings import Settings
from utils.logger import get_logger
from utils.retry import retry

log = get_logger("claude_researcher")

TAVILY_URL = "https://api.tavily.com/search"

SYSTEM_PROMPT = """You are a research assistant that collects factual information about art and design universities.
You will be given Tavily web search results. Use only the supplied evidence to infer the most accurate answer and return ONLY a JSON object — no markdown, no explanation.
If a value is unknown from the evidence, use null. All text fields should be in English."""

USER_TEMPLATE = """Research this school and return a JSON object with these exact keys:

School: {name_en} ({name_zh})
Website: {website}

Web evidence:
{evidence}

Required JSON keys:
- name_en: string — official English name of the school, properly spaced, no abbreviation suffixes (e.g. "Royal College of Art", not "Royal College ofArt, RCA")
- city: string — city where the main campus is located
- founded_year: integer or null
- school_type: string — one of: "art_academy", "design_school", "university_art_dept", "film_school", "architecture_school", "performing_arts", "multi_disciplinary"
- description: string — ~150 word English description of the school
- feature_tags: list of strings — e.g. ["fine_arts", "graphic_design", "sculpture"]
- strength_disciplines: list of strings — top 3–5 academic strengths
- notable_alumni: list of strings — up to 5 famous alumni
- entry_score_requirements: string or null — e.g. "portfolio + IELTS 6.5" or "GPA 3.0"
- annual_intake: integer or null — approximate number of students admitted per year
- application_deadline: string or null — e.g. "January 15" or "Rolling admissions"
- international_students_page: string or null — URL of the international admissions page

Return ONLY the JSON object."""


@retry(max_attempts=3, base_delay=3.0)
def research_school(
    client: anthropic.Anthropic,
    settings: Settings,
    name_en: str,
    name_zh: str,
    website: str,
) -> dict:
    evidence = _build_evidence(settings, name_en, name_zh, website)
    prompt = USER_TEMPLATE.format(
        name_en=name_en,
        name_zh=name_zh or "",
        website=website or "unknown",
        evidence=evidence,
    )

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )

    text_blocks = [b.text for b in response.content if b.type == "text" and getattr(b, "text", None)]
    final_text = "\n".join(text_blocks).strip()

    if not final_text:
        log.warning(f"Empty response for {name_en} from Claude extraction")
        return {}

    return _parse_json(final_text, name_en)


def _build_evidence(settings: Settings, name_en: str, name_zh: str, website: str) -> str:
    domain = _domain_from_url(website)
    queries = [
        f'{name_en} art design school city founded notable alumni',
        f'{name_en} admissions application deadline international students entry requirements',
    ]
    if name_zh:
        queries.append(f'{name_en} {name_zh} art design school')
    if domain:
        queries.append(f'{name_en} site:{domain} admissions international students')

    blocks = []
    seen_urls = set()
    for query in queries:
        for result in _tavily_search(settings.tavily_api_key, query):
            url = result.get("url") or ""
            if url in seen_urls:
                continue
            seen_urls.add(url)
            title = (result.get("title") or "").strip()
            content = (result.get("content") or "").strip()
            if len(content) > 500:
                content = content[:500] + "..."
            blocks.append(
                f"Title: {title}\nURL: {url}\nSnippet: {content}"
            )
            if len(blocks) >= 10:
                break
        if len(blocks) >= 10:
            break

    if not blocks:
        return "No web evidence found."

    return "\n\n".join(blocks)


def _tavily_search(api_key: str, query: str) -> list[dict]:
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
            "include_raw_content": False,
            "include_answer": False,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("results") or []


def _domain_from_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url)
    return parsed.netloc.replace("www.", "")


def _parse_json(text: str, name_en: str) -> dict:
    """Extract and parse JSON from Claude's response text."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])

    start = text.find("{")
    end = text.rfind("}") + 1
    if start == -1 or end == 0:
        log.error(f"No JSON found in Claude response for {name_en}: {text[:200]}")
        return {}

    try:
        return json.loads(text[start:end])
    except json.JSONDecodeError as e:
        log.error(f"JSON parse error for {name_en}: {e}\nText: {text[start:start+300]}")
        return {}
