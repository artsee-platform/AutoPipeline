import json
import time
import anthropic
from utils.logger import get_logger
from utils.retry import retry

log = get_logger("claude_researcher")

SYSTEM_PROMPT = """You are a research assistant that collects factual information about art and design universities.
Given a school name and website, use web search to find accurate data and return ONLY a JSON object — no markdown, no explanation.
If a value is unknown, use null. All text fields should be in English."""

USER_TEMPLATE = """Research this school and return a JSON object with these exact keys:

School: {name_en} ({name_zh})
Website: {website}

Required JSON keys:
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
    name_en: str,
    name_zh: str,
    website: str,
) -> dict:
    """Use Claude with web_search tool to research a school and return structured data."""

    prompt = USER_TEMPLATE.format(
        name_en=name_en,
        name_zh=name_zh or "",
        website=website or "unknown",
    )

    messages = [{"role": "user", "content": prompt}]

    # Agentic loop: keep going until Claude stops using tools
    max_iterations = 8
    for iteration in range(max_iterations):
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=SYSTEM_PROMPT,
            tools=[{"type": "web_search_20250305", "name": "web_search"}],
            messages=messages,
        )

        # Collect tool uses and text blocks
        tool_uses = []
        text_blocks = []
        for block in response.content:
            if block.type == "tool_use":
                tool_uses.append(block)
            elif block.type == "text":
                text_blocks.append(block.text)

        # If no tool calls, we have the final answer
        if response.stop_reason == "end_turn" or not tool_uses:
            final_text = "\n".join(text_blocks).strip()
            return _parse_json(final_text, name_en)

        # Append Claude's response to messages, then add tool results
        messages.append({"role": "assistant", "content": response.content})

        tool_results = []
        for tu in tool_uses:
            # web_search results are already embedded in the tool_use block by the API
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tu.id,
                "content": getattr(tu, "content", "") or "",
            })

        messages.append({"role": "user", "content": tool_results})
        time.sleep(0.5)

    log.warning(f"Max iterations reached for {name_en}, returning empty dict")
    return {}


def _parse_json(text: str, name_en: str) -> dict:
    """Extract and parse JSON from Claude's response text."""
    # Strip markdown code fences if present
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])

    # Find JSON object boundaries
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
