"""
QS World University global (overall) ranking for a school.

Resolution order:
1. Match against bundled QS overall CSV (same source as stage 2).
2. If no numeric rank: Tavily web snippets + Claude extract (no HTML crawling).

Database: `schools.qs_overall_rank` is an **integer** (or NULL). Use **NULL** when QS
does not list the institution (no numeric rank after both steps). In UI or API
responses, show NULL as `NOT_RANKED_LABEL` (\"未上榜\") via `display_qs_overall_rank`.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import anthropic
import pandas as pd
import requests

from config.settings import Settings
from pipeline.qs_matcher import QSIndex
from utils.logger import get_logger

log = get_logger("qs_global_rank")

_DATA_DIR = Path(__file__).parent.parent / "data"
OVERALL_CSV = _DATA_DIR / "qs_data_metrics.csv"
TAVILY_URL = "https://api.tavily.com/search"

NOT_RANKED_LABEL = "未上榜"

_LLM_SYSTEM = """You are a data extraction assistant. Extract QS World University Rankings 2026 data from web search snippets.
Return ONLY a valid JSON object — no markdown, no explanation.
Use null for any rank not explicitly stated in the evidence.
Do NOT infer or guess ranks. Only extract numbers you can see in the evidence.
For range ranks like "51-100", return the lower bound integer (51)."""

_LLM_USER = """Find QS 2026 rankings for this school from the web evidence below.

School: {name_en}
Country: {country}

Web evidence:
{evidence}

Return ONLY this JSON (all values integer or null):
{{
  "qs_overall_rank": null,
  "qs_art_humanities_rank": null,
  "qs_architecture_built_environment_rank": null,
  "qs_art_design_rank": null,
  "qs_history_of_art_rank": null
}}

Rules:
- Only fill a field if the evidence explicitly states a QS 2026 rank number for that category.
- If the school is a department of a larger university, you may use the parent university's QS rank for qs_overall_rank only — but only if the evidence clearly connects them.
- Return null for any field without clear evidence."""


def load_overall_index() -> QSIndex | None:
    """QSIndex for overall QS world rankings from local CSV, or None if file missing."""
    if not os.path.exists(OVERALL_CSV):
        log.warning(f"Overall CSV not found: {OVERALL_CSV}")
        return None
    df = pd.read_csv(OVERALL_CSV)
    log.info(f"Indexed {len(df)} rows from {OVERALL_CSV}")
    return QSIndex(
        df,
        inst_col="Institution Name",
        rank_col="2026 Rank",
        country_col="Country/Territory",
    )


def display_qs_overall_rank(rank: int | None) -> str:
    """Human-readable rank for logs, APIs, or UI (not for integer DB column)."""
    if rank is None:
        return NOT_RANKED_LABEL
    return str(rank)


def _tavily_search(api_key: str, query: str, max_results: int = 5) -> list[dict]:
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
                "max_results": max_results,
                "include_raw_content": False,
                "include_answer": False,
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("results") or []
    except Exception as e:
        log.warning(f"  [LLM] Tavily search failed: {e}")
        return []


def _build_qs_evidence(api_key: str, name_en: str) -> str:
    queries = [
        f'"{name_en}" QS World University Rankings 2026',
        f'"{name_en}" QS subject rankings 2026 art design architecture humanities',
    ]
    blocks: list[str] = []
    seen: set[str] = set()
    for query in queries:
        for r in _tavily_search(api_key, query, max_results=4):
            url = r.get("url") or ""
            if url in seen:
                continue
            seen.add(url)
            title = (r.get("title") or "").strip()
            content = (r.get("content") or "").strip()[:600]
            blocks.append(f"[{title}]\n{url}\n{content}")
            if len(blocks) >= 8:
                break
        if len(blocks) >= 8:
            break
    return "\n\n---\n\n".join(blocks) if blocks else ""


def _parse_llm_ranks(text: str, name_en: str) -> dict[str, int | None]:
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    start, end = text.find("{"), text.rfind("}") + 1
    if start == -1 or end == 0:
        log.warning(f"  [LLM] no JSON in response for {name_en!r}")
        return {}
    try:
        raw = json.loads(text[start:end])
    except json.JSONDecodeError as e:
        log.warning(f"  [LLM] JSON parse error for {name_en!r}: {e}")
        return {}

    result: dict[str, int | None] = {}
    for field in [
        "qs_overall_rank",
        "qs_art_humanities_rank",
        "qs_architecture_built_environment_rank",
        "qs_art_design_rank",
        "qs_history_of_art_rank",
    ]:
        val = raw.get(field)
        if val is None:
            result[field] = None
        else:
            try:
                result[field] = int(val)
            except (TypeError, ValueError):
                result[field] = None
    return result


def llm_lookup_qs_ranks(
    name_en: str,
    country: str,
    settings: Settings,
    claude: anthropic.Anthropic,
) -> dict[str, int | None]:
    """Tavily + Claude when local QS files do not yield an overall (or subject) rank."""
    evidence = _build_qs_evidence(settings.tavily_api_key, name_en)
    if not evidence:
        log.info(f"  [LLM] no web evidence found for {name_en!r}")
        return {}

    prompt = _LLM_USER.format(
        name_en=name_en, country=country or "unknown", evidence=evidence
    )
    try:
        resp = claude.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=256,
            system=_LLM_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as e:
        log.warning(f"  [LLM] Claude call failed for {name_en!r}: {e}")
        return {}

    text = "".join(b.text for b in resp.content if b.type == "text")
    ranks = _parse_llm_ranks(text, name_en)

    found = {k: v for k, v in ranks.items() if v is not None}
    if found:
        log.info(f"  [LLM] found for {name_en!r}: {found}")
    else:
        log.info(f"  [LLM] nothing found for {name_en!r}")
    return ranks


def resolve_qs_overall_rank_with_llm(
    name_en: str,
    country: str,
    overall_index: QSIndex | None,
    settings: Settings,
    claude: anthropic.Anthropic,
) -> tuple[int | None, dict[str, int | None], bool]:
    """
    Overall QS world rank: local matcher first, then LLM if still unknown.

    Returns (qs_overall_rank, llm_ranks_dict, used_llm).
    """
    qs_overall: int | None = None
    if overall_index is not None:
        qs_overall = overall_index.match(name_en, country).rank

    llm_ranks: dict[str, int | None] = {}
    used_llm = False
    if qs_overall is None:
        llm_ranks = llm_lookup_qs_ranks(name_en, country, settings, claude)
        used_llm = True
        if llm_ranks.get("qs_overall_rank") is not None:
            qs_overall = llm_ranks["qs_overall_rank"]

    return qs_overall, llm_ranks, used_llm


def fetch_qs_overall_rank(
    name_en: str, country: str, settings: Settings
) -> int | None:
    """
    End-to-end helper: return numeric `schools.qs_overall_rank` for one school, or None.

    Loads the overall index, runs matcher + optional LLM. For batch processing prefer
    stage 2 or call resolve_qs_overall_rank_with_llm with a shared Anthropic client.
    """
    claude = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    idx = load_overall_index()
    n, _, _ = resolve_qs_overall_rank_with_llm(
        name_en, country, idx, settings, claude
    )
    return n
