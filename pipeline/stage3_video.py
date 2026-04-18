"""Stage 3 — Video metadata via yt-dlp (YouTube + Bilibili), then Claude augmentation."""
import json
import time
import anthropic
import yt_dlp
from config.settings import Settings
from db.supabase_client import get_client, fetch_by_status, update_status, upsert_school
from utils.logger import get_logger

log = get_logger("stage3")

YDL_OPTS_BASE = {
    "quiet": True,
    "no_warnings": True,
    "extract_flat": True,
    "skip_download": True,
}


def _ydl_search(query: str, n: int = 3) -> list[dict]:
    """Return metadata dicts for top n results from a yt-dlp search query."""
    opts = {
        **YDL_OPTS_BASE,
        "default_search": "auto",
    }
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(query, download=False)
            entries = info.get("entries", []) if info else []
            return [
                {
                    "title": e.get("title", ""),
                    "description": e.get("description", "") or "",
                    "url": e.get("url") or e.get("webpage_url", ""),
                    "uploader": e.get("uploader", ""),
                    "view_count": e.get("view_count"),
                }
                for e in entries[:n]
                if e
            ]
    except Exception as e:
        log.warning(f"yt-dlp search failed for '{query}': {e}")
        return []


def _augment_with_claude(
    client: anthropic.Anthropic,
    school_name: str,
    videos: list[dict],
    existing_description: str,
    existing_tags: list,
) -> dict:
    """Ask Claude to extract additional feature_tags from video metadata."""
    if not videos:
        return {}

    video_summaries = "\n".join(
        f"- [{v['title']}] {v['description'][:300]}" for v in videos
    )
    prompt = f"""Given these video titles and descriptions from YouTube/Bilibili for the school "{school_name}":

{video_summaries}

Current description: {existing_description or 'None'}
Current feature tags: {existing_tags or []}

Extract any ADDITIONAL feature tags not already in the current list. Tags should be lowercase with underscores.
Also, if the videos reveal information that should update the description, provide a revised description.

Return ONLY a JSON object with keys:
- additional_tags: list of strings (new tags only, max 5)
- description_update: string or null (revised description only if meaningfully improved)"""

    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=512,
        messages=[{"role": "user", "content": prompt}],
    )
    text = response.content[0].text.strip()
    # Parse JSON
    start, end = text.find("{"), text.rfind("}") + 1
    if start == -1:
        return {}
    try:
        return json.loads(text[start:end])
    except Exception:
        return {}


def run(settings: Settings, batch_size: int) -> None:
    supabase = get_client(settings)
    claude = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    schools = fetch_by_status(supabase, "qs_done", batch_size)
    log.info(f"Fetching video metadata for {len(schools)} schools")

    for school in schools:
        name_en = school["name_en"]
        name_zh = school.get("name_zh") or ""
        log.info(f"→ {name_en}")

        try:
            # YouTube search
            yt_videos = _ydl_search(f"ytsearch3:{name_en} official university campus")
            # Bilibili search (requires bilibili extractor, may fail outside CN)
            bili_videos = _ydl_search(f"bilisearch3:{name_zh}") if name_zh else []

            all_videos = yt_videos + bili_videos

            # Claude augmentation (cheap Haiku model)
            augment = _augment_with_claude(
                claude,
                school_name=name_en,
                videos=all_videos,
                existing_description=school.get("description") or "",
                existing_tags=school.get("feature_tags") or [],
            )

            # Merge additional tags
            existing_tags = school.get("feature_tags") or []
            new_tags = augment.get("additional_tags") or []
            merged_tags = list(dict.fromkeys(existing_tags + new_tags))  # deduplicate

            description = augment.get("description_update") or school.get("description")

            updates = {
                "feature_tags": merged_tags,
                "description": description,
                "status": "done",
            }
            upsert_school(supabase, {**school, **updates})
            log.info(f"  ✓ {name_en}: {len(all_videos)} videos, +{len(new_tags)} tags")

        except Exception as exc:
            log.error(f"  ✗ {name_en}: {exc}")
            update_status(supabase, name_en, "error")

        time.sleep(1.0)
