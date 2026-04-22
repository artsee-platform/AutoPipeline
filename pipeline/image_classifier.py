"""Multimodal image classifier powered by Claude vision.

Given a pre-filtered list of candidate images, ask Claude which one best
matches a target concept (school logo/crest or campus scenery). We fetch
each candidate ourselves, base64-encode, and hand the bytes to Claude
directly — this side-steps any quirks around Anthropic's URL image source
(different networks, auth-walled static hosts, etc.) and fails in a
predictable way we can fall back from.

Only small per-image downloads happen here; nothing is persisted to disk.
"""

import base64
import json
import mimetypes
from typing import Optional

import anthropic
import requests

from utils.logger import get_logger

log = get_logger("image_classifier")

VISION_MODEL = "claude-sonnet-4-6"
MAX_CANDIDATES_PER_CALL = 8
MAX_IMAGE_BYTES = 4 * 1024 * 1024          # 4 MB — Claude's per-image limit is 5 MB
DOWNLOAD_TIMEOUT_S = 10

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

# Claude vision supports: image/jpeg, image/png, image/gif, image/webp
_ALLOWED_MEDIA = {"image/jpeg", "image/png", "image/gif", "image/webp"}


LOGO_PROMPT = """You are looking at {n} candidate images (indexed 0..{last}) extracted from
the official website of the school: "{school}".

Pick the single image that best represents the school's official LOGO / CREST /
SHIELD / EMBLEM / WORDMARK.

Prefer:
- a clean badge, shield, emblem, or wordmark on a simple background
- a symbolic mark that clearly represents the institution

Reject:
- photos of buildings, people, events, or campus scenery
- generic social-share banners
- unrelated UI icons (social media, payment, search, etc.)
- unrelated third-party logos

If NONE of the candidates is clearly the school's own logo/crest, return index = -1.

Reply with ONLY a single JSON object, no prose, no markdown:
{{"index": <int>, "confidence": <0.0-1.0>, "reason": "<short>"}}"""


CAMPUS_PROMPT = """You are looking at {n} candidate images (indexed 0..{last}) extracted from
the official website of the school: "{school}".

Pick the single image that best serves as a COVER PHOTO representing the school's
campus — ideally campus scenery, signature architecture, an aerial view, or an
iconic landmark of the institution.

Prefer:
- wide photographic shots of campus buildings, quads, or grounds
- recognisable architectural landmarks of the school
- clean, cinematic, high-quality photography

Reject:
- logos, crests, wordmarks, or any vector/symbolic artwork
- close-up portraits, single-person headshots, or small group shots
- generic indoor classroom / lab stock photos unrelated to this campus
- poster/event banners dominated by text overlay
- unrelated stock imagery not depicting this school

If NONE of the candidates clearly shows campus / landmark scenery, return index = -1.

Reply with ONLY a single JSON object, no prose, no markdown:
{{"index": <int>, "confidence": <0.0-1.0>, "reason": "<short>"}}"""


def _fetch_as_base64(url: str) -> Optional[tuple[str, str]]:
    """Download an image URL and return (media_type, base64_data) or None on any failure."""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=DOWNLOAD_TIMEOUT_S, stream=True)
        resp.raise_for_status()
    except Exception as e:
        log.debug(f"download failed {url}: {e}")
        return None

    # Peek at content-length when available so we skip huge images early
    cl = resp.headers.get("Content-Length")
    if cl and cl.isdigit() and int(cl) > MAX_IMAGE_BYTES:
        log.debug(f"skip oversized image ({cl} bytes): {url}")
        return None

    data = b""
    for chunk in resp.iter_content(chunk_size=32768):
        if not chunk:
            continue
        data += chunk
        if len(data) > MAX_IMAGE_BYTES:
            log.debug(f"skip oversized image (stream > {MAX_IMAGE_BYTES}): {url}")
            return None

    if not data:
        return None

    media = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
    if media not in _ALLOWED_MEDIA:
        # Infer from URL extension as a fallback
        guessed, _ = mimetypes.guess_type(url)
        if guessed in _ALLOWED_MEDIA:
            media = guessed
        else:
            log.debug(f"skip unsupported media {media or 'unknown'}: {url}")
            return None

    return media, base64.standard_b64encode(data).decode("ascii")


def _build_content_blocks(candidates, prompt: str, school_name: str) -> tuple[list, list]:
    """Download each candidate, skip failures, return (content_blocks, kept_candidates).

    Appends the fully-rendered prompt (with n / last / school substituted) as the
    final text block.
    """
    content: list = []
    kept = []
    for c in candidates:
        fetched = _fetch_as_base64(c.url)
        if not fetched:
            continue
        media, b64 = fetched
        idx = len(kept)
        content.append({"type": "text", "text": f"Candidate {idx}:"})
        content.append({
            "type": "image",
            "source": {"type": "base64", "media_type": media, "data": b64},
        })
        kept.append(c)

    if not kept:
        return [], []

    n = len(kept)
    content.append({
        "type": "text",
        "text": prompt.format(n=n, last=n - 1, school=school_name),
    })
    return content, kept


def _call_claude(claude: anthropic.Anthropic, content: list) -> Optional[dict]:
    try:
        resp = claude.messages.create(
            model=VISION_MODEL,
            max_tokens=400,
            messages=[{"role": "user", "content": content}],
        )
    except Exception as e:
        log.warning(f"Claude vision API error: {e}")
        return None

    text = "".join(b.text for b in resp.content if b.type == "text" and getattr(b, "text", None)).strip()
    if not text:
        return None

    start = text.find("{")
    end = text.rfind("}") + 1
    if start < 0 or end <= start:
        log.warning(f"no JSON in Claude reply: {text[:200]}")
        return None
    try:
        return json.loads(text[start:end])
    except json.JSONDecodeError as e:
        log.warning(f"JSON parse error: {e}; raw={text[start:start+200]}")
        return None


def _pick(
    claude: anthropic.Anthropic,
    school_name: str,
    candidates: list,
    prompt_template: str,
    label: str,
) -> tuple[Optional[str], str]:
    """Ask Claude to pick the best candidate.

    Returns (url_or_none, status) where status is one of:
      - "match"     : Claude picked a candidate (url is set)
      - "none"      : Claude explicitly said none of the candidates match (trust it)
      - "error"     : something went wrong (network / parse / empty input)
    Callers should only attempt a fallback when status == "error".
    """
    if not candidates:
        return None, "error"
    cand = candidates[:MAX_CANDIDATES_PER_CALL]

    content, kept = _build_content_blocks(cand, prompt_template, school_name)
    if not kept:
        log.info(f"[{label}] {school_name}: all candidate downloads failed")
        return None, "error"

    data = _call_claude(claude, content)
    if not data:
        return None, "error"

    try:
        idx = int(data.get("index", -1))
    except (TypeError, ValueError):
        idx = -1

    if idx < 0 or idx >= len(kept):
        log.info(f"[{label}] {school_name}: Claude picked none ({data})")
        return None, "none"

    best = kept[idx].url
    log.info(
        f"[{label}] {school_name}: idx={idx} conf={data.get('confidence')} "
        f"reason={data.get('reason')!r} url={best}"
    )
    return best, "match"


def pick_best_logo(
    claude: anthropic.Anthropic,
    school_name: str,
    candidates: list,
) -> tuple[Optional[str], str]:
    """Pick best logo. Returns (url, status); status in {'match','none','error'}."""
    return _pick(claude, school_name, candidates, LOGO_PROMPT, "logo")


def pick_best_campus(
    claude: anthropic.Anthropic,
    school_name: str,
    candidates: list,
) -> tuple[Optional[str], str]:
    """Pick best campus photo. Returns (url, status); status in {'match','none','error'}."""
    return _pick(claude, school_name, candidates, CAMPUS_PROMPT, "campus")
