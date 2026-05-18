"""Rendered homepage brand/logo screenshots.

Many university sites do not expose a clean logo image URL. The visible brand
lockup is often an inline SVG, CSS background, or a composed block containing a
crest plus institution name. This module captures that rendered brand element
from the top header instead of relying on raw image URLs.
"""

from dataclasses import dataclass
from pathlib import Path
import re

from utils.logger import get_logger

log = get_logger("rendered_logo")


@dataclass(frozen=True)
class RenderedLogoCandidate:
    label: str
    image_bytes: bytes
    width: int
    height: int


_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

_BRAND_JS = r"""
(schoolName) => {
  const tokens = String(schoolName || '')
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter(t => t.length >= 4 && !['school', 'college', 'university', 'arts'].includes(t));

  const selectors = [
    '#logo',
    '#logo a',
    '#logo img',
    '[id*="logo" i]',
    '[class*="logo" i]',
    '[class*="brand" i]',
    'a[aria-label*="university" i]',
    'a[aria-label*="college" i]',
    'a[aria-label*="academy" i]',
    'img[alt*="university" i]',
    'img[alt*="college" i]',
    'img[alt*="academy" i]',
    'header a[href="/"]',
    'header a[href="./"]',
    'header a[href*="home"]',
    'header [class*="logo" i]',
    'header [class*="brand" i]',
    'header [class*="site-title" i]',
    'nav a[href="/"]',
    'nav [class*="logo" i]',
    'nav [class*="brand" i]'
  ];

  const seen = new Set();
  const out = [];
  for (const sel of selectors) {
    for (const el of document.querySelectorAll(sel)) {
      if (seen.has(el)) continue;
      seen.add(el);
      const rect = el.getBoundingClientRect();
      const style = window.getComputedStyle(el);
      if (style.visibility === 'hidden' || style.display === 'none') continue;
      if (rect.width < 32 || rect.height < 18 || rect.width > 760 || rect.height > 240) continue;
      if (rect.top > 260) continue;

      const text = (el.innerText || el.getAttribute('aria-label') || el.getAttribute('title') || el.getAttribute('alt') || '').toLowerCase();
      const cls = String(el.className || '').toLowerCase();
      const id = String(el.id || '').toLowerCase();
      const hasMedia = Boolean(el.matches('img,svg,picture') || el.querySelector('img,svg,picture'));
      let score = 0;
      if (rect.left < window.innerWidth * 0.45) score += 4;
      if (rect.top < 180) score += 3;
      if (hasMedia) score += 4;
      if (cls.includes('logo') || cls.includes('brand') || id.includes('logo')) score += 6;
      if (text.includes('university') || text.includes('college') || text.includes('school')) score += 2;
      for (const token of tokens) {
        if (text.includes(token)) score += 3;
      }
      out.push({ score, rect: { x: rect.x, y: rect.y, width: rect.width, height: rect.height }, text, cls });
    }
  }

  out.sort((a, b) => b.score - a.score);
  return out.slice(0, 4);
}
"""


def capture_rendered_logo_candidates(
    url: str,
    school_name: str,
    *,
    limit: int = 3,
    timeout_ms: int = 16000,
) -> list[RenderedLogoCandidate]:
    if not url:
        return []

    from playwright.sync_api import sync_playwright

    out: list[RenderedLogoCandidate] = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                ctx = browser.new_context(
                    user_agent=_USER_AGENT,
                    viewport={"width": 1440, "height": 900},
                    device_scale_factor=2,
                )
                page = ctx.new_page()
                page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                if not _wait_past_verification(page):
                    log.warning("rendered logo skipped verification/interstitial page for %s", url)
                    return []
                for label, rect in _fixed_regions(page.viewport_size or {"width": 1440, "height": 900}):
                    try:
                        data = page.screenshot(type="png", clip=rect, omit_background=False)
                    except Exception:
                        continue
                    out.append(RenderedLogoCandidate(
                        label=label,
                        image_bytes=data,
                        width=int(rect["width"]),
                        height=int(rect["height"]),
                    ))
                candidates = page.evaluate(_BRAND_JS, school_name) or []
                for i, cand in enumerate(candidates[:limit]):
                    rect = _padded_rect(cand["rect"], page.viewport_size or {"width": 1440, "height": 900})
                    try:
                        data = page.screenshot(type="png", clip=rect, omit_background=False)
                    except Exception:
                        continue
                    out.append(RenderedLogoCandidate(
                        label=f"rendered-logo-{i}",
                        image_bytes=data,
                        width=int(rect["width"]),
                        height=int(rect["height"]),
                    ))
            finally:
                browser.close()
    except Exception as exc:
        log.warning("rendered logo capture failed for %s: %s", url, exc)
        return []

    return out


def _wait_past_verification(page, *, timeout_ms: int = 9000) -> bool:
    """Return False if the page still looks like an anti-bot/interstitial screen."""
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


def write_rendered_logo_candidates(
    url: str,
    school_name: str,
    out_dir: Path,
    slug: str,
) -> list[tuple[str, Path]]:
    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[tuple[str, Path]] = []
    safe_slug = re.sub(r"[^a-z0-9]+", "-", slug.lower()).strip("-") or "school"
    for cand in capture_rendered_logo_candidates(url, school_name):
        path = out_dir / f"{safe_slug}-{cand.label}.png"
        path.write_bytes(cand.image_bytes)
        written.append((cand.label, path.resolve()))
    return written


def _padded_rect(rect: dict, viewport: dict) -> dict:
    pad = 10
    x = max(0, float(rect["x"]) - pad)
    y = max(0, float(rect["y"]) - pad)
    width = min(float(rect["width"]) + pad * 2, float(viewport["width"]) - x)
    height = min(float(rect["height"]) + pad * 2, float(viewport["height"]) - y)
    return {"x": x, "y": y, "width": width, "height": height}


def _fixed_regions(viewport: dict) -> list[tuple[str, dict]]:
    vw = float(viewport["width"])
    vh = float(viewport["height"])
    return [
        ("region-top-left", {"x": 0, "y": 0, "width": min(520, vw), "height": min(220, vh)}),
        ("region-top-center", {"x": max(0, (vw - 640) / 2), "y": 0, "width": min(640, vw), "height": min(220, vh)}),
        ("region-top-right", {"x": max(0, vw - 560), "y": 0, "width": min(560, vw), "height": min(220, vh)}),
        ("region-left-rail-top", {"x": 0, "y": 0, "width": min(260, vw), "height": min(520, vh)}),
        ("region-header-wide", {"x": 0, "y": 0, "width": vw, "height": min(260, vh)}),
    ]
