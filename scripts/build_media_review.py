#!/usr/bin/env python3
"""Build a lightweight HTML review page for school media candidates.

The report references remote images directly; it does not download image files.

Usage:
  python scripts/build_media_review.py --schools "Royal College of Art,Rhode Island School of Design"
"""

from __future__ import annotations

import argparse
import html
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import load_settings
from db.supabase_client import TABLE as SCHOOLS_TABLE, get_client
from scrapers.headless_image_scraper import collect_candidates, select_campus_candidates, select_logo_candidates
from scrapers.image_search import search_campus_image_candidates
from scrapers.rendered_logo import write_rendered_logo_candidates


OUT_PATH = Path("data/media_review/school_media_review.html")
ASSET_DIR = Path("data/media_review/assets")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schools", required=True, help="Comma-separated school name_en values")
    parser.add_argument("--out", type=Path, default=OUT_PATH)
    args = parser.parse_args()

    names = [n.strip() for n in args.schools.split(",") if n.strip()]
    settings = load_settings()
    client = get_client(settings)
    rows = (
        client.table(SCHOOLS_TABLE)
        .select("id,name_en,name_zh,official_website,logo_url,campus_image_urls")
        .in_("name_en", names)
        .execute()
        .data
        or []
    )

    sections = []
    for row in rows:
        website = row.get("official_website") or ""
        raw = collect_candidates(website) if website else []
        logo = select_logo_candidates(raw, limit=10)
        campus = select_campus_candidates(raw, limit=10)
        campus.extend(search_campus_image_candidates(settings.tavily_api_key, row["name_en"], official_website=website))
        rendered_logo = write_rendered_logo_candidates(
            website,
            row["name_en"],
            ASSET_DIR,
            row.get("id") or row["name_en"],
        ) if website else []

        sections.append(_school_section(row, logo, campus[:12], rendered_logo))

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(_page("\n".join(sections)), encoding="utf-8")
    print(args.out.resolve())


def _school_section(row: dict, logo_cands, campus_cands, rendered_logo) -> str:
    title = html.escape(f"{row.get('name_en')} / {row.get('name_zh') or ''}")
    current_logo = row.get("logo_url")
    current_campus = (row.get("campus_image_urls") or [None])[0]
    return f"""
    <section>
      <h2>{title}</h2>
      <p><strong>school_id:</strong> <code>{html.escape(row.get('id') or '')}</code></p>
      <h3>Current</h3>
      <div class="grid">
        {_card("current logo", current_logo)}
        {_card("current campus", current_campus)}
      </div>
      <h3>Rendered Brand Candidates</h3>
      <div class="grid">{''.join(_local_card(label, path) for label, path in rendered_logo)}</div>
      <h3>Logo Candidates</h3>
      <div class="grid">{''.join(_card(f'logo {i}', c.url) for i, c in enumerate(logo_cands))}</div>
      <h3>Campus Candidates</h3>
      <div class="grid">{''.join(_card(f'campus {i}', c.url) for i, c in enumerate(campus_cands))}</div>
    </section>
    """


def _card(label: str, url: str | None) -> str:
    if not url:
        return f"<article><div class='missing'>missing</div><p>{html.escape(label)}</p></article>"
    safe_url = html.escape(url, quote=True)
    return f"""
    <article>
      <img src="{safe_url}" loading="lazy" referrerpolicy="no-referrer" />
      <p>{html.escape(label)}</p>
      <code>{html.escape(url)}</code>
    </article>
    """


def _local_card(label: str, path: Path) -> str:
    safe_url = html.escape(path.as_uri(), quote=True)
    return f"""
    <article>
      <img src="{safe_url}" loading="lazy" />
      <p>{html.escape(label)}</p>
      <code>{html.escape(str(path))}</code>
    </article>
    """


def _page(body: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>School Media Review</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 32px; color: #171717; }}
    section {{ border-top: 1px solid #ddd; padding: 24px 0; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 16px; }}
    article {{ border: 1px solid #ddd; border-radius: 8px; padding: 10px; background: #fff; }}
    img {{ width: 100%; aspect-ratio: 4 / 3; object-fit: contain; background: #f5f5f5; }}
    p {{ margin: 8px 0; font-weight: 600; }}
    code {{ display: block; font-size: 11px; word-break: break-all; color: #555; }}
    .missing {{ display: grid; place-items: center; width: 100%; aspect-ratio: 4 / 3; background: #f5f5f5; color: #888; }}
  </style>
</head>
<body>
  <h1>School Media Review</h1>
  {body}
</body>
</html>
"""


if __name__ == "__main__":
    main()
