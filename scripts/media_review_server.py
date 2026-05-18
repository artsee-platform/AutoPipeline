#!/usr/bin/env python3
"""Local clickable review UI for school media.

Usage:
  python scripts/media_review_server.py --batch 10
  python scripts/media_review_server.py --schools "Royal College of Art,University of the Arts London"

Open http://127.0.0.1:8787 in the browser, select one logo and up to five
campus images for each school, then submit. Approved images are uploaded to
Supabase Storage and the DB is updated.
"""

from __future__ import annotations

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import html
import json
from pathlib import Path
import re
import sys
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import load_settings
from db.media_status import upsert_media_status
from db.supabase_client import TABLE as SCHOOLS_TABLE, get_client
from pipeline.media_storage import store_school_media, store_school_media_bytes
from pipeline.media_validator import MEDIA_OK
from scrapers.headless_image_scraper import collect_candidates, select_campus_candidates, select_logo_candidates
from scrapers.image_search import search_campus_image_candidates
from scrapers.rendered_logo import write_rendered_logo_candidates


ASSET_DIR = ROOT / "data" / "media_review" / "assets"
MAX_CAMPUS = 5


class ReviewState:
    def __init__(self, batch: int, names: list[str] | None):
        self.settings = load_settings()
        self.client = get_client(self.settings)
        self.batch = batch
        self.names = names
        self.schools: list[dict] = []
        self.candidates: dict[str, dict] = {}
        self.refresh(load_candidates=False)

    def refresh(self, *, load_candidates: bool = True) -> None:
        self.schools = self._fetch_schools()
        self.candidates = {}
        if load_candidates:
            self.candidates = {row["id"]: self._build_candidates(row) for row in self.schools}

    def candidates_for(self, school_id: str, website_override: str = "") -> dict:
        cache_key = f"{school_id}|{website_override.strip()}"
        if cache_key not in self.candidates:
            row = next((s for s in self.schools if s["id"] == school_id), None)
            if not row:
                raise ValueError("School not found in current review batch")
            self.candidates[cache_key] = self._build_candidates(row, website_override=website_override)
        return self.candidates[cache_key]

    def _fetch_schools(self) -> list[dict]:
        cols = "id,name_en,name_zh,official_website,logo_url,campus_image_urls"
        q = self.client.table(SCHOOLS_TABLE).select(cols)
        if self.names:
            q = q.in_("name_en", self.names)
        else:
            q = (
                q
                .in_("status", ["active", "done", "enriched"])
                .not_.is_("official_website", "null")
                .or_("logo_url.is.null,campus_image_urls.is.null,campus_image_urls.eq.{}")
                .limit(self.batch)
            )
        return q.execute().data or []

    def _build_candidates(self, row: dict, website_override: str = "") -> dict:
        website = website_override.strip() or row.get("official_website") or ""
        raw = collect_candidates(website) if website else []
        rendered = (
            write_rendered_logo_candidates(website, row["name_en"], ASSET_DIR, row["id"])
            if website else []
        )
        logo = [{"kind": "logo_url", "label": f"logo {i}", "value": c.url} for i, c in enumerate(select_logo_candidates(raw, limit=8))]
        rendered_logo = [
            {"kind": "logo_file", "label": label, "value": str(path)}
            for label, path in rendered
        ]
        campus = select_campus_candidates(raw, limit=8)
        campus.extend(search_campus_image_candidates(
            self.settings.tavily_api_key,
            row["name_en"],
            official_website=website,
            limit=8,
        ))
        seen = set()
        campus_items = []
        for c in campus:
            if c.url in seen:
                continue
            seen.add(c.url)
            campus_items.append({"kind": "campus_url", "label": f"campus {len(campus_items)}", "value": c.url})
            if len(campus_items) >= 12:
                break
        return {
            "logo": rendered_logo + logo,
            "campus": campus_items,
        }

    def approve(self, payload: dict) -> dict:
        school_id = payload.get("school_id")
        row = next((s for s in self.schools if s["id"] == school_id), None)
        if not row:
            raise ValueError("School not found in current review batch")

        update = {}
        logo_status = MEDIA_OK if row.get("logo_url") else "missing"
        campus_status = MEDIA_OK if row.get("campus_image_urls") else "missing"

        logo = payload.get("logo")
        if logo:
            if logo.get("kind") == "logo_file":
                stored = store_school_media_bytes(
                    self.client,
                    bucket=self.settings.school_media_bucket,
                    school_id=school_id,
                    kind="rendered-logo",
                    data=Path(logo["value"]).read_bytes(),
                    media_type="image/png",
                )
            else:
                stored = store_school_media(
                    self.client,
                    bucket=self.settings.school_media_bucket,
                    school_id=school_id,
                    kind="logo",
                    source_url=logo["value"],
                )
            if not stored:
                raise ValueError("Logo upload failed")
            update["logo_url"] = stored.public_url
            logo_status = MEDIA_OK

        campus_urls = payload.get("campus") or []
        if campus_urls:
            public_urls = []
            for idx, item in enumerate(campus_urls[:MAX_CAMPUS], start=1):
                stored = store_school_media(
                    self.client,
                    bucket=self.settings.school_media_bucket,
                    school_id=school_id,
                    kind=f"campus-{idx}",
                    source_url=item["value"],
                )
                if stored:
                    public_urls.append(stored.public_url)
            if public_urls:
                update["campus_image_urls"] = public_urls
                campus_status = MEDIA_OK

        if update:
            self.client.table(SCHOOLS_TABLE).update(update).eq("id", school_id).execute()
        upsert_media_status(
            self.client,
            school_id,
            logo_status=logo_status,
            campus_image_status=campus_status,
        )
        row.update(update)
        return {"ok": True, "updated": update}


def make_handler(state: ReviewState):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path.startswith("/assets/"):
                return self._serve_asset(parsed.path)
            if parsed.path == "/candidates":
                qs = parse_qs(parsed.query)
                school_id = (qs.get("school_id") or [""])[0]
                website = (qs.get("website") or [""])[0]
                try:
                    return self._json({"ok": True, "html": render_candidates(school_id, state.candidates_for(school_id, website))})
                except Exception as exc:
                    return self._json({"ok": False, "error": str(exc)}, status=400)
            if parsed.path == "/refresh":
                state.refresh(load_candidates=False)
                return self._redirect("/")
            return self._html(render_page(state))

        def do_POST(self):
            if self.path != "/approve":
                return self._json({"ok": False, "error": "Not found"}, status=404)
            length = int(self.headers.get("Content-Length") or "0")
            data = json.loads(self.rfile.read(length).decode("utf-8"))
            try:
                result = state.approve(data)
            except Exception as exc:
                return self._json({"ok": False, "error": str(exc)}, status=400)
            return self._json(result)

        def log_message(self, format, *args):
            return

        def _serve_asset(self, path: str):
            filename = path.split("/assets/", 1)[1]
            target = (ASSET_DIR / filename).resolve()
            if not str(target).startswith(str(ASSET_DIR.resolve())) or not target.exists():
                return self._json({"ok": False, "error": "asset not found"}, status=404)
            self.send_response(200)
            self.send_header("Content-Type", "image/png")
            self.end_headers()
            self.wfile.write(target.read_bytes())

        def _html(self, body: str):
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(body.encode("utf-8"))

        def _json(self, data: dict, status: int = 200):
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(data).encode("utf-8"))

        def _redirect(self, location: str):
            self.send_response(302)
            self.send_header("Location", location)
            self.end_headers()

    return Handler


def render_page(state: ReviewState) -> str:
    cards = "\n".join(render_school(row) for row in state.schools)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>School Media Review</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 28px; color: #181818; background: #f7f7f7; }}
    header {{ display: flex; align-items: center; justify-content: space-between; margin-bottom: 20px; }}
    section {{ background: white; border: 1px solid #ddd; border-radius: 8px; padding: 18px; margin-bottom: 18px; }}
    h1, h2, h3 {{ margin: 0 0 12px; }}
    h3 {{ margin-top: 18px; font-size: 15px; color: #555; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(190px, 1fr)); gap: 12px; }}
    label.card {{ display: block; border: 2px solid #ddd; border-radius: 8px; padding: 8px; background: #fff; cursor: pointer; }}
    label.card:has(input:checked) {{ border-color: #1358d8; box-shadow: 0 0 0 3px rgba(19, 88, 216, .12); }}
    img {{ width: 100%; aspect-ratio: 4 / 3; object-fit: contain; background: #f2f2f2; }}
    .campus img {{ object-fit: cover; }}
    input {{ margin-right: 6px; }}
    code {{ display: block; font-size: 10px; color: #666; word-break: break-all; margin-top: 6px; }}
    button, a.button {{ border: 0; border-radius: 6px; padding: 10px 14px; background: #111; color: white; text-decoration: none; cursor: pointer; font-size: 14px; }}
    .status {{ margin-left: 10px; font-weight: 600; }}
    .muted {{ color: #777; }}
  </style>
</head>
<body>
  <header>
    <div>
      <h1>School Media Review</h1>
      <div class="muted">Select one logo and up to {MAX_CAMPUS} campus images per school.</div>
    </div>
    <a class="button" href="/refresh">Refresh batch</a>
  </header>
  {cards}
  <script>
    async function loadCandidates(schoolId) {{
      const root = document.querySelector(`[data-school-id="${{schoolId}}"]`);
      const target = root.querySelector('.candidate-root');
      const website = root.querySelector('.website-input').value;
      target.innerHTML = '<p class="muted">Loading candidates...</p>';
      const resp = await fetch('/candidates?school_id=' + encodeURIComponent(schoolId) + '&website=' + encodeURIComponent(website));
      const data = await resp.json();
      target.innerHTML = data.ok ? data.html : '<p class="muted">Error: ' + data.error + '</p>';
    }}
    async function approve(schoolId) {{
      const root = document.querySelector(`[data-school-id="${{schoolId}}"]`);
      const logoInput = root.querySelector('input[name="logo-' + schoolId + '"]:checked');
      const campusInputs = [...root.querySelectorAll('input[name="campus-' + schoolId + '"]:checked')].slice(0, {MAX_CAMPUS});
      const payload = {{
        school_id: schoolId,
        logo: logoInput ? JSON.parse(logoInput.value) : null,
        campus: campusInputs.map(i => JSON.parse(i.value))
      }};
      const status = root.querySelector('.status');
      status.textContent = 'Submitting...';
      const resp = await fetch('/approve', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(payload)
      }});
      const data = await resp.json();
      status.textContent = data.ok ? 'Saved' : 'Error: ' + data.error;
    }}
  </script>
</body>
</html>"""


def render_school(row: dict) -> str:
    sid = row["id"]
    title = html.escape(f"{row.get('name_en')} / {row.get('name_zh') or ''}")
    return f"""
    <section data-school-id="{html.escape(sid)}">
      <h2>{title}</h2>
      <div class="muted">Candidate source URL</div>
      <input class="website-input" value="{html.escape(row.get('official_website') or '', quote=True)}" style="width: min(760px, 100%); padding: 8px; margin: 8px 0 14px; font-size: 14px;" />
      <p><button onclick="loadCandidates('{html.escape(sid)}')">Load candidates</button></p>
      <div class="candidate-root"></div>
      <p><button onclick="approve('{html.escape(sid)}')">Submit selected</button><span class="status"></span></p>
    </section>
    """


def render_candidates(sid: str, candidates: dict) -> str:
    logo_cards = "\n".join(render_option(sid, "logo", item) for item in candidates.get("logo", []))
    campus_cards = "\n".join(render_option(sid, "campus", item) for item in candidates.get("campus", []))
    return f"""
      <h3>Logo</h3>
      <div class="grid">{logo_cards or '<p class="muted">No logo candidates</p>'}</div>
      <h3>Campus</h3>
      <div class="grid campus">{campus_cards or '<p class="muted">No campus candidates</p>'}</div>
    """


def render_option(school_id: str, group: str, item: dict) -> str:
    value = html.escape(json.dumps(item), quote=True)
    label = html.escape(item["label"])
    raw = item["value"]
    if item["kind"] == "logo_file":
        src = "/assets/" + html.escape(Path(raw).name, quote=True)
    else:
        src = html.escape(raw, quote=True)
    input_type = "radio" if group == "logo" else "checkbox"
    return f"""
    <label class="card">
      <input type="{input_type}" name="{group}-{html.escape(school_id)}" value="{value}" />
      {label}
      <img src="{src}" loading="lazy" referrerpolicy="no-referrer" />
      <code>{html.escape(raw)}</code>
    </label>
    """


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", type=int, default=10)
    parser.add_argument("--schools", default=None, help="Comma-separated name_en values")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    args = parser.parse_args()

    names = [n.strip() for n in args.schools.split(",") if n.strip()] if args.schools else None
    state = ReviewState(batch=args.batch, names=names)
    server = ThreadingHTTPServer((args.host, args.port), make_handler(state))
    print(f"http://{args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
