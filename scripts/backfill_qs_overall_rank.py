#!/usr/bin/env python3
"""Backfill or refresh only `schools.qs_overall_rank` (integer or NULL).

NULL means QS overall rank not found / not listed (show \"未上榜\" in UI via
`display_qs_overall_rank`). Same resolution as stage 2: local CSV + Tavily/Claude.
Does not touch `status`, subject ranks, or any other column.

Examples (from repo root `yjxauto/`):
  python scripts/backfill_qs_overall_rank.py
  python scripts/backfill_qs_overall_rank.py --batch 30
  python scripts/backfill_qs_overall_rank.py --schools "Royal College of Art,Parsons School of Design"
  python scripts/backfill_qs_overall_rank.py --all --batch 50
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import anthropic

from config.settings import load_settings
from db.supabase_client import TABLE, get_client
from pipeline.qs_global_rank import (
    display_qs_overall_rank,
    load_overall_index,
    resolve_qs_overall_rank_with_llm,
)
from utils.logger import get_logger

log = get_logger("backfill_qs_overall_rank")

_COLS = "id,name_en,raw_country,qs_overall_rank"


def _fetch_targets(client, batch: int, all_rows: bool, names: list[str] | None):
    q = client.table(TABLE).select(_COLS)

    if names:
        q = q.in_("name_en", names).limit(batch)
    elif all_rows:
        q = q.order("name_en").limit(batch)
    else:
        q = q.is_("qs_overall_rank", "null").order("name_en").limit(batch)

    resp = q.execute()
    return resp.data or []


def run(
    batch: int,
    all_rows: bool = False,
    names: list[str] | None = None,
    sleep_llm: float = 0.5,
) -> None:
    settings = load_settings()
    client = get_client(settings)
    claude = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    idx = load_overall_index()

    targets = _fetch_targets(client, batch, all_rows, names)
    log.info(
        "Backfill qs_overall_rank: %s school(s) (batch=%s, all=%s, names=%s)",
        len(targets),
        batch,
        all_rows,
        "yes" if names else "no",
    )

    ok = 0
    errors = 0
    for row in targets:
        name_en = row["name_en"]
        country = row.get("raw_country") or ""
        try:
            n, _, used_llm = resolve_qs_overall_rank_with_llm(
                name_en, country, idx, settings, claude
            )
            client.table(TABLE).update({"qs_overall_rank": n}).eq("id", row["id"]).execute()
            tag = " [LLM]" if used_llm else ""
            log.info(
                "  %s%s -> qs_overall_rank=%s (%s)",
                name_en,
                tag,
                n,
                display_qs_overall_rank(n),
            )
            ok += 1
            if used_llm and sleep_llm:
                time.sleep(sleep_llm)
        except Exception as exc:
            log.error("  %s: %s", name_en, exc)
            errors += 1

    log.info("Done: %s updated, %s errors", ok, errors)


def main() -> None:
    parser = argparse.ArgumentParser(description="Update only schools.qs_overall_rank (int or NULL)")
    parser.add_argument("--batch", type=int, default=None, help="Max schools (default: BATCH_SIZE from .env)")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Ignore NULL filter; process first --batch rows (ordered by name_en)",
    )
    parser.add_argument(
        "--schools",
        type=str,
        default=None,
        help="Comma-separated name_en list (overrides default filter)",
    )
    args = parser.parse_args()

    try:
        settings = load_settings()
    except EnvironmentError as e:
        log.error("%s", e)
        sys.exit(1)

    batch = args.batch or settings.batch_size
    names = (
        [n.strip() for n in args.schools.split(",") if n.strip()]
        if args.schools
        else None
    )

    run(
        batch=batch,
        all_rows=args.all,
        names=names,
    )


if __name__ == "__main__":
    main()
