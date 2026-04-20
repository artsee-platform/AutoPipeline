#!/usr/bin/env python3
"""
Art/Design Schools Data Pipeline
=================================
Usage:
  python run_pipeline.py --stage 0                    # seed Supabase from xlsx (explicit only)
  python run_pipeline.py --stage 1 --batch 10         # web enrich 10 schools
  python run_pipeline.py --stage 2 --batch 20         # QS rankings lookup
  python run_pipeline.py --stage 3 --batch 10         # video metadata
  python run_pipeline.py --stage 4 --batch 5          # seed programs (Tavily + Claude)
  python run_pipeline.py --stage 4 --batch 200 --reset-programs  # wipe programs, re-seed all schools
  python run_pipeline.py --stage 5 --batch 10         # fees / admissions / evaluations (no art links)
  python run_pipeline.py --stage 5 --batch 10 --fill-art-categories  # also program_art_categories
  python run_pipeline.py --stage 1-3 --batch 10       # run all enrich stages
  python run_pipeline.py                              # default: runs stages 0-3
  python run_pipeline.py --retry-errors               # reset error rows → pending
"""
import argparse
import sys
from config.settings import load_settings
from utils.logger import get_logger

log = get_logger("pipeline")


def parse_stages(stage_str: str) -> list[int]:
    """Parse '1-4' → [1,2,3,4], '2' → [2]. Supports up to stage 5."""
    if "-" in stage_str:
        start, end = stage_str.split("-", 1)
        return list(range(int(start), int(end) + 1))
    return [int(stage_str)]


def main():
    parser = argparse.ArgumentParser(
        description="Art/Design Schools Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--stage",
        type=str,
        default=None,
        help="Stage(s) to run: 0–5, or range like 1-3 (4=programs, 5=program satellite tables)",
    )
    parser.add_argument(
        "--batch",
        type=int,
        default=None,
        help="Number of schools to process per stage (overrides BATCH_SIZE in .env)",
    )
    parser.add_argument(
        "--retry-errors",
        action="store_true",
        help="Reset all error-status rows back to pending, then exit",
    )
    parser.add_argument(
        "--reset-programs",
        action="store_true",
        help="With --stage 4: delete all rows in programs before seeding (full re-run with current strategy)",
    )
    parser.add_argument(
        "--fill-art-categories",
        action="store_true",
        help="With --stage 5: also fill program_art_categories (default: only fees/admissions/evaluations)",
    )
    args = parser.parse_args()

    try:
        settings = load_settings()
    except EnvironmentError as e:
        log.error(str(e))
        log.error("Copy .env.example to .env and fill in your credentials.")
        sys.exit(1)

    batch_size = args.batch or settings.batch_size

    # Handle --retry-errors
    if args.retry_errors:
        from db.supabase_client import get_client, reset_errors_to_pending
        reset_errors_to_pending(get_client(settings))
        return

    # Determine which stages to run
    if args.stage is None:
        stages = [0, 1, 2, 3]
    else:
        try:
            stages = parse_stages(args.stage)
        except ValueError:
            log.error(f"Invalid --stage value: {args.stage!r}. Use 0-5 or a range like 1-3.")
            sys.exit(1)
        invalid = [s for s in stages if s not in {0, 1, 2, 3, 4, 5}]
        if invalid:
            log.error(f"Invalid stage(s): {invalid}. Use only 0–5 or a range like 1-5.")
            sys.exit(1)

    log.info(f"Running stages {stages} with batch_size={batch_size}")

    for stage in stages:
        log.info(f"{'='*40}")
        log.info(f"  Stage {stage}")
        log.info(f"{'='*40}")

        if stage == 0:
            from pipeline.stage0_seed import run
            run(settings)

        elif stage == 1:
            from pipeline.stage1_web_enrich import run
            run(settings, batch_size)

        elif stage == 2:
            from pipeline.stage2_qs_rankings import run
            run(settings, batch_size)

        elif stage == 3:
            from pipeline.stage3_video import run
            run(settings, batch_size)

        elif stage == 4:
            if args.reset_programs:
                from db.supabase_client import get_client
                log.warning("Deleting all rows from table: programs (--reset-programs)")
                client = get_client(settings)
                tbl = client.table("programs")
                # PostgREST requires a filter. Prefer uuid PK; fall back to serial int.
                try:
                    tbl.delete().neq(
                        "id", "00000000-0000-0000-0000-000000000000"
                    ).execute()
                except Exception as e:
                    log.debug("programs delete (uuid filter) failed, retry int: %s", e)
                    tbl.delete().neq("id", -1).execute()
            from pipeline.stage4_programs import run
            run(settings, batch_size)

        elif stage == 5:
            from pipeline.stage5_program_satellite import run
            run(settings, batch_size, fill_art_categories=args.fill_art_categories)

        else:
            log.error(f"Unknown stage: {stage}")
            sys.exit(1)

    log.info("Pipeline run complete.")


if __name__ == "__main__":
    main()
