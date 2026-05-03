"""Delete all rows in `public.program_evaluations` so Stage 5 can re-insert full prose.

Use after widening columns (migrate_p5 + migrate_p6) and updating stage5 so
competition_level / data_source are no longer truncated. Re-run:

    python run_pipeline.py --stage 5 --batch <N>

with a batch large enough to cover every program that needs satellite data.

This wipes acceptance_rate and application_difficulty_score too — Stage 5 will
re-fetch them from the model in the same JSON pass.

Usage:
    python -m scripts.clear_program_evaluations --yes
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import load_settings
from db.supabase_client import get_client
from utils.logger import get_logger

log = get_logger("clear_program_evaluations")

TABLE = "program_evaluations"
ZERO = "00000000-0000-0000-0000-000000000000"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Required confirmation flag (deletes every evaluation row).",
    )
    args = parser.parse_args()
    if not args.yes:
        print("Refusing to delete: pass --yes to confirm wiping program_evaluations.")
        return 2

    settings = load_settings()
    client = get_client(settings)
    try:
        resp = client.table(TABLE).delete().neq("id", ZERO).execute()
    except Exception:
        try:
            resp = client.table(TABLE).delete().neq("id", -1).execute()
        except Exception as exc:
            log.error("delete failed: %s", exc)
            return 1
    n = len(resp.data) if resp.data else 0
    log.info("deleted %s rows from %s (reported by API; may be partial)", n, TABLE)
    print(
        f"Done. Re-fill with: python run_pipeline.py --stage 5 --batch <count>\n"
        f"Use a batch size ≥ number of programs missing satellite rows "
        f"(or run Stage 5 repeatedly until idle)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
