"""Stage 4 — Sync 'done' rows from Supabase → Lark Base (Bitable)."""
from config.settings import Settings
from db.supabase_client import get_client as get_supabase, fetch_by_status, update_status
from db.lark_client import get_client as get_lark, upsert_record
from utils.logger import get_logger

log = get_logger("stage4")


def run(settings: Settings, batch_size: int) -> None:
    supabase = get_supabase(settings)
    lark = get_lark(settings)

    schools = fetch_by_status(supabase, "done", batch_size)
    log.info(f"Syncing {len(schools)} schools to Lark Base")

    for school in schools:
        name_en = school["name_en"]
        try:
            upsert_record(lark, settings, school)
            update_status(supabase, name_en, "synced")
            log.info(f"  ✓ synced: {name_en}")
        except Exception as exc:
            log.error(f"  ✗ {name_en}: {exc}")
            update_status(supabase, name_en, "error")
