from typing import List, Optional
from supabase import create_client, Client
from config.settings import Settings
from utils.logger import get_logger

log = get_logger("supabase")

TABLE = "schools_auto"


def get_client(settings: Settings) -> Client:
    return create_client(settings.supabase_url, settings.supabase_service_key)


def fetch_by_status(client: Client, status: str, limit: int) -> List[dict]:
    resp = (
        client.table(TABLE)
        .select("*")
        .eq("status", status)
        .limit(limit)
        .execute()
    )
    return resp.data or []


def upsert_school(client: Client, data: dict) -> None:
    """Update by id if present, otherwise insert."""
    row_id = data.get("id")
    if row_id:
        payload = {k: v for k, v in data.items() if k != "id"}
        client.table(TABLE).update(payload).eq("id", row_id).execute()
    else:
        client.table(TABLE).insert(data).execute()


def update_status(client: Client, name_en: str, status: str, extra: Optional[dict] = None) -> None:
    payload = {"status": status}
    if extra:
        payload.update(extra)
    client.table(TABLE).update(payload).eq("name_en", name_en).execute()


def reset_errors_to_pending(client: Client) -> int:
    resp = client.table(TABLE).update({"status": "pending"}).eq("status", "error").execute()
    count = len(resp.data) if resp.data else 0
    log.info(f"Reset {count} error rows → pending")
    return count
