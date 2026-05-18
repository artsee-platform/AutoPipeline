"""Helpers for the lightweight school_media_status table."""

from typing import Optional
from datetime import datetime, timezone

from pipeline.media_validator import MEDIA_MISSING, VALID_STATUSES

TABLE = "school_media_status"


def upsert_media_status(
    client,
    school_id: str,
    *,
    logo_status: str,
    campus_image_status: str,
) -> None:
    payload = {
        "school_id": school_id,
        "logo_status": _clean_status(logo_status),
        "campus_image_status": _clean_status(campus_image_status),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    client.table(TABLE).upsert(payload, on_conflict="school_id").execute()


def fetch_school_ids_needing_media(client, limit: int) -> list[str]:
    resp = (
        client.table(TABLE)
        .select("school_id")
        .or_("logo_status.neq.ok,campus_image_status.neq.ok")
        .limit(limit)
        .execute()
    )
    return [r["school_id"] for r in (resp.data or []) if r.get("school_id")]


def _clean_status(value: Optional[str]) -> str:
    if value in VALID_STATUSES:
        return value
    return MEDIA_MISSING
