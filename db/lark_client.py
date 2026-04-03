import lark_oapi as lark
from lark_oapi.api.bitable.v1 import (
    ListAppTableRecordRequest,
    CreateAppTableRecordRequest,
    UpdateAppTableRecordRequest,
    AppTableRecord,
)
from config.settings import Settings
from utils.logger import get_logger

log = get_logger("lark")

# Map Supabase column names → Lark Bitable field names.
# Adjust if your Bitable field names differ from column names.
FIELD_MAP = {
    "name_zh": "name_zh",
    "name_en": "name_en",
    "country": "country",
    "city": "city",
    "school_type": "school_type",
    "qs_art_rank": "qs_art_rank",
    "qs_architecture_rank": "qs_architecture_rank",
    "school_tier": "school_tier",
    "official_website": "official_website",
    "international_students_page": "international_students_page",
    "founded_year": "founded_year",
    "description": "description",
    "feature_tags": "feature_tags",
    "qs_overall_rank": "qs_overall_rank",
    "entry_score_requirements": "entry_score_requirements",
    "annual_intake": "annual_intake",
    "application_deadline": "application_deadline",
    "strength_disciplines": "strength_disciplines",
    "notable_alumni": "notable_alumni",
    "logo_url": "logo_url",
    "campus_image_urls": "campus_image_urls",
}


def get_client(settings: Settings) -> lark.Client:
    return (
        lark.Client.builder()
        .app_id(settings.lark_app_id)
        .app_secret(settings.lark_app_secret)
        .build()
    )


def _to_fields(school: dict) -> dict:
    """Convert a school dict to Lark Bitable field dict.
    Lists are serialised to comma-separated strings for text fields.
    """
    fields = {}
    for col, field_name in FIELD_MAP.items():
        val = school.get(col)
        if val is None:
            continue
        if isinstance(val, list):
            val = ", ".join(str(v) for v in val)
        fields[field_name] = val
    return fields


def _find_record_id(client: lark.Client, settings: Settings, name_en: str) -> str | None:
    """Return the Bitable record_id for name_en, or None if not found."""
    req = (
        ListAppTableRecordRequest.builder()
        .app_token(settings.lark_base_app_token)
        .table_id(settings.lark_table_id)
        .filter(f'CurrentValue.[name_en] = "{name_en}"')
        .page_size(1)
        .build()
    )
    resp = client.bitable.v1.app_table_record.list(req)
    if not resp.success():
        log.error(f"Lark list_records failed for {name_en}: {resp.msg}")
        return None
    items = resp.data.items if resp.data and resp.data.items else []
    return items[0].record_id if items else None


def upsert_record(client: lark.Client, settings: Settings, school: dict) -> None:
    name_en = school.get("name_en", "")
    fields = _to_fields(school)

    record_id = _find_record_id(client, settings, name_en)

    if record_id:
        req = (
            UpdateAppTableRecordRequest.builder()
            .app_token(settings.lark_base_app_token)
            .table_id(settings.lark_table_id)
            .record_id(record_id)
            .request_body(AppTableRecord.builder().fields(fields).build())
            .build()
        )
        resp = client.bitable.v1.app_table_record.update(req)
        if not resp.success():
            raise RuntimeError(f"Lark update_record failed for {name_en}: {resp.msg}")
        log.info(f"Lark updated: {name_en}")
    else:
        req = (
            CreateAppTableRecordRequest.builder()
            .app_token(settings.lark_base_app_token)
            .table_id(settings.lark_table_id)
            .request_body(AppTableRecord.builder().fields(fields).build())
            .build()
        )
        resp = client.bitable.v1.app_table_record.create(req)
        if not resp.success():
            raise RuntimeError(f"Lark create_record failed for {name_en}: {resp.msg}")
        log.info(f"Lark created: {name_en}")
