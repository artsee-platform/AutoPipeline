"""Stage 0 — Load schools.xlsx, clean data, seed Supabase with status='pending'."""
import pandas as pd
from config.settings import Settings
from db.supabase_client import get_client, upsert_school
from utils.logger import get_logger

log = get_logger("stage0")

XLSX_PATH = "data/schools.xlsx"


def _clean_str(val) -> str | None:
    if pd.isna(val):
        return None
    return str(val).replace("\n", "").strip() or None


def load_and_clean_xlsx() -> list[dict]:
    df = pd.read_excel(XLSX_PATH, dtype=str)

    # Fill down merged cells for continent and country_or_area
    df["continent"] = df["continent"].ffill()
    df["country_or_area"] = df["country_or_area"].ffill()

    schools = []
    for _, row in df.iterrows():
        name_en = _clean_str(row.get("name_en"))
        if not name_en:
            continue  # skip blank rows

        school = {
            "name_en": name_en,
            "name_zh": _clean_str(row.get("name_zh")),
            "country": _clean_str(row.get("country_or_area")),
            "official_website": _clean_str(row.get("official_website")),
            "status": "pending",
        }
        schools.append(school)

    log.info(f"Loaded {len(schools)} schools from {XLSX_PATH}")
    return schools


def run(settings: Settings) -> None:
    schools = load_and_clean_xlsx()
    client = get_client(settings)

    # Fetch existing rows to avoid duplicates and unique constraint violations
    existing = set()
    existing_websites = set()
    resp = client.table("schools_auto").select("name_en, official_website").execute()
    if resp.data:
        existing = {r["name_en"] for r in resp.data}
        existing_websites = {r["official_website"] for r in resp.data if r.get("official_website")}
    log.info(f"Found {len(existing)} existing rows in Supabase")

    seen_websites = set()
    inserted = skipped = 0
    for school in schools:
        if school["name_en"] in existing:
            skipped += 1
            continue
        # Null out website if it's already used (unique constraint)
        website = school.get("official_website")
        if website and (website in existing_websites or website in seen_websites):
            school["official_website"] = None
        elif website:
            seen_websites.add(website)
        client.table("schools_auto").insert(school).execute()
        inserted += 1

    log.info(f"Seeded {inserted} new rows, skipped {skipped} existing")
