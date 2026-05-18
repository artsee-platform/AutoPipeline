#!/usr/bin/env python3
"""Approve a media URL by uploading it to Supabase Storage and updating the DB.

Usage:
  python scripts/approve_media_candidate.py --school "Royal College of Art" --logo-url "https://..."
  python scripts/approve_media_candidate.py --school "Royal College of Art" --campus-url "https://..."
  python scripts/approve_media_candidate.py --program-id "<uuid>" --cover-url "https://..."
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import load_settings
from db.media_status import upsert_media_status
from db.supabase_client import TABLE as SCHOOLS_TABLE, get_client
from pipeline.media_storage import store_program_cover_media, store_school_media
from pipeline.media_validator import MEDIA_OK


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--school", help="School name_en for logo/campus approval")
    parser.add_argument("--logo-url")
    parser.add_argument("--logo-file", type=Path)
    parser.add_argument("--campus-url")
    parser.add_argument("--program-id")
    parser.add_argument("--cover-url")
    args = parser.parse_args()

    settings = load_settings()
    client = get_client(settings)

    if args.program_id and args.cover_url:
        _approve_program_cover(client, settings.school_media_bucket, args.program_id, args.cover_url)
        return

    if not args.school:
        raise SystemExit("--school is required for logo/campus approval")
    if not args.logo_url and not args.logo_file and not args.campus_url:
        raise SystemExit("Provide --logo-url, --campus-url, or --cover-url with --program-id")

    row = (
        client.table(SCHOOLS_TABLE)
        .select("id,name_en,logo_url,campus_image_urls")
        .eq("name_en", args.school)
        .limit(1)
        .execute()
        .data
        or []
    )
    if not row:
        raise SystemExit(f"School not found: {args.school}")
    school = row[0]

    payload = {}
    logo_status = None
    campus_status = None

    if args.logo_url:
        stored = store_school_media(
            client,
            bucket=settings.school_media_bucket,
            school_id=school["id"],
            kind="logo",
            source_url=args.logo_url,
        )
        if not stored:
            raise SystemExit("Logo upload failed")
        payload["logo_url"] = stored.public_url
        logo_status = MEDIA_OK

    if args.logo_file:
        from pipeline.media_storage import store_school_media_bytes

        stored = store_school_media_bytes(
            client,
            bucket=settings.school_media_bucket,
            school_id=school["id"],
            kind="rendered-logo",
            data=args.logo_file.read_bytes(),
            media_type="image/png",
        )
        if not stored:
            raise SystemExit("Rendered logo upload failed")
        payload["logo_url"] = stored.public_url
        logo_status = MEDIA_OK

    if args.campus_url:
        stored = store_school_media(
            client,
            bucket=settings.school_media_bucket,
            school_id=school["id"],
            kind="campus-1",
            source_url=args.campus_url,
        )
        if not stored:
            raise SystemExit("Campus upload failed")
        payload["campus_image_urls"] = [stored.public_url]
        campus_status = MEDIA_OK

    client.table(SCHOOLS_TABLE).update(payload).eq("id", school["id"]).execute()
    final_logo_status = logo_status or (MEDIA_OK if school.get("logo_url") else "missing")
    final_campus_status = campus_status or (MEDIA_OK if school.get("campus_image_urls") else "missing")
    upsert_media_status(
        client,
        school["id"],
        logo_status=final_logo_status,
        campus_image_status=final_campus_status,
    )
    print(payload)


def _approve_program_cover(client, bucket: str, program_id: str, cover_url: str) -> None:
    rows = (
        client.table("programs")
        .select("id,school_id,program_name")
        .eq("id", program_id)
        .limit(1)
        .execute()
        .data
        or []
    )
    if not rows:
        raise SystemExit(f"Program not found: {program_id}")
    program = rows[0]
    stored = store_program_cover_media(
        client,
        bucket=bucket,
        school_id=program["school_id"],
        program_key=program["id"],
        source_url=cover_url,
    )
    if not stored:
        raise SystemExit("Program cover upload failed")
    client.table("programs").update({"cover_image_url": stored.public_url}).eq("id", program_id).execute()
    print({"cover_image_url": stored.public_url})


if __name__ == "__main__":
    main()
