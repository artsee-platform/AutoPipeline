"""Supabase Storage upload helpers for school media.

Images are streamed from the source URL into memory and uploaded directly to
Storage. No local files are written.
"""

from dataclasses import dataclass
import hashlib
import mimetypes
from urllib.parse import urlparse

import requests

from utils.logger import get_logger

log = get_logger("media_storage")

MAX_STORAGE_IMAGE_BYTES = 8 * 1024 * 1024
DOWNLOAD_TIMEOUT_S = 15

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

_EXT_BY_MEDIA = {
    "image/jpeg": "jpg",
    "image/png": "png",
    "image/webp": "webp",
    "image/gif": "gif",
    "image/svg+xml": "svg",
    "image/x-icon": "ico",
    "image/vnd.microsoft.icon": "ico",
}


@dataclass(frozen=True)
class StoredMedia:
    public_url: str
    object_path: str
    media_type: str
    bytes_count: int


def ensure_public_bucket(client, bucket: str) -> None:
    """Create the public bucket if it does not already exist."""
    try:
        client.storage.get_bucket(bucket)
        return
    except Exception:
        pass

    try:
        client.storage.create_bucket(bucket, options={"public": True})
        log.info("Created Supabase Storage bucket: %s", bucket)
    except Exception as exc:
        # If another run created it between get/create, later upload will tell us.
        log.debug("create bucket %s failed/exists: %s", bucket, exc)


def store_school_media(
    client,
    *,
    bucket: str,
    school_id: str,
    kind: str,
    source_url: str,
) -> StoredMedia | None:
    """Download a source image into memory and upload it to Supabase Storage."""
    if not source_url:
        return None

    downloaded = _download_image(source_url)
    if not downloaded:
        return None
    data, media_type = downloaded
    ext = _extension_for(source_url, media_type)
    digest = hashlib.sha1(source_url.encode("utf-8")).hexdigest()[:10]
    object_path = f"schools/{school_id}/{kind}-{digest}.{ext}"

    ensure_public_bucket(client, bucket)
    storage = client.storage.from_(bucket)
    file_options = {
        "content-type": media_type,
        "cache-control": "31536000",
        "upsert": "true",
    }

    try:
        try:
            storage.upload(object_path, data, file_options=file_options)
        except Exception:
            storage.update(object_path, data, file_options=file_options)
        public_url = storage.get_public_url(object_path)
    except Exception as exc:
        log.warning("storage upload failed for %s: %s", source_url, exc)
        return None

    return StoredMedia(
        public_url=public_url,
        object_path=object_path,
        media_type=media_type,
        bytes_count=len(data),
    )


def store_school_media_bytes(
    client,
    *,
    bucket: str,
    school_id: str,
    kind: str,
    data: bytes,
    media_type: str = "image/png",
) -> StoredMedia | None:
    """Upload in-memory media bytes to Supabase Storage."""
    if not data:
        return None
    ext = _EXT_BY_MEDIA.get(media_type, "png")
    digest = hashlib.sha1(data).hexdigest()[:10]
    object_path = f"schools/{school_id}/{kind}-{digest}.{ext}"
    stored = _upload_bytes(client, bucket=bucket, object_path=object_path, data=data, media_type=media_type)
    if not stored:
        return None
    return StoredMedia(
        public_url=stored,
        object_path=object_path,
        media_type=media_type,
        bytes_count=len(data),
    )


def store_program_cover_media(
    client,
    *,
    bucket: str,
    school_id: str,
    program_key: str,
    source_url: str,
) -> StoredMedia | None:
    """Upload a program cover image to Storage and return its public URL."""
    if not source_url:
        return None

    downloaded = _download_image(source_url)
    if not downloaded:
        return None
    data, media_type = downloaded
    ext = _extension_for(source_url, media_type)
    safe_key = "".join(ch if ch.isalnum() else "-" for ch in program_key.lower())[:80].strip("-")
    if not safe_key:
        safe_key = hashlib.sha1(source_url.encode("utf-8")).hexdigest()[:10]
    object_path = f"programs/{school_id}/{safe_key}/cover.{ext}"

    ensure_public_bucket(client, bucket)
    storage = client.storage.from_(bucket)
    file_options = {
        "content-type": media_type,
        "cache-control": "31536000",
        "upsert": "true",
    }

    try:
        try:
            storage.upload(object_path, data, file_options=file_options)
        except Exception:
            storage.update(object_path, data, file_options=file_options)
        public_url = storage.get_public_url(object_path)
    except Exception as exc:
        log.warning("program cover upload failed for %s: %s", source_url, exc)
        return None

    return StoredMedia(
        public_url=public_url,
        object_path=object_path,
        media_type=media_type,
        bytes_count=len(data),
    )


def _upload_bytes(client, *, bucket: str, object_path: str, data: bytes, media_type: str) -> str | None:
    ensure_public_bucket(client, bucket)
    storage = client.storage.from_(bucket)
    file_options = {
        "content-type": media_type,
        "cache-control": "31536000",
        "upsert": "true",
    }

    try:
        try:
            storage.upload(object_path, data, file_options=file_options)
        except Exception:
            storage.update(object_path, data, file_options=file_options)
        return storage.get_public_url(object_path)
    except Exception as exc:
        log.warning("storage upload failed for %s: %s", object_path, exc)
        return None


def _download_image(url: str) -> tuple[bytes, str] | None:
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=DOWNLOAD_TIMEOUT_S, stream=True)
        resp.raise_for_status()
    except Exception as exc:
        log.warning("image download failed for storage upload %s: %s", url, exc)
        return None

    media_type = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
    if not media_type.startswith("image/"):
        guessed, _ = mimetypes.guess_type(urlparse(url).path)
        media_type = guessed or media_type
    if not media_type.startswith("image/"):
        log.warning("storage upload skipped non-image %s (%s)", url, media_type or "unknown")
        return None

    chunks: list[bytes] = []
    total = 0
    for chunk in resp.iter_content(chunk_size=65536):
        if not chunk:
            continue
        chunks.append(chunk)
        total += len(chunk)
        if total > MAX_STORAGE_IMAGE_BYTES:
            log.warning("storage upload skipped oversized image %s", url)
            return None

    if total == 0:
        return None
    return b"".join(chunks), media_type


def _extension_for(url: str, media_type: str) -> str:
    if media_type in _EXT_BY_MEDIA:
        return _EXT_BY_MEDIA[media_type]
    guessed, _ = mimetypes.guess_type(urlparse(url).path)
    if guessed in _EXT_BY_MEDIA:
        return _EXT_BY_MEDIA[guessed]
    return "img"
