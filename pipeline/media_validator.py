"""Lightweight URL image validation for school media.

The validator never persists image bytes. It streams a bounded amount into
memory, checks that the response is a plausible image, and extracts dimensions
for the common raster formats we use in the UI.
"""

from dataclasses import dataclass
import struct
from typing import Optional

import requests

from utils.logger import get_logger

log = get_logger("media_validator")

MEDIA_OK = "ok"
MEDIA_MISSING = "missing"
MEDIA_BROKEN = "broken"
MEDIA_LOW_QUALITY = "low_quality"
MEDIA_WRONG_TYPE = "wrong_type"

VALID_STATUSES = {
    MEDIA_OK,
    MEDIA_MISSING,
    MEDIA_BROKEN,
    MEDIA_LOW_QUALITY,
    MEDIA_WRONG_TYPE,
}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

_ALLOWED_CONTENT_TYPES = {
    "image/jpeg",
    "image/png",
    "image/webp",
    "image/gif",
    "image/svg+xml",
    "image/x-icon",
    "image/vnd.microsoft.icon",
}

MAX_VALIDATION_BYTES = 2 * 1024 * 1024
DOWNLOAD_TIMEOUT_S = 10


@dataclass(frozen=True)
class ValidationResult:
    status: str
    width: Optional[int] = None
    height: Optional[int] = None
    media_type: str = ""
    reason: str = ""


def validate_logo_url(url: Optional[str]) -> ValidationResult:
    return validate_image_url(url, min_width=16, min_height=16, allow_svg=True)


def validate_campus_url(url: Optional[str]) -> ValidationResult:
    return validate_image_url(url, min_width=400, min_height=250, allow_svg=False)


def validate_image_url(
    url: Optional[str],
    *,
    min_width: int,
    min_height: int,
    allow_svg: bool,
) -> ValidationResult:
    if not url:
        return ValidationResult(MEDIA_MISSING, reason="empty_url")

    try:
        resp = requests.get(url, headers=_HEADERS, timeout=DOWNLOAD_TIMEOUT_S, stream=True)
        resp.raise_for_status()
    except Exception as exc:
        return ValidationResult(MEDIA_BROKEN, reason=str(exc)[:160])

    media_type = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
    if media_type not in _ALLOWED_CONTENT_TYPES:
        return ValidationResult(MEDIA_WRONG_TYPE, media_type=media_type, reason="unsupported_content_type")

    data = b""
    for chunk in resp.iter_content(chunk_size=32768):
        if not chunk:
            continue
        data += chunk
        if len(data) > MAX_VALIDATION_BYTES:
            break

    if not data:
        return ValidationResult(MEDIA_BROKEN, media_type=media_type, reason="empty_response")

    if media_type == "image/svg+xml" or url.lower().split("?", 1)[0].endswith(".svg"):
        if allow_svg:
            return ValidationResult(MEDIA_OK, media_type=media_type)
        return ValidationResult(MEDIA_WRONG_TYPE, media_type=media_type, reason="svg_not_allowed")

    dimensions = _image_dimensions(data)
    if not dimensions:
        # Some icon formats do not expose dimensions through our tiny parser, but
        # they can still be valid logo fallbacks.
        if media_type in {"image/x-icon", "image/vnd.microsoft.icon"} and allow_svg:
            return ValidationResult(MEDIA_OK, media_type=media_type)
        return ValidationResult(MEDIA_BROKEN, media_type=media_type, reason="cannot_decode_dimensions")

    width, height = dimensions
    if width < min_width or height < min_height:
        return ValidationResult(
            MEDIA_LOW_QUALITY,
            width=width,
            height=height,
            media_type=media_type,
            reason="too_small",
        )

    return ValidationResult(MEDIA_OK, width=width, height=height, media_type=media_type)


def _image_dimensions(data: bytes) -> Optional[tuple[int, int]]:
    if data.startswith(b"\x89PNG\r\n\x1a\n") and len(data) >= 24:
        return struct.unpack(">II", data[16:24])

    if data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
        if len(data) >= 10:
            return struct.unpack("<HH", data[6:10])
        return None

    if data.startswith(b"\xff\xd8"):
        return _jpeg_dimensions(data)

    if data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        return _webp_dimensions(data)

    return None


def _jpeg_dimensions(data: bytes) -> Optional[tuple[int, int]]:
    i = 2
    while i + 9 < len(data):
        if data[i] != 0xFF:
            i += 1
            continue
        marker = data[i + 1]
        i += 2
        if marker in {0xD8, 0xD9}:
            continue
        if i + 2 > len(data):
            return None
        size = struct.unpack(">H", data[i:i + 2])[0]
        if size < 2 or i + size > len(data):
            return None
        if marker in {
            0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7,
            0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF,
        }:
            if i + 7 > len(data):
                return None
            height = struct.unpack(">H", data[i + 3:i + 5])[0]
            width = struct.unpack(">H", data[i + 5:i + 7])[0]
            return width, height
        i += size
    return None


def _webp_dimensions(data: bytes) -> Optional[tuple[int, int]]:
    chunk = data[12:16]
    if chunk == b"VP8X" and len(data) >= 30:
        width = 1 + int.from_bytes(data[24:27], "little")
        height = 1 + int.from_bytes(data[27:30], "little")
        return width, height
    if chunk == b"VP8 " and len(data) >= 30:
        start = data.find(b"\x9d\x01\x2a")
        if start >= 0 and start + 7 <= len(data):
            width = struct.unpack("<H", data[start + 3:start + 5])[0] & 0x3FFF
            height = struct.unpack("<H", data[start + 5:start + 7])[0] & 0x3FFF
            return width, height
    if chunk == b"VP8L" and len(data) >= 25:
        b0, b1, b2, b3 = data[21], data[22], data[23], data[24]
        width = 1 + (((b1 & 0x3F) << 8) | b0)
        height = 1 + (((b3 & 0x0F) << 10) | (b2 << 2) | ((b1 & 0xC0) >> 6))
        return width, height
    return None
