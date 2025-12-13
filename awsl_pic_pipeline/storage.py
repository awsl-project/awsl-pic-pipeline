import json
import logging
import time
from typing import List, Optional

import httpx
from pydantic import BaseModel

from .config import settings
from .models.pydantic_models import Blob, Blobs, BlobGroup, UploadGroup

_logger = logging.getLogger(__name__)
_client: httpx.Client = httpx.Client(timeout=60)


class TelegramFile(BaseModel):
    file_id: str
    width: Optional[int] = None
    height: Optional[int] = None


BATCH_SIZE: int = 6
MAX_RETRIES: int = 10
RETRY_DELAY: float = 5.0


def build_telegram_url(file_id: str) -> str:
    """Build the final URL for accessing a file via awsl-telegram-storage."""
    if not settings.awsl_storage_url:
        raise ValueError("awsl_storage_url must be configured")
    return f"{settings.awsl_storage_url.rstrip('/')}/file/{file_id}"


def get_largest_file(files: List[TelegramFile]) -> Optional[TelegramFile]:
    """Get the largest file from a list of photo sizes."""
    if not files:
        return None
    files_with_size: List[TelegramFile] = [f for f in files if f.width and f.height]
    if files_with_size:
        return max(files_with_size, key=lambda f: f.width * f.height)
    return files[-1]


def get_first_file_over_800(files: List[TelegramFile]) -> Optional[TelegramFile]:
    """Get the first file that exceeds 800 pixels in width or height."""
    if not files:
        return None
    for f in files:
        if f.width and f.width > 800:
            return f
        if f.height and f.height > 800:
            return f
    return files[-1]


def _files_to_blobs(files: List[TelegramFile]) -> Blobs:
    """Convert TelegramFile list to Blobs with original and large."""
    original_file: Optional[TelegramFile] = get_largest_file(files)
    large_file: Optional[TelegramFile] = get_first_file_over_800(files)

    blobs_dict: dict[str, Blob] = {}
    if original_file:
        blobs_dict["original"] = Blob(
            url=build_telegram_url(original_file.file_id),
            file_id=original_file.file_id,
            width=original_file.width,
            height=original_file.height,
        )
    if large_file:
        blobs_dict["large"] = Blob(
            url=build_telegram_url(large_file.file_id),
            file_id=large_file.file_id,
            width=large_file.width,
            height=large_file.height,
        )
    return Blobs(blobs=blobs_dict)


def upload_media_group(group: UploadGroup) -> Optional[List[BlobGroup]]:
    """
    Upload photos to Telegram via awsl-telegram-storage service.
    Automatically splits into batches of 9 if more than 9 URLs.

    Args:
        group: UploadGroup containing blob_groups and caption

    Returns:
        List of BlobGroup with telegram file info, or None on failure
    """
    if not settings.awsl_storage_url or not settings.awsl_storage_api_token:
        raise ValueError("awsl_storage_url and awsl_storage_api_token must be configured")

    if not group.blob_groups:
        raise ValueError("At least 1 BlobGroup required")

    urls: List[str] = [list(bg.blobs.blobs.values())[0].url for bg in group.blob_groups]
    all_files: List[List[TelegramFile]] = []
    batches: List[List[str]] = [urls[i:i + BATCH_SIZE] for i in range(0, len(urls), BATCH_SIZE)]

    for batch_urls in batches:
        files: Optional[List[List[TelegramFile]]] = _upload_batch(batch_urls, group.caption)
        if files is None:
            return None
        all_files.extend(files)

    result: List[BlobGroup] = []
    for blob_group, files in zip(group.blob_groups, all_files):
        result.append(BlobGroup(
            id=blob_group.id,
            awsl_id=blob_group.awsl_id,
            blobs=_files_to_blobs(files),
        ))

    return result


def _upload_batch(urls: List[str], caption: Optional[str] = None) -> Optional[List[List[TelegramFile]]]:
    """Upload a single batch of URLs (max 9) with retry."""
    api_url: str = f"{settings.awsl_storage_url.rstrip('/')}/api/upload/group"

    payload: dict = {"urls": urls}
    if caption:
        payload["caption"] = caption

    headers: dict[str, str] = {
        "X-Api-Token": settings.awsl_storage_api_token,
        "Content-Type": "application/json",
    }

    last_error: Optional[str] = None
    for attempt in range(MAX_RETRIES):
        try:
            response: httpx.Response = _client.post(api_url, json=payload, headers=headers)
            data: dict = response.json()

            if not data.get("success"):
                last_error = data.get("error", "Unknown error")
                if "WEBPAGE_MEDIA_EMPTY" in last_error:
                    _logger.warning("WEBPAGE_MEDIA_EMPTY detected, skipping: %s", last_error)
                    return None
                _logger.warning("Upload failed (attempt %d/%d): %s", attempt + 1, MAX_RETRIES, last_error)
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue

            files: List[List[TelegramFile]] = [
                [TelegramFile(file_id=f["file_id"], width=f.get("width"), height=f.get("height"))
                 for f in group]
                for group in data.get("files", [])
            ]

            _logger.info("Uploaded %d images to Telegram", len(files))
            return files

        except httpx.HTTPError as e:
            last_error = str(e)
            _logger.warning("Request failed (attempt %d/%d): %s", attempt + 1, MAX_RETRIES, last_error)
            time.sleep(RETRY_DELAY * (attempt + 1))
        except json.JSONDecodeError as e:
            last_error = f"Invalid JSON response: {e}"
            _logger.warning("JSON parse failed (attempt %d/%d): %s", attempt + 1, MAX_RETRIES, last_error)
            time.sleep(RETRY_DELAY * (attempt + 1))

    _logger.error("Upload failed after %d attempts: %s", MAX_RETRIES, last_error)
    return None
