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
INDIVIDUAL_RETRY_DELAY: float = 3.0  # Delay between individual image retries


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


class UploadResult(BaseModel):
    """Result of upload operation with success and failed blob groups."""
    succeeded: List[BlobGroup]
    failed: List[BlobGroup]


class BatchUploadResult(BaseModel):
    """Result of batch upload with files and error type."""
    files: Optional[List[List[TelegramFile]]]
    is_webpage_media_empty: bool = False


def upload_media_group(group: UploadGroup) -> UploadResult:
    """
    Upload photos to Telegram via awsl-telegram-storage service.
    Automatically splits into batches of 6 if more than 6 URLs.
    On WEBPAGE_MEDIA_EMPTY error, retries each image individually.

    Args:
        group: UploadGroup containing blob_groups and caption

    Returns:
        UploadResult with succeeded and failed blob groups
    """
    if not settings.awsl_storage_url or not settings.awsl_storage_api_token:
        raise ValueError("awsl_storage_url and awsl_storage_api_token must be configured")

    if not group.blob_groups:
        raise ValueError("At least 1 BlobGroup required")

    urls: List[str] = [list(bg.blobs.blobs.values())[0].url for bg in group.blob_groups]
    all_files: List[Optional[List[TelegramFile]]] = []
    batches: List[List[str]] = [urls[i:i + BATCH_SIZE] for i in range(0, len(urls), BATCH_SIZE)]

    for batch_urls in batches:
        batch_result: BatchUploadResult = _upload_batch(batch_urls, group.caption)

        if batch_result.files is not None:
            # Batch upload succeeded
            all_files.extend(batch_result.files)
        elif batch_result.is_webpage_media_empty:
            # WEBPAGE_MEDIA_EMPTY detected, retry each image individually
            _logger.info("WEBPAGE_MEDIA_EMPTY detected, retrying batch of %d images individually", len(batch_urls))
            for i, url in enumerate(batch_urls):
                single_result: BatchUploadResult = _upload_batch([url], group.caption)
                if single_result.files and len(single_result.files) > 0:
                    all_files.append(single_result.files[0])
                    _logger.info("Successfully uploaded image %d/%d", i + 1, len(batch_urls))
                else:
                    all_files.append(None)
                    _logger.warning("Failed to upload image %d/%d: %s", i + 1, len(batch_urls), url)
                if i < len(batch_urls) - 1:  # Don't delay after last image
                    time.sleep(INDIVIDUAL_RETRY_DELAY)
        else:
            # Other error, mark all as failed
            _logger.error("Batch upload failed with non-WEBPAGE_MEDIA_EMPTY error, marking all as failed")
            all_files.extend([None] * len(batch_urls))

    succeeded: List[BlobGroup] = []
    failed: List[BlobGroup] = []

    for blob_group, files in zip(group.blob_groups, all_files):
        if files:
            succeeded.append(BlobGroup(
                id=blob_group.id,
                awsl_id=blob_group.awsl_id,
                blobs=_files_to_blobs(files),
            ))
        else:
            failed.append(blob_group)
            _logger.warning("Failed blob_group: pic_id=%s", blob_group.id)

    _logger.info("Upload result: %d succeeded, %d failed", len(succeeded), len(failed))
    return UploadResult(succeeded=succeeded, failed=failed)


def _upload_batch(urls: List[str], caption: Optional[str] = None) -> BatchUploadResult:
    """Upload a single batch of URLs (max 6) with retry."""
    api_url: str = f"{settings.awsl_storage_url.rstrip('/')}/api/upload/group"

    payload: dict = {"urls": urls}
    if caption:
        payload["caption"] = caption

    headers: dict[str, str] = {
        "X-Api-Token": settings.awsl_storage_api_token,
        "Content-Type": "application/json",
    }

    last_error: Optional[str] = None
    is_webpage_media_empty: bool = False

    for attempt in range(MAX_RETRIES):
        try:
            response: httpx.Response = _client.post(api_url, json=payload, headers=headers)
            data: dict = response.json()

            if not data.get("success"):
                last_error = data.get("error", "Unknown error")
                if "WEBPAGE_MEDIA_EMPTY" in last_error:
                    _logger.warning("WEBPAGE_MEDIA_EMPTY detected: %s", last_error)
                    is_webpage_media_empty = True
                    return BatchUploadResult(files=None, is_webpage_media_empty=True)
                _logger.warning("Upload failed (attempt %d/%d): %s", attempt + 1, MAX_RETRIES, last_error)
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue

            files: List[List[TelegramFile]] = [
                [TelegramFile(file_id=f["file_id"], width=f.get("width"), height=f.get("height"))
                 for f in group]
                for group in data.get("files", [])
            ]

            _logger.info("Uploaded %d images to Telegram", len(files))
            return BatchUploadResult(files=files, is_webpage_media_empty=False)

        except httpx.HTTPError as e:
            last_error = str(e)
            _logger.warning("Request failed (attempt %d/%d): %s", attempt + 1, MAX_RETRIES, last_error)
            time.sleep(RETRY_DELAY * (attempt + 1))
        except json.JSONDecodeError as e:
            last_error = f"Invalid JSON response: {e}"
            _logger.warning("JSON parse failed (attempt %d/%d): %s", attempt + 1, MAX_RETRIES, last_error)
            time.sleep(RETRY_DELAY * (attempt + 1))

    _logger.error("Upload failed after %d attempts: %s", MAX_RETRIES, last_error)
    return BatchUploadResult(files=None, is_webpage_media_empty=is_webpage_media_empty)
