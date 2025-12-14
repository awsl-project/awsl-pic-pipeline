import json
import logging
import time
from typing import List, Optional

import httpx
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .config import settings
from .models.models import Pic, AwslBlobV2, Mblog, AwslProducer
from .models.pydantic_models import Blob, Blobs, BlobGroup, UploadGroup
from .storage import upload_media_group, UploadResult

_logger = logging.getLogger(__name__)
_client: httpx.Client = httpx.Client(timeout=10)
engine = create_engine(settings.db_url, pool_size=100)
DBSession = sessionmaker(bind=engine)
PIC_TYPES: List[str] = ["original", "large"]
UPLOAD_DELAY: float = 3.0


def delete_pic(blob_group: BlobGroup) -> None:
    """Mark pic as deleted."""
    if not settings.enable_delete:
        _logger.info("Delete disabled, skipping pic_id=%s", blob_group.id)
        return
    session = DBSession()
    try:
        for picobj in session.query(Pic).filter(Pic.pic_id == blob_group.id).all():
            picobj.deleted = True
            picobj.cleaned = True
        session.commit()
    finally:
        session.close()


def delete_upload_group(group: UploadGroup) -> None:
    """Mark all pics in upload group as deleted."""
    if not settings.enable_delete:
        _logger.info("Delete disabled, skipping awsl_id=%s", group.awsl_id)
        return
    for blob_group in group.blob_groups:
        delete_pic(blob_group)
    _logger.info("Deleted all pics for awsl_id=%s", group.awsl_id)


def get_all_pic_to_upload() -> List[UploadGroup]:
    """Get pics grouped by awsl_id with caption."""
    session = DBSession()
    try:
        # First, get the top N awsl_id values to process
        # This ensures we limit by the number of groups, not total rows
        # Also join Mblog to ensure awsl_id has corresponding mblog record
        awsl_ids_subquery = session.query(Pic.awsl_id).outerjoin(
            AwslBlobV2, Pic.pic_id == AwslBlobV2.pic_id
        ).join(
            Mblog, Pic.awsl_id == Mblog.id
        ).filter(
            AwslBlobV2.pic_id.is_(None)
        ).filter(
            Pic.deleted.isnot(True)
        ).group_by(Pic.awsl_id).order_by(Pic.awsl_id.desc()).limit(settings.migration_limit).subquery()

        # Then get all pics belonging to those awsl_ids
        pics = session.query(Pic, Mblog, AwslProducer).filter(
            Pic.awsl_id.in_(session.query(awsl_ids_subquery.c.awsl_id))
        ).outerjoin(
            AwslBlobV2, Pic.pic_id == AwslBlobV2.pic_id
        ).join(
            Mblog, Pic.awsl_id == Mblog.id
        ).outerjoin(
            AwslProducer, Mblog.uid == AwslProducer.uid
        ).filter(
            AwslBlobV2.pic_id.is_(None)
        ).filter(
            Pic.deleted.isnot(True)
        ).order_by(Pic.awsl_id.desc()).all()

        awsl_groups: dict[str, UploadGroup] = {}
        filtered_stats: dict[str, int] = {
            "json_error": 0,
            "no_valid_type": 0,
            "invalid_url": 0,
        }
        for pic, mblog, producer in pics:
            try:
                pic_info: dict = json.loads(pic.pic_info) if pic.pic_info else {}
            except json.JSONDecodeError:
                filtered_stats["json_error"] += 1
                continue

            found_valid_pic = False
            for pic_type in PIC_TYPES:
                if pic_type not in pic_info or not isinstance(pic_info[pic_type], dict):
                    continue
                pic_data: dict = pic_info[pic_type]
                url: Optional[str] = pic_data.get("url")
                if not url or ".gif" in url:
                    filtered_stats["invalid_url"] += 1
                    continue

                found_valid_pic = True
                blob_group: BlobGroup = BlobGroup(
                    id=pic.pic_id,
                    awsl_id=pic.awsl_id,
                    blobs=Blobs(blobs={
                        pic_type: Blob(
                            url=url,
                            width=pic_data.get("width"),
                            height=pic_data.get("height"),
                        )
                    })
                )

                if pic.awsl_id not in awsl_groups:
                    wb_url: str = f"https://weibo.com/{mblog.uid}/{mblog.mblogid}" if mblog else ""
                    screen_name: str = ""
                    if mblog and mblog.re_user:
                        try:
                            re_user: dict = json.loads(mblog.re_user)
                            screen_name = re_user.get("screen_name", "")
                        except json.JSONDecodeError:
                            pass
                    if not screen_name and producer:
                        screen_name = producer.name or ""
                    awsl_groups[pic.awsl_id] = UploadGroup(
                        awsl_id=pic.awsl_id,
                        blob_groups=[],
                        caption=f"#{screen_name} {wb_url}" if screen_name else wb_url,
                    )
                awsl_groups[pic.awsl_id].blob_groups.append(blob_group)
                break

            if not found_valid_pic:
                filtered_stats["no_valid_type"] += 1

        res: List[UploadGroup] = list(awsl_groups.values())
        _logger.info("get_all_pic_to_upload: %d groups (filtered_pics: invalid_url=%d, no_type=%d, json_err=%d)",
                    len(res), filtered_stats["invalid_url"], filtered_stats["no_valid_type"], filtered_stats["json_error"])
    finally:
        session.close()
    return res


def save_telegram_files(blob_groups: List[BlobGroup]) -> None:
    """Save uploaded file info to database."""
    session = DBSession()
    try:
        for blob_group in blob_groups:
            record: AwslBlobV2 = AwslBlobV2(
                awsl_id=blob_group.awsl_id,
                pic_id=blob_group.id,
                pic_info=blob_group.blobs.model_dump_json(),
            )
            session.add(record)
            _logger.info("Saved: pic_id=%s", blob_group.id)
        session.commit()
    finally:
        session.close()


def upload_group_to_telegram(group: UploadGroup) -> bool:
    """Upload a group of pics to Telegram, handling partial success."""
    result: UploadResult = upload_media_group(group)

    # Save successfully uploaded pics
    if result.succeeded:
        save_telegram_files(result.succeeded)
        _logger.info("Saved %d succeeded pics for awsl_id=%s", len(result.succeeded), group.awsl_id)

    # Delete failed pics
    if result.failed:
        for blob_group in result.failed:
            delete_pic(blob_group)
        _logger.warning("Deleted %d failed pics for awsl_id=%s", len(result.failed), group.awsl_id)

    # Return True if at least some pics succeeded
    if result.succeeded:
        return True
    else:
        _logger.error("All pics failed for awsl_id=%s", group.awsl_id)
        return False


def migration() -> None:
    """Main migration function."""
    groups: List[UploadGroup] = get_all_pic_to_upload()
    total_groups: int = len(groups)
    success_count: int = 0
    fail_count: int = 0

    _logger.info("Starting migration: %d groups to process", total_groups)

    for idx, group in enumerate(groups, 1):
        _logger.info("Processing group %d/%d (awsl_id=%s)", idx, total_groups, group.awsl_id)
        try:
            if upload_group_to_telegram(group):
                success_count += 1
            else:
                fail_count += 1
        except Exception as e:
            _logger.exception("Error uploading group %d/%d (awsl_id=%s): %s", idx, total_groups, group.awsl_id, e)
            delete_upload_group(group)
            fail_count += 1
        time.sleep(UPLOAD_DELAY)

    _logger.info("Migration completed: success=%d, fail=%d, total=%d", success_count, fail_count, total_groups)
