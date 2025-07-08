import os
import tempfile
from typing import List, Dict, Any

from sqlalchemy import insert
from sqlalchemy.exc import SQLAlchemyError

from db.connection import AsyncSessionLocal
from db.models import CMSnapshot, CMIndexSnapshot, CMCallAuctionSnapshot
from utils.logger import get_logger
from utils.parser import parse_snapshot

logger = get_logger(__name__)


async def save_to_db(records: List[Dict[str, Any]], file_type: str = "mkt") -> None:
    """
    Bulk‐insert snapshot records into the appropriate table based on file type.
    """
    if not records:
        logger.debug("No records to save")
        return

    # Determine the correct model based on file type
    if file_type == "mkt":
        model = CMSnapshot
        table_name = "CM Market Snapshot"
    elif file_type == "ind":
        model = CMIndexSnapshot
        table_name = "CM Index Snapshot"
    elif file_type == "ca2":
        model = CMCallAuctionSnapshot
        table_name = "CM Call Auction Snapshot"
    else:
        logger.error(f"Unknown file type: {file_type}")
        return

    async with AsyncSessionLocal() as session:
        try:
            # Use low-level INSERT for maximum performance
            await session.execute(insert(model), records)
            await session.commit()
            logger.info(f"✅ Successfully saved {len(records)} records to {table_name}")
        except SQLAlchemyError as e:
            await session.rollback()
            logger.error(f"❌ DB error while saving {table_name} records: {e}", exc_info=True)
            raise
        except Exception as e:
            await session.rollback()
            logger.error(f"❌ Unexpected error while saving {table_name} records: {e}", exc_info=True)
            raise


async def ingest_file(path: str) -> None:
    """
    Parse a snapshot .gz file at `path` and persist its records.
    """
    logger.info(f"Ingesting file: {path}")
    try:
        # Determine file type from extension
        lower_path = path.lower()
        if lower_path.endswith(".mkt.gz"):
            file_type = "mkt"
        elif lower_path.endswith(".ind.gz"):
            file_type = "ind"
        elif lower_path.endswith(".ca2.gz"):
            file_type = "ca2"
        else:
            logger.warning(f"Unknown file type for {path}")
            return

        records = parse_snapshot(path)
        await save_to_db(records, file_type)
        logger.info(f"✅ Successfully ingested {len(records)} {file_type.upper()} records from {os.path.basename(path)}")
    except Exception:
        logger.error(f"❌ Failed ingesting {path}", exc_info=True)
        raise


async def ingest_bytes(data: bytes, filename: str) -> None:
    """
    Helper for raw bytes (from SFTP). Writes `data` to a temp .gz file,
    then calls `ingest_file`.
    """
    suffix = os.path.splitext(filename)[1]  # e.g. ".mkt.gz", ".ind.gz", ".ca2.gz"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(data)
        tmp_path = tmp.name

    try:
        await ingest_file(tmp_path)
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass