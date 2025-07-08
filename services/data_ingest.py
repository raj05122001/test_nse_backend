import os
import tempfile
from typing import List, Dict, Any

from sqlalchemy import insert
from sqlalchemy.exc import SQLAlchemyError

from db.connection import get_db
from db.models import CMSnapshot
from utils.logger import get_logger
from utils.parser import parse_snapshot

logger = get_logger(__name__)


async def save_to_db(records: List[Dict[str, Any]]) -> None:
    """
    Bulk‐insert snapshot records into the CMSnapshot table.
    """
    if not records:
        logger.debug("No records to save")
        return

    async with get_db() as session:
        try:
            # Use low-level INSERT for maximum performance
            await session.execute(insert(CMSnapshot), records)
            await session.commit()
            logger.info(f"Saved {len(records)} records to database")
        except SQLAlchemyError as e:
            await session.rollback()
            logger.error(f"DB error while saving records: {e}", exc_info=True)
            raise


async def ingest_file(path: str) -> None:
    """
    Parse a snapshot .gz file at `path` and persist its records.
    """
    logger.info(f"Ingesting file: {path}")
    try:
        records = parse_snapshot(path)
        await save_to_db(records)
        logger.info(f"Ingested {len(records)} records from {os.path.basename(path)}")
    except Exception:
        logger.error(f"Failed ingesting {path}", exc_info=True)
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
