import asyncio
import os
import tempfile
from datetime import datetime, timedelta
from typing import Set

from services.sftp_client import SFTPClient
from services.data_ingest import save_to_db
from services.broadcaster import publish_data
from utils.parser import parse_snapshot
from config import settings
from utils.logger import get_logger
from sqlalchemy.future import select
from db.connection import AsyncSessionLocal, engine
from db.models import ProcessedFile

logger = get_logger(__name__)

async def init_db():
    # Create tables if they don't exist
    async with engine.begin() as conn:
        await conn.run_sync(lambda sync_conn: ProcessedFile.metadata.create_all(bind=sync_conn))

async def load_processed() -> Set[str]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(ProcessedFile.remote_path))
        return set(result.scalars().all())

async def mark_processed(remote_path: str) -> None:
    async with AsyncSessionLocal() as session:
        session.add(ProcessedFile(remote_path=remote_path))
        try:
            await session.commit()
        except Exception:
            await session.rollback()

async def start_sftp_watcher() -> None:
    # Ensure database is ready
    await init_db()

    sftp = SFTPClient()
    processed: Set[str] = await load_processed()

    try:
        await asyncio.to_thread(sftp.connect)
        logger.info("SFTP watcher started successfully")
    except Exception as e:
        logger.error(f"Initial SFTP connection failed: {e}", exc_info=True)
        return

    while True:
        # Build today's and fallback path
        today = datetime.now().strftime("%B%d%Y")  # e.g. July112025
        base_path = f"{settings.SFTP_REMOTE_PATH}/DATA/{today}"
        logger.info(f"Checking for files in {base_path}")

        try:
            remote_files = await asyncio.to_thread(sftp.list_files, base_path)
        except Exception:
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%B%d%Y")
            fallback = f"{settings.SFTP_REMOTE_PATH}/DATA/{yesterday}"
            try:
                remote_files = await asyncio.to_thread(sftp.list_files, fallback)
                logger.info(f"Using fallback directory: {fallback}")
            except Exception as e:
                logger.error(f"Could not access any directory: {e}")
                remote_files = []

        processed_count = 0

        for remote_path in remote_files:
            if remote_path in processed:
                logger.debug(f"Skipping already processed file: {remote_path}")
                continue

            lower = remote_path.lower()
            # Only interested in mkt, ind, ca2
            if not lower.endswith(('.mkt.gz', '.ind.gz', '.ca2.gz')):
                # mark as processed so it's never retried
                await mark_processed(remote_path)
                processed.add(remote_path)
                continue

            filename = os.path.basename(remote_path)
            try:
                logger.info(f"🔄 Processing new file: {filename}")
                data = await asyncio.to_thread(sftp.download_file, remote_path)
                logger.info(f"📥 Downloaded {len(data)} bytes from {filename}")

                # Write to temp file
                with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{filename}") as tmp:
                    tmp.write(data)
                    tmp_path = tmp.name

                # Parse
                records = parse_snapshot(tmp_path)
                os.remove(tmp_path)
                logger.info(f"✅ Parsed {len(records)} records from {filename}")

                # Always mark processed to avoid re-download
                await mark_processed(remote_path)
                processed.add(remote_path)

                if records:
                    # Determine file_type
                    if lower.endswith('.mkt.gz'):
                        file_type = 'mkt'
                    elif lower.endswith('.ind.gz'):
                        file_type = 'ind'
                    else:
                        file_type = 'ca2'

                    logger.info(f"💾 Saving {len(records)} records to database as {file_type}")
                    await save_to_db(records, file_type)

                    logger.info(f"📡 Broadcasting {len(records)} records to WebSocket clients")
                    await publish_data(records)

                    logger.info(f"🎉 Successfully processed {filename} with {len(records)} records")
                else:
                    logger.warning(f"⚠️ No records found in {filename} — marked processed")

                processed_count += 1

            except Exception as e:
                logger.error(f"❌ Error processing {filename}: {e}", exc_info=True)

        logger.info(f"🔄 Cycle complete: {processed_count} new files processed. Total processed: {len(processed)}")
        logger.info(f"😴 Sleeping for {settings.POLL_INTERVAL_SECONDS} seconds")
        await asyncio.sleep(settings.POLL_INTERVAL_SECONDS)
