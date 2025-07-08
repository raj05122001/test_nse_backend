import asyncio
import os
import tempfile
from typing import Set
from datetime import datetime

from services.sftp_client import SFTPClient
from services.data_ingest import save_to_db
from services.broadcaster import publish_data
from utils.parser import parse_snapshot
from config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

async def start_sftp_watcher() -> None:
    """
    Periodically poll the configured SFTP path for new snapshot files,
    download, parse, save to DB, and broadcast over WebSocket.
    """
    sftp = SFTPClient()
    processed: Set[str] = set()

    # Ensure initial connection
    try:
        await asyncio.to_thread(sftp.connect)
        logger.info("SFTP watcher started successfully")
    except Exception as e:
        logger.error(f"Initial SFTP connection failed: {e}", exc_info=True)
        return

    while True:
        try:
            # Generate today's path dynamically
            today = datetime.now().strftime("%B%d%Y")  # July082025
            today_path = f"{settings.SFTP_REMOTE_PATH}/DATA/{today}"
            
            logger.info(f"Checking for files in {today_path}")
            
            try:
                # Get files from today's directory
                remote_files = await asyncio.to_thread(
                    sftp.list_files, today_path
                )
                logger.info(f"Found {len(remote_files)} files in today's directory")
                
            except Exception as e:
                logger.warning(f"Could not access today's directory {today_path}: {e}")
                # Try yesterday's directory as fallback
                from datetime import timedelta
                yesterday = (datetime.now() - timedelta(days=1)).strftime("%B%d%Y")
                yesterday_path = f"{settings.SFTP_REMOTE_PATH}/DATA/{yesterday}"
                try:
                    remote_files = await asyncio.to_thread(
                        sftp.list_files, yesterday_path
                    )
                    logger.info(f"Using yesterday's directory: {yesterday_path} with {len(remote_files)} files")
                except Exception as e2:
                    logger.error(f"Could not access yesterday's directory either: {e2}")
                    remote_files = []

            # Process each file
            processed_count = 0
            for remote_path in remote_files:
                if remote_path in processed:
                    logger.debug(f"Skipping already processed file: {os.path.basename(remote_path)}")
                    continue

                lower = remote_path.lower()
                if not lower.endswith((".mkt.gz", ".ind.gz", ".ca2.gz")):
                    logger.debug(f"Skipping non-market file: {os.path.basename(remote_path)}")
                    continue

                try:
                    logger.info(f"🔄 Processing new file: {os.path.basename(remote_path)}")
                    
                    # Download file
                    data = await asyncio.to_thread(sftp.download_file, remote_path)
                    logger.info(f"📥 Downloaded {len(data)} bytes from {os.path.basename(remote_path)}")

                    # Write to temp file
                    filename = os.path.basename(remote_path)
                    with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{filename}") as tmp:
                        tmp.write(data)
                        tmp_path = tmp.name

                    # Parse records
                    logger.info(f"📊 Parsing file: {filename}")
                    records = parse_snapshot(tmp_path)
                    logger.info(f"✅ Parsed {len(records)} records from {filename}")

                    if len(records) > 0:
                        # Persist to DB
                        logger.info(f"💾 Saving {len(records)} records to database")
                        lower_filename = filename.lower()
                        if lower_filename.endswith(".mkt.gz"):
                            file_type = "mkt"
                        elif lower_filename.endswith(".ind.gz"):
                            file_type = "ind"  
                        elif lower_filename.endswith(".ca2.gz"):
                            file_type = "ca2"
                        else:
                            file_type = "mkt"  # default

                        await save_to_db(records, file_type)

                        # Broadcast to WebSocket clients
                        logger.info(f"📡 Broadcasting {len(records)} records to WebSocket clients")
                        await publish_data(records)

                        logger.info(f"🎉 Successfully processed {filename} ({len(records)} records)")
                    else:
                        logger.warning(f"⚠️ No records found in {filename}")
                    
                    processed.add(remote_path)
                    processed_count += 1

                except Exception as e:
                    logger.error(f"❌ Error processing {remote_path}: {e}", exc_info=True)
                finally:
                    # Clean up temp file
                    try:
                        if 'tmp_path' in locals():
                            os.remove(tmp_path)
                    except Exception:
                        pass

            logger.info(f"🔄 Processing cycle complete. Processed {processed_count} new files. Total processed files: {len(processed)}")

        except Exception as e:
            logger.error("❌ Error in SFTP watcher cycle", exc_info=True)

        # Sleep before next poll
        logger.info(f"😴 Sleeping for {settings.POLL_INTERVAL_SECONDS} seconds")
        await asyncio.sleep(settings.POLL_INTERVAL_SECONDS)