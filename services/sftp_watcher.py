import asyncio
import os
import tempfile
from typing import Set

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
    except Exception:
        logger.error("Initial SFTP connection failed", exc_info=True)
        return

    while True:
        try:
            remote_files = await asyncio.to_thread(
                sftp.list_files, settings.SFTP_REMOTE_PATH
            )
            for remote_path in remote_files:
                if remote_path in processed:
                    continue

                lower = remote_path.lower()
                if not lower.endswith((".mkt.gz", ".ind.gz", ".ca2.gz")):
                    continue

                try:
                    logger.info(f"Processing new file: {remote_path}")
                    data = await asyncio.to_thread(sftp.download_file, remote_path)

                    # Write to a temp file so parser can read it
                    filename = os.path.basename(remote_path)
                    with tempfile.NamedTemporaryFile(delete=False, suffix=filename) as tmp:
                        tmp.write(data)
                        tmp_path = tmp.name

                    # Parse records
                    records = parse_snapshot(tmp_path)

                    # Persist to DB
                    await save_to_db(records)

                    # Broadcast to WebSocket clients
                    await publish_data(records)

                    logger.info(
                        f"Completed processing {remote_path} ({len(records)} records)"
                    )

                    processed.add(remote_path)
                except Exception:
                    logger.error(f"Error processing {remote_path}", exc_info=True)
                finally:
                    # Clean up temp file
                    try:
                        os.remove(tmp_path)
                    except Exception:
                        pass

        except Exception:
            logger.error("Error listing or iterating SFTP files", exc_info=True)

        # Sleep before next poll
        await asyncio.sleep(settings.POLL_INTERVAL_SECONDS)
