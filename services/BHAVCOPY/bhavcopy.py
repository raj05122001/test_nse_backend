# services/BHAVCOPY/BhavCOPY.py

import asyncio
from datetime import date, timedelta
from pathlib import Path

from utils.logger import get_logger
from services.sftp_client import SFTPClient
from config import settings

logger = get_logger(__name__)

def get_previous_business_day(ref: date = None) -> date:
    """
    Return the most recent business day before ref (defaults to today),
    rolling back through Saturday/Sunday.
    """
    if ref is None:
        ref = date.today()
    prev = ref - timedelta(days=1)
    while prev.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        prev -= timedelta(days=1)
    return prev

class BhavcopyDownloader:
    """
    Download the NSE bhavcopy file for the previous business day.
    """
    def __init__(self):
        self.client = SFTPClient()

    def download_latest_bhavcopy(self) -> tuple[bytes, str]:
        """
        Determine the right date (yesterday or last Friday),
        construct the remote path, download the file, and return
        its bytes and filename.
        """
        biz_date  = get_previous_business_day()
        folder    = biz_date.strftime("%B%d%Y")   # e.g. "July072025"
        file_date = biz_date.strftime("%d%m%Y")   # e.g. "07072025"
        filename  = f"CMBhavcopy_{file_date}.txt"

        # Build remote path WITHOUT duplicating /CM30
        remote_root = settings.SFTP_REMOTE_PATH.rstrip("/")            # "/CM30"
        remote_path = f"{remote_root}/BHAVCOPY/{folder}/{filename}"    # "/CM30/BHAVCOPY/July072025/CMBhavcopy_07072025.txt"

        return self.download_file(remote_path), filename

    def download_file(self, remote_path: str) -> bytes:
        """
        Delegate to the SFTPClient wrapper's download_file method
        after ensuring the connection is open.
        """
        self.client.connect()
        try:
            logger.info(f"Downloading SFTP file: {remote_path}")
            data = self.client.download_file(remote_path)
            logger.debug(f"Downloaded {len(data)} bytes from {remote_path}")
            return data
        except Exception as e:
            logger.error(f"Error downloading {remote_path}: {e}", exc_info=True)
            raise

async def start_sftp_watcher() -> None:
    """
    One-off task: fetch the latest bhavcopy and save it locally
    under a `data/` folder next to this script.
    """
    downloader = BhavcopyDownloader()

    # Ensure we can connect
    try:
        await asyncio.to_thread(downloader.client.connect)
        logger.info("Connected to SFTP server successfully")
    except Exception as e:
        logger.error(f"Could not connect to SFTP server: {e}", exc_info=True)
        return

    try:
        data, filename = await asyncio.to_thread(downloader.download_latest_bhavcopy)

        # Save into ./data/ next to this file
        base_dir = Path(__file__).resolve().parent
        data_dir = base_dir / "data"
        data_dir.mkdir(parents=True, exist_ok=True)

        local_path = data_dir / filename
        with open(local_path, "wb") as f:
            f.write(data)

        logger.info(f"Saved bhavcopy to {local_path}")

    except Exception as e:
        logger.error(f"Failed to download and save bhavcopy: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(start_sftp_watcher())
