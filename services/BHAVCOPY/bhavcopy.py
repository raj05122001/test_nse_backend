# services/BHAVCOPY/BhavCOPY.py

import asyncio
import json
import re
from datetime import date, timedelta, datetime
from decimal import Decimal
from pathlib import Path

from utils.logger import get_logger
from services.sftp_client import SFTPClient
from config import settings

from sqlalchemy.dialects.postgresql import insert as pg_insert
from db.connection import AsyncSessionLocal
from db.models import CMStockHistorical # your Demo(Base) model

logger = get_logger(__name__)

# After symbol & series, these are the columns in each line:
HEADERS = [
    "trade_high_price",
    "trade_low_price",
    "opening_price",
    "closing_price",
    "previous_close_price",
    "total_traded_quantity",
    "total_traded_value",
]

def get_previous_business_day(ref: date = None) -> date:
    """
    Return the most recent business day before ref (defaults to today),
    rolling back through Saturday/Sunday.
    """
    if ref is None:
        ref = date.today()
    prev = ref - timedelta(days=1)
    while prev.weekday() >= 5:  # Saturday=5, Sunday=6
        prev -= timedelta(days=1)
    return prev

def extract_date_from_filename(fname: str) -> date:
    """
    Given 'CMBhavcopy_DDMMYYYY.txt', return a date object.
    """
    m = re.match(r"CMBhavcopy_(\d{2})(\d{2})(\d{4})\.txt$", fname)
    if not m:
        raise ValueError(f"Filename {fname!r} not in expected format")
    dd, mm, yyyy = m.groups()
    return date(int(yyyy), int(mm), int(dd))

def parse_bhavcopy_to_json(text: str, fname: str) -> dict:
    """
    Given the raw bhavcopy text and its filename, return:
      {
        "business_date": "YYYY-MM-DD",
        "records": [ {symbol, series, trade_high_price…}, … ]
      }
    """
    biz_date = extract_date_from_filename(fname)
    biz_date_str = biz_date.isoformat()

    records = []
    for line in text.splitlines():
        parts = line.strip().split()
        if not parts:
            continue

        # handle missing-series rows (8 parts) vs full (9 parts)
        if len(parts) == 8:
            symbol, *vals = parts
            series = ""
        elif len(parts) == 9:
            symbol, series, *vals = parts
        else:
            # malformed; skip
            continue

        rec = {"symbol": symbol, "series": series}
        for key, val in zip(HEADERS, vals):
            if key == "total_traded_quantity":
                rec[key] = int(val)
            else:
                try:
                    rec[key] = float(val)
                except ValueError:
                    rec[key] = val

        records.append(rec)

    return {"business_date": biz_date_str, "records": records}


class BhavcopyDownloader:
    """
    Download the NSE bhavcopy file for the previous business day.
    """
    def __init__(self):
        self.client = SFTPClient()

    def download_latest_bhavcopy(self) -> tuple[bytes, str]:
        """
        Determine the right date, construct the remote path,
        download the file, and return its bytes and filename.
        """
        biz_date  = get_previous_business_day()
        folder    = biz_date.strftime("%B%d%Y")    # e.g. "July072025"
        file_date = biz_date.strftime("%d%m%Y")    # e.g. "07072025"
        filename  = f"CMBhavcopy_{file_date}.txt"

        remote_root = settings.SFTP_REMOTE_PATH.rstrip("/")
        remote_path = f"{remote_root}/BHAVCOPY/{folder}/{filename}"

        return self.download_file(remote_path), filename

    def download_file(self, remote_path: str) -> bytes:
        """
        Connect (if needed) and download via SFTPClient.
        """
        self.client.connect()
        try:
            logger.info(f"Downloading bhavcopy: {remote_path}")
            data = self.client.download_file(remote_path)
            logger.debug(f"Downloaded {len(data)} bytes")
            return data
        except Exception as e:
            logger.error(f"Download failed: {e}", exc_info=True)
            raise


async def start_sftp_bhavcopy() -> None:
    """
    One-off task: fetch the latest bhavcopy, parse it, save JSON, and insert into DB.
    """
    downloader = BhavcopyDownloader()

    # 1) Connect
    try:
        await asyncio.to_thread(downloader.client.connect)
        logger.info("SFTP connected")
    except Exception as e:
        logger.error(f"SFTP connection error: {e}", exc_info=True)
        return

    # 2) Download
    try:
        raw_bytes, filename = await asyncio.to_thread(downloader.download_latest_bhavcopy)
    except Exception as e:
        logger.error(f"Failed to download bhavcopy: {e}", exc_info=True)
        return

    # 3) Parse to JSON‐dict
    raw_text = raw_bytes.decode("utf-8")
    payload  = parse_bhavcopy_to_json(raw_text, filename)

    # 4) (Optional) write JSON file for audit
    base_dir = Path(__file__).resolve().parent
    data_dir = base_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    json_path = data_dir / filename.replace(".txt", ".json")
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info(f"Wrote JSON with {len(payload['records'])} records to {json_path}")

    # 5) Build insert‐dicts and bulk‐insert into Demo with ON CONFLICT DO NOTHING
    biz_ts = int(datetime.fromisoformat(payload["business_date"]).timestamp())
    insert_rows = []
    for rec in payload["records"]:
        insert_rows.append({
            "symbol":      rec["symbol"],
            "timestamp":   biz_ts,
            "open_price":  Decimal(rec["opening_price"]),
            "high_price":  Decimal(rec["trade_high_price"]),
            "low_price":   Decimal(rec["trade_low_price"]),
            "close_price": Decimal(rec["closing_price"]),
            "volume":      rec["total_traded_quantity"],
            "series":      rec["series"],
        })

    if insert_rows:
        async with AsyncSessionLocal() as session:
            stmt = pg_insert(CMStockHistorical).values(insert_rows)
            stmt = stmt.on_conflict_do_nothing(index_elements=["symbol", "timestamp"])
            await session.execute(stmt)
            await session.commit()
        logger.info(f"Attempted to insert {len(insert_rows)} rows; duplicates were ignored.")

if __name__ == "__main__":
    asyncio.run(start_sftp_bhavcopy())
