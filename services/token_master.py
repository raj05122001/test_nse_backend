import asyncio
import os
import tempfile
from datetime import datetime, timedelta
from typing import Set, List, Dict, Optional

import pandas as pd
from sqlalchemy import select, update, and_, delete
from sqlalchemy.dialects.postgresql import insert

from services.sftp_client import SFTPClient
from config import settings
from utils.logger import get_logger
from utils.security_format import SecuritiesConverter
from db.connection import get_db, AsyncSessionLocal
from db.models import CMTokenMaster

logger = get_logger(__name__)

class TokenMasterProcessor:
    """Process Securities.dat files and update database"""

    def __init__(self):
        self.converter = SecuritiesConverter()
        self.sftp = SFTPClient()
        self.processed_files: Set[str] = set()

    async def save_securities_to_db(self, securities: List[Dict], update_date: str) -> int:
        """
        Save/Update securities data in database with UPSERT logic
        Returns number of records processed
        """
        if not securities:
            logger.warning("No securities data to save")
            return 0

        processed_count = 0

        try:
            async with AsyncSessionLocal() as session:
                logger.info(f"Processing {len(securities)} securities for database update...")

                batch_size = 1000
                total_batches = (len(securities) + batch_size - 1) // batch_size

                for batch_num in range(total_batches):
                    start_idx = batch_num * batch_size
                    end_idx = min(start_idx + batch_size, len(securities))
                    batch = securities[start_idx:end_idx]

                    upsert_data = []
                    for security in batch:
                        if "NSETEST" in security.get('symbol', '').upper():
                            continue

                        record_data = {
                            'token_number': security['token_number'],
                            'symbol': security['symbol'],
                            'series': security['series'],
                            'issued_capital': float(security.get('issued_capital', 0)),
                            'settlement_cycle': int(security.get('settlement_cycle', 1)),
                            'company_name': security.get('company_name', ''),
                            'permitted_to_trade': int(security.get('permitted_to_trade', 1)),
                            'data_length': int(security.get('data_length', 0)),
                            'settlement_cycle_desc': 'T+0' if security.get('settlement_cycle', 1) == 0 else 'T+1',
                            'permitted_to_trade_desc': self._get_permitted_desc(security.get('permitted_to_trade', 1)),
                            'last_updated': update_date
                        }
                        upsert_data.append(record_data)

                    if upsert_data:
                        stmt = insert(CMTokenMaster).values(upsert_data)
                        upsert_stmt = stmt.on_conflict_do_update(
                            index_elements=['token_number'],
                            set_={
                                'symbol': stmt.excluded.symbol,
                                'series': stmt.excluded.series,
                                'issued_capital': stmt.excluded.issued_capital,
                                'settlement_cycle': stmt.excluded.settlement_cycle,
                                'company_name': stmt.excluded.company_name,
                                'permitted_to_trade': stmt.excluded.permitted_to_trade,
                                'data_length': stmt.excluded.data_length,
                                'settlement_cycle_desc': stmt.excluded.settlement_cycle_desc,
                                'permitted_to_trade_desc': stmt.excluded.permitted_to_trade_desc,
                                'last_updated': stmt.excluded.last_updated
                            }
                        )
                        await session.execute(upsert_stmt)
                        processed_count += len(upsert_data)
                        logger.info(f"Batch {batch_num + 1}/{total_batches}: Processed {len(upsert_data)} records")

                await session.commit()
                logger.info(f"✅ Successfully processed {processed_count} securities in database")
                return processed_count

        except Exception as e:
            logger.error(f"❌ Error saving securities to database: {e}", exc_info=True)
            return 0

    def _get_permitted_desc(self, permitted_code: int) -> str:
        """Get description for permitted to trade code"""
        mapping = {
            0: 'Listed but not permitted to trade',
            1: 'Permitted to trade',
            2: 'BSE listed (BSE exclusive security)'
        }
        return mapping.get(permitted_code, 'Unknown')

    async def process_securities_file(self, file_path: str, file_date: str) -> bool:
        """Process a single Securities.dat file"""
        try:
            logger.info(f"🔄 Processing Securities file: {file_path}")

            df = await asyncio.to_thread(
                self.converter.convert_to_csv,
                file_path,
                file_path.replace('.dat', '.csv')
            )
            if df is None or df.empty:
                logger.warning("No data extracted from Securities file")
                return False

            securities = df.to_dict('records')
            saved_count = await self.save_securities_to_db(securities, file_date)

            if saved_count > 0:
                logger.info(f"✅ Successfully processed {saved_count} securities from {os.path.basename(file_path)}")
                return True
            else:
                logger.error(f"❌ Failed to save securities from {os.path.basename(file_path)}")
                return False

        except Exception as e:
            logger.error(f"❌ Error processing securities file {file_path}: {e}", exc_info=True)
            return False

    async def download_and_process_file(self, remote_path: str, local_dir: str, file_date: str) -> bool:
        """Download file from SFTP and process it"""
        try:
            filename = os.path.basename(remote_path)
            local_path = os.path.join(local_dir, f"{file_date}_{filename}")

            logger.info(f"📥 Downloading {remote_path} → {local_path}")
            file_data = await asyncio.to_thread(self.sftp.download_file, remote_path)

            with open(local_path, 'wb') as f:
                f.write(file_data)
            logger.info(f"✅ Downloaded {len(file_data):,} bytes")

            success = await self.process_securities_file(local_path, file_date)

            try:
                os.unlink(local_path)
            except:
                pass

            return success

        except Exception as e:
            logger.error(f"❌ Error downloading/processing {remote_path}: {e}", exc_info=True)
            return False


class SFTPTokenMasterWatcher:
    """Main SFTP watcher for Token Master updates"""

    def __init__(self):
        self.processor = TokenMasterProcessor()
        self.processed_files: Set[str] = set()

    def get_target_dates(self) -> List[tuple]:
        """Get list of target dates to check"""
        today = datetime.now()
        yesterday = today - timedelta(days=1)

        dates = []
        for date_obj in [today, yesterday]:
            date_str = date_obj.strftime("%B%d%Y")  # e.g. "July082025"
            dates.append((date_str, date_obj.strftime("%Y-%m-%d")))

        return dates

    async def scan_and_process_securities(self) -> None:
        """Scan SFTP directories for Securities.dat files"""
        try:
            await asyncio.to_thread(self.processor.sftp.connect)
            logger.info("🔗 SFTP connection established")

            target_dates = self.get_target_dates()
            files_processed = 0
            temp_dir = tempfile.mkdtemp(prefix="securities_")

            try:
                for date_str, iso_date in target_dates:
                    remote_dir = f"{settings.SFTP_REMOTE_PATH}/SECURITY/{date_str}"
                    try:
                        logger.info(f"🔍 Scanning directory: {remote_dir}")
                        files = await asyncio.to_thread(self.processor.sftp.list_files, remote_dir)

                        securities_files = [
                            f for f in files
                            if os.path.basename(f).lower() == 'securities.dat'
                        ]
                        logger.info(f"📁 Found {len(securities_files)} Securities.dat files in {date_str}")

                        for file_path in securities_files:
                            file_key = f"{date_str}:{os.path.basename(file_path)}"
                            if file_key not in self.processed_files:
                                logger.info(f"🆕 Processing new file: {os.path.basename(file_path)}")
                                success = await self.processor.download_and_process_file(
                                    file_path, temp_dir, iso_date
                                )
                                if success:
                                    self.processed_files.add(file_key)
                                    files_processed += 1
                                    logger.info(f"✅ Successfully processed {os.path.basename(file_path)}")
                                else:
                                    logger.error(f"❌ Failed to process {os.path.basename(file_path)}")
                            else:
                                logger.debug(f"⏭️ Skipping already processed: {os.path.basename(file_path)}")

                    except Exception as e:
                        logger.error(f"❌ Error scanning {remote_dir}: {e}")
                        continue

                if files_processed > 0:
                    logger.info(f"🎉 Successfully processed {files_processed} new Securities files")
                else:
                    logger.info("ℹ️ No new Securities files found")
            finally:
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)

        except Exception as e:
            logger.error(f"❌ Error in scan_and_process_securities: {e}", exc_info=True)
        finally:
            try:
                self.processor.sftp.close()
            except:
                pass


async def start_sftp_watcher() -> None:
    """
    Run a one-off scan of the SFTP Token Master directory.
    """
    watcher = SFTPTokenMasterWatcher()
    logger.info("🚀 Running one-off SFTP Token Master scan…")
    await watcher.scan_and_process_securities()
    logger.info("✅ One-off scan complete.")


async def manual_process_file(file_path: str) -> None:
    """
    Manually process a single Securities.dat file (for testing)
    """
    processor = TokenMasterProcessor()
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        success = await processor.process_securities_file(file_path, today)
        if success:
            logger.info(f"✅ Manual processing successful: {file_path}")
        else:
            logger.error(f"❌ Manual processing failed: {file_path}")
    except Exception as e:
        logger.error(f"❌ Error in manual processing: {e}", exc_info=True)


async def get_database_stats() -> None:
    """Get current database statistics"""
    try:
        async with AsyncSessionLocal() as session:
            total_result = await session.execute(
                select([CMTokenMaster.token_number]).count()
            )
            total_count = total_result.scalar()

            series_result = await session.execute(
                select(
                    CMTokenMaster.series,
                    CMTokenMaster.token_number.func.count()
                )
                .group_by(CMTokenMaster.series)
                .order_by(CMTokenMaster.token_number.func.count().desc())
                .limit(10)
            )
            series_stats = series_result.all()

            logger.info(f"📊 Database Statistics:")
            logger.info(f"   Total securities: {total_count:,}")
            logger.info(f"   Top series:")
            for series, count in series_stats:
                logger.info(f"     {series}: {count:,}")

    except Exception as e:
        logger.error(f"❌ Error getting database stats: {e}", exc_info=True)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()
        if cmd == "stats":
            asyncio.run(get_database_stats())
        elif cmd == "manual":
            if len(sys.argv) > 2:
                asyncio.run(manual_process_file(sys.argv[2]))
            else:
                print("Usage: python token_master.py manual <file_path>")
        else:
            print("Usage: python token_master.py [stats|manual <file_path>]")
    else:
        # Run a single scan and exit
        asyncio.run(start_sftp_watcher())
