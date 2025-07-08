import asyncio
import pandas as pd
from db.connection import AsyncSessionLocal
from db.models import CMContractStreamInfo
from datetime import datetime
import os

async def load_contract_info_from_csv():
    """Load contract info from downloaded CSV file"""
    
    csv_file_path = "cm_contract_stream_info.csv"
    
    if not os.path.exists(csv_file_path):
        print(f"❌ File not found: {csv_file_path}")
        print("📁 Please make sure the file is in the project root directory")
        return
    
    try:
        # Read CSV file
        print(f"📖 Reading CSV file: {csv_file_path}")
        df = pd.read_csv(csv_file_path)
        
        print(f"📊 CSV Info:")
        print(f"   - Shape: {df.shape}")
        print(f"   - Columns: {list(df.columns)}")
        
        # Show first few rows
        print(f"\n🔍 First 5 rows:")
        print(df.head())
        
        # Check if DataFrame is empty
        if df.empty:
            print("⚠️ CSV file is empty or couldn't be parsed properly")
            return
            
        # Map CSV columns to our database fields
        # Adjust column names based on actual CSV structure
        column_mapping = {
            # Add mappings based on actual CSV columns
            # Example mappings (adjust based on actual CSV):
            'Symbol': 'symbol',
            'Token': 'symbol_token', 
            'ISIN': 'isin',
            'Series': 'series',
            'Instrument': 'instrument_type',
            'Segment': 'segment'
        }
        
        print(f"\n🔄 Processing {len(df)} records...")
        
        async with AsyncSessionLocal() as session:
            current_timestamp = int(datetime.now().timestamp())
            added_count = 0
            skipped_count = 0
            
            for index, row in df.iterrows():
                try:
                    # Extract data from row (adjust field names based on actual CSV)
                    symbol = str(row.get('Symbol', row.get('symbol', ''))).strip()
                    symbol_token = row.get('Token', row.get('symbol_token', row.get('token', 0)))
                    series = str(row.get('Series', row.get('series', 'EQ'))).strip()
                    instrument_type = str(row.get('Instrument', row.get('instrument_type', 'EQ'))).strip()
                    segment = str(row.get('Segment', row.get('segment', 'CM'))).strip()
                    
                    # Validate required fields
                    if not symbol or not symbol_token:
                        skipped_count += 1
                        continue
                        
                    # Convert token to int if it's string
                    try:
                        symbol_token = int(symbol_token)
                    except (ValueError, TypeError):
                        skipped_count += 1
                        continue
                    
                    # Check if record already exists
                    existing = await session.get(CMContractStreamInfo, symbol_token)
                    if existing:
                        skipped_count += 1
                        continue
                    
                    # Create new record
                    contract = CMContractStreamInfo(
                        timestamp=current_timestamp,
                        record_count=1,
                        segment=segment[:4],  # Limit to 4 chars
                        symbol_token=symbol_token,
                        instrument_type=instrument_type[:16],  # Limit to 16 chars
                        symbol=symbol[:32],  # Limit to 32 chars
                        zero1=0,
                        zero2=0,
                        series=series[:8]  # Limit to 8 chars
                    )
                    
                    session.add(contract)
                    added_count += 1
                    
                    # Show progress every 100 records
                    if added_count % 100 == 0:
                        print(f"   📝 Processed {added_count} records...")
                        
                except Exception as e:
                    print(f"⚠️ Error processing row {index}: {e}")
                    skipped_count += 1
                    continue
            
            # Commit all changes
            await session.commit()
            
            print(f"\n✅ Contract info loading complete!")
            print(f"   📈 Added: {added_count} records")
            print(f"   ⏭️ Skipped: {skipped_count} records")
            
            # Show some sample loaded data
            if added_count > 0:
                from sqlalchemy import text
                async with session.begin():
                    result = await session.execute(text("""
                        SELECT symbol, symbol_token, instrument_type, series 
                        FROM cm_contract_stream_info 
                        ORDER BY symbol 
                        LIMIT 10
                    """))
                    samples = result.fetchall()
                    
                    print(f"\n🎯 Sample loaded records:")
                    for sample in samples:
                        print(f"   {sample[0]} (Token: {sample[1]}, Type: {sample[2]}, Series: {sample[3]})")
                        
    except Exception as e:
        print(f"❌ Error loading CSV: {e}")
        print("💡 Make sure pandas is installed: pip install pandas")

if __name__ == "__main__":
    asyncio.run(load_contract_info_from_csv())