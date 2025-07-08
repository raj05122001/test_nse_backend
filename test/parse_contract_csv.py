import asyncio
import csv
from db.connection import AsyncSessionLocal
from db.models import CMContractStreamInfo
from datetime import datetime

async def parse_all_nse_contracts():
    """Parse ALL NSE contract stream info without static symbols"""
    
    csv_file = "cm_contract_stream_info.csv"
    
    try:
        print(f"📖 Parsing ALL NSE Contract Stream Info: {csv_file}")
        
        records_added = 0
        records_skipped = 0
        
        async with AsyncSessionLocal() as session:
            current_timestamp = int(datetime.now().timestamp())
            
            with open(csv_file, 'r', encoding='utf-8') as file:
                csv_reader = csv.reader(file)
                
                # First line contains metadata: timestamp,record_count,
                metadata = next(csv_reader)
                total_expected = int(metadata[1])
                print(f"📋 Metadata: Timestamp={metadata[0]}, Expected Records={total_expected}")
                
                for line_num, row in enumerate(csv_reader, 2):
                    try:
                        if len(row) < 8:  # Need all 8 columns
                            records_skipped += 1
                            continue
                        
                        # Parse format: C,2,1,EQUITY,GOLDSTAR,0,0,SM
                        segment = row[0].strip()           # C (Capital Market)
                        record_count = int(row[1])         # Record count/identifier
                        symbol_token = int(row[2])         # Actual NSE token number
                        instrument_type = row[3].strip()   # EQUITY
                        symbol = row[4].strip()            # Stock symbol
                        zero1 = int(row[5])               # Always 0
                        zero2 = int(row[6])               # Always 0  
                        series = row[7].strip()           # Series (EQ, SM, TB, etc.)
                        
                        # Validate essential fields
                        if not symbol or len(symbol) < 1 or symbol_token <= 0:
                            records_skipped += 1
                            continue
                        
                        # Check if token already exists (avoid duplicates)
                        from sqlalchemy import select
                        existing_stmt = select(CMContractStreamInfo).where(
                            CMContractStreamInfo.symbol_token == symbol_token
                        )
                        existing_result = await session.execute(existing_stmt)
                        if existing_result.first():
                            records_skipped += 1
                            continue
                        
                        # Create contract record
                        contract = CMContractStreamInfo(
                            timestamp=current_timestamp,
                            record_count=record_count,
                            segment=segment[:4],
                            symbol_token=symbol_token,
                            instrument_type=instrument_type[:16],
                            symbol=symbol[:32],
                            zero1=zero1,
                            zero2=zero2,
                            series=series[:8]
                        )
                        
                        session.add(contract)
                        records_added += 1
                        
                        # Show progress every 2000 records
                        if records_added % 2000 == 0:
                            print(f"   📝 Processed {records_added}/{total_expected} records ({(records_added/total_expected)*100:.1f}%)")
                            await session.commit()  # Commit in batches
                        
                        # Show only first 5 records as sample
                        if records_added <= 5:
                            print(f"   ✅ Sample {records_added}: {symbol} → Token: {symbol_token}, Series: {series}")
                        
                    except Exception as e:
                        if line_num <= 10:
                            print(f"   ⚠️ Line {line_num} error: {e} | Row: {row}")
                        records_skipped += 1
                        continue
            
            # Final commit
            await session.commit()
            
            print(f"\n✅ ALL NSE Contracts parsing complete!")
            print(f"   📈 Successfully added: {records_added} records")
            print(f"   ⏭️ Skipped/Duplicates: {records_skipped} records")
            print(f"   🎯 Success Rate: {(records_added/(records_added + records_skipped))*100:.1f}%")
            
            # Show database statistics
            if records_added > 0:
                from sqlalchemy import text
                
                print(f"\n📊 Database Statistics:")
                
                # Total count
                total_result = await session.execute(text("""
                    SELECT COUNT(*) FROM cm_contract_stream_info
                """))
                total_count = total_result.scalar()
                print(f"   📈 Total contracts in database: {total_count}")
                
                # Series breakdown
                series_result = await session.execute(text("""
                    SELECT series, COUNT(*) as count
                    FROM cm_contract_stream_info 
                    GROUP BY series 
                    ORDER BY count DESC
                """))
                series_summary = series_result.fetchall()
                print(f"\n🔎 Contracts by Series:")
                for series_info in series_summary:
                    print(f"   📊 {series_info[0]}: {series_info[1]} contracts")
                
                # Show token range
                range_result = await session.execute(text("""
                    SELECT MIN(symbol_token) as min_token, MAX(symbol_token) as max_token
                    FROM cm_contract_stream_info
                """))
                token_range = range_result.first()
                print(f"\n🔢 Token Range: {token_range[0]} to {token_range[1]}")
                
                # Sample from different series
                print(f"\n📋 Sample contracts from each major series:")
                for series_name in ['EQ', 'SM', 'TB', 'SG']:
                    sample_result = await session.execute(text(f"""
                        SELECT symbol, symbol_token 
                        FROM cm_contract_stream_info 
                        WHERE series = '{series_name}'
                        ORDER BY symbol_token
                        LIMIT 3
                    """))
                    samples = sample_result.fetchall()
                    if samples:
                        print(f"   📊 {series_name}: {[f'{s[0]}({s[1]})' for s in samples]}")
                        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(parse_all_nse_contracts())