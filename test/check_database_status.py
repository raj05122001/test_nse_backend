import asyncio
from db.connection import AsyncSessionLocal
from sqlalchemy import text

async def check_complete_database_status():
    """Check complete database status"""
    
    async with AsyncSessionLocal() as session:
        print("📊 Complete Database Status Report\n")
        
        # Contract Info Status
        try:
            result = await session.execute(text("SELECT COUNT(*) FROM cm_contract_stream_info"))
            contract_count = result.scalar()
            print(f"📋 Contract Info:")
            print(f"   Total Contracts: {contract_count:,}")
            
            # Series breakdown
            result = await session.execute(text("""
                SELECT series, COUNT(*) as count 
                FROM cm_contract_stream_info 
                GROUP BY series 
                ORDER BY count DESC 
                LIMIT 10
            """))
            series_data = result.fetchall()
            print(f"   Top Series:")
            for series, count in series_data:
                print(f"     {series}: {count:,} contracts")
            
        except Exception as e:
            print(f"   ❌ Contract Info Error: {e}")
        
        # Market Snapshot Status
        print(f"\n📈 Market Data Status:")
        try:
            # Check all snapshot tables
            tables = ['cm_snapshot', 'cm_index_snapshot', 'cm_call_auction_snapshot']
            
            for table in tables:
                try:
                    result = await session.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.scalar()
                    print(f"   {table}: {count:,} records")
                    
                    if count > 0:
                        # Show latest timestamp
                        result = await session.execute(text(f"""
                            SELECT 
                                MAX(timestamp) as latest_time,
                                COUNT(DISTINCT security_token) as unique_tokens
                            FROM {table}
                        """))
                        stats = result.first()
                        if stats:
                            import datetime
                            latest_time = datetime.datetime.fromtimestamp(stats[0])
                            print(f"     Latest Data: {latest_time}")
                            if table == 'cm_snapshot':
                                print(f"     Unique Tokens: {stats[1]:,}")
                        
                except Exception as e:
                    print(f"   ❌ {table}: Error - {e}")
                    
        except Exception as e:
            print(f"   ❌ Market Data Error: {e}")
        
        # Active Symbols with Market Data
        print(f"\n🎯 Active Trading Status:")
        try:
            result = await session.execute(text("""
                SELECT 
                    c.symbol,
                    c.symbol_token,
                    c.series,
                    s.last_traded_price/100.0 as price_rs,
                    s.timestamp
                FROM cm_contract_stream_info c
                JOIN cm_snapshot s ON c.symbol_token = s.security_token
                WHERE s.timestamp = (
                    SELECT MAX(timestamp) 
                    FROM cm_snapshot s2 
                    WHERE s2.security_token = c.symbol_token
                )
                AND c.series = 'EQ'
                ORDER BY s.timestamp DESC
                LIMIT 10
            """))
            active_stocks = result.fetchall()
            
            if active_stocks:
                print(f"   📊 Active EQ Stocks with Recent Data:")
                for stock in active_stocks:
                    print(f"     {stock[0]} (Token: {stock[1]}) - ₹{stock[3]:.2f}")
            else:
                print(f"   ⚠️ No active trading data found")
                
        except Exception as e:
            print(f"   ❌ Active Trading Error: {e}")

if __name__ == "__main__":
    asyncio.run(check_complete_database_status())