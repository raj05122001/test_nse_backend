import asyncio
import time
from db.connection import engine
from sqlalchemy import text

async def monitor_database():
    """Monitor database for new records"""
    last_count = 0
    
    while True:
        try:
            async with engine.begin() as conn:
                # Check record count
                result = await conn.execute(text("SELECT COUNT(*) FROM cm_snapshot"))
                current_count = result.fetchone()[0]
                
                if current_count != last_count:
                    print(f"📈 Database updated! Records: {last_count} → {current_count} (+{current_count - last_count})")
                    
                    # Show latest records
                    result = await conn.execute(text("""
                        SELECT 
                            security_token,
                            last_traded_price/100.0 as price_rs,
                            to_timestamp(timestamp) as time
                        FROM cm_snapshot 
                        ORDER BY id DESC 
                        LIMIT 3
                    """))
                    latest = result.fetchall()
                    
                    print("🔥 Latest records:")
                    for record in latest:
                        print(f"   Token: {record[0]}, Price: ₹{record[1]:.2f}, Time: {record[2]}")
                    print("-" * 50)
                    
                    last_count = current_count
                else:
                    print(f"⏳ No new records. Current count: {current_count}")
                    
        except Exception as e:
            print(f"❌ Database error: {e}")
            
        await asyncio.sleep(10)  # Check every 10 seconds

if __name__ == "__main__":
    print("🔍 Starting database monitor...")
    asyncio.run(monitor_database())