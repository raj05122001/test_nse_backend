import asyncio
from datetime import datetime
from db.connection import get_db
from db.models import CMSnapshot

async def insert_test_data():
    """Insert some test data to verify database connectivity"""
    async for session in get_db():
        try:
            # Create test snapshot
            test_snapshot = CMSnapshot(
                transcode=5,
                timestamp=int(datetime.now().timestamp()),
                message_length=96,
                security_token=22,
                last_traded_price=150000,  # 1500.00 in paisa
                best_buy_quantity=1000,
                best_buy_price=149900,
                best_sell_quantity=500,
                best_sell_price=150100,
                total_traded_quantity=5000,
                average_traded_price=150000,
                open_price=149000,
                high_price=151000,
                low_price=148000,
                close_price=150000,
                interval_open_price=149500,
                interval_high_price=150500,
                interval_low_price=149000,
                interval_close_price=150000,
                interval_total_traded_quantity=2000,
                indicative_close_price=150200
            )
            
            session.add(test_snapshot)
            await session.commit()
            print("✅ Test data inserted successfully!")
            
            # Verify insertion
            from sqlalchemy import select
            result = await session.execute(select(CMSnapshot).limit(5))
            snapshots = result.scalars().all()
            print(f"✅ Found {len(snapshots)} records in database")
            
            for snapshot in snapshots:
                print(f"   Token: {snapshot.security_token}, Price: {snapshot.last_traded_price}")
                
        except Exception as e:
            print(f"❌ Error inserting test data: {e}")
            await session.rollback()
        finally:
            break

if __name__ == "__main__":
    asyncio.run(insert_test_data())