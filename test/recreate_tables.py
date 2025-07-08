import asyncio
from db.connection import engine, Base
from db.models import CMSnapshot, CMIndexSnapshot, CMCallAuctionSnapshot, CMContractStreamInfo
from sqlalchemy import text

async def recreate_tables():
    """Drop and recreate only CM tables with new schema"""
    async with engine.begin() as conn:
        # Define only the CM tables we want to recreate
        cm_tables_to_drop = [
            'cm_snapshot',
            'cm_index_snapshot', 
            'cm_call_auction_snapshot',
            'cm_contract_stream_info',
            'cm_market_snapshot',  # if exists
            'cm_call_auction_snapshot',  # if exists  
            'cm_index_snapshot'  # if exists
        ]
        
        print("🗑️ Dropping existing CM tables...")
        for table_name in cm_tables_to_drop:
            try:
                await conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
                print(f"   ✅ Dropped {table_name}")
            except Exception as e:
                print(f"   ⚠️ Could not drop {table_name}: {e}")
        
        # Create only our new CM tables
        print("\n🏗️ Creating new CM tables...")
        tables_to_create = [CMSnapshot, CMIndexSnapshot, CMCallAuctionSnapshot, CMContractStreamInfo]
        
        for table_class in tables_to_create:
            try:
                await conn.run_sync(table_class.__table__.create, checkfirst=True)
                print(f"   ✅ Created {table_class.__tablename__}")
            except Exception as e:
                print(f"   ⚠️ Could not create {table_class.__tablename__}: {e}")
        
        print("\n✅ CM Tables recreated successfully!")
        
        # Show all tables (to verify nothing else was deleted)
        result = await conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """))
        all_tables = result.fetchall()
        print(f"\n📋 All Tables in Database ({len(all_tables)}):")
        for table in all_tables:
            if table[0].startswith('cm_'):
                print(f"   🆕 {table[0]} (CM table)")
            else:
                print(f"   📊 {table[0]} (existing table)")
                
        # Show CM table structures
        print(f"\n🔍 New CM Table Structures:")
        cm_table_names = [t[0] for t in all_tables if t[0].startswith('cm_')]
        
        for table_name in cm_table_names:
            try:
                result = await conn.execute(text(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position
                """))
                columns = result.fetchall()
                print(f"\n📊 {table_name}:")
                for col in columns:
                    print(f"   - {col[0]} ({col[1]})")
                    
            except Exception as e:
                print(f"   ❌ Error checking {table_name}: {e}")

if __name__ == "__main__":
    asyncio.run(recreate_tables())