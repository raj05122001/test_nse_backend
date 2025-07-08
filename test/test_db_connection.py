import asyncio
from db.connection import engine, AsyncSessionLocal
from sqlalchemy import text

async def test_database():
    """Test database connection and tables"""
    try:
        async with engine.begin() as conn:
            # Test connection
            result = await conn.execute(text("SELECT version()"))
            version = result.fetchone()
            print(f"✅ Database connected: {version[0]}")
            
            # Check if tables exist
            result = await conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """))
            tables = result.fetchall()
            print(f"✅ Found {len(tables)} tables:")
            for table in tables:
                print(f"   - {table[0]}")
                
            # Check table contents
            try:
                result = await conn.execute(text("SELECT COUNT(*) FROM cm_snapshot"))
                count = result.fetchone()
                print(f"✅ cm_snapshot table has {count[0]} records")
            except Exception as e:
                print(f"❌ Error checking cm_snapshot: {e}")
                
    except Exception as e:
        print(f"❌ Database connection failed: {e}")

if __name__ == "__main__":
    asyncio.run(test_database())