import asyncio
from db.connection import AsyncSessionLocal
from sqlalchemy import text

async def assign_realistic_tokens():
    """Assign more realistic token numbers based on symbol hash"""
    
    async with AsyncSessionLocal() as session:
        try:
            # Get all symbols
            result = await session.execute(text("""
                SELECT id, symbol FROM cm_contract_stream_info ORDER BY id
            """))
            records = result.fetchall()
            
            print(f"🔄 Updating {len(records)} token numbers...")
            
            for record in records:
                record_id, symbol = record
                # Generate a realistic token number using hash
                import hashlib
                hash_value = int(hashlib.md5(symbol.encode()).hexdigest()[:6], 16)
                new_token = (hash_value % 90000) + 10000  # Range: 10000-99999
                
                await session.execute(text(f"""
                    UPDATE cm_contract_stream_info 
                    SET symbol_token = {new_token} 
                    WHERE id = {record_id}
                """))
            
            await session.commit()
            print(f"✅ Updated all token numbers!")
            
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(assign_realistic_tokens())