import asyncio
import aiohttp
import json

async def test_all_apis():
    """Complete API testing"""
    
    base_url = "http://localhost:8000"
    
    print("🧪 Testing Complete NSE API Functionality\n")
    
    async with aiohttp.ClientSession() as session:
        
        # Test 1: Health Check
        print("1️⃣ Health Check:")
        try:
            async with session.get(f"{base_url}/") as response:
                result = await response.json()
                print(f"   ✅ Server Status: {response.status} - {result.get('message')}")
        except Exception as e:
            print(f"   ❌ Server Error: {e}")
        
        # Test 2: Contract Suggestions
        print("\n2️⃣ Testing Symbol Suggestions:")
        suggestion_tests = ["A", "AB", "REL", "TCS", "GOLD"]
        for test_query in suggestion_tests:
            try:
                async with session.get(f"{base_url}/api/snapshots/search/suggestions?q={test_query}&limit=5") as response:
                    result = await response.json()
                    if response.status == 200 and result.get('count', 0) > 0:
                        print(f"   ✅ '{test_query}': Found {result['count']} suggestions")
                        # Show first suggestion
                        if result['suggestions']:
                            first = result['suggestions'][0]
                            print(f"      Example: {first['symbol']} (Token: {first['token']})")
                    else:
                        print(f"   ⚠️ '{test_query}': No suggestions found")
            except Exception as e:
                print(f"   ❌ '{test_query}': Error - {e}")
        
        # Test 3: Token-based Search
        print("\n3️⃣ Testing Token-based Search:")
        token_tests = [1, 13, 22, 100, 1000]  # ABB=13, ACC=22 from earlier
        for token in token_tests:
            try:
                async with session.get(f"{base_url}/api/snapshots/search?query={token}") as response:
                    if response.status == 200:
                        result = await response.json()
                        print(f"   ✅ Token {token}: Found snapshot for {result.get('security_token')} - Price: ₹{result.get('last_traded_price', 0)/100:.2f}")
                    elif response.status == 404:
                        print(f"   ⚠️ Token {token}: No market data available (contract exists but no trading data)")
                    else:
                        result = await response.json()
                        print(f"   ❌ Token {token}: Error - {result.get('detail')}")
            except Exception as e:
                print(f"   ❌ Token {token}: Error - {e}")
        
        # Test 4: Symbol-based Search  
        print("\n4️⃣ Testing Symbol-based Search:")
        symbol_tests = ["ABB", "ACC", "AARTIIND", "GOLDSTAR", "RELIANCE"]
        for symbol in symbol_tests:
            try:
                async with session.get(f"{base_url}/api/snapshots/search?query={symbol}") as response:
                    if response.status == 200:
                        result = await response.json()
                        print(f"   ✅ {symbol}: Found snapshot - Token: {result.get('security_token')}, Price: ₹{result.get('last_traded_price', 0)/100:.2f}")
                    elif response.status == 404:
                        print(f"   ⚠️ {symbol}: Contract found but no market data available")
                    else:
                        result = await response.json()
                        print(f"   ❌ {symbol}: Error - {result.get('detail')}")
            except Exception as e:
                print(f"   ❌ {symbol}: Error - {e}")
        
        # Test 5: Check Market Data Status
        print("\n5️⃣ Checking Market Data Status:")
        try:
            # Check total snapshots
            async with session.get(f"{base_url}/api/snapshots/history?start_ts=1&end_ts=9999999999&limit=1") as response:
                if response.status == 200:
                    result = await response.json()
                    print(f"   📊 Market snapshots available: {len(result.get('snapshots', []))}")
                else:
                    print(f"   📊 Market data status: {response.status}")
        except Exception as e:
            print(f"   ❌ Market data check error: {e}")

if __name__ == "__main__":
    asyncio.run(test_all_apis())