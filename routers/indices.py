from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, desc, and_, or_, case
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Dict, Any, Optional
import aiohttp
from datetime import datetime
import json

from db.connection import get_db
from db.models import CMSnapshot, CMCallAuctionSnapshot, CMTokenMaster, CMStockHistorical

from sqlalchemy import func
from datetime import timedelta

router = APIRouter(prefix="/api/indices", tags=["indices"])

# Index compositions from your JSON
INDEX_COMPOSITIONS = {
    "all": {
        "nifty50": "NIFTY 50",
        "nifty100":"NIFTY 100", 
        "niftyNext50":"NIFTY NEXT 50",
        "niftyIT":"NIFTY IT",
        "nifty50USD":"NIFTY 50 USD",
        "niftyBank":"NIFTY BANK",
        "niftyMidcap50":"NIFTY MIDCAP 50",
        "niftyRealty":"NIFTY REALTY",
        "niftyInfra":"NIFTY INFRA",
        "niftyEnergy":"NIFTY ENERGY"
    },
    "nifty50": [
        "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
        "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BEL", "BHARTIARTL",
        "CIPLA", "COALINDIA", "DRREDDY", "EICHERMOT", "ETERNAL",
        "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE", "HEROMOTOCO",
        "HINDALCO", "HINDUNILVR", "ICICIBANK", "INDUSINDBK", "INFY",
        "ITC", "JIOFIN", "JSWSTEEL", "KOTAKBANK", "LT",
        "M&M", "MARUTI", "NESTLEIND", "NTPC", "ONGC",
        "POWERGRID", "RELIANCE", "SBILIFE", "SHRIRAMFIN", "SBIN",
        "SUNPHARMA", "TCS", "TATACONSUM", "TATAMOTORS", "TATASTEEL",
        "TECHM", "TITAN", "TRENT", "ULTRACEMCO", "WIPRO"
    ],
    "nifty100": [
        "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
        "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BEL", "BHARTIARTL",
        "CIPLA", "COALINDIA", "DRREDDY", "EICHERMOT", "ETERNAL",
        "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE", "HEROMOTOCO",
        "HINDALCO", "HINDUNILVR", "ICICIBANK", "INDUSINDBK", "INFY",
        "ITC", "JIOFIN", "JSWSTEEL", "KOTAKBANK", "LT",
        "M&M", "MARUTI", "NESTLEIND", "NTPC", "ONGC",
        "POWERGRID", "RELIANCE", "SBILIFE", "SHRIRAMFIN", "SBIN",
        "SUNPHARMA", "TCS", "TATACONSUM", "TATAMOTORS", "TATASTEEL",
        "TECHM", "TITAN", "TRENT", "ULTRACEMCO", "WIPRO",
        "ABB", "ADANIENSOL", "ADANIGREEN", "ADANIPOWER", "AMBUJACEM",
        "BAJAJHLDNG", "BAJAJHFL", "BANKBARODA", "BPCL", "BRITANNIA",
        "BOSCHLTD", "CANBK", "CGPOWER", "CHOLAFIN", "DABUR",
        "DIVISLAB", "DLF", "DMART", "GAIL", "GODREJCP",
        "HAVELLS", "HAL", "HYUNDAI", "ICICIGI", "ICICIPRULI",
        "INDHOTEL", "IOC", "INDIGO", "NAUKRI", "IRFC",
        "JINDALSTEL", "JSWENERGY", "LICI", "LODHA", "LTIM",
        "PIDILITIND", "PFC", "PNB", "RECLTD", "MOTHERSON",
        "SHREECEM", "SIEMENS", "SWIGGY", "TATAPOWER", "TORNTPHARM",
        "TVSMOTOR", "UNITDSPR", "VBL", "VEDL", "ZYDUSLIFE"
    ],
    "niftyNext50": [
        "ABB", "ADANIENSOL", "ADANIGREEN", "ADANIPOWER", "AMBUJACEM",
        "BAJAJHLDNG", "BAJAJHFL", "BANKBARODA", "BPCL", "BRITANNIA",
        "BOSCHLTD", "CANBK", "CGPOWER", "CHOLAFIN", "DABUR",
        "DIVISLAB", "DLF", "DMART", "GAIL", "GODREJCP",
        "HAVELLS", "HAL", "HYUNDAI", "ICICIGI", "ICICIPRULI",
        "INDHOTEL", "IOC", "INDIGO", "NAUKRI", "IRFC",
        "JINDALSTEL", "JSWENERGY", "LICI", "LODHA", "LTIM",
        "PIDILITIND", "PFC", "PNB", "RECLTD", "MOTHERSON",
        "SHREECEM", "SIEMENS", "SWIGGY", "TATAPOWER", "TORNTPHARM",
        "TVSMOTOR", "UNITDSPR", "VBL", "VEDL", "ZYDUSLIFE"
    ],
    "niftyIT": [
        "INFY", "TCS", "HCLTECH", "TECHM", "WIPRO",
        "PERSISTENT", "COFORGE", "LTIM", "MPHASIS", "OFSS"
    ],
    "nifty50USD": [
        "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
        "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BEL", "BHARTIARTL",
        "CIPLA", "COALINDIA", "DRREDDY", "EICHERMOT", "ETERNAL",
        "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE", "HEROMOTOCO",
        "HINDALCO", "HINDUNILVR", "ICICIBANK", "INDUSINDBK", "INFY",
        "ITC", "JIOFIN", "JSWSTEEL", "KOTAKBANK", "LT",
        "M&M", "MARUTI", "NESTLEIND", "NTPC", "ONGC",
        "POWERGRID", "RELIANCE", "SBILIFE", "SHRIRAMFIN", "SBIN",
        "SUNPHARMA", "TCS", "TATACONSUM", "TATAMOTORS", "TATASTEEL",
        "TECHM", "TITAN", "TRENT", "ULTRACEMCO", "WIPRO"
    ],
    "niftyBank": [
        "HDFCBANK", "ICICIBANK", "SBIN", "KOTAKBANK", "AXISBANK",
        "PNB", "BANKBARODA", "CANBK", "INDUSINDBK", "AUBANK",
        "IDFCFIRSTB", "FEDERALBNK"
    ],
    "niftyMidcap50": [
        "ABCAPITAL", "ACC", "ASHOKLEY", "AUROPHARMA", "BHARATFORG",
        "COLPAL", "CONCOR", "CGPOWER", "CUMMINSIND", "FEDERALBNK",
        "GMRINFRA", "GODREJPROP", "HINDPETRO", "IDEA", "INDHOTEL",
        "LUPIN", "MARICO", "MPHASIS", "MRF", "NMDC",
        "OBEROIRLTY", "OFSS", "PERSISTENT", "PETRONET", "PHOENIXLTD",
        "SRF", "SAIL", "SUNDRMFAST", "SUPREMEIND", "SUZLON",
        "TATACOMM", "UPL", "VOLTAS", "YESBANK", "INDUSTOWER",
        "L&TFH", "MUTHOOTFIN", "PIIND", "ASTRAL", "APLAPOLLO",
        "IDFCFIRSTB", "ALKEM", "AUBANK", "DIXON", "HDFCAMC",
        "POLYCAB", "KPITTECH", "SBICARD", "MAXHEALTH", "PBFINTECH"
    ],
    "niftyRealty": [
        "DLF", "LODHA", "GODREJPROP", "PHOENIXLTD", "PRESTIGE",
        "OBEROIRLTY", "BRIGADE", "ANANTRAJ", "SOBHA", "RAYMOND"
    ],
    "niftyInfra": [
        "RELIANCE", "BHARTIARTL", "LT", "ULTRACEMCO", "NTPC",
        "ADANIPORTS", "ONGC", "POWERGRID", "INDIGO", "IOC",
        "DLF", "GRASIM", "ADANIGREEN", "BPCL", "AMBUJACEM",
        "TATAPOWER", "GAIL", "MAXHEALTH", "SIEMENS", "SHREECEM",
        "APOLLOHOSP", "MOTHERSON", "INDUSTOWERS", "CGPOWER", "INDHOTEL",
        "CUMMINSIND", "HINDPETRO", "ASHOKLEY", "GODREJPROP", "BHARATFORGE"
    ],
    "niftyEnergy": [
        "ABB", "ADANIPOWER", "BPCL", "BHEL", "CASTROLIND",
        "CESC", "COALINDIA", "CGPOWER", "GAIL", "GSPL",
        "GUJGAS", "HINDPETRO", "IOC", "IGL", "JPPOWER",
        "JSWENERGY", "NTPC", "NLCINDIA", "NHPC", "ONGC",
        "OIL", "PETRONET", "POWERGRID", "RELIANCE", "SIEMENS",
        "SUZLON", "TATAPOWER", "THERMAX", "TORNTPOWER", "SCHNEIDER",
        "GVT&D", "SJVN", "TRITURBINE", "AEGISLOG", "INOXWIND",
        "ADANIENSOL", "MGL", "ADANIGREEN", "ATGL", "POWERINDIA"
    ]
}

@router.get("/stocks/{index_name}")
async def get_index_stocks_prices(
    index_name: str,
    session: AsyncSession = Depends(get_db)
):
    """
    Get current prices for all stocks in a specific index
    
    Example: 
    - /api/indices/stocks/nifty50 -> सभी NIFTY 50 stocks का price
    - /api/indices/stocks/nifty100 -> सभी NIFTY 100 stocks का price
    - /api/indices/stocks/niftyIT -> सभी NIFTY IT stocks का price
    """
    
    # Validate index name
    if index_name.lower() not in INDEX_COMPOSITIONS:
        available_indices = list(INDEX_COMPOSITIONS["all"].keys())
        raise HTTPException(
            status_code=404, 
            detail=f"Index '{index_name}' not found. Available indices: {available_indices}"
        )
    
    # Get stock symbols for this index
    stock_symbols = INDEX_COMPOSITIONS[index_name.lower()]
    index_display_name = INDEX_COMPOSITIONS["all"].get(index_name.lower(), index_name.upper())
    
    try:
        # Step 1: Get tokens for all symbols in this index from CMTokenMaster
        # Priority: EQ series first, then others if EQ not found
        symbol_stmt = (
            select(CMTokenMaster.token_number, CMTokenMaster.symbol, CMTokenMaster.company_name, CMTokenMaster.series)
            .where(CMTokenMaster.symbol.in_(stock_symbols))
            .order_by(
                CMTokenMaster.symbol,
                # Prioritize EQ series
                case(
                    (CMTokenMaster.series == 'EQ', 1),
                    (CMTokenMaster.series == 'BE', 2), 
                    else_=3
                )
            )
        )
        symbol_result = await session.execute(symbol_stmt)
        symbol_to_token = {}
        symbol_details = {}
        
        # Take only the first (highest priority) entry for each symbol
        for row in symbol_result:
            if row.symbol not in symbol_to_token:  # Only take first occurrence
                symbol_to_token[row.symbol] = row.token_number
                symbol_details[row.symbol] = {
                    'token': row.token_number,
                    'company_name': row.company_name.strip(),
                    'series': row.series
                }
        
        if not symbol_to_token:
            raise HTTPException(status_code=404, detail=f"No tokens found for {index_name} stocks")
        
        # Step 2: Get latest snapshots from both CMSnapshot and CMCallAuctionSnapshot
        tokens = list(symbol_to_token.values())
        
        # Get regular market snapshots
        snapshot_stmt = (
            select(CMSnapshot)
            .where(CMSnapshot.security_token.in_(tokens))
            .order_by(CMSnapshot.timestamp.desc())
        )
        snapshot_result = await session.execute(snapshot_stmt)
        regular_snapshots = snapshot_result.scalars().all()
        
        # Get call auction snapshots
        call_auction_stmt = (
            select(CMCallAuctionSnapshot)
            .where(CMCallAuctionSnapshot.security_token.in_(tokens))
            .order_by(CMCallAuctionSnapshot.timestamp.desc())
        )
        call_auction_result = await session.execute(call_auction_stmt)
        call_auction_snapshots = call_auction_result.scalars().all()
        
        # Step 3: Get latest data for each token (prefer regular market data over call auction)
        latest_snapshots = {}
        latest_call_auction = {}
        
        # Process regular snapshots first
        for snapshot in regular_snapshots:
            token = snapshot.security_token
            if token not in latest_snapshots:
                latest_snapshots[token] = snapshot
        
        # Process call auction snapshots
        for snapshot in call_auction_snapshots:
            token = snapshot.security_token
            if token not in latest_call_auction:
                latest_call_auction[token] = snapshot
        
        # Step 4: Format response with stock prices (prioritize regular market data)
        stocks_data = []
        token_to_symbol = {v: k for k, v in symbol_to_token.items()}
        
        # Get all tokens that have some data
        all_data_tokens = set(latest_snapshots.keys()) | set(latest_call_auction.keys())
        
        for token in all_data_tokens:
            symbol = token_to_symbol.get(token, f"TOKEN_{token}")
            details = symbol_details.get(symbol, {})
            
            # Use regular market data if available, otherwise use call auction data
            if token in latest_snapshots:
                snapshot = latest_snapshots[token]
                data_source = "regular_market"
                
                # Calculate percentage change
                percentage_change = 0.0
                if snapshot.open_price and snapshot.open_price > 0:
                    percentage_change = ((snapshot.last_traded_price - snapshot.open_price) / snapshot.open_price) * 100
                
                # Determine price trend
                trend = "neutral"
                if percentage_change > 0.5:
                    trend = "bullish"
                elif percentage_change < -0.5:
                    trend = "bearish"
                
                stocks_data.append({
                    "symbol": symbol,
                    "company_name": details.get('company_name', symbol),
                    "series": details.get('series', 'EQ'),
                    "token": token,
                    "data_source": data_source,
                    "current_price": snapshot.last_traded_price / 100.0 if snapshot.last_traded_price else 0.0,
                    "open_price": snapshot.open_price / 100.0 if snapshot.open_price else 0.0,
                    "high_price": snapshot.high_price / 100.0 if snapshot.high_price else 0.0,
                    "low_price": snapshot.low_price / 100.0 if snapshot.low_price else 0.0,
                    "close_price": snapshot.close_price / 100.0 if snapshot.close_price else 0.0,
                    "volume": snapshot.total_traded_quantity if snapshot.total_traded_quantity else 0,
                    "change": (snapshot.last_traded_price - snapshot.open_price) / 100.0 if (snapshot.last_traded_price and snapshot.open_price) else 0.0,
                    "change_percent": round(percentage_change, 2),
                    "trend": trend,
                    "best_buy_price": snapshot.best_buy_price / 100.0 if snapshot.best_buy_price else 0.0,
                    "best_sell_price": snapshot.best_sell_price / 100.0 if snapshot.best_sell_price else 0.0,
                    "average_traded_price": snapshot.average_traded_price / 100.0 if snapshot.average_traded_price else 0.0,
                    "last_updated": datetime.fromtimestamp(snapshot.timestamp).strftime("%Y-%m-%d %H:%M:%S") if snapshot.timestamp else None
                })
            
            elif token in latest_call_auction:
                snapshot = latest_call_auction[token]
                data_source = "call_auction"
                
                # Calculate percentage change for call auction
                percentage_change = 0.0
                if snapshot.open_price and snapshot.open_price > 0:
                    percentage_change = ((snapshot.last_traded_price - snapshot.open_price) / snapshot.open_price) * 100
                elif snapshot.first_open_price and snapshot.first_open_price > 0:
                    percentage_change = ((snapshot.last_traded_price - snapshot.first_open_price) / snapshot.first_open_price) * 100
                
                # Determine price trend
                trend = "neutral"
                if percentage_change > 0.5:
                    trend = "bullish"
                elif percentage_change < -0.5:
                    trend = "bearish"
                
                stocks_data.append({
                    "symbol": symbol,
                    "company_name": details.get('company_name', symbol),
                    "series": details.get('series', 'EQ'),
                    "token": token,
                    "data_source": data_source,
                    "current_price": snapshot.last_traded_price / 100.0 if snapshot.last_traded_price else 0.0,
                    "open_price": snapshot.open_price / 100.0 if snapshot.open_price else 0.0,
                    "first_open_price": snapshot.first_open_price / 100.0 if snapshot.first_open_price else 0.0,
                    "high_price": snapshot.high_price / 100.0 if snapshot.high_price else 0.0,
                    "low_price": snapshot.low_price / 100.0 if snapshot.low_price else 0.0,
                    "close_price": snapshot.close_price / 100.0 if snapshot.close_price else 0.0,
                    "volume": snapshot.total_traded_quantity if snapshot.total_traded_quantity else 0,
                    "indicative_volume": snapshot.indicative_traded_quantity if snapshot.indicative_traded_quantity else 0,
                    "change": (snapshot.last_traded_price - (snapshot.open_price or snapshot.first_open_price)) / 100.0 if (snapshot.last_traded_price and (snapshot.open_price or snapshot.first_open_price)) else 0.0,
                    "change_percent": round(percentage_change, 2),
                    "trend": trend,
                    "best_buy_price": snapshot.best_buy_price / 100.0 if snapshot.best_buy_price else 0.0,
                    "best_sell_price": snapshot.best_sell_price / 100.0 if snapshot.best_sell_price else 0.0,
                    "average_traded_price": snapshot.average_traded_price / 100.0 if snapshot.average_traded_price else 0.0,
                    "buy_bbmm_flag": getattr(snapshot, 'buy_bbmm_flag', None),
                    "sell_bbmm_flag": getattr(snapshot, 'sell_bbmm_flag', None),
                    "last_updated": datetime.fromtimestamp(snapshot.timestamp).strftime("%Y-%m-%d %H:%M:%S") if snapshot.timestamp else None
                })
        
        # Step 5: Add missing stocks (जो database में नहीं हैं)
        found_symbols = {stock["symbol"] for stock in stocks_data}
        missing_symbols = [symbol for symbol in stock_symbols if symbol not in found_symbols]
        
        for symbol in missing_symbols:
            stocks_data.append({
                "symbol": symbol,
                "token": None,
                "current_price": 0.0,
                "status": "No data available",
                "trend": "unknown"
            })
        
        # Sort by current price descending
        stocks_data.sort(key=lambda x: x.get("current_price", 0), reverse=True)
        
        # Calculate index summary
        total_stocks = len(stock_symbols)
        available_stocks = len([s for s in stocks_data if s.get("current_price", 0) > 0])
        gainers = len([s for s in stocks_data if s.get("change_percent", 0) > 0])
        losers = len([s for s in stocks_data if s.get("change_percent", 0) < 0])
        
        return {
            "status": "success",
            "index_name": index_display_name,
            "index_key": index_name.lower(),
            "summary": {
                "total_stocks": total_stocks,
                "available_data": available_stocks,
                "gainers": gainers,
                "losers": losers,
                "unchanged": available_stocks - gainers - losers
            },
            "stocks": stocks_data,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching {index_name} stocks: {str(e)}")

@router.get("/available-indices")
async def get_available_indices():
    """
    Get list of all available indices
    """
    return {
        "status": "success",
        "available_indices": INDEX_COMPOSITIONS["all"],
        "count": len(INDEX_COMPOSITIONS["all"])
    }

@router.get("/stocks/{index_name}/top-performers")
async def get_top_performers(
    index_name: str,
    limit: int = Query(10, ge=1, le=50),
    session: AsyncSession = Depends(get_db)
):
    """
    Get top performing stocks from an index
    """
    if index_name.lower() not in INDEX_COMPOSITIONS:
        raise HTTPException(status_code=404, detail=f"Index '{index_name}' not found")
    
    # Get all stocks data (reusing previous function logic)
    response = await get_index_stocks_prices(index_name, session)
    stocks = response["stocks"]
    
    # Filter only stocks with data and sort by percentage change
    valid_stocks = [s for s in stocks if s.get("current_price", 0) > 0]
    
    # Top gainers
    top_gainers = sorted(valid_stocks, key=lambda x: x.get("change_percent", 0), reverse=True)[:limit]
    
    # Top losers
    top_losers = sorted(valid_stocks, key=lambda x: x.get("change_percent", 0))[:limit]
    
    # Most traded (by volume)
    most_traded = sorted(valid_stocks, key=lambda x: x.get("volume", 0), reverse=True)[:limit]
    
    return {
        "status": "success",
        "index_name": response["index_name"],
        "top_gainers": top_gainers,
        "top_losers": top_losers,
        "most_traded": most_traded,
        "timestamp": datetime.now().isoformat()
    }

@router.get("/gainers-losers/{index_name}")
async def get_gainers_losers(
    index_name: str,
    limit: int = Query(10, ge=1, le=50),
    session: AsyncSession = Depends(get_db)
):
    """
    Get detailed gainers and losers analysis for an index
    
    Example:
    - /api/indices/gainers-losers/nifty50?limit=10
    - /api/indices/gainers-losers/niftyIT?limit=5
    """
    if index_name.lower() not in INDEX_COMPOSITIONS:
        raise HTTPException(status_code=404, detail=f"Index '{index_name}' not found")
    
    # Get all stocks data
    response = await get_index_stocks_prices(index_name, session)
    stocks = response["stocks"]
    
    # Filter only stocks with valid data
    valid_stocks = [s for s in stocks if s.get("current_price", 0) > 0]
    
    if not valid_stocks:
        raise HTTPException(status_code=404, detail=f"No valid data found for {index_name}")
    
    # Categorize stocks by performance
    gainers = [s for s in valid_stocks if s.get("change_percent", 0) > 0]
    losers = [s for s in valid_stocks if s.get("change_percent", 0) < 0]
    unchanged = [s for s in valid_stocks if s.get("change_percent", 0) == 0]
    
    # Sort gainers and losers
    top_gainers = sorted(gainers, key=lambda x: x.get("change_percent", 0), reverse=True)[:limit]
    top_losers = sorted(losers, key=lambda x: x.get("change_percent", 0))[:limit]
    
    # Calculate statistics
    total_stocks = len(valid_stocks)
    gainers_count = len(gainers)
    losers_count = len(losers)
    unchanged_count = len(unchanged)
    
    # Market sentiment analysis
    if gainers_count > losers_count * 1.5:
        market_sentiment = "bullish"
    elif losers_count > gainers_count * 1.5:
        market_sentiment = "bearish"
    else:
        market_sentiment = "neutral"
    
    # Calculate average changes
    avg_gainer_change = sum(s.get("change_percent", 0) for s in gainers) / len(gainers) if gainers else 0
    avg_loser_change = sum(s.get("change_percent", 0) for s in losers) / len(losers) if losers else 0
    
    # Volume analysis
    total_gainer_volume = sum(s.get("volume", 0) for s in gainers)
    total_loser_volume = sum(s.get("volume", 0) for s in losers)
    
    return {
        "status": "success",
        "index_name": response["index_name"],
        "index_key": index_name.lower(),
        "market_analysis": {
            "sentiment": market_sentiment,
            "total_stocks_analyzed": total_stocks,
            "gainers_count": gainers_count,
            "losers_count": losers_count,
            "unchanged_count": unchanged_count,
            "gainers_percentage": round((gainers_count / total_stocks) * 100, 2),
            "losers_percentage": round((losers_count / total_stocks) * 100, 2),
            "avg_gainer_change": round(avg_gainer_change, 2),
            "avg_loser_change": round(avg_loser_change, 2),
            "gainer_volume": total_gainer_volume,
            "loser_volume": total_loser_volume
        },
        "top_gainers": top_gainers,
        "top_losers": top_losers,
        "performance_brackets": {
            "strong_gainers": [s for s in gainers if s.get("change_percent", 0) >= 5.0],
            "moderate_gainers": [s for s in gainers if 1.0 <= s.get("change_percent", 0) < 5.0],
            "weak_gainers": [s for s in gainers if 0 < s.get("change_percent", 0) < 1.0],
            "weak_losers": [s for s in losers if -1.0 < s.get("change_percent", 0) < 0],
            "moderate_losers": [s for s in losers if -5.0 < s.get("change_percent", 0) <= -1.0],
            "strong_losers": [s for s in losers if s.get("change_percent", 0) <= -5.0]
        },
        "timestamp": datetime.now().isoformat()
    }

@router.get("/market-movers")
async def get_market_movers(
    indices: List[str] = Query(["nifty50", "nifty100", "niftyBank", "niftyIT"], description="List of indices to analyze"),
    limit: int = Query(5, ge=1, le=20),
    session: AsyncSession = Depends(get_db)
):
    """
    Get top market movers across multiple indices
    
    Example:
    - /api/indices/market-movers?indices=nifty50,niftyBank&limit=5
    """
    
    all_gainers = []
    all_losers = []
    index_summaries = {}
    
    for index_name in indices:
        if index_name.lower() not in INDEX_COMPOSITIONS:
            continue
            
        try:
            # Get stocks data for this index
            response = await get_index_stocks_prices(index_name, session)
            stocks = response["stocks"]
            valid_stocks = [s for s in stocks if s.get("current_price", 0) > 0]
            
            if not valid_stocks:
                continue
            
            # Get gainers and losers for this index
            gainers = [s for s in valid_stocks if s.get("change_percent", 0) > 0]
            losers = [s for s in valid_stocks if s.get("change_percent", 0) < 0]
            
            # Add index info to each stock
            for stock in gainers:
                stock["source_index"] = index_name
                all_gainers.append(stock)
            
            for stock in losers:
                stock["source_index"] = index_name
                all_losers.append(stock)
            
            # Store index summary
            index_summaries[index_name] = {
                "index_name": response["index_name"],
                "total_stocks": len(valid_stocks),
                "gainers": len(gainers),
                "losers": len(losers),
                "avg_change": round(sum(s.get("change_percent", 0) for s in valid_stocks) / len(valid_stocks), 2) if valid_stocks else 0
            }
            
        except Exception as e:
            # Skip this index if there's an error
            continue
    
    # Remove duplicates (same stock in multiple indices) and keep the best performer
    unique_gainers = {}
    unique_losers = {}
    
    for stock in all_gainers:
        symbol = stock["symbol"]
        if symbol not in unique_gainers or stock["change_percent"] > unique_gainers[symbol]["change_percent"]:
            unique_gainers[symbol] = stock
    
    for stock in all_losers:
        symbol = stock["symbol"]
        if symbol not in unique_losers or stock["change_percent"] < unique_losers[symbol]["change_percent"]:
            unique_losers[symbol] = stock
    
    # Sort and limit
    top_gainers = sorted(unique_gainers.values(), key=lambda x: x.get("change_percent", 0), reverse=True)[:limit]
    top_losers = sorted(unique_losers.values(), key=lambda x: x.get("change_percent", 0))[:limit]
    
    return {
        "status": "success",
        "analysis_scope": indices,
        "market_movers": {
            "top_gainers": top_gainers,
            "top_losers": top_losers
        },
        "index_summaries": index_summaries,
        "overall_stats": {
            "total_unique_gainers": len(unique_gainers),
            "total_unique_losers": len(unique_losers),
            "indices_analyzed": len(index_summaries)
        },
        "timestamp": datetime.now().isoformat()
    }

@router.get("/performance-comparison")
async def get_performance_comparison(
    indices: List[str] = Query(["nifty50", "niftyBank", "niftyIT"], description="Indices to compare"),
    session: AsyncSession = Depends(get_db)
):
    """
    Compare performance across multiple indices
    
    Example:
    - /api/indices/performance-comparison?indices=nifty50,niftyBank,niftyIT
    """
    
    comparison_data = {}
    
    for index_name in indices:
        if index_name.lower() not in INDEX_COMPOSITIONS:
            continue
            
        try:
            response = await get_index_stocks_prices(index_name, session)
            stocks = response["stocks"]
            valid_stocks = [s for s in stocks if s.get("current_price", 0) > 0]
            
            if not valid_stocks:
                continue
            
            # Calculate performance metrics
            gainers = [s for s in valid_stocks if s.get("change_percent", 0) > 0]
            losers = [s for s in valid_stocks if s.get("change_percent", 0) < 0]
            
            avg_change = sum(s.get("change_percent", 0) for s in valid_stocks) / len(valid_stocks)
            max_gain = max(s.get("change_percent", 0) for s in valid_stocks)
            max_loss = min(s.get("change_percent", 0) for s in valid_stocks)
            
            total_volume = sum(s.get("volume", 0) for s in valid_stocks)
            
            comparison_data[index_name] = {
                "index_name": response["index_name"],
                "total_stocks": len(valid_stocks),
                "gainers_count": len(gainers),
                "losers_count": len(losers),
                "gainers_percentage": round((len(gainers) / len(valid_stocks)) * 100, 2),
                "average_change": round(avg_change, 2),
                "max_gainer": max_gain,
                "max_loser": max_loss,
                "total_volume": total_volume,
                "performance_rating": "positive" if avg_change > 0.5 else "negative" if avg_change < -0.5 else "neutral"
            }
            
        except Exception as e:
            continue
    
    # Rank indices by performance
    ranked_indices = sorted(
        comparison_data.items(), 
        key=lambda x: x[1]["average_change"], 
        reverse=True
    )
    
    return {
        "status": "success",
        "comparison_data": comparison_data,
        "performance_ranking": [
            {
                "rank": i + 1,
                "index_key": index_key,
                "index_name": data["index_name"],
                "average_change": data["average_change"],
                "performance_rating": data["performance_rating"]
            }
            for i, (index_key, data) in enumerate(ranked_indices)
        ],
        "best_performer": ranked_indices[0][0] if ranked_indices else None,
        "worst_performer": ranked_indices[-1][0] if ranked_indices else None,
        "timestamp": datetime.now().isoformat()
    }

@router.get("/stocks/{index_name}/summary")
async def get_index_summary(
    index_name: str,
    session: AsyncSession = Depends(get_db)
):
    """
    Get summarized data for an index (market cap weighted if possible)
    """
    if index_name.lower() not in INDEX_COMPOSITIONS:
        raise HTTPException(status_code=404, detail=f"Index '{index_name}' not found")
    
    response = await get_index_stocks_prices(index_name, session)
    stocks = response["stocks"]
    valid_stocks = [s for s in stocks if s.get("current_price", 0) > 0]
    
    if not valid_stocks:
        raise HTTPException(status_code=404, detail=f"No valid data found for {index_name}")
    
    # Calculate aggregate metrics
    total_market_value = sum(s["current_price"] * s.get("volume", 1) for s in valid_stocks)
    avg_change = sum(s.get("change_percent", 0) for s in valid_stocks) / len(valid_stocks)
    
    # Price ranges
    highest_price = max(s["current_price"] for s in valid_stocks)
    lowest_price = min(s["current_price"] for s in valid_stocks)
    
    # Volume analysis
    total_volume = sum(s.get("volume", 0) for s in valid_stocks)
    
    return {
        "status": "success",
        "index_name": response["index_name"],
        "summary": {
            "total_stocks": response["summary"]["total_stocks"],
            "active_stocks": len(valid_stocks),
            "average_change_percent": round(avg_change, 2),
            "total_market_value": total_market_value,
            "highest_stock_price": highest_price,
            "lowest_stock_price": lowest_price,
            "total_volume": total_volume,
            "gainers_count": response["summary"]["gainers"],
            "losers_count": response["summary"]["losers"]
        },
        "timestamp": datetime.now().isoformat()
    }

from sqlalchemy import and_

from sqlalchemy import and_
from decimal import Decimal

@router.get("/stocks/{index_name}/52w-high")
async def get_top_52w_high(
    index_name: str,
    limit: int = Query(10, ge=1, le=100),
    session: AsyncSession = Depends(get_db),
):
    key = index_name.lower()
    if key not in INDEX_COMPOSITIONS:
        raise HTTPException(404, f"Index '{index_name}' not found")

    # 1) EQ-series tokens + company info
    tok_q = (
        select(
            CMTokenMaster.token_number,
            CMTokenMaster.symbol,
            CMTokenMaster.company_name,
            CMTokenMaster.series,
        )
        .where(
            CMTokenMaster.symbol.in_(INDEX_COMPOSITIONS[key]),
            CMTokenMaster.series == "EQ",
        )
    )
    rows = await session.execute(tok_q)
    symbol_info = {
        sym: {"token": tok, "company_name": nm.strip(), "series": ser}
        for tok, sym, nm, ser in rows
    }
    if not symbol_info:
        raise HTTPException(404, "No EQ-series symbols for this index")

    # 2) 52-week cutoff
    cutoff = int((datetime.utcnow() - timedelta(weeks=52)).timestamp())

    # 3) 52-wk highs
    high_rows = (
        await session.execute(
            select(
                CMStockHistorical.symbol,
                func.max(CMStockHistorical.high_price).label("high_52w"),
            )
            .where(
                CMStockHistorical.symbol.in_(symbol_info.keys()),
                CMStockHistorical.timestamp >= cutoff,
            )
            .group_by(CMStockHistorical.symbol)
        )
    ).all()

    results = []
    for sym, high_52w in high_rows:
        info = symbol_info[sym]
        token = info["token"]

        # 4) latest CMSnapshot
        reg = await session.execute(
            select(CMSnapshot).where(CMSnapshot.security_token == token)
                              .order_by(CMSnapshot.timestamp.desc())  # Changed from .id.desc()
                              .limit(1)
        )
        snap = reg.scalars().first()

        # 5) fallback to call‐auction if no regular
        if not snap:
            ca = await session.execute(
                select(CMCallAuctionSnapshot)
                .where(CMCallAuctionSnapshot.security_token == token)
                .order_by(CMCallAuctionSnapshot.timestamp.desc())  # Changed from .id.desc()
                .limit(1)
            )
            snap = ca.scalars().first()

        # 6) extract & convert - Fixed the attribute access and conversion
        if snap:
            def to_float(x): 
                if x is None:
                    return None
                if isinstance(x, Decimal):
                    return float(x) / 100.0
                return float(x) / 100.0 if x else None

            # Handle both CMSnapshot and CMCallAuctionSnapshot
            last = to_float(snap.last_traded_price)
            op   = to_float(snap.open_price)
            hi   = to_float(snap.high_price)
            lo   = to_float(snap.low_price)
            cl   = to_float(snap.close_price)
            
            # Calculate change if possible
            change_percent = 0.0
            if last and op and op > 0:
                change_percent = ((last - op) / op) * 100
        else:
            last = op = hi = lo = cl = None
            change_percent = 0.0

        results.append({
            "symbol": sym,
            "company_name": info["company_name"],
            "series": info["series"],
            "token": token,
            "52w_high": float(high_52w) / 100.0 if high_52w is not None else None,
            "current_price": last,
            "open": op,
            "high": hi,
            "low": lo,
            "close": cl,
            "change_percent": round(change_percent, 2),
            "has_live_data": snap is not None,
            "data_source": "regular_market" if token in [s.security_token for s in [snap] if hasattr(s, 'security_token') and isinstance(s, CMSnapshot)] else "call_auction" if snap else "none"
        })

    # Sort by 52w high and limit
    top = sorted(results, key=lambda x: x["52w_high"] or 0, reverse=True)[:limit]
    
    return {
        "status": "success",
        "index_name": INDEX_COMPOSITIONS["all"].get(key, index_name),
        "top_52w_high": top,
        "data_summary": {
            "total_symbols": len(symbol_info),
            "symbols_with_52w_data": len(high_rows),
            "symbols_with_live_data": len([r for r in results if r["has_live_data"]])
        },
        "timestamp": datetime.utcnow().isoformat(),
    }


@router.get("/stocks/{index_name}/52w-low")
async def get_top_52w_low(
    index_name: str,
    limit: int = Query(10, ge=1, le=100),
    session: AsyncSession = Depends(get_db),
):
    key = index_name.lower()
    if key not in INDEX_COMPOSITIONS:
        raise HTTPException(404, f"Index '{index_name}' not found")

    # 1) EQ-series tokens
    tok_q = (
        select(
            CMTokenMaster.token_number,
            CMTokenMaster.symbol,
            CMTokenMaster.company_name,
            CMTokenMaster.series,
        )
        .where(
            CMTokenMaster.symbol.in_(INDEX_COMPOSITIONS[key]),
            CMTokenMaster.series == "EQ",
        )
    )
    rows = await session.execute(tok_q)
    symbol_info = {
        sym: {"token": tok, "company_name": nm.strip(), "series": ser}
        for tok, sym, nm, ser in rows
    }
    if not symbol_info:
        raise HTTPException(404, "No EQ-series symbols for this index")

    # 2) 52-wk cutoff
    cutoff = int((datetime.utcnow() - timedelta(weeks=52)).timestamp())

    # 3) 52-wk lows
    low_rows = (
        await session.execute(
            select(
                CMStockHistorical.symbol,
                func.min(CMStockHistorical.low_price).label("low_52w"),
            )
            .where(
                CMStockHistorical.symbol.in_(symbol_info.keys()),
                CMStockHistorical.timestamp >= cutoff,
            )
            .group_by(CMStockHistorical.symbol)
        )
    ).all()

    results = []
    for sym, low_52w in low_rows:
        info = symbol_info[sym]
        token = info["token"]

        # 4) latest CMSnapshot
        reg = await session.execute(
            select(CMSnapshot).where(CMSnapshot.security_token == token)
                              .order_by(CMSnapshot.timestamp.desc())  # Changed from .id.desc()
                              .limit(1)
        )
        snap = reg.scalars().first()

        # 5) fallback to call‐auction
        if not snap:
            ca = await session.execute(
                select(CMCallAuctionSnapshot)
                .where(CMCallAuctionSnapshot.security_token == token)
                .order_by(CMCallAuctionSnapshot.timestamp.desc())  # Changed from .id.desc()
                .limit(1)
            )
            snap = ca.scalars().first()

        # 6) extract & convert - Fixed the attribute access and conversion
        if snap:
            def to_float(x): 
                if x is None:
                    return None
                if isinstance(x, Decimal):
                    return float(x) / 100.0
                return float(x) / 100.0 if x else None

            # Handle both CMSnapshot and CMCallAuctionSnapshot
            last = to_float(snap.last_traded_price)
            op   = to_float(snap.open_price)
            hi   = to_float(snap.high_price)
            lo   = to_float(snap.low_price)
            cl   = to_float(snap.close_price)
            
            # Calculate change if possible
            change_percent = 0.0
            if last and op and op > 0:
                change_percent = ((last - op) / op) * 100
        else:
            last = op = hi = lo = cl = None
            change_percent = 0.0

        results.append({
            "symbol": sym,
            "company_name": info["company_name"],
            "series": info["series"],
            "token": token,
            "52w_low": float(low_52w) / 100.0 if low_52w is not None else None,
            "current_price": last,
            "open": op,
            "high": hi,
            "low": lo,
            "close": cl,
            "change_percent": round(change_percent, 2),
            "has_live_data": snap is not None,
            "data_source": "regular_market" if token in [s.security_token for s in [snap] if hasattr(s, 'security_token') and isinstance(s, CMSnapshot)] else "call_auction" if snap else "none"
        })

    # Sort by 52w low (ascending for lowest first)
    top = sorted(results, key=lambda x: x["52w_low"] or float("inf"))[:limit]
    
    return {
        "status": "success",
        "index_name": INDEX_COMPOSITIONS["all"].get(key, index_name),
        "top_52w_low": top,
        "data_summary": {
            "total_symbols": len(symbol_info),
            "symbols_with_52w_data": len(low_rows),
            "symbols_with_live_data": len([r for r in results if r["has_live_data"]])
        },
        "timestamp": datetime.utcnow().isoformat(),
    }

