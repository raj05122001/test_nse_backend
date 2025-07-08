from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, Union

from db.connection import get_db
from db.models import CMSnapshot, CMContractStreamInfo
from db.schema import (
    CMSnapshot as CMSnapshotSchema,
    SnapshotListResponse,
    CMContractStreamInfo as ContractInfoSchema,
    ContractStreamInfoListResponse,
)

router = APIRouter(prefix="/api", tags=["rest"])

# Existing endpoints...

@router.get(
    "/snapshots/search",
    response_model=CMSnapshotSchema,
    description="Get latest snapshot by token number or stock symbol"
)
async def search_latest_snapshot(
    query: str = Query(..., description="Security token number or stock symbol (e.g., '22' or 'RELIANCE')"),
    session: AsyncSession = Depends(get_db),
):
    """
    Search for latest market snapshot by:
    - Token number (e.g., 22, 1594, 2885)
    - Stock symbol (e.g., RELIANCE, TCS, INFY)
    """
    
    # Try to determine if query is a number (token) or text (symbol)
    try:
        # If it's a number, search by token
        token = int(query)
        stmt = (
            select(CMSnapshot)
            .where(CMSnapshot.security_token == token)
            .order_by(CMSnapshot.timestamp.desc())
            .limit(1)
        )
        result = await session.execute(stmt)
        snapshot = result.scalar_one_or_none()
        
        if snapshot:
            return snapshot
            
    except ValueError:
        # If not a number, it's probably a symbol
        pass
    
    # Search by symbol name in contract info first
    symbol_upper = query.upper()
    
    # First, find the token for this symbol
    symbol_stmt = (
        select(CMContractStreamInfo.symbol_token)
        .where(CMContractStreamInfo.symbol.ilike(f"%{symbol_upper}%"))
        .limit(1)
    )
    symbol_result = await session.execute(symbol_stmt)
    token_row = symbol_result.first()
    
    if token_row:
        security_token = token_row[0]
        
        # Now get the latest snapshot for this token
        stmt = (
            select(CMSnapshot)
            .where(CMSnapshot.security_token == security_token)
            .order_by(CMSnapshot.timestamp.desc())
            .limit(1)
        )
        result = await session.execute(stmt)
        snapshot = result.scalar_one_or_none()
        
        if snapshot:
            return snapshot
    
    # If still not found, try fuzzy search
    fuzzy_stmt = (
        select(CMContractStreamInfo.symbol_token)
        .where(
            or_(
                CMContractStreamInfo.symbol.ilike(f"{symbol_upper}%"),
                CMContractStreamInfo.symbol.ilike(f"%{symbol_upper}"),
            )
        )
        .limit(1)
    )
    fuzzy_result = await session.execute(fuzzy_stmt)
    fuzzy_token = fuzzy_result.first()
    
    if fuzzy_token:
        security_token = fuzzy_token[0]
        stmt = (
            select(CMSnapshot)
            .where(CMSnapshot.security_token == security_token)
            .order_by(CMSnapshot.timestamp.desc())
            .limit(1)
        )
        result = await session.execute(stmt)
        snapshot = result.scalar_one_or_none()
        
        if snapshot:
            return snapshot
    
    raise HTTPException(
        status_code=404, 
        detail=f"No snapshot found for '{query}'. Try token number or exact stock symbol."
    )


@router.get(
    "/snapshots/search/suggestions",
    description="Get symbol suggestions for search"
)
async def get_symbol_suggestions(
    q: str = Query(..., min_length=2, description="Search query (minimum 2 characters)"),
    limit: int = Query(10, ge=1, le=50, description="Maximum number of suggestions"),
    session: AsyncSession = Depends(get_db),
):
    """
    Get stock symbol suggestions for autocomplete
    """
    search_term = q.upper()
    
    stmt = (
        select(
            CMContractStreamInfo.symbol,
            CMContractStreamInfo.symbol_token,
            CMContractStreamInfo.instrument_type
        )
        .where(
            or_(
                CMContractStreamInfo.symbol.ilike(f"{search_term}%"),
                CMContractStreamInfo.symbol.ilike(f"%{search_term}%")
            )
        )
        .order_by(CMContractStreamInfo.symbol)
        .limit(limit)
    )
    
    result = await session.execute(stmt)
    suggestions = result.fetchall()
    
    return {
        "suggestions": [
            {
                "symbol": row[0],
                "token": row[1],
                "type": row[2],
                "search_query": f"{row[0]} (Token: {row[1]})"
            }
            for row in suggestions
        ],
        "count": len(suggestions)
    }