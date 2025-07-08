# app/routers/rest.py

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional

from db.connection import get_db
from db.models import CMSnapshot, CMContractStreamInfo
from db.schema import (
    CMSnapshot as CMSnapshotSchema,
    SnapshotListResponse,
    CMContractStreamInfo as ContractInfoSchema,
    ContractStreamInfoListResponse,
)

router = APIRouter(prefix="/api", tags=["rest"])


@router.get(
    "/snapshots/latest/{token}",
    response_model=CMSnapshotSchema,
    description="Get the most recent CM snapshot for the given security token",
)
async def get_latest_snapshot(
    token: int,
    session: AsyncSession = Depends(get_db),
):
    stmt = (
        select(CMSnapshot)
        .where(CMSnapshot.security_token == token)
        .order_by(CMSnapshot.timestamp.desc())
        .limit(1)
    )
    result = await session.execute(stmt)
    snapshot = result.scalar_one_or_none()
    if not snapshot:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    return snapshot


@router.get(
    "/snapshots/history",
    response_model=SnapshotListResponse,
    description="Get CM snapshot history for a token within [start_ts, end_ts]",
)
async def get_snapshot_history(
    token: int = Query(..., description="Security token"),
    start_ts: int = Query(..., description="Start timestamp (epoch seconds)"),
    end_ts: int = Query(..., description="End timestamp (epoch seconds)"),
    session: AsyncSession = Depends(get_db),
):
    stmt = (
        select(CMSnapshot)
        .where(
            CMSnapshot.security_token == token,
            CMSnapshot.timestamp >= start_ts,
            CMSnapshot.timestamp <= end_ts,
        )
        .order_by(CMSnapshot.timestamp)
    )
    result = await session.execute(stmt)
    snapshots = result.scalars().all()
    return SnapshotListResponse(snapshots=snapshots)


@router.get(
    "/contracts/cm",
    response_model=ContractStreamInfoListResponse,
    description="List all CM contract stream info records",
)
async def list_cm_contracts(
    session: AsyncSession = Depends(get_db),
):
    stmt = select(CMContractStreamInfo)
    result = await session.execute(stmt)
    records = result.scalars().all()
    return ContractStreamInfoListResponse(contracts=records)
