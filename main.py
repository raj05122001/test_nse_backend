import asyncio
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI

from routers.rest import router as rest_router
from routers.websocket import router as ws_router
from routers.market import router as market_router
from services.broadcaster import broadcast_loop
from services.sftp_watcher import start_sftp_watcher
from db.connection import engine, Base

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    try:
        # Create database tables
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        
        # Start background tasks
        broadcast_task = asyncio.create_task(broadcast_loop())
        sftp_task = asyncio.create_task(start_sftp_watcher())
        
        yield
        
    finally:
        # Shutdown
        broadcast_task.cancel()
        sftp_task.cancel()
        await engine.dispose()

app = FastAPI(
    title="NSE Market Data API",
    version="0.1.0",
    description="REST and WebSocket API for NSE market snapshots",
    lifespan=lifespan
)

# Include routers
app.include_router(market_router)
app.include_router(rest_router)
app.include_router(ws_router)

@app.get("/", include_in_schema=False)
async def root():
    return {"message": "NSE API is running"}

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )