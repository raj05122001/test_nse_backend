import asyncio

import uvicorn
from fastapi import FastAPI

from routers.rest import router as rest_router
from routers.websocket import router as ws_router
from services.broadcaster import broadcast_loop
from services.sftp_watcher import start_sftp_watcher

app = FastAPI(
    title="NSE Market Data API",
    version="0.1.0",
    description="REST and WebSocket API for NSE market snapshots"
)

# Include routers
app.include_router(rest_router)
app.include_router(ws_router)

@app.get("/", include_in_schema=False)
async def root():
    return {"message": "NSE API is running"}

@app.on_event("startup")
async def on_startup():
    """
    Start background tasks on application startup:
    - SFTP watcher polling for new snapshot files
    - Broadcaster loop to push parsed data to WebSocket clients
    """
    # Load settings (implicitly via import)
    # Start broadcaster
    asyncio.create_task(broadcast_loop())
    # Start SFTP watcher
    asyncio.create_task(start_sftp_watcher())

@app.on_event("shutdown")
async def on_shutdown():
    """
    Graceful shutdown for background tasks if needed.
    """
    # TODO: signal tasks to stop if you implement termination logic
    pass

if __name__ == "__main__":
    # Run with reload in development
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
