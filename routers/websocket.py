from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from services.broadcaster import manager

router = APIRouter(tags=["websocket"])

@router.websocket("/ws/market")
async def websocket_market(websocket: WebSocket):
    """
    WebSocket endpoint for real-time market snapshot streaming.
    Clients connect here and receive JSON batches from the broadcast queue.
    """
    # Accept connection and register
    await manager.connect(websocket)
    try:
        while True:
            # Await any client message to detect disconnects. We don't process incoming messages.
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
