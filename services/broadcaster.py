import asyncio
import json
from typing import Any, Dict, List

from fastapi import WebSocket
from utils.logger import get_logger

logger = get_logger(__name__)

# Global queue for publishing market snapshots
data_queue: asyncio.Queue[List[Dict[str, Any]]] = asyncio.Queue()

class ConnectionManager:
    """
    Manages WebSocket connections and broadcasting messages to all clients.
    """
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket) -> None:
        """
        Accept and register a new WebSocket connection.
        """
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket client connected: {websocket.client}")

    def disconnect(self, websocket: WebSocket) -> None:
        """
        Remove a WebSocket connection.
        """
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"WebSocket client disconnected: {websocket.client}")

    async def broadcast(self, message: str) -> None:
        """
        Send a text message to all active connections.
        Removes any connections that error out.
        """
        disconnected: List[WebSocket] = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception as e:
                logger.error(f"Error sending message to {connection.client}: {e}")
                disconnected.append(connection)

        for conn in disconnected:
            self.disconnect(conn)

# Singleton manager instance
manager = ConnectionManager()

async def broadcast_loop() -> None:
    """
    Background task: consume lists of records from data_queue
    and broadcast JSON-encoded payloads to all clients.
    """
    logger.info("Starting broadcaster loop...")
    while True:
        # Wait for a batch of records
        records = await data_queue.get()
        try:
            # Serialize to JSON (list of dicts)
            payload = json.dumps(records, default=str)
            await manager.broadcast(payload)
            logger.debug(f"Broadcasted {len(records)} records to {len(manager.active_connections)} clients")
        except Exception as e:
            logger.error(f"Broadcast error: {e}")

async def publish_data(records: List[Dict[str, Any]]) -> None:
    """
    Put a list of parsed snapshot records onto the queue
    to be broadcast to WebSocket clients.
    """
    await data_queue.put(records)
    logger.debug(f"Published {len(records)} records to broadcast queue")
