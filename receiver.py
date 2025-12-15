from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from typing import Any, Dict, Set
from contextlib import asynccontextmanager
import json
import os
import asyncio
import logging
from utils.kmw import PyKafBridge

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka setup
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
TOPIC      = os.getenv("KAFKA_TOPIC","network.data.ingested")


kafka_bridge = None


# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()
        self._lock = asyncio.Lock()
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        async with self._lock:
            self.active_connections.add(websocket)
        logger.info(f"WebSocket connected. Total connections: {len(self.active_connections)}")
    
    async def disconnect(self, websocket: WebSocket):
        async with self._lock:
            self.active_connections.discard(websocket)
        logger.info(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")
    
    async def broadcast(self, message: dict):
        """Broadcast message to all connected clients"""
        disconnected = set()
        
        async with self._lock:
            connections = self.active_connections.copy()
        
        for connection in connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Error broadcasting to client: {e}")
                disconnected.add(connection)
        
        if disconnected:
            async with self._lock:
                self.active_connections -= disconnected


manager = ConnectionManager()


@asynccontextmanager
async def lifespan(app: FastAPI):

    # Startup: Start Kafka bridge
    global kafka_bridge
    kafka_bridge = PyKafBridge(TOPIC, hostname=KAFKA_HOST, port=KAFKA_PORT)

    yield

    # Shutdown: Stop Kafka bridge
    await kafka_bridge.close()

# Initialize FastAPI app
app = FastAPI(lifespan=lifespan)

# Can be expanded later
class DataPacket(BaseModel):
    data: Dict[str, Any]

# Fields to extract and send to Kafka (can be expanded later)
REQUIRED_FIELDS = [
    "timestamp",
    "datarate",
    "mean_latency",
    "rsrp",
    "sinr",
    "rsrq",
    "direction",
    "network",
    "cqi",
    "cell_index",
    "primary_bandwidth",
    "ul_bandwidth",
    "latitude",
    "longitude",
    "altitude",
    "velocity",
]

@app.post("/receive")
async def receive_data(request: Request):
    """Receive a data packet (dict) and return only the required fields.

    For any REQUIRED_FIELDS key missing from the incoming packet, the
    returned value will be None.
    """
    payload = await request.json()
    data = payload or {}

    print("Received:", data)
    results = []

    # Producer sends a batch under analyticsData where each element contains analyticsMetadata with the measurements
    if isinstance(data, dict) and "analyticsData" in data:
        analytics_list = data.get("analyticsData") or []
        for entry in analytics_list:
            meta = entry.get("analyticsMetadata", {}) if isinstance(entry, dict) else {}

            #ts = meta.get("timestamp") if meta.get("timestamp") is not None else entry.get("timestamp")

            filtered = {}
            for field in REQUIRED_FIELDS:
                if field == "timestamp":
                    filtered[field] = entry.get("timestamp")
                else:
                    filtered[field] = meta.get(field)

            message = json.dumps(filtered)

            if kafka_bridge is None:
                print("Kafka bridge not available - skipping produce (batch entry)")
                results.append({"status": "no-kafka", "data": filtered})
                continue

            try:
                ok = kafka_bridge.produce(TOPIC, message)
            except Exception:
                ok = False

            if ok:
                results.append({"status": "ok", "data": filtered})
                # Broadcast to WebSocket clients
                asyncio.create_task(manager.broadcast({
                    "type": "data_ingested",
                    "data": filtered
                }))
            else:
                results.append({"status": "error", "message": "Failed to send to Kafka", "data": filtered})

        print(results)

        return {"results": results}

    # Fallback
    filtered = {k: data.get(k) for k in REQUIRED_FIELDS}
    message = json.dumps(filtered)

    # Send to Kafka if available
    if kafka_bridge is None:
        print("Kafka bridge not available - skipping produce")
        return {"status": "no-kafka", "data": filtered}

    try:
        success = kafka_bridge.produce(TOPIC, message)
    except Exception:
        success = False

    if success:
        # Broadcast to WebSocket clients
        asyncio.create_task(manager.broadcast({
            "type": "data_ingested",
            "data": filtered
        }))
        return {"status": "ok", "data": filtered}
    else:
        return {"status": "error", "message": "Failed to send to Kafka"}


@app.websocket("/ws/ingestion")
async def websocket_ingestion(websocket: WebSocket):
    """
    WebSocket endpoint for real-time data ingestion updates.
    
    Clients connect to this endpoint to receive live updates when data is ingested.
    
    Message format:
    - {"type": "data_ingested", "data": {...}}
    """
    await manager.connect(websocket)
    
    try:
        while True:
            # Keep connection alive and listen for client messages
            data = await websocket.receive_text()
            
            # Handle ping/pong
            try:
                message = json.loads(data)
                if message.get("type") == "ping":
                    await websocket.send_json({"type": "pong"})
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON received: {data}")
                
    except WebSocketDisconnect:
        await manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        await manager.disconnect(websocket)