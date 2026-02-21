import threading
import time
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Any, Dict, Set
from contextlib import asynccontextmanager
import json
import os
import asyncio
import logging
from subscription_registry import SubscriptionRegistry
from utils.kmw import PyKafBridge
from collections import deque
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka setup
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
TOPIC      = os.getenv("KAFKA_TOPIC","network.data.ingested")

# Subscribe
HOST = os.getenv("HOST", "data-ingestion")
PORT = os.getenv("PORT", 7000)
PRODUCER_MAX_TIME_OUT = int(os.getenv("PRODUCER_MAX_TIMEOUT", 30))


subscription_registry = SubscriptionRegistry(max_failures=5)
#initialize thread to check producers
def check_producers_life(subscription_registry : SubscriptionRegistry):
    while True:
        subscription_registry.update_producers_life(PRODUCER_MAX_TIME_OUT)
        logger.info("Checking producers")
        time.sleep(5)

check_producers_thread = threading.Thread(target=check_producers_life, args=(subscription_registry,))
check_producers_thread.start()

raw_data_store = deque(maxlen=1000)  # Store last 1000 entries
kafka_bridge = None


# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        self._lock = asyncio.Lock()
    
    async def connect(self, websocket: WebSocket, subscription_id : str):
        await websocket.accept()
        async with self._lock:
            if subscription_id not in self.active_connections:
                self.active_connections[subscription_id] = set()
            self.active_connections[subscription_id].add(websocket)
        logger.info(f"WebSocket connected for producer {subscription_id}. Total connections: {len(self.active_connections)}")
    
    async def disconnect(self, websocket: WebSocket):
        async with self._lock:
            for producer_id, connections in self.active_connections.items():
                if websocket in connections:
                    connections.discard(websocket)
                    logger.info(f"WebSocket disconnected from producer {producer_id}. Remaining: {len(connections)}")
                    break

    async def broadcast(self, subscription_id : str, message: dict):
        """Broadcast message to all connected clients"""
        disconnected = set()
        
        async with self._lock:
            connections = self.active_connections.get(subscription_id, set()).copy()
        
        for connection in connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Error broadcasting to client: {e}")
                disconnected.add(connection)
        
        if disconnected:
            async with self._lock:
                self.active_connections[subscription_id] -= disconnected


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

# CORS middleware
origins = os.getenv("CORS_ORIGINS", "")
allowed_origins = [o.strip() for o in origins.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],  # Allows GET, POST, etc.
    allow_headers=["*"],  # Allows all headers
)

# Can be expanded later
class DataPacket(BaseModel):
    data: Dict[str, Any]

class SubscribeRequest(BaseModel):
    producer_url : str

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

@app.post("/subscriptions")
async def subscribe_to_producer(request : SubscribeRequest):
    try:
        response = requests.post(request.producer_url, json={"url" : f"http://{HOST}:{PORT}/receive"}, timeout=5)
        response_json =json.loads(response.text)
        id = response_json["subscription_id"]
        subscription_registry.add(id, request.producer_url)
        logger.info(f"Producer {request.producer_url} added with subscription id: {id}")
        return ({"id" : id}, 201)
    except requests.exceptions.Timeout:
        logger.warning(f"Producer {request.producer_url} didnt respond")
        return ("Producer didnt respond", 504)

    except requests.exceptions.ConnectionError:
        logger.warning(f"Cannot connect to producer {request.producer_url}")
        raise HTTPException(status_code=502, detail="Cannot connect to producer")
    except:
        logger.warning(f"Producer {request.producer_url} gave unexpected answer")
        return ({"message" : "Producer gave unexpected answer"}, 500)

@app.get("/subscriptions")
async def get_subscriptions():
    """
    Returns all producers in the system
    Returns in this format:
    {"producers" : [{id : url}, {id : url}]}
    """
    producers = subscription_registry.all_producers()
    producers_list = [ {p : producers[p] } for p in producers ]
    return {"producers" : producers_list}   

@app.delete("/subscriptions/{subscription_id}")
async def unsubscrive_to_producer(subscription_id : str):
    try:
        url = subscription_registry.get_url(subscription_id)
        if url[-1] == "/":
            url = url[:-1]
        response = requests.delete(f"{url}/{subscription_id}")
        if response.status_code == 200:
            subscription_registry.remove(subscription_id)
            return ({"subscription_id" : subscription_id}, 200)
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)     
    except KeyError:
        raise HTTPException(status_code=404, detail="Subscription not found")
    except :
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/receive")
async def receive_data(request: Request):
    """Receive a data packet (dict) and return only the required fields.

    For any REQUIRED_FIELDS key missing from the incoming packet, the
    returned value will be None.
    """
    payload = await request.json()
    data = payload or {}
    
    try:
        id = data["subscription_id"]
        subscription_registry.received_data(id)
        if id not in subscription_registry.all_producers():
            print(id, subscription_registry.all_producers())
            logger.warning("Received data from unsubscribed producer")
            return ("Not subscribed", 403)
    except:
        logger.warning("Received unexpected data from producer")

    print("Received:", data)
    results = []

    # Producer sends a batch under analyticsData where each element contains analyticsMetadata with the measurements
    if isinstance(data, dict) and "analyticsData" in data:
        analytics_list = data.get("analyticsData") or []
        for entry in analytics_list:
            meta = entry.get("analyticsMetadata", {}) if isinstance(entry, dict) else {}

            #ts = meta.get("timestamp") if meta.get("timestamp") is not None else entry.get("timestamp")

            raw = {}
            filtered = {}
            raw["timestamp"] = entry.get("timestamp")
            filtered["timestamp"] = entry.get("timestamp")
            for field in meta:
                    if field in REQUIRED_FIELDS:
                        filtered[field] = meta.get(field)
                    raw[field] = meta.get(field)

            raw_data_store.append(raw) # Raw data stored
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
                asyncio.create_task(manager.broadcast(id,{
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


app.websocket("/ws/ingestion/{subscription_id}")
async def websocket_ingestion(websocket: WebSocket, subscription_id : str):
    """
    WebSocket endpoint for real-time data ingestion updates.
    
    Clients connect to this endpoint to receive live updates when data is ingested.
    
    Message format:
    - {"type": "data_ingested", "data": {...}}
    """
    await manager.connect(websocket, subscription_id)
    
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



