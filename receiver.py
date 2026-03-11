import asyncio
import json
import logging
import os
import time
import threading
import requests
from collections import deque
from contextlib import asynccontextmanager
from typing import Any, Dict, Set

from fastapi import FastAPI, Request, Response, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from utils.kmw import PyKafBridge

from policy_client import PolicyClient
from subscription_registry import SubscriptionRegistry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka setup
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
TOPIC = os.getenv("KAFKA_TOPIC", "network.data.ingested")

POLICY_SERVICE_URL = os.getenv("POLICY_SERVICE_URL", "http://policy-service:8788")
POLICY_COMPONENT_ID = os.getenv("POLICY_COMPONENT_ID", "ingestion-service")
POLICY_ENABLED = os.getenv("POLICY_ENABLED", "false").lower() == "true"

# Track all fields seen from incoming data
_discovered_fields: set[str] = set()


def get_discovered_fields() -> list[str]:
    """Get all field names discovered from incoming data."""
    return list(_discovered_fields)


policy_client = PolicyClient(
    service_url=POLICY_SERVICE_URL,
    component_id=POLICY_COMPONENT_ID,
    fields=get_discovered_fields,  # Function to get dynamic fields
    enable_policy=POLICY_ENABLED
)

REQUIRED_FIELDS = {"timestamp", "cell_index"}

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


check_producers_thread = threading.Thread(target=check_producers_life, args=(subscription_registry,), daemon=True)
check_producers_thread.start()

raw_data_store = deque(maxlen=1000)  # Store last 1000 entries
kafka_bridge = None


def build_allowed_fields() -> dict:
    """
    Build allowed_fields dict with producer categories for Policy registration.

    Returns:
        {
            "ingestion-service:{subscription_id}": [fields],
            "ingestion-service:{label}": [fields],
        }
    """
    producers = subscription_registry.get_all_with_labels()
    allowed_fields = {}

    # Use discovered fields for all producers (they share the same schema).
    # Fall back to REQUIRED_FIELDS so that allowed_fields entries are never
    # empty lists — an empty list causes the policy service's field discovery
    # to return zero fields (no fallback to data_columns when keys exist).
    fields_list = list(_discovered_fields) if _discovered_fields else list(REQUIRED_FIELDS)

    for sub_id, info in producers.items():

        # Add with subscription_id as key
        allowed_fields[f"ingestion-service:{sub_id}"] = fields_list

        # Also add with label as key (if different from sub_id)
        label = info["label"]
        if label != sub_id:
            allowed_fields[f"ingestion-service:{label}"] = fields_list

        # Also add with sanitized URL as key for readability
        url = info["url"]
        if url:
            safe_url = url.replace("://", "_").replace("/", "_").replace(":", "_")
            safe_url = safe_url.strip("_")[:30]  # Limit length
            if safe_url:
                allowed_fields[f"ingestion-service:{safe_url}"] = fields_list

    return allowed_fields


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
    global kafka_bridge, policy_client
    kafka_bridge = PyKafBridge(TOPIC, hostname=KAFKA_HOST, port=KAFKA_PORT)

    try:
        # Use discovered fields if available (e.g. from a previous run still in
        # memory), otherwise seed with REQUIRED_FIELDS so the policy service
        # always knows at least about the mandatory columns.
        startup_columns = list(_discovered_fields) if _discovered_fields else list(REQUIRED_FIELDS)
        await policy_client.register_component(
            component_type="ingestion",
            role=os.getenv("POLICY_ROLENAME", "Ingestion"),
            data_columns=startup_columns,
            allowed_fields=build_allowed_fields(),  # Producer categories
            auto_create_attributes=True
        )
        logger.info(f"Component registered with Policy Service ({len(startup_columns)} fields)")
    except Exception as e:
        logger.warning(f"Failed to register with Policy Service: {e}")

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


@app.post("/subscriptions", status_code=201)
async def subscribe_to_producer(request : SubscribeRequest):
    try:
        response = requests.post(request.producer_url, json={"url" : f"http://{HOST}:{PORT}/receive"}, timeout=5)
        response_json =json.loads(response.text)
        id = response_json["subscription_id"]
        subscription_registry.add(id, request.producer_url)
        logger.info(f"Producer {request.producer_url} added with subscription id: {id}")
        return {"id" : id}
    except requests.exceptions.Timeout:
        logger.warning(f"Producer {request.producer_url} didnt respond")
        raise HTTPException(status_code=504, detail="Producer didnt respond")

    except requests.exceptions.ConnectionError:
        logger.warning(f"Cannot connect to producer {request.producer_url}")
        raise HTTPException(status_code=502, detail="Cannot connect to producer")
    except:
        logger.warning(f"Producer {request.producer_url} gave unexpected answer")
        raise HTTPException(status_code=500, detail="Producer gave unexpected answer")

@app.get("/subscriptions")
async def get_subscriptions():
    """
    Returns all producers in the system with their labels.
    Returns in this format:
    {"producers" : [{sub_id : {url, label}}, {sub_id : {url, label}}]}
    """
    producers_with_labels = subscription_registry.get_all_with_labels()
    producers_list = [{sub_id: {"url": info["url"], "label": info["label"]}} for sub_id, info in producers_with_labels.items()]
    return {"producers": producers_list}   

@app.put("/subscriptions/{subscription_id}/label")
async def set_producer_label(subscription_id: str, label_data: dict):
    """
    Set a custom label for a producer.

    Args:
        subscription_id: The producer's subscription ID
        label_data: {"label": "custom_label"}

    Returns:
        {"subscription_id": str, "label": str}
    """
    label = label_data.get("label", "").strip()
    if not label:
        raise HTTPException(status_code=400, detail="Label cannot be empty")

    success = subscription_registry.set_label(subscription_id, label)
    if not success:
        raise HTTPException(status_code=404, detail="Producer not found")

    # Re-register with Policy to update allowed_fields with new labels
    try:
        await policy_client.register_component(
            component_type="ingestion",
            role=os.getenv("POLICY_ROLENAME", "Ingestion"),
            data_columns=list(_discovered_fields),
            allowed_fields=build_allowed_fields(),  # Update with new label
            auto_create_attributes=False  # Don't recreate attributes
        )
    except Exception as e:
        logger.warning(f"Failed to update Policy registration after label change: {e}")

    return {"subscription_id": subscription_id, "label": label}

@app.delete("/subscriptions/{subscription_id}")
async def unsubscrive_to_producer(subscription_id : str):
    try:
        url = subscription_registry.get_url(subscription_id)
        if url[-1] == "/":
            url = url[:-1]
        response = requests.delete(f"{url}/{subscription_id}")
        if response.status_code == 200:
            subscription_registry.remove(subscription_id)
            return {"subscription_id" : subscription_id}
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)     
    except KeyError:
        raise HTTPException(status_code=404, detail="Subscription not found")
    except HTTPException:
        raise
    except:
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/receive")
async def receive_data(request: Request):
    """Receive a data packet (dict) and return only the required fields.

    For any REQUIRED_FIELDS key missing from the incoming packet, the
    returned value will be None.
    """

    # Use the component_id as the source for policy checks when writing to Kafka
    # The external client sending data is irrelevant - we (ingestion-service) are the source
    source_id = POLICY_COMPONENT_ID

    payload = await request.json()
    data = payload or {}
    id = ""

    try:
        id = data["subscription_id"]
        subscription_registry.received_data(id)
        if id not in subscription_registry.all_producers():
            print(id, subscription_registry.all_producers())
            logger.warning("Received data from unsubscribed producer")
            raise HTTPException(status_code=403, detail="Not subscribed")
    except KeyError:
        logger.warning("Received unexpected data from producer")
        raise HTTPException(status_code=400, detail="Bad request")
    except HTTPException:
        raise

    logger.info(f"DEBUG: receive_data called, keys: {list(data.keys()) if isinstance(data, dict) else 'not dict'}")
    results = []

    # Producer sends a batch under analyticsData where each element contains analyticsMetadata with the measurements
    if isinstance(data, dict) and "analyticsData" in data:
        logger.info("DEBUG: Processing analyticsData batch")
        analytics_list = data.get("analyticsData") or []
        logger.info(f"DEBUG: analytics_list has {len(analytics_list)} entries")
        for i, entry in enumerate(analytics_list):
            meta = entry.get("analyticsMetadata", {}) if isinstance(entry, dict) else {}

            # ts = meta.get("timestamp") if meta.get("timestamp") is not None else entry.get("timestamp")
            record = {**meta, "timestamp": entry.get("timestamp")}

            # Track all fields for policy field discovery
            previous_field_count = len(_discovered_fields)
            _discovered_fields.update(record.keys())

            # Debug logging - log every time to see what's happening
            logger.info(f"Field discovery: previous={previous_field_count}, current={len(_discovered_fields)}, record_keys={len(record.keys())}")

            # Debug logging - use logger instead of print
            if previous_field_count == 0:
                logger.info(f"First data received! Discovered {len(_discovered_fields)} fields: {list(_discovered_fields)}")

            # If new fields were discovered, re-register with Policy Service
            if len(_discovered_fields) > previous_field_count:
                logger.info(f"New fields discovered! Total: {len(_discovered_fields)}. Re-registering...")
                try:
                    await policy_client.register_component(
                        component_type="ingestion",
                        role=os.getenv("POLICY_ROLENAME", "Ingestion"),
                        data_columns=list(_discovered_fields),
                        allowed_fields=build_allowed_fields(),  # Update producer categories
                        auto_create_attributes=True
                    )
                    logger.info(f"Updated component registration with {len(_discovered_fields)} fields")
                except Exception as e:
                    logger.warning(f"Failed to update component registration: {e}")

            missing = REQUIRED_FIELDS - record.keys()
            if missing or any(record.get(f) is None for f in REQUIRED_FIELDS):
                results.append(
                    {
                        "status": "error",
                        "message": f"Missing mandatory fields: {missing or REQUIRED_FIELDS}",
                    }
                )
                continue

            raw_data_store.append(record)  # Raw data stored

            if kafka_bridge is None:
                print("Kafka bridge not available - skipping produce (batch entry)")
                results.append({"status": "no-kafka", "data": record})
                continue

            result = await policy_client.process_data(
                source_id=source_id,
                sink_id="kafka",
                data=record,
                action="write"
            )

            if not result.allowed:
                logger.warning(f"Data filtered by policy: {result.reason}")
                continue

            message = json.dumps(result.data)

            try:
                ok = kafka_bridge.produce(TOPIC, message)
            except Exception:
                ok = False

            if ok:
                results.append({"status": "ok", "data": record})
                # Broadcast to WebSocket clients
                asyncio.create_task(
                    manager.broadcast(id, {"type": "data_ingested", "data": record})
                )
            else:
                results.append(
                    {
                        "status": "error",
                        "message": "Failed to send to Kafka",
                        "data": record,
                    }
                )

        print(results)

        return {"results": results}

    # Fallback
    missing = REQUIRED_FIELDS - data.keys()
    if missing or any(data.get(f) is None for f in REQUIRED_FIELDS):
        return {
            "status": "error",
            "message": f"Missing mandatory fields: {missing or REQUIRED_FIELDS}",
        }
    message = json.dumps(data)  # Send to Kafka if available
    if kafka_bridge is None:
        print("Kafka bridge not available - skipping produce")
        return {"status": "no-kafka", "data": data}

    try:
        success = kafka_bridge.produce(TOPIC, message)
    except Exception:
        success = False

    if success:
        # Broadcast to WebSocket clients
        asyncio.create_task(manager.broadcast(id,{"type": "data_ingested", "data": data}))
        return {"status": "ok", "data": data}
    else:
        return {"status": "error", "message": "Failed to send to Kafka"}


@app.websocket("/ws/ingestion/{subscription_id}")
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



