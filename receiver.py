from fastapi import FastAPI, Request
from pydantic import BaseModel
from typing import Any, Dict
from contextlib import asynccontextmanager
import json
import os
from utils.kmw import PyKafBridge

# Kafka setup
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
TOPIC      = os.getenv("KAFKA_TOPIC", "raw-data")


kafka_bridge = None


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
        return {"status": "ok", "data": filtered}
    else:
        return {"status": "error", "message": "Failed to send to Kafka"}
