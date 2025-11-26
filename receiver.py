from fastapi import FastAPI
from pydantic import BaseModel
from typing import Any, Dict
import json
import threading
import os
from Comms.src.kmw import PyKafBridge


# Kafka setup
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
TOPIC = os.getenv("KAFKA_TOPIC", "raw-data")

if PyKafBridge is not None:
    try:
        kafka_bridge = PyKafBridge(KAFKA_HOST, KAFKA_PORT, TOPIC)
    except Exception:
        kafka_bridge = None
else:
    kafka_bridge = None


def start_kafka():
    if kafka_bridge is not None:
        try:
            kafka_bridge.start()
        except Exception:
            pass

# Initialize FastAPI app
app = FastAPI()

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
    "ul_bandwidth"
]

@app.post("/receive")
async def receive_data(packet: DataPacket):
    """Receive a data packet (dict) and return only the required fields.

    For any REQUIRED_FIELDS key missing from the incoming packet, the
    returned value will be None.
    """
    #print("Received:", packet.data)    

    # Keep only keys in REQUIRED_FIELDS, default to None
    filtered = {k: packet.data.get(k) for k in REQUIRED_FIELDS}

    # Convert to JSON string to send to Kafka
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

# Start Kafka bridge in a separate thread on startup
@app.on_event("startup")
def startup_event():
    threading.Thread(target=start_kafka, daemon=True).start()
    print("Kafka bridge started")

# Stop Kafka bridge on shutdown
@app.on_event("shutdown")
def shutdown_event():
    if kafka_bridge is not None:
        try:
            kafka_bridge.stop()
        except Exception:
            pass
        print("Kafka bridge stopped")