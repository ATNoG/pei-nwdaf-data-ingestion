import asyncio
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, Set

import requests
from fastapi import FastAPI, HTTPException, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from utils.kmw import PyKafBridge

from policy_client import PolicyClient
from registry import NfRegistry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
TOPIC = os.getenv("KAFKA_TOPIC", "network.data.ingested")

POLICY_SERVICE_URL = os.getenv("POLICY_SERVICE_URL", "http://policy-service:8788")
POLICY_COMPONENT_ID = os.getenv("POLICY_COMPONENT_ID", "ingestion-service")
POLICY_ENABLED = os.getenv("POLICY_ENABLED", "false").lower() == "true"

HOST = os.getenv("HOST", "data-ingestion")
PORT = int(os.getenv("PORT", "7000"))

REGISTRY_DB = os.getenv("REGISTRY_DB", "registry.db")

# Tracks metric field names discovered from incoming NEF data (for Policy registration)
_discovered_fields: set[str] = set()


def get_discovered_fields() -> list[str]:
    return list(_discovered_fields)


policy_client = PolicyClient(
    service_url=POLICY_SERVICE_URL,
    component_id=POLICY_COMPONENT_ID,
    fields=get_discovered_fields,
    enable_policy=True,
    heartbeat_interval=30,
) if POLICY_ENABLED else None

nf_registry = NfRegistry(db_path=REGISTRY_DB)
kafka_bridge = None

# ── Parsers ───────────────────────────────────────────────────────────────────

_BITRATE_RE = re.compile(r"^(\d+\.?\d*)\s*(bps|Kbps|Mbps|Gbps|Tbps)$")
_BITRATE_MUL = {"bps": 1e-6, "Kbps": 1e-3, "Mbps": 1.0, "Gbps": 1e3, "Tbps": 1e6}


def parse_bitrate_mbps(value: str) -> float | None:
    """Parse a 3GPP BitRate string to Mbps. Returns None if unparseable."""
    m = _BITRATE_RE.match(value.strip())
    if not m:
        return None
    return round(float(m.group(1)) * _BITRATE_MUL[m.group(2)], 6)


def parse_datetime_to_unix(value: str) -> int | None:
    """Parse an ISO 8601 datetime string to a Unix timestamp. Returns None on failure."""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception:
        return None


# ── NEF event normalizers ─────────────────────────────────────────────────────

def _normalize_perf_data(info: dict, context_tags: dict) -> dict | None:
    tags = dict(context_tags)

    ue_ip = info.get("ueIpAddr") or {}
    if ue_ip.get("ipv4Addr"):
        tags["ueIpv4Addr"] = ue_ip["ipv4Addr"]
    elif ue_ip.get("ipv6Addr"):
        tags["ueIpv6Addr"] = ue_ip["ipv6Addr"]
    if info.get("appId"):
        tags["appId"] = info["appId"]

    if not tags:
        return None

    perf = info.get("perfData") or {}
    metrics: dict[str, Any] = {}

    for src, dst in [
        ("thrputUl", "thrputUl_mbps"), ("thrputDl", "thrputDl_mbps"),
        ("maxThrputUl", "maxThrputUl_mbps"), ("minThrputUl", "minThrputUl_mbps"),
        ("maxThrputDl", "maxThrputDl_mbps"), ("minThrputDl", "minThrputDl_mbps"),
    ]:
        val = perf.get(src)
        if val is not None:
            parsed = parse_bitrate_mbps(str(val))
            if parsed is not None:
                metrics[dst] = parsed

    for src, dst in [
        ("pdb", "pdb_ms"), ("pdbDl", "pdbDl_ms"),
        ("maxPdbUl", "maxPdbUl_ms"), ("maxPdbDl", "maxPdbDl_ms"),
        ("plr", "plr_per_thousand"), ("plrDl", "plrDl_per_thousand"),
        ("maxPlrUl", "maxPlrUl_per_thousand"), ("maxPlrDl", "maxPlrDl_per_thousand"),
    ]:
        val = perf.get(src)
        if val is not None:
            metrics[dst] = int(val)

    ts_str = info.get("timeStamp")
    timestamp = parse_datetime_to_unix(ts_str) if ts_str else int(time.time())

    return {
        "timestamp": timestamp or int(time.time()),
        "tags": tags,
        "event": "PERF_DATA",
        "metrics": metrics,
    }


def _normalize_ue_mobility(info: dict, context_tags: dict) -> dict | None:
    tags = dict(context_tags)
    if info.get("supi"):
        tags["supi"] = info["supi"]
    if info.get("gpsi"):
        tags["gpsi"] = info["gpsi"]

    if not tags:
        return None

    trajectory = []
    for traj in info.get("ueTrajs") or []:
        ts = parse_datetime_to_unix(traj["ts"]) if traj.get("ts") else None
        nr = (traj.get("location") or {}).get("nrLocation") or {}
        trajectory.append({
            "ts": ts,
            "tac": (nr.get("tai") or {}).get("tac"),
            "nrCellId": (nr.get("ncgi") or {}).get("nrCellId"),
        })

    timestamp = trajectory[0]["ts"] if trajectory and trajectory[0].get("ts") else int(time.time())

    return {
        "timestamp": timestamp or int(time.time()),
        "tags": tags,
        "event": "UE_MOBILITY",
        "metrics": {"trajectory": trajectory},
    }


def _normalize_ue_comm(info: dict, context_tags: dict) -> dict | None:
    tags = dict(context_tags)
    if info.get("supi"):
        tags["supi"] = info["supi"]
    if info.get("interGroupId"):
        tags["interGroupId"] = info["interGroupId"]
    if info.get("gpsi"):
        tags["gpsi"] = info["gpsi"]

    if not tags:
        return None

    comms = []
    for comm in info.get("comms") or []:
        comms.append({
            "startTime": parse_datetime_to_unix(comm["startTime"]) if comm.get("startTime") else None,
            "endTime": parse_datetime_to_unix(comm["endTime"]) if comm.get("endTime") else None,
            "ulVol": comm.get("ulVol"),
            "dlVol": comm.get("dlVol"),
        })

    timestamp = comms[0]["endTime"] if comms and comms[0].get("endTime") else int(time.time())

    return {
        "timestamp": timestamp,
        "tags": tags,
        "event": "UE_COMM",
        "metrics": {"comms": comms},
    }


_EVENT_NORMALIZERS: dict[str, tuple[str, Any]] = {
    "PERF_DATA":    ("perfDataInfos",    _normalize_perf_data),
    "UE_MOBILITY":  ("ueMobilityInfos",  _normalize_ue_mobility),
    "UE_COMM":      ("ueCommInfos",      _normalize_ue_comm),
}

# ── WebSocket manager ─────────────────────────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, notif_id: str):
        await websocket.accept()
        async with self._lock:
            if notif_id not in self.active_connections:
                self.active_connections[notif_id] = set()
            self.active_connections[notif_id].add(websocket)
        logger.info(f"WebSocket connected for notifId={notif_id}")

    async def disconnect(self, websocket: WebSocket):
        async with self._lock:
            for nid, connections in self.active_connections.items():
                if websocket in connections:
                    connections.discard(websocket)
                    break

    async def broadcast(self, notif_id: str, message: dict):
        disconnected = set()
        async with self._lock:
            connections = self.active_connections.get(notif_id, set()).copy()
        for conn in connections:
            try:
                await conn.send_json(message)
            except Exception as e:
                logger.error(f"WebSocket broadcast error: {e}")
                disconnected.add(conn)
        if disconnected:
            async with self._lock:
                self.active_connections[notif_id] -= disconnected


manager = ConnectionManager()

# ── App lifecycle ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global kafka_bridge
    kafka_bridge = PyKafBridge(TOPIC, hostname=KAFKA_HOST, port=KAFKA_PORT)
    if policy_client is not None:
        try:
            await policy_client.register_component(
                component_type="ingestion",
                role=os.getenv("POLICY_ROLENAME", "Ingestion"),
                data_columns=list(_discovered_fields) or ["timestamp", "tags", "event", "metrics"],
                allowed_fields={},
                auto_create_attributes=True,
            )
            await policy_client.start_heartbeat()
            logger.info("Registered with Policy Service")
        except Exception as e:
            logger.warning(f"Failed to register with Policy Service: {e}")
    yield
    if policy_client is not None:
        await policy_client.stop_heartbeat()
    try:
        await kafka_bridge.close()
    except Exception as e:
        logger.warning(f"Error closing Kafka bridge: {e}")


app = FastAPI(lifespan=lifespan)

origins = os.getenv("CORS_ORIGINS", "")
allowed_origins = [o.strip() for o in origins.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Pydantic models ───────────────────────────────────────────────────────────

class NefSubscriptionRequest(BaseModel):
    notifId: str
    nefUrl: str
    events: list[str]
    snssai: dict | None = None
    dnn: str | None = None

# ── NEF endpoints ─────────────────────────────────────────────────────────────

@app.post("/nef/subscriptions", status_code=201)
async def create_nef_subscription(body: NefSubscriptionRequest):
    """Subscribe to a NEF and store the subscription context."""
    nef_payload = {
        "notifId": body.notifId,
        "notifUri": f"http://{HOST}:{PORT}/nef/notify",
        "eventsSubs": [{"event": e} for e in body.events],
    }
    loop = asyncio.get_running_loop()
    try:
        r = await loop.run_in_executor(
            None, lambda: requests.post(body.nefUrl, json=nef_payload, timeout=5)
        )
        r.raise_for_status()
        nef_sub_id = r.json().get("subscriptionId")
    except requests.exceptions.Timeout:
        raise HTTPException(status_code=504, detail="NEF did not respond")
    except requests.exceptions.ConnectionError:
        raise HTTPException(status_code=502, detail="Cannot connect to NEF")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"NEF subscription failed: {e}")

    nf_registry.add(
        notif_id=body.notifId,
        snssai=body.snssai,
        dnn=body.dnn,
        events=body.events,
        nef_sub_id=nef_sub_id,
        nef_url=body.nefUrl,
    )
    logger.info(f"NEF subscription created: notifId={body.notifId} nefSubId={nef_sub_id}")
    return {"notifId": body.notifId, "nefSubscriptionId": nef_sub_id}


@app.get("/nef/subscriptions")
async def list_nef_subscriptions():
    return {"subscriptions": nf_registry.all()}


@app.delete("/nef/subscriptions/{notif_id}", status_code=204)
async def delete_nef_subscription(notif_id: str):
    sub = nf_registry.get(notif_id)
    if not sub:
        raise HTTPException(status_code=404, detail="Subscription not found")

    if sub.get("nef_sub_id") and sub.get("nef_url"):
        try:
            nef_base = sub["nef_url"].rstrip("/")
            url = f"{nef_base}/{sub['nef_sub_id']}"
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: requests.delete(url, timeout=5))
        except Exception as e:
            logger.warning(f"Failed to unsubscribe from NEF: {e}")

    nf_registry.remove(notif_id)
    return Response(status_code=204)


@app.post("/nef/notify", status_code=204)
async def nef_notify(request: Request):
    """Receive a NefEventExposureNotif callback from the NEF (TS 29.591)."""
    payload = await request.json()
    notif_id = payload.get("notifId")

    if not notif_id:
        raise HTTPException(status_code=400, detail="Missing notifId")

    context = nf_registry.get(notif_id)
    if not context:
        raise HTTPException(status_code=403, detail="Unknown notifId")

    context_tags: dict[str, Any] = {}
    if snssai := context.get("snssai"):
        if (sst := snssai.get("sst")) is not None:
            context_tags["snssai_sst"] = sst
        if sd := snssai.get("sd"):
            context_tags["snssai_sd"] = sd
    if context.get("dnn"):
        context_tags["dnn"] = context["dnn"]

    records = []
    for event_notif in payload.get("eventNotifs") or []:
        event = event_notif.get("event")
        normalizer_info = _EVENT_NORMALIZERS.get(event)
        if not normalizer_info:
            logger.warning(f"Unsupported event type: {event}")
            continue
        field_name, normalizer = normalizer_info
        for info in event_notif.get(field_name) or []:
            rec = normalizer(info, context_tags)
            if rec:
                records.append(rec)
            else:
                logger.warning(f"Dropped {event} record: no UE identifier")

    if not records:
        return Response(status_code=204)

    # Update discovered metric fields for Policy registration
    for rec in records:
        _discovered_fields.update(rec.get("metrics", {}).keys())

    if policy_client is not None:
        results = await asyncio.gather(*(
            policy_client.process_data(source_id=POLICY_COMPONENT_ID, sink_id="kafka", data=rec, action="write")
            for rec in records
        ))
        allowed_records = []
        for result in results:
            if result.allowed:
                allowed_records.append(result.data)
            else:
                logger.warning(f"Record filtered by policy: {result.reason}")
    else:
        allowed_records = records

    if not allowed_records:
        return Response(status_code=204)

    #logger.info(f"[NEF RECORDS]\n{json.dumps(allowed_records, indent=2)}")

    if kafka_bridge is not None:
        message = json.dumps(allowed_records)
        try:
            ok = kafka_bridge.produce(TOPIC, message)
        except Exception:
            ok = False

        if ok:
            logger.info(f"NEF: produced {len(allowed_records)} records (notifId={notif_id})")
            for rec in allowed_records:
                asyncio.create_task(manager.broadcast(notif_id, {"type": "data_ingested", "data": rec}))
        else:
            logger.error(f"NEF: failed to produce {len(allowed_records)} records")
    else:
        logger.warning("NEF: Kafka bridge not available, skipping produce")

    return Response(status_code=204)


@app.websocket("/ws/ingestion/{notif_id}")
async def websocket_ingestion(websocket: WebSocket, notif_id: str):
    """Real-time stream of ingested NEF events for a given notifId."""
    await manager.connect(websocket, notif_id)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                if msg.get("type") == "ping":
                    await websocket.send_json({"type": "pong"})
            except json.JSONDecodeError:
                pass
    except WebSocketDisconnect:
        await manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        await manager.disconnect(websocket)
