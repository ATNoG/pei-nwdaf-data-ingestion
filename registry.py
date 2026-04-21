import json
import logging
import sqlite3
import threading
import time

logger = logging.getLogger(__name__)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS nef_subscriptions (
    notif_id   TEXT PRIMARY KEY,
    snssai     TEXT,
    dnn        TEXT,
    events     TEXT NOT NULL,
    nef_sub_id TEXT,
    nef_url    TEXT,
    created_at INTEGER NOT NULL
)
"""


class NfRegistry:
    """Persistent store for NEF subscription context (SQLite + in-memory cache)."""

    def __init__(self, db_path: str = "nef_registry.db"):
        self._lock = threading.Lock()
        self._cache: dict[str, dict] = {}
        self._db_path = db_path
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._connect() as conn:
            conn.execute(_CREATE_TABLE)
        with self._connect() as conn:
            for row in conn.execute("SELECT * FROM nef_subscriptions"):
                self._cache[row["notif_id"]] = self._row_to_dict(row)
        logger.info(f"NfRegistry: loaded {len(self._cache)} subscriptions from '{self._db_path}'")

    def _row_to_dict(self, row: sqlite3.Row) -> dict:
        return {
            "notif_id": row["notif_id"],
            "snssai": json.loads(row["snssai"]) if row["snssai"] else None,
            "dnn": row["dnn"],
            "events": json.loads(row["events"]),
            "nef_sub_id": row["nef_sub_id"],
            "nef_url": row["nef_url"],
            "created_at": row["created_at"],
        }

    def add(
        self,
        notif_id: str,
        snssai: dict | None,
        dnn: str | None,
        events: list[str],
        nef_sub_id: str | None = None,
        nef_url: str | None = None,
    ):
        record = {
            "notif_id": notif_id,
            "snssai": snssai,
            "dnn": dnn,
            "events": events,
            "nef_sub_id": nef_sub_id,
            "nef_url": nef_url,
            "created_at": int(time.time()),
        }
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO nef_subscriptions
                       (notif_id, snssai, dnn, events, nef_sub_id, nef_url, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        notif_id,
                        json.dumps(snssai) if snssai else None,
                        dnn,
                        json.dumps(events),
                        nef_sub_id,
                        nef_url,
                        record["created_at"],
                    ),
                )
            self._cache[notif_id] = record
        logger.info(f"NfRegistry: added subscription '{notif_id}' events={events}")

    def get(self, notif_id: str) -> dict | None:
        with self._lock:
            entry = self._cache.get(notif_id)
            return dict(entry) if entry is not None else None

    def remove(self, notif_id: str) -> bool:
        with self._lock:
            if notif_id not in self._cache:
                return False
            with self._connect() as conn:
                conn.execute("DELETE FROM nef_subscriptions WHERE notif_id = ?", (notif_id,))
            del self._cache[notif_id]
        logger.info(f"NfRegistry: removed subscription '{notif_id}'")
        return True

    def all(self) -> list[dict]:
        with self._lock:
            return list(self._cache.values())
