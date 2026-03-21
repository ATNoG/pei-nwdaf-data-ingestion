import logging
import threading
import time
import uuid
from typing import Dict, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SubscriptionRegistry:
    def __init__(self, max_failures : int = 10):
        self.producers: Dict[str, str] = {}
        self.producers_failures : Dict[str, int] = {}
        self.producers_last_info : Dict[str, int] = {}
        self.producers_labels: Dict[str, str] = {}  # subscription_id -> label
        self.producers_active : Dict[str, bool] = {}
        self.producers_heartbeat_url : Dict[str, str] = {}       
        self.max_failures = max_failures
        self.lock = threading.Lock()

    def add(self, subscription_id, url: str, heartbeal_url : str):
        with self.lock:
            self.producers[subscription_id] = url
            self.producers_failures[subscription_id] = 0
            self.producers_last_info[subscription_id] = int(time.time())
            # Default label is subscription_id
            self.producers_labels[subscription_id] = subscription_id
            self.producers_active[subscription_id] = True
            self.producers_heartbeat_url[subscription_id] = heartbeal_url

    def remove(self, sub_id: str):
        with self.lock:
            self.producers.pop(sub_id, None)
            self.producers_failures.pop(sub_id, None)
            self.producers_last_info.pop(sub_id, None)
            self.producers_labels.pop(sub_id, None)
            self.producers_active.pop(sub_id, None)
            self.producers_heartbeat_url.pop(sub_id, None)

    def set_label(self, subscription_id: str, label: str) -> bool:
        """Update or set a producer's label. Returns True if producer exists."""
        with self.lock:
            if subscription_id not in self.producers:
                return False
            self.producers_labels[subscription_id] = label
            return True

    def get_label(self, subscription_id: str) -> str:
        """Get producer's label, fallback to subscription_id."""
        with self.lock:
            return self.producers_labels.get(subscription_id, subscription_id)

    def get_all_with_labels(self) -> Dict[str, dict]:
        """Return all producers with their labels: {sub_id: {url, label}}."""
        with self.lock:
            return {
                sub_id: {
                    "url": url,
                    "label": self.producers_labels.get(sub_id, sub_id),
                }
                for sub_id, url in self.producers.items() if self.producers_active[sub_id]
            }

    def get_all_active_producers(self) -> List[str]:
        """Return all producers as [id, id, id]."""
        active = []
        with self.lock:
            for prod in self.producers:
                if self.producers_active[prod]:
                    active.append(prod)

        return active

    def get_all_with_status(self) -> Dict[str, list]:  # no timeout param needed
        with self.lock:
            active, inactive = [], []
            for sub_id, url in self.producers.items():
                entry = {sub_id: {
                    "url": url,
                    "label": self.producers_labels.get(sub_id, sub_id),
                    "last_seen": self.producers_last_info.get(sub_id, 0)
                }}
                (active if self.producers_active.get(sub_id) else inactive).append(entry)
            return {"active": active, "inactive": inactive}
   
    def all_producers(self) -> Dict[str, str]:
        with self.lock:
            return self.producers.copy()

    def get_url(self, id : str) -> str:
        return self.producers[id]
    
    def get_heartbeat_url(self, id : str) -> str:
        return self.producers_heartbeat_url[id]

    def received_data(self, id : str):
        now = int(time.time())
        
        with self.lock:
            if id not in self.producers:
                return
            if self.producers_active[id] is False:
                self.producers_active[id] = True
            self.producers_last_info[id] = now
    
    def record_failure(self, id : str):
        with self.lock:
            self.producers_failures[id] += 1
            if self.producers_failures[id] > self.max_failures:
                self.producers_active[id] = False

    def record_success(self, id : str):
        with self.lock:
            self.producers_failures[id] = 0
            self.producers_active[id] = True

