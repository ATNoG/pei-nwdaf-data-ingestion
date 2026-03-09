import logging
import threading
import time
import uuid
from typing import Dict, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SubscriptionRegistry:
    def __init__(self, max_failures: int = 5):
        self.producers: Dict[str, str] = {}
        self.producers_failures : Dict[str, int] = {}
        self.producers_last_info : Dict[str, int] = {}
        self.producers_labels: Dict[str, str] = {}  # subscription_id -> label

        self.max_failures = max_failures
        self.lock = threading.Lock()

    def add(self, subscription_id, url: str):
        with self.lock:
            self.producers[subscription_id] = url
            self.producers_failures[subscription_id] = 0
            self.producers_last_info[subscription_id] = int(time.time())
            # Default label is subscription_id
            self.producers_labels[subscription_id] = subscription_id

    def remove(self, sub_id: str):
        with self.lock:
            self.producers.pop(sub_id, None)
            self.producers_failures.pop(sub_id, None)
            self.producers_last_info.pop(sub_id, None)
            self.producers_labels.pop(sub_id, None)

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
                    "label": self.producers_labels.get(sub_id, sub_id)
                }
                for sub_id, url in self.producers.items()
            }

    def get_all_producers(self) -> Dict[str, str]:
        """Return all producers as {subscription_id: url}."""
        with self.lock:
            return self.producers.copy()

    def record_failure(self, id: str):
        with self.lock:
            if id in self.producers_failures:
                self.producers_failures[id] += 1
                logger.log(logging.INFO, f"Producer subscription with id:{id} took too long to respond")

                if self.producers_failures[id] >= self.max_failures:
                    self.remove(id)
                    logger.warning(f"{id} didnt respond {self.max_failures} times, assuming it is dead")

    def all_producers(self) -> Dict[str, str]:
        with self.lock:
            return self.producers.copy()

    def get_url(self, id : str) -> str:
        return self.producers[id]

    def received_data(self, id : str):
        now = int(time.time())
        with self.lock:
            self.producers_last_info[id] = now

    def update_producers_life(self, timeout : int):
        to_remove = []
        with self.lock:
            producers = self.producers
            now = int(time.time())
            for producer in producers:
                last_info = self.producers_last_info[producer]
                if now - last_info > timeout:
                    to_remove.append(producer)
                    logger.info(f"Producer {producer} removed for timeout")
        for prod_remove in to_remove:
            self.remove(prod_remove)
