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

        self.max_failures = max_failures
        self.lock = threading.Lock()

    def add(self, subscription_id, url: str):
        subscription_id = str(uuid.uuid4())
        with self.lock: 
            self.producers[subscription_id] = url
            self.producers_failures[subscription_id] = 0
            self.producers_last_info[subscription_id] = int(time.time())

    def remove(self, sub_id: str):
        with self.lock:
            self.producers.pop(sub_id, None)
            self.producers_failures.pop(sub_id, None)
            self.producers_last_info.pop(sub_id, None)

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
                return self.producers

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
