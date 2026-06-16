import threading
import logging

logger = logging.getLogger(__name__)

class LocalCache:
    def __init__(self):
        self._storage = {}
        self._lock = threading.Lock()

    def get(self, key: str):
        with self._lock:
            return self._storage.get(key)

    def put(self, key: str, value: str):
        with self._lock:
            self._storage[key] = value
            logger.info(f"Lokalny Cache (Python): Zaktualizowano '{key}' = '{value}'")

    def get_all(self):
        with self._lock:
            return dict(self._storage)  # Zwracamy kopię słownika

local_cache = LocalCache()