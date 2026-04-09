import time
import threading


class BoundedCache:
    """
    FIX 8: Bounded in-memory cache with TTL and max size eviction.
    For production at scale, replace with Redis.
    """

    def __init__(self, max_size: int = 2000):
        self._data = {}
        self._lock = threading.Lock()
        self._max_size = max_size

    def get(self, key: str):
        with self._lock:
            entry = self._data.get(key)
            if not entry:
                return None
            value, expires = entry
            if time.time() > expires:
                del self._data[key]
                return None
            return value

    def set(self, key: str, value, ttl: int = 120):
        with self._lock:
            if len(self._data) >= self._max_size:
                oldest = min(self._data, key=lambda k: self._data[k][1])
                del self._data[oldest]
            self._data[key] = (value, time.time() + ttl)

    def delete(self, key: str):
        with self._lock:
            self._data.pop(key, None)


_cache = BoundedCache(2000)
cache_get = _cache.get
cache_set = _cache.set
cache_delete = _cache.delete


def get_or_compute(cache_key: str, compute_fn, ttl: int = 120, *args, **kwargs):
    """Always check cache first, then compute and cache."""
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    result = compute_fn(*args, **kwargs)
    if result is not None:
        cache_set(cache_key, result, ttl=ttl)
    return result


