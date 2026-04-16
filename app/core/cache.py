import os
import time
import threading

REDIS_URL = os.getenv("REDIS_URL")
_redis = None

if REDIS_URL:
    try:
        import redis
        _redis = redis.from_url(REDIS_URL, decode_responses=False)
        _redis.ping()
    except Exception:
        _redis = None


class BoundedCache:
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


class HybridCache:
    """
    Uses Redis if REDIS_URL is set, falls back to in-memory.
    Survives server restarts when Redis is configured.
    """

    def __init__(self, max_size: int = 2000):
        self._local = BoundedCache(max_size)

    def get(self, key: str):
        if _redis:
            try:
                import pickle
                val = _redis.get(f"cp:{key}")
                return pickle.loads(val) if val else None
            except Exception:
                pass
        return self._local.get(key)

    def set(self, key: str, value, ttl: int = 120):
        if _redis:
            try:
                import pickle
                _redis.setex(f"cp:{key}", ttl, pickle.dumps(value))
                return
            except Exception:
                pass
        self._local.set(key, value, ttl)

    def delete(self, key: str):
        if _redis:
            try:
                _redis.delete(f"cp:{key}")
            except Exception:
                pass
        self._local.delete(key)


_cache = HybridCache(2000)
cache_get = _cache.get
cache_set = _cache.set
cache_delete = _cache.delete


def get_or_compute(cache_key: str, compute_fn, ttl: int = 120, *args, **kwargs):
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    result = compute_fn(*args, **kwargs)
    if result is not None:
        cache_set(cache_key, result, ttl=ttl)
    return result
