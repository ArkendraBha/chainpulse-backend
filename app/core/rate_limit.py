import os
import time
import threading
from fastapi import HTTPException, Request

# Try Redis first, fall back to in-memory
USE_REDIS = False
_redis_client = None

try:
    import redis

    REDIS_URL = os.getenv("REDIS_URL")
    if REDIS_URL:
        _redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        _redis_client.ping()
        USE_REDIS = True
except Exception:
    USE_REDIS = False


class RateLimiter:
    def __init__(self):
        self._buckets = {}
        self._lock = threading.Lock()

    def _get_identifier(self, request: Request) -> str:
        forwarded = request.headers.get("x-forwarded-for")
        return (
            forwarded.split(",")[0].strip()
            if forwarded
            else (request.client.host if request.client else "unknown")
        )

    def require(
        self,
        request: Request,
        max_requests: int = 60,
        window_seconds: int = 60,
    ):
        identifier = self._get_identifier(request)
        key = f"rl:{identifier}:{max_requests}:{window_seconds}"

        if USE_REDIS:
            self._redis_require(key, max_requests, window_seconds)
        else:
            self._memory_require(key, max_requests, window_seconds)

    def _redis_require(
        self,
        key: str,
        max_requests: int,
        window_seconds: int,
    ):
        try:
            pipe = _redis_client.pipeline()
            now = time.time()
            window_start = now - window_seconds

            pipe.zremrangebyscore(key, 0, window_start)
            pipe.zcard(key)
            pipe.zadd(key, {str(now): now})
            pipe.expire(key, window_seconds)
            results = pipe.execute()

            request_count = results[1]
            if request_count >= max_requests:
                raise HTTPException(
                    status_code=429,
                    detail="Rate limit exceeded.",
                    headers={"Retry-After": str(window_seconds)},
                )
        except HTTPException:
            raise
        except Exception:
            # Redis failure - fail open
            pass

    def _memory_require(
        self,
        key: str,
        max_requests: int,
        window_seconds: int,
    ):
        now = time.time()
        with self._lock:
            if key not in self._buckets:
                self._buckets[key] = {
                    "tokens": max_requests - 1,
                    "last_refill": now,
                }
                return
            bucket = self._buckets[key]
            elapsed = now - bucket["last_refill"]
            refill = elapsed * (max_requests / window_seconds)
            bucket["tokens"] = min(max_requests, bucket["tokens"] + refill)
            bucket["last_refill"] = now
            if bucket["tokens"] < 1:
                raise HTTPException(
                    status_code=429,
                    detail="Rate limit exceeded.",
                    headers={"Retry-After": str(window_seconds)},
                )
            bucket["tokens"] -= 1


rate_limiter = RateLimiter()
