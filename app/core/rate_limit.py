import time
import threading
from fastapi import HTTPException, Request


class InMemoryRateLimiter:
    """
    FIX 2: Token bucket rate limiter.
    For production at scale, replace with Redis-based limiter.
    """

    def __init__(self):
        self._buckets: dict = {}
        self._lock = threading.Lock()

    def _get_key(self, request: Request, key_type: str = "ip") -> str:
        if key_type == "ip":
            forwarded = request.headers.get("x-forwarded-for")
            ip = (
                forwarded.split(",")[0].strip()
                if forwarded
                else (request.client.host if request.client else "unknown")
            )
            return f"rate:{ip}"
        return f"rate:{key_type}"

    def require(
        self,
        request: Request,
        max_requests: int = 60,
        window_seconds: int = 60,
        key_type: str = "ip",
    ):
        key = self._get_key(request, key_type)
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
                    detail="Rate limit exceeded. Try again later.",
                    headers={"Retry-After": str(window_seconds)},
                )

            bucket["tokens"] -= 1


rate_limiter = InMemoryRateLimiter()


