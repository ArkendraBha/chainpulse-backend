from unittest.mock import MagicMock
from fastapi import HTTPException
from app.core.rate_limit import RateLimiter


def fake_request(ip="1.2.3.4"):
    req = MagicMock()
    req.headers.get.return_value = None
    req.client.host = ip
    return req


def test_rate_limit_allows_small_burst():
    limiter = RateLimiter()
    req = fake_request()
    for _ in range(5):
        limiter.require(req, max_requests=10, window_seconds=60)


def test_rate_limit_blocks():
    limiter = RateLimiter()
    req = fake_request()
    blocked = False
    try:
        for _ in range(20):
            limiter.require(req, max_requests=3, window_seconds=60)
    except HTTPException as e:
        blocked = e.status_code == 429
    assert blocked
