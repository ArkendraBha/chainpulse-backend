import redis
import json
import os

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

DEFAULT_TTL = 60  # seconds

def cache_get(key):
    value = r.get(key)
    return json.loads(value) if value else None

def cache_set(key, value, ttl=DEFAULT_TTL):
    r.setex(key, ttl, json.dumps(value))