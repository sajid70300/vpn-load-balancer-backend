import json
from datetime import datetime
from decimal import Decimal
import redis.asyncio as aioredis
from typing import Optional, Any
from app.config import settings

# Redis client
redis_client = None


async def get_redis():
    global redis_client
    if redis_client is None:
        redis_client = await aioredis.from_url(settings.CACHE_REDIS_URL, decode_responses=True)
    return redis_client


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime and Decimal objects"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


async def get_cache(key: str) -> Optional[Any]:
    """Get value from cache"""
    redis = await get_redis()
    value = await redis.get(key)
    return json.loads(value) if value else None


async def set_cache(key: str, value: Any, ttl: int = 3):
    """Set value in cache with custom JSON encoder"""
    redis = await get_redis()
    # Use custom encoder to handle datetime objects
    json_str = json.dumps(value, cls=DateTimeEncoder)
    await redis.setex(key, ttl, json_str)


async def delete_cache(pattern: str):
    """Delete cache key(s). Supports wildcards with *"""
    redis = await get_redis()
    
    if '*' in pattern:
        # Pattern matching - delete multiple keys
        keys = await redis.keys(pattern)
        if keys:
            await redis.delete(*keys)
    else:
        # Single key
        await redis.delete(pattern)