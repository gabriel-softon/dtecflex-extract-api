import json, time
import redis
import redis.asyncio as aioredis
from src.dtecflex_extract_api.config.celery import settings

REDIS_URL = getattr(settings, "REDIS_URL", "redis://localhost:6379/0")
r_sync = redis.from_url(REDIS_URL, decode_responses=True)
r_async = aioredis.from_url(REDIS_URL, decode_responses=True)

CHANNEL_PREFIX = "publish:ch:" 
LOCK_PREFIX    = "publish:lock:"
META_PREFIX    = "publish:meta:"

def job_key(date_dir: str, category: str | None, cat_prefix: str | None) -> str:
    base = f"{cat_prefix}{date_dir}" if cat_prefix else f"ALL:{date_dir}"
    return base

def channel_name(key: str) -> str: return f"{CHANNEL_PREFIX}{key}"
def lock_key(key: str) -> str:    return f"{LOCK_PREFIX}{key}"
def meta_key(job_key: str) -> str:
    return f"{META_PREFIX}{job_key}"


def acquire_lock(key: str, ttl_sec: int = 60*45) -> bool:
    return bool(r_sync.set(lock_key(key), str(int(time.time())), ex=ttl_sec, nx=True))

def release_lock(key: str):
    r_sync.delete(lock_key(key))

def publish(key: str, payload: dict):
    r_sync.publish(channel_name(key), json.dumps(payload))

def save_meta(job_key: str, **fields):
    if not fields:
        return
    clean = {str(k): _coerce_redis_value(v) for k, v in fields.items()}
    r_sync.hset(meta_key(job_key), mapping=clean)
    r_sync.expire(meta_key(job_key), 60*90)

def get_meta(key: str) -> dict:
    return r_sync.hgetall(meta_key(key)) or {}

def _coerce_redis_value(v):
    if v is None:
        return ""  # ou "null", se preferir
    if isinstance(v, (str, int, float, bool)):
        return v
    # dict/list/tuple/set/objetos → JSON
    try:
        return json.dumps(v, ensure_ascii=False, default=str)
    except Exception:
        return str(v)