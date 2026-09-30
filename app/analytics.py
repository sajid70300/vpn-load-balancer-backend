"""
Connection analytics — request-path recording.

Design goals (this runs on the hottest endpoints of the system):

  * ZERO database access. Each event is one pipelined Redis HINCRBY (+ EXPIRE)
    into a hash for the current 5-minute bucket — sub-millisecond.
  * It can NEVER fail or slow a request. Every public function swallows all
    errors, is bounded by a short timeout, and does nothing when
    ANALYTICS_ENABLED is false (the kill switch).
  * Bounded cardinality: country is validated to a 2-letter code (else 'XX')
    and protocol to openvpn / shadowsocks (else 'other'), so garbage from a
    client can never create an unbounded number of counter fields.

A Celery task (app/analytics_tasks.py) copies these counters into the
server_traffic_5m / server_traffic_hourly tables once a minute.

What is counted (per server row, client country, protocol):
    asg  = "assigned": the backend handed this server out (/v2/best_server/)
    ok   = connection attempts the CLIENT APP reported as successful
    fail = connection attempts the CLIENT APP reported as failed
Success/failure are client-reported (via /v2/connection_feedback/); the
backend has no independent way to confirm a Shadowsocks connection.
"""
import asyncio
import time
from typing import Iterable, Optional, Tuple

from app.cache import get_redis
from app.config import settings

BUCKET_SECONDS = 300                 # 5-minute counters
BUCKET_KEY_PREFIX = "an:b:"          # an:b:<bucket start epoch seconds>
BUCKET_TTL_SECONDS = 3 * 3600        # safety net: a bucket that is never flushed expires
REDIS_TIMEOUT_SECONDS = 0.5          # hard cap on the extra time analytics may ever add

METRIC_ASSIGNED = "asg"
METRIC_SUCCESS = "ok"
METRIC_FAILED = "fail"

KNOWN_PROTOCOLS = ("openvpn", "shadowsocks")
OTHER_PROTOCOL = "other"
UNKNOWN_COUNTRY = "XX"

_last_error_log = 0.0


def normalize_country(value: Optional[str]) -> str:
    """'pk' / ' PK ' -> 'PK'; anything that is not exactly two ASCII letters -> 'XX'."""
    if isinstance(value, str):
        v = value.strip().upper()
        if len(v) == 2 and v.isascii() and v.isalpha():
            return v
    return UNKNOWN_COUNTRY


def normalize_protocol(value: Optional[str]) -> str:
    if isinstance(value, str):
        v = value.strip().lower()
        if v in KNOWN_PROTOCOLS:
            return v
    return OTHER_PROTOCOL


def bucket_epoch(ts: float) -> int:
    return int(ts // BUCKET_SECONDS) * BUCKET_SECONDS


def bucket_key(epoch: int) -> str:
    return f"{BUCKET_KEY_PREFIX}{epoch}"


def field_name(server_id: int, country: str, protocol: str, metric: str) -> str:
    return f"{server_id}|{country}|{protocol}|{metric}"


def _log_failure(what: str, exc: BaseException) -> None:
    """
    At most one log line per minute, so a Redis outage can't flood the logs.
    ASCII-only and itself wrapped: logging must be incapable of raising (a console
    that cannot encode a character would otherwise turn a harmless analytics
    problem into a failed request).
    """
    global _last_error_log
    try:
        now = time.monotonic()
        if now - _last_error_log >= 60:
            _last_error_log = now
            print(f"WARNING analytics: {what} failed (ignored, the request is unaffected): {exc!r}".encode("ascii", "replace").decode("ascii"))
    except Exception:
        pass


async def _write(fields: list) -> None:
    redis = await get_redis()
    key = bucket_key(bucket_epoch(time.time()))
    pipe = redis.pipeline(transaction=False)
    for f in fields:
        pipe.hincrby(key, f, 1)
    pipe.expire(key, BUCKET_TTL_SECONDS)
    await pipe.execute()


async def _safe_write(fields: list, what: str) -> None:
    if not fields or not settings.ANALYTICS_ENABLED:
        return
    try:
        await asyncio.wait_for(_write(fields), timeout=REDIS_TIMEOUT_SECONDS)
    except Exception as exc:  # includes timeouts; CancelledError is intentionally NOT swallowed
        _log_failure(what, exc)


def _extract_assignment(decision) -> Optional[Tuple[int, str]]:
    """(server_id, primary protocol) from a BestServerDecision OR its cached dict form."""
    try:
        if isinstance(decision, dict):
            cfg = decision.get("primary_config") or {}
            server_id = cfg.get("server_id")
            protocol = decision.get("primary_protocol")
        else:
            cfg = getattr(decision, "primary_config", None)
            server_id = getattr(cfg, "server_id", None)
            protocol = getattr(decision, "primary_protocol", None)
        if isinstance(server_id, bool) or not isinstance(server_id, int) or server_id <= 0:
            return None
        return server_id, normalize_protocol(protocol)
    except Exception:
        return None


async def record_assignment(decision, country: Optional[str]) -> None:
    """A /v2/best_server/ response handed a server out (fresh OR served from cache)."""
    try:
        extracted = _extract_assignment(decision)
        if extracted is None:
            return
        server_id, protocol = extracted
        fields = [field_name(server_id, normalize_country(country), protocol, METRIC_ASSIGNED)]
    except Exception as exc:
        _log_failure("assignment", exc)
        return
    await _safe_write(fields, "assignment")


async def record_feedback(
    server_id: int,
    country: Optional[str],
    attempts: Iterable[Tuple[Optional[str], Optional[bool]]],
) -> None:
    """
    Client-reported outcome(s) from /v2/connection_feedback/.
    attempts = [(protocol, success), ...] — one entry per protocol actually tried;
    an entry whose success is None (not attempted) is ignored.
    """
    try:
        if isinstance(server_id, bool) or not isinstance(server_id, int) or server_id <= 0:
            return
        cc = normalize_country(country)
        fields = []
        for protocol, success in attempts:
            if success is None:
                continue
            fields.append(field_name(
                server_id, cc, normalize_protocol(protocol),
                METRIC_SUCCESS if success else METRIC_FAILED,
            ))
    except Exception as exc:
        _log_failure("feedback", exc)
        return
    await _safe_write(fields, "feedback")
