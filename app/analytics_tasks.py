"""
Connection analytics — Celery side (sync).

flush_analytics()   every ~60s:
    1. copies the Redis 5-minute counters (written by app/analytics.py on the
       request path) into server_traffic_5m,
    2. rebuilds the affected hour(s) of server_traffic_hourly from those rows,
    3. once per 5-minute bucket, snapshots each server's live sessions and its
       CURRENT capacity into server_usage_5m (so capacity history is never
       rewritten by later edits).
cleanup_analytics() hourly: retention (old rows are deleted in bounded slices).

Safety properties (this shares a single-process Celery worker with the VPN
health-check tasks, so it must stay small and must never raise):

  * IDEMPOTENT. Rows are written with "SET counts = <bucket total>" upserts, not
    "counts = counts + n". Re-flushing the same bucket (after a crash, a retry,
    or because the bucket was still open) can never double count.
  * A bucket that is still open (or in its short grace period) is re-flushed
    only when its Redis total changed; a closed, fully-flushed bucket is marked
    (an:fin:<epoch>) and never touched again.
  * Redis markers are set only AFTER the DB commit. If anything fails, the run
    rolls back and the same data is simply retried on the next tick.
  * A Redis lock stops two workers from flushing at once, and every failure is
    caught: the task can log an error but never propagates one.
  * ANALYTICS_ENABLED=false makes both tasks no-ops.
"""
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import redis as redis_lib
from sqlalchemy import DateTime, delete, func, literal, select, text

from app.analytics import (
    BUCKET_KEY_PREFIX,
    BUCKET_SECONDS,
    BUCKET_TTL_SECONDS,
    KNOWN_PROTOCOLS,
    METRIC_ASSIGNED,
    METRIC_FAILED,
    METRIC_SUCCESS,
    OTHER_PROTOCOL,
)
from app.config import settings
from app.database import SyncSessionLocal
from app.models import (
    ServerTraffic5m,
    ServerTrafficHourly,
    ServerUsage5m,
    VPNServer,
    VPNUserSession,
)

CLOSE_GRACE_SECONDS = 90        # a bucket accepts stragglers this long after it ends
LOCK_FLUSH = "an:lock:flush"
LOCK_CLEANUP = "an:lock:cleanup"
FLUSH_LOCK_TTL = 55
CLEANUP_LOCK_TTL = 600
USAGE_GATE_TTL = 900
INSERT_CHUNK = 500              # rows per INSERT statement
CLEANUP_MAX_SLICES = 60         # bounded work per cleanup run

_METRIC_TO_COLUMN = {
    METRIC_ASSIGNED: "assigned",
    METRIC_SUCCESS: "success",
    METRIC_FAILED: "failed",
}
_ALLOWED_PROTOCOLS = set(KNOWN_PROTOCOLS) | {OTHER_PROTOCOL}
_TRAFFIC_KEYS = ["bucket_start", "server_id", "country", "protocol"]
_TRAFFIC_COUNTERS = ["assigned", "success", "failed"]

_redis_client = None


# ─── plumbing (small, monkeypatchable seams for tests) ────────────────────────

def _redis():
    """Same Redis DB as the API's cache/counters (CACHE_REDIS_URL) — with timeouts,
    so a Redis problem can never hang the shared Celery worker."""
    global _redis_client
    if _redis_client is None:
        _redis_client = redis_lib.Redis.from_url(
            settings.CACHE_REDIS_URL,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=3,
        )
    return _redis_client


def _session():
    return SyncSessionLocal()


def _log(message: str) -> None:
    """ASCII-only print that cannot raise (these run inside except blocks of a shared worker)."""
    try:
        print(message.encode("ascii", "replace").decode("ascii"))
    except Exception:
        pass


def _dt(epoch: int) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def _dialect_insert(db):
    name = db.get_bind().dialect.name
    if name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert
    elif name == "sqlite":
        from sqlalchemy.dialects.sqlite import insert
    else:  # pragma: no cover
        raise RuntimeError(f"analytics: unsupported database dialect {name!r}")
    return insert


def _guard(db, seconds: int = 20) -> None:
    """Bound every statement in the current transaction (Postgres only). The Celery
    worker is single-process, so one runaway analytics query must not be able to
    stall the VPN health-check tasks queued behind it."""
    if db.get_bind().dialect.name == "postgresql":
        db.execute(text(f"SET LOCAL statement_timeout = '{int(seconds)}s'"))


def _sig_key(epoch: int) -> str:
    return f"an:sig:{epoch}"


def _fin_key(epoch: int) -> str:
    return f"an:fin:{epoch}"


def _usage_key(epoch: int) -> str:
    return f"an:usage:{epoch}"


def _epoch_from_key(key: str) -> Optional[int]:
    try:
        return int(str(key).rsplit(":", 1)[1])
    except (IndexError, ValueError):
        return None


# ─── parsing ──────────────────────────────────────────────────────────────────

def parse_bucket_hash(data: Dict[str, str]) -> Dict[Tuple[int, str, str], Dict[str, int]]:
    """
    Redis hash {"<server>|<CC>|<protocol>|<metric>": "<n>"} ->
    {(server_id, country, protocol): {"assigned": n, "success": n, "failed": n}}.
    Anything malformed is skipped, never raised — it must not be able to break a flush.
    """
    rows: Dict[Tuple[int, str, str], Dict[str, int]] = {}
    for field, value in data.items():
        try:
            server_s, country, protocol, metric = str(field).split("|")
            server_id = int(server_s)
            count = int(value)
        except (ValueError, TypeError):
            continue
        column = _METRIC_TO_COLUMN.get(metric)
        if (
            column is None
            or server_id <= 0
            or count < 0
            or protocol not in _ALLOWED_PROTOCOLS
            or len(country) != 2
            or not country.isascii()
            or not country.isalpha()
            or country != country.upper()
        ):
            continue
        row = rows.setdefault((server_id, country, protocol), {"assigned": 0, "success": 0, "failed": 0})
        row[column] += count
    return rows


# ─── DB writes ────────────────────────────────────────────────────────────────

def _merged(db, model, stmt, column: str, monotonic: bool):
    """Value written on conflict: the incoming one, or (monotonic) the larger of stored/incoming."""
    incoming = getattr(stmt.excluded, column)
    if not monotonic:
        return incoming
    greatest = func.greatest if db.get_bind().dialect.name == "postgresql" else func.max
    return greatest(getattr(model, column), incoming)


def _upsert(db, model, rows: List[dict], key_columns: List[str], update_columns: Iterable[str],
            monotonic: bool = False) -> None:
    """
    INSERT ... ON CONFLICT (key) DO UPDATE SET col = <incoming>   (overwrite, never "+ n").
    monotonic=True keeps the larger of the stored and incoming value: the counters of a bucket only
    ever grow, so this stays idempotent AND can never let a late, partial re-flush (e.g. after a
    clock-skewed straggler recreated an already-finalised bucket) shrink a good row.
    """
    if not rows:
        return
    insert = _dialect_insert(db)
    update_columns = list(update_columns)
    for i in range(0, len(rows), INSERT_CHUNK):
        stmt = insert(model).values(rows[i:i + INSERT_CHUNK])
        stmt = stmt.on_conflict_do_update(
            index_elements=key_columns,
            set_={c: _merged(db, model, stmt, c, monotonic) for c in update_columns},
        )
        db.execute(stmt)


def _upsert_traffic_rows(db, epoch: int, rows: Dict[Tuple[int, str, str], Dict[str, int]]) -> None:
    bucket = _dt(epoch)
    payload = [
        {
            "bucket_start": bucket,
            "server_id": server_id,
            "country": country,
            "protocol": protocol,
            **counters,
        }
        for (server_id, country, protocol), counters in rows.items()
    ]
    _upsert(db, ServerTraffic5m, payload, _TRAFFIC_KEYS, _TRAFFIC_COUNTERS, monotonic=True)


def _recompute_hour(db, hour_epoch: int) -> None:
    """Rebuild one hour of server_traffic_hourly from its server_traffic_5m rows (idempotent)."""
    h0, h1 = _dt(hour_epoch), _dt(hour_epoch + 3600)
    src = ServerTraffic5m
    select_stmt = (
        select(
            literal(h0, type_=DateTime(timezone=True)).label("bucket_start"),
            src.server_id,
            src.country,
            src.protocol,
            func.sum(src.assigned).label("assigned"),
            func.sum(src.success).label("success"),
            func.sum(src.failed).label("failed"),
        )
        .where(src.bucket_start >= h0, src.bucket_start < h1)
        .group_by(src.server_id, src.country, src.protocol)
    )
    insert = _dialect_insert(db)
    stmt = insert(ServerTrafficHourly).from_select(_TRAFFIC_KEYS + _TRAFFIC_COUNTERS, select_stmt)
    stmt = stmt.on_conflict_do_update(
        index_elements=_TRAFFIC_KEYS,
        set_={c: _merged(db, ServerTrafficHourly, stmt, c, True) for c in _TRAFFIC_COUNTERS},
    )
    db.execute(stmt)


def _flush_traffic(db, r, now: float, result: dict) -> None:
    _guard(db)
    pending: List[Tuple[int, str, int, bool, int]] = []    # (epoch, key, total, closed, row_count)
    hours = set()

    for key in sorted(r.scan_iter(match=BUCKET_KEY_PREFIX + "*", count=200)):
        epoch = _epoch_from_key(key)
        if epoch is None:
            continue
        closed = now >= epoch + BUCKET_SECONDS + CLOSE_GRACE_SECONDS
        if closed and r.exists(_fin_key(epoch)):
            continue                                        # already fully flushed

        rows = parse_bucket_hash(r.hgetall(key))
        total = sum(sum(c.values()) for c in rows.values())

        if r.get(_sig_key(epoch)) == str(total):
            if closed:                                      # nothing new; finalise
                r.set(_fin_key(epoch), "1", ex=BUCKET_TTL_SECONDS)
                r.delete(key)
            continue

        if rows:
            _upsert_traffic_rows(db, epoch, rows)
            hours.add(epoch - epoch % 3600)
        pending.append((epoch, key, total, closed, len(rows)))

    if not pending:
        return

    for hour_epoch in sorted(hours):
        _recompute_hour(db, hour_epoch)
    db.commit()

    # Only after a successful commit: remember what was written / finalise closed buckets.
    for epoch, key, total, closed, _ in pending:
        r.set(_sig_key(epoch), str(total), ex=BUCKET_TTL_SECONDS)
        if closed:
            r.set(_fin_key(epoch), "1", ex=BUCKET_TTL_SECONDS)
            r.delete(key)
    result["buckets"] = len(pending)
    result["rows"] = sum(p[4] for p in pending)


def _sample_usage(db, r, now: float, result: dict) -> None:
    """One snapshot per 5-minute bucket: sessions per server + the capacity in force now."""
    epoch = int(now // BUCKET_SECONDS) * BUCKET_SECONDS
    gate = _usage_key(epoch)
    if not r.set(gate, "1", nx=True, ex=USAGE_GATE_TTL):
        return                                              # this bucket is already sampled
    try:
        _guard(db)
        session_rows = db.execute(
            select(VPNUserSession.server_id, VPNUserSession.protocol, func.count())
            .group_by(VPNUserSession.server_id, VPNUserSession.protocol)
        ).all()
        by_server: Dict[int, Dict[str, int]] = {}
        for server_id, protocol, count in session_rows:
            slot = by_server.setdefault(server_id, {"total": 0, "openvpn": 0, "shadowsocks": 0})
            slot["total"] += count
            if protocol in ("openvpn", "shadowsocks"):
                slot[protocol] += count

        servers = db.execute(
            select(VPNServer.id, VPNServer.max_capacity, VPNServer.is_active)
        ).all()
        bucket = _dt(epoch)
        payload = []
        for server_id, max_capacity, is_active in servers:
            slot = by_server.get(server_id, {"total": 0, "openvpn": 0, "shadowsocks": 0})
            payload.append({
                "bucket_start": bucket,
                "server_id": server_id,
                "active_sessions": slot["total"],
                "openvpn_sessions": slot["openvpn"],
                "shadowsocks_sessions": slot["shadowsocks"],
                "max_capacity": int(max_capacity or 0),
                "is_active": bool(is_active),
            })
        _upsert(
            db, ServerUsage5m, payload, ["bucket_start", "server_id"],
            ["active_sessions", "openvpn_sessions", "shadowsocks_sessions", "max_capacity", "is_active"],
        )
        db.commit()
        result["usage_rows"] = len(payload)
    except Exception:
        db.rollback()
        try:
            r.delete(gate)                                  # let the next tick retry this bucket
        except Exception:
            pass
        raise


# ─── Celery entry points ──────────────────────────────────────────────────────

def flush_analytics(now: Optional[float] = None) -> Optional[dict]:
    """Never raises. Returns a small summary dict (or None when disabled / locked)."""
    if not settings.ANALYTICS_ENABLED:
        return None
    started = time.time()
    now = started if now is None else now
    result = {"buckets": 0, "rows": 0, "usage_rows": 0}

    try:
        r = _redis()
        if not r.set(LOCK_FLUSH, "1", nx=True, ex=FLUSH_LOCK_TTL):
            return None                                     # another flush is running
    except Exception as exc:
        _log(f"WARNING analytics flush: Redis unavailable, skipping this run: {exc!r}")
        return None

    db = None
    try:
        db = _session()
        for step, name in ((_flush_traffic, "traffic"), (_sample_usage, "usage")):
            try:
                step(db, r, now, result)
            except Exception as exc:
                db.rollback()
                _log(f"ERROR analytics flush ({name}) failed, will retry next run: {exc!r}")
        elapsed = time.time() - started
        if elapsed > 5:
            _log(f"WARNING analytics flush was slow: {elapsed:.1f}s {result}")
    except Exception as exc:
        _log(f"ERROR analytics flush failed: {exc!r}")
    finally:
        if db is not None:
            try:
                db.close()
            except Exception:
                pass
        try:
            r.delete(LOCK_FLUSH)
        except Exception:
            pass
    return result


def _delete_older_than(db, model, cutoff: datetime) -> int:
    """Delete rows older than cutoff in day-sized slices (bounded transaction size)."""
    removed = 0
    for _ in range(CLEANUP_MAX_SLICES):
        _guard(db, seconds=60)
        oldest = db.execute(select(func.min(model.bucket_start))).scalar()
        if oldest is None:
            break
        if oldest.tzinfo is None:                           # SQLite returns naive UTC
            oldest = oldest.replace(tzinfo=timezone.utc)
        if oldest >= cutoff:
            break
        slice_end = min(oldest + timedelta(days=1), cutoff)
        res = db.execute(delete(model).where(model.bucket_start < slice_end))
        db.commit()
        removed += res.rowcount or 0
    return removed


def cleanup_analytics(now: Optional[datetime] = None) -> Optional[dict]:
    """Retention. Never raises."""
    if not settings.ANALYTICS_ENABLED:
        return None
    now = now or datetime.now(timezone.utc)
    result = {}

    try:
        r = _redis()
        if not r.set(LOCK_CLEANUP, "1", nx=True, ex=CLEANUP_LOCK_TTL):
            return None
    except Exception as exc:
        _log(f"WARNING analytics cleanup: Redis unavailable, skipping this run: {exc!r}")
        return None

    db = None
    try:
        db = _session()
        plan = (
            (ServerTraffic5m, settings.ANALYTICS_5M_RETENTION_DAYS),
            (ServerTrafficHourly, settings.ANALYTICS_HOURLY_RETENTION_DAYS),
            (ServerUsage5m, settings.ANALYTICS_USAGE_RETENTION_DAYS),
        )
        for model, days in plan:
            try:
                cutoff = now - timedelta(days=max(1, int(days)))   # never below 1 day
                result[model.__tablename__] = _delete_older_than(db, model, cutoff)
            except Exception as exc:
                db.rollback()
                _log(f"ERROR analytics cleanup ({model.__tablename__}) failed: {exc!r}")
    except Exception as exc:
        _log(f"ERROR analytics cleanup failed: {exc!r}")
    finally:
        if db is not None:
            try:
                db.close()
            except Exception:
                pass
        try:
            r.delete(LOCK_CLEANUP)
        except Exception:
            pass
    return result
