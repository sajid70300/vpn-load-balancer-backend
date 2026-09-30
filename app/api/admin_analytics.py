"""
Admin API — connection analytics.

  GET /admin/analytics/servers/{server_id}   per-server report (requests, successes,
                                             failures, by country, by protocol)
  GET /admin/analytics/overview              everything the Home Overview page shows

Read-only, and NEVER reads the hot routing path: everything here comes from the
summary tables filled by the analytics Celery task (server_traffic_5m /
server_traffic_hourly / server_usage_5m), plus a few cheap queries on existing
tables. Each response is cached in Redis for a short time, so any number of
admins refreshing the dashboard costs the database roughly one query set per
cache interval. A Redis problem only disables the cache — it never fails a request.

Metric definitions (also returned to the UI in "definitions"):
  assigned = times the backend handed this server out to an app (/v2/best_server/).
             This is a request *sent to* the server, NOT a confirmed connection.
  success / failed = connection attempts the CLIENT APP reported back
             (/v2/connection_feedback/). The backend cannot independently confirm a
             Shadowsocks connection, so these are "reported", not "verified".
             One attempt = one protocol tried, so a cycle where the primary protocol
             fails and the fallback succeeds is 1 failed + 1 success.
  success_rate = success / (success + failed).

Time handling: everything is UTC. Ranges of 1h/6h use 5-minute counters, ranges of
24h/7d/30d use hourly counters, so their start is aligned to the bucket boundary
(reported back as period.from). History starts when this feature was deployed.
"""
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import and_, case, desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.analytics import BUCKET_SECONDS, KNOWN_PROTOCOLS
from app.auth import verify_api_key
from app.cache import get_cache, set_cache
from app.database import get_db
from app.models import (
    ALL_APPS_KEY,
    ActiveUsersHistory,
    Notification,
    ProtocolMetrics,
    ServerTraffic5m,
    ServerTrafficHourly,
    VPNServer,
    VPNUserSession,
)

router = APIRouter(prefix="/admin/analytics", tags=["Admin - Analytics"])

# period -> (seconds, which table). 1h/6h need 5-minute resolution; longer ranges
# read the (much smaller) hourly table.
PERIODS = {
    "1h":  (3600,           "5m"),
    "6h":  (6 * 3600,       "5m"),
    "24h": (24 * 3600,      "hourly"),
    "7d":  (7 * 86400,      "hourly"),
    "30d": (30 * 86400,     "hourly"),
}
SERVER_CACHE_TTL = 20
OVERVIEW_LIVE_TTL = 15
OVERVIEW_METRICS_TTL = 60
OVERVIEW_TOP_SERVERS = 10
OVERVIEW_ALERTS = 5
ACTIVE_USERS_POINT_SECONDS = 900          # 15-minute points for the 24h active-users chart

DEFINITIONS = {
    "assigned": "Times the backend handed this server out to an app (a request sent to the server — not a confirmed connection).",
    "success": "Connection attempts the client app reported as successful. Reported by the app; not independently verified by the backend.",
    "failed": "Connection attempts the client app reported as failed. Reported by the app; attempts that never reach the reporting step are not counted.",
    "success_rate": "success / (success + failed).",
    "failure_rate": "failed / (success + failed).",
    "attempt": "One protocol tried. If the primary protocol fails and the fallback succeeds, that is 1 failed + 1 success.",
    "time": "All times are UTC. 1h and 6h use 5-minute buckets; 24h, 7d and 30d use hourly buckets, so the start is aligned to the bucket boundary.",
}


# ─── helpers ──────────────────────────────────────────────────────────────────

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _aware(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def _iso(dt: Optional[datetime]) -> Optional[str]:
    dt = _aware(dt)
    return dt.isoformat().replace("+00:00", "Z") if dt else None


def _i(value) -> int:
    return int(value or 0)


def _rate(numerator: int, denominator: int) -> Optional[float]:
    return round(numerator / denominator * 100, 2) if denominator > 0 else None


def _counts(assigned: int, success: int, failed: int) -> dict:
    attempts = success + failed
    return {
        "assigned": assigned,
        "success": success,
        "failed": failed,
        "attempts": attempts,
        "success_rate": _rate(success, attempts),
        "failure_rate": _rate(failed, attempts),
    }


async def _cached(key: str, ttl: int, compute: Callable[[], Awaitable[dict]]) -> dict:
    """Redis cache around a compute function. Any Redis error just skips the cache."""
    try:
        hit = await get_cache(key)
        if hit is not None:
            return hit
    except Exception:
        pass
    value = await compute()
    try:
        await set_cache(key, value, ttl=ttl)
    except Exception:
        pass
    return value


def _floor(ts: float, step: int) -> int:
    return int(ts // step) * step


# ─── per-server report ────────────────────────────────────────────────────────

async def _server_report(
    db: AsyncSession,
    server_id: int,
    period: str,
    country: Optional[str],
    protocol: Optional[str],
    scope: str,
) -> dict:
    server = (await db.execute(select(VPNServer).where(VPNServer.id == server_id))).scalar_one_or_none()
    if server is None:
        raise HTTPException(status_code=404, detail="Server not found")

    if scope == "machine":
        # Every app row of the same physical server + type (this is exactly how the
        # "Per Server" table on VPN Servers Analytics groups rows).
        members = (await db.execute(
            select(VPNServer.id, VPNServer.max_capacity, VPNServer.app_name).where(and_(
                VPNServer.ip_address == server.ip_address,
                VPNServer.server_type == server.server_type,
            ))
        )).all()
        capacity = max((_i(m.max_capacity) for m in members), default=0)   # rows share one machine capacity
        apps = sorted({m.app_name for m in members if m.app_name})
    else:
        members = [(server.id, server.max_capacity, server.app_name)]
        capacity = _i(server.max_capacity)
        apps = [server.app_name] if server.app_name else []
    ids = [m[0] for m in members]

    now = _utc_now()
    seconds, granularity = PERIODS[period]
    step = BUCKET_SECONDS if granularity == "5m" else 3600
    from_dt = datetime.fromtimestamp(_floor(now.timestamp() - seconds, step), tz=timezone.utc)
    table = ServerTraffic5m if granularity == "5m" else ServerTrafficHourly

    # live sessions (indexed by server_id; only currently-active sessions exist)
    live_rows = (await db.execute(
        select(VPNUserSession.protocol, func.count())
        .where(VPNUserSession.server_id.in_(ids))
        .group_by(VPNUserSession.protocol)
    )).all()
    by_protocol_live = {"openvpn": 0, "shadowsocks": 0, "other": 0}
    for proto, count in live_rows:
        by_protocol_live[proto if proto in KNOWN_PROTOCOLS else "other"] += _i(count)
    active = sum(by_protocol_live.values())

    # counters: one grouped query (<= ~2 x number-of-countries rows); filters are applied below
    grouped = (await db.execute(
        select(
            table.country, table.protocol,
            func.sum(table.assigned), func.sum(table.success), func.sum(table.failed),
        )
        .where(table.server_id.in_(ids), table.bucket_start >= from_dt)
        .group_by(table.country, table.protocol)
    )).all()
    rows = [(c, p, _i(a), _i(s), _i(f)) for c, p, a, s, f in grouped]

    data_start = (await db.execute(
        select(func.min(ServerTrafficHourly.bucket_start)).where(ServerTrafficHourly.server_id.in_(ids))
    )).scalar()

    cc = country.upper() if country else None

    def total(selected) -> dict:
        return _counts(sum(r[2] for r in selected), sum(r[3] for r in selected), sum(r[4] for r in selected))

    # summary respects both filters; the country table ignores the country filter
    # (so countries can always be compared); the protocol table ignores the protocol filter.
    summary = total([r for r in rows if (not cc or r[0] == cc) and (not protocol or r[1] == protocol)])

    per_country: Dict[str, list] = {}
    for c, p, a, s, f in rows:
        if protocol and p != protocol:
            continue
        acc = per_country.setdefault(c, [0, 0, 0])
        acc[0] += a; acc[1] += s; acc[2] += f
    by_country = [{"country": c, **_counts(*acc)} for c, acc in per_country.items()]
    by_country.sort(key=lambda r: (-r["attempts"], -r["assigned"], r["country"]))

    per_protocol: Dict[str, list] = {p: [0, 0, 0] for p in KNOWN_PROTOCOLS}
    for c, p, a, s, f in rows:
        if cc and c != cc:
            continue
        acc = per_protocol.setdefault(p, [0, 0, 0])
        acc[0] += a; acc[1] += s; acc[2] += f
    by_protocol = [
        {"protocol": p, **_counts(*acc)}
        for p, acc in per_protocol.items()
        if p in KNOWN_PROTOCOLS or any(acc)          # 'other' only when it has data
    ]
    by_protocol_total = total([r for r in rows if not cc or r[0] == cc])

    return {
        "server": {
            "id": server.id,
            "name": server.name,
            "ip_address": server.ip_address,
            "app_name": server.app_name,
            "server_type": server.server_type,
            "server_city": server.server_city,
            "server_country": server.server_country,
            "is_active": bool(server.is_active),
            "max_capacity": _i(server.max_capacity),
        },
        "scope": scope,
        "apps": apps,
        "server_ids": ids,
        "period": {
            "key": period,
            "from": _iso(from_dt),
            "to": _iso(now),
            "granularity": "5-minute" if granularity == "5m" else "hourly",
        },
        "filters": {"country": cc, "protocol": protocol},
        "live": {
            "active_sessions": active,
            "by_protocol": by_protocol_live,
            "capacity": capacity,
            "utilization_pct": round(active / capacity * 100, 2) if capacity > 0 else None,
            "as_of": _iso(now),
        },
        "summary": summary,
        "by_country": by_country,
        "by_protocol": by_protocol,
        "by_protocol_total": by_protocol_total,
        "data_start": _iso(data_start),
        "definitions": DEFINITIONS,
        "generated_at": _iso(now),
    }


@router.get("/servers/{server_id}")
async def server_analytics(
    server_id: int,
    period: str = Query("24h", pattern="^(1h|6h|24h|7d|30d)$"),
    country: Optional[str] = Query(None, pattern="^[A-Za-z]{2}$", description="ISO country code filter"),
    protocol: Optional[str] = Query(None, pattern="^(openvpn|shadowsocks)$"),
    scope: str = Query("server", pattern="^(server|machine)$",
                       description="server = this app's row; machine = all app rows of the same physical server"),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    """Requests / successes / failures for one server, with country and protocol breakdowns."""
    key = f"an:api:srv:{server_id}:{scope}:{period}:{(country or '-').upper()}:{protocol or '-'}"
    return await _cached(
        key, SERVER_CACHE_TTL,
        lambda: _server_report(db, server_id, period, country, protocol, scope),
    )


# ─── Home overview ────────────────────────────────────────────────────────────

async def _overview_live(db: AsyncSession, app_name: Optional[str]) -> dict:
    now = _utc_now()
    app_cond = [VPNServer.app_name == app_name] if app_name else []

    totals = (await db.execute(
        select(func.count(VPNServer.id), func.sum(case((VPNServer.is_active == True, 1), else_=0)))  # noqa: E712
        .where(*app_cond)
    )).one()
    servers_total, servers_active = _i(totals[0]), _i(totals[1])

    # One row per ACTIVE server row, with its live session count.
    load_rows = (await db.execute(
        select(
            VPNServer.id, VPNServer.name, VPNServer.server_city, VPNServer.server_country,
            VPNServer.app_name, VPNServer.max_capacity, VPNServer.cpu_usage, VPNServer.ram_usage,
            VPNServer.physical_machine_id, VPNServer.ip_address,
            func.count(VPNUserSession.id).label("sessions"),
        )
        .select_from(VPNServer)
        .outerjoin(VPNUserSession, VPNUserSession.server_id == VPNServer.id)
        .where(VPNServer.is_active == True, *app_cond)  # noqa: E712
        .group_by(VPNServer.id)
    )).all()

    # Capacity: each physical machine counted ONCE (rows of the same machine share its
    # capacity) — same rule as GET /admin/stats/capacity.
    per_machine: Dict[tuple, int] = {}
    current_sessions = 0
    for r in load_rows:
        key = ("machine", r.physical_machine_id) if r.physical_machine_id is not None else ("ip", r.ip_address)
        per_machine[key] = max(per_machine.get(key, 0), _i(r.max_capacity))
        current_sessions += _i(r.sessions)
    total_capacity = sum(per_machine.values())

    def utilisation(r) -> float:
        cap = _i(r.max_capacity)
        return round(_i(r.sessions) / cap * 100, 1) if cap > 0 else 0.0

    top = sorted(load_rows, key=lambda r: (-utilisation(r), -_i(r.sessions), r.name or ""))[:OVERVIEW_TOP_SERVERS]
    server_load = [
        {
            "id": r.id,
            "name": r.name,
            "city": r.server_city,
            "country": r.server_country,
            "app_name": r.app_name,
            "sessions": _i(r.sessions),
            "capacity": _i(r.max_capacity),
            "utilization_pct": utilisation(r),
            "cpu_pct": round(float(r.cpu_usage or 0), 1),
            "ram_pct": round(float(r.ram_usage or 0), 1),
        }
        for r in top
    ]

    proto_rows = (await db.execute(
        select(VPNUserSession.protocol, func.count())
        .join(VPNServer, VPNUserSession.server_id == VPNServer.id)
        .where(VPNServer.is_active == True, *app_cond)  # noqa: E712
        .group_by(VPNUserSession.protocol)
    )).all()
    protocol_sessions = {"openvpn": 0, "shadowsocks": 0, "other": 0}
    for proto, count in proto_rows:
        protocol_sessions[proto if proto in KNOWN_PROTOCOLS else "other"] += _i(count)

    notif_q = select(Notification).order_by(desc(Notification.created_at), desc(Notification.id)).limit(OVERVIEW_ALERTS)
    unread_q = select(func.count()).select_from(Notification).where(Notification.is_read == False)  # noqa: E712
    if app_name:
        notif_q = notif_q.where(Notification.app_name == app_name)
        unread_q = unread_q.where(Notification.app_name == app_name)
    notifications = (await db.execute(notif_q)).scalars().all()
    unread = _i((await db.execute(unread_q)).scalar())

    # Active users, last 24h — from the existing history snapshots (naive-UTC convention,
    # exactly like GET /admin/stats/user-history).
    history_key = app_name if app_name else ALL_APPS_KEY
    cutoff = datetime.utcnow() - timedelta(hours=24)
    history_rows = (await db.execute(
        select(ActiveUsersHistory.recorded_at, ActiveUsersHistory.total_users)
        .where(ActiveUsersHistory.app_name == history_key, ActiveUsersHistory.recorded_at >= cutoff)
        .order_by(ActiveUsersHistory.recorded_at)
    )).all()
    buckets: Dict[int, int] = {}
    for recorded_at, users in history_rows:
        ts = _aware(recorded_at).timestamp()
        b = _floor(ts, ACTIVE_USERS_POINT_SECONDS)
        buckets[b] = max(buckets.get(b, 0), _i(users))       # peak per bucket keeps short spikes
    active_users_24h = [
        {"t": _iso(datetime.fromtimestamp(b, tz=timezone.utc)), "users": u}
        for b, u in sorted(buckets.items())
    ]

    return {
        "app_name": app_name,
        "servers": {"total": servers_total, "active": servers_active},
        "sessions": {
            "current": current_sessions,
            "capacity": total_capacity,
            "available": max(0, total_capacity - current_sessions),
            "utilization_pct": round(current_sessions / total_capacity * 100, 2) if total_capacity > 0 else 0.0,
        },
        "protocol_sessions": protocol_sessions,
        "server_load": server_load,
        "alerts": {
            "unread": unread,
            "items": [
                {
                    "id": n.id,
                    "type": n.type,
                    "server_name": n.server_name,
                    "app_name": n.app_name,
                    "message": n.message,
                    "is_read": bool(n.is_read),
                    "created_at": _iso(n.created_at),
                }
                for n in notifications
            ],
        },
        "active_users_24h": active_users_24h,
        "generated_at": _iso(now),
    }


async def _overview_metrics(db: AsyncSession, app_name: Optional[str]) -> dict:
    """All-time protocol metrics (heavier — cached longer) + the last-24h hourly series."""
    now = _utc_now()

    pm = ProtocolMetrics
    has_time = and_(pm.avg_connect_time_ms > 0, pm.success_count > 0)
    q = select(
        pm.protocol,
        func.sum(pm.success_count),
        func.sum(pm.total_attempts),
        func.sum(case((has_time, pm.avg_connect_time_ms * pm.success_count), else_=0.0)),
        func.sum(case((has_time, pm.success_count), else_=0)),
    ).group_by(pm.protocol)
    if app_name:
        q = q.join(VPNServer, VPNServer.id == pm.server_id).where(VPNServer.app_name == app_name)
    rows = (await db.execute(q)).all()

    all_success = all_attempts = 0
    weighted_ms = weight = 0.0
    for _proto, success, attempts, ms_x_success, success_w in rows:
        all_success += _i(success)
        all_attempts += _i(attempts)
        weighted_ms += float(ms_x_success or 0)
        weight += float(success_w or 0)

    # last 24 hourly points (current hour included), protocol success rate per hour
    H = ServerTrafficHourly
    current_hour = _floor(now.timestamp(), 3600)
    first_hour = current_hour - 23 * 3600
    hq = (
        select(H.bucket_start, H.protocol, func.sum(H.success), func.sum(H.failed))
        .where(H.bucket_start >= datetime.fromtimestamp(first_hour, tz=timezone.utc),
               H.protocol.in_(list(KNOWN_PROTOCOLS)))
        .group_by(H.bucket_start, H.protocol)
    )
    since_q = select(func.min(H.bucket_start))
    if app_name:
        app_servers = select(VPNServer.id).where(VPNServer.app_name == app_name)
        hq = hq.where(H.server_id.in_(app_servers))
        since_q = since_q.where(H.server_id.in_(app_servers))
    hourly = (await db.execute(hq)).all()
    collecting_since = (await db.execute(since_q)).scalar()

    cell: Dict[tuple, tuple] = {}
    for bucket_start, proto, success, failed in hourly:
        cell[(int(_aware(bucket_start).timestamp()), proto)] = (_i(success), _i(failed))

    series = []
    day_success = day_failed = 0
    for h in range(first_hour, current_hour + 1, 3600):
        point = {"hour": _iso(datetime.fromtimestamp(h, tz=timezone.utc))}
        for proto in KNOWN_PROTOCOLS:
            s, f = cell.get((h, proto), (0, 0))
            point[proto] = _rate(s, s + f)
            point[f"{proto}_attempts"] = s + f
            day_success += s
            day_failed += f
        series.append(point)

    return {
        "all_time": {
            "success_rate": _rate(all_success, all_attempts),
            "attempts": all_attempts,
            "avg_connect_ms": round(weighted_ms / weight, 1) if weight > 0 else None,
        },
        "last_24h": {
            "success_rate": _rate(day_success, day_success + day_failed),
            "attempts": day_success + day_failed,
        },
        "protocol_success_24h": series,
        "collecting_since": _iso(collecting_since),
    }


@router.get("/overview")
async def overview(
    app_name: Optional[str] = Query(None, max_length=100, description="Filter to one app (its app_id); omit for all apps"),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    """Everything the Home Overview page needs, in one cached call."""
    tag = app_name or "-"
    live = await _cached(f"an:api:ov:live:{tag}", OVERVIEW_LIVE_TTL, lambda: _overview_live(db, app_name))
    metrics = await _cached(f"an:api:ov:met:{tag}", OVERVIEW_METRICS_TTL, lambda: _overview_metrics(db, app_name))
    return {**live, "metrics": metrics, "definitions": DEFINITIONS}
