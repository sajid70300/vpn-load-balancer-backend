"""
Admin API — connection analytics.

  GET /admin/analytics/servers/{server_id}   per-server report (requests, successes,
                                             failures, by country, by protocol)
  GET /admin/analytics/servers/{server_id}/series   the same server over time (graphs)
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
Custom from/to ranges are supported (clamped to the retained history, aligned to buckets).
The report cards and the graphs share one window (app/analytics_series.py), so the sum of
the graph points always equals the card totals.
"""
import re
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import and_, case, desc, func, literal_column, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.analytics import KNOWN_PROTOCOLS
from app.analytics_series import OTHER, TOP_COUNTRIES, Window, build_points, iso as _iso_epoch, resolve_window
from app.auth import verify_api_key
from app.cache import get_cache, set_cache
from app.config import settings
from app.database import get_db
from app.models import (
    ALL_APPS_KEY,
    ActiveUsersHistory,
    Notification,
    ProtocolMetrics,
    ServerTraffic5m,
    ServerTrafficHourly,
    ServerUsage5m,
    VPNServer,
    VPNUserSession,
)

router = APIRouter(prefix="/admin/analytics", tags=["Admin - Analytics"])

PERIOD_PATTERN = "^(1h|6h|24h|7d|30d|custom)$"
SERVER_CACHE_TTL = 20
# Longer ranges change slowly and cost more to compute, so they are cached longer.
SERIES_CACHE_TTL = {"1h": 20, "6h": 20, "24h": 30, "7d": 60, "30d": 120, "custom": 120}
QUERY_TIMEOUT_SECONDS = 15                # PostgreSQL statement_timeout for the graph queries
_SAFE_COUNTRY = re.compile(r"^[A-Z]{2}$")
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
    "time": "All times are UTC. The statistics cards read 5-minute buckets for 1h/6h and hourly buckets for longer ranges, so the start is aligned to the bucket boundary. Graphs use a bucket width that grows with the range (5 minutes up to 1 day) and never more than 300 points.",
    "capacity": "Capacity and sessions are snapshots taken about every 5 minutes, and each snapshot keeps the capacity that applied at that moment, so changing a server's capacity never rewrites history. While a server is disabled its capacity is not counted.",
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


# ─── shared helpers for the per-server report and graphs ──────────────────────

def _dt(epoch: int) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def _window_or_422(period: str, from_raw: Optional[str], to_raw: Optional[str],
                   resolution: Optional[int], now: datetime) -> Window:
    try:
        return resolve_window(
            period, from_raw, to_raw, resolution, int(now.timestamp()),
            settings.ANALYTICS_5M_RETENTION_DAYS, settings.ANALYTICS_HOURLY_RETENTION_DAYS,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


async def _guard_timeout(db: AsyncSession, seconds: int = QUERY_TIMEOUT_SECONDS) -> None:
    """Bound every statement of this request (PostgreSQL only): a runaway analytics query must
    fail fast instead of loading the database that also serves the VPN clients."""
    if db.get_bind().dialect.name == "postgresql":
        await db.execute(text(f"SET LOCAL statement_timeout = '{int(seconds)}s'"))


async def _members(db: AsyncSession, server: VPNServer, scope: str):
    """(ids, capacity, apps) for the requested scope. 'machine' = every app row of the same
    physical server + type (exactly how the 'Per Server' table groups rows)."""
    if scope == "machine":
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
    return [m[0] for m in members], capacity, apps


async def _first_bucket(db: AsyncSession, table, ids: List[int]) -> Optional[datetime]:
    """
    Earliest recorded bucket for any of these servers. One MIN() PER server id: each is an O(1)
    lookup at the start of the (server_id, bucket_start) index. A single MIN() over an IN-list
    would have to read every index entry of every listed server, which grows with history.
    """
    subs = [select(func.min(table.bucket_start)).where(table.server_id == i).scalar_subquery() for i in ids]
    row = (await db.execute(select(*subs))).one()
    found = [_aware(v) for v in row if v is not None]
    return min(found) if found else None


# ─── per-server report ────────────────────────────────────────────────────────

async def _server_report(
    db: AsyncSession,
    server_id: int,
    period: str,
    country: Optional[str],
    protocol: Optional[str],
    scope: str,
    window: Window,
) -> dict:
    server = (await db.execute(select(VPNServer).where(VPNServer.id == server_id))).scalar_one_or_none()
    if server is None:
        raise HTTPException(status_code=404, detail="Server not found")

    ids, capacity, apps = await _members(db, server, scope)

    now = _utc_now()
    granularity = window.report_source
    from_dt = _dt(window.from_s)
    to_dt = _dt(window.to_s) if window.to_s is not None else None
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
        .where(*([table.server_id.in_(ids), table.bucket_start >= from_dt]
                 + ([table.bucket_start < to_dt] if to_dt is not None else [])))
        .group_by(table.country, table.protocol)
    )).all()
    rows = [(c, p, _i(a), _i(s), _i(f)) for c, p, a, s, f in grouped]

    data_start = await _first_bucket(db, ServerTrafficHourly, ids)

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
            "to": _iso(to_dt) if to_dt is not None else _iso(now),
            "granularity": "5-minute" if granularity == "5m" else "hourly",
            "clamped": window.clamped,
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
    period: str = Query("24h", pattern=PERIOD_PATTERN),
    country: Optional[str] = Query(None, pattern="^[A-Za-z]{2}$", description="ISO country code filter"),
    protocol: Optional[str] = Query(None, pattern="^(openvpn|shadowsocks)$"),
    scope: str = Query("server", pattern="^(server|machine)$",
                       description="server = this app's row; machine = all app rows of the same physical server"),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
    from_: Optional[str] = Query(None, alias="from", max_length=40, description="custom range start (ISO, UTC)"),
    to: Optional[str] = Query(None, max_length=40, description="custom range end (ISO, UTC)"),
):
    """Requests / successes / failures for one server, with country and protocol breakdowns."""
    window = _window_or_422(period, from_, to, None, _utc_now())
    span = f":{window.from_s}:{window.to_s}" if period == "custom" else ""
    key = f"an:api:srv:{server_id}:{scope}:{period}{span}:{(country or '-').upper()}:{protocol or '-'}"
    return await _cached(
        key, SERVER_CACHE_TTL,
        lambda: _server_report(db, server_id, period, country, protocol, scope, window),
    )


# ─── per-server graphs (time series) ──────────────────────────────────────────

async def _series_report(
    db: AsyncSession,
    server_id: int,
    country: Optional[str],
    protocol: Optional[str],
    scope: str,
    window: Window,
) -> dict:
    await _guard_timeout(db)
    server = (await db.execute(select(VPNServer).where(VPNServer.id == server_id))).scalar_one_or_none()
    if server is None:
        raise HTTPException(status_code=404, detail="Server not found")
    ids, _capacity, apps = await _members(db, server, scope)

    now = _utc_now()
    from_dt = _dt(window.from_s)
    to_dt = _dt(window.to_s) if window.to_s is not None else None
    T = ServerTraffic5m if window.series_source == "5m" else ServerTrafficHourly
    cc = country.upper() if country else None

    base = [T.server_id.in_(ids), T.bucket_start >= from_dt]
    if to_dt is not None:
        base.append(T.bucket_start < to_dt)
    if protocol:
        base.append(T.protocol == protocol)

    # Which countries get their own line: a country filter -> just that one; otherwise the top N
    # by reported attempts for this range, and everything else is folded into OTHER. This keeps
    # the response (and the work after the scan) bounded however many countries exist.
    if cc:
        countries = [cc]
        group_expr = T.country
        traffic_filter = base + [T.country == cc]
    else:
        top = (await db.execute(
            select(T.country).where(*base).group_by(T.country)
            .order_by(desc(func.sum(T.success + T.failed)), desc(func.sum(T.assigned)), T.country)
            .limit(TOP_COUNTRIES)
        )).scalars().all()
        countries = [c for c in top if _SAFE_COUNTRY.fullmatch(c or "")]
        traffic_filter = base
        # The codes are inlined (validated A-Z above), not bound: PostgreSQL only accepts the
        # SELECT expression in GROUP BY if it is textually identical, which bind parameters break.
        in_list = ", ".join(f"'{c}'" for c in countries)
        group_expr = literal_column(
            f"CASE WHEN {T.__tablename__}.country IN ({in_list}) THEN {T.__tablename__}.country ELSE '{OTHER}' END"
        ) if countries else None

    traffic_rows = []
    if group_expr is not None:
        grouped = (await db.execute(
            select(
                T.bucket_start, group_expr.label("cg"), T.protocol,
                func.sum(T.assigned), func.sum(T.success), func.sum(T.failed),
            )
            .where(*traffic_filter)
            .group_by(T.bucket_start, group_expr, T.protocol)
        )).all()
        traffic_rows = [
            (int(_aware(b).timestamp()), cg, p, _i(a), _i(s), _i(f)) for b, cg, p, a, s, f in grouped
        ]
    if not cc and any(r[1] == OTHER for r in traffic_rows):
        countries = countries + [OTHER]

    # Sessions + the capacity in force at each snapshot (inactive servers contribute no capacity).
    U = ServerUsage5m
    usage_where = [U.server_id.in_(ids), U.bucket_start >= from_dt]
    if to_dt is not None:
        usage_where.append(U.bucket_start < to_dt)
    usage = (await db.execute(
        select(
            U.bucket_start, func.sum(U.active_sessions), func.sum(U.openvpn_sessions),
            func.sum(U.shadowsocks_sessions),
            func.max(case((U.is_active == True, U.max_capacity))),          # noqa: E712
            func.sum(case((U.is_active == True, 1), else_=0)),              # noqa: E712
        ).where(*usage_where).group_by(U.bucket_start)
    )).all()
    usage_rows = [
        (int(_aware(b).timestamp()), _i(sess), _i(ov), _i(ss), (int(cap) if cap is not None else None), _i(act))
        for b, sess, ov, ss, cap, act in usage
    ]

    traffic_start = await _first_bucket(db, ServerTrafficHourly, ids)
    usage_start = await _first_bucket(db, ServerUsage5m, ids)

    points = build_points(
        window, traffic_rows, usage_rows, countries,
        int(traffic_start.timestamp()) if traffic_start else None,
    )
    grid_end = window.from_s + window.points * window.step
    return {
        "scope": scope,
        "apps": apps,
        "server_ids": ids,
        "window": {
            "key": window.key,
            "from": _iso_epoch(window.from_s),
            "to": _iso_epoch(window.to_s if window.to_s is not None else min(grid_end, window.now_s)),
            "step_seconds": window.step,
            "source": "5-minute" if window.series_source == "5m" else "hourly",
            "points": len(points),
            "clamped": window.clamped,
            "resolution_adjusted": window.resolution_adjusted,
        },
        "filters": {"country": cc, "protocol": protocol},
        "countries": countries,
        "points": points,
        "data_start": _iso(traffic_start),
        "usage_start": _iso(usage_start),
        "retention_days": {
            "fine": settings.ANALYTICS_5M_RETENTION_DAYS,
            "hourly": settings.ANALYTICS_HOURLY_RETENTION_DAYS,
            "usage": settings.ANALYTICS_USAGE_RETENTION_DAYS,
        },
        "definitions": {"capacity": DEFINITIONS["capacity"], "time": DEFINITIONS["time"]},
        "generated_at": _iso(now),
    }


@router.get("/servers/{server_id}/series")
async def server_series(
    server_id: int,
    period: str = Query("24h", pattern=PERIOD_PATTERN),
    country: Optional[str] = Query(None, pattern="^[A-Za-z]{2}$", description="ISO country code filter"),
    protocol: Optional[str] = Query(None, pattern="^(openvpn|shadowsocks)$"),
    scope: str = Query("server", pattern="^(server|machine)$"),
    resolution: Optional[int] = Query(None, ge=300, le=86400,
                                      description="graph bucket width in seconds; omit for automatic"),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
    from_: Optional[str] = Query(None, alias="from", max_length=40, description="custom range start (ISO, UTC)"),
    to: Optional[str] = Query(None, max_length=40, description="custom range end (ISO, UTC)"),
):
    """
    The same statistics over time: requests / successes / failures (total, per protocol, per top
    country), sessions, capacity and utilization. Bounded work: at most 300 points, the top 5
    countries + OTHER, and one cached result per (server, range, filters).
    """
    window = _window_or_422(period, from_, to, resolution, _utc_now())
    key = (
        f"an:api:ser:{server_id}:{scope}:{period}:{window.from_s}:{window.to_s or 0}:{window.step}"
        f":{(country or '-').upper()}:{protocol or '-'}"
    )
    return await _cached(
        key, SERIES_CACHE_TTL[period],
        lambda: _series_report(db, server_id, country, protocol, scope, window),
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
