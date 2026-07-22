"""
Admin API - Statistics & Data Export
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, cast, Date
from sqlalchemy.orm import selectinload
from typing import Optional
from datetime import datetime, timedelta, timezone
from calendar import monthrange
import io
import csv

from app.database import get_db
from app.models import VPNServer, VPNUserSession, SystemPeakStats, ActiveUsersHistory
from app.auth import verify_api_key

router = APIRouter(prefix="/admin", tags=["Admin - Stats & Export"])


@router.get("/stats/summary")
async def get_summary_stats(
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key)
):
    """Get summary statistics for dashboard."""

    total_servers_result = await db.execute(select(func.count()).select_from(VPNServer))
    total_servers = total_servers_result.scalar()

    active_servers_result = await db.execute(
        select(func.count()).select_from(VPNServer).where(VPNServer.is_active == True)
    )
    active_servers = active_servers_result.scalar()

    capacity_result = await db.execute(
        select(func.sum(VPNServer.max_capacity)).select_from(VPNServer)
        .where(VPNServer.is_active == True)
    )
    total_capacity = capacity_result.scalar() or 0

    total_users_result = await db.execute(select(func.count()).select_from(VPNUserSession))
    total_users = total_users_result.scalar()

    avg_load_result = await db.execute(
        select(func.avg(VPNServer.load_score)).select_from(VPNServer)
        .where(VPNServer.is_active == True)
    )
    avg_load_score = avg_load_result.scalar() or 0.0

    free_servers_result = await db.execute(
        select(func.count()).select_from(VPNServer)
        .where(and_(VPNServer.is_active == True, VPNServer.server_type == 'free'))
    )
    free_servers = free_servers_result.scalar()

    premium_servers_result = await db.execute(
        select(func.count()).select_from(VPNServer)
        .where(and_(VPNServer.is_active == True, VPNServer.server_type == 'premium'))
    )
    premium_servers = premium_servers_result.scalar()

    bandwidth_result = await db.execute(
        select(
            func.sum(VPNUserSession.bytes_received),
            func.sum(VPNUserSession.bytes_sent)
        ).select_from(VPNUserSession)
    )
    bandwidth = bandwidth_result.first()
    total_bytes_received = bandwidth[0] or 0
    total_bytes_sent     = bandwidth[1] or 0

    return {
        "servers": {
            "total":    total_servers,
            "active":   active_servers,
            "inactive": total_servers - active_servers,
            "free":     free_servers,
            "premium":  premium_servers,
        },
        "users": {
            "total":            total_users,
            "capacity":         total_capacity,
            "usage_percentage": round((total_users / total_capacity * 100), 2) if total_capacity > 0 else 0,
        },
        "performance": {
            "avg_load_score": round(avg_load_score, 2),
        },
        "bandwidth": {
            "total_received_bytes": total_bytes_received,
            "total_sent_bytes":     total_bytes_sent,
            "total_received_gb":    round(total_bytes_received / 1024 / 1024 / 1024, 2),
            "total_sent_gb":        round(total_bytes_sent     / 1024 / 1024 / 1024, 2),
        },
    }


@router.get("/stats/apps")
async def get_app_stats(
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key)
):
    """Get statistics grouped by app_name."""

    apps_result = await db.execute(
        select(VPNServer.app_name).distinct().where(VPNServer.app_name.isnot(None))
    )
    app_names = [row[0] for row in apps_result]

    app_stats = []
    for app_name in app_names:
        server_count_result = await db.execute(
            select(func.count()).select_from(VPNServer)
            .where(and_(VPNServer.app_name == app_name, VPNServer.is_active == True))
        )
        server_count = server_count_result.scalar()

        user_count_result = await db.execute(
            select(func.count()).select_from(VPNUserSession)
            .join(VPNServer)
            .where(VPNServer.app_name == app_name)
        )
        user_count = user_count_result.scalar()

        app_stats.append({
            "app_name":      app_name,
            "active_servers": server_count,
            "total_users":   user_count,
        })

    return {"apps": app_stats}


@router.get("/stats/peak-users")
async def get_peak_users(
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key)
):
    """
    All-time peak concurrent active-user count and when it happened.
    Updated by a dedicated Celery task every ~60s
    (see app/tasks.py: track_active_users_snapshot()).
    """
    result = await db.execute(select(SystemPeakStats).where(SystemPeakStats.id == 1))
    row = result.scalar_one_or_none()

    if not row:
        return {"peak_users": 0, "peak_at": None}

    return {"peak_users": row.peak_users, "peak_at": row.peak_at}


@router.get("/stats/user-history")
async def get_user_history(
    range: str = Query("7d", pattern="^(24h|7d|30d|all)$"),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key)
):
    """
    Active-user snapshots recorded roughly every 2 hours, for the VPN Server
    Analytics 'History' trend chart.
    range: 24h | 7d | 30d | all (default 7d).
    """
    query = select(ActiveUsersHistory).order_by(ActiveUsersHistory.recorded_at.asc())

    if range != "all":
        hours_map = {"24h": 24, "7d": 24 * 7, "30d": 24 * 30}
        cutoff = datetime.utcnow() - timedelta(hours=hours_map[range])
        query = query.where(ActiveUsersHistory.recorded_at >= cutoff)

    result = await db.execute(query)
    rows = result.scalars().all()

    return {
        "range": range,
        "points": [
            {"recorded_at": r.recorded_at, "total_users": r.total_users}
            for r in rows
        ],
    }


@router.get("/stats/daily-peaks")
async def get_daily_peaks(
    month: str = Query(..., pattern="^[0-9]{4}-(0[1-9]|1[0-2])$", description="YYYY-MM"),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key)
):
    """
    One peak-users value per calendar day for the given month — a
    'when were we busier/quieter' record.

    Deliberately derived from data we already have (active_users_history,
    recorded every ~2 hours) via a day-level MAX() aggregation — no new
    table, no new Celery task. Trade-off: if a day's true peak happened
    between two snapshots, this can slightly understate it. Acceptable for
    a rough historical record; not intended as an exact/audited figure.

    Only returns days up to today for the current month (future days
    haven't happened yet, so they're omitted rather than shown as a
    misleading zero).
    """
    year, mon = map(int, month.split("-"))
    days_in_month = monthrange(year, mon)[1]

    now_utc = datetime.now(timezone.utc)
    if (year, mon) > (now_utc.year, now_utc.month):
        days_to_generate = 0
    elif (year, mon) == (now_utc.year, now_utc.month):
        days_to_generate = now_utc.day
    else:
        days_to_generate = days_in_month

    if days_to_generate == 0:
        return {"month": month, "days": []}

    month_start = datetime(year, mon, 1, tzinfo=timezone.utc)
    month_end   = datetime(year, mon, days_to_generate, 23, 59, 59, tzinfo=timezone.utc)

    day_col = cast(ActiveUsersHistory.recorded_at, Date)
    query = (
        select(day_col.label("day"), func.max(ActiveUsersHistory.total_users).label("peak_users"))
        .where(and_(
            ActiveUsersHistory.recorded_at >= month_start,
            ActiveUsersHistory.recorded_at <= month_end,
        ))
        .group_by(day_col)
    )
    result = await db.execute(query)
    peaks_by_day = {row.day.isoformat(): row.peak_users for row in result.all()}

    days = []
    for d in range(1, days_to_generate + 1):
        day_str = f"{year:04d}-{mon:02d}-{d:02d}"
        days.append({"day": day_str, "peak_users": peaks_by_day.get(day_str, 0)})

    return {"month": month, "days": days}


@router.get("/export/servers")
async def export_servers_csv(
    server_type: Optional[str] = Query(None, pattern="^(free|premium)$"),
    app_name: Optional[str] = None,
    is_active: Optional[bool] = None,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key)
):
    """Export servers to CSV."""
    query = select(VPNServer).order_by(VPNServer.display_order, VPNServer.name)
    conditions = []
    if server_type:
        conditions.append(VPNServer.server_type == server_type)
    if app_name:
        conditions.append(VPNServer.app_name == app_name)
    if is_active is not None:
        conditions.append(VPNServer.is_active == is_active)
    if conditions:
        query = query.where(and_(*conditions))

    result = await db.execute(query)
    servers = result.scalars().all()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        'ID', 'Name', 'IP Address', 'Management Port', 'Server Type', 'App Name',
        'City', 'Max Capacity', 'Is Active', 'Is Priority', 'Config Tag', 'CN Match',
        'SS Port', 'SS Encryption',
        'CPU Usage', 'RAM Usage', 'Ping (ms)', 'Load Score',
        'Peak Users', 'Peak CPU', 'Peak RAM', 'Last Health Check'
    ])
    for server in servers:
        writer.writerow([
            server.id,
            server.name,
            server.ip_address,
            server.management_port,
            server.server_type,
            server.app_name or '',
            server.server_city or '',
            server.max_capacity,
            'Yes' if server.is_active else 'No',
            'Yes' if server.is_priority_group else 'No',
            server.config_tag or '',
            server.cn_match or '',
            server.ss_port or '',
            server.ss_encryption or '',
            f"{server.cpu_usage:.1f}%",
            f"{server.ram_usage:.1f}%",
            f"{server.ping_latency_ms:.1f}",
            f"{server.load_score:.2f}",
            server.peak_users,
            f"{server.peak_cpu:.1f}%",
            f"{server.peak_ram:.1f}%",
            server.last_health_check.isoformat() if server.last_health_check else '',
        ])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=vpn_servers_export.csv"}
    )


@router.get("/export/sessions")
async def export_sessions_csv(
    server_type: Optional[str] = Query(None, pattern="^(free|premium)$"),
    app_name: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key)
):
    """Export user sessions to CSV."""
    query = select(VPNUserSession).options(selectinload(VPNUserSession.server))

    conditions = []
    if server_type:
        conditions.append(VPNServer.server_type == server_type)
    if app_name:
        conditions.append(VPNServer.app_name == app_name)
    if conditions:
        query = query.join(VPNServer).where(and_(*conditions))

    query = query.order_by(VPNUserSession.connected_time.desc())

    result = await db.execute(query)
    sessions = result.scalars().all()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        'Session ID', 'User ID', 'Device IP', 'Protocol', 'Server Name', 'Server Type',
        'App Name', 'Config Tag', 'Connected Time', 'Bytes Received',
        'Bytes Sent', 'Total Bandwidth (MB)'
    ])
    for session in sessions:
        total_mb = (session.bytes_received + session.bytes_sent) / 1024 / 1024
        writer.writerow([
            session.id,
            session.user_id,
            session.device_ip,
            session.protocol,
            session.server.name,
            session.server.server_type,
            session.server.app_name or '',
            session.config_tag or '',
            session.connected_time.isoformat(),
            session.bytes_received,
            session.bytes_sent,
            f"{total_mb:.2f}",
        ])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=vpn_sessions_export.csv"}
    )