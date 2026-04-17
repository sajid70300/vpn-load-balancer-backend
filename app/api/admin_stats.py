"""
Admin API - Statistics & Data Export
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from sqlalchemy.orm import selectinload
from typing import Optional
import io
import csv

from app.database import get_db
from app.models import VPNServer, VPNUserSession
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