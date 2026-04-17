"""
Admin API - VPN Server Management (Unified)

Each VPNServer row represents one physical server for one app.
Both OpenVPN and Shadowsocks config fields live on the same row.
Sessions for both protocols are linked to this single server_id.
"""

from fastapi import APIRouter, Depends, Query, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, delete
from pydantic import BaseModel
from typing import Optional

from app.database import get_db
from app.models import VPNServer, VPNUserSession
from app.auth import verify_api_key
from app.cache import delete_cache
from app.audit import audit_log

router = APIRouter(prefix="/admin/servers", tags=["Admin - Servers"])


# ─── Pydantic schemas ──────────────────────────────────────────────────────────

class ServerCreate(BaseModel):
    name: str
    ip_address: str
    app_name: Optional[str] = None
    server_type: str = "free"          # free | premium
    server_city: Optional[str] = None
    server_country: Optional[str] = None
    flag_image_url: Optional[str] = None
    max_capacity: int = 100
    is_priority_group: bool = False
    monitoring_api_url: Optional[str] = None
    is_active: bool = True

    # OpenVPN
    management_port: int = 7505
    ovpn_base64: Optional[str] = None
    config_tag: Optional[str] = None
    cn_match: Optional[str] = None

    # Shadowsocks
    ss_port: int = 8388
    ss_password: str
    ss_encryption: str = "aes-256-gcm"


class ServerUpdate(BaseModel):
    name: Optional[str] = None
    ip_address: Optional[str] = None
    app_name: Optional[str] = None
    server_type: Optional[str] = None
    server_city: Optional[str] = None
    server_country: Optional[str] = None
    flag_image_url: Optional[str] = None
    max_capacity: Optional[int] = None
    is_priority_group: Optional[bool] = None
    monitoring_api_url: Optional[str] = None
    is_active: Optional[bool] = None

    management_port: Optional[int] = None
    ovpn_base64: Optional[str] = None
    config_tag: Optional[str] = None
    cn_match: Optional[str] = None

    ss_port: Optional[int] = None
    ss_password: Optional[str] = None
    ss_encryption: Optional[str] = None


# ─── Helper ───────────────────────────────────────────────────────────────────

def _server_out(server: VPNServer, current_users: int) -> dict:
    return {
        "id":                 server.id,
        "name":               server.name,
        "ip_address":         server.ip_address,
        "app_name":           server.app_name,
        "server_type":        server.server_type,
        "server_city":        server.server_city,
        "server_country":     server.server_country,
        "flag_image_url":     server.flag_image_url,
        "max_capacity":       server.max_capacity,
        "is_priority_group":  server.is_priority_group,
        "monitoring_api_url": server.monitoring_api_url,
        "is_active":          server.is_active,
        "display_order":      server.display_order,
        "current_users":      current_users,
        # OpenVPN
        "management_port":    server.management_port,
        "ovpn_base64":        (server.ovpn_base64[:100] + "...") if server.ovpn_base64 else None,
        "config_tag":         server.config_tag,
        "cn_match":           server.cn_match,
        # Shadowsocks
        "ss_port":            server.ss_port,
        "ss_password":        server.ss_password,
        "ss_encryption":      server.ss_encryption,
        # Metrics
        "cpu_usage":          round(server.cpu_usage, 2),
        "ram_usage":          round(server.ram_usage, 2),
        "ping_latency_ms":    round(server.ping_latency_ms, 2),
        "load_score":         round(server.load_score, 2),
        "last_health_check":  server.last_health_check,
        "peak_users":         server.peak_users,
        "peak_users_time":    server.peak_users_time,
        "peak_cpu":           server.peak_cpu,
        "peak_cpu_time":      server.peak_cpu_time,
        "peak_ram":           server.peak_ram,
        "peak_ram_time":      server.peak_ram_time,
    }


async def _clear_caches():
    await delete_cache("best_server_v2:*")
    await delete_cache("best_server:*")
    await delete_cache("servers_load:*")
    await delete_cache("servers_config:*")


# ─── Endpoints ────────────────────────────────────────────────────────────────

@router.get("/")
async def list_servers(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    search: Optional[str] = None,
    server_type: Optional[str] = Query(None, pattern="^(free|premium)$"),
    app_name: Optional[str] = None,
    is_active: Optional[bool] = None,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key),
):
    """List all VPN servers. Each row is one physical server (both protocols unified)."""
    conditions = []
    if server_type:
        conditions.append(VPNServer.server_type == server_type)
    if app_name:
        conditions.append(VPNServer.app_name == app_name)
    if is_active is not None:
        conditions.append(VPNServer.is_active == is_active)
    if search:
        pat = f"%{search}%"
        conditions.append(
            VPNServer.name.ilike(pat) |
            VPNServer.ip_address.ilike(pat) |
            VPNServer.server_city.ilike(pat)
        )

    q = (
        select(VPNServer, func.count(VPNUserSession.id).label('session_count'))
        .outerjoin(VPNUserSession)
        .order_by(VPNServer.display_order, VPNServer.name)
        .group_by(VPNServer.id)
    )
    if conditions:
        q = q.where(and_(*conditions))

    result = await db.execute(q)
    rows = result.all()

    servers_out = [_server_out(server, count) for server, count in rows]
    total = len(servers_out)

    return {"total": total, "skip": skip, "limit": limit, "servers": servers_out[skip: skip + limit]}


@router.get("/{server_id}")
async def get_server(
    server_id: int,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key),
):
    """Get a single server with full protocol config."""
    result = await db.execute(
        select(VPNServer, func.count(VPNUserSession.id).label('session_count'))
        .outerjoin(VPNUserSession)
        .where(VPNServer.id == server_id)
        .group_by(VPNServer.id)
    )
    row = result.one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail="Server not found")

    server, count = row
    entry = _server_out(server, count)
    # Return full ovpn_base64 (not truncated) for single-server view
    entry["ovpn_base64"] = server.ovpn_base64
    return entry


@router.post("/", status_code=status.HTTP_201_CREATED)
async def create_server(
    payload: ServerCreate,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key),
):
    """Create a VPN server with both OpenVPN and Shadowsocks config on one row."""
    # Check IP uniqueness (per app_name + server_type)
    app_cond = VPNServer.app_name.is_(None) if payload.app_name is None else VPNServer.app_name == payload.app_name
    existing = await db.execute(
        select(VPNServer).where(and_(
            VPNServer.ip_address  == payload.ip_address,
            app_cond,
            VPNServer.server_type == payload.server_type,
        ))
    )
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=400,
            detail=f"Server already exists for {payload.ip_address} / app={payload.app_name} / {payload.server_type}"
        )

    max_order_result = await db.execute(select(func.max(VPNServer.display_order)))
    max_order = max_order_result.scalar() or 0

    server = VPNServer(
        name               = payload.name,
        ip_address         = payload.ip_address,
        app_name           = payload.app_name,
        server_type        = payload.server_type,
        server_city        = payload.server_city,
        server_country     = payload.server_country,
        flag_image_url     = payload.flag_image_url,
        max_capacity       = payload.max_capacity,
        is_priority_group  = payload.is_priority_group,
        monitoring_api_url = payload.monitoring_api_url,
        is_active          = payload.is_active,
        display_order      = max_order + 1,
        cpu_usage          = 0.0,
        ram_usage          = 0.0,
        ping_latency_ms    = 0.0,
        load_score         = 0.0,
        # OpenVPN
        management_port    = payload.management_port,
        ovpn_base64        = payload.ovpn_base64,
        config_tag         = payload.config_tag,
        cn_match           = payload.cn_match,
        # Shadowsocks
        ss_port            = payload.ss_port,
        ss_password        = payload.ss_password,
        ss_encryption      = payload.ss_encryption,
    )

    db.add(server)
    await db.commit()
    await db.refresh(server)

    await _clear_caches()
    await audit_log(db, token, action="server.create", resource_type="server",
        resource_id=str(server.id), app_name=payload.app_name,
        details={"name": payload.name, "ip_address": payload.ip_address, "server_id": server.id})

    return {
        "message":    "Server created successfully",
        "server_id":  server.id,
        "name":       server.name,
        "ip_address": server.ip_address,
    }


@router.put("/{server_id}")
async def update_server(
    server_id: int,
    payload: ServerUpdate,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key),
):
    """Update a server. All fields optional for partial update."""
    result = await db.execute(select(VPNServer).where(VPNServer.id == server_id))
    server = result.scalar_one_or_none()
    if not server:
        raise HTTPException(status_code=404, detail="Server not found")

    for field in ("name", "ip_address", "app_name", "server_type", "server_city",
                  "server_country", "flag_image_url", "max_capacity",
                  "is_priority_group", "monitoring_api_url", "is_active",
                  "management_port", "ovpn_base64", "config_tag", "cn_match",
                  "ss_port", "ss_password", "ss_encryption"):
        val = getattr(payload, field)
        if val is not None:
            setattr(server, field, val)

    await db.commit()
    await _clear_caches()
    await audit_log(db, token, action="server.update", resource_type="server",
        resource_id=str(server_id), app_name=server.app_name,
        details={"name": server.name, "ip_address": server.ip_address})

    return {"message": "Server updated successfully", "server_id": server_id}


@router.delete("/{server_id}")
async def delete_server(
    server_id: int,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key),
):
    """Delete a server and all its sessions."""
    result = await db.execute(select(VPNServer).where(VPNServer.id == server_id))
    server = result.scalar_one_or_none()
    if not server:
        raise HTTPException(status_code=404, detail="Server not found")

    server_name = server.name

    deleted_sessions = await db.execute(
        delete(VPNUserSession).where(VPNUserSession.server_id == server_id)
    )
    await db.execute(delete(VPNServer).where(VPNServer.id == server_id))
    await db.commit()

    await _clear_caches()
    await audit_log(db, token, action="server.delete", resource_type="server",
        resource_id=str(server_id), app_name=server.app_name,
        details={"name": server_name, "server_id": server_id})

    return {
        "message":          f"Server '{server_name}' deleted",
        "server_id":        server_id,
        "deleted_sessions": deleted_sessions.rowcount,
    }


@router.post("/{server_id}/toggle-active")
async def toggle_server_active(
    server_id: int,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key),
):
    """Toggle is_active on the server."""
    result = await db.execute(select(VPNServer).where(VPNServer.id == server_id))
    server = result.scalar_one_or_none()
    if not server:
        raise HTTPException(status_code=404, detail="Server not found")

    server.is_active = not server.is_active
    await db.commit()
    await _clear_caches()
    await audit_log(db, token, action="server.toggle_active", resource_type="server",
        resource_id=str(server_id), app_name=server.app_name,
        details={"name": server.name, "is_active": server.is_active})

    return {
        "message":   f"Server {'activated' if server.is_active else 'deactivated'}",
        "server_id": server_id,
        "is_active": server.is_active,
    }


@router.post("/{server_id}/reset-peaks")
async def reset_server_peaks(
    server_id: int,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key),
):
    """Reset peak values for a server."""
    result = await db.execute(select(VPNServer).where(VPNServer.id == server_id))
    server = result.scalar_one_or_none()
    if not server:
        raise HTTPException(status_code=404, detail="Server not found")

    server.peak_users      = 0
    server.peak_users_time = None
    server.peak_cpu        = 0.0
    server.peak_cpu_time   = None
    server.peak_ram        = 0.0
    server.peak_ram_time   = None

    await db.commit()
    await audit_log(db, token, action="server.reset_peaks", resource_type="server",
        resource_id=str(server_id), app_name=server.app_name,
        details={"name": server.name})

    return {"message": "Peak values reset successfully", "server_id": server_id}