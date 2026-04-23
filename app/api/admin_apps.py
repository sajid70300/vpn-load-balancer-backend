"""
Admin API - App Management
Manages VPN application tenants.

Each physical server is finalized for an app as a SINGLE VPNServer row
containing both OpenVPN and Shadowsocks config.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, delete
from typing import List, Optional

from app.database import get_db
from app.models import App, VPNServer, VPNUserSession, PhysicalMachine
from app.schemas import AppCreate, AppResponse, AppAnalytics
from app.auth import verify_api_key
from app.audit import audit_log

router = APIRouter(prefix="/admin/apps", tags=["Admin - App Management"])


# ─── App CRUD ─────────────────────────────────────────────────────────────────

@router.get("/", response_model=List[AppResponse])
async def list_apps(
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key)
):
    result = await db.execute(select(App).order_by(App.created_at.desc()))
    apps   = result.scalars().all()

    response = []
    for app in apps:
        # Count distinct physical servers (one row per server now)
        server_count_result = await db.execute(
            select(func.count()).select_from(VPNServer)
            .where(VPNServer.app_name == app.app_id)
        )
        total_servers = server_count_result.scalar() or 0

        session_count_result = await db.execute(
            select(func.count()).select_from(VPNUserSession)
            .join(VPNServer, VPNUserSession.server_id == VPNServer.id)
            .where(VPNServer.app_name == app.app_id)
        )
        active_users = session_count_result.scalar() or 0

        response.append(AppResponse(
            id=app.id,
            name=app.name,
            app_id=app.app_id,
            status=app.status,
            created_at=app.created_at,
            updated_at=app.updated_at,
            active_users=active_users,
            total_servers=total_servers,
        ))

    return response


@router.post("/", response_model=AppResponse, status_code=status.HTTP_201_CREATED)
async def create_app(
    payload: AppCreate,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key)
):
    existing = await db.execute(select(App).where(App.app_id == payload.app_id))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=400, detail=f"App '{payload.app_id}' already exists")

    app = App(name=payload.name, app_id=payload.app_id)
    db.add(app)
    await db.commit()
    await db.refresh(app)

    await audit_log(db, token, action="app.create", resource_type="app",
        resource_id=app.app_id, app_name=app.app_id,
        details={"name": app.name, "app_id": app.app_id})

    return AppResponse(
        id=app.id, name=app.name, app_id=app.app_id, status=app.status,
        created_at=app.created_at, updated_at=app.updated_at,
        active_users=0, total_servers=0,
    )


@router.delete("/{app_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_app(
    app_id: str,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key)
):
    result = await db.execute(select(App).where(App.app_id == app_id))
    app = result.scalar_one_or_none()
    if not app:
        raise HTTPException(status_code=404, detail="App not found")

    # Delete all sessions for this app's servers
    server_ids_result = await db.execute(
        select(VPNServer.id).where(VPNServer.app_name == app_id)
    )
    server_ids = [row[0] for row in server_ids_result.all()]

    if server_ids:
        await db.execute(
            delete(VPNUserSession).where(VPNUserSession.server_id.in_(server_ids))
        )
        await db.execute(
            delete(VPNServer).where(VPNServer.id.in_(server_ids))
        )

    await db.delete(app)
    await db.commit()

    await audit_log(db, token, action="app.delete", resource_type="app",
        resource_id=app_id, app_name=app_id, details={"name": app.name})


@router.post("/{app_id}/toggle-status")
async def toggle_app_status(
    app_id: str,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key)
):
    result = await db.execute(select(App).where(App.app_id == app_id))
    app = result.scalar_one_or_none()
    if not app:
        raise HTTPException(status_code=404, detail="App not found")

    app.status = 'inactive' if app.status == 'active' else 'active'
    await db.commit()

    await audit_log(db, token, action="app.toggle_status", resource_type="app",
        resource_id=app_id, app_name=app_id, details={"status": app.status})

    return {"app_id": app_id, "status": app.status}


@router.get("/{app_id}/analytics", response_model=AppAnalytics)
async def get_app_analytics(
    app_id: str,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key)
):
    result = await db.execute(select(App).where(App.app_id == app_id))
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="App not found")

    app_result = await db.execute(select(App).where(App.app_id == app_id))
    app = app_result.scalar_one_or_none()

    servers_result = await db.execute(
        select(VPNServer, func.count(VPNUserSession.id).label('sessions'))
        .outerjoin(VPNUserSession)
        .where(VPNServer.app_name == app_id)
        .group_by(VPNServer.id)
    )
    servers_data = servers_result.all()

    total_servers  = len(servers_data)
    active_servers = sum(1 for s, _ in servers_data if s.is_active)
    active_sessions = sum(c for _, c in servers_data)
    total_capacity  = sum(s.max_capacity for s, _ in servers_data if s.is_active)

    load_pcts = []
    for s, c in servers_data:
        if s.is_active and s.max_capacity > 0:
            load_pcts.append((c / s.max_capacity) * 100)
    current_load_pct = round(sum(load_pcts) / len(load_pcts), 2) if load_pcts else 0.0

    return AppAnalytics(
        app_id=app_id,
        name=app.name,
        current_load_pct=current_load_pct,
        active_sessions=active_sessions,
        total_servers=total_servers,
        active_servers=active_servers,
        total_capacity=total_capacity,
    )


# ─── Finalized server endpoints ───────────────────────────────────────────────

class FinalizeServerPayload(BaseModel):
    """
    Sent from AppConfigure when configuring a PhysicalMachine for this app.
    Creates (or updates) a single VPNServer row for this machine+app.
    server_type can be overridden per-app (free or premium),
    allowing the same physical machine to serve both tiers in the same app.
    """
    machine_id:        int
    server_type:       str = "free"  # overrides machine default; free | premium
    is_priority_group: bool = False

    # OpenVPN config
    management_port: int = 7505
    ovpn_base64: Optional[str] = None
    config_tag: Optional[str] = None
    cn_match: Optional[str] = None

    # Shadowsocks config
    ss_port: int = 8388
    ss_password: str
    ss_encryption: str = "aes-256-gcm"


@router.get("/{app_id}/servers")
async def list_app_servers(
    app_id: str,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key),
):
    """List all finalized VPNServer entries for this app (one row per physical machine)."""
    result = await db.execute(select(App).where(App.app_id == app_id))
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="App not found")

    rows_result = await db.execute(
        select(VPNServer, func.count(VPNUserSession.id).label('session_count'))
        .outerjoin(VPNUserSession)
        .where(VPNServer.app_name == app_id)
        .order_by(VPNServer.display_order, VPNServer.name)
        .group_by(VPNServer.id)
    )
    rows = rows_result.all()

    out = []
    for server, session_count in rows:
        out.append({
            "id":                  server.id,
            "physical_machine_id": server.physical_machine_id,
            "name":                server.name,
            "ip_address":          server.ip_address,
            "app_name":            server.app_name,
            "server_type":         server.server_type,
            "server_city":         server.server_city,
            "server_country":      server.server_country,
            "flag_image_url":      server.flag_image_url,
            "max_capacity":        server.max_capacity,
            "is_priority_group":   server.is_priority_group,
            "is_active":           server.is_active,
            "current_users":       session_count,
            "cpu_usage":           round(server.cpu_usage, 2),
            "ram_usage":           round(server.ram_usage, 2),
            "ping_latency_ms":     round(server.ping_latency_ms, 2),
            "load_score":          round(server.load_score, 2),
            # Protocol availability flags
            "has_openvpn":         server.management_port is not None,
            "has_shadowsocks":     server.ss_port is not None and server.ss_password is not None,
            # OpenVPN config (for edit pre-fill)
            "management_port":     server.management_port,
            "ovpn_base64":         server.ovpn_base64,
            "config_tag":          server.config_tag,
            "cn_match":            server.cn_match,
            # Shadowsocks config (for edit pre-fill)
            "ss_port":             server.ss_port,
            "ss_password":         server.ss_password,
            "ss_encryption":       server.ss_encryption,
        })

    return {"app_id": app_id, "servers": out, "total": len(out)}


@router.post("/{app_id}/servers/finalize", status_code=201)
async def finalize_server_for_app(
    app_id:  str,
    payload: FinalizeServerPayload,
    db:      AsyncSession = Depends(get_db),
    token:   str          = Depends(verify_api_key),
):
    """
    Finalize a PhysicalMachine for this app with full protocol config.
    Creates a SINGLE VPNServer row with both OpenVPN and Shadowsocks fields.
    If a row already exists for this machine+app, it is UPDATED (idempotent).
    """
    app_result = await db.execute(select(App).where(App.app_id == app_id))
    if not app_result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="App not found")

    m_result = await db.execute(
        select(PhysicalMachine).where(PhysicalMachine.id == payload.machine_id)
    )
    machine = m_result.scalar_one_or_none()
    if not machine:
        raise HTTPException(status_code=404, detail="Physical machine not found")

    # Check if already finalized for this app+server_type combination (update path)
    # Same machine can be finalized twice in the same app as free AND premium
    existing_result = await db.execute(
        select(VPNServer).where(
            and_(
                VPNServer.physical_machine_id == payload.machine_id,
                VPNServer.app_name == app_id,
                VPNServer.server_type == payload.server_type,
            )
        )
    )
    existing = existing_result.scalar_one_or_none()

    max_order_result = await db.execute(select(func.max(VPNServer.display_order)))
    max_order = max_order_result.scalar() or 0

    shared = dict(
        physical_machine_id = machine.id,
        name                = machine.name,
        ip_address          = machine.ip_address,
        app_name            = app_id,
        server_type         = payload.server_type,
        server_city         = machine.server_city,
        server_country      = machine.server_country,
        flag_image_url      = machine.flag_image_url,
        max_capacity        = machine.max_capacity,
        monitoring_api_url  = machine.monitoring_api_url,
        is_active           = machine.is_active,
        is_priority_group   = payload.is_priority_group,
        # OpenVPN
        management_port     = payload.management_port,
        ovpn_base64         = payload.ovpn_base64,
        config_tag          = payload.config_tag,
        cn_match            = payload.cn_match,
        # Shadowsocks
        ss_port             = payload.ss_port,
        ss_password         = payload.ss_password,
        ss_encryption       = payload.ss_encryption,
    )

    if existing:
        for k, v in shared.items():
            setattr(existing, k, v)
        server = existing
    else:
        server = VPNServer(
            **shared,
            display_order   = max_order + 1,
            cpu_usage       = 0.0,
            ram_usage       = 0.0,
            ping_latency_ms = 0.0,
            load_score      = 0.0,
        )
        db.add(server)

    await db.commit()
    await db.refresh(server)

    from app.cache import delete_cache
    await delete_cache("best_server_v2:*")
    await delete_cache("best_server:*")
    await delete_cache("servers_load:*")
    await delete_cache("servers_config:*")

    action = "app.update_server" if existing else "app.finalize_server"
    await audit_log(db, token, action=action, resource_type="app",
        resource_id=app_id, app_name=app_id,
        details={"machine_id": machine.id, "ip_address": machine.ip_address,
                 "server_id": server.id})

    return {
        "message":    f"Server '{machine.name}' finalized for app '{app_id}'",
        "server_id":  server.id,
        "ip_address": machine.ip_address,
    }


@router.delete("/{app_id}/servers/{server_id}", status_code=204)
async def remove_finalized_server(
    app_id:    str,
    server_id: int,
    db:        AsyncSession = Depends(get_db),
    token:     str          = Depends(verify_api_key),
):
    """
    Remove a finalized server from this app.
    Deletes the VPNServer row and its sessions.
    The PhysicalMachine record is NOT deleted.
    """
    anchor_result = await db.execute(
        select(VPNServer).where(
            and_(VPNServer.id == server_id, VPNServer.app_name == app_id)
        )
    )
    server = anchor_result.scalar_one_or_none()
    if not server:
        raise HTTPException(status_code=404, detail="Finalized server not found for this app")

    await db.execute(
        delete(VPNUserSession).where(VPNUserSession.server_id == server_id)
    )
    await db.execute(
        delete(VPNServer).where(VPNServer.id == server_id)
    )
    await db.commit()

    from app.cache import delete_cache
    await delete_cache("best_server_v2:*")
    await delete_cache("best_server:*")
    await delete_cache("servers_load:*")
    await delete_cache("servers_config:*")

    await audit_log(db, token, action="app.remove_server", resource_type="app",
        resource_id=app_id, app_name=app_id,
        details={"machine_id": server.physical_machine_id, "server_id": server_id})