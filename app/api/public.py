"""
Public API endpoints with Decision Engine Integration
Supports both OpenVPN and Shadowsocks with intelligent protocol selection.
"""

from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from sqlalchemy.orm import selectinload
from typing import Optional
from datetime import datetime
import random
import geoip2.database
import geoip2.errors

from app.database import get_db
from app.models import VPNServer, VPNUserSession
from app.schemas import (
    BestServerResponse, BestServerDecision, ConnectionFeedback, ShadowsocksDisconnect,
    ServersLoadResponse, ServerLoadItem, LoadSummary,
    AllUsersResponse, UserSession, ServerConfig, ServerConfigDecision
)
from app.auth import verify_api_key
from app.cache import get_cache, set_cache, delete_cache
from app.decision_engine import DecisionEngine
from app.config import settings

router = APIRouter()


@router.get("/v1/my-info/", tags=["Public API"])
async def my_info(request: Request):
    """
    Returns the caller's IP address, country code, and ASN.
    Uses local MaxMind GeoLite2 databases — no external API calls, no rate limits.

    Mobile apps call this FIRST, then pass country + asn to /v2/best_server/.

    Example response:
        { "ip": "1.2.3.4", "country": "PK", "asn": "AS17557", "isp": "Pakistan Telecom" }
    """
    # Get real client IP (works behind proxies/nginx too)
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        ip = forwarded_for.split(",")[0].strip()
    else:
        ip = request.client.host

    result = {
        "ip": ip,
        "country": None,
        "asn": None,
        "isp": None,
    }

    # Country lookup
    try:
        with geoip2.database.Reader(settings.GEOIP_COUNTRY_PATH) as reader:
            country_response = reader.country(ip)
            result["country"] = country_response.country.iso_code  # e.g. "PK"
    except (geoip2.errors.AddressNotFoundError, Exception):
        pass  # country stays None — caller should handle gracefully

    # ASN lookup
    try:
        with geoip2.database.Reader(settings.GEOIP_ASN_PATH) as reader:
            asn_response = reader.asn(ip)
            result["asn"] = f"AS{asn_response.autonomous_system_number}"  # e.g. "AS17557"
            result["isp"] = asn_response.autonomous_system_organization    # e.g. "PTCL"
    except (geoip2.errors.AddressNotFoundError, Exception):
        pass  # asn stays None — caller should handle gracefully

    return result


@router.get("/", tags=["Root"])
async def root():
    return {
        "name": "VPN Load Balancer API",
        "version": "3.0.0",
        "description": "Intelligent VPN Load Balancer with OpenVPN and Shadowsocks support",
        "protocols": ["OpenVPN", "Shadowsocks"],
        "docs": "/docs",
    }


@router.post("/v2/best_server/", response_model=BestServerDecision, tags=["Decision Engine"])
async def best_server_v2(
    app_name: str = Query(..., description="Application name"),
    server_type: Optional[str] = Query(None, pattern="^(free|premium)$"),
    country: Optional[str] = Query(None, description="User's country code (e.g., US, CN)"),
    asn: Optional[str] = Query(None, description="User's ISP ASN (e.g., AS15169)"),
    network_type: Optional[str] = Query(None, description="wifi or mobile"),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key)
):
    """
    Intelligent server + protocol selection.

    Returns the best server with primary and fallback protocol configurations.
    Both primary and fallback share the same server_id (unified row).

    Client should:
    1. Try connecting with primary protocol
    2. If fails, try fallback protocol (same server_id)
    3. If both fail, call /v2/connection_feedback/ and request new server
    """
    cache_key = (
        f"best_server_v2:app={app_name}:type={server_type or 'all'}"
        f":country={country or 'all'}:asn={asn or 'all'}:net={network_type or 'all'}"
    )
    cached = await get_cache(cache_key)
    if cached:
        return cached

    engine = DecisionEngine(db)
    try:
        decision = await engine.get_best_server(
            app_name=app_name,
            user_country=country,
            user_asn=asn,
            network_type=network_type,
            server_type=server_type
        )
        await set_cache(cache_key, decision.model_dump(), ttl=3)
        return decision
    except ValueError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.post("/v2/connection_feedback/", tags=["Decision Engine"])
async def connection_feedback(
    feedback: ConnectionFeedback,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key)
):
    """
    Connection feedback endpoint. Call once per connection cycle.

    Scenarios:
      • Primary succeeded:
          primary_protocol="openvpn", primary_success=true
          secondary_protocol=null

      • Primary failed, secondary succeeded:
          primary_protocol="openvpn",      primary_success=false
          secondary_protocol="shadowsocks", secondary_success=true

      • Both failed (triggers cooldown):
          primary_protocol="openvpn",      primary_success=false
          secondary_protocol="shadowsocks", secondary_success=false

    server_id is the single unified row id (same for both protocols).
    user_id is required only when Shadowsocks is the successful protocol.
    """
    result = await db.execute(select(VPNServer).where(VPNServer.id == feedback.server_id))
    server = result.scalar_one_or_none()
    if not server:
        raise HTTPException(status_code=404, detail="Server not found")

    engine = DecisionEngine(db)
    await engine.process_connection_feedback(
        server_id               = feedback.server_id,
        server_ip               = server.ip_address,
        app_name                = server.app_name,
        country                 = feedback.country,
        asn                     = feedback.asn,
        network_type            = feedback.network_type,
        primary_protocol        = feedback.primary_protocol,
        primary_success         = feedback.primary_success,
        primary_connect_time_ms = feedback.primary_connect_time_ms,
        secondary_protocol      = feedback.secondary_protocol,
        secondary_success       = feedback.secondary_success,
        secondary_connect_time_ms = feedback.secondary_connect_time_ms,
    )

    # Track Shadowsocks session when shadowsocks was the successful protocol.
    # OpenVPN sessions are managed by the Celery monitoring task.
    successful_protocol = None
    if feedback.primary_success:
        successful_protocol = feedback.primary_protocol
    elif feedback.secondary_success:
        successful_protocol = feedback.secondary_protocol

    if successful_protocol == 'shadowsocks' and feedback.user_id:
        existing_session = await db.execute(
            select(VPNUserSession).where(
                and_(
                    VPNUserSession.server_id == feedback.server_id,
                    VPNUserSession.user_id   == feedback.user_id
                )
            )
        )
        existing = existing_session.scalar_one_or_none()
        if not existing:
            db.add(VPNUserSession(
                server_id = feedback.server_id,
                user_id   = feedback.user_id,
                device_ip = "0.0.0.0",
                protocol  = 'shadowsocks'
            ))
        else:
            existing.connected_time = datetime.utcnow()
        await db.commit()

    await delete_cache("best_server_v2:*")

    both_failed = (
        not feedback.primary_success
        and feedback.secondary_protocol is not None
        and feedback.secondary_success is False
    )

    return {
        "message":            "Feedback processed successfully",
        "server_id":          feedback.server_id,
        "primary":            {"protocol": feedback.primary_protocol,  "result": "success" if feedback.primary_success else "failure"},
        "secondary":          {"protocol": feedback.secondary_protocol, "result": ("success" if feedback.secondary_success else "failure") if feedback.secondary_protocol else None},
        "cooldown_triggered": both_failed,
    }


@router.post("/v2/shadowsocks/disconnect/", tags=["Decision Engine"])
async def shadowsocks_disconnect(
    body: ShadowsocksDisconnect,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key)
):
    """
    Call when a user disconnects from Shadowsocks.
    Removes the active session so the user is no longer counted toward capacity.
    OpenVPN sessions are managed by the background monitoring task.
    """
    result = await db.execute(select(VPNServer).where(VPNServer.id == body.server_id))
    server = result.scalar_one_or_none()
    if not server:
        raise HTTPException(status_code=404, detail="Server not found")

    session_result = await db.execute(
        select(VPNUserSession).where(
            and_(
                VPNUserSession.server_id == body.server_id,
                VPNUserSession.user_id   == body.user_id,
                VPNUserSession.protocol  == 'shadowsocks',
            )
        )
    )
    session = session_result.scalar_one_or_none()

    if session:
        await db.delete(session)
        await db.commit()
        await delete_cache("best_server_v2:*")
        return {
            "message":   "Disconnected successfully",
            "user_id":   body.user_id,
            "server_id": body.server_id,
        }

    return {
        "message":   "No active session found — already disconnected",
        "user_id":   body.user_id,
        "server_id": body.server_id,
    }


@router.get("/best_server/", response_model=BestServerResponse, tags=["Public API - Legacy"])
async def best_server_legacy(
    server_type: Optional[str] = Query(None, pattern="^(free|premium)$"),
    app_name: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key)
):
    """
    Legacy endpoint — OpenVPN config only.
    Use /v2/best_server/ for full multi-protocol support.
    """
    cache_key = f"best_server:app={app_name or 'all'}:type={server_type or 'all'}"
    cached = await get_cache(cache_key)
    if cached:
        return cached

    conditions = [VPNServer.is_active == True]
    if server_type:
        conditions.append(VPNServer.server_type == server_type)
    if app_name:
        conditions.append(VPNServer.app_name == app_name)

    query = (
        select(VPNServer, func.count(VPNUserSession.id).label('session_count'))
        .outerjoin(VPNUserSession)
        .where(and_(*conditions))
        .group_by(VPNServer.id)
    )

    result = await db.execute(query)
    servers_data = result.all()

    if not servers_data:
        raise HTTPException(status_code=503, detail="No active servers available")

    priority_servers = [(s, count) for s, count in servers_data if s.is_priority_group and count < s.max_capacity]

    if priority_servers:
        priority_servers.sort(key=lambda x: x[0].load_score)
        min_score = priority_servers[0][0].load_score
        candidates = [s for s in priority_servers if s[0].load_score <= min_score + 5]
        selected, session_count = random.choice(candidates[:3]) if len(candidates) > 1 else priority_servers[0]
    else:
        regular_servers = [(s, count) for s, count in servers_data if not s.is_priority_group and count < s.max_capacity]
        if not regular_servers:
            raise HTTPException(status_code=503, detail="All servers at full capacity")
        regular_servers.sort(key=lambda x: x[0].load_score)
        min_score = regular_servers[0][0].load_score
        candidates = [s for s in regular_servers if s[0].load_score <= min_score + 5]
        selected, session_count = random.choice(candidates[:3]) if len(candidates) > 1 else regular_servers[0]

    response_data = {
        "app_name":     selected.app_name,
        "server":       selected.name,
        "ip_address":   selected.ip_address,
        "max_capacity": selected.max_capacity,
        "current_users": session_count,
        "load_score":   round(selected.load_score, 2),
        "cpu_usage":    round(selected.cpu_usage, 2),
        "ram_usage":    round(selected.ram_usage, 2),
        "ping_ms":      round(selected.ping_latency_ms, 2),
        "server_type":  selected.server_type,
        "server_city":  selected.server_city,
        "flag_image_url": selected.flag_image_url,
        "ovpn_base64":  selected.ovpn_base64,
    }

    await set_cache(cache_key, response_data, ttl=3)
    return response_data


@router.get("/servers_load/", response_model=ServersLoadResponse, tags=["Public API"])
async def servers_load(
    server_type: Optional[str] = Query(None, pattern="^(free|premium)$"),
    app_name: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Get load information for all servers."""
    cache_key = f"servers_load:app={app_name or 'all'}:type={server_type or 'all'}"
    cached = await get_cache(cache_key)
    if cached:
        return cached

    conditions = []
    if server_type:
        conditions.append(VPNServer.server_type == server_type)
    if app_name:
        conditions.append(VPNServer.app_name == app_name)

    query = (
        select(
            VPNServer,
            func.count(VPNUserSession.id).label('session_count'),
            func.coalesce(func.sum(VPNUserSession.bytes_received), 0).label('total_received'),
            func.coalesce(func.sum(VPNUserSession.bytes_sent), 0).label('total_sent')
        )
        .outerjoin(VPNUserSession)
        .group_by(VPNServer.id)
    )
    if conditions:
        query = query.where(and_(*conditions))

    result = await db.execute(query)
    servers_data = result.all()

    server_list = []
    total_capacity = 0
    total_connected = 0
    total_bytes_received_sum = 0
    total_bytes_sent_sum = 0
    active_count = 0

    for server, session_count, total_received, total_sent in servers_data:
        if server.is_active:
            connected = session_count
            total_capacity += server.max_capacity
            total_connected += connected
            total_bytes_received_sum += total_received
            total_bytes_sent_sum += total_sent
            active_count += 1
            load_pct = round((connected / server.max_capacity) * 100, 2) if server.max_capacity > 0 else 0
        else:
            connected = "DOWN"
            load_pct = "DOWN"
            total_received = 0
            total_sent = 0

        server_list.append(ServerLoadItem(
            server=server.name,
            app_name=server.app_name,
            ip_address=server.ip_address,
            max_capacity=server.max_capacity,
            server_type=server.server_type,
            server_city=server.server_city,
            connected_users=connected,
            load_percentage=load_pct,
            load_score=round(server.load_score, 2),
            cpu_usage=round(server.cpu_usage, 2),
            ram_usage=round(server.ram_usage, 2),
            ping_ms=round(server.ping_latency_ms, 2),
            last_health_check=server.last_health_check,
            total_bytes_received=total_received,
            total_bytes_sent=total_sent,
        ))

    overall_load = round((total_connected / total_capacity) * 100, 2) if total_capacity > 0 else 0

    response_data = ServersLoadResponse(
        servers_load=server_list,
        summary=LoadSummary(
            total_servers=len(servers_data),
            active_servers=active_count,
            total_capacity=total_capacity,
            total_connected_users=total_connected,
            overall_load_percentage=overall_load,
            total_bytes_received=total_bytes_received_sum,
            total_bytes_sent=total_bytes_sent_sum
        )
    )

    await set_cache(cache_key, response_data.model_dump(), ttl=3)
    return response_data


@router.get("/all_users/", response_model=AllUsersResponse, tags=["Public API"])
async def all_users(
    server_type: Optional[str] = Query(None, pattern="^(free|premium)$"),
    app_name: Optional[str] = None,
    config_tag: Optional[str] = None,
    protocol: Optional[str] = Query(None, pattern="^(openvpn|shadowsocks)$"),
    search: Optional[str] = Query(None, description="Search by user ID, device IP, server name, or server IP"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=1000),
    db: AsyncSession = Depends(get_db)
):
    """Get paginated active user sessions with optional filtering and search."""

    # Build shared filter conditions -- reused for COUNT and data query
    conditions = []
    needs_server_join = False

    if server_type:
        conditions.append(VPNServer.server_type == server_type)
        needs_server_join = True
    if app_name:
        conditions.append(VPNServer.app_name == app_name)
        needs_server_join = True
    if config_tag:
        conditions.append(VPNUserSession.config_tag == config_tag)
    if protocol:
        conditions.append(VPNUserSession.protocol == protocol)
    if search:
        pat = f"%{search}%"
        # user_id and device_ip are on VPNUserSession (no join needed for them alone)
        # server name and server ip_address are on VPNServer (join required)
        conditions.append(
            VPNUserSession.user_id.ilike(pat)
            | VPNUserSession.device_ip.ilike(pat)
            | VPNServer.name.ilike(pat)
            | VPNServer.ip_address.ilike(pat)
        )
        needs_server_join = True

    # Fast COUNT(*) -- cheap, no row fetching
    count_query = select(func.count()).select_from(VPNUserSession)
    if needs_server_join:
        count_query = count_query.join(VPNServer)
    if conditions:
        count_query = count_query.where(and_(*conditions))
    total = (await db.execute(count_query)).scalar() or 0

    # Paginated data query -- only fetches `limit` rows
    data_query = (
        select(VPNUserSession)
        .options(selectinload(VPNUserSession.server))
        .order_by(VPNUserSession.connected_time.desc())
        .offset(skip)
        .limit(limit)
    )
    if needs_server_join:
        data_query = data_query.join(VPNServer)
    if conditions:
        data_query = data_query.where(and_(*conditions))

    result = await db.execute(data_query)
    sessions = result.scalars().all()

    users = [
        UserSession(
            user_id=s.user_id,
            device_ip=s.device_ip,
            bytes_received=s.bytes_received,
            bytes_sent=s.bytes_sent,
            connected_time=s.connected_time,
            server_name=s.server.name,
            server_ip=s.server.ip_address,
            server_type=s.server.server_type,
            app_name=s.server.app_name,
            config_tag=s.config_tag,
            protocol=s.protocol
        )
        for s in sessions
    ]

    return AllUsersResponse(users=users, total=total, skip=skip, limit=limit)


@router.get("/servers_config/", tags=["Public API"])
async def servers_config(
    app_name: str = Query(..., description="App name (required)"),
    server_type: Optional[str] = Query(None, pattern="^(free|premium)$"),
    country: Optional[str] = Query(None, description="User's country code"),
    asn: Optional[str] = Query(None, description="User's ISP ASN"),
    network_type: Optional[str] = Query(None, description="wifi or mobile"),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key)
):
    """
    Get server configurations for mobile apps.
    app_name is required — only servers belonging to that app are returned.

    When country/asn/network_type are provided (decision mode):
      Returns ServerConfigDecision list — each server scored for primary/fallback protocol.

    When omitted (legacy mode):
      Returns flat ServerConfig list.
    """
    use_decision_mode = any([country, asn, network_type])

    if use_decision_mode:
        cache_key = (
            f"servers_config_decision:app={app_name or 'all'}"
            f":type={server_type or 'all'}"
            f":country={country or 'all'}:asn={asn or 'all'}:net={network_type or 'all'}"
        )
        cached = await get_cache(cache_key)
        if cached:
            return cached

        conditions = [VPNServer.is_active == True]
        if app_name:
            conditions.append(VPNServer.app_name == app_name)
        if server_type:
            conditions.append(VPNServer.server_type == server_type)

        # Each active server row is already one physical server
        servers_result = await db.execute(
            select(VPNServer)
            .where(and_(*conditions))
            .order_by(VPNServer.ip_address)
        )
        active_servers = servers_result.scalars().all()

        engine = DecisionEngine(db)
        decisions = []

        for srv in active_servers:
            decision = await engine.get_protocol_decision_for_server(
                ip_address   = srv.ip_address,
                server_type  = srv.server_type,
                app_name     = app_name,
                user_country = country,
                user_asn     = asn,
                network_type = network_type,
            )
            if decision is None:
                continue

            decisions.append(ServerConfigDecision(
                app_name          = decision.app_name,
                server            = decision.primary_config.server_name,
                ip_address        = srv.ip_address,
                is_active         = True,
                server_type       = decision.server_type,
                server_city       = decision.server_city,
                flag_image_url    = decision.flag_image_url,
                primary_protocol  = decision.primary_protocol,
                primary_config    = decision.primary_config,
                primary_score     = decision.primary_score,
                fallback_protocol = decision.fallback_protocol,
                fallback_config   = decision.fallback_config,
                fallback_score    = decision.fallback_score,
            ).model_dump())

        await set_cache(cache_key, decisions, ttl=3)
        return decisions

    else:
        cache_key = f"servers_config:app={app_name or 'all'}:type={server_type or 'all'}"
        cached = await get_cache(cache_key)
        if cached:
            return cached

        query = select(VPNServer).order_by(VPNServer.name)
        conditions = []
        if app_name:
            conditions.append(VPNServer.app_name == app_name)
        if server_type:
            conditions.append(VPNServer.server_type == server_type)
        if conditions:
            query = query.where(and_(*conditions))

        result = await db.execute(query)
        servers = result.scalars().all()

        configs = [
            ServerConfig(
                app_name       = s.app_name,
                server         = s.name,
                ip_address     = s.ip_address,
                is_active      = s.is_active,
                server_type    = s.server_type,
                server_city    = s.server_city,
                flag_image_url = s.flag_image_url,
                ovpn_base64    = s.ovpn_base64,
            )
            for s in servers
        ]

        await set_cache(cache_key, [c.model_dump() for c in configs], ttl=3)
        return configs