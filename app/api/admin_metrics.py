"""
Admin API - Protocol Metrics & Policies Management
View and manage protocol performance metrics, country policies, and ISP policies
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, delete, update
from typing import Optional, List
from datetime import datetime

from app.database import get_db
from app.models import VPNServer, ProtocolMetrics, CountryPolicy, ISPPolicy
from app.schemas import (
    ProtocolMetricsResponse, CountryPolicyCreate, CountryPolicyResponse,
    ISPPolicyCreate, ISPPolicyResponse
)
from app.auth import verify_api_key
from app.audit import audit_log

router = APIRouter(prefix="/admin", tags=["Admin - Metrics & Policies"])


# ==================== Protocol Metrics ====================

@router.get("/metrics/protocols")
async def get_protocol_metrics(
    app_name: Optional[str] = None,
    protocol: Optional[str] = Query(None, pattern="^(openvpn|shadowsocks)$"),
    country: Optional[str] = None,
    asn: Optional[str] = None,
    min_attempts: int = Query(5, ge=1),
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key)
):
    """
    Get protocol performance metrics
    
    Shows success rates, connection times, failures, and cooldowns
    for each protocol across different contexts (country, ISP).
    
    Filter by:
    - app_name: Specific app
    - protocol: openvpn or shadowsocks
    - country: User country
    - asn: ISP identifier
    - min_attempts: Minimum attempts to show (default 5)
    """
    
    conditions = [ProtocolMetrics.total_attempts >= min_attempts]
    
    if app_name:
        conditions.append(ProtocolMetrics.app_name == app_name)
    if protocol:
        conditions.append(ProtocolMetrics.protocol == protocol)
    if country:
        conditions.append(ProtocolMetrics.country == country)
    if asn:
        conditions.append(ProtocolMetrics.asn == asn)
    
    query = (
        select(ProtocolMetrics, VPNServer.name)
        .join(VPNServer)
        .where(and_(*conditions))
        .order_by(ProtocolMetrics.success_rate.desc())
    )
    
    result = await db.execute(query)
    metrics_data = result.all()
    
    metrics_list = []
    for metrics, server_name in metrics_data:
        metrics_list.append(ProtocolMetricsResponse(
            server_id=metrics.server_id,
            server_name=server_name,
            protocol=metrics.protocol,
            country=metrics.country,
            asn=metrics.asn,
            success_count=metrics.success_count,
            failure_count=metrics.failure_count,
            total_attempts=metrics.total_attempts,
            success_rate=round(metrics.success_rate * 100, 2),  # Convert to percentage
            avg_connect_time_ms=round(metrics.avg_connect_time_ms, 2),
            consecutive_failures=metrics.consecutive_failures,
            cooldown_until=metrics.cooldown_until,
            cooldown_level=metrics.cooldown_level,
            last_success_at=metrics.last_success_at,
            last_failure_time=metrics.last_failure_time,
            last_failure_reason=metrics.last_failure_reason
        ))
    
    return {
        "total": len(metrics_list),
        "metrics": metrics_list
    }


@router.get("/metrics/summary")
async def get_metrics_summary(
    app_name: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key)
):
    """
    Get summary statistics for protocol performance
    
    Shows aggregate success rates, average connection times,
    and protocol distribution across the system.
    """
    
    conditions = []
    if app_name:
        conditions.append(ProtocolMetrics.app_name == app_name)
    
    # Overall stats
    query = select(
        ProtocolMetrics.protocol,
        func.sum(ProtocolMetrics.success_count).label('total_success'),
        func.sum(ProtocolMetrics.failure_count).label('total_failures'),
        func.sum(ProtocolMetrics.total_attempts).label('total_attempts'),
        func.avg(ProtocolMetrics.avg_connect_time_ms).label('avg_time')
    )
    
    if conditions:
        query = query.where(and_(*conditions))
    
    query = query.group_by(ProtocolMetrics.protocol)
    
    result = await db.execute(query)
    protocol_stats = result.all()
    
    summary = []
    for proto, success, failures, attempts, avg_time in protocol_stats:
        success_rate = (success / attempts * 100) if attempts > 0 else 0
        summary.append({
            "protocol": proto,
            "total_attempts": attempts,
            "success_count": success,
            "failure_count": failures,
            "success_rate": round(success_rate, 2),
            "avg_connect_time_ms": round(avg_time, 2) if avg_time else 0
        })
    
    # Count active servers (each row supports both protocols)
    server_query = select(func.count(VPNServer.id)).where(VPNServer.is_active == True)
    if app_name:
        server_query = server_query.where(VPNServer.app_name == app_name)
    server_result = await db.execute(server_query)
    active_server_count = server_result.scalar() or 0
    server_counts = {"openvpn": active_server_count, "shadowsocks": active_server_count}
    
    # Count metrics in cooldown
    cooldown_query = select(
        ProtocolMetrics.protocol,
        func.count(ProtocolMetrics.id).label('cooldown_count')
    ).where(ProtocolMetrics.cooldown_until > datetime.utcnow())
    
    if app_name:
        cooldown_query = cooldown_query.where(ProtocolMetrics.app_name == app_name)
    
    cooldown_query = cooldown_query.group_by(ProtocolMetrics.protocol)
    
    cooldown_result = await db.execute(cooldown_query)
    cooldown_counts = {proto: count for proto, count in cooldown_result.all()}
    
    return {
        "protocol_stats": summary,
        "active_servers": server_counts,
        "cooled_down_metrics": cooldown_counts
    }


@router.delete("/metrics/{metric_id}")
async def delete_metric(
    metric_id: int,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key)
):
    """Delete a specific protocol metric entry"""
    
    result = await db.execute(
        select(ProtocolMetrics).where(ProtocolMetrics.id == metric_id)
    )
    metric = result.scalar_one_or_none()
    
    if not metric:
        raise HTTPException(status_code=404, detail="Metric not found")
    
    await db.delete(metric)
    await db.commit()
    
    await audit_log(db, token, action="metric.delete", resource_type="metric",
        resource_id=str(metric_id), app_name=metric.app_name,
        details={"server_id": metric.server_id, "protocol": metric.protocol})
    
    return {"message": "Metric deleted successfully", "metric_id": metric_id}


@router.post("/metrics/{metric_id}/reset-cooldown")
async def reset_cooldown(
    metric_id: int,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key)
):
    """Manually reset cooldown for a protocol metric"""
    
    result = await db.execute(
        select(ProtocolMetrics).where(ProtocolMetrics.id == metric_id)
    )
    metric = result.scalar_one_or_none()
    
    if not metric:
        raise HTTPException(status_code=404, detail="Metric not found")
    
    metric.cooldown_until = None
    metric.cooldown_level = 'none'
    metric.consecutive_failures = 0
    
    await db.commit()
    
    await audit_log(db, token, action="metric.reset_cooldown", resource_type="metric",
        resource_id=str(metric_id), app_name=metric.app_name,
        details={"server_id": metric.server_id, "protocol": metric.protocol})
    
    return {
        "message": "Cooldown reset successfully",
        "metric_id": metric_id,
        "server_id": metric.server_id,
        "protocol": metric.protocol
    }


# ==================== Country Policies ====================

@router.get("/policies/countries", response_model=List[CountryPolicyResponse])
async def list_country_policies(
    app_name: Optional[str] = None,
    country: Optional[str] = None,
    is_active: Optional[bool] = None,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key)
):
    """
    List country-level protocol policies
    
    These policies provide hints/biases to the decision engine
    about which protocols work better in specific countries.
    """
    
    # Country policies are global — app_name filter is intentionally ignored
    conditions = []
    if country:
        conditions.append(CountryPolicy.country == country)
    if is_active is not None:
        conditions.append(CountryPolicy.is_active == is_active)
    
    query = select(CountryPolicy)
    if conditions:
        query = query.where(and_(*conditions))
    
    query = query.order_by(CountryPolicy.country)
    
    result = await db.execute(query)
    policies = result.scalars().all()
    
    return [
        CountryPolicyResponse(
            id=p.id,
            app_name=p.app_name,
            country=p.country,
            preferred_protocol=p.preferred_protocol,
            fallback_protocol=p.fallback_protocol,
            protocol_bias_score=p.protocol_bias_score,
            is_active=p.is_active,
            notes=p.notes,
            created_at=p.created_at,
            updated_at=p.updated_at
        )
        for p in policies
    ]


@router.post("/policies/countries", status_code=201)
async def create_country_policy(
    policy: CountryPolicyCreate,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key)
):
    """
    Create a global country-level protocol policy.
    
    Country policies are global — they apply across all apps and servers.
    Only one policy per country is allowed.
    
    Example:
    {
        "country": "CN",
        "preferred_protocol": "shadowsocks",
        "fallback_protocol": "openvpn",
        "protocol_bias_score": 20.0,
        "notes": "China blocks OpenVPN aggressively"
    }
    """
    
    # Global uniqueness: one policy per country
    existing = await db.execute(
        select(CountryPolicy).where(
            CountryPolicy.country == policy.country
        )
    )
    
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=400,
            detail=f"A global policy for country '{policy.country}' already exists"
        )
    
    new_policy = CountryPolicy(
        app_name=None,
        country=policy.country,
        preferred_protocol=policy.preferred_protocol,
        fallback_protocol=policy.fallback_protocol,
        protocol_bias_score=policy.protocol_bias_score,
        notes=policy.notes,
        is_active=True
    )
    
    db.add(new_policy)
    await db.commit()
    await db.refresh(new_policy)
    
    await audit_log(db, token, action="country_policy.create", resource_type="policy",
        resource_id=str(new_policy.id), app_name=policy.app_name,
        details={"country": policy.country, "preferred": policy.preferred_protocol, "fallback": policy.fallback_protocol})
    
    return {
        "message": "Country policy created successfully",
        "policy_id": new_policy.id,
        "country": new_policy.country
    }


@router.put("/policies/countries/{policy_id}")
async def update_country_policy(
    policy_id: int,
    preferred_protocol: Optional[str] = None,
    fallback_protocol: Optional[str] = None,
    protocol_bias_score: Optional[float] = None,
    is_active: Optional[bool] = None,
    notes: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key)
):
    """Update an existing country policy"""
    
    result = await db.execute(
        select(CountryPolicy).where(CountryPolicy.id == policy_id)
    )
    policy = result.scalar_one_or_none()
    
    if not policy:
        raise HTTPException(status_code=404, detail="Country policy not found")
    
    if preferred_protocol is not None:
        policy.preferred_protocol = preferred_protocol
    if fallback_protocol is not None:
        policy.fallback_protocol = fallback_protocol
    if protocol_bias_score is not None:
        policy.protocol_bias_score = protocol_bias_score
    if is_active is not None:
        policy.is_active = is_active
    if notes is not None:
        policy.notes = notes
    
    await db.commit()
    
    await audit_log(db, token, action="country_policy.update", resource_type="policy",
        resource_id=str(policy_id), app_name=policy.app_name,
        details={"country": policy.country})
    
    return {
        "message": "Country policy updated successfully",
        "policy_id": policy_id
    }


@router.delete("/policies/countries/{policy_id}")
async def delete_country_policy(
    policy_id: int,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key)
):
    """Delete a country policy"""
    
    result = await db.execute(
        select(CountryPolicy).where(CountryPolicy.id == policy_id)
    )
    policy = result.scalar_one_or_none()
    
    if not policy:
        raise HTTPException(status_code=404, detail="Country policy not found")
    
    country = policy.country
    app_nm = policy.app_name
    await db.delete(policy)
    await db.commit()
    
    await audit_log(db, token, action="country_policy.delete", resource_type="policy",
        resource_id=str(policy_id), app_name=app_nm, details={"country": country})
    
    return {
        "message": f"Country policy for {country} deleted successfully"
    }


# ==================== ISP Policies ====================

@router.get("/policies/isps", response_model=List[ISPPolicyResponse])
async def list_isp_policies(
    app_name: Optional[str] = None,
    country: Optional[str] = None,
    asn: Optional[str] = None,
    protocol: Optional[str] = Query(None, pattern="^(openvpn|shadowsocks)$"),
    status: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key)
):
    """
    List ISP-level protocol policies
    
    These track which protocols work well or poorly on specific ISPs.
    Status can be: preferred, degraded, blocked
    """
    
    # ISP policies are global — app_name filter is intentionally ignored
    conditions = []
    if country:
        conditions.append(ISPPolicy.country == country)
    if asn:
        conditions.append(ISPPolicy.asn == asn)
    if protocol:
        conditions.append(ISPPolicy.protocol == protocol)
    if status:
        conditions.append(ISPPolicy.status == status)
    
    query = select(ISPPolicy)
    if conditions:
        query = query.where(and_(*conditions))
    
    query = query.order_by(ISPPolicy.country, ISPPolicy.asn)
    
    result = await db.execute(query)
    policies = result.scalars().all()
    
    return [
        ISPPolicyResponse(
            id=p.id,
            app_name=p.app_name,
            country=p.country,
            asn=p.asn,
            protocol=p.protocol,
            status=p.status,
            bias_score=p.bias_score,
            expiry=p.expiry,
            notes=p.notes,
            created_at=p.created_at
        )
        for p in policies
    ]


@router.post("/policies/isps", status_code=201)
async def create_isp_policy(
    policy: ISPPolicyCreate,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key)
):
    """
    Create a global ISP-level protocol policy.
    
    ISP policies are global — they apply across all apps and servers.
    
    Example:
    {
        "country": "US",
        "asn": "AS15169",
        "protocol": "shadowsocks",
        "status": "preferred",
        "bias_score": 15.0,
        "notes": "Google Fiber works great with Shadowsocks"
    }
    """
    
    # Global uniqueness: one policy per country+asn+protocol
    existing = await db.execute(
        select(ISPPolicy).where(
            and_(
                ISPPolicy.country  == policy.country,
                ISPPolicy.asn      == policy.asn,
                ISPPolicy.protocol == policy.protocol
            )
        )
    )
    
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=400,
            detail=f"A global ISP policy for {policy.asn} with {policy.protocol} already exists"
        )
    
    new_policy = ISPPolicy(
        app_name=None,
        country=policy.country,
        asn=policy.asn,
        protocol=policy.protocol,
        status=policy.status,
        bias_score=policy.bias_score,
        expiry=policy.expiry,
        notes=policy.notes
    )
    
    db.add(new_policy)
    await db.commit()
    await db.refresh(new_policy)
    
    await audit_log(db, token, action="isp_policy.create", resource_type="policy",
        resource_id=str(new_policy.id), app_name=policy.app_name,
        details={"asn": policy.asn, "protocol": policy.protocol, "status": policy.status, "country": policy.country})
    
    return {
        "message": "ISP policy created successfully",
        "policy_id": new_policy.id,
        "asn": new_policy.asn,
        "protocol": new_policy.protocol
    }


@router.put("/policies/isps/{policy_id}")
async def update_isp_policy(
    policy_id: int,
    status: Optional[str] = None,
    bias_score: Optional[float] = None,
    expiry: Optional[datetime] = None,
    notes: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key)
):
    """Update an existing ISP policy"""
    
    result = await db.execute(
        select(ISPPolicy).where(ISPPolicy.id == policy_id)
    )
    policy = result.scalar_one_or_none()
    
    if not policy:
        raise HTTPException(status_code=404, detail="ISP policy not found")
    
    if status is not None:
        policy.status = status
    if bias_score is not None:
        policy.bias_score = bias_score
    if expiry is not None:
        policy.expiry = expiry
    if notes is not None:
        policy.notes = notes
    
    await db.commit()
    
    await audit_log(db, token, action="isp_policy.update", resource_type="policy",
        resource_id=str(policy_id), app_name=policy.app_name,
        details={"asn": policy.asn, "protocol": policy.protocol})
    
    return {
        "message": "ISP policy updated successfully",
        "policy_id": policy_id
    }


@router.delete("/policies/isps/{policy_id}")
async def delete_isp_policy(
    policy_id: int,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key)
):
    """Delete an ISP policy"""
    
    result = await db.execute(
        select(ISPPolicy).where(ISPPolicy.id == policy_id)
    )
    policy = result.scalar_one_or_none()
    
    if not policy:
        raise HTTPException(status_code=404, detail="ISP policy not found")
    
    asn = policy.asn
    protocol = policy.protocol
    app_nm = policy.app_name
    await db.delete(policy)
    await db.commit()
    
    await audit_log(db, token, action="isp_policy.delete", resource_type="policy",
        resource_id=str(policy_id), app_name=app_nm, details={"asn": asn, "protocol": protocol})
    
    return {
        "message": f"ISP policy for {asn} ({protocol}) deleted successfully"
    }