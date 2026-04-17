"""
Admin API – Audit Logs
Read-only endpoint that serves the audit trail to the dashboard.
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, desc
from typing import Optional
import json

from app.database import get_db
from app.models import AuditLog
from app.auth import verify_api_key

router = APIRouter(prefix="/admin/audit", tags=["Admin - Audit Logs"])


@router.get("/logs")
async def get_audit_logs(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    action: Optional[str] = None,
    resource_type: Optional[str] = None,
    user_email: Optional[str] = None,
    app_name: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key),
):
    """
    Paginated audit log with optional filters.
    Returns newest-first.
    """
    conditions = []
    if action:
        conditions.append(AuditLog.action == action)
    if resource_type:
        conditions.append(AuditLog.resource_type == resource_type)
    if user_email:
        conditions.append(AuditLog.user_email == user_email)
    if app_name:
        conditions.append(AuditLog.app_name == app_name)

    # Total count
    count_q = select(func.count()).select_from(AuditLog)
    if conditions:
        count_q = count_q.where(and_(*conditions))
    total = (await db.execute(count_q)).scalar() or 0

    # Fetch rows
    q = select(AuditLog).order_by(desc(AuditLog.timestamp))
    if conditions:
        q = q.where(and_(*conditions))
    q = q.offset(skip).limit(limit)

    result = await db.execute(q)
    logs = result.scalars().all()

    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "logs": [
            {
                "id": log.id,
                "user_email": log.user_email,
                "user_role": log.user_role,
                "action": log.action,
                "resource_type": log.resource_type,
                "resource_id": log.resource_id,
                "app_name": log.app_name,
                "details": json.loads(log.details) if log.details else None,
                "timestamp": log.timestamp.isoformat() if log.timestamp else None,
            }
            for log in logs
        ],
    }


@router.get("/logs/summary")
async def get_audit_summary(
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key),
):
    """Summary counts for the audit log dashboard cards."""
    total = (await db.execute(select(func.count()).select_from(AuditLog))).scalar() or 0

    server_actions = (await db.execute(
        select(func.count()).select_from(AuditLog).where(AuditLog.resource_type == "server")
    )).scalar() or 0

    settings_changes = (await db.execute(
        select(func.count()).select_from(AuditLog).where(AuditLog.resource_type == "settings")
    )).scalar() or 0

    policy_changes = (await db.execute(
        select(func.count()).select_from(AuditLog).where(AuditLog.resource_type == "policy")
    )).scalar() or 0

    app_actions = (await db.execute(
        select(func.count()).select_from(AuditLog).where(AuditLog.resource_type == "app")
    )).scalar() or 0

    user_actions = (await db.execute(
        select(func.count()).select_from(AuditLog).where(AuditLog.resource_type == "user")
    )).scalar() or 0

    # Distinct action types for filter dropdown
    actions_result = await db.execute(
        select(AuditLog.action).distinct().order_by(AuditLog.action)
    )
    action_types = [row[0] for row in actions_result.all()]

    return {
        "total": total,
        "server_actions": server_actions,
        "settings_changes": settings_changes,
        "policy_changes": policy_changes,
        "app_actions": app_actions,
        "user_actions": user_actions,
        "action_types": action_types,
    }