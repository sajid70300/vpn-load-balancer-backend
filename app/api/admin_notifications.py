"""
Admin API – Notifications
Serves system-generated notifications to all dashboard users.

Notifications are created automatically by Celery tasks (tasks.py) for:
  - server_down      : a server became unreachable and was marked inactive
  - capacity_reached : a server's session count reached its max_capacity

Notifications are global (not per-user). Any admin marking them read
clears the unread badge for everyone.
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update, desc
from typing import Optional

from app.database import get_db
from app.models import Notification
from app.auth import verify_api_key

router = APIRouter(prefix="/admin/notifications", tags=["Admin - Notifications"])


def _notif_out(n: Notification) -> dict:
    return {
        "id":          n.id,
        "type":        n.type,
        "server_id":   n.server_id,
        "server_name": n.server_name,
        "server_ip":   n.server_ip,
        "app_name":    n.app_name,
        "message":     n.message,
        "is_read":     n.is_read,
        "created_at":  n.created_at.isoformat() if n.created_at else None,
    }


@router.get("/")
async def list_notifications(
    unread_only: bool          = Query(False),
    limit:       int           = Query(50, ge=1, le=200),
    db:          AsyncSession  = Depends(get_db),
    token:       str           = Depends(verify_api_key),
):
    """
    Return recent notifications, newest first.
    Pass ?unread_only=true to get only unread ones (used for badge count).
    """
    q = select(Notification).order_by(desc(Notification.created_at)).limit(limit)
    if unread_only:
        q = q.where(Notification.is_read == False)

    result = await db.execute(q)
    notifications = result.scalars().all()

    # Unread count (always returned so the frontend can drive the badge)
    unread_count_result = await db.execute(
        select(func.count()).select_from(Notification).where(Notification.is_read == False)
    )
    unread_count = unread_count_result.scalar() or 0

    return {
        "unread_count":  unread_count,
        "notifications": [_notif_out(n) for n in notifications],
    }


@router.post("/mark-all-read")
async def mark_all_read(
    db:    AsyncSession = Depends(get_db),
    token: str          = Depends(verify_api_key),
):
    """
    Mark every unread notification as read.
    Called when the user opens the notification panel.
    """
    await db.execute(
        update(Notification)
        .where(Notification.is_read == False)
        .values(is_read=True)
    )
    await db.commit()
    return {"marked_read": True}


@router.post("/{notification_id}/mark-read")
async def mark_one_read(
    notification_id: int,
    db:              AsyncSession = Depends(get_db),
    token:           str          = Depends(verify_api_key),
):
    """Mark a single notification as read."""
    result = await db.execute(
        select(Notification).where(Notification.id == notification_id)
    )
    n = result.scalar_one_or_none()
    if not n:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Notification not found")
    n.is_read = True
    await db.commit()
    await db.refresh(n)
    return _notif_out(n)