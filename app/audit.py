"""
Audit logging utility.

Usage in any admin endpoint:

    from app.audit import audit_log

    await audit_log(db, token,
        action="server.create",
        resource_type="server",
        resource_id=str(server.id),
        app_name=server.app_name,
        details={"name": server.name, "ip": server.ip_address},
    )

`token` is the raw Bearer string returned by verify_api_key.
If it's a JWT, user info is extracted; if it's the static API key, user shows as "api_key".
"""

import json
from typing import Optional, Any
from sqlalchemy.ext.asyncio import AsyncSession
from app.models import AuditLog


def _extract_user_from_token(token: str) -> dict:
    """Try to decode JWT for user info. Returns fallback for API key."""
    try:
        from app.api.admin_users import decode_access_token
        payload = decode_access_token(token)
        return {
            "user_id": int(payload.get("sub", 0)),
            "user_email": payload.get("email", "unknown"),
            "user_role": payload.get("role", "unknown"),
        }
    except Exception:
        return {"user_id": None, "user_email": "api_key", "user_role": "system"}


async def audit_log(
    db: AsyncSession,
    token: str,
    *,
    action: str,
    resource_type: str,
    resource_id: Optional[str] = None,
    app_name: Optional[str] = None,
    details: Optional[Any] = None,
):
    """Write one audit log row. Always commits independently."""
    user_info = _extract_user_from_token(token)
    entry = AuditLog(
        user_id=user_info["user_id"],
        user_email=user_info["user_email"],
        user_role=user_info["user_role"],
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        app_name=app_name,
        details=json.dumps(details, default=str) if details else None,
    )
    db.add(entry)
    # Don't commit here — let the endpoint's session commit handle it.
    # The get_db() generator commits on success.


async def audit_log_user(
    db: AsyncSession,
    user,
    *,
    action: str,
    resource_type: str,
    resource_id: Optional[str] = None,
    app_name: Optional[str] = None,
    details: Optional[Any] = None,
):
    """Write audit log using a DashboardUser object directly (for admin_users endpoints)."""
    entry = AuditLog(
        user_id=user.id,
        user_email=user.email,
        user_role=user.role,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        app_name=app_name,
        details=json.dumps(details, default=str) if details else None,
    )
    db.add(entry)