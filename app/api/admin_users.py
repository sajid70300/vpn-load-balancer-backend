
"""
Admin API – User Management & Authentication
Handles register, login, JWT auth, approval flow, role management.

Roles:
  superadmin – first registered user, can manage everyone
  admin      – promoted by superadmin, can delete users
  user       – view-only, no write operations

Registration flow:
  1. User submits name + email + password
  2. If no users exist → auto-approved as superadmin
  3. Otherwise → status='pending', superadmin must approve
  4. Once approved (status='active'), user can login
"""

from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

import hashlib
import hmac
import os
import json
import base64

from app.database import get_db
from app.models import DashboardUser, UserRole, UserStatus
from app.schemas import (
    RegisterRequest, LoginRequest, LoginResponse,
    UserResponse, UserRoleUpdate,
)
from app.config import settings
from app.audit import audit_log_user

router = APIRouter(prefix="/auth", tags=["Authentication & Users"])


# ─── Password hashing (SHA-256 + salt, no extra dependency) ──────────────────

def hash_password(password: str) -> str:
    salt = os.urandom(16).hex()
    hashed = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100_000).hex()
    return f"{salt}${hashed}"


def verify_password(password: str, stored: str) -> bool:
    salt, hashed = stored.split("$", 1)
    check = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100_000).hex()
    return hmac.compare_digest(check, hashed)


# ─── JWT helpers (manual, no pyjwt dependency) ───────────────────────────────

def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _b64url_decode(s: str) -> bytes:
    s += "=" * (4 - len(s) % 4)
    return base64.urlsafe_b64decode(s)


def create_access_token(user_id: int, email: str, role: str) -> str:
    header = _b64url_encode(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    now = datetime.utcnow()
    payload_dict = {
        "sub": str(user_id),
        "email": email,
        "role": role,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)).timestamp()),
    }
    payload = _b64url_encode(json.dumps(payload_dict).encode())
    signing_input = f"{header}.{payload}".encode()
    signature = _b64url_encode(
        hmac.new(settings.SECRET_KEY.encode(), signing_input, hashlib.sha256).digest()
    )
    return f"{header}.{payload}.{signature}"


def decode_access_token(token: str) -> dict:
    """Decode and verify a JWT. Returns the payload dict or raises."""
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("Invalid token")
    header_b64, payload_b64, signature_b64 = parts
    signing_input = f"{header_b64}.{payload_b64}".encode()
    expected_sig = _b64url_encode(
        hmac.new(settings.SECRET_KEY.encode(), signing_input, hashlib.sha256).digest()
    )
    if not hmac.compare_digest(expected_sig, signature_b64):
        raise ValueError("Invalid signature")
    payload = json.loads(_b64url_decode(payload_b64))
    if datetime.utcnow().timestamp() > payload.get("exp", 0):
        raise ValueError("Token expired")
    return payload


# ─── Dependency: get current user from JWT Bearer token ──────────────────────

from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi import Security

_bearer = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Security(_bearer),
    db: AsyncSession = Depends(get_db),
) -> DashboardUser:
    """Decode JWT, fetch user from DB, reject if not active."""
    try:
        payload = decode_access_token(credentials.credentials)
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))

    result = await db.execute(
        select(DashboardUser).where(DashboardUser.id == int(payload["sub"]))
    )
    user = result.scalar_one_or_none()
    if not user or user.status != UserStatus.ACTIVE.value:
        raise HTTPException(status_code=401, detail="User not found or inactive")
    return user


def require_role(*roles: str):
    """Dependency factory: require the current user to have one of the given roles."""
    async def _check(user: DashboardUser = Depends(get_current_user)):
        if user.role not in roles:
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        return user
    return _check


# ─── Helper ──────────────────────────────────────────────────────────────────

def _user_response(u: DashboardUser) -> UserResponse:
    return UserResponse(
        id=u.id, name=u.name, email=u.email, role=u.role,
        status=u.status, created_at=u.created_at, last_login_at=u.last_login_at,
    )


# ─── Endpoints ───────────────────────────────────────────────────────────────

@router.post("/register", response_model=UserResponse, status_code=201)
async def register(payload: RegisterRequest, db: AsyncSession = Depends(get_db)):
    """
    Register a new dashboard user.
    - First ever user → superadmin + active (can login immediately)
    - All others → user + pending (needs superadmin approval)
    """
    # Check duplicate email
    existing = await db.execute(
        select(DashboardUser).where(DashboardUser.email == payload.email)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Email already registered")

    # Determine if this is the first user
    count_result = await db.execute(select(func.count()).select_from(DashboardUser))
    total_users = count_result.scalar() or 0

    if total_users == 0:
        role = UserRole.SUPERADMIN.value
        user_status = UserStatus.ACTIVE.value
    else:
        role = UserRole.USER.value
        user_status = UserStatus.PENDING.value

    new_user = DashboardUser(
        name=payload.name,
        email=payload.email,
        password_hash=hash_password(payload.password),
        role=role,
        status=user_status,
    )
    db.add(new_user)
    await db.commit()
    await db.refresh(new_user)

    return _user_response(new_user)


@router.post("/login", response_model=LoginResponse)
async def login(payload: LoginRequest, db: AsyncSession = Depends(get_db)):
    """
    Authenticate and return a JWT.
    Only active users can login. Pending/rejected users are refused.
    """
    result = await db.execute(
        select(DashboardUser).where(DashboardUser.email == payload.email)
    )
    user = result.scalar_one_or_none()

    if not user or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    if user.status == UserStatus.PENDING.value:
        raise HTTPException(status_code=403, detail="Your account is pending approval")
    if user.status == UserStatus.REJECTED.value:
        raise HTTPException(status_code=403, detail="Your registration was rejected")

    # Update last_login_at
    user.last_login_at = datetime.utcnow()
    await db.commit()

    token = create_access_token(user.id, user.email, user.role)
    return LoginResponse(access_token=token, user=_user_response(user))


@router.get("/me", response_model=UserResponse)
async def get_me(user: DashboardUser = Depends(get_current_user)):
    """Return the currently authenticated user's profile."""
    return _user_response(user)


# ─── User management (superadmin / admin) ────────────────────────────────────

@router.get("/users", response_model=list[UserResponse])
async def list_users(
    user: DashboardUser = Depends(require_role(UserRole.SUPERADMIN.value, UserRole.ADMIN.value)),
    db: AsyncSession = Depends(get_db),
):
    """List all users. Only superadmin and admin can see this."""
    result = await db.execute(select(DashboardUser).order_by(DashboardUser.created_at.asc()))
    return [_user_response(u) for u in result.scalars().all()]


@router.get("/users/pending", response_model=list[UserResponse])
async def list_pending_users(
    user: DashboardUser = Depends(require_role(UserRole.SUPERADMIN.value)),
    db: AsyncSession = Depends(get_db),
):
    """List pending registration requests. Superadmin only."""
    result = await db.execute(
        select(DashboardUser)
        .where(DashboardUser.status == UserStatus.PENDING.value)
        .order_by(DashboardUser.created_at.asc())
    )
    return [_user_response(u) for u in result.scalars().all()]


@router.put("/users/{user_id}/approve", response_model=UserResponse)
async def approve_user(
    user_id: int,
    user: DashboardUser = Depends(require_role(UserRole.SUPERADMIN.value)),
    db: AsyncSession = Depends(get_db),
):
    """Approve a pending registration. Superadmin only."""
    result = await db.execute(select(DashboardUser).where(DashboardUser.id == user_id))
    target = result.scalar_one_or_none()
    if not target:
        raise HTTPException(status_code=404, detail="User not found")
    if target.status != UserStatus.PENDING.value:
        raise HTTPException(status_code=400, detail="User is not in pending state")

    target.status = UserStatus.ACTIVE.value
    await db.commit()
    await db.refresh(target)

    await audit_log_user(db, user, action="user.approve", resource_type="user",
        resource_id=str(target.id), details={"name": target.name, "email": target.email})

    return _user_response(target)


@router.put("/users/{user_id}/reject", response_model=UserResponse)
async def reject_user(
    user_id: int,
    user: DashboardUser = Depends(require_role(UserRole.SUPERADMIN.value)),
    db: AsyncSession = Depends(get_db),
):
    """Reject a pending registration. Superadmin only."""
    result = await db.execute(select(DashboardUser).where(DashboardUser.id == user_id))
    target = result.scalar_one_or_none()
    if not target:
        raise HTTPException(status_code=404, detail="User not found")
    if target.status != UserStatus.PENDING.value:
        raise HTTPException(status_code=400, detail="User is not in pending state")

    target.status = UserStatus.REJECTED.value
    await db.commit()
    await db.refresh(target)

    await audit_log_user(db, user, action="user.reject", resource_type="user",
        resource_id=str(target.id), details={"name": target.name, "email": target.email})

    return _user_response(target)


@router.put("/users/{user_id}/role", response_model=UserResponse)
async def update_user_role(
    user_id: int,
    payload: UserRoleUpdate,
    user: DashboardUser = Depends(require_role(UserRole.SUPERADMIN.value)),
    db: AsyncSession = Depends(get_db),
):
    """Change a user's role. Superadmin only. Cannot change own role."""
    if payload.role not in (UserRole.ADMIN.value, UserRole.USER.value):
        raise HTTPException(status_code=400, detail="role must be 'admin' or 'user'")

    if user_id == user.id:
        raise HTTPException(status_code=400, detail="Cannot change your own role")

    result = await db.execute(select(DashboardUser).where(DashboardUser.id == user_id))
    target = result.scalar_one_or_none()
    if not target:
        raise HTTPException(status_code=404, detail="User not found")
    if target.role == UserRole.SUPERADMIN.value:
        raise HTTPException(status_code=400, detail="Cannot change superadmin role")

    target.role = payload.role
    await db.commit()
    await db.refresh(target)

    await audit_log_user(db, user, action="user.role_change", resource_type="user",
        resource_id=str(target.id), details={"name": target.name, "email": target.email, "new_role": payload.role})

    return _user_response(target)


@router.delete("/users/{user_id}", status_code=204)
async def delete_user(
    user_id: int,
    user: DashboardUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Delete a user.
    - Superadmin can delete admin + user
    - Admin can delete user only
    - Cannot delete yourself
    - Cannot delete superadmin
    """
    if user_id == user.id:
        raise HTTPException(status_code=400, detail="Cannot delete yourself")

    result = await db.execute(select(DashboardUser).where(DashboardUser.id == user_id))
    target = result.scalar_one_or_none()
    if not target:
        raise HTTPException(status_code=404, detail="User not found")

    if target.role == UserRole.SUPERADMIN.value:
        raise HTTPException(status_code=400, detail="Cannot delete superadmin")

    # Permission checks
    if user.role == UserRole.ADMIN.value:
        if target.role != UserRole.USER.value:
            raise HTTPException(status_code=403, detail="Admin can only delete users")
    elif user.role != UserRole.SUPERADMIN.value:
        raise HTTPException(status_code=403, detail="Insufficient permissions")

    await db.delete(target)
    await db.commit()

    await audit_log_user(db, user, action="user.delete", resource_type="user",
        resource_id=str(user_id), details={"name": target.name, "email": target.email})