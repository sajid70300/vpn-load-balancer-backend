"""
Admin API – Global Settings
Manages the single-row GlobalSettings table that controls routing behaviour
system-wide: protocol mode, emergency overrides, cooldown timers.
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.database import get_db
from app.models import GlobalSettings
from app.schemas import GlobalSettingsResponse, GlobalSettingsUpdate
from app.auth import verify_api_key
from app.audit import audit_log
from app.cache import get_cache, set_cache, delete_cache

router = APIRouter(prefix="/admin/settings", tags=["Admin - Global Settings"])

VALID_PROTOCOL_MODES = {'auto', 'force_openvpn', 'force_shadowsocks'}

# Cache key for global settings — long TTL since they change very rarely.
# Invalidated explicitly on every PUT so staleness is never an issue.
SETTINGS_CACHE_KEY = "global_settings"
SETTINGS_CACHE_TTL = 3600  # 1 hour — effectively permanent until invalidated


def _to_response(s: GlobalSettings) -> GlobalSettingsResponse:
    return GlobalSettingsResponse(
        protocol_mode=s.protocol_mode,
        disable_new_connections=s.disable_new_connections,
        enforce_country_policies=s.enforce_country_policies,
        enforce_isp_policies=s.enforce_isp_policies,
        cooldown_soft_seconds=s.cooldown_soft_seconds,
        cooldown_medium_seconds=s.cooldown_medium_seconds,
        cooldown_hard_seconds=s.cooldown_hard_seconds,
        failure_rate_threshold=s.failure_rate_threshold,
        cooldown_country_block_asn_threshold=s.cooldown_country_block_asn_threshold,
        updated_at=s.updated_at,
    )


async def _get_or_create_settings(db: AsyncSession) -> GlobalSettings:
    """Always returns the single settings row, creating it if absent."""
    result = await db.execute(select(GlobalSettings).where(GlobalSettings.id == 1))
    settings = result.scalar_one_or_none()
    if not settings:
        settings = GlobalSettings(id=1)
        db.add(settings)
        await db.commit()
        await db.refresh(settings)
    return settings


@router.get("/", response_model=GlobalSettingsResponse)
async def get_global_settings(
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key),
):
    """
    Return current global routing settings.
    Result is served from Redis cache (TTL 1 hour). Cache is invalidated
    automatically on every PUT so the dashboard always sees the latest value.
    """
    cached = await get_cache(SETTINGS_CACHE_KEY)
    if cached:
        return cached

    s = await _get_or_create_settings(db)
    response = _to_response(s)
    await set_cache(SETTINGS_CACHE_KEY, response.model_dump(), ttl=SETTINGS_CACHE_TTL)
    return response


@router.put("/", response_model=GlobalSettingsResponse)
async def update_global_settings(
    payload: GlobalSettingsUpdate,
    db: AsyncSession = Depends(get_db),
    token: str = Depends(verify_api_key),
):
    """
    Update global routing settings.

    protocol_mode values:
      'auto'              – decision engine runs normally
      'force_openvpn'     – primary protocol is always OpenVPN
      'force_shadowsocks' – primary protocol is always Shadowsocks

    On every update:
      1. DB row is updated
      2. global_settings cache is invalidated (decision engine reads fresh)
      3. best_server_v2 cache is invalidated (Android app sees new policy immediately)
    """
    s = await _get_or_create_settings(db)

    if payload.protocol_mode is not None:
        if payload.protocol_mode not in VALID_PROTOCOL_MODES:
            raise HTTPException(
                status_code=400,
                detail=f"protocol_mode must be one of: {VALID_PROTOCOL_MODES}",
            )
        s.protocol_mode = payload.protocol_mode

    if payload.disable_new_connections is not None:
        s.disable_new_connections = payload.disable_new_connections

    if payload.enforce_country_policies is not None:
        s.enforce_country_policies = payload.enforce_country_policies

    if payload.enforce_isp_policies is not None:
        s.enforce_isp_policies = payload.enforce_isp_policies

    if payload.cooldown_soft_seconds is not None:
        if payload.cooldown_soft_seconds < 0:
            raise HTTPException(status_code=400, detail="cooldown_soft_seconds must be >= 0")
        s.cooldown_soft_seconds = payload.cooldown_soft_seconds

    if payload.cooldown_medium_seconds is not None:
        if payload.cooldown_medium_seconds < 0:
            raise HTTPException(status_code=400, detail="cooldown_medium_seconds must be >= 0")
        s.cooldown_medium_seconds = payload.cooldown_medium_seconds

    if payload.cooldown_hard_seconds is not None:
        if payload.cooldown_hard_seconds < 0:
            raise HTTPException(status_code=400, detail="cooldown_hard_seconds must be >= 0")
        s.cooldown_hard_seconds = payload.cooldown_hard_seconds

    if payload.failure_rate_threshold is not None:
        if not (0.0 <= payload.failure_rate_threshold <= 100.0):
            raise HTTPException(status_code=400, detail="failure_rate_threshold must be 0–100")
        s.failure_rate_threshold = payload.failure_rate_threshold

    if payload.cooldown_country_block_asn_threshold is not None:
        if payload.cooldown_country_block_asn_threshold < 1:
            raise HTTPException(status_code=400, detail="cooldown_country_block_asn_threshold must be >= 1")
        s.cooldown_country_block_asn_threshold = payload.cooldown_country_block_asn_threshold

    await db.commit()
    await db.refresh(s)

    # 1. Invalidate the settings cache — next read goes to DB and re-caches
    await delete_cache(SETTINGS_CACHE_KEY)
    # 2. Invalidate routing cache — Android app picks up new policy immediately
    await delete_cache("best_server_v2:*")
    # 3. Invalidate policy bias cache — enforce_country/isp toggle changes must
    #    take effect instantly; bias values were cached with the old toggle state
    await delete_cache("policy_bias:*")

    response = _to_response(s)
    # Re-populate the settings cache with the fresh value right away
    await set_cache(SETTINGS_CACHE_KEY, response.model_dump(), ttl=SETTINGS_CACHE_TTL)

    await audit_log(db, token, action="global_settings.update", resource_type="settings",
        details=response.model_dump())

    return response