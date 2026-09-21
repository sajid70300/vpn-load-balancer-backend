"""Tests for the history snapshot interval setting (admin_settings.py)."""
import asyncio

import pytest
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from app.api import admin_settings
from app.models import GlobalSettings
from app.schemas import GlobalSettingsUpdate
from conftest import make_db


@pytest.fixture
def cache(monkeypatch, noop_audit):
    store, deleted = {}, []

    async def get(k):
        return store.get(k)

    async def put(k, v, ttl=3):
        store[k] = v

    async def delete(p):
        deleted.append(p)
        store.pop(p, None)

    monkeypatch.setattr(admin_settings, "get_cache", get)
    monkeypatch.setattr(admin_settings, "set_cache", put)
    monkeypatch.setattr(admin_settings, "delete_cache", delete)
    noop_audit(admin_settings)
    return store, deleted


def test_default_is_30_and_can_be_changed(cache):
    async def go():
        async with make_db() as db:
            assert (await admin_settings.get_global_settings(db, "t")).history_interval_minutes == 30
            r = await admin_settings.update_global_settings(GlobalSettingsUpdate(history_interval_minutes=5), db, "t")
            assert r.history_interval_minutes == 5

    asyncio.run(go())


def test_engine_written_cache_does_not_hide_the_real_value(cache):
    """The decision engine shares the 'global_settings' cache key and writes a payload
    WITHOUT the interval; the admin GET must not serve that stale shape."""
    store, _ = cache

    async def go():
        async with make_db() as db:
            await admin_settings.update_global_settings(GlobalSettingsUpdate(history_interval_minutes=5), db, "t")
            store["global_settings"] = {
                "protocol_mode": "auto", "disable_new_connections": False,
                "enforce_country_policies": True, "enforce_isp_policies": True,
                "cooldown_soft_seconds": 300, "cooldown_hard_seconds": 3600,
                "failure_rate_threshold": 10.0, "cooldown_country_block_asn_threshold": 3,
            }
            r = await admin_settings.get_global_settings(db, "t")
            value = r["history_interval_minutes"] if isinstance(r, dict) else r.history_interval_minutes
            assert value == 5

    asyncio.run(go())


@pytest.mark.parametrize("bad", [0, -3, 1441])
def test_invalid_interval_rejected(cache, bad):
    async def go():
        async with make_db() as db:
            with pytest.raises(HTTPException) as e:
                await admin_settings.update_global_settings(GlobalSettingsUpdate(history_interval_minutes=bad), db, "t")
            assert e.value.status_code == 400

    asyncio.run(go())


def test_interval_only_change_does_not_flush_routing_caches(cache):
    _, deleted = cache

    async def go():
        async with make_db() as db:
            await admin_settings.update_global_settings(GlobalSettingsUpdate(history_interval_minutes=10), db, "t")
            assert "best_server_v2:*" not in deleted and "policy_bias:*" not in deleted

            deleted.clear()
            r = await admin_settings.update_global_settings(GlobalSettingsUpdate(protocol_mode="force_openvpn"), db, "t")
            assert "best_server_v2:*" in deleted and "policy_bias:*" in deleted
            assert r.history_interval_minutes == 10          # untouched by a routing change

    asyncio.run(go())


def test_decision_engine_query_never_selects_the_new_column():
    """Hot-path safety: routing keeps working even if the migration hasn't run yet."""
    sql = str(select(GlobalSettings).where(GlobalSettings.id == 1).compile(dialect=postgresql.dialect()))
    assert "history_interval_minutes" not in sql
