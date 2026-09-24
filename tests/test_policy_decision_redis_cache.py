"""
Regression tests for the 2026-09-24 cross-request policy-decision Redis
cache (decision_engine.py + app/api/admin_metrics.py): a second request for
the same (country, asn, flags) must reuse the first request's database
lookup via Redis, and any policy create/update/delete must instantly
invalidate it so an admin's change is never served stale.
"""
import asyncio

import pytest
from sqlalchemy import event

from app.decision_engine import DecisionEngine
from app.models import CountryPolicy, ISPPolicy
from conftest import make_db


@pytest.fixture
def fake_cache(monkeypatch):
    """Shared fake Redis-backed cache (get/set/delete with wildcard support)
    for both decision_engine.py and admin_metrics.py, so cache invalidation
    from one module is visible to the other — exactly as the real Redis is
    shared in production."""
    import app.decision_engine as de
    import app.api.admin_metrics as am
    store = {}

    async def get(k):
        return store.get(k)

    async def put(k, v, ttl=3):
        store[k] = v

    async def delete(pattern):
        if pattern.endswith("*"):
            prefix = pattern[:-1]
            for k in [k for k in store if k.startswith(prefix)]:
                store.pop(k, None)
        else:
            store.pop(pattern, None)

    monkeypatch.setattr(de, "get_cache", get)
    monkeypatch.setattr(de, "set_cache", put)
    monkeypatch.setattr(am, "delete_cache", delete)
    return store


def _count_statements(engine):
    counter = {"n": 0}

    def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        counter["n"] += 1

    event.listen(engine, "before_cursor_execute", before_cursor_execute)
    return counter, before_cursor_execute


def test_second_engine_instance_reuses_redis_cached_decision(fake_cache):
    """The actual point of this fix: a second, unrelated request (a fresh
    DecisionEngine, no per-request cache overlap) for the same key must not
    hit the database again."""
    async def go():
        async with make_db() as db:
            db.add(ISPPolicy(country="PK", asn="AS1", protocol="shadowsocks", status="preferred"))
            await db.commit()

            engine1 = DecisionEngine(db)
            r1 = await engine1._get_policy_decision("PK", "AS1", True, True)
            assert r1 == ("shadowsocks", "openvpn")

            engine2 = DecisionEngine(db)
            counter, hook = _count_statements(db.get_bind())
            try:
                r2 = await engine2._get_policy_decision("PK", "AS1", True, True)
            finally:
                event.remove(db.get_bind(), "before_cursor_execute", hook)

            assert r2 == r1
            assert counter["n"] == 0, "a second request for the same key must be served from Redis, not the database"

    asyncio.run(go())


def test_no_policy_result_is_also_cached_correctly(fake_cache):
    """The 'no policy applies' (None) case must be cached as reliably as a
    real decision — not confused with 'not cached yet'."""
    async def go():
        async with make_db() as db:
            engine1 = DecisionEngine(db)
            assert await engine1._get_policy_decision("PK", "AS1", True, True) is None

            engine2 = DecisionEngine(db)
            counter, hook = _count_statements(db.get_bind())
            try:
                r2 = await engine2._get_policy_decision("PK", "AS1", True, True)
            finally:
                event.remove(db.get_bind(), "before_cursor_execute", hook)

            assert r2 is None
            assert counter["n"] == 0

    asyncio.run(go())


def test_different_keys_cached_independently_via_redis(fake_cache):
    async def go():
        async with make_db() as db:
            db.add(ISPPolicy(country="PK", asn="AS1", protocol="shadowsocks", status="preferred"))
            db.add(ISPPolicy(country="IR", asn="AS2", protocol="openvpn", status="preferred"))
            await db.commit()

            e1 = DecisionEngine(db)
            pk = await e1._get_policy_decision("PK", "AS1", True, True)
            ir = await e1._get_policy_decision("IR", "AS2", True, True)

            e2 = DecisionEngine(db)
            assert await e2._get_policy_decision("PK", "AS1", True, True) == pk == ("shadowsocks", "openvpn")
            assert await e2._get_policy_decision("IR", "AS2", True, True) == ir == ("openvpn", "shadowsocks")

    asyncio.run(go())


def test_creating_a_policy_invalidates_the_cache(fake_cache):
    """An admin adding a NEW policy must be picked up on the very next
    request, not wait out the TTL."""
    async def go():
        async with make_db() as db:
            engine1 = DecisionEngine(db)
            assert await engine1._get_policy_decision("PK", "AS1", True, True) is None

            import app.api.admin_metrics as am
            policy = ISPPolicy(country="PK", asn="AS1", protocol="shadowsocks", status="preferred")
            db.add(policy)
            await db.commit()
            await am.delete_cache("policy_decision:*")  # what create_isp_policy() now does

            engine2 = DecisionEngine(db)
            assert await engine2._get_policy_decision("PK", "AS1", True, True) == ("shadowsocks", "openvpn")

    asyncio.run(go())


def test_admin_endpoints_actually_call_the_invalidation(fake_cache):
    """Confirms the real endpoint code paths (not just my own test calling
    delete_cache by hand) invalidate the cache — patches only the DB layer,
    exercises the real FastAPI endpoint functions."""
    async def go():
        import app.api.admin_metrics as am
        async with make_db() as db:
            async def noop_audit(*a, **kw):
                pass
            am.audit_log = noop_audit

            fake_cache["policy_decision:PK:AS1:True:True"] = ["shadowsocks", "openvpn"]

            payload = am.ISPPolicyCreate(
                asn="AS1", protocol="openvpn", status="preferred", country="PK",
            )
            await am.create_isp_policy(payload, db, "tok")
            assert "policy_decision:PK:AS1:True:True" not in fake_cache, \
                "create_isp_policy must invalidate the policy_decision cache"

    asyncio.run(go())
