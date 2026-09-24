"""
Regression tests for the 2026-09-24 policy-decision caching fix
(decision_engine.py): _get_policy_decision() must return byte-identical
results to before, while no longer re-querying the database for a
(country, asn, enforce flags) combination it has already resolved once
within the same DecisionEngine instance (i.e. within one request).
"""
import asyncio
from datetime import datetime, timedelta

import pytest
from sqlalchemy import event

from app.decision_engine import DecisionEngine
from app.models import CountryPolicy, ISPPolicy
from conftest import make_db


@pytest.fixture(autouse=True)
def no_redis_cache(monkeypatch):
    """This file tests the per-request (in-memory) cache layer specifically —
    stub out the cross-request Redis layer added on top of it (always a
    miss) so these tests exercise exactly what they did before that layer
    existed. The Redis layer itself has its own dedicated tests in
    test_policy_decision_redis_cache.py."""
    import app.decision_engine as de

    async def always_miss(k):
        return None

    async def noop_set(k, v, ttl=3):
        pass

    monkeypatch.setattr(de, "get_cache", always_miss)
    monkeypatch.setattr(de, "set_cache", noop_set)


def _count_statements(engine):
    counter = {"n": 0}

    def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        counter["n"] += 1

    event.listen(engine, "before_cursor_execute", before_cursor_execute)
    return counter, before_cursor_execute


def test_no_policy_at_all_returns_none():
    async def go():
        async with make_db() as db:
            engine = DecisionEngine(db)
            result = await engine._get_policy_decision("PK", "AS1", True, True)
            assert result is None

    asyncio.run(go())


def test_isp_preferred_result_unchanged():
    async def go():
        async with make_db() as db:
            db.add(ISPPolicy(country="PK", asn="AS1", protocol="shadowsocks", status="preferred"))
            await db.commit()
            engine = DecisionEngine(db)
            result = await engine._get_policy_decision("PK", "AS1", True, True)
            assert result == ("shadowsocks", "openvpn")

    asyncio.run(go())


def test_isp_blocked_single_protocol_result_unchanged():
    async def go():
        async with make_db() as db:
            db.add(ISPPolicy(country="PK", asn="AS1", protocol="openvpn", status="blocked"))
            await db.commit()
            engine = DecisionEngine(db)
            result = await engine._get_policy_decision("PK", "AS1", True, True)
            assert result == ("shadowsocks", "shadowsocks")

    asyncio.run(go())


def test_isp_degraded_falls_through_to_country_policy():
    async def go():
        async with make_db() as db:
            db.add(ISPPolicy(country="IR", asn="AS9", protocol="openvpn", status="degraded"))
            db.add(CountryPolicy(country="IR", is_active=True,
                                 preferred_protocol="shadowsocks", fallback_protocol="openvpn"))
            await db.commit()
            engine = DecisionEngine(db)
            result = await engine._get_policy_decision("IR", "AS9", True, True)
            assert result == ("shadowsocks", "openvpn")

    asyncio.run(go())


def test_expired_isp_policy_ignored_same_as_before():
    async def go():
        async with make_db() as db:
            db.add(ISPPolicy(country="IR", asn="AS9", protocol="openvpn", status="blocked",
                             expiry=datetime.utcnow() - timedelta(days=1)))
            await db.commit()
            engine = DecisionEngine(db)
            result = await engine._get_policy_decision("IR", "AS9", True, True)
            assert result is None

    asyncio.run(go())


def test_country_policy_used_when_no_asn_given():
    async def go():
        async with make_db() as db:
            db.add(CountryPolicy(country="UK", is_active=True,
                                 preferred_protocol="openvpn", fallback_protocol="shadowsocks"))
            await db.commit()
            engine = DecisionEngine(db)
            result = await engine._get_policy_decision("UK", None, True, True)
            assert result == ("openvpn", "shadowsocks")

    asyncio.run(go())


def test_repeated_calls_with_same_key_only_query_the_database_once():
    """The actual point of this fix: simulates /servers_config/'s loop calling
    this once per server, all with the SAME country/asn — must only hit the
    database on the first call."""
    async def go():
        async with make_db() as db:
            db.add(ISPPolicy(country="PK", asn="AS1", protocol="shadowsocks", status="preferred"))
            await db.commit()

            engine = DecisionEngine(db)
            counter, hook = _count_statements(db.get_bind())
            try:
                results = [await engine._get_policy_decision("PK", "AS1", True, True) for _ in range(20)]
            finally:
                event.remove(db.get_bind(), "before_cursor_execute", hook)

            assert all(r == ("shadowsocks", "openvpn") for r in results), "every call must still return the correct result"
            print(f"SQL statements for 20 repeated policy lookups (same key): {counter['n']}")
            assert counter["n"] <= 2, (
                f"expected at most ~1 query total for 20 identical lookups, got {counter['n']} — "
                f"the cache may not be taking effect"
            )

    asyncio.run(go())


def test_different_country_asn_combinations_are_cached_independently():
    async def go():
        async with make_db() as db:
            db.add(ISPPolicy(country="PK", asn="AS1", protocol="shadowsocks", status="preferred"))
            db.add(ISPPolicy(country="IR", asn="AS2", protocol="openvpn", status="preferred"))
            await db.commit()

            engine = DecisionEngine(db)
            pk = await engine._get_policy_decision("PK", "AS1", True, True)
            ir = await engine._get_policy_decision("IR", "AS2", True, True)
            pk_again = await engine._get_policy_decision("PK", "AS1", True, True)

            assert pk == ("shadowsocks", "openvpn")
            assert ir == ("openvpn", "shadowsocks")
            assert pk_again == pk, "re-fetching the first key later must still return its own correct cached result"

    asyncio.run(go())


def test_each_new_engine_instance_starts_with_an_empty_cache():
    """Guards against any accidental cross-request cache leakage — a fresh
    DecisionEngine (created per request in public.py) must never see another
    request's cached policy decisions."""
    async def go():
        async with make_db() as db:
            db.add(ISPPolicy(country="PK", asn="AS1", protocol="shadowsocks", status="preferred"))
            await db.commit()

            engine1 = DecisionEngine(db)
            await engine1._get_policy_decision("PK", "AS1", True, True)
            assert ("PK", "AS1", True, True) in engine1._policy_decision_cache

            engine2 = DecisionEngine(db)
            assert engine2._policy_decision_cache == {}, "a new engine instance must not inherit another instance's cache"

    asyncio.run(go())


def test_query_count_scenario_matching_servers_config_loop():
    """Simulates a /servers_config/ request for an app with 40 active
    servers — before this fix, this meant ~40 identical policy queries."""
    async def go():
        async with make_db() as db:
            db.add(CountryPolicy(country="PK", is_active=True,
                                 preferred_protocol="openvpn", fallback_protocol="shadowsocks"))
            await db.commit()

            engine = DecisionEngine(db)
            N = 40
            counter, hook = _count_statements(db.get_bind())
            try:
                for _ in range(N):
                    result = await engine._get_policy_decision("PK", None, True, True)
                    assert result == ("openvpn", "shadowsocks")
            finally:
                event.remove(db.get_bind(), "before_cursor_execute", hook)

            print(f"SQL statements for {N}-server servers_config-style loop: {counter['n']}")
            assert counter["n"] < N, f"expected well under {N} statements, got {counter['n']}"

    asyncio.run(go())
