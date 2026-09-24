"""
Regression tests for the 2026-09-22 connection-pool fix (early commits in
decision_engine.py / public.py's servers_config): every added `await
self.db.commit()` must be a pure pool-release with ZERO effect on the actual
routing decisions.

These drive the real DecisionEngine (not a mock) against an in-memory
database, and also verify the session survives committing mid-request and
continuing to be used for further reads/writes — the exact pattern this fix
introduces.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from app.decision_engine import DecisionEngine
from app.models import (
    CountryPolicy, GlobalSettings, ISPPolicy, ProtocolMetrics, VPNServer, VPNUserSession,
)
from conftest import make_db


def _server(**kw):
    base = dict(
        name="s", ip_address="1.1.1.1", app_name="appA", server_type="free",
        max_capacity=100, is_active=True, is_priority_group=False,
        cpu_usage=10.0, ram_usage=20.0, ping_latency_ms=30.0, load_score=5.0,
        management_port=7505, ovpn_base64="b64", ss_port=8388, ss_password="p", ss_encryption="aes",
    )
    base.update(kw)
    return VPNServer(**base)


@pytest.fixture(autouse=True)
def fake_cache(monkeypatch):
    """Real Redis logic isn't under test here — a simple in-memory stand-in
    keeps these tests fast and deterministic, matching the 5s/long-TTL caches
    the engine already relies on."""
    import app.decision_engine as de
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

    class FakeRedis:
        def __init__(self):
            self.kv = {}

        async def get(self, k):
            return self.kv.get(k)

        async def setex(self, k, ttl, v):
            self.kv[k] = v

        async def exists(self, k):
            return k in self.kv

        async def sadd(self, k, v):
            self.kv.setdefault(k, set()).add(v)

        async def expire(self, k, ttl):
            pass

        async def smembers(self, k):
            return self.kv.get(k, set())

        async def srem(self, k, v):
            self.kv.get(k, set()).discard(v)

        async def keys(self, pattern):
            prefix = pattern[:-1] if pattern.endswith("*") else pattern
            return [k for k in self.kv if k.startswith(prefix)]

        async def delete(self, *keys):
            for k in keys:
                self.kv.pop(k, None)

        def pipeline(self):
            return _FakePipeline(self.kv)

    class _FakePipeline:
        """Minimal stand-in for redis.asyncio's Pipeline, used by
        _filter_out_cooldown_servers()'s batched cooldown check."""
        def __init__(self, kv):
            self.kv = kv
            self.queued = []

        def exists(self, key):
            self.queued.append(key)
            return self

        async def execute(self):
            return [1 if k in self.kv else 0 for k in self.queued]

    fake_redis = FakeRedis()

    async def get_redis():
        return fake_redis

    monkeypatch.setattr(de, "get_cache", get)
    monkeypatch.setattr(de, "set_cache", put)
    monkeypatch.setattr(de, "get_redis", get_redis)

    # process_connection_feedback()'s _invalidate_metrics_cache() does a fresh
    # `from app.cache import delete_cache` at call time, which reaches
    # app.cache's OWN get_redis singleton — patch that too, or it tries a real
    # network connection to the dummy CACHE_REDIS_URL from conftest.py.
    import app.cache as cache_module
    monkeypatch.setattr(cache_module, "get_redis", get_redis)
    return store


def test_engine_survives_committing_mid_request_and_keeps_reading():
    """The core mechanical risk of this fix: does the session still work,
    and do already-fetched ORM attributes stay readable, after a commit
    happens partway through a request?"""
    async def go():
        async with make_db() as db:
            db.add_all([_server(name="s1", ip_address="1.1.1.1", is_priority_group=True, load_score=1.0)])
            await db.commit()

            engine = DecisionEngine(db)
            decision = await engine.get_best_server(app_name="appA")
            assert decision.primary_config.ip_address == "1.1.1.1"

            # The session must still be usable for a completely separate,
            # later query in the same request — proving commit() didn't
            # close or break it.
            from sqlalchemy import select
            again = (await db.execute(select(VPNServer))).scalars().all()
            assert len(again) == 1

    asyncio.run(go())


def test_priority_and_load_score_ordering_unchanged():
    async def go():
        async with make_db() as db:
            db.add_all([
                _server(name="regular-good", ip_address="1.1.1.1", is_priority_group=False, cpu_usage=5, ram_usage=5),
                _server(name="regular-bad", ip_address="1.1.1.2", is_priority_group=False, cpu_usage=90, ram_usage=90),
                _server(name="priority-worse-hw", ip_address="1.1.1.3", is_priority_group=True, cpu_usage=95, ram_usage=95),
            ])
            await db.commit()
            engine = DecisionEngine(db)
            decision = await engine.get_best_server(app_name="appA")
            # Priority server wins even with worse hardware load — unaffected by the fix.
            assert decision.primary_config.ip_address == "1.1.1.3"

    asyncio.run(go())


def test_capacity_filtering_unchanged():
    async def go():
        async with make_db() as db:
            full = _server(name="full", ip_address="1.1.1.1", max_capacity=2)
            db.add(full)
            await db.flush()
            db.add_all([VPNUserSession(server_id=full.id, user_id=f"u{i}", device_ip="1.1.1.1") for i in range(2)])
            db.add(_server(name="open", ip_address="1.1.1.2", max_capacity=2))
            await db.commit()

            engine = DecisionEngine(db)
            decision = await engine.get_best_server(app_name="appA")
            assert decision.primary_config.ip_address == "1.1.1.2"

    asyncio.run(go())


def test_no_servers_and_no_capacity_still_raise_the_same_errors():
    async def go():
        async with make_db() as db:
            engine = DecisionEngine(db)
            with pytest.raises(ValueError, match="No active servers available"):
                await engine.get_best_server(app_name="appA")

    asyncio.run(go())

    async def go2():
        async with make_db() as db:
            full = _server(name="full", ip_address="1.1.1.1", max_capacity=1)
            db.add(full)
            await db.flush()
            db.add(VPNUserSession(server_id=full.id, user_id="u1", device_ip="1.1.1.1"))
            await db.commit()
            engine = DecisionEngine(db)
            # A server excluded for being at capacity is filtered out inside
            # _load_servers() itself, so with only one (full) server this is
            # indistinguishable from having none — "No active servers
            # available", not "All servers at capacity" (that message is for
            # servers that pass the capacity filter but every one fails
            # protocol scoring). This is pre-existing behavior, unrelated to
            # the pool fix — asserting it here just pins it down.
            with pytest.raises(ValueError, match="No active servers available"):
                await engine.get_best_server(app_name="appA")

    asyncio.run(go2())


def test_isp_policy_blocked_and_preferred_unchanged():
    async def go():
        async with make_db() as db:
            db.add(_server(name="s", ip_address="1.1.1.1"))
            db.add(ISPPolicy(country="PK", asn="AS111", protocol="openvpn", status="blocked"))
            await db.commit()
            engine = DecisionEngine(db)
            decision = await engine.get_best_server(app_name="appA", user_country="PK", user_asn="AS111")
            assert decision.primary_protocol == "shadowsocks"
            assert decision.fallback_protocol == "shadowsocks"

    asyncio.run(go())

    async def go2():
        async with make_db() as db:
            # Deliberately a different ASN than go() above — these two
            # scenarios are independent, and since a real admin change to a
            # policy goes through admin_metrics.py (which invalidates the
            # policy-decision cache), while this test edits the DB directly,
            # reusing the same key here would incorrectly hit go()'s cached
            # answer instead of exercising this scenario at all.
            db.add(_server(name="s", ip_address="1.1.1.1"))
            db.add(ISPPolicy(country="PK", asn="AS222", protocol="shadowsocks", status="preferred"))
            await db.commit()
            engine = DecisionEngine(db)
            decision = await engine.get_best_server(app_name="appA", user_country="PK", user_asn="AS222")
            assert decision.primary_protocol == "shadowsocks"
            assert decision.fallback_protocol == "openvpn"

    asyncio.run(go2())


def test_country_policy_used_when_no_isp_policy_unchanged():
    async def go():
        async with make_db() as db:
            db.add(_server(name="s", ip_address="1.1.1.1"))
            db.add(CountryPolicy(country="IR", is_active=True,
                                 preferred_protocol="shadowsocks", fallback_protocol="openvpn"))
            await db.commit()
            engine = DecisionEngine(db)
            decision = await engine.get_best_server(app_name="appA", user_country="IR")
            assert decision.primary_protocol == "shadowsocks"
            assert decision.fallback_protocol == "openvpn"

    asyncio.run(go())


def test_expired_isp_policy_falls_through_to_country_policy_unchanged():
    async def go():
        async with make_db() as db:
            db.add(_server(name="s", ip_address="1.1.1.1"))
            db.add(ISPPolicy(country="IR", asn="AS1", protocol="openvpn", status="blocked",
                             expiry=datetime.utcnow() - timedelta(days=1)))
            db.add(CountryPolicy(country="IR", is_active=True,
                                 preferred_protocol="shadowsocks", fallback_protocol="openvpn"))
            await db.commit()
            engine = DecisionEngine(db)
            decision = await engine.get_best_server(app_name="appA", user_country="IR", user_asn="AS1")
            assert decision.primary_protocol == "shadowsocks"

    asyncio.run(go())


def test_auto_scoring_from_protocol_metrics_db_fallback_unchanged():
    """No policy applies and the Redis cache is empty, so this must fall through
    to _get_protocol_metrics_db — exactly the path that got a mid-function commit."""
    async def go():
        async with make_db() as db:
            srv = _server(name="s", ip_address="1.1.1.1")
            db.add(srv)
            await db.flush()
            db.add_all([
                ProtocolMetrics(server_id=srv.id, protocol="openvpn", country=None, asn=None, network_type=None,
                                success_count=90, failure_count=10, total_attempts=100,
                                avg_connect_time_ms=200.0),
                ProtocolMetrics(server_id=srv.id, protocol="shadowsocks", country=None, asn=None, network_type=None,
                                success_count=10, failure_count=90, total_attempts=100,
                                avg_connect_time_ms=5000.0),
            ])
            await db.commit()
            engine = DecisionEngine(db)
            decision = await engine.get_best_server(app_name="appA")
            assert decision.primary_protocol == "openvpn"       # clearly better success rate + speed
            assert decision.fallback_protocol == "shadowsocks"

    asyncio.run(go())


def test_force_protocol_mode_unchanged():
    async def go():
        async with make_db() as db:
            db.add(GlobalSettings(id=1, protocol_mode="force_shadowsocks"))
            db.add(_server(name="s", ip_address="1.1.1.1"))
            await db.commit()
            engine = DecisionEngine(db)
            decision = await engine.get_best_server(app_name="appA")
            assert decision.primary_protocol == "shadowsocks"
            assert decision.fallback_protocol == "openvpn"

    asyncio.run(go())


def test_servers_config_style_multi_server_loop_unchanged():
    """Simulates public.py's servers_config decision-mode loop directly against
    the engine: many servers, each independently decided, exactly like the loop
    that motivated committing inside get_protocol_decision_for_server."""
    async def go():
        async with make_db() as db:
            for i in range(10):
                db.add(_server(name=f"s{i}", ip_address=f"10.0.0.{i}", app_name="appA"))
            await db.commit()

            engine = DecisionEngine(db)
            from sqlalchemy import select
            servers = (await db.execute(select(VPNServer).where(VPNServer.app_name == "appA"))).scalars().all()
            assert len(servers) == 10

            results = []
            for s in servers:
                d = await engine.get_protocol_decision_for_server(
                    ip_address=s.ip_address, server_type=s.server_type, app_name="appA")
                results.append(d)

            assert len(results) == 10
            assert all(r is not None for r in results)
            assert {r.primary_config.ip_address for r in results} == {f"10.0.0.{i}" for i in range(10)}

    asyncio.run(go())


def test_connection_feedback_and_cooldown_unchanged():
    async def go():
        async with make_db() as db:
            srv = _server(name="s", ip_address="1.1.1.1")
            db.add(srv)
            db.add(GlobalSettings(id=1, cooldown_soft_seconds=60, cooldown_hard_seconds=300))
            await db.commit()

            engine = DecisionEngine(db)
            await engine.process_connection_feedback(
                server_id=srv.id, server_ip=srv.ip_address, app_name="appA",
                country="PK", asn="AS1", network_type="wifi",
                primary_protocol="openvpn", primary_success=False, primary_connect_time_ms=None,
                secondary_protocol="shadowsocks", secondary_success=False, secondary_connect_time_ms=None,
            )

            from sqlalchemy import select
            metrics = (await db.execute(select(ProtocolMetrics).where(ProtocolMetrics.server_id == srv.id))).scalars().all()
            assert {m.protocol: m.failure_count for m in metrics} == {"openvpn": 1, "shadowsocks": 1}

            in_cooldown = await engine._server_in_cooldown(srv.ip_address, "PK", "AS1")
            assert in_cooldown is True

    asyncio.run(go())


def test_connection_is_actually_released_not_just_output_unchanged():
    """The tests above prove decisions are unaffected, but they'd pass just as
    well if the commits were silently removed (removing a commit changes
    nothing about WHAT is decided, only how long the connection is held).
    This test verifies the actual mechanism: after each read call site that
    got a commit, the session has no open transaction — the connection is
    genuinely back in the pool, not just "probably fine"."""
    async def go():
        async with make_db() as db:
            db.add(_server(name="s", ip_address="1.1.1.1", is_priority_group=True))
            db.add(CountryPolicy(country="IR", is_active=True,
                                 preferred_protocol="shadowsocks", fallback_protocol="openvpn"))
            await db.commit()

            engine = DecisionEngine(db)

            servers = await engine._load_servers("appA", None)
            assert db.in_transaction() is False, \
                "_load_servers() left a transaction open — the connection is still held"

            decision = await engine.get_protocol_decision_for_server(
                ip_address="1.1.1.1", server_type="free", app_name="appA", user_country="IR")
            assert decision is not None
            assert db.in_transaction() is False, \
                "get_protocol_decision_for_server() left a transaction open"

    asyncio.run(go())
