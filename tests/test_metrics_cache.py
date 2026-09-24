"""
Regression tests for the 2026-09-25 per-request metrics-cache fix
(decision_engine.py): _get_protocol_metrics_cached() must now avoid even the
Redis round-trip for a (protocol, country, asn, network_type) key it has
already resolved once within the same DecisionEngine instance (i.e. within
one request) — found via a live py-spy profile showing this was still a
major cost despite its existing 5s Redis cache.
"""
import asyncio

import pytest
from sqlalchemy import event

from app.decision_engine import DecisionEngine
from app.models import ProtocolMetrics, VPNServer
from conftest import make_db


@pytest.fixture
def fake_cache(monkeypatch):
    """Counts real Redis round-trips (get_cache/set_cache calls), not just
    database statements — that's the specific thing this fix reduces."""
    import app.decision_engine as de
    store = {}
    calls = {"get": 0, "set": 0}

    async def get(k):
        calls["get"] += 1
        return store.get(k)

    async def put(k, v, ttl=3):
        calls["set"] += 1
        store[k] = v

    monkeypatch.setattr(de, "get_cache", get)
    monkeypatch.setattr(de, "set_cache", put)
    return store, calls


def _server(name="s", ip="1.1.1.1", app="appA"):
    return VPNServer(name=name, ip_address=ip, app_name=app, server_type="free",
                     max_capacity=100, is_active=True)


def test_result_unchanged_no_metrics(fake_cache):
    async def go():
        async with make_db() as db:
            engine = DecisionEngine(db)
            result = await engine._get_protocol_metrics_cached("appA", "openvpn", "PK", "AS1", "wifi")
            assert result is None

    asyncio.run(go())


def test_result_unchanged_with_metrics(fake_cache):
    async def go():
        async with make_db() as db:
            srv = _server()
            db.add(srv)
            await db.flush()
            db.add(ProtocolMetrics(server_id=srv.id, protocol="openvpn", country="PK", asn="AS1",
                                   network_type="wifi", success_count=8, failure_count=2,
                                   total_attempts=10, avg_connect_time_ms=100.0))
            await db.commit()

            engine = DecisionEngine(db)
            result = await engine._get_protocol_metrics_cached("appA", "openvpn", "PK", "AS1", "wifi")
            assert result["success_count"] == 8 and result["total_attempts"] == 10

    asyncio.run(go())


def test_repeated_calls_same_key_avoid_even_the_redis_round_trip(fake_cache):
    """The actual point of this fix: simulates /servers_config/'s loop
    calling this once per server (per protocol), all with the same
    country/asn/network_type — must only touch Redis on the first call."""
    store, calls = fake_cache

    async def go():
        async with make_db() as db:
            engine = DecisionEngine(db)
            results = []
            for _ in range(30):
                results.append(await engine._get_protocol_metrics_cached("appA", "openvpn", "PK", "AS1", "wifi"))

            assert all(r == results[0] for r in results)
            print(f"Redis get_cache calls for 30 repeated lookups (same key): {calls['get']}")
            assert calls["get"] <= 1, (
                f"expected at most 1 Redis get_cache call for 30 identical lookups, got {calls['get']}"
            )

    asyncio.run(go())


def test_different_keys_cached_independently(fake_cache):
    async def go():
        async with make_db() as db:
            srv = _server()
            db.add(srv)
            await db.flush()
            db.add(ProtocolMetrics(server_id=srv.id, protocol="openvpn", country="PK", asn="AS1",
                                   network_type="wifi", success_count=9, failure_count=1,
                                   total_attempts=10, avg_connect_time_ms=50.0))
            db.add(ProtocolMetrics(server_id=srv.id, protocol="shadowsocks", country="IR", asn="AS2",
                                   network_type="mobile", success_count=3, failure_count=7,
                                   total_attempts=10, avg_connect_time_ms=500.0))
            await db.commit()

            engine = DecisionEngine(db)
            a = await engine._get_protocol_metrics_cached("appA", "openvpn", "PK", "AS1", "wifi")
            b = await engine._get_protocol_metrics_cached("appA", "shadowsocks", "IR", "AS2", "mobile")
            assert a["success_count"] == 9
            assert b["success_count"] == 3

            # Re-fetching the first key later must still return its own value.
            a_again = await engine._get_protocol_metrics_cached("appA", "openvpn", "PK", "AS1", "wifi")
            assert a_again == a

    asyncio.run(go())


def test_each_new_engine_instance_starts_with_an_empty_metrics_cache(fake_cache):
    async def go():
        async with make_db() as db:
            engine1 = DecisionEngine(db)
            await engine1._get_protocol_metrics_cached("appA", "openvpn", "PK", "AS1", "wifi")
            assert engine1._metrics_cache != {}

            engine2 = DecisionEngine(db)
            assert engine2._metrics_cache == {}, "a new engine instance must not inherit another instance's metrics cache"

    asyncio.run(go())


def test_servers_config_style_loop_scenario(fake_cache):
    """A servers_config-style loop scoring 30 servers x 2 protocols must
    make at most 2 real Redis calls (one per protocol) instead of up to 60."""
    store, calls = fake_cache

    async def go():
        async with make_db() as db:
            engine = DecisionEngine(db)
            for _ in range(30):
                for protocol in ("openvpn", "shadowsocks"):
                    await engine._get_protocol_metrics_cached("appA", protocol, "PK", "AS1", "wifi")

            print(f"Redis get_cache calls for 30 servers x 2 protocols: {calls['get']}")
            assert calls["get"] <= 2, f"expected at most 2 Redis calls (one per protocol), got {calls['get']}"

    asyncio.run(go())
