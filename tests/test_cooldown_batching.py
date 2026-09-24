"""
Regression tests for the 2026-09-24 cooldown-check batching fix
(decision_engine.py): filtering a server list for active cooldowns must now
happen in ONE Redis round-trip for the whole list, instead of one
round-trip per server, with results identical to calling the original
_server_in_cooldown() once per server.
"""
import asyncio

import pytest

from app.decision_engine import DecisionEngine, _cd_asn_key, _cd_country_key
from app.models import VPNServer
from conftest import make_db


def _server(name, ip):
    return VPNServer(name=name, ip_address=ip, app_name="appA", server_type="free",
                     max_capacity=100, is_active=True)


class FakePipeline:
    """Minimal stand-in for redis.asyncio's Pipeline — records queued
    .exists() calls and answers them all in one .execute() call, counted
    separately from any other Redis traffic."""
    def __init__(self, kv, round_trip_counter):
        self.kv = kv
        self.round_trip_counter = round_trip_counter
        self.queued = []

    def exists(self, key):
        self.queued.append(key)
        return self

    async def execute(self):
        self.round_trip_counter["n"] += 1
        return [1 if k in self.kv else 0 for k in self.queued]


class FakeRedis:
    def __init__(self):
        self.kv = set()
        self.round_trips = {"n": 0}

    async def exists(self, key):
        self.round_trips["n"] += 1
        return 1 if key in self.kv else 0

    def pipeline(self):
        return FakePipeline(self.kv, self.round_trips)


@pytest.fixture
def fake_redis(monkeypatch):
    import app.decision_engine as de
    r = FakeRedis()

    async def get_redis():
        return r

    # get_best_server() also touches the (separate) settings/metrics cache —
    # stub those too so the end-to-end test doesn't need a real Redis.
    store = {}

    async def get_cache(k):
        return store.get(k)

    async def set_cache(k, v, ttl=3):
        store[k] = v

    monkeypatch.setattr(de, "get_redis", get_redis)
    monkeypatch.setattr(de, "get_cache", get_cache)
    monkeypatch.setattr(de, "set_cache", set_cache)
    return r


def test_no_country_returns_all_servers_unchanged(fake_redis):
    async def go():
        async with make_db() as db:
            engine = DecisionEngine(db)
            servers = [{"server": _server("a", "1.1.1.1")}, {"server": _server("b", "2.2.2.2")}]
            result = await engine._filter_out_cooldown_servers(servers, None, "AS1")
            assert result == servers
            assert fake_redis.round_trips["n"] == 0, "no country -> must not touch Redis at all"

    asyncio.run(go())


def test_empty_server_list_returns_empty(fake_redis):
    async def go():
        async with make_db() as db:
            engine = DecisionEngine(db)
            result = await engine._filter_out_cooldown_servers([], "PK", "AS1")
            assert result == []

    asyncio.run(go())


def test_country_level_cooldown_excludes_server_matching_old_behavior(fake_redis):
    async def go():
        async with make_db() as db:
            fake_redis.kv.add(_cd_country_key("1.1.1.1", "PK"))
            engine = DecisionEngine(db)
            servers = [
                {"server": _server("blocked", "1.1.1.1")},
                {"server": _server("ok", "2.2.2.2")},
            ]
            result = await engine._filter_out_cooldown_servers(servers, "PK", None)
            assert [s["server"].name for s in result] == ["ok"]

            # Cross-check against the ORIGINAL single-server method for the same inputs.
            assert await engine._server_in_cooldown("1.1.1.1", "PK", None) is True
            assert await engine._server_in_cooldown("2.2.2.2", "PK", None) is False

    asyncio.run(go())


def test_asn_level_cooldown_excludes_server_matching_old_behavior(fake_redis):
    async def go():
        async with make_db() as db:
            fake_redis.kv.add(_cd_asn_key("1.1.1.1", "PK", "AS1"))
            engine = DecisionEngine(db)
            servers = [
                {"server": _server("blocked", "1.1.1.1")},
                {"server": _server("ok", "2.2.2.2")},
            ]
            result = await engine._filter_out_cooldown_servers(servers, "PK", "AS1")
            assert [s["server"].name for s in result] == ["ok"]

            assert await engine._server_in_cooldown("1.1.1.1", "PK", "AS1") is True
            assert await engine._server_in_cooldown("2.2.2.2", "PK", "AS1") is False

    asyncio.run(go())


def test_asn_cooldown_does_not_affect_a_different_asn(fake_redis):
    """The batched version must still distinguish per-ASN, not just per-server."""
    async def go():
        async with make_db() as db:
            fake_redis.kv.add(_cd_asn_key("1.1.1.1", "PK", "AS1"))
            engine = DecisionEngine(db)
            servers = [{"server": _server("s", "1.1.1.1")}]
            # Same server, different ASN — must NOT be filtered out.
            result = await engine._filter_out_cooldown_servers(servers, "PK", "AS999")
            assert len(result) == 1

    asyncio.run(go())


def test_mixed_servers_some_in_cooldown_some_not(fake_redis):
    async def go():
        async with make_db() as db:
            fake_redis.kv.add(_cd_country_key("2.2.2.2", "IR"))
            fake_redis.kv.add(_cd_asn_key("4.4.4.4", "IR", "AS9"))
            engine = DecisionEngine(db)
            servers = [
                {"server": _server("free1", "1.1.1.1")},
                {"server": _server("country-blocked", "2.2.2.2")},
                {"server": _server("free2", "3.3.3.3")},
                {"server": _server("asn-blocked", "4.4.4.4")},
            ]
            result = await engine._filter_out_cooldown_servers(servers, "IR", "AS9")
            assert [s["server"].name for s in result] == ["free1", "free2"]

    asyncio.run(go())


def test_uses_one_round_trip_regardless_of_server_count(fake_redis):
    """The actual point of this fix."""
    async def go():
        async with make_db() as db:
            engine = DecisionEngine(db)
            servers = [{"server": _server(f"s{i}", f"10.0.0.{i}")} for i in range(50)]
            result = await engine._filter_out_cooldown_servers(servers, "PK", "AS1")
            assert len(result) == 50, "none of these should be in cooldown"
            print(f"Redis round-trips for 50 servers: pipeline={fake_redis.round_trips['n']}")
            assert fake_redis.round_trips["n"] == 1, (
                f"expected exactly 1 round-trip (one pipeline execute) for 50 servers, "
                f"got {fake_redis.round_trips['n']}"
            )

    asyncio.run(go())


def test_batched_vs_old_sequential_round_trip_count_side_by_side(fake_redis):
    """Direct proof the round-trip counting itself is sound, by calling BOTH
    the untouched old method (per-server) and the new batched method on the
    same fake Redis in the same test — not a git-revert (this is a new
    method, not a modified one, so reverting would just delete it rather
    than exercise a meaningful 'old' code path)."""
    async def go():
        async with make_db() as db:
            engine = DecisionEngine(db)
            servers = [{"server": _server(f"s{i}", f"10.0.0.{i}")} for i in range(20)]

            fake_redis.round_trips["n"] = 0
            for srv in servers:
                await engine._server_in_cooldown(srv["server"].ip_address, "PK", "AS1")
            old_count = fake_redis.round_trips["n"]

            fake_redis.round_trips["n"] = 0
            await engine._filter_out_cooldown_servers(servers, "PK", "AS1")
            new_count = fake_redis.round_trips["n"]

            print(f"old (per-server) round-trips: {old_count}, new (batched) round-trips: {new_count}")
            assert old_count == 40, "sanity check: old method does 2 Redis calls per server (country + asn)"
            assert new_count == 1
            assert new_count < old_count

    asyncio.run(go())


def test_get_best_server_end_to_end_still_skips_cooldown_servers(fake_redis):
    """Full integration: get_best_server() must still correctly avoid a
    server that's in cooldown, using the new batched filter internally."""
    async def go():
        async with make_db() as db:
            db.add(_server("blocked", "1.1.1.1"))
            db.add_all([
                VPNServer(name="ok", ip_address="2.2.2.2", app_name="appA", server_type="free",
                         max_capacity=100, is_active=True, cpu_usage=5, ram_usage=5,
                         ping_latency_ms=10, load_score=1, management_port=7505,
                         ovpn_base64="x", ss_port=1, ss_password="p", ss_encryption="aes"),
            ])
            await db.commit()
            fake_redis.kv.add(_cd_country_key("1.1.1.1", "PK"))

            engine = DecisionEngine(db)
            decision = await engine.get_best_server(app_name="appA", user_country="PK")
            assert decision.primary_config.ip_address == "2.2.2.2"

    asyncio.run(go())
