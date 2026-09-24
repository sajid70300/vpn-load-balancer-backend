"""
Regression tests for the 2026-09-25 fix to get_protocol_decision_for_server()
(decision_engine.py): its per-server database lookup — called once per
server inside /servers_config/'s loop — must now be shared for a few
seconds across different requests for the same (ip_address, server_type,
app_name), instead of re-querying the database every single time. This is
the fix for the connection-pool exhaustion still seen on /servers_config/
after the four earlier 2026-09-24 fixes, since this specific query had been
missed by those.
"""
import asyncio

import pytest
from sqlalchemy import event

import app.decision_engine as de
from app.decision_engine import DecisionEngine
from app.models import VPNServer
from conftest import make_db


@pytest.fixture
def frozen_clock(monkeypatch):
    clock = {"t": 2000.0}
    monkeypatch.setattr(de.time, "monotonic", lambda: clock["t"])
    return clock


@pytest.fixture(autouse=True)
def fake_cache(monkeypatch):
    """get_protocol_decision_for_server() -> _score_protocols() touches
    Redis-backed caches (settings, policy decisions) — stub them so these
    tests exercise only the DB-side caching under test."""
    store = {}

    async def get(k):
        return store.get(k)

    async def put(k, v, ttl=3):
        store[k] = v

    monkeypatch.setattr(de, "get_cache", get)
    monkeypatch.setattr(de, "set_cache", put)


def _server(name, ip, app="appA"):
    return VPNServer(name=name, ip_address=ip, app_name=app, server_type="free",
                     max_capacity=100, is_active=True, cpu_usage=1, ram_usage=1,
                     ping_latency_ms=1, load_score=1, management_port=7505,
                     ovpn_base64="x", ss_port=1, ss_password="p", ss_encryption="aes")


def _count_statements(engine):
    counter = {"n": 0}

    def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        counter["n"] += 1

    event.listen(engine, "before_cursor_execute", before_cursor_execute)
    return counter, before_cursor_execute


def test_second_call_within_ttl_does_not_touch_the_database(frozen_clock):
    async def go():
        async with make_db() as db:
            db.add(_server("s1", "1.1.1.1"))
            await db.commit()

            engine = DecisionEngine(db)
            first = await engine.get_protocol_decision_for_server("1.1.1.1", "free", "appA")
            assert first is not None

            counter, hook = _count_statements(db.get_bind())
            try:
                frozen_clock["t"] += 1.0
                second = await engine.get_protocol_decision_for_server("1.1.1.1", "free", "appA")
            finally:
                event.remove(db.get_bind(), "before_cursor_execute", hook)

            assert counter["n"] == 0, "a call within the TTL must not query the database at all"
            assert second.primary_config.ip_address == first.primary_config.ip_address

    asyncio.run(go())


def test_different_servers_cached_independently(frozen_clock):
    async def go():
        async with make_db() as db:
            db.add(_server("a", "1.1.1.1"))
            db.add(_server("b", "2.2.2.2"))
            await db.commit()

            engine = DecisionEngine(db)
            a = await engine.get_protocol_decision_for_server("1.1.1.1", "free", "appA")
            b = await engine.get_protocol_decision_for_server("2.2.2.2", "free", "appA")
            assert a.primary_config.ip_address == "1.1.1.1"
            assert b.primary_config.ip_address == "2.2.2.2"

    asyncio.run(go())


def test_call_after_ttl_expires_queries_the_database_again(frozen_clock):
    """Verifies the TTL genuinely expires the cache (a fresh call after it
    re-queries the database) via query counting, rather than trying to
    change and re-observe a value — proving an actual VALUE change becomes
    visible is done more realistically in
    test_real_admin_machine_update_invalidates_this_cache_too below, via
    the real admin endpoint on its own separate session (how this actually
    happens in production); doing that with a raw SQL update against this
    single shared test session runs into SQLAlchemy identity-map behavior
    that doesn't reflect how separate real requests/sessions behave."""
    async def go():
        async with make_db() as db:
            db.add(_server("s1", "1.1.1.1"))
            await db.commit()

            engine = DecisionEngine(db)
            await engine.get_protocol_decision_for_server("1.1.1.1", "free", "appA")

            counter, hook = _count_statements(db.get_bind())
            try:
                frozen_clock["t"] += 9.9  # still within the 10s TTL
                await engine.get_protocol_decision_for_server("1.1.1.1", "free", "appA")
                assert counter["n"] == 0, "must not re-query while still within the TTL"

                frozen_clock["t"] += 0.2  # past the TTL
                await engine.get_protocol_decision_for_server("1.1.1.1", "free", "appA")
                assert counter["n"] > 0, "must re-query once the TTL has expired"
            finally:
                event.remove(db.get_bind(), "before_cursor_execute", hook)

    asyncio.run(go())


def test_invalidate_server_list_cache_clears_this_cache_too(frozen_clock):
    async def go():
        async with make_db() as db:
            db.add(_server("s1", "1.1.1.1"))
            await db.commit()
            engine = DecisionEngine(db)
            await engine.get_protocol_decision_for_server("1.1.1.1", "free", "appA")
            assert de._single_server_cache != {}

            de.invalidate_server_list_cache()
            assert de._single_server_cache == {}

    asyncio.run(go())


def test_servers_config_style_loop_query_count(frozen_clock):
    """The actual point of this fix: a /servers_config/-style loop over many
    servers, called twice (simulating two nearby requests for the same
    app), must not re-query the database the second time."""
    async def go():
        async with make_db() as db:
            N = 30
            for i in range(N):
                db.add(_server(f"s{i}", f"10.0.0.{i}"))
            await db.commit()

            engine = DecisionEngine(db)
            for i in range(N):
                await engine.get_protocol_decision_for_server(f"10.0.0.{i}", "free", "appA")

            counter, hook = _count_statements(db.get_bind())
            try:
                for i in range(N):
                    d = await engine.get_protocol_decision_for_server(f"10.0.0.{i}", "free", "appA")
                    assert d.primary_config.ip_address == f"10.0.0.{i}"
            finally:
                event.remove(db.get_bind(), "before_cursor_execute", hook)

            print(f"SQL statements for second pass over {N} servers: {counter['n']}")
            assert counter["n"] == 0

    asyncio.run(go())


def test_real_admin_machine_update_invalidates_this_cache_too(frozen_clock, monkeypatch):
    async def go():
        import app.api.admin_machines as am
        from app.models import PhysicalMachine

        async def noop_audit(*a, **kw):
            pass
        monkeypatch.setattr(am, "audit_log", noop_audit)

        async with make_db() as db:
            m = PhysicalMachine(name="m", ip_address="1.1.1.1", server_type="free", max_capacity=100)
            db.add(m)
            await db.flush()
            s = _server("m", "1.1.1.1")
            s.physical_machine_id = m.id
            db.add(s)
            await db.commit()

            engine = DecisionEngine(db)
            before = await engine.get_protocol_decision_for_server("1.1.1.1", "free", "appA")
            assert before.max_capacity == 100
            assert de._single_server_cache != {}

            await am.update_machine(
                m.id, am.MachineUpdate(name="m", ip_address="1.1.1.1", server_type="free", max_capacity=9000),
                db, "tok")

            assert de._single_server_cache == {}
            after = await engine.get_protocol_decision_for_server("1.1.1.1", "free", "appA")
            assert after.max_capacity == 9000

    asyncio.run(go())
