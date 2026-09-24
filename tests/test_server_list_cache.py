"""
Regression tests for the 2026-09-24 server-list caching fix
(decision_engine.py): _load_servers() must now be shared for a few seconds
across different requests for the same (app_name, server_type), instead of
re-querying the database every single time — while staying byte-identical
to the uncached result within that window, and never serving one app's
list for another's.
"""
import asyncio
from types import SimpleNamespace

import pytest
from sqlalchemy import event

import app.decision_engine as de
from app.decision_engine import DecisionEngine
from app.models import VPNServer, VPNUserSession
from conftest import make_db


@pytest.fixture(autouse=True)
def clear_server_list_cache():
    """The cache is a module-level dict (deliberately shared across
    DecisionEngine instances) — must not leak state between tests."""
    de._server_list_cache.clear()
    yield
    de._server_list_cache.clear()


@pytest.fixture
def frozen_clock(monkeypatch):
    clock = {"t": 1000.0}
    monkeypatch.setattr(de.time, "monotonic", lambda: clock["t"])
    return clock


def _server(name, ip, app="appA", cap=100):
    return VPNServer(name=name, ip_address=ip, app_name=app, server_type="free",
                     max_capacity=cap, is_active=True, cpu_usage=1, ram_usage=1,
                     ping_latency_ms=1, load_score=1)


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
            first = await engine._load_servers("appA", None)
            assert len(first) == 1

            counter, hook = _count_statements(db.get_bind())
            try:
                frozen_clock["t"] += 1.0  # still within the 10s TTL
                second = await engine._load_servers("appA", None)
            finally:
                event.remove(db.get_bind(), "before_cursor_execute", hook)

            assert counter["n"] == 0, "a call within the TTL window must not query the database at all"
            assert [s["server"].id for s in second] == [s["server"].id for s in first]

    asyncio.run(go())


def test_call_after_ttl_expires_hits_the_database_again_and_stays_correct(frozen_clock):
    async def go():
        async with make_db() as db:
            s1 = _server("s1", "1.1.1.1")
            db.add(s1)
            await db.commit()

            engine = DecisionEngine(db)
            first = await engine._load_servers("appA", None)
            assert len(first) == 1

            # A new server appears (e.g. an admin added one) before the TTL expires.
            db.add(_server("s2", "2.2.2.2"))
            await db.commit()

            frozen_clock["t"] += 9.9  # still within the 10s TTL — must NOT see the new server yet
            still_cached = await engine._load_servers("appA", None)
            assert len(still_cached) == 1, "must still be serving the cached (now slightly stale) list"

            frozen_clock["t"] += 0.2  # now past the 10s TTL
            fresh = await engine._load_servers("appA", None)
            assert len(fresh) == 2, "after the TTL expires, the real current list must be fetched"

    asyncio.run(go())


def test_different_apps_are_cached_independently(frozen_clock):
    async def go():
        async with make_db() as db:
            db.add(_server("a", "1.1.1.1", app="appA"))
            db.add(_server("b", "2.2.2.2", app="appB"))
            await db.commit()

            engine = DecisionEngine(db)
            a = await engine._load_servers("appA", None)
            b = await engine._load_servers("appB", None)
            assert [s["server"].name for s in a] == ["a"]
            assert [s["server"].name for s in b] == ["b"]

    asyncio.run(go())


def test_different_server_type_is_cached_independently(frozen_clock):
    async def go():
        async with make_db() as db:
            db.add(VPNServer(name="free1", ip_address="1.1.1.1", app_name="appA", server_type="free",
                             max_capacity=100, is_active=True, cpu_usage=1, ram_usage=1, ping_latency_ms=1, load_score=1))
            db.add(VPNServer(name="prem1", ip_address="2.2.2.2", app_name="appA", server_type="premium",
                             max_capacity=100, is_active=True, cpu_usage=1, ram_usage=1, ping_latency_ms=1, load_score=1))
            await db.commit()

            engine = DecisionEngine(db)
            free = await engine._load_servers("appA", "free")
            prem = await engine._load_servers("appA", "premium")
            both = await engine._load_servers("appA", None)
            assert [s["server"].name for s in free] == ["free1"]
            assert [s["server"].name for s in prem] == ["prem1"]
            assert {s["server"].name for s in both} == {"free1", "prem1"}

    asyncio.run(go())


def test_shared_across_different_engine_instances_ie_different_requests(frozen_clock):
    """The actual point of this fix: a second, unrelated request (a fresh
    DecisionEngine, as public.py creates per request) for the same app must
    reuse the first request's list without hitting the database."""
    async def go():
        async with make_db() as db:
            db.add(_server("s1", "1.1.1.1"))
            await db.commit()

            engine1 = DecisionEngine(db)
            await engine1._load_servers("appA", None)

            engine2 = DecisionEngine(db)
            counter, hook = _count_statements(db.get_bind())
            try:
                result = await engine2._load_servers("appA", None)
            finally:
                event.remove(db.get_bind(), "before_cursor_execute", hook)

            assert counter["n"] == 0
            assert len(result) == 1

    asyncio.run(go())


def test_invalidate_function_clears_the_cache(frozen_clock):
    async def go():
        async with make_db() as db:
            db.add(_server("s1", "1.1.1.1"))
            await db.commit()
            engine = DecisionEngine(db)
            await engine._load_servers("appA", None)
            assert de._server_list_cache != {}

            de.invalidate_server_list_cache()
            assert de._server_list_cache == {}

    asyncio.run(go())


def test_real_admin_machine_update_invalidates_the_server_list_cache(frozen_clock, monkeypatch):
    """Confirms the real admin endpoint (not just a hand-written call to
    invalidate_server_list_cache) triggers this — same real-time capacity
    sync guarantee the client explicitly asked for."""
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
            s.max_capacity = 100
            db.add(s)
            await db.commit()

            engine = DecisionEngine(db)
            before = await engine._load_servers("appA", None)
            assert before[0]["max_capacity"] == 100
            assert de._server_list_cache != {}, "sanity check: something got cached"

            await am.update_machine(
                m.id, am.MachineUpdate(name="m", ip_address="1.1.1.1", server_type="free", max_capacity=1000),
                db, "tok")

            assert de._server_list_cache == {}, "the real update_machine endpoint must invalidate the server-list cache"

            after = await engine._load_servers("appA", None)
            assert after[0]["max_capacity"] == 1000

    asyncio.run(go())


def test_capacity_filtering_still_correct_through_the_cache(frozen_clock):
    async def go():
        async with make_db() as db:
            full = _server("full", "1.1.1.1", cap=1)
            db.add(full)
            await db.flush()
            db.add(VPNUserSession(server_id=full.id, user_id="u1", device_ip="1.1.1.1"))
            db.add(_server("open", "2.2.2.2", cap=1))
            await db.commit()

            engine = DecisionEngine(db)
            result = await engine._load_servers("appA", None)
            assert [s["server"].name for s in result] == ["open"], "the full server must still be excluded"

    asyncio.run(go())
