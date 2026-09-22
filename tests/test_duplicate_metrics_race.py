"""
Regression tests for the 2026-09-22 fix to _get_or_create_metrics()
(decision_engine.py): duplicate ProtocolMetrics rows for the same key no
longer crash the request with MultipleResultsFound.
"""
import asyncio

import pytest

from app.decision_engine import DecisionEngine
from app.models import ProtocolMetrics, VPNServer
from conftest import make_db


@pytest.fixture(autouse=True)
def fake_cache(monkeypatch):
    """process_connection_feedback() touches Redis (settings cache, cooldown,
    metrics-cache invalidation) — stub it out so these tests exercise the DB
    logic under test without needing a real Redis."""
    import app.decision_engine as de
    import app.cache as cache_module
    store = {}

    async def get(k):
        return store.get(k)

    async def put(k, v, ttl=3):
        store[k] = v

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

    fake_redis = FakeRedis()

    async def get_redis():
        return fake_redis

    monkeypatch.setattr(de, "get_cache", get)
    monkeypatch.setattr(de, "set_cache", put)
    monkeypatch.setattr(de, "get_redis", get_redis)
    monkeypatch.setattr(cache_module, "get_redis", get_redis)


def _server(**kw):
    base = dict(name="s", ip_address="1.1.1.1", app_name="appA", server_type="free",
               max_capacity=100, is_active=True)
    base.update(kw)
    return VPNServer(**base)


def test_no_existing_row_still_creates_one_as_before():
    """Unchanged behavior: the common case (no duplicates) must work exactly as before."""
    async def go():
        async with make_db() as db:
            srv = _server()
            db.add(srv)
            await db.commit()
            engine = DecisionEngine(db)
            m = await engine._get_or_create_metrics(srv.id, "appA", "openvpn", "PK", "AS1", "wifi")
            assert m.id is not None
            assert m.server_id == srv.id and m.protocol == "openvpn" and m.country == "PK"

    asyncio.run(go())


def test_single_existing_row_still_used_as_before():
    async def go():
        async with make_db() as db:
            srv = _server()
            db.add(srv)
            await db.flush()
            existing = ProtocolMetrics(server_id=srv.id, protocol="openvpn", country="PK", asn="AS1",
                                       network_type="wifi", success_count=5, failure_count=1,
                                       total_attempts=6, success_rate=5 / 6)
            db.add(existing)
            await db.commit()

            engine = DecisionEngine(db)
            m = await engine._get_or_create_metrics(srv.id, "appA", "openvpn", "PK", "AS1", "wifi")
            assert m.id == existing.id and m.total_attempts == 6

    asyncio.run(go())


def test_duplicate_rows_no_longer_crash_and_pick_the_oldest(capsys):
    """The exact bug from production: two rows exist for the same key. Before the
    fix, scalar_one_or_none() raised MultipleResultsFound here (a 500 in
    /v2/connection_feedback/ and /servers_config/). Now it must return the
    older row deterministically instead of crashing."""
    async def go():
        async with make_db() as db:
            srv = _server()
            db.add(srv)
            await db.flush()
            older = ProtocolMetrics(server_id=srv.id, protocol="shadowsocks", country="IR", asn=None,
                                    network_type=None, success_count=10, failure_count=0,
                                    total_attempts=10, success_rate=1.0)
            db.add(older)
            await db.flush()
            newer = ProtocolMetrics(server_id=srv.id, protocol="shadowsocks", country="IR", asn=None,
                                    network_type=None, success_count=2, failure_count=0,
                                    total_attempts=2, success_rate=1.0)
            db.add(newer)
            await db.commit()
            assert older.id < newer.id

            engine = DecisionEngine(db)
            # Must not raise MultipleResultsFound.
            m = await engine._get_or_create_metrics(srv.id, "appA", "shadowsocks", "IR", None, None)
            assert m.id == older.id, "should deterministically converge on the OLDEST row"

    asyncio.run(go())
    assert "Duplicate protocol_metrics rows" in capsys.readouterr().out


def test_three_duplicate_rows_all_handled():
    async def go():
        async with make_db() as db:
            srv = _server()
            db.add(srv)
            await db.flush()
            ids = []
            for i in range(3):
                r = ProtocolMetrics(server_id=srv.id, protocol="openvpn", country=None, asn=None,
                                    network_type=None, success_count=i, failure_count=0,
                                    total_attempts=max(i, 1), success_rate=1.0)
                db.add(r)
                await db.flush()
                ids.append(r.id)
            await db.commit()

            engine = DecisionEngine(db)
            m = await engine._get_or_create_metrics(srv.id, "appA", "openvpn", None, None, None)
            assert m.id == min(ids)

    asyncio.run(go())


def test_process_connection_feedback_survives_pre_existing_duplicates():
    """End-to-end: the actual caller (process_connection_feedback, used by
    /v2/connection_feedback/) must complete successfully even when duplicates
    already exist for the key it's about to update."""
    async def go():
        async with make_db() as db:
            srv = _server()
            db.add(srv)
            await db.flush()
            for _ in range(2):
                db.add(ProtocolMetrics(server_id=srv.id, protocol="openvpn", country="PK", asn="AS1",
                                       network_type="wifi", success_count=0, failure_count=0,
                                       total_attempts=0, success_rate=0.0))
            await db.commit()

            engine = DecisionEngine(db)
            # Must not raise.
            await engine.process_connection_feedback(
                server_id=srv.id, server_ip=srv.ip_address, app_name="appA",
                country="PK", asn="AS1", network_type="wifi",
                primary_protocol="openvpn", primary_success=True, primary_connect_time_ms=100.0,
                secondary_protocol=None, secondary_success=None, secondary_connect_time_ms=None,
            )

            from sqlalchemy import select
            rows = (await db.execute(select(ProtocolMetrics).where(ProtocolMetrics.server_id == srv.id))).scalars().all()
            # Still 2 rows (dedup/merge is deliberately NOT done here — that's the
            # separate follow-up migration); exactly one of them got the update.
            assert len(rows) == 2
            updated = [r for r in rows if r.total_attempts == 1]
            assert len(updated) == 1 and updated[0].success_count == 1

    asyncio.run(go())
