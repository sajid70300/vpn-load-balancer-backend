"""
Tests for releasing the health-check "skip" cooldown when an admin switches a
server / machine back ON (app/monitor_cooldown.py + the four admin paths).

Background: after the health check marks a machine down it sets the Redis key
`inactive_retry:<ip>:<port>` and skips the machine for ~10 minutes. Before this
change an admin re-enabling the server did not remove that key, so the machine
was not re-verified for up to 10 minutes. Now every "switch ON" path removes it
so the next cycle probes the machine again.
"""
import asyncio

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

import app.monitor_cooldown as mc
import app.tasks as tasks
from app.api import admin_machines as am
from app.api import admin_servers as asrv
from app.database import Base
from app.models import PhysicalMachine, VPNServer, VPNUserSession
from conftest import make_db


# ── fakes ────────────────────────────────────────────────────────────────────

class FakeAsyncRedis:
    def __init__(self, keys=(), fail=False, fail_close=False):
        self.kv = {k: "1" for k in keys}
        self.fail = fail
        self.fail_close = fail_close
        self.closed = False
        self.delete_calls = []

    async def delete(self, *keys):
        self.delete_calls.append(keys)
        if self.fail:
            raise ConnectionError("redis down")
        n = 0
        for k in keys:
            if k in self.kv:
                del self.kv[k]
                n += 1
        return n

    async def aclose(self):
        self.closed = True
        if self.fail_close:
            raise RuntimeError("close failed")


class Recorder:
    """Stands in for clear_inactive_cooldowns inside an API module."""
    def __init__(self):
        self.calls = []

    async def __call__(self, targets):
        self.calls.append(sorted(targets))
        return 0


# ── the helper itself ───────────────────────────────────────────────────────

def test_key_format_matches_what_tasks_writes():
    """The key the helper deletes must be exactly the key tasks.py sets when it
    marks a machine down (otherwise the delete silently does nothing)."""
    from sqlalchemy.pool import StaticPool

    engine = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    class R:
        def __init__(self):
            self.d = {}

        def set(self, k, v, nx=False, ex=None):
            self.d[k] = v
            return True

        def delete(self, *k):
            pass

    redis = R()
    orig = (tasks.redis_client, tasks.get_db_session, tasks.get_openvpn_status)
    try:
        tasks.redis_client = redis
        tasks.get_db_session = lambda: Session()
        tasks.get_openvpn_status = lambda ip, port: None          # machine is down
        with Session() as s:
            row = VPNServer(name="s", ip_address="10.9.8.7", management_port=7599, app_name="a",
                            server_type="free", is_active=True, max_capacity=10)
            s.add(row)
            s.commit()
            ids = [row.id]
        tasks.process_server_group(ids, 600)
    finally:
        tasks.redis_client, tasks.get_db_session, tasks.get_openvpn_status = orig

    assert mc.inactive_retry_key("10.9.8.7", 7599) in redis.d, redis.d


def test_redis_address_matches_the_one_tasks_py_uses():
    """Guard against drift: the delete must hit the same host/port/db the Celery
    job writes to, regardless of CACHE_REDIS_URL."""
    kw = tasks.redis_client.connection_pool.connection_kwargs
    assert (mc.REDIS_HOST, mc.REDIS_PORT, mc.REDIS_DB) == (kw["host"], kw["port"], kw["db"])


def test_helper_deletes_exact_keys_once_and_closes(monkeypatch):
    fake = FakeAsyncRedis(keys=["inactive_retry:1.1.1.1:7505", "inactive_retry:2.2.2.2:7505",
                                "inactive_retry:3.3.3.3:7505"])
    monkeypatch.setattr(mc, "_new_client", lambda host: fake)

    removed = asyncio.run(mc.clear_inactive_cooldowns(
        [("1.1.1.1", 7505), ("1.1.1.1", 7505), ("2.2.2.2", 7505)]))   # duplicate on purpose

    assert removed == 2
    assert list(fake.kv) == ["inactive_retry:3.3.3.3:7505"]        # untouched machine keeps its key
    assert len(fake.delete_calls) == 1, "one round trip for all keys"
    assert fake.closed is True


def test_helper_with_nothing_to_do_never_touches_redis(monkeypatch):
    def boom(host):
        raise AssertionError("no client must be created when there are no targets")
    monkeypatch.setattr(mc, "_new_client", boom)
    assert asyncio.run(mc.clear_inactive_cooldowns([])) == 0
    assert asyncio.run(mc.clear_inactive_cooldowns([("", 7505)])) == 0


def test_helper_swallows_redis_errors(monkeypatch):
    fake = FakeAsyncRedis(fail=True)
    monkeypatch.setattr(mc, "_new_client", lambda host: fake)
    assert asyncio.run(mc.clear_inactive_cooldowns([("1.1.1.1", 7505)])) == 0
    assert fake.closed is True


def test_helper_swallows_connect_and_close_failures(monkeypatch):
    def cannot_connect(host):
        raise ConnectionError("cannot connect")
    monkeypatch.setattr(mc, "_new_client", cannot_connect)
    assert asyncio.run(mc.clear_inactive_cooldowns([("1.1.1.1", 7505)])) == 0

    fake = FakeAsyncRedis(keys=["inactive_retry:1.1.1.1:7505"], fail_close=True)
    monkeypatch.setattr(mc, "_new_client", lambda host: fake)
    assert asyncio.run(mc.clear_inactive_cooldowns([("1.1.1.1", 7505)])) == 1   # close error ignored


def test_helper_falls_back_to_the_second_host(monkeypatch):
    """If the numeric address cannot be reached, the 'localhost' name that
    tasks.py itself uses is tried next, and its result is what counts."""
    tried = []
    good = FakeAsyncRedis(keys=["inactive_retry:1.1.1.1:7505"])

    def per_host(host):
        tried.append(host)
        if host == "127.0.0.1":
            raise ConnectionError("refused")
        return good

    monkeypatch.setattr(mc, "_new_client", per_host)
    assert asyncio.run(mc.clear_inactive_cooldowns([("1.1.1.1", 7505)])) == 1
    assert tried == ["127.0.0.1", "localhost"]
    assert good.kv == {}


def test_helper_stops_at_the_first_host_that_answers(monkeypatch):
    tried = []
    fake = FakeAsyncRedis()

    def per_host(host):
        tried.append(host)
        return fake

    monkeypatch.setattr(mc, "_new_client", per_host)
    assert asyncio.run(mc.clear_inactive_cooldowns([("1.1.1.1", 7505)])) == 0   # nothing to delete is still success
    assert tried == ["127.0.0.1"], "must not hit the second host once the first one answered"


def test_helper_is_capped_when_redis_hangs(monkeypatch):
    """A Redis that accepts the connection but never answers must not stall an
    admin request: the whole operation is bounded by _TOTAL_TIMEOUT_SECONDS."""
    import time

    class Hanging:
        closed = False

        async def delete(self, *keys):
            await asyncio.sleep(30)

        async def aclose(self):
            Hanging.closed = True

    monkeypatch.setattr(mc, "_new_client", lambda host: Hanging())
    monkeypatch.setattr(mc, "_TOTAL_TIMEOUT_SECONDS", 0.3)

    t0 = time.time()
    assert asyncio.run(mc.clear_inactive_cooldowns([("1.1.1.1", 7505)])) == 0
    assert time.time() - t0 < 2.0


# ── admin_servers: per-row toggle / edit ────────────────────────────────────

def _servers_env(monkeypatch, noop_audit):
    noop_audit(asrv)

    async def no_cache(pattern):
        pass

    monkeypatch.setattr(asrv, "delete_cache", no_cache)
    rec = Recorder()
    monkeypatch.setattr(asrv, "clear_inactive_cooldowns", rec)
    return rec


async def _server(db, active=False, port=7505):
    s = VPNServer(name="s", ip_address="5.5.5.5", management_port=port, app_name="a", server_type="free",
                  max_capacity=100, is_active=active, admin_disabled=not active)
    db.add(s)
    await db.commit()
    return s


def test_server_toggle_on_clears_cooldown(monkeypatch, noop_audit):
    rec = _servers_env(monkeypatch, noop_audit)

    async def go():
        async with make_db() as db:
            s = await _server(db, active=False, port=7600)
            out = await asrv.toggle_server_active(s.id, db, "tok")
            assert out["is_active"] is True
            assert rec.calls == [[("5.5.5.5", 7600)]]
    asyncio.run(go())


def test_server_toggle_off_keeps_cooldown(monkeypatch, noop_audit):
    rec = _servers_env(monkeypatch, noop_audit)

    async def go():
        async with make_db() as db:
            s = await _server(db, active=True)
            out = await asrv.toggle_server_active(s.id, db, "tok")
            assert out["is_active"] is False
            assert rec.calls == []
    asyncio.run(go())


@pytest.mark.parametrize("flag, expect_call", [(True, True), (False, False), (None, False)])
def test_server_edit_clears_only_when_switched_on(monkeypatch, noop_audit, flag, expect_call):
    rec = _servers_env(monkeypatch, noop_audit)

    async def go():
        async with make_db() as db:
            s = await _server(db, active=False)
            await asrv.update_server(s.id, asrv.ServerUpdate(name="renamed", is_active=flag), db, "tok")
            assert bool(rec.calls) is expect_call
            if expect_call:
                assert rec.calls == [[("5.5.5.5", 7505)]]
    asyncio.run(go())


def test_server_toggle_end_to_end_removes_only_that_machines_key(monkeypatch, noop_audit):
    """No stubbing of the helper: the real key for this machine is removed and a
    different machine's key is left alone."""
    noop_audit(asrv)

    async def no_cache(pattern):
        pass

    monkeypatch.setattr(asrv, "delete_cache", no_cache)
    fake = FakeAsyncRedis(keys=["inactive_retry:5.5.5.5:7505", "inactive_retry:9.9.9.9:7505"])
    monkeypatch.setattr(mc, "_new_client", lambda host: fake)

    async def go():
        async with make_db() as db:
            s = await _server(db, active=False)
            await asrv.toggle_server_active(s.id, db, "tok")
    asyncio.run(go())

    assert list(fake.kv) == ["inactive_retry:9.9.9.9:7505"]


def test_server_toggle_still_succeeds_when_redis_is_down(monkeypatch, noop_audit):
    noop_audit(asrv)

    async def no_cache(pattern):
        pass

    monkeypatch.setattr(asrv, "delete_cache", no_cache)

    def cannot_connect(host):
        raise ConnectionError("redis is down")
    monkeypatch.setattr(mc, "_new_client", cannot_connect)

    async def go():
        async with make_db() as db:
            s = await _server(db, active=False)
            out = await asrv.toggle_server_active(s.id, db, "tok")
            assert out["is_active"] is True and out["admin_disabled"] is False
            row = (await db.execute(select(VPNServer).where(VPNServer.id == s.id))).scalar_one()
            assert row.is_active is True                       # the change itself was saved
    asyncio.run(go())


# ── admin_machines: machine toggle / edit ───────────────────────────────────

def _machines_env(monkeypatch, noop_audit):
    noop_audit(am)

    async def no_cache(pattern):
        pass

    monkeypatch.setattr(am, "delete_cache", no_cache)
    rec = Recorder()
    monkeypatch.setattr(am, "clear_inactive_cooldowns", rec)
    return rec


async def _machine(db, active=False):
    m = PhysicalMachine(name="m", ip_address="6.6.6.6", server_type="free", max_capacity=100,
                        is_active=active, admin_disabled=not active)
    db.add(m)
    await db.flush()
    rows = [
        VPNServer(name="m", ip_address="6.6.6.6", management_port=7505, app_name=app, server_type="free",
                  max_capacity=100, is_active=active, admin_disabled=not active, physical_machine_id=m.id)
        for app in ("a", "b")
    ]
    db.add_all(rows)
    await db.commit()
    return m, rows


def test_machine_toggle_on_clears_cooldown(monkeypatch, noop_audit):
    rec = _machines_env(monkeypatch, noop_audit)

    async def go():
        async with make_db() as db:
            m, _ = await _machine(db, active=False)
            out = await am.toggle_machine_active(m.id, db, "tok")
            assert out["is_active"] is True
            assert rec.calls == [[("6.6.6.6", 7505), ("6.6.6.6", 7505)]]   # one per linked row (helper de-duplicates)
    asyncio.run(go())


def test_machine_toggle_off_keeps_cooldown(monkeypatch, noop_audit):
    rec = _machines_env(monkeypatch, noop_audit)

    async def go():
        async with make_db() as db:
            m, _ = await _machine(db, active=True)
            out = await am.toggle_machine_active(m.id, db, "tok")
            assert out["is_active"] is False
            assert rec.calls == []
    asyncio.run(go())


@pytest.mark.parametrize("flag, expect_call", [(True, True), (False, False), (None, False)])
def test_machine_edit_clears_only_when_switched_on(monkeypatch, noop_audit, flag, expect_call):
    rec = _machines_env(monkeypatch, noop_audit)

    async def go():
        async with make_db() as db:
            m, _ = await _machine(db, active=False)
            await am.update_machine(m.id, am.MachineUpdate(name="m2", is_active=flag), db, "tok")
            assert bool(rec.calls) is expect_call
    asyncio.run(go())


def test_machine_toggle_end_to_end_removes_the_machines_key(monkeypatch, noop_audit):
    noop_audit(am)

    async def no_cache(pattern):
        pass

    monkeypatch.setattr(am, "delete_cache", no_cache)
    fake = FakeAsyncRedis(keys=["inactive_retry:6.6.6.6:7505", "inactive_retry:9.9.9.9:7505"])
    monkeypatch.setattr(mc, "_new_client", lambda host: fake)

    async def go():
        async with make_db() as db:
            m, _ = await _machine(db, active=False)
            await am.toggle_machine_active(m.id, db, "tok")
    asyncio.run(go())

    assert list(fake.kv) == ["inactive_retry:9.9.9.9:7505"]
