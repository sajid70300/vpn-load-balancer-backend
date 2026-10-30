"""
Machine-level capacity: a physical machine shared by several apps is judged by
its TOTAL users (all apps, both protocols) against PhysicalMachine.max_capacity,
both when choosing a server (decision_engine) and for the "capacity reached"
alert (app/machine_capacity.py + Celery task `machine_capacity_check`).

Before this change every app row compared only ITS OWN sessions with the full
machine capacity, so a machine in 5 apps could carry ~5x its capacity without
any app seeing it as full and without any alert.

These drive the real DecisionEngine / real alert function against in-memory
databases (no mocks of the logic under test).
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import app.decision_engine as de
import app.machine_capacity as mc
import app.tasks as tasks
from app.config import settings
from app.database import Base
from app.decision_engine import DecisionEngine
from app.models import Notification, PhysicalMachine, VPNServer, VPNUserSession
from conftest import make_db


# ── helpers ─────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def fake_cache(monkeypatch):
    """In-memory stand-in for Redis (same approach as the other engine tests)."""
    store = {}

    async def get(k):
        return store.get(k)

    async def put(k, v, ttl=3):
        store[k] = v

    class _Pipe:
        def __init__(self):
            self.q = []

        def exists(self, key):
            self.q.append(key)
            return self

        async def execute(self):
            return [0 for _ in self.q]

    class FakeRedis:
        def pipeline(self):
            return _Pipe()

        async def get(self, k):
            return None

        async def exists(self, k):
            return 0

        async def delete(self, *keys):
            return 0

    fr = FakeRedis()

    async def get_redis():
        return fr

    monkeypatch.setattr(de, "get_cache", get)
    monkeypatch.setattr(de, "set_cache", put)
    monkeypatch.setattr(de, "get_redis", get_redis)
    import app.cache as cache_module
    monkeypatch.setattr(cache_module, "get_redis", get_redis)
    return store


@pytest.fixture(autouse=True)
def default_settings(monkeypatch):
    monkeypatch.setattr(settings, "MACHINE_CAPACITY_ENABLED", True)
    monkeypatch.setattr(settings, "CAPACITY_OVERFLOW_FALLBACK", True)
    monkeypatch.setattr(settings, "CAPACITY_ALERT_REPEAT_MINUTES", 30)


@pytest.fixture
def frozen_clock(monkeypatch):
    clock = {"t": 1000.0}
    monkeypatch.setattr(de.time, "monotonic", lambda: clock["t"])
    return clock


def _machine(ip, cap=300, active=True, name=None):
    return PhysicalMachine(name=name or ip, ip_address=ip, server_type="free",
                           max_capacity=cap, is_active=active)


def _row(m, app, cap=None, active=True, priority=False, cpu=10.0, ram=10.0, ping=10.0, load=5.0):
    return VPNServer(name=m.name, ip_address=m.ip_address, app_name=app, server_type="free",
                     max_capacity=m.max_capacity if cap is None else cap,
                     physical_machine_id=m.id, is_active=active, is_priority_group=priority,
                     cpu_usage=cpu, ram_usage=ram, ping_latency_ms=ping, load_score=load,
                     management_port=7505, ovpn_base64="b64", ss_port=8388, ss_password="p",
                     ss_encryption="aes")


def _users(row, n, protocol="openvpn", tag="u"):
    return [VPNUserSession(server_id=row.id, user_id=f"{tag}{row.id}-{i}", device_ip="1.1.1.1",
                           protocol=protocol) for i in range(n)]


async def _add_machine(db, ip, cap, apps, users_per_app, *, extra_rows=(), **row_kw):
    """Machine with one row per app, each carrying `users_per_app` sessions."""
    m = _machine(ip, cap)
    db.add(m)
    await db.flush()
    rows = []
    for app in apps:
        r = _row(m, app, **row_kw)
        db.add(r)
        await db.flush()
        db.add_all(_users(r, users_per_app))
        rows.append(r)
    return m, rows


def _count_statements(engine):
    counter = {"n": 0}

    def hook(conn, cursor, statement, parameters, context, executemany):
        counter["n"] += 1

    event.listen(engine, "before_cursor_execute", hook)
    return counter, hook


# ═════════════════════════════════════════════════════════════════════════════
#  Routing
# ═════════════════════════════════════════════════════════════════════════════

def test_machine_shared_by_5_apps_is_full_even_though_each_app_sees_100_of_300():
    """The scenario from the bug report: capacity 300, 5 apps x 100 users = 500."""
    async def go():
        async with make_db() as db:
            apps = ["appA", "appB", "appC", "appD", "appE"]
            full, _ = await _add_machine(db, "10.0.0.1", 300, apps, 100)
            ok, _ = await _add_machine(db, "10.0.0.2", 300, ["appA"], 10)
            await db.commit()

            engine = DecisionEngine(db)
            servers, overflow = await engine._load_server_tiers("appA", None)
            assert [s["server"].ip_address for s in servers] == ["10.0.0.2"]
            assert [s["server"].ip_address for s in overflow] == ["10.0.0.1"]
            assert overflow[0]["machine_sessions"] == 500
            assert overflow[0]["machine_capacity"] == 300
            # _load_servers() keeps its old contract: only servers with room
            assert [s["server"].ip_address for s in await engine._load_servers("appA", None)] == ["10.0.0.2"]

    asyncio.run(go())


def test_best_server_avoids_the_full_machine_even_if_it_looks_better_per_app():
    async def go():
        async with make_db() as db:
            # Full machine has the BETTER cpu/ram, and only 100 users per app.
            await _add_machine(db, "10.0.0.1", 300, ["appA", "appB", "appC", "appD", "appE"], 100,
                               cpu=5.0, ram=5.0)
            await _add_machine(db, "10.0.0.2", 300, ["appA"], 10, cpu=40.0, ram=40.0)
            await db.commit()

            decision = await DecisionEngine(db).get_best_server(app_name="appA")
            assert decision.primary_config.ip_address == "10.0.0.2"

    asyncio.run(go())


def test_boundary_total_equal_to_capacity_is_full_one_less_is_not():
    async def go():
        async with make_db() as db:
            await _add_machine(db, "10.0.0.1", 300, ["appA", "appB", "appC"], 100)   # 300/300 -> full
            await _add_machine(db, "10.0.0.2", 300, ["appA", "appB", "appC"], 99)    # 297/300 -> ok
            await db.commit()

            servers, overflow = await DecisionEngine(db)._load_server_tiers("appA", None)
            assert [s["server"].ip_address for s in servers] == ["10.0.0.2"]
            assert [s["server"].ip_address for s in overflow] == ["10.0.0.1"]

    asyncio.run(go())


def test_sessions_on_other_apps_inactive_rows_and_shadowsocks_all_count():
    async def go():
        async with make_db() as db:
            m = _machine("10.0.0.1", 100)
            db.add(m)
            await db.flush()
            a = _row(m, "appA")
            b_inactive = _row(m, "appB", active=False)     # a disabled app's users are still on the machine
            db.add_all([a, b_inactive])
            await db.flush()
            db.add_all(_users(a, 40, protocol="openvpn"))
            db.add_all(_users(b_inactive, 20, protocol="openvpn", tag="x"))
            db.add_all(_users(b_inactive, 40, protocol="shadowsocks", tag="s"))
            await db.commit()

            servers, overflow = await DecisionEngine(db)._load_server_tiers("appA", None)
            assert servers == []
            assert len(overflow) == 1 and overflow[0]["machine_sessions"] == 100
            assert overflow[0]["sessions"] == 40            # the row's own count is untouched

    asyncio.run(go())


def test_rows_without_a_machine_keep_the_old_per_app_behaviour():
    async def go():
        async with make_db() as db:
            ok = VPNServer(name="ok", ip_address="1.1.1.1", app_name="appA", server_type="free",
                           max_capacity=10, is_active=True, cpu_usage=1, ram_usage=1,
                           ping_latency_ms=1, load_score=1)
            full = VPNServer(name="full", ip_address="2.2.2.2", app_name="appA", server_type="free",
                             max_capacity=2, is_active=True, cpu_usage=1, ram_usage=1,
                             ping_latency_ms=1, load_score=1)
            db.add_all([ok, full])
            await db.flush()
            db.add_all(_users(ok, 9))
            db.add_all(_users(full, 2))
            await db.commit()

            servers, overflow = await DecisionEngine(db)._load_server_tiers("appA", None)
            assert [s["server"].ip_address for s in servers] == ["1.1.1.1"]
            assert overflow == []
            assert "machine_sessions" not in servers[0]

    asyncio.run(go())


def test_score_prefers_the_machine_with_fewer_total_users():
    """Same CPU/RAM/ping. M1 looks emptier for appA alone (5 users vs 60) but
    carries far more users in total."""
    async def build(db):
        m1, _ = await _add_machine(db, "10.0.0.1", 350, ["appA"], 5)
        m1b = _row(m1, "appB")
        db.add(m1b)
        await db.flush()
        db.add_all(_users(m1b, 240, tag="b"))
        await _add_machine(db, "10.0.0.2", 350, ["appA"], 60)
        await db.commit()

    async def go():
        async with make_db() as db:
            await build(db)
            decision = await DecisionEngine(db).get_best_server(app_name="appA")
            assert decision.primary_config.ip_address == "10.0.0.2"      # fewer TOTAL users

    asyncio.run(go())

    async def go_off():
        async with make_db() as db:
            await build(db)
            decision = await DecisionEngine(db).get_best_server(app_name="appA")
            assert decision.primary_config.ip_address == "10.0.0.1"      # old behaviour

    de._server_list_cache.clear()
    settings.MACHINE_CAPACITY_ENABLED = False
    try:
        asyncio.run(go_off())
    finally:
        settings.MACHINE_CAPACITY_ENABLED = True


def test_priority_row_on_a_full_machine_no_longer_pins_traffic_to_it():
    async def go():
        async with make_db() as db:
            await _add_machine(db, "10.0.0.1", 100, ["appA", "appB"], 60, priority=True)    # 120/100, priority
            await _add_machine(db, "10.0.0.2", 100, ["appA"], 5, priority=False)
            await db.commit()

            decision = await DecisionEngine(db).get_best_server(app_name="appA")
            assert decision.primary_config.ip_address == "10.0.0.2"

    asyncio.run(go())


def test_response_still_reports_the_rows_own_users_and_capacity():
    async def go():
        async with make_db() as db:
            await _add_machine(db, "10.0.0.1", 300, ["appA", "appB", "appC"], 50)   # 150/300, not full
            await db.commit()

            decision = await DecisionEngine(db).get_best_server(app_name="appA")
            assert decision.current_users == 50          # this app's row, NOT the machine total (150)
            assert decision.max_capacity == 300

    asyncio.run(go())


# ── overflow fallback ───────────────────────────────────────────────────────

def test_when_every_candidate_is_machine_full_the_least_loaded_one_is_served():
    async def go():
        async with make_db() as db:
            await _add_machine(db, "10.0.0.1", 100, ["appA", "appB"], 70)   # 140/100
            await _add_machine(db, "10.0.0.2", 100, ["appA", "appB"], 55)   # 110/100
            await db.commit()

            decision = await DecisionEngine(db).get_best_server(app_name="appA")
            assert decision.primary_config.ip_address == "10.0.0.2"

    asyncio.run(go())


def test_fallback_can_be_switched_off(monkeypatch):
    monkeypatch.setattr(settings, "CAPACITY_OVERFLOW_FALLBACK", False)

    async def go():
        async with make_db() as db:
            await _add_machine(db, "10.0.0.1", 100, ["appA", "appB"], 70)
            await db.commit()
            with pytest.raises(ValueError, match="No active servers available"):
                await DecisionEngine(db).get_best_server(app_name="appA")

    asyncio.run(go())


def test_a_row_full_under_its_own_app_count_is_never_rescued_by_the_fallback():
    async def go():
        async with make_db() as db:
            await _add_machine(db, "10.0.0.1", 100, ["appA"], 100)      # row itself is at 100/100
            await db.commit()
            with pytest.raises(ValueError, match="No active servers available"):
                await DecisionEngine(db).get_best_server(app_name="appA")

    asyncio.run(go())


def test_overflow_is_used_when_the_normal_servers_are_all_in_cooldown_for_this_user():
    async def build(db):
        await _add_machine(db, "10.0.0.1", 100, ["appA", "appB"], 70)    # machine-full (overflow)
        await _add_machine(db, "10.0.0.2", 100, ["appA"], 5)             # normal
        await db.commit()

    def cooldown_blocks_10_0_0_2(engine):
        async def fake(servers, country, asn):
            return [s for s in servers if s["server"].ip_address != "10.0.0.2"]
        engine._filter_out_cooldown_servers = fake

    async def go():
        async with make_db() as db:
            await build(db)
            engine = DecisionEngine(db)
            cooldown_blocks_10_0_0_2(engine)
            decision = await engine.get_best_server(app_name="appA", user_country="PK")
            assert decision.primary_config.ip_address == "10.0.0.1"   # old logic would also have allowed it

    asyncio.run(go())

    async def go_no_fallback():
        async with make_db() as db:
            await build(db)
            engine = DecisionEngine(db)
            cooldown_blocks_10_0_0_2(engine)
            with pytest.raises(ValueError, match="All servers are in cooldown for your region"):
                await engine.get_best_server(app_name="appA", user_country="PK")

    de._server_list_cache.clear()
    settings.CAPACITY_OVERFLOW_FALLBACK = False
    try:
        asyncio.run(go_no_fallback())
    finally:
        settings.CAPACITY_OVERFLOW_FALLBACK = True


# ── kill switch / failure handling / cache ──────────────────────────────────

def test_kill_switch_restores_the_old_behaviour_and_skips_the_extra_query(monkeypatch):
    monkeypatch.setattr(settings, "MACHINE_CAPACITY_ENABLED", False)

    async def go():
        async with make_db() as db:
            await _add_machine(db, "10.0.0.1", 300, ["appA", "appB", "appC", "appD", "appE"], 100)
            await db.commit()

            counter, hook = _count_statements(db.get_bind())
            try:
                servers, overflow = await DecisionEngine(db)._load_server_tiers("appA", None)
            finally:
                event.remove(db.get_bind(), "before_cursor_execute", hook)

            assert [s["server"].ip_address for s in servers] == ["10.0.0.1"]   # old: still eligible
            assert overflow == []
            assert "machine_sessions" not in servers[0]
            assert counter["n"] == 1, "kill switch off must issue exactly the old single query"

    asyncio.run(go())


def test_enabled_adds_exactly_one_extra_query_on_a_cache_miss_and_none_on_a_hit(frozen_clock):
    async def go():
        async with make_db() as db:
            await _add_machine(db, "10.0.0.1", 300, ["appA", "appB"], 10)
            await db.commit()
            engine = DecisionEngine(db)

            counter, hook = _count_statements(db.get_bind())
            try:
                first = await engine._load_server_tiers("appA", None)
                assert counter["n"] == 2                      # main query + one machine-totals query
                frozen_clock["t"] += 1.0                      # inside the 10s TTL
                second = await engine._load_server_tiers("appA", None)
                assert counter["n"] == 2, "a cache hit must not touch the database"
            finally:
                event.remove(db.get_bind(), "before_cursor_execute", hook)
            assert [s["server"].id for s in second[0]] == [s["server"].id for s in first[0]]

    asyncio.run(go())


def test_a_failing_machine_lookup_never_breaks_routing(monkeypatch):
    async def boom(db, ids):
        raise RuntimeError("db hiccup")

    monkeypatch.setattr(de, "load_machine_loads", boom)

    async def go():
        async with make_db() as db:
            await _add_machine(db, "10.0.0.1", 300, ["appA", "appB", "appC", "appD", "appE"], 100)
            await db.commit()

            decision = await DecisionEngine(db).get_best_server(app_name="appA")
            assert decision.primary_config.ip_address == "10.0.0.1"       # per-app fallback this cycle
            assert (await db.execute(select(PhysicalMachine.id))).all()   # session still usable

    asyncio.run(go())


def test_an_old_two_element_cache_entry_is_still_understood(frozen_clock):
    async def go():
        async with make_db() as db:
            await _add_machine(db, "10.0.0.1", 300, ["appA"], 10)
            await db.commit()
            engine = DecisionEngine(db)
            real = (await engine._load_server_tiers("appA", None))[0]
            de._server_list_cache[("appA", None)] = (frozen_clock["t"] + 5, real)       # 2-tuple
            servers, overflow = await engine._load_server_tiers("appA", None)
            assert servers == real and overflow == []

    asyncio.run(go())


def test_load_score_without_machine_data_is_the_original_formula():
    engine = DecisionEngine(None)
    srv = {"cpu_usage": 20.0, "ram_usage": 30.0, "sessions": 50, "max_capacity": 100, "ping_ms": 30.0}
    expected = round((80.0 * 0.35) + (70.0 * 0.30) + (50.0 * 0.25) + ((100.0 - (30 / 50) * 20.0) * 0.10), 4)
    assert engine._server_load_score(srv) == expected
    with_machine = dict(srv, machine_sessions=50, machine_capacity=100)
    assert engine._server_load_score(with_machine) == expected      # same ratio -> same score


# ═════════════════════════════════════════════════════════════════════════════
#  Alert
# ═════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def sdb():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as s:
        s.Session = Session
        yield s
    engine.dispose()


def _seed_machine(s, ip, cap, apps, users_per_app, active=True, name=None):
    m = _machine(ip, cap, active=active, name=name)
    s.add(m)
    s.flush()
    for app in apps:
        r = _row(m, app)
        s.add(r)
        s.flush()
        s.add_all(_users(r, users_per_app))
    s.commit()
    return m


def _alerts(s):
    return s.execute(select(Notification).where(Notification.type == "capacity_reached")
                     .order_by(Notification.id)).scalars().all()


def test_alert_fires_for_a_machine_whose_total_across_apps_reaches_capacity(sdb):
    _seed_machine(sdb, "10.0.0.1", 300, ["appA", "appB", "appC", "appD", "appE"], 100, name="Canada")
    assert mc.check_machine_capacity_alerts(sdb) == 1
    (n,) = _alerts(sdb)
    assert n.server_ip == "10.0.0.1" and n.server_name == "Canada"
    assert n.server_id is None and n.app_name is None and n.is_read is False
    assert "300" in n.message and "500" in n.message and "5 app" in n.message


def test_alert_threshold_is_total_greater_or_equal_capacity(sdb):
    _seed_machine(sdb, "10.0.0.1", 300, ["appA", "appB", "appC"], 99)     # 297 < 300
    assert mc.check_machine_capacity_alerts(sdb) == 0
    _seed_machine(sdb, "10.0.0.2", 300, ["appA", "appB", "appC"], 100)    # == 300
    assert mc.check_machine_capacity_alerts(sdb) == 1
    assert [n.server_ip for n in _alerts(sdb)] == ["10.0.0.2"]


def test_alert_ignores_inactive_machines_and_zero_capacity(sdb):
    _seed_machine(sdb, "10.0.0.1", 100, ["appA", "appB"], 80, active=False)
    _seed_machine(sdb, "10.0.0.2", 0, ["appA"], 5)
    assert mc.check_machine_capacity_alerts(sdb) == 0


def test_alert_only_for_the_full_machine(sdb):
    _seed_machine(sdb, "10.0.0.1", 100, ["appA", "appB"], 60)
    _seed_machine(sdb, "10.0.0.2", 100, ["appA", "appB"], 10)
    assert mc.check_machine_capacity_alerts(sdb) == 1
    assert [n.server_ip for n in _alerts(sdb)] == ["10.0.0.1"]


def test_alert_does_not_repeat_while_unread_or_inside_the_repeat_window(sdb):
    _seed_machine(sdb, "10.0.0.1", 100, ["appA", "appB"], 60)
    now = datetime.now(timezone.utc)

    assert mc.check_machine_capacity_alerts(sdb, now) == 1
    assert mc.check_machine_capacity_alerts(sdb, now + timedelta(seconds=60)) == 0     # unread exists

    for n in _alerts(sdb):                                    # an admin opens the bell: all read
        n.is_read = True
    sdb.commit()
    assert mc.check_machine_capacity_alerts(sdb, now + timedelta(minutes=10)) == 0     # inside 30 min
    assert mc.check_machine_capacity_alerts(sdb, now + timedelta(minutes=31)) == 1     # window passed, still full
    assert len(_alerts(sdb)) == 2


def test_unread_alert_blocks_even_after_the_window(sdb):
    _seed_machine(sdb, "10.0.0.1", 100, ["appA", "appB"], 60)
    now = datetime.now(timezone.utc)
    assert mc.check_machine_capacity_alerts(sdb, now) == 1
    assert mc.check_machine_capacity_alerts(sdb, now + timedelta(hours=5)) == 0


def test_alert_stops_when_the_machine_is_no_longer_full(sdb):
    m = _seed_machine(sdb, "10.0.0.1", 100, ["appA", "appB"], 60)
    now = datetime.now(timezone.utc)
    assert mc.check_machine_capacity_alerts(sdb, now) == 1
    sdb.query(VPNUserSession).delete()
    sdb.commit()
    for n in _alerts(sdb):
        n.is_read = True
    sdb.commit()
    assert mc.check_machine_capacity_alerts(sdb, now + timedelta(hours=2)) == 0


def test_alert_kill_switch(sdb, monkeypatch):
    _seed_machine(sdb, "10.0.0.1", 100, ["appA", "appB"], 60)
    monkeypatch.setattr(settings, "MACHINE_CAPACITY_ENABLED", False)
    assert mc.check_machine_capacity_alerts(sdb) == 0
    assert mc.run_machine_capacity_check() == 0
    assert _alerts(sdb) == []


def test_celery_entry_point_creates_the_alert_and_never_raises(sdb, monkeypatch):
    _seed_machine(sdb, "10.0.0.1", 100, ["appA", "appB"], 60)
    import app.database as database
    monkeypatch.setattr(database, "SyncSessionLocal", sdb.Session)
    assert mc.run_machine_capacity_check() == 1

    def broken():
        raise RuntimeError("database is down")

    monkeypatch.setattr(database, "SyncSessionLocal", broken)
    assert mc.run_machine_capacity_check() == 0           # swallowed, no exception


def test_celery_task_is_registered_and_scheduled_and_safe_to_call_without_a_database():
    import celery_app as ca
    assert "machine_capacity_check" in ca.celery_app.tasks
    entry = ca.celery_app.conf.beat_schedule["machine-capacity-check-every-60-seconds"]
    assert entry["task"] == "machine_capacity_check" and entry["schedule"] == 60.0
    assert entry["options"]["expires"] == 55
    # The test environment points at an unreachable database: must not raise.
    ca.celery_app.tasks["machine_capacity_check"]()


# ── the old per-app alert is replaced (not duplicated) ──────────────────────

def _fetch_metrics_env(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 't.db'}", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    class FakeRedis:
        def delete(self, *k):
            return 0

        def set(self, *a, **k):
            return True

    class Resp:
        status_code = 200

        def json(self):
            return {"cpu_percent": 10.0, "ram_percent": 20.0, "ping_ms": 5.0}

    monkeypatch.setattr(tasks, "get_db_session", lambda: Session())
    monkeypatch.setattr(tasks, "redis_client", FakeRedis())
    monkeypatch.setattr(tasks.requests, "get", lambda *a, **k: Resp())

    with Session() as s:
        m = _machine("10.0.0.1", 2)
        s.add(m)
        s.flush()
        row = _row(m, "appA", cap=2)                      # the row itself is full: 3 of 2
        row.monitoring_api_url = "http://10.0.0.1:9100"
        s.add(row)
        s.flush()
        s.add_all(_users(row, 3))
        s.commit()
        rid = row.id
    return engine, Session, rid


def test_old_per_app_alert_is_off_when_machine_capacity_is_on(tmp_path, monkeypatch):
    engine, Session, rid = _fetch_metrics_env(tmp_path, monkeypatch)
    tasks.fetch_server_metrics(rid)
    with Session() as s:
        assert s.query(Notification).filter_by(type="capacity_reached").count() == 0
        assert s.get(VPNServer, rid).cpu_usage == 10.0          # the rest of the task still works
    engine.dispose()


def test_old_per_app_alert_is_unchanged_with_the_kill_switch_off(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "MACHINE_CAPACITY_ENABLED", False)
    engine, Session, rid = _fetch_metrics_env(tmp_path, monkeypatch)
    tasks.fetch_server_metrics(rid)
    with Session() as s:
        assert s.query(Notification).filter_by(type="capacity_reached").count() == 1
    engine.dispose()


# ═════════════════════════════════════════════════════════════════════════════
#  The real /v2/best_server/ route end to end (real DecisionEngine, in-memory DB)
# ═════════════════════════════════════════════════════════════════════════════

def _call_route(monkeypatch, db, app_name="appA"):
    from fastapi import BackgroundTasks
    from app.api import public

    async def get_cache(k):
        return None

    async def set_cache(k, v, ttl=3):
        pass

    monkeypatch.setattr(public, "get_cache", get_cache)
    monkeypatch.setattr(public, "set_cache", set_cache)
    return public.best_server_v2(background_tasks=BackgroundTasks(), app_name=app_name, server_type=None,
                                 country=None, asn=None, network_type=None, db=db, _="t")


def test_route_serves_the_non_full_machine_and_reports_the_rows_own_numbers(monkeypatch):
    async def go():
        async with make_db() as db:
            await _add_machine(db, "10.0.0.1", 300, ["appA", "appB", "appC", "appD", "appE"], 100)   # 500/300
            await _add_machine(db, "10.0.0.2", 300, ["appA"], 30, cpu=50.0, ram=50.0)
            await db.commit()
            decision = await _call_route(monkeypatch, db)
            assert decision.primary_config.ip_address == "10.0.0.2"
            assert (decision.current_users, decision.max_capacity) == (30, 300)

    asyncio.run(go())


def test_route_still_answers_when_only_the_machine_rule_says_full(monkeypatch):
    async def go():
        async with make_db() as db:
            await _add_machine(db, "10.0.0.1", 100, ["appA", "appB"], 70)
            await db.commit()
            decision = await _call_route(monkeypatch, db)
            assert decision.primary_config.ip_address == "10.0.0.1"      # fallback, not a 503

    asyncio.run(go())


def test_route_returns_503_exactly_as_before_when_the_apps_own_row_is_full(monkeypatch):
    from fastapi import HTTPException

    async def go():
        async with make_db() as db:
            await _add_machine(db, "10.0.0.1", 100, ["appA"], 100)
            await db.commit()
            with pytest.raises(HTTPException) as e:
                await _call_route(monkeypatch, db)
            assert e.value.status_code == 503 and e.value.detail == "No active servers available"

    asyncio.run(go())


def test_route_with_the_kill_switch_off_behaves_exactly_like_the_old_code(monkeypatch):
    monkeypatch.setattr(settings, "MACHINE_CAPACITY_ENABLED", False)

    async def go():
        async with make_db() as db:
            await _add_machine(db, "10.0.0.1", 300, ["appA", "appB", "appC", "appD", "appE"], 100,
                               cpu=5.0, ram=5.0)                                             # 500/300 but looks fine per app
            await _add_machine(db, "10.0.0.2", 300, ["appA"], 30, cpu=50.0, ram=50.0)
            await db.commit()
            decision = await _call_route(monkeypatch, db)
            assert decision.primary_config.ip_address == "10.0.0.1"                         # the old (buggy) choice

    asyncio.run(go())
