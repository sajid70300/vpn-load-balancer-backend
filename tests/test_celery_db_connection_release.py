"""
Regression tests for the 2026-10-14 fix to app/tasks.py: Celery monitoring
tasks must NOT hold a database connection (an open, idle transaction) while
they wait on the network.

Why it matters: the production PostgreSQL role has
idle_in_transaction_session_timeout = 30s. A task that kept its session open
while probing an unreachable VPN machine (20-60+ s of retries) had the
connection killed, and its next query failed with
"SSL connection has been closed unexpectedly".

These tests use a real file-based SQLite database with a real connection pool
and read engine.pool.checkedout() at the exact moment the (faked) slow network
call runs, so "no connection is held during the wait" is measured, not assumed.
"""
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.tasks as tasks
from app.database import Base
from app.models import VPNServer, VPNUserSession


class FakeRedis:
    def __init__(self):
        self.d = {}

    def llen(self, k):
        return 0

    def set(self, k, v, nx=False, ex=None):
        if nx and k in self.d:
            return None
        self.d[k] = v
        return True

    def get(self, k):
        return self.d.get(k)

    def exists(self, k):
        return 1 if k in self.d else 0

    def ttl(self, k):
        return 10

    def delete(self, *keys):
        for k in keys:
            self.d.pop(k, None)


LIVE_RESPONSE = """OpenVPN CLIENT LIST
Updated,2026-10-14 13:40:00
Common Name,Real Address,Bytes Received,Bytes Sent,Connected Since
alice,udp4:1.2.3.4:5555,100,200,2026-10-14 13:00:00
bob,udp4:1.2.3.5:5556,300,400,2026-10-14 13:01:00
ROUTING TABLE
Virtual Address,Common Name,Real Address,Last Ref
10.8.0.2,alice,udp4:1.2.3.4:5555,2026-10-14 13:39:00
10.8.0.3,bob,udp4:1.2.3.5:5556,2026-10-14 13:39:00
GLOBAL STATS
Max bcast/mcast queue length,0
END
"""


@pytest.fixture
def env(tmp_path, monkeypatch):
    # File-based SQLite => a real QueuePool whose checkedout() is meaningful.
    engine = create_engine(f"sqlite:///{tmp_path / 't.db'}", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    redis = FakeRedis()
    monkeypatch.setattr(tasks, "redis_client", redis)
    monkeypatch.setattr(tasks, "get_db_session", lambda: Session())

    class Env:
        pass

    e = Env()
    e.engine, e.Session, e.redis = engine, Session, redis

    def seed(ip="10.0.0.1", rows=3, url="http://10.0.0.1:9100"):
        with Session() as s:
            ids = []
            for i in range(rows):
                row = VPNServer(name=f"srv{i}", ip_address=ip, management_port=7505, app_name=f"app{i}",
                                server_type="free", is_active=True, monitoring_api_url=url,
                                max_capacity=100, peak_users=0)
                s.add(row)
                s.flush()
                ids.append(row.id)
                s.add(VPNUserSession(server_id=row.id, user_id="stale", device_ip="9.9.9.9", protocol="openvpn"))
                s.add(VPNUserSession(server_id=row.id, user_id="ss", device_ip="8.8.8.8", protocol="shadowsocks"))
            s.commit()
        return ids

    def state():
        with Session() as s:
            rows = s.query(VPNServer).order_by(VPNServer.id).all()
            return {
                "active": [r.is_active for r in rows],
                "ovpn_users": sorted({x.user_id for x in s.query(VPNUserSession).filter_by(protocol="openvpn")}),
                "ovpn_count": s.query(VPNUserSession).filter_by(protocol="openvpn").count(),
                "ss_count": s.query(VPNUserSession).filter_by(protocol="shadowsocks").count(),
            }

    e.seed, e.state = seed, state
    yield e
    engine.dispose()


# ── process_server_group ────────────────────────────────────────────────────

def test_dead_server_no_connection_held_during_probe_and_state_is_correct(env, monkeypatch):
    ids = env.seed()
    held = []

    def dead(ip, port):
        held.append(env.engine.pool.checkedout())
        return None

    monkeypatch.setattr(tasks, "get_openvpn_status", dead)
    tasks.process_server_group(ids, 600)

    assert held == [0], "a DB connection was held open during the slow management-port probe"
    s = env.state()
    assert s["active"] == [False, False, False]
    assert s["ovpn_count"] == 0                       # stale OpenVPN sessions cleared
    assert s["ss_count"] == 3                         # Shadowsocks sessions untouched
    assert env.redis.d.get("inactive_retry:10.0.0.1:7505") == "1"   # skip-cooldown set after the save


def test_live_server_no_connection_held_and_sessions_synced(env, monkeypatch):
    ids = env.seed()
    held = []

    def live(ip, port):
        held.append(env.engine.pool.checkedout())
        return LIVE_RESPONSE

    monkeypatch.setattr(tasks, "get_openvpn_status", live)
    tasks.process_server_group(ids, 600)

    assert held == [0]
    s = env.state()
    assert s["active"] == [True, True, True]
    assert s["ovpn_users"] == ["alice", "bob"]        # stale user gone, real users added
    assert s["ss_count"] == 3
    assert "inactive_retry:10.0.0.1:7505" not in env.redis.d


def test_admin_edit_made_during_the_probe_is_respected(env, monkeypatch):
    """The rows are re-read after the network wait, so an admin disabling a
    server while the probe is in flight is not overwritten with stale data."""
    ids = env.seed(rows=1)

    def live_and_admin_disables(ip, port):
        with env.Session() as s:
            row = s.get(VPNServer, ids[0])
            row.admin_disabled = True
            row.is_active = False
            s.commit()
        return LIVE_RESPONSE

    monkeypatch.setattr(tasks, "get_openvpn_status", live_and_admin_disables)
    tasks.process_server_group(ids, 600)

    with env.Session() as s:
        row = s.get(VPNServer, ids[0])
        assert row.admin_disabled is True
        assert row.is_active is False, "the health check must never re-activate an admin-disabled server"


def test_cooldown_is_not_set_when_saving_inactive_state_fails(env, monkeypatch):
    """Old behaviour: cooldown was set BEFORE the write, so a failed write left
    a dead server marked active AND skipped for 10 minutes. Now the cooldown is
    only set once the inactive state is committed, so the next cycle retries."""
    ids = env.seed()
    monkeypatch.setattr(tasks, "get_openvpn_status", lambda ip, port: None)

    real_get = tasks.get_db_session

    def failing_commit_session():
        s = real_get()
        s.commit = lambda: (_ for _ in ()).throw(RuntimeError("simulated DB failure on commit"))
        return s

    monkeypatch.setattr(tasks, "get_db_session", failing_commit_session)
    tasks.process_server_group(ids, 600)        # logs the error, must not raise

    assert "inactive_retry:10.0.0.1:7505" not in env.redis.d
    assert env.state()["active"] == [True, True, True]      # nothing half-saved


def test_missing_group_returns_quietly(env, monkeypatch):
    called = []
    monkeypatch.setattr(tasks, "get_openvpn_status", lambda ip, port: called.append(1))
    tasks.process_server_group([9999], 600)
    assert called == [], "no probe must happen for rows that do not exist"


# ── monitor_vpn_status ─────────────────────────────────────────────────────

def test_monitor_vpn_status_releases_connection_and_groups_by_machine(env, monkeypatch):
    a = env.seed(ip="10.0.0.1", rows=2)
    b = env.seed(ip="10.0.0.2", rows=1)
    held, groups = [], []

    def fake_group(server_ids, inactive_retry_seconds=600, **kw):
        held.append(env.engine.pool.checkedout())
        groups.append(sorted(server_ids))

    monkeypatch.setattr(tasks, "process_server_group", fake_group)
    tasks.monitor_vpn_status()

    assert sorted(groups) == sorted([sorted(a), sorted(b)])
    assert held == [0, 0], "the outer session stayed open across the per-machine network work"
    assert "monitor_vpn_status_lock" not in env.redis.d     # lock released


def test_monitor_vpn_status_skips_machines_in_cooldown(env, monkeypatch):
    env.seed(ip="10.0.0.1", rows=1)
    env.seed(ip="10.0.0.2", rows=1)
    env.redis.d["inactive_retry:10.0.0.1:7505"] = "1"
    seen = []
    monkeypatch.setattr(tasks, "process_server_group", lambda ids, inactive_retry_seconds=600, **kw: seen.append(ids))
    tasks.monitor_vpn_status()
    assert len(seen) == 1


# ── monitor_all_server_metrics / fetch_server_metrics ───────────────────────

def test_monitor_metrics_releases_connection_and_fetches_each_machine_once(env, monkeypatch):
    env.seed(ip="10.0.0.1", rows=3, url="http://10.0.0.1:9100")      # 3 rows, ONE machine
    env.seed(ip="10.0.0.2", rows=1, url="http://10.0.0.2:9100")
    held, fetched = [], []

    def fake_fetch(server_id):
        held.append(env.engine.pool.checkedout())
        fetched.append(server_id)

    monkeypatch.setattr(tasks, "fetch_server_metrics", fake_fetch)
    tasks.monitor_all_server_metrics()

    assert len(fetched) == 2, "each physical machine (monitoring URL) must be fetched exactly once"
    assert held == [0, 0], "the outer session stayed open across the HTTP calls"
    assert "monitor_metrics_lock" not in env.redis.d


def test_monitor_metrics_skips_urls_in_cooldown(env, monkeypatch):
    env.seed(ip="10.0.0.1", rows=1, url="http://10.0.0.1:9100")
    env.seed(ip="10.0.0.2", rows=1, url="http://10.0.0.2:9100")
    env.redis.d["metrics_retry:http://10.0.0.1:9100"] = "1"
    fetched = []
    monkeypatch.setattr(tasks, "fetch_server_metrics", lambda sid: fetched.append(sid))
    tasks.monitor_all_server_metrics()
    assert len(fetched) == 1


class _Resp:
    def __init__(self, code=200, data=None):
        self.status_code = code
        self._data = data or {}

    def json(self):
        return self._data


def test_fetch_server_metrics_no_connection_held_during_http_and_applies_to_all_rows(env, monkeypatch):
    ids = env.seed(ip="10.0.0.1", rows=3, url="http://10.0.0.1:9100")
    held = []

    def fake_get(url, timeout=None):
        held.append(env.engine.pool.checkedout())
        return _Resp(200, {"cpu_percent": 12.5, "ram_percent": 33.0, "ping_ms": 4.0})

    monkeypatch.setattr(tasks.requests, "get", fake_get)
    env.redis.d["metrics_retry:http://10.0.0.1:9100"] = "1"    # stale cooldown must be cleared on success
    tasks.fetch_server_metrics(ids[0])

    assert held == [0], "a DB connection was held open during the monitoring-API HTTP call"
    with env.Session() as s:
        rows = s.query(VPNServer).order_by(VPNServer.id).all()
        assert [r.cpu_usage for r in rows] == [12.5, 12.5, 12.5]      # applied to every sibling row
        assert [r.ram_usage for r in rows] == [33.0, 33.0, 33.0]
        assert all(r.load_score > 0 for r in rows)
    assert "metrics_retry:http://10.0.0.1:9100" not in env.redis.d


def test_fetch_server_metrics_failure_sets_cooldown_and_changes_nothing(env, monkeypatch):
    ids = env.seed(ip="10.0.0.1", rows=1, url="http://10.0.0.1:9100")

    def boom(url, timeout=None):
        raise tasks.requests.exceptions.ConnectTimeout("simulated")

    monkeypatch.setattr(tasks.requests, "get", boom)
    tasks.fetch_server_metrics(ids[0])

    assert env.redis.d.get("metrics_retry:http://10.0.0.1:9100") == "1"
    with env.Session() as s:
        assert s.get(VPNServer, ids[0]).cpu_usage == 0.0


def test_fetch_server_metrics_non_200_sets_cooldown(env, monkeypatch):
    ids = env.seed(ip="10.0.0.1", rows=1, url="http://10.0.0.1:9100")
    monkeypatch.setattr(tasks.requests, "get", lambda url, timeout=None: _Resp(503))
    tasks.fetch_server_metrics(ids[0])
    assert env.redis.d.get("metrics_retry:http://10.0.0.1:9100") == "1"
