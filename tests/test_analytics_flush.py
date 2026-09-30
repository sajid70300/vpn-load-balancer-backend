"""Tests for the analytics Celery tasks (app/analytics_tasks.py): flush, rollup, usage snapshots, retention."""
import fnmatch
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import app.analytics_tasks as at
from app.analytics import BUCKET_SECONDS, bucket_key, field_name
from app.config import settings
from app.database import Base
from app.models import (
    ServerTraffic5m, ServerTrafficHourly, ServerUsage5m, VPNServer, VPNUserSession,
)

H = 3600
T0 = 1_800_000_000 - (1_800_000_000 % H)      # an exact hour boundary (UTC epoch seconds)


class FakeSyncRedis:
    """The subset of redis-py that the flush/cleanup tasks use (strings + hashes + scan)."""

    def __init__(self):
        self.d = {}

    def set(self, k, v, nx=False, ex=None):
        if nx and k in self.d:
            return None
        self.d[k] = v
        return True

    def get(self, k):
        v = self.d.get(k)
        return v if isinstance(v, str) else None

    def exists(self, k):
        return 1 if k in self.d else 0

    def delete(self, *ks):
        for k in ks:
            self.d.pop(k, None)

    def hgetall(self, k):
        v = self.d.get(k)
        return dict(v) if isinstance(v, dict) else {}

    def hincr(self, k, f, n=1):                 # test helper: what the API's HINCRBY does
        self.d.setdefault(k, {})
        self.d[k][f] = str(int(self.d[k].get(f, "0")) + n)

    def scan_iter(self, match=None, count=None):
        return iter([k for k in list(self.d) if fnmatch.fnmatch(k, match or "*")])


@pytest.fixture
def env(monkeypatch):
    engine = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    r = FakeSyncRedis()
    monkeypatch.setattr(at, "_redis", lambda: r)
    monkeypatch.setattr(at, "_session", lambda: Session())
    monkeypatch.setattr(settings, "ANALYTICS_ENABLED", True)

    class Env:
        redis = r

        def bump(self, bucket_epoch, server_id, country, protocol, metric, n=1):
            r.hincr(bucket_key(bucket_epoch), field_name(server_id, country, protocol, metric), n)

        def rows5m(self):
            with Session() as s:
                return {
                    (int(x.bucket_start.replace(tzinfo=timezone.utc).timestamp()), x.server_id, x.country, x.protocol):
                    (x.assigned, x.success, x.failed)
                    for x in s.execute(select(ServerTraffic5m)).scalars()
                }

        def rowsH(self):
            with Session() as s:
                return {
                    (int(x.bucket_start.replace(tzinfo=timezone.utc).timestamp()), x.server_id, x.country, x.protocol):
                    (x.assigned, x.success, x.failed)
                    for x in s.execute(select(ServerTrafficHourly)).scalars()
                }

        def usage(self):
            with Session() as s:
                return {
                    (int(x.bucket_start.replace(tzinfo=timezone.utc).timestamp()), x.server_id):
                    (x.active_sessions, x.openvpn_sessions, x.shadowsocks_sessions, x.max_capacity, x.is_active)
                    for x in s.execute(select(ServerUsage5m)).scalars()
                }

    e = Env()
    e.Session = Session
    return e


# ─── parsing ──────────────────────────────────────────────────────────────────

def test_parse_valid_fields_and_sums_metrics_per_key():
    rows = at.parse_bucket_hash({
        "7|PK|openvpn|asg": "3", "7|PK|openvpn|ok": "2", "7|PK|openvpn|fail": "1",
        "7|US|shadowsocks|ok": "5",
    })
    assert rows == {
        (7, "PK", "openvpn"): {"assigned": 3, "success": 2, "failed": 1},
        (7, "US", "shadowsocks"): {"assigned": 0, "success": 5, "failed": 0},
    }


@pytest.mark.parametrize("field,value", [
    ("garbage", "1"), ("7|PK|openvpn", "1"), ("x|PK|openvpn|ok", "1"), ("0|PK|openvpn|ok", "1"),
    ("-3|PK|openvpn|ok", "1"), ("7|pk|openvpn|ok", "1"), ("7|PKK|openvpn|ok", "1"), ("7|P1|openvpn|ok", "1"),
    ("7|PK|wireguard|ok", "1"), ("7|PK|openvpn|nope", "1"), ("7|PK|openvpn|ok", "abc"), ("7|PK|openvpn|ok", "-1"),
    ("7|PK|openvpn|ok|extra", "1"),
])
def test_parse_skips_malformed_fields_without_raising(field, value):
    assert at.parse_bucket_hash({field: value}) == {}


# ─── flush: correctness ───────────────────────────────────────────────────────

def test_flush_writes_5m_rows_and_hourly_rollup(env):
    b = T0 + 10 * BUCKET_SECONDS                     # inside hour T0
    env.bump(b, 7, "PK", "openvpn", "asg", 4)
    env.bump(b, 7, "PK", "openvpn", "ok", 3)
    env.bump(b, 7, "PK", "openvpn", "fail", 1)
    env.bump(b, 7, "US", "shadowsocks", "ok", 2)

    result = at.flush_analytics(now=b + BUCKET_SECONDS + 10)
    assert result["buckets"] == 1 and result["rows"] == 2       # 2 (country, protocol) rows in the bucket

    assert env.rows5m() == {
        (b, 7, "PK", "openvpn"): (4, 3, 1),
        (b, 7, "US", "shadowsocks"): (0, 2, 0),
    }
    assert env.rowsH() == {
        (T0, 7, "PK", "openvpn"): (4, 3, 1),
        (T0, 7, "US", "shadowsocks"): (0, 2, 0),
    }


def test_hourly_sums_all_buckets_of_the_hour_and_separates_hours(env):
    for i in range(3):
        env.bump(T0 + i * BUCKET_SECONDS, 7, "PK", "openvpn", "ok", 10)        # 3 buckets in hour 0
    env.bump(T0 + H + 5 * BUCKET_SECONDS, 7, "PK", "openvpn", "ok", 7)         # hour 1
    at.flush_analytics(now=T0 + 2 * H)
    assert env.rowsH() == {
        (T0, 7, "PK", "openvpn"): (0, 30, 0),
        (T0 + H, 7, "PK", "openvpn"): (0, 7, 0),
    }


def test_reflushing_the_same_bucket_never_double_counts(env):
    b = T0
    env.bump(b, 7, "PK", "openvpn", "ok", 5)
    now = b + 100                                    # bucket still OPEN
    for _ in range(5):
        at.flush_analytics(now=now)
    assert env.rows5m() == {(b, 7, "PK", "openvpn"): (0, 5, 0)}
    assert env.rowsH() == {(T0, 7, "PK", "openvpn"): (0, 5, 0)}


def test_open_bucket_updates_as_new_events_arrive_without_duplicates(env):
    b = T0
    env.bump(b, 7, "PK", "openvpn", "ok", 5)
    at.flush_analytics(now=b + 60)
    env.bump(b, 7, "PK", "openvpn", "ok", 2)
    env.bump(b, 7, "PK", "openvpn", "fail", 1)
    at.flush_analytics(now=b + 120)
    assert env.rows5m() == {(b, 7, "PK", "openvpn"): (0, 7, 1)}
    assert env.rowsH() == {(T0, 7, "PK", "openvpn"): (0, 7, 1)}


def test_unchanged_bucket_is_not_rewritten(env, monkeypatch):
    env.bump(T0, 7, "PK", "openvpn", "ok", 5)
    at.flush_analytics(now=T0 + 60)
    calls = []
    real = at._upsert_traffic_rows
    monkeypatch.setattr(at, "_upsert_traffic_rows", lambda *a, **k: (calls.append(1), real(*a, **k)))
    at.flush_analytics(now=T0 + 120)
    assert calls == []                               # same total -> skipped (no write amplification)


def test_late_stragglers_inside_the_grace_period_are_still_counted(env):
    b = T0
    env.bump(b, 7, "PK", "openvpn", "ok", 5)
    at.flush_analytics(now=b + BUCKET_SECONDS + 5)                       # bucket just ended, in grace
    assert (b, 7, "PK", "openvpn") in env.rows5m()
    assert env.redis.exists(at._fin_key(b)) == 0                          # NOT finalised yet
    env.bump(b, 7, "PK", "openvpn", "ok", 1)                              # straggler
    at.flush_analytics(now=b + BUCKET_SECONDS + 30)
    assert env.rows5m()[(b, 7, "PK", "openvpn")] == (0, 6, 0)


def test_closed_bucket_is_finalised_and_its_redis_hash_freed(env):
    b = T0
    env.bump(b, 7, "PK", "openvpn", "ok", 5)
    at.flush_analytics(now=b + BUCKET_SECONDS + at.CLOSE_GRACE_SECONDS + 1)
    assert env.redis.exists(at._fin_key(b)) == 1
    assert env.redis.exists(bucket_key(b)) == 0                           # memory freed
    assert env.rows5m() == {(b, 7, "PK", "openvpn"): (0, 5, 0)}
    result = at.flush_analytics(now=b + BUCKET_SECONDS + at.CLOSE_GRACE_SECONDS + 61)
    assert result["buckets"] == 0                                         # nothing left to do


def test_several_buckets_flush_in_one_run(env):
    for i in range(4):
        env.bump(T0 + i * BUCKET_SECONDS, 7, "PK", "openvpn", "ok", 1)
    result = at.flush_analytics(now=T0 + H)
    assert result["buckets"] == 4
    assert len(env.rows5m()) == 4
    assert env.rowsH() == {(T0, 7, "PK", "openvpn"): (0, 4, 0)}


def test_bucket_with_only_malformed_data_is_handled_and_not_retried_forever(env):
    env.redis.d[bucket_key(T0)] = {"garbage": "1", "7|zz|openvpn|ok": "2"}
    at.flush_analytics(now=T0 + 60)
    assert env.rows5m() == {}
    assert env.redis.get(at._sig_key(T0)) == "0"


# ─── flush: failure safety ────────────────────────────────────────────────────

def test_failed_flush_rolls_back_and_the_retry_produces_exact_counts(env, monkeypatch):
    b = T0
    env.bump(b, 7, "PK", "openvpn", "ok", 5)
    env.bump(b, 7, "PK", "openvpn", "fail", 2)

    real = at._recompute_hour
    def boom(*a, **k):
        raise RuntimeError("db hiccup")
    monkeypatch.setattr(at, "_recompute_hour", boom)
    at.flush_analytics(now=b + 60)                   # must not raise
    assert env.rows5m() == {}                        # rolled back — nothing half-written
    assert env.rowsH() == {}
    assert env.redis.get(at._sig_key(b)) is None     # marker NOT set, so it will be retried

    monkeypatch.setattr(at, "_recompute_hour", real)
    at.flush_analytics(now=b + 120)
    assert env.rows5m() == {(b, 7, "PK", "openvpn"): (0, 5, 2)}   # exactly once
    assert env.rowsH() == {(T0, 7, "PK", "openvpn"): (0, 5, 2)}


def test_crash_after_commit_before_markers_recounts_identically(env):
    """Simulates the worst case: DB commit succeeded but the Redis marker write was lost."""
    b = T0
    env.bump(b, 7, "PK", "openvpn", "ok", 5)
    at.flush_analytics(now=b + 60)
    env.redis.delete(at._sig_key(b))                 # marker lost
    at.flush_analytics(now=b + 120)                  # re-flushes the same bucket
    assert env.rows5m() == {(b, 7, "PK", "openvpn"): (0, 5, 0)}      # still 5, not 10


def test_a_late_partial_reflush_can_never_shrink_finalised_data(env):
    """Clock-skewed straggler recreates an already-finalised bucket, its markers have expired:
    the small partial count must NOT overwrite the good stored row (5m or hourly)."""
    b = T0
    env.bump(b, 7, "PK", "openvpn", "ok", 10)
    env.bump(b, 7, "PK", "openvpn", "fail", 4)
    at.flush_analytics(now=b + BUCKET_SECONDS + at.CLOSE_GRACE_SECONDS + 1)     # finalised
    assert env.rows5m() == {(b, 7, "PK", "openvpn"): (0, 10, 4)}

    env.redis.delete(at._fin_key(b), at._sig_key(b))                              # markers expired
    env.bump(b, 7, "PK", "openvpn", "ok", 1)                                      # straggler recreates the hash
    at.flush_analytics(now=b + 3 * H)
    assert env.rows5m() == {(b, 7, "PK", "openvpn"): (0, 10, 4)}                  # unchanged, not (0, 1, 0)
    assert env.rowsH() == {(T0, 7, "PK", "openvpn"): (0, 10, 4)}


def test_counts_that_grew_are_taken_over_column_by_column(env):
    b = T0
    env.bump(b, 7, "PK", "openvpn", "ok", 5)
    at.flush_analytics(now=b + 60)
    env.bump(b, 7, "PK", "openvpn", "ok", 3)                                      # ok grows
    env.bump(b, 7, "PK", "openvpn", "asg", 2)                                     # a new column appears
    at.flush_analytics(now=b + 120)
    assert env.rows5m() == {(b, 7, "PK", "openvpn"): (2, 8, 0)}


def test_lock_prevents_concurrent_flush(env):
    env.bump(T0, 7, "PK", "openvpn", "ok", 5)
    env.redis.d[at.LOCK_FLUSH] = "1"
    assert at.flush_analytics(now=T0 + 60) is None
    assert env.rows5m() == {}
    assert env.redis.d[at.LOCK_FLUSH] == "1"         # someone else's lock is left alone


def test_lock_is_released_after_a_run_even_when_it_fails(env, monkeypatch):
    monkeypatch.setattr(at, "_flush_traffic", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x")))
    at.flush_analytics(now=T0 + 60)
    assert at.LOCK_FLUSH not in env.redis.d


def test_disabled_flag_makes_the_task_a_noop(env, monkeypatch):
    env.bump(T0, 7, "PK", "openvpn", "ok", 5)
    monkeypatch.setattr(settings, "ANALYTICS_ENABLED", False)
    assert at.flush_analytics(now=T0 + 60) is None
    assert env.rows5m() == {}
    assert at.cleanup_analytics() is None


def test_redis_down_is_swallowed(monkeypatch):
    class Dead:
        def set(self, *a, **k):
            raise ConnectionError("redis down")
    monkeypatch.setattr(at, "_redis", lambda: Dead())
    monkeypatch.setattr(settings, "ANALYTICS_ENABLED", True)
    assert at.flush_analytics(now=T0) is None        # no exception
    assert at.cleanup_analytics() is None


# ─── usage snapshots (sessions + capacity history) ────────────────────────────

def _seed_servers(env):
    with env.Session() as s:
        a = VPNServer(name="a", ip_address="1.1.1.1", app_name="appA", server_type="free", max_capacity=100, is_active=True)
        b = VPNServer(name="b", ip_address="2.2.2.2", app_name="appB", server_type="premium", max_capacity=50, is_active=False)
        s.add_all([a, b])
        s.commit()
        s.add_all(
            [VPNUserSession(server_id=a.id, user_id=f"o{i}", device_ip="9.9.9.9", protocol="openvpn") for i in range(3)]
            + [VPNUserSession(server_id=a.id, user_id=f"s{i}", device_ip="9.9.9.9", protocol="shadowsocks") for i in range(2)]
        )
        s.commit()
        return a.id, b.id


def test_usage_snapshot_records_sessions_per_protocol_and_capacity(env):
    a, b = _seed_servers(env)
    at.flush_analytics(now=T0 + 30)
    u = env.usage()
    assert u[(T0, a)] == (5, 3, 2, 100, True)
    assert u[(T0, b)] == (0, 0, 0, 50, False)        # inactive server: 0 sessions, flagged inactive


def test_usage_is_sampled_once_per_bucket(env):
    a, _ = _seed_servers(env)
    at.flush_analytics(now=T0 + 30)
    with env.Session() as s:                         # more sessions arrive inside the same bucket
        s.add(VPNUserSession(server_id=a, user_id="late", device_ip="9.9.9.9", protocol="openvpn"))
        s.commit()
    at.flush_analytics(now=T0 + 90)
    assert env.usage()[(T0, a)][0] == 5              # not re-sampled within the bucket
    at.flush_analytics(now=T0 + BUCKET_SECONDS + 5)
    assert env.usage()[(T0 + BUCKET_SECONDS, a)][0] == 6


def test_capacity_history_is_not_rewritten_when_capacity_changes(env):
    a, _ = _seed_servers(env)
    at.flush_analytics(now=T0 + 30)
    with env.Session() as s:
        s.execute(VPNServer.__table__.update().where(VPNServer.id == a).values(max_capacity=500))
        s.commit()
    at.flush_analytics(now=T0 + BUCKET_SECONDS + 5)
    u = env.usage()
    assert u[(T0, a)][3] == 100                      # the past keeps the capacity that applied then
    assert u[(T0 + BUCKET_SECONDS, a)][3] == 500


def test_failed_usage_snapshot_is_retried_on_the_next_tick(env, monkeypatch):
    _seed_servers(env)
    real = at._upsert
    monkeypatch.setattr(at, "_upsert", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("db")))
    at.flush_analytics(now=T0 + 30)
    assert env.usage() == {}
    monkeypatch.setattr(at, "_upsert", real)
    at.flush_analytics(now=T0 + 90)
    assert len(env.usage()) == 2


def test_usage_failure_does_not_block_traffic_flush(env, monkeypatch):
    env.bump(T0, 7, "PK", "openvpn", "ok", 5)
    monkeypatch.setattr(at, "_sample_usage", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x")))
    at.flush_analytics(now=T0 + 60)
    assert env.rows5m() == {(T0, 7, "PK", "openvpn"): (0, 5, 0)}


# ─── retention ────────────────────────────────────────────────────────────────

def _dt(epoch):
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def _seed_old_and_new(env, now):
    old, recent = now - timedelta(days=100), now - timedelta(hours=2)
    with env.Session() as s:
        for model in (ServerTraffic5m, ServerTrafficHourly):
            s.add(model(bucket_start=old, server_id=1, country="PK", protocol="openvpn", assigned=1, success=1, failed=0))
            s.add(model(bucket_start=recent, server_id=1, country="PK", protocol="openvpn", assigned=1, success=1, failed=0))
        s.add(ServerUsage5m(bucket_start=old, server_id=1))
        s.add(ServerUsage5m(bucket_start=recent, server_id=1))
        s.commit()


def test_cleanup_deletes_only_rows_older_than_retention(env, monkeypatch):
    now = _dt(T0)
    _seed_old_and_new(env, now)
    mid = now - timedelta(days=10)                   # older than 3d (5m) / 30d usage? -> 5m only
    with env.Session() as s:
        s.add(ServerTraffic5m(bucket_start=mid, server_id=1, country="PK", protocol="openvpn"))
        s.add(ServerTrafficHourly(bucket_start=mid, server_id=1, country="PK", protocol="openvpn"))
        s.commit()
    monkeypatch.setattr(settings, "ANALYTICS_5M_RETENTION_DAYS", 3)
    monkeypatch.setattr(settings, "ANALYTICS_HOURLY_RETENTION_DAYS", 60)
    monkeypatch.setattr(settings, "ANALYTICS_USAGE_RETENTION_DAYS", 30)
    result = at.cleanup_analytics(now=now)
    assert result == {"server_traffic_5m": 2, "server_traffic_hourly": 1, "server_usage_5m": 1}
    with env.Session() as s:
        assert s.scalar(select(func.count()).select_from(ServerTraffic5m)) == 1
        assert s.scalar(select(func.count()).select_from(ServerTrafficHourly)) == 2   # recent + 10-day-old
        assert s.scalar(select(func.count()).select_from(ServerUsage5m)) == 1


def test_cleanup_never_wipes_everything_when_retention_is_misconfigured(env, monkeypatch):
    now = _dt(T0)
    _seed_old_and_new(env, now)
    for name in ("ANALYTICS_5M_RETENTION_DAYS", "ANALYTICS_HOURLY_RETENTION_DAYS", "ANALYTICS_USAGE_RETENTION_DAYS"):
        monkeypatch.setattr(settings, name, 0)       # 0 / negative is clamped to 1 day
    at.cleanup_analytics(now=now)
    with env.Session() as s:
        assert s.scalar(select(func.count()).select_from(ServerTraffic5m)) == 1     # the 2-hours-old row survives


def test_cleanup_works_through_a_large_backlog_in_bounded_slices(env, monkeypatch):
    now = _dt(T0)
    with env.Session() as s:
        for d in range(5, 40):                       # 35 daily rows, all older than the 3-day retention
            s.add(ServerTraffic5m(bucket_start=now - timedelta(days=d), server_id=1, country="PK", protocol="openvpn"))
        s.commit()
    monkeypatch.setattr(settings, "ANALYTICS_5M_RETENTION_DAYS", 3)
    result = at.cleanup_analytics(now=now)
    assert result["server_traffic_5m"] == 35
    with env.Session() as s:
        assert s.scalar(select(func.count()).select_from(ServerTraffic5m)) == 0


def test_cleanup_on_empty_tables_is_fine(env):
    assert at.cleanup_analytics(now=_dt(T0)) == {"server_traffic_5m": 0, "server_traffic_hourly": 0, "server_usage_5m": 0}


def test_task_logging_is_ascii_and_cannot_raise(env, monkeypatch, capsys):
    at._log("ERROR analytics — café ❌")
    assert capsys.readouterr().out.isascii()

    def bad_print(*a, **k):
        raise UnicodeEncodeError("charmap", "x", 0, 1, "cannot encode")

    monkeypatch.setattr("builtins.print", bad_print)
    at._log("anything")                                                            # must not raise
    monkeypatch.setattr(at, "_flush_traffic", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("café ❌")))
    at.flush_analytics(now=T0 + 60)                                                # must not raise
