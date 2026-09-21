"""Tests for the active-user snapshot Celery task (tasks.py) — interval gating."""
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import app.tasks as tasks
from app.database import Base
from app.models import ALL_APPS_KEY, ActiveUsersHistory, GlobalSettings, SystemPeakStats, VPNServer, VPNUserSession


class FakeRedis:
    def __init__(self):
        self.d = {}

    def set(self, k, v, nx=False, ex=None):
        if nx and k in self.d:
            return None
        self.d[k] = v
        return True

    def get(self, k):
        return self.d.get(k)

    def exists(self, k):
        return k in self.d

    def delete(self, k):
        self.d.pop(k, None)


@pytest.fixture
def env(monkeypatch):
    engine = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    redis = FakeRedis()
    clock = [1_000_000.0]

    monkeypatch.setattr(tasks, "redis_client", redis)
    monkeypatch.setattr(tasks, "get_db_session", lambda: Session())
    monkeypatch.setattr(tasks, "time", SimpleNamespace(time=lambda: clock[0], sleep=lambda s: None))

    with Session() as s:
        s.add(GlobalSettings(id=1))
        srv = VPNServer(name="s", ip_address="1.1.1.1", app_name="appA", server_type="free",
                        max_capacity=100, is_active=True)
        s.add(srv)
        s.commit()
        s.add_all([VPNUserSession(server_id=srv.id, user_id=f"u{i}", device_ip="1.1.1.1") for i in range(7)])
        s.commit()

    class Env:
        def snaps(self):
            with Session() as s:
                return s.query(ActiveUsersHistory).count()

        def set_interval(self, minutes):
            with Session() as s:
                s.execute(text(f"UPDATE global_settings SET history_interval_minutes={minutes} WHERE id=1"))
                s.commit()

        def tick(self, seconds=60):
            clock[0] += seconds
            tasks.track_active_users_snapshot()

        def peak(self):
            with Session() as s:
                return s.query(SystemPeakStats).filter_by(app_name=ALL_APPS_KEY).one().peak_users

    e = Env()
    e.redis, e.Session = redis, Session
    return e


def test_default_interval_is_30_and_first_run_records_immediately(env):
    assert tasks.get_history_interval_minutes() == 30
    env.tick(0)
    assert env.snaps() == 2                       # one app + the combined row
    for _ in range(28):
        env.tick()
    assert env.snaps() == 2                       # still inside the 30-minute window
    env.tick()
    env.tick()
    assert env.snaps() == 4                       # lands at the 30-minute mark


def test_changed_interval_applies_on_the_very_next_tick(env):
    env.tick(0)
    before = env.snaps()
    env.set_interval(1)
    env.tick()
    assert env.snaps() == before + 2
    for _ in range(5):
        env.tick()
    assert env.snaps() == before + 2 + 10         # one snapshot per minute


def test_one_minute_interval_tolerates_scheduling_jitter(env):
    env.set_interval(1)
    env.tick(0)
    before = env.snaps()
    env.tick(58)
    env.tick(61)
    assert env.snaps() == before + 4              # neither a 58s nor a 61s gap skips a minute


def test_five_minute_interval_and_no_double_recording(env):
    env.set_interval(5)
    env.tick(0)
    before = env.snaps()
    for _ in range(4):
        env.tick()
    assert env.snaps() == before                  # not before the 5th minute
    env.tick()
    assert env.snaps() == before + 2
    now = env.snaps()
    env.tick(10)
    env.tick(10)
    assert env.snaps() == now                     # two ticks close together never double-record


def test_peak_tracking_runs_every_tick_regardless_of_interval(env):
    env.set_interval(30)
    env.tick(0)
    assert env.peak() == 7


@pytest.mark.parametrize("stored", [0, 99999])
def test_out_of_range_stored_value_falls_back_to_30(env, stored):
    env.set_interval(stored)
    assert tasks.get_history_interval_minutes() == 30


def test_db_error_reading_the_setting_falls_back_to_30(env, monkeypatch):
    def broken():
        raise RuntimeError("db down")

    monkeypatch.setattr(tasks, "get_db_session", broken)
    assert tasks.get_history_interval_minutes() == 30


def test_garbled_redis_timestamp_heals_itself(env):
    env.redis.d[tasks.HISTORY_LAST_SNAPSHOT_KEY] = "garbage"
    before = env.snaps()
    env.tick()
    assert env.snaps() == before + 2
    assert float(env.redis.d[tasks.HISTORY_LAST_SNAPSHOT_KEY])
