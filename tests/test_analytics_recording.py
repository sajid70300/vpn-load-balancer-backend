"""
Tests for request-path analytics recording (app/analytics.py) and its hooks in
app/api/public.py.

The key guarantees: recording is one non-blocking Redis increment, it can never
fail or noticeably delay a request (Redis errors / hangs / disabled flag), and the
existing endpoints behave exactly as before.
"""
import asyncio
import time
from types import SimpleNamespace

import pytest
from fastapi import BackgroundTasks, HTTPException

from app import analytics
from app.api import public
from app.config import settings
from app.models import VPNServer, VPNUserSession
from app.schemas import BestServerDecision, ConnectionFeedback, ProtocolConfig
from conftest import make_db

NOW = 1_800_000_123.0
BUCKET = analytics.bucket_epoch(NOW)


class FakePipeline:
    def __init__(self, redis):
        self.redis, self.ops = redis, []

    def hincrby(self, key, field, n=1):
        self.ops.append(("hincrby", key, field, n))
        return self

    def expire(self, key, seconds):
        self.ops.append(("expire", key, seconds))
        return self

    async def execute(self):
        for op in self.ops:
            if op[0] == "hincrby":
                _, key, field, n = op
                h = self.redis.hashes.setdefault(key, {})
                h[field] = h.get(field, 0) + n
            else:
                self.redis.ttls[op[1]] = op[2]
        self.redis.executed += 1


class FakeAsyncRedis:
    def __init__(self):
        self.hashes, self.ttls, self.executed = {}, {}, 0

    def pipeline(self, transaction=False):
        assert transaction is False           # no MULTI/EXEC overhead
        return FakePipeline(self)


@pytest.fixture
def redis(monkeypatch):
    fake = FakeAsyncRedis()

    async def get_redis():
        return fake

    monkeypatch.setattr(analytics, "get_redis", get_redis)
    monkeypatch.setattr(analytics, "time", SimpleNamespace(time=lambda: NOW, monotonic=time.monotonic))
    monkeypatch.setattr(settings, "ANALYTICS_ENABLED", True)
    monkeypatch.setattr(analytics, "_last_error_log", 0.0)
    return fake


def _decision(server_id=7, protocol="openvpn"):
    def cfg(proto):
        return ProtocolConfig(protocol=proto, server_id=server_id, server_name="srv", ip_address="1.2.3.4")
    return BestServerDecision(
        app_name="appA", primary_protocol=protocol, primary_config=cfg(protocol), primary_score=90.0,
        fallback_protocol="shadowsocks", fallback_config=cfg("shadowsocks"), fallback_score=80.0,
        server_type="free", server_city="X", server_country="Y", flag_image_url=None,
        cpu_usage=1.0, ram_usage=1.0, ping_ms=1.0, load_score=1.0, current_users=1, max_capacity=100,
    )


# ─── normalisation ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw,expected", [
    ("pk", "PK"), (" us ", "US"), ("PK", "PK"), (None, "XX"), ("", "XX"), ("PAK", "XX"), ("P", "XX"),
    ("1A", "XX"), ("é", "XX"), ("éé", "XX"), (5, "XX"), ("P K", "XX"),
])
def test_normalize_country(raw, expected):
    assert analytics.normalize_country(raw) == expected


@pytest.mark.parametrize("raw,expected", [
    ("openvpn", "openvpn"), ("OpenVPN", "openvpn"), (" shadowsocks ", "shadowsocks"),
    ("wireguard", "other"), (None, "other"), ("", "other"), (3, "other"),
])
def test_normalize_protocol(raw, expected):
    assert analytics.normalize_protocol(raw) == expected


def test_bucket_math():
    assert analytics.bucket_epoch(1_800_000_000) % 300 == 0
    assert analytics.bucket_epoch(299.9) == 0 and analytics.bucket_epoch(300.0) == 300


# ─── recording ────────────────────────────────────────────────────────────────

def test_assignment_is_one_hincrby_plus_expire_in_one_pipeline(redis):
    asyncio.run(analytics.record_assignment(_decision(7, "openvpn"), "pk"))
    assert redis.hashes == {analytics.bucket_key(BUCKET): {"7|PK|openvpn|asg": 1}}
    assert redis.ttls == {analytics.bucket_key(BUCKET): analytics.BUCKET_TTL_SECONDS}
    assert redis.executed == 1                      # a single network round trip


def test_assignment_accepts_the_cached_dict_form(redis):
    cached = _decision(9, "shadowsocks").model_dump()
    asyncio.run(analytics.record_assignment(cached, None))
    assert redis.hashes[analytics.bucket_key(BUCKET)] == {"9|XX|shadowsocks|asg": 1}


def test_repeated_events_accumulate(redis):
    async def go():
        for _ in range(3):
            await analytics.record_assignment(_decision(7), "PK")
    asyncio.run(go())
    assert redis.hashes[analytics.bucket_key(BUCKET)]["7|PK|openvpn|asg"] == 3


@pytest.mark.parametrize("decision", [
    None, {}, {"primary_config": None}, {"primary_config": {"server_id": "7"}}, {"primary_config": {"server_id": 0}},
    {"primary_config": {"server_id": -1}}, {"primary_config": {"server_id": True}}, SimpleNamespace(), "junk", 42,
])
def test_unusable_decisions_are_ignored_not_raised(redis, decision):
    asyncio.run(analytics.record_assignment(decision, "PK"))
    assert redis.hashes == {} and redis.executed == 0


def test_feedback_primary_success_only(redis):
    asyncio.run(analytics.record_feedback(7, "pk", [("openvpn", True), (None, None)]))
    assert redis.hashes[analytics.bucket_key(BUCKET)] == {"7|PK|openvpn|ok": 1}


def test_feedback_primary_fail_secondary_success_counts_one_of_each(redis):
    asyncio.run(analytics.record_feedback(7, "PK", [("openvpn", False), ("shadowsocks", True)]))
    assert redis.hashes[analytics.bucket_key(BUCKET)] == {"7|PK|openvpn|fail": 1, "7|PK|shadowsocks|ok": 1}
    assert redis.executed == 1


def test_feedback_both_failed(redis):
    asyncio.run(analytics.record_feedback(7, "PK", [("openvpn", False), ("shadowsocks", False)]))
    assert redis.hashes[analytics.bucket_key(BUCKET)] == {"7|PK|openvpn|fail": 1, "7|PK|shadowsocks|fail": 1}


def test_feedback_garbage_dimensions_are_bounded(redis):
    asyncio.run(analytics.record_feedback(7, "<script>", [("wireguard-9000", True)]))
    assert redis.hashes[analytics.bucket_key(BUCKET)] == {"7|XX|other|ok": 1}


@pytest.mark.parametrize("server_id", [0, -5, None, "7", True])
def test_feedback_with_invalid_server_id_is_ignored(redis, server_id):
    asyncio.run(analytics.record_feedback(server_id, "PK", [("openvpn", True)]))
    assert redis.hashes == {}


def test_feedback_with_nothing_attempted_writes_nothing(redis):
    asyncio.run(analytics.record_feedback(7, "PK", [(None, None), ("openvpn", None)]))
    assert redis.hashes == {} and redis.executed == 0


# ─── it can never hurt a request ──────────────────────────────────────────────

def test_disabled_flag_does_no_redis_work_at_all(redis, monkeypatch):
    monkeypatch.setattr(settings, "ANALYTICS_ENABLED", False)
    called = []

    async def spy():
        called.append(1)
        return redis

    monkeypatch.setattr(analytics, "get_redis", spy)
    asyncio.run(analytics.record_assignment(_decision(), "PK"))
    asyncio.run(analytics.record_feedback(7, "PK", [("openvpn", True)]))
    assert called == [] and redis.hashes == {}


def test_redis_errors_are_swallowed(redis, monkeypatch):
    async def broken():
        raise ConnectionError("redis down")

    monkeypatch.setattr(analytics, "get_redis", broken)
    asyncio.run(analytics.record_assignment(_decision(), "PK"))            # must not raise
    asyncio.run(analytics.record_feedback(7, "PK", [("openvpn", True)]))   # must not raise


def test_pipeline_errors_are_swallowed(redis, monkeypatch):
    async def bad_execute(self):
        raise TimeoutError("slow")

    monkeypatch.setattr(FakePipeline, "execute", bad_execute)
    asyncio.run(analytics.record_feedback(7, "PK", [("openvpn", True)]))


def test_a_hanging_redis_is_cut_off_by_the_timeout(redis, monkeypatch):
    async def hang():
        await asyncio.sleep(30)

    monkeypatch.setattr(analytics, "get_redis", hang)
    monkeypatch.setattr(analytics, "REDIS_TIMEOUT_SECONDS", 0.05)
    started = time.monotonic()
    asyncio.run(analytics.record_feedback(7, "PK", [("openvpn", True)]))
    assert time.monotonic() - started < 1.0


def test_cancellation_is_not_swallowed(redis, monkeypatch):
    async def hang():
        await asyncio.sleep(30)

    monkeypatch.setattr(analytics, "get_redis", hang)

    async def go():
        task = asyncio.ensure_future(analytics.record_feedback(7, "PK", [("openvpn", True)]))
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(go())


def test_logging_itself_can_never_raise_even_if_the_console_cannot_encode(redis, monkeypatch):
    """Regression: a print() of a non-ASCII char on a cp1252/ASCII console raised inside the
    error handler and would have failed the request. The message is ASCII and print is guarded."""
    async def broken():
        raise ConnectionError("redis down")

    def bad_print(*a, **k):
        raise UnicodeEncodeError("charmap", "x", 0, 1, "cannot encode")

    monkeypatch.setattr(analytics, "get_redis", broken)
    monkeypatch.setattr("builtins.print", bad_print)
    asyncio.run(analytics.record_feedback(7, "PK", [("openvpn", True)]))          # must not raise
    asyncio.run(analytics.record_assignment(_decision(), "PK"))                    # must not raise


def test_logged_message_is_pure_ascii(redis, monkeypatch, capsys):
    async def broken():
        raise ConnectionError("rédis — down")

    monkeypatch.setattr(analytics, "get_redis", broken)
    asyncio.run(analytics.record_feedback(7, "PK", [("openvpn", True)]))
    out = capsys.readouterr().out
    assert out and out.isascii()


def test_error_logging_is_rate_limited(redis, monkeypatch, capsys):
    async def broken():
        raise ConnectionError("down")

    monkeypatch.setattr(analytics, "get_redis", broken)

    async def go():
        for _ in range(50):
            await analytics.record_feedback(7, "PK", [("openvpn", True)])

    asyncio.run(go())
    assert capsys.readouterr().out.count("analytics:") == 1


# ─── hooks in public.py ───────────────────────────────────────────────────────

def test_best_server_v2_is_declared_with_background_tasks_and_hides_it_from_the_api_schema():
    import main
    route = next(r for r in main.app.routes if getattr(r, "path", "") == "/v2/best_server/")
    assert route.dependant.background_tasks_param_name == "background_tasks"
    params = [p["name"] for p in main.app.openapi()["paths"]["/v2/best_server/"]["post"]["parameters"]]
    assert "background_tasks" not in params
    assert set(params) == {"app_name", "server_type", "country", "asn", "network_type"}   # unchanged contract


def _call_best_server(monkeypatch, *, cached=None, engine_result=None, engine_error=None):
    stored = []

    async def get_cache(k):
        return cached

    async def set_cache(k, v, ttl=3):
        stored.append(k)

    class FakeEngine:
        def __init__(self, db):
            pass

        async def get_best_server(self, **kw):
            if engine_error:
                raise engine_error
            return engine_result

    monkeypatch.setattr(public, "get_cache", get_cache)
    monkeypatch.setattr(public, "set_cache", set_cache)
    monkeypatch.setattr(public, "DecisionEngine", FakeEngine)
    bt = BackgroundTasks()
    coro = public.best_server_v2(
        background_tasks=bt, app_name="appA", server_type=None, country="pk", asn=None,
        network_type=None, db=None, _="t",
    )
    return bt, coro, stored


def test_best_server_fresh_decision_queues_a_background_count_and_returns_unchanged(redis, monkeypatch):
    decision = _decision(7, "openvpn")
    bt, coro, stored = _call_best_server(monkeypatch, engine_result=decision)
    result = asyncio.run(coro)
    assert result is decision                                  # response untouched
    assert len(bt.tasks) == 1 and redis.hashes == {}           # queued, NOT run inline
    asyncio.run(bt())                                          # what Starlette does after the response
    assert redis.hashes[analytics.bucket_key(BUCKET)] == {"7|PK|openvpn|asg": 1}


def test_best_server_cache_hit_is_also_counted(redis, monkeypatch):
    cached = _decision(9, "shadowsocks").model_dump()
    bt, coro, _ = _call_best_server(monkeypatch, cached=cached)
    assert asyncio.run(coro) == cached
    asyncio.run(bt())
    assert redis.hashes[analytics.bucket_key(BUCKET)] == {"9|PK|shadowsocks|asg": 1}


def test_best_server_no_server_available_still_returns_503_and_counts_nothing(redis, monkeypatch):
    bt, coro, _ = _call_best_server(monkeypatch, engine_error=ValueError("No servers available"))
    with pytest.raises(HTTPException) as e:
        asyncio.run(coro)
    assert e.value.status_code == 503 and e.value.detail == "No servers available"
    assert bt.tasks == []


def test_best_server_still_works_when_analytics_redis_is_broken(redis, monkeypatch):
    async def broken():
        raise ConnectionError("down")

    monkeypatch.setattr(analytics, "get_redis", broken)
    decision = _decision()
    bt, coro, _ = _call_best_server(monkeypatch, engine_result=decision)
    assert asyncio.run(coro) is decision
    asyncio.run(bt())                                          # background task must not raise either


def _feedback(**kw):
    base = dict(server_id=1, country="pk", asn="AS1", network_type="wifi", primary_protocol="openvpn",
                primary_success=True, primary_connect_time_ms=120.0)
    base.update(kw)
    return ConnectionFeedback(**base)


def _run_feedback(monkeypatch, feedback, *, with_server=True):
    calls = {"engine": 0, "deleted": []}

    class FakeEngine:
        def __init__(self, db):
            pass

        async def process_connection_feedback(self, **kw):
            calls["engine"] += 1
            calls["kw"] = kw

    async def delete_cache(p):
        calls["deleted"].append(p)

    monkeypatch.setattr(public, "DecisionEngine", FakeEngine)
    monkeypatch.setattr(public, "delete_cache", delete_cache)

    async def go():
        async with make_db() as db:
            if with_server:
                db.add(VPNServer(id=1, name="s", ip_address="1.1.1.1", app_name="appA", server_type="free", max_capacity=10))
                await db.commit()
            resp = await public.connection_feedback(feedback, db, "t")
            from sqlalchemy import select
            sessions = (await db.execute(select(VPNUserSession))).scalars().all()
            return resp, sessions

    return asyncio.run(go()), calls


def test_feedback_endpoint_records_and_returns_exactly_as_before(redis, monkeypatch):
    (resp, sessions), calls = _run_feedback(monkeypatch, _feedback())
    assert resp["message"] == "Feedback processed successfully" and resp["cooldown_triggered"] is False
    assert calls["engine"] == 1 and calls["deleted"] == ["best_server_v2:*"]
    assert redis.hashes[analytics.bucket_key(BUCKET)] == {"1|PK|openvpn|ok": 1}
    assert sessions == []


def test_feedback_fallback_flow_counts_each_attempt_and_still_tracks_the_shadowsocks_session(redis, monkeypatch):
    fb = _feedback(primary_success=False, secondary_protocol="shadowsocks", secondary_success=True,
                   user_id="u1", device_ip="5.5.5.5")
    (resp, sessions), _ = _run_feedback(monkeypatch, fb)
    assert redis.hashes[analytics.bucket_key(BUCKET)] == {"1|PK|openvpn|fail": 1, "1|PK|shadowsocks|ok": 1}
    assert [(s.user_id, s.protocol) for s in sessions] == [("u1", "shadowsocks")]     # existing behaviour intact
    assert resp["primary"]["result"] == "failure" and resp["secondary"]["result"] == "success"


def test_feedback_both_failed_reports_cooldown_and_counts_two_failures(redis, monkeypatch):
    fb = _feedback(primary_success=False, secondary_protocol="shadowsocks", secondary_success=False)
    (resp, _), _ = _run_feedback(monkeypatch, fb)
    assert resp["cooldown_triggered"] is True
    assert redis.hashes[analytics.bucket_key(BUCKET)] == {"1|PK|openvpn|fail": 1, "1|PK|shadowsocks|fail": 1}


def test_feedback_for_unknown_server_is_404_and_counts_nothing(redis, monkeypatch):
    with pytest.raises(HTTPException) as e:
        _run_feedback(monkeypatch, _feedback(), with_server=False)
    assert e.value.status_code == 404
    assert redis.hashes == {}


def test_feedback_endpoint_still_succeeds_when_analytics_redis_is_broken(redis, monkeypatch):
    async def broken():
        raise ConnectionError("down")

    monkeypatch.setattr(analytics, "get_redis", broken)
    (resp, _), calls = _run_feedback(monkeypatch, _feedback())
    assert resp["message"] == "Feedback processed successfully" and calls["engine"] == 1
