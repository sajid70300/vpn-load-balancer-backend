"""Tests for the read-only analytics API (app/api/admin_analytics.py)."""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException

from app.api import admin_analytics as api
from app.models import (
    ALL_APPS_KEY, ActiveUsersHistory, Notification, ProtocolMetrics, ServerTraffic5m,
    ServerTrafficHourly, VPNServer, VPNUserSession,
)
from conftest import make_db

H = 3600
T0 = 1_800_000_000 - (1_800_000_000 % H)                # exact hour
NOW = datetime.fromtimestamp(T0 + 1800, tz=timezone.utc)  # half past the hour


def dt(epoch):
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


@pytest.fixture(autouse=True)
def fixed_clock_and_cache(monkeypatch):
    store = {"calls": {"get": 0, "set": 0}, "data": {}}

    async def get_cache(k):
        store["calls"]["get"] += 1
        return store["data"].get(k)

    async def set_cache(k, v, ttl=3):
        import json
        store["calls"]["set"] += 1
        store["data"][k] = json.loads(json.dumps(v))     # what Redis would round-trip
        store.setdefault("ttl", {})[k] = ttl

    monkeypatch.setattr(api, "_utc_now", lambda: NOW)
    monkeypatch.setattr(api, "get_cache", get_cache)
    monkeypatch.setattr(api, "set_cache", set_cache)
    return store


def run(coro):
    return asyncio.run(coro)


async def seed_servers(db):
    db.add_all([
        VPNServer(id=1, name="fra-a", ip_address="1.1.1.1", app_name="appA", server_type="free", max_capacity=100,
                  physical_machine_id=10, is_active=True, server_city="Frankfurt", server_country="DE"),
        VPNServer(id=2, name="fra-b", ip_address="1.1.1.1", app_name="appB", server_type="free", max_capacity=100,
                  physical_machine_id=10, is_active=True),
        VPNServer(id=3, name="fra-prem", ip_address="1.1.1.1", app_name="appA", server_type="premium", max_capacity=40,
                  physical_machine_id=10, is_active=True),
        VPNServer(id=4, name="tok", ip_address="2.2.2.2", app_name="appA", server_type="free", max_capacity=60,
                  physical_machine_id=11, is_active=True),
        VPNServer(id=5, name="off", ip_address="3.3.3.3", app_name="appB", server_type="free", max_capacity=50,
                  physical_machine_id=12, is_active=False),
    ])
    await db.commit()


def add_hourly(db, epoch, server_id, country, protocol, a, s, f):
    db.add(ServerTrafficHourly(bucket_start=dt(epoch), server_id=server_id, country=country, protocol=protocol,
                               assigned=a, success=s, failed=f))


def add_5m(db, epoch, server_id, country, protocol, a, s, f):
    db.add(ServerTraffic5m(bucket_start=dt(epoch), server_id=server_id, country=country, protocol=protocol,
                           assigned=a, success=s, failed=f))


def report(db, server_id=1, period="24h", country=None, protocol=None, scope="server"):
    return api.server_analytics(server_id, period, country, protocol, scope, db, "t")


# ─── per-server report ────────────────────────────────────────────────────────

def test_report_summary_country_and_protocol_breakdowns():
    async def go():
        async with make_db() as db:
            await seed_servers(db)
            add_hourly(db, T0 - 2 * H, 1, "PK", "openvpn", 10, 8, 2)
            add_hourly(db, T0 - 2 * H, 1, "PK", "shadowsocks", 4, 3, 1)
            add_hourly(db, T0 - 1 * H, 1, "US", "openvpn", 6, 5, 1)
            add_hourly(db, T0,         1, "PK", "openvpn", 5, 4, 1)        # current, partial hour
            add_hourly(db, T0 - 2 * H, 2, "PK", "openvpn", 99, 99, 99)     # a different server: excluded
            await db.commit()
            return await report(db)
    r = run(go())

    assert r["summary"] == {"assigned": 25, "success": 20, "failed": 5, "attempts": 25,
                            "success_rate": 80.0, "failure_rate": 20.0}
    by_country = {c["country"]: c for c in r["by_country"]}
    assert by_country["PK"]["assigned"] == 19 and by_country["PK"]["success"] == 15 and by_country["PK"]["failed"] == 4
    assert by_country["PK"]["success_rate"] == round(15 / 19 * 100, 2)
    assert by_country["US"]["attempts"] == 6
    assert [c["country"] for c in r["by_country"]] == ["PK", "US"]          # busiest first

    protos = {p["protocol"]: p for p in r["by_protocol"]}
    assert set(protos) == {"openvpn", "shadowsocks"}                       # 'other' hidden when empty
    assert (protos["openvpn"]["success"], protos["openvpn"]["failed"]) == (17, 4)
    assert (protos["shadowsocks"]["success"], protos["shadowsocks"]["failed"]) == (3, 1)
    assert r["by_protocol_total"]["attempts"] == 25

    assert r["period"]["key"] == "24h" and r["period"]["granularity"] == "hourly"
    assert r["period"]["from"].endswith("Z") and r["generated_at"].endswith("Z")
    assert r["server"]["name"] == "fra-a" and r["server"]["max_capacity"] == 100
    assert "reported" in r["definitions"]["success"].lower()


def test_country_filter_narrows_summary_and_protocol_table_but_not_the_country_table():
    async def go():
        async with make_db() as db:
            await seed_servers(db)
            add_hourly(db, T0 - H, 1, "PK", "openvpn", 10, 8, 2)
            add_hourly(db, T0 - H, 1, "PK", "shadowsocks", 4, 3, 1)
            add_hourly(db, T0 - H, 1, "US", "openvpn", 6, 5, 1)
            await db.commit()
            return await report(db, country="pk")             # lower-case accepted
    r = run(go())
    assert r["filters"]["country"] == "PK"
    assert r["summary"]["attempts"] == 14                                   # PK only
    assert {c["country"] for c in r["by_country"]} == {"PK", "US"}          # all countries stay comparable
    assert r["by_protocol_total"]["attempts"] == 14


def test_protocol_filter_narrows_summary_and_country_table_but_not_the_protocol_table():
    async def go():
        async with make_db() as db:
            await seed_servers(db)
            add_hourly(db, T0 - H, 1, "PK", "openvpn", 10, 8, 2)
            add_hourly(db, T0 - H, 1, "PK", "shadowsocks", 4, 3, 1)
            add_hourly(db, T0 - H, 1, "US", "shadowsocks", 6, 5, 1)
            await db.commit()
            return await report(db, protocol="shadowsocks")
    r = run(go())
    assert r["summary"]["attempts"] == 10                                   # shadowsocks only
    assert {c["country"]: c["attempts"] for c in r["by_country"]} == {"PK": 4, "US": 6}
    assert {p["protocol"]: p["attempts"] for p in r["by_protocol"]} == {"openvpn": 10, "shadowsocks": 10}


def test_combined_country_and_protocol_filter():
    async def go():
        async with make_db() as db:
            await seed_servers(db)
            add_hourly(db, T0 - H, 1, "PK", "openvpn", 10, 8, 2)
            add_hourly(db, T0 - H, 1, "PK", "shadowsocks", 4, 3, 1)
            add_hourly(db, T0 - H, 1, "US", "openvpn", 6, 5, 1)
            await db.commit()
            return await report(db, country="PK", protocol="openvpn")
    r = run(go())
    assert r["summary"] == {"assigned": 10, "success": 8, "failed": 2, "attempts": 10,
                            "success_rate": 80.0, "failure_rate": 20.0}


def test_hourly_periods_are_aligned_to_the_hour_and_exclude_older_data():
    async def go():
        async with make_db() as db:
            await seed_servers(db)
            add_hourly(db, T0 - 24 * H, 1, "PK", "openvpn", 1, 1, 0)        # first hour of the window: included
            add_hourly(db, T0 - 25 * H, 1, "PK", "openvpn", 100, 100, 0)    # older: excluded
            add_hourly(db, T0 - 8 * 86400, 1, "PK", "openvpn", 10000, 10000, 0)
            await db.commit()
            day = await report(db, period="24h")
            week = await report(db, period="7d")
            month = await report(db, period="30d")
            return day, week, month
    day, week, month = run(go())
    assert day["summary"]["success"] == 1
    assert day["period"]["from"] == dt(T0 - 24 * H).isoformat().replace("+00:00", "Z")
    assert week["summary"]["success"] == 101
    assert month["summary"]["success"] == 10101


def test_short_periods_use_five_minute_buckets_and_align_to_five_minutes():
    async def go():
        async with make_db() as db:
            await seed_servers(db)
            add_5m(db, T0 - 1800, 1, "PK", "openvpn", 1, 1, 0)              # exactly now-1h aligned: included
            add_5m(db, T0 - 2100, 1, "PK", "openvpn", 50, 50, 0)            # 5 min earlier: excluded from 1h
            add_5m(db, T0 + 600, 1, "PK", "openvpn", 2, 2, 0)
            add_hourly(db, T0 - H, 1, "PK", "openvpn", 999, 999, 0)         # hourly table must NOT be used for 1h
            await db.commit()
            return await report(db, period="1h"), await report(db, period="6h")
    one, six = run(go())
    assert one["period"]["granularity"] == "5-minute"
    assert one["summary"]["success"] == 3
    assert one["period"]["from"] == dt(T0 - 1800).isoformat().replace("+00:00", "Z")
    assert six["summary"]["success"] == 53


def test_empty_data_returns_zeros_and_null_rates():
    async def go():
        async with make_db() as db:
            await seed_servers(db)
            return await report(db)
    r = run(go())
    assert r["summary"] == {"assigned": 0, "success": 0, "failed": 0, "attempts": 0,
                            "success_rate": None, "failure_rate": None}
    assert r["by_country"] == [] and r["data_start"] is None
    assert [p["protocol"] for p in r["by_protocol"]] == ["openvpn", "shadowsocks"]


def test_live_block_counts_sessions_per_protocol_and_utilisation():
    async def go():
        async with make_db() as db:
            await seed_servers(db)
            db.add_all([VPNUserSession(server_id=1, user_id=f"o{i}", device_ip="9.9.9.9", protocol="openvpn") for i in range(3)]
                       + [VPNUserSession(server_id=1, user_id="s0", device_ip="9.9.9.9", protocol="shadowsocks")]
                       + [VPNUserSession(server_id=2, user_id="x", device_ip="9.9.9.9", protocol="openvpn")])
            await db.commit()
            return await report(db)
    live = run(go())["live"]
    assert live["active_sessions"] == 4 and live["by_protocol"] == {"openvpn": 3, "shadowsocks": 1, "other": 0}
    assert live["capacity"] == 100 and live["utilization_pct"] == 4.0


def test_zero_capacity_gives_null_utilisation_not_a_crash():
    async def go():
        async with make_db() as db:
            db.add(VPNServer(id=1, name="z", ip_address="9.9.9.9", app_name="a", server_type="free", max_capacity=0))
            await db.commit()
            return await report(db)
    assert run(go())["live"]["utilization_pct"] is None


def test_machine_scope_combines_app_rows_of_the_same_server_and_type():
    async def go():
        async with make_db() as db:
            await seed_servers(db)
            add_hourly(db, T0 - H, 1, "PK", "openvpn", 10, 8, 2)            # appA free
            add_hourly(db, T0 - H, 2, "PK", "openvpn", 20, 15, 5)           # appB free, same machine
            add_hourly(db, T0 - H, 3, "PK", "openvpn", 999, 999, 0)         # premium row: different type -> excluded
            add_hourly(db, T0 - H, 4, "PK", "openvpn", 999, 999, 0)         # other machine -> excluded
            db.add(VPNUserSession(server_id=1, user_id="a", device_ip="9.9.9.9", protocol="openvpn"))
            db.add(VPNUserSession(server_id=2, user_id="b", device_ip="9.9.9.9", protocol="shadowsocks"))
            await db.commit()
            return await report(db, server_id=1, scope="machine"), await report(db, server_id=1, scope="server")
    machine, single = run(go())
    assert sorted(machine["server_ids"]) == [1, 2] and machine["apps"] == ["appA", "appB"]
    assert machine["summary"]["attempts"] == 30 and machine["summary"]["assigned"] == 30
    assert machine["live"]["active_sessions"] == 2
    assert machine["live"]["capacity"] == 100                              # shared machine capacity, not 100 + 100
    assert single["summary"]["attempts"] == 10 and single["server_ids"] == [1]


def test_unknown_server_is_404_and_is_not_cached(fixed_clock_and_cache):
    async def go():
        async with make_db() as db:
            with pytest.raises(HTTPException) as e:
                await report(db, server_id=999)
            return e.value.status_code
    assert run(go()) == 404
    assert fixed_clock_and_cache["calls"]["set"] == 0


def test_result_is_cached_and_the_second_call_does_not_touch_the_database(fixed_clock_and_cache, monkeypatch):
    calls = []
    real = api._server_report

    async def counting(*a, **k):
        calls.append(1)
        return await real(*a, **k)

    monkeypatch.setattr(api, "_server_report", counting)

    async def go():
        async with make_db() as db:
            await seed_servers(db)
            first = await report(db, period="24h", country="pk")
            second = await report(db, period="24h", country="PK")             # same cache key (case-insensitive)
            third = await report(db, period="7d", country="PK")               # different period -> different key
            return first, second, third
    first, second, third = run(go())
    assert len(calls) == 2 and first == second and third["period"]["key"] == "7d"
    assert set(fixed_clock_and_cache["ttl"].values()) == {api.SERVER_CACHE_TTL}


def test_a_broken_cache_never_breaks_the_endpoint(monkeypatch):
    async def boom(*a, **k):
        raise ConnectionError("redis down")

    monkeypatch.setattr(api, "get_cache", boom)
    monkeypatch.setattr(api, "set_cache", boom)

    async def go():
        async with make_db() as db:
            await seed_servers(db)
            return await report(db)
    assert run(go())["server"]["id"] == 1


def test_data_start_reports_when_collection_began():
    async def go():
        async with make_db() as db:
            await seed_servers(db)
            add_hourly(db, T0 - 5 * H, 1, "PK", "openvpn", 1, 1, 0)
            add_hourly(db, T0 - 2 * H, 1, "PK", "openvpn", 1, 1, 0)
            await db.commit()
            return await report(db)
    assert run(go())["data_start"] == dt(T0 - 5 * H).isoformat().replace("+00:00", "Z")


# ─── Home overview ────────────────────────────────────────────────────────────

def overview(db, app_name=None):
    return api.overview(app_name, db, "t")


def test_overview_on_a_completely_empty_database_is_all_zeros_not_an_error():
    async def go():
        async with make_db() as db:
            return await overview(db)
    r = run(go())
    assert r["servers"] == {"total": 0, "active": 0}
    assert r["sessions"] == {"current": 0, "capacity": 0, "available": 0, "utilization_pct": 0.0}
    assert r["server_load"] == [] and r["alerts"] == {"unread": 0, "items": []} and r["active_users_24h"] == []
    assert r["metrics"]["all_time"] == {"success_rate": None, "attempts": 0, "avg_connect_ms": None}
    assert len(r["metrics"]["protocol_success_24h"]) == 24 and r["metrics"]["collecting_since"] is None


def _seed_overview(db):
    async def go():
        await seed_servers(db)
        db.add_all(
            [VPNUserSession(server_id=1, user_id=f"a{i}", device_ip="9.9.9.9", protocol="openvpn") for i in range(10)]
            + [VPNUserSession(server_id=1, user_id=f"b{i}", device_ip="9.9.9.9", protocol="shadowsocks") for i in range(5)]
            + [VPNUserSession(server_id=2, user_id=f"c{i}", device_ip="9.9.9.9", protocol="openvpn") for i in range(4)]
            + [VPNUserSession(server_id=4, user_id=f"d{i}", device_ip="9.9.9.9", protocol="shadowsocks") for i in range(30)]
            + [VPNUserSession(server_id=5, user_id="off", device_ip="9.9.9.9", protocol="openvpn")]     # inactive server
        )
        await db.commit()
    return go()


def test_overview_live_numbers_capacity_counts_each_machine_once():
    async def go():
        async with make_db() as db:
            await _seed_overview(db)
            return await overview(db)
    r = run(go())
    assert r["servers"] == {"total": 5, "active": 4}
    # active machines: 10 (rows 1,2,3 share it: capacity max(100,100,40)=100) + 11 (60) = 160
    assert r["sessions"]["capacity"] == 160
    assert r["sessions"]["current"] == 49                 # 10+5+4+30, the inactive server's session is excluded
    assert r["sessions"]["available"] == 111 and r["sessions"]["utilization_pct"] == round(49 / 160 * 100, 2)
    assert r["protocol_sessions"] == {"openvpn": 14, "shadowsocks": 35, "other": 0}
    top = r["server_load"]
    assert top[0]["name"] == "tok" and top[0]["utilization_pct"] == 50.0       # 30 / 60
    assert [t["name"] for t in top] == ["tok", "fra-a", "fra-b", "fra-prem"]   # busiest first; inactive server absent
    assert top[1]["sessions"] == 15 and top[1]["capacity"] == 100


def test_overview_app_filter_scopes_everything_to_that_app():
    async def go():
        async with make_db() as db:
            await _seed_overview(db)
            return await overview(db, "appB")
    r = run(go())
    assert r["app_name"] == "appB"
    assert r["servers"] == {"total": 2, "active": 1}
    assert r["sessions"]["current"] == 4 and r["sessions"]["capacity"] == 100
    assert r["protocol_sessions"] == {"openvpn": 4, "shadowsocks": 0, "other": 0}
    assert [t["name"] for t in r["server_load"]] == ["fra-b"]


def test_overview_server_load_is_limited_to_the_ten_busiest():
    async def go():
        async with make_db() as db:
            for i in range(1, 16):
                db.add(VPNServer(id=i, name=f"s{i}", ip_address=f"10.0.0.{i}", app_name="a", server_type="free",
                                 max_capacity=100, physical_machine_id=i, is_active=True))
            await db.commit()
            for i in range(1, 16):
                for j in range(i):
                    db.add(VPNUserSession(server_id=i, user_id=f"u{i}-{j}", device_ip="9.9.9.9", protocol="openvpn"))
            await db.commit()
            return await overview(db)
    r = run(go())
    assert len(r["server_load"]) == 10 and r["server_load"][0]["name"] == "s15"
    assert r["servers"]["active"] == 15


def test_overview_alerts_newest_first_with_unread_count_and_app_filter():
    async def go():
        async with make_db() as db:
            base = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
            for i in range(8):
                db.add(Notification(type="server_down", server_name=f"s{i}", app_name="appA" if i % 2 == 0 else "appB",
                                    message=f"m{i}", is_read=(i < 3), created_at=base + timedelta(minutes=i)))
            await db.commit()
            return await overview(db), await overview(db, "appA")
    allapps, only_a = run(go())
    assert [a["message"] for a in allapps["alerts"]["items"]] == ["m7", "m6", "m5", "m4", "m3"]
    assert allapps["alerts"]["unread"] == 5
    assert all(a["app_name"] == "appA" for a in only_a["alerts"]["items"]) and only_a["alerts"]["unread"] == 2   # appA: ids 0,2 read; 4,6 unread


def test_overview_active_users_24h_uses_history_downsampled_to_peaks_and_respects_app():
    async def go():
        async with make_db() as db:
            now = datetime.utcnow()
            for minutes_ago, users in ((120, 10), (118, 40), (117, 25), (30, 5)):
                db.add(ActiveUsersHistory(app_name=ALL_APPS_KEY, total_users=users, recorded_at=now - timedelta(minutes=minutes_ago)))
            db.add(ActiveUsersHistory(app_name=ALL_APPS_KEY, total_users=777, recorded_at=now - timedelta(hours=30)))   # too old
            db.add(ActiveUsersHistory(app_name="appA", total_users=3, recorded_at=now - timedelta(minutes=10)))
            await db.commit()
            return await overview(db), await overview(db, "appA")
    allapps, only_a = run(go())
    pts = allapps["active_users_24h"]
    assert 777 not in [p["users"] for p in pts]
    assert max(p["users"] for p in pts) == 40                      # peak per 15-min bucket keeps the spike
    assert [p["users"] for p in only_a["active_users_24h"]] == [3]
    assert all(p["t"].endswith("Z") for p in pts)


def test_overview_all_time_metrics_are_lifetime_success_rate_and_weighted_connect_time():
    async def go():
        async with make_db() as db:
            await seed_servers(db)
            db.add_all([
                ProtocolMetrics(server_id=1, protocol="openvpn", country="PK", asn="A1", total_attempts=100,
                                success_count=90, failure_count=10, avg_connect_time_ms=1000.0),
                ProtocolMetrics(server_id=1, protocol="shadowsocks", country="PK", asn="A1", total_attempts=100,
                                success_count=50, failure_count=50, avg_connect_time_ms=3000.0),
                ProtocolMetrics(server_id=2, protocol="openvpn", country="US", asn="A2", total_attempts=10,
                                success_count=0, failure_count=10, avg_connect_time_ms=0.0),   # no success: no time weight
            ])
            await db.commit()
            return await overview(db), await overview(db, "appB")
    allapps, only_b = run(go())
    at = allapps["metrics"]["all_time"]
    assert at["attempts"] == 210 and at["success_rate"] == round(140 / 210 * 100, 2)
    assert at["avg_connect_ms"] == round((1000 * 90 + 3000 * 50) / 140, 1)          # weighted by successes
    assert only_b["metrics"]["all_time"]["attempts"] == 10 and only_b["metrics"]["all_time"]["success_rate"] == 0.0
    assert only_b["metrics"]["all_time"]["avg_connect_ms"] is None                 # app filter works via the server's app


def test_overview_last_24h_series_is_hourly_per_protocol_with_gaps_as_null():
    async def go():
        async with make_db() as db:
            await seed_servers(db)
            add_hourly(db, T0 - 3 * H, 1, "PK", "openvpn", 0, 9, 1)
            add_hourly(db, T0 - 3 * H, 4, "US", "openvpn", 0, 1, 1)        # another server, same hour: summed
            add_hourly(db, T0 - 3 * H, 1, "PK", "shadowsocks", 0, 1, 3)
            add_hourly(db, T0, 1, "PK", "openvpn", 0, 5, 0)                # current hour
            add_hourly(db, T0 - 30 * H, 1, "PK", "openvpn", 0, 100, 0)     # outside the 24h window
            add_hourly(db, T0 - H, 1, "PK", "other", 0, 100, 0)            # non-standard protocol: ignored
            add_hourly(db, T0 - 5 * H, 5, "PK", "openvpn", 0, 2, 0)        # appB server
            await db.commit()
            return await overview(db), await overview(db, "appB")
    allapps, only_b = run(go())
    series = allapps["metrics"]["protocol_success_24h"]
    assert len(series) == 24 and series[-1]["hour"] == dt(T0).isoformat().replace("+00:00", "Z")
    by_hour = {s["hour"]: s for s in series}
    h3 = by_hour[dt(T0 - 3 * H).isoformat().replace("+00:00", "Z")]
    assert h3["openvpn"] == round(10 / 12 * 100, 2) and h3["shadowsocks"] == 25.0
    assert h3["openvpn_attempts"] == 12
    empty = by_hour[dt(T0 - 10 * H).isoformat().replace("+00:00", "Z")]
    assert empty["openvpn"] is None and empty["shadowsocks"] is None            # gap, not a fake 0%
    assert series[-1]["openvpn"] == 100.0
    assert allapps["metrics"]["last_24h"]["attempts"] == (10 + 2 + 4) + 5 + 2
    assert allapps["metrics"]["collecting_since"] == dt(T0 - 30 * H).isoformat().replace("+00:00", "Z")
    assert only_b["metrics"]["last_24h"]["attempts"] == 2


def test_overview_is_cached_in_two_parts_with_different_lifetimes(fixed_clock_and_cache):
    async def go():
        async with make_db() as db:
            await seed_servers(db)
            await overview(db)
            await overview(db)
    run(go())
    assert fixed_clock_and_cache["calls"]["set"] == 2                    # live + metrics, once each
    ttls = fixed_clock_and_cache["ttl"]
    assert ttls["an:api:ov:live:-"] == api.OVERVIEW_LIVE_TTL and ttls["an:api:ov:met:-"] == api.OVERVIEW_METRICS_TTL
    assert api.OVERVIEW_METRICS_TTL > api.OVERVIEW_LIVE_TTL


def test_overview_includes_definitions_and_generated_at():
    async def go():
        async with make_db() as db:
            return await overview(db)
    r = run(go())
    assert r["generated_at"].endswith("Z") and "success" in r["definitions"]
