"""Endpoint tests for the per-server graphs (GET /admin/analytics/servers/{id}/series) and the
custom-range support of the report."""
import asyncio
import json
import random
from datetime import datetime, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import insert
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import asyncpg as pg_asyncpg

from app.api import admin_analytics as api
from app.config import settings
from app.models import ServerTraffic5m, ServerTrafficHourly, ServerUsage5m, VPNServer
from conftest import make_db

H = 3600
D = 86400
T0 = 1_800_000_000                                   # an exact hour boundary
NOW_S = T0 + 1800
NOW = datetime.fromtimestamp(NOW_S, tz=timezone.utc)


def dt(epoch):
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def ziso(epoch):
    return dt(epoch).isoformat().replace("+00:00", "Z")


@pytest.fixture(autouse=True)
def clock_and_cache(monkeypatch):
    store = {"calls": {"get": 0, "set": 0}, "data": {}, "ttl": {}}

    async def get_cache(k):
        store["calls"]["get"] += 1
        return store["data"].get(k)

    async def set_cache(k, v, ttl=3):
        store["calls"]["set"] += 1
        store["data"][k] = json.loads(json.dumps(v))
        store["ttl"][k] = ttl

    monkeypatch.setattr(api, "_utc_now", lambda: NOW)
    monkeypatch.setattr(api, "get_cache", get_cache)
    monkeypatch.setattr(api, "set_cache", set_cache)
    return store


def run(coro):
    return asyncio.run(coro)


async def seed_servers(db):
    db.add_all([
        VPNServer(id=1, name="a", ip_address="1.1.1.1", app_name="appA", server_type="free", max_capacity=100, is_active=True),
        VPNServer(id=2, name="b", ip_address="1.1.1.1", app_name="appB", server_type="free", max_capacity=100, is_active=True),
        VPNServer(id=3, name="c", ip_address="1.1.1.1", app_name="appC", server_type="premium", max_capacity=40, is_active=True),
        VPNServer(id=4, name="d", ip_address="2.2.2.2", app_name="appA", server_type="free", max_capacity=60, is_active=True),
    ])
    await db.commit()


async def bulk(db, model, rows):
    for i in range(0, len(rows), 5000):
        await db.execute(insert(model), rows[i:i + 5000])
    await db.commit()


def row5(epoch, sid, c, p, a, s, f):
    return dict(bucket_start=dt(epoch), server_id=sid, country=c, protocol=p, assigned=a, success=s, failed=f)


def hourly_from_5m(rows5):
    """What the flush task's hourly rebuild produces: the sum of the 5-minute rows of each hour."""
    acc = {}
    for r in rows5:
        h = int(r["bucket_start"].timestamp()) // H * H
        k = (h, r["server_id"], r["country"], r["protocol"])
        a = acc.setdefault(k, [0, 0, 0])
        a[0] += r["assigned"]; a[1] += r["success"]; a[2] += r["failed"]
    return [dict(bucket_start=dt(h), server_id=sid, country=c, protocol=p, assigned=v[0], success=v[1], failed=v[2])
            for (h, sid, c, p), v in acc.items()]


def usage_row(epoch, sid, sessions, cap, active=True, ovpn=None):
    return dict(bucket_start=dt(epoch), server_id=sid, active_sessions=sessions,
                openvpn_sessions=sessions if ovpn is None else ovpn, shadowsocks_sessions=sessions - (sessions if ovpn is None else ovpn),
                max_capacity=cap, is_active=active)


def series(db, sid=1, period="24h", country=None, protocol=None, scope="server", resolution=None, frm=None, to=None):
    return api.server_series(sid, period, country, protocol, scope, resolution, db, "t", frm, to)


def report(db, sid=1, period="24h", country=None, protocol=None, scope="server", frm=None, to=None):
    return api.server_analytics(sid, period, country, protocol, scope, db, "t", frm, to)


def random_traffic(rng, sids=(1,), hours=27, countries=("PK", "AE", "US", "GB", "IN", "DE", "FR", "TR"), density=0.6):
    rows = []
    start = (NOW_S - hours * H) // 300 * 300
    for b in range(start, NOW_S + 1, 300):
        for sid in sids:
            for c in countries:
                for p in ("openvpn", "shadowsocks"):
                    if rng.random() < density:
                        rows.append(row5(b, sid, c, p, rng.randint(0, 9), rng.randint(0, 9), rng.randint(0, 5)))
    return rows


def points_total(res, metric):
    return sum(p[metric] or 0 for p in res["points"])


# ─── core correctness: graph == report ────────────────────────────────────────

@pytest.mark.parametrize("period", ["1h", "6h", "24h", "7d"])
def test_graph_points_add_up_exactly_to_the_report_cards(period):
    rng = random.Random(11)
    rows = random_traffic(rng, hours=30)

    async def go():
        async with make_db() as db:
            await seed_servers(db)
            await bulk(db, ServerTraffic5m, rows)
            await bulk(db, ServerTrafficHourly, hourly_from_5m(rows))
            return await series(db, period=period), await report(db, period=period)
    s, r = run(go())
    for metric in ("assigned", "success", "failed"):
        assert points_total(s, metric) == r["summary"][metric], (period, metric)
    assert s["window"]["from"] == r["period"]["from"]
    assert s["window"]["key"] == period


def test_filters_apply_to_graph_and_report_consistently():
    rng = random.Random(3)
    rows = random_traffic(rng)

    async def go():
        async with make_db() as db:
            await seed_servers(db)
            await bulk(db, ServerTraffic5m, rows)
            await bulk(db, ServerTrafficHourly, hourly_from_5m(rows))
            return (await series(db, country="pk", protocol="shadowsocks"),
                    await report(db, country="pk", protocol="shadowsocks"))
    s, r = run(go())
    assert s["countries"] == ["PK"] and s["filters"] == {"country": "PK", "protocol": "shadowsocks"}
    for metric in ("assigned", "success", "failed"):
        assert points_total(s, metric) == r["summary"][metric]
    assert all(p["protocols"]["openvpn"]["assigned"] == 0 for p in s["points"] if p["protocols"])


def test_other_servers_data_never_leaks_into_the_graph():
    rng = random.Random(4)
    mine = random_traffic(rng, sids=(1,), hours=5)
    other = random_traffic(rng, sids=(4,), hours=5)

    async def go():
        async with make_db() as db:
            await seed_servers(db)
            await bulk(db, ServerTraffic5m, mine + other)
            await bulk(db, ServerTrafficHourly, hourly_from_5m(mine + other))
            return await series(db, sid=1, period="6h")
    s = run(go())
    lo = s_from(s)
    expected = sum(r["assigned"] for r in mine if int(r["bucket_start"].timestamp()) >= lo)
    assert points_total(s, "assigned") == expected > 0


def s_from(res):
    return int(datetime.fromisoformat(res["window"]["from"].replace("Z", "+00:00")).timestamp())


# ─── countries: top N + OTHER, bounded however many exist ─────────────────────

def test_top_five_countries_get_lines_and_the_rest_are_folded_into_other():
    rows = []
    for rank, c in enumerate(["PK", "AE", "US", "GB", "IN", "DE", "FR", "TR"]):
        rows.append(row5(T0 - 600, 1, c, "openvpn", 100 - rank * 10, 100 - rank * 10, 0))
    async def go():
        async with make_db() as db:
            await seed_servers(db)
            await bulk(db, ServerTraffic5m, rows)
            await bulk(db, ServerTrafficHourly, hourly_from_5m(rows))
            return await series(db, period="1h")
    s = run(go())
    assert s["countries"] == ["PK", "AE", "US", "GB", "IN", "OTHER"]
    busy = next(p for p in s["points"] if p["assigned"])
    assert busy["countries"]["OTHER"]["success"] == (50 + 40 + 30)                # DE + FR + TR
    assert sum(c["success"] for c in busy["countries"].values()) == busy["success"]


def test_no_other_line_when_five_or_fewer_countries_exist():
    rows = [row5(T0 - 600, 1, c, "openvpn", 5, 5, 0) for c in ("PK", "AE", "US")]
    async def go():
        async with make_db() as db:
            await seed_servers(db)
            await bulk(db, ServerTraffic5m, rows)
            await bulk(db, ServerTrafficHourly, hourly_from_5m(rows))
            return await series(db, period="1h")
    assert run(go())["countries"] == ["AE", "PK", "US"]            # equal activity: alphabetical, deterministic


def test_response_size_is_bounded_no_matter_how_many_countries_exist():
    """The scalability guarantee: 150 countries must not make the response (or the work done on it) larger."""
    rng = random.Random(9)
    countries = [chr(65 + i // 26) + chr(65 + i % 26) for i in range(150)]
    rows = []
    for b in range((NOW_S - 6 * H) // 300 * 300, NOW_S, 300):
        for c in countries:
            for p in ("openvpn", "shadowsocks"):
                rows.append(row5(b, 1, c, p, rng.randint(1, 5), rng.randint(0, 4), rng.randint(0, 2)))

    async def go():
        async with make_db() as db:
            await seed_servers(db)
            await bulk(db, ServerTraffic5m, rows)
            await bulk(db, ServerTrafficHourly, hourly_from_5m(rows))
            return await series(db, period="6h"), await report(db, period="6h")
    s, r = run(go())
    assert len(s["countries"]) <= 6 and len(s["points"]) <= 300
    assert len(json.dumps(s)) < 150_000
    for metric in ("assigned", "success", "failed"):
        assert points_total(s, metric) == r["summary"][metric]               # still exact


# ─── machine scope + usage / capacity ─────────────────────────────────────────

def test_machine_scope_sums_app_rows_and_uses_the_shared_capacity():
    # rows 1 and 2 are the same physical server + type (free, 1.1.1.1); row 3 is premium on that
    # ip and row 4 is another machine: neither belongs to the machine view of row 1.
    rows = [row5(T0 - 600, 1, "PK", "openvpn", 2, 2, 0), row5(T0 - 600, 2, "PK", "openvpn", 3, 3, 0),
            row5(T0 - 600, 3, "PK", "openvpn", 99, 99, 0), row5(T0 - 600, 4, "PK", "openvpn", 99, 99, 0)]
    usage = []
    for b in range((NOW_S - 2 * H) // 300 * 300, NOW_S, 300):
        usage += [usage_row(b, 1, 10, 100), usage_row(b, 2, 20, 100), usage_row(b, 3, 5, 40), usage_row(b, 4, 7, 60)]

    async def go():
        async with make_db() as db:
            await seed_servers(db)
            await bulk(db, ServerTraffic5m, rows)
            await bulk(db, ServerTrafficHourly, hourly_from_5m(rows))
            await bulk(db, ServerUsage5m, usage)
            return await series(db, sid=1, period="6h", scope="machine"), await series(db, sid=1, period="6h", scope="server")
    machine, single = run(go())
    assert sorted(machine["server_ids"]) == [1, 2] and single["server_ids"] == [1]
    assert machine["apps"] == ["appA", "appB"]
    assert points_total(machine, "assigned") == 2 + 3 and points_total(single, "assigned") == 2
    snap = next(p for p in machine["points"] if p["sessions"])
    assert snap["sessions"]["peak"] == 30                                      # 10 + 20 across the app rows
    assert snap["capacity"] == 100                                             # shared machine capacity, not 100 + 100
    assert snap["utilization"]["peak"] == 30.0


def test_machine_scope_matches_the_report_grouping_rule():
    rows = [row5(T0 - 600, sid, "PK", "openvpn", 1, 1, 0) for sid in (1, 2, 3, 4)]

    async def go():
        async with make_db() as db:
            await seed_servers(db)
            await bulk(db, ServerTraffic5m, rows)
            await bulk(db, ServerTrafficHourly, hourly_from_5m(rows))
            return await series(db, sid=1, period="1h", scope="machine"), await report(db, sid=1, period="1h", scope="machine")
    s, r = run(go())
    assert sorted(s["server_ids"]) == sorted(r["server_ids"]) == [1, 2]
    assert points_total(s, "assigned") == r["summary"]["assigned"] == 2


def test_sessions_capacity_and_utilization_come_from_the_snapshots_not_from_today():
    usage = []
    for b in range((NOW_S - 3 * H) // 300 * 300, NOW_S, 300):
        cap = 100 if b < NOW_S - H else 400                                    # capacity raised an hour ago
        usage.append(usage_row(b, 1, 50, cap))

    async def go():
        async with make_db() as db:
            await seed_servers(db)                                             # the server row says 100 today
            await bulk(db, ServerUsage5m, usage)
            return await series(db, period="6h")
    s = run(go())
    old = next(p for p in s["points"] if p["capacity"] == 100)
    new = next(p for p in s["points"] if p["capacity"] == 400)
    assert old["utilization"]["peak"] == 50.0 and new["utilization"]["peak"] == 12.5     # history not rewritten
    assert s["usage_start"] is not None


def test_a_disabled_period_is_reported_without_capacity():
    usage = [usage_row(b, 1, 0, 100, active=(b >= NOW_S - H)) for b in range((NOW_S - 2 * H) // 300 * 300, NOW_S, 300)]

    async def go():
        async with make_db() as db:
            await seed_servers(db)
            await bulk(db, ServerUsage5m, usage)
            return await series(db, period="6h")
    s = run(go())
    disabled = [p for p in s["points"] if p["server_active"] is False]
    enabled = [p for p in s["points"] if p["server_active"]]
    assert disabled and enabled
    assert all(p["capacity"] is None and p["utilization"] is None for p in disabled)
    assert all(p["capacity"] == 100 for p in enabled)


def test_buckets_before_data_exists_are_null_and_after_it_started_they_are_zero():
    rows = [row5(NOW_S - 5 * H, 1, "PK", "openvpn", 4, 3, 1)]          # recording began 5 hours ago

    async def go():
        async with make_db() as db:
            await seed_servers(db)
            await bulk(db, ServerTraffic5m, rows)
            await bulk(db, ServerTrafficHourly, hourly_from_5m(rows))
            return await series(db, period="7d")
    s = run(go())
    assert s["points"][0]["assigned"] is None                                 # a week ago: nothing was recorded yet
    assert s["points"][-1]["assigned"] == 0                                   # recorded period, quiet hour
    assert s["data_start"] is not None


def test_empty_database_gives_a_complete_grid_of_nulls_not_an_error():
    async def go():
        async with make_db() as db:
            await seed_servers(db)
            return await series(db, period="24h")
    s = run(go())
    assert s["countries"] == [] and s["data_start"] is None and s["usage_start"] is None
    assert len(s["points"]) == s["window"]["points"] > 0
    assert all(p["assigned"] is None and p["sessions"] is None for p in s["points"])


# ─── custom ranges ────────────────────────────────────────────────────────────

def test_custom_range_for_graph_and_report_cover_the_same_data():
    rng = random.Random(5)
    rows = random_traffic(rng, hours=40)
    frm, to = ziso(NOW_S - 30 * H), ziso(NOW_S - 6 * H)

    async def go():
        async with make_db() as db:
            await seed_servers(db)
            await bulk(db, ServerTraffic5m, rows)
            await bulk(db, ServerTrafficHourly, hourly_from_5m(rows))
            return await series(db, period="custom", frm=frm, to=to), await report(db, period="custom", frm=frm, to=to)
    s, r = run(go())
    assert s["window"]["key"] == "custom" and not s["window"]["clamped"]
    assert s["window"]["from"] == r["period"]["from"] and s["window"]["to"] == r["period"]["to"]
    for metric in ("assigned", "success", "failed"):
        assert points_total(s, metric) == r["summary"][metric]
    lo, hi = s_from(s), int(datetime.fromisoformat(s["window"]["to"].replace("Z", "+00:00")).timestamp())
    expected = sum(x["success"] for x in hourly_from_5m(rows) if lo <= int(x["bucket_start"].timestamp()) < hi)
    assert r["summary"]["success"] == expected                                 # exact, aligned to the report table


def test_custom_range_in_the_future_is_clamped_and_flagged():
    async def go():
        async with make_db() as db:
            await seed_servers(db)
            return await series(db, period="custom", frm=ziso(NOW_S - 2 * H), to=ziso(NOW_S + 9 * D))
    s = run(go())
    assert s["window"]["clamped"] is True


@pytest.mark.parametrize("frm,to", [
    (None, None), ("2026-01-02T00:00:00Z", "2026-01-01T00:00:00Z"), ("garbage", "2026-01-01T00:00:00Z"),
    ("2020-01-01T00:00:00Z", "2020-02-01T00:00:00Z"),
])
def test_invalid_custom_ranges_are_422_not_500(frm, to):
    async def go():
        async with make_db() as db:
            await seed_servers(db)
            with pytest.raises(HTTPException) as e:
                await series(db, period="custom", frm=frm, to=to)
            with pytest.raises(HTTPException) as e2:
                await report(db, period="custom", frm=frm, to=to)
            return e.value.status_code, e2.value.status_code
    assert run(go()) == (422, 422)


def test_requested_resolution_is_used_when_valid_and_flagged_when_adjusted():
    async def go():
        async with make_db() as db:
            await seed_servers(db)
            return await series(db, period="24h", resolution=1800), await series(db, period="24h", resolution=700)
    ok, adjusted = run(go())
    assert ok["window"]["step_seconds"] == 1800 and ok["window"]["resolution_adjusted"] is False
    assert adjusted["window"]["step_seconds"] == 900 and adjusted["window"]["resolution_adjusted"] is True


def test_every_preset_stays_within_the_point_cap():
    async def go():
        async with make_db() as db:
            await seed_servers(db)
            return [await series(db, period=p) for p in ("1h", "6h", "24h", "7d", "30d")]
    for s in run(go()):
        assert 1 <= len(s["points"]) <= 300


# ─── errors, caching ──────────────────────────────────────────────────────────

def test_unknown_server_is_404_and_not_cached(clock_and_cache):
    async def go():
        async with make_db() as db:
            with pytest.raises(HTTPException) as e:
                await series(db, sid=999)
            return e.value.status_code
    assert run(go()) == 404
    assert clock_and_cache["calls"]["set"] == 0


def test_results_are_cached_with_a_ttl_that_grows_with_the_range(clock_and_cache, monkeypatch):
    calls = []
    real = api._series_report

    async def counting(*a, **k):
        calls.append(1)
        return await real(*a, **k)

    monkeypatch.setattr(api, "_series_report", counting)

    async def go():
        async with make_db() as db:
            await seed_servers(db)
            await series(db, period="24h")
            await series(db, period="24h")                    # identical request: served from cache
            await series(db, period="24h", country="pk")      # different filter: its own entry
            await series(db, period="30d")
    run(go())
    assert len(calls) == 3
    ttls = sorted(set(clock_and_cache["ttl"].values()))
    assert ttls == [api.SERIES_CACHE_TTL["24h"], api.SERIES_CACHE_TTL["30d"]]
    assert api.SERIES_CACHE_TTL["30d"] > api.SERIES_CACHE_TTL["24h"] > 0


def test_a_broken_cache_never_breaks_the_graph_endpoint(monkeypatch):
    async def boom(*a, **k):
        raise ConnectionError("redis down")

    monkeypatch.setattr(api, "get_cache", boom)
    monkeypatch.setattr(api, "set_cache", boom)

    async def go():
        async with make_db() as db:
            await seed_servers(db)
            return await series(db)
    assert run(go())["points"]


# ─── multi-id first-bucket lookup + PostgreSQL SQL shape ──────────────────────

def test_first_bucket_returns_the_earliest_over_several_servers():
    async def go():
        async with make_db() as db:
            await seed_servers(db)
            await bulk(db, ServerTrafficHourly, [
                dict(bucket_start=dt(T0 - 5 * H), server_id=1, country="PK", protocol="openvpn", assigned=1, success=1, failed=0),
                dict(bucket_start=dt(T0 - 9 * H), server_id=2, country="PK", protocol="openvpn", assigned=1, success=1, failed=0),
                dict(bucket_start=dt(T0 - 1 * H), server_id=3, country="PK", protocol="openvpn", assigned=1, success=1, failed=0),
            ])
            return (await api._first_bucket(db, ServerTrafficHourly, [1, 2, 3]),
                    await api._first_bucket(db, ServerTrafficHourly, [1]),
                    await api._first_bucket(db, ServerTrafficHourly, [4]))
    both, one, none = run(go())
    assert int(both.timestamp()) == T0 - 9 * H and int(one.timestamp()) == T0 - 5 * H and none is None


def test_malformed_country_codes_are_never_inlined_into_sql_and_never_lose_data():
    """Country codes are validated at write time; this guards the one place they are inlined into SQL."""
    rows = [row5(T0 - 600, 1, "PK", "openvpn", 10, 10, 0),
            row5(T0 - 600, 1, "P'", "openvpn", 3, 3, 0),             # quote
            row5(T0 - 600, 1, "X\n", "openvpn", 2, 2, 0),            # trailing newline
            row5(T0 - 600, 1, "'; DROP TABLE vpn_status_vpnserver; --", "openvpn", 1, 1, 0)]

    async def go():
        async with make_db() as db:
            await seed_servers(db)
            await bulk(db, ServerTraffic5m, rows)
            await bulk(db, ServerTrafficHourly, hourly_from_5m(rows))
            res = await series(db, period="1h")
            still_there = (await db.execute(__import__("sqlalchemy").select(VPNServer))).scalars().all()
            return res, len(still_there)
    s, servers_left = run(go())
    assert servers_left == 4                                                  # nothing was executed
    assert s["countries"] == ["PK"] or s["countries"][0] == "PK"
    assert points_total(s, "assigned") == 10 + 3 + 2 + 1                       # nothing lost: odd codes fall into OTHER


def test_the_country_grouping_sql_is_valid_for_postgresql_identical_in_select_and_group_by():
    """PostgreSQL rejects 'must appear in GROUP BY' when a CASE contains bind parameters (select and
    group-by copies get different $n). The series query therefore inlines the (validated) codes."""
    rows = [row5(T0 - 600, 1, c, "openvpn", 5, 5, 0) for c in ("PK", "AE", "US", "GB", "IN", "DE", "FR")]
    seen = []

    async def go():
        async with make_db() as db:
            await seed_servers(db)
            await bulk(db, ServerTraffic5m, rows)
            await bulk(db, ServerTrafficHourly, hourly_from_5m(rows))
            real = db.execute

            async def spy(stmt, *a, **k):
                try:
                    seen.append(str(stmt.compile(dialect=pg_asyncpg.dialect())))
                except Exception:
                    pass
                return await real(stmt, *a, **k)

            db.execute = spy
            return await series(db, period="1h")
    run(go())
    grouped = [q for q in seen if "CASE WHEN server_traffic_5m.country IN" in q]
    assert grouped, seen
    q = grouped[0]
    select_part, group_part = q.split("GROUP BY")
    expr = q[q.index("CASE WHEN"):q.index(" END") + 4]
    assert expr in group_part and expr in select_part
    assert "$" not in expr                                                  # no bind parameters inside the CASE


def test_statement_timeout_is_set_only_on_postgresql():
    class Bind:
        class dialect:
            name = "postgresql"

    class FakeDb:
        def __init__(self):
            self.stmts = []

        def get_bind(self):
            return Bind()

        async def execute(self, stmt):
            self.stmts.append(str(stmt))

    db = FakeDb()
    run(api._guard_timeout(db))
    assert db.stmts == [f"SET LOCAL statement_timeout = '{api.QUERY_TIMEOUT_SECONDS}s'"]

    async def sqlite():
        async with make_db() as sdb:
            await api._guard_timeout(sdb)                                   # no error, nothing executed
    run(sqlite())


def test_the_new_routes_are_registered_and_do_not_change_existing_ones():
    import main
    paths = {r.path for r in main.app.routes}
    assert "/admin/analytics/servers/{server_id}/series" in paths
    assert "/admin/analytics/servers/{server_id}" in paths
    schema = main.app.openapi()["paths"]
    names = {p["name"] for p in schema["/admin/analytics/servers/{server_id}/series"]["get"]["parameters"]}
    assert {"period", "country", "protocol", "scope", "resolution", "from", "to"} <= names
    assert "from" in {p["name"] for p in schema["/admin/analytics/servers/{server_id}"]["get"]["parameters"]}
