"""Tests for the overall-capacity summary and the history endpoint (admin_stats.py)."""
import asyncio
from datetime import datetime, timedelta

from sqlalchemy import text

from app.api import admin_stats
from app.models import ALL_APPS_KEY, ActiveUsersHistory, PhysicalMachine, VPNServer, VPNUserSession
from conftest import make_db


def _sessions(server, n, tag):
    return [VPNUserSession(server_id=server.id, user_id=f"{tag}{i}", device_ip="1.2.3.4") for i in range(n)]


def test_capacity_counts_shared_machine_once_and_only_active():
    async def go():
        async with make_db() as db:
            m1 = PhysicalMachine(name="m1", ip_address="1.1.1.1", max_capacity=1000)
            m2 = PhysicalMachine(name="m2", ip_address="2.2.2.2", max_capacity=500)
            m3 = PhysicalMachine(name="m3", ip_address="3.3.3.3", max_capacity=800)
            db.add_all([m1, m2, m3])
            await db.flush()

            def row(name, ip, app, typ, cap, active, mid):
                return VPNServer(name=name, ip_address=ip, app_name=app, server_type=typ,
                                 max_capacity=cap, is_active=active, physical_machine_id=mid)

            rows = [
                row("m1", "1.1.1.1", "a", "free", 1000, True, m1.id),      # m1 finalized 3 times
                row("m1", "1.1.1.1", "b", "free", 1000, True, m1.id),
                row("m1", "1.1.1.1", "a", "premium", 1000, True, m1.id),
                row("m2", "2.2.2.2", "a", "free", 500, True, m2.id),
                row("m3", "3.3.3.3", "a", "free", 800, False, m3.id),      # inactive: excluded
                row("legacy", "9.9.9.9", "a", "free", 200, True, None),    # no machine id: grouped by ip
            ]
            db.add_all(rows)
            await db.flush()
            db.add_all(_sessions(rows[0], 300, "a") + _sessions(rows[1], 200, "b") + _sessions(rows[2], 100, "c")
                       + _sessions(rows[3], 250, "d") + _sessions(rows[4], 999, "x") + _sessions(rows[5], 50, "l"))
            await db.commit()

            r = await admin_stats.get_capacity_summary(db, "t")
            assert r["total_max_capacity"] == 1700            # 1000 + 500 + 200, shared machine once
            assert r["row_capacity_sum"] == 3700              # raw row sum, for comparison only
            assert r["current_sessions"] == 900               # inactive server's 999 excluded
            assert r["available_capacity"] == 800
            assert r["overall_utilization_pct"] == round(900 / 1700 * 100, 2)
            assert r["active_machines"] == 3

            db.add_all(_sessions(rows[3], 2000, "z"))          # push over capacity
            await db.commit()
            r = await admin_stats.get_capacity_summary(db, "t")
            assert r["available_capacity"] == 0 and r["overall_utilization_pct"] > 100

    asyncio.run(go())


def test_capacity_with_no_servers_has_no_division_by_zero():
    async def go():
        async with make_db() as db:
            r = await admin_stats.get_capacity_summary(db, "t")
            assert r["total_max_capacity"] == 0 and r["overall_utilization_pct"] == 0.0
            assert r["available_capacity"] == 0

    asyncio.run(go())


def test_history_raw_bucketed_and_auto_bucketed():
    async def go():
        async with make_db() as db:
            base = datetime.utcnow().replace(second=0, microsecond=0) - timedelta(hours=23)
            pts = [ActiveUsersHistory(app_name=ALL_APPS_KEY, total_users=1000 + (i % 7),
                                      recorded_at=base + timedelta(minutes=i)) for i in range(1380)]
            pts[700].total_users = 9999                           # a one-minute spike
            pts.append(ActiveUsersHistory(app_name="other", total_users=5, recorded_at=base))
            db.add_all(pts)
            await db.commit()

            # few points: returned raw, exactly like before
            r = await admin_stats.get_user_history("24h", None, None, db, "t")
            assert r["bucket_minutes"] is None and len(r["points"]) == 1380
            assert r["collection_interval_minutes"] == 30

            # explicit resolution: max per bucket keeps the spike; ordered, tz-aware, aligned
            r = await admin_stats.get_user_history("24h", None, 30, db, "t")
            assert 46 <= len(r["points"]) <= 48
            assert max(p["total_users"] for p in r["points"]) == 9999
            times = [p["recorded_at"] for p in r["points"]]
            assert times == sorted(times) and all(t.tzinfo for t in times)
            assert all(int(t.timestamp()) % 1800 == 0 for t in times)

            # many points: aggregated automatically so the payload stays small
            db.add_all([ActiveUsersHistory(app_name=ALL_APPS_KEY, total_users=2000,
                                           recorded_at=base - timedelta(minutes=i + 1)) for i in range(4000)])
            await db.commit()
            r = await admin_stats.get_user_history("all", None, None, db, "t")
            assert r["bucket_minutes"] and 0 < len(r["points"]) <= 2000
            assert max(p["total_users"] for p in r["points"]) == 9999

            # per-app isolation
            r = await admin_stats.get_user_history("24h", "other", None, db, "t")
            assert [p["total_users"] for p in r["points"]] == [5]

    asyncio.run(go())


def test_history_reports_configured_collection_interval():
    async def go():
        async with make_db() as db:
            await db.execute(text("INSERT INTO global_settings (id, history_interval_minutes) VALUES (1, 7)"))
            await db.commit()
            r = await admin_stats.get_user_history("24h", None, None, db, "t")
            assert r["collection_interval_minutes"] == 7

    asyncio.run(go())
