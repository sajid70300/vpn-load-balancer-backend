"""
Regression tests for machine edits (admin_machines.py).

Guards the production bug where saving a capacity change reset premium
servers to free, and the real-time capacity / cache behaviour.
"""
import asyncio

from sqlalchemy import select

from app.api import admin_machines as am
from app.decision_engine import DecisionEngine
from app.models import PhysicalMachine, VPNServer, VPNUserSession
from conftest import make_db

ROUTING_CACHES = {"best_server_v2:*", "best_server:*", "servers_load:*", "servers_config:*"}


def _types(rows):
    return [(r.app_name, r.server_type) for r in rows]


async def _machine_with_rows(db):
    m = PhysicalMachine(name="m", ip_address="1.1.1.1", server_type="free", max_capacity=500)
    db.add(m)
    await db.flush()

    def row(app, typ):
        return VPNServer(name="m", ip_address="1.1.1.1", app_name=app, server_type=typ,
                         max_capacity=500, is_active=True, physical_machine_id=m.id,
                         cpu_usage=10, ram_usage=10, ping_latency_ms=20, load_score=5)

    rows = [row("a", "free"), row("b", "premium"), row("c", "free"), row("c", "premium")]
    db.add_all(rows)
    await db.commit()
    return m, rows


def _patch(monkeypatch, noop_audit):
    audit = noop_audit(am)
    deleted = []

    async def fake_delete(pattern):
        deleted.append(pattern)

    monkeypatch.setattr(am, "delete_cache", fake_delete)
    return audit, deleted


def test_capacity_edit_never_changes_premium(monkeypatch, noop_audit):
    """The exact payload the Edit Machine form sends (type included)."""
    audit, deleted = _patch(monkeypatch, noop_audit)

    async def go():
        async with make_db() as db:
            m, rows = await _machine_with_rows(db)
            await am.update_machine(
                m.id, am.MachineUpdate(name="m", ip_address="1.1.1.1", server_type="free", max_capacity=900),
                db, "tok")
            assert _types(rows) == [("a", "free"), ("b", "premium"), ("c", "free"), ("c", "premium")]
            assert all(r.max_capacity == 900 for r in rows)
            assert set(deleted) == ROUTING_CACHES
            assert audit[-1]["details"]["max_capacity"] == 900
            assert "server_type_old" not in audit[-1]["details"]

    asyncio.run(go())


def test_capacity_only_payload_without_type(monkeypatch, noop_audit):
    _patch(monkeypatch, noop_audit)

    async def go():
        async with make_db() as db:
            m, rows = await _machine_with_rows(db)
            await am.update_machine(m.id, am.MachineUpdate(max_capacity=2000), db, "tok")
            assert _types(rows)[1] == ("b", "premium")
            assert all(r.max_capacity == 2000 for r in rows)

    asyncio.run(go())


def test_deliberate_type_change_keeps_per_app_overrides(monkeypatch, noop_audit):
    audit, _ = _patch(monkeypatch, noop_audit)

    async def go():
        async with make_db() as db:
            m, rows = await _machine_with_rows(db)
            await am.update_machine(m.id, am.MachineUpdate(server_type="premium"), db, "tok")
            got = _types(rows)
            assert got[0] == ("a", "premium")                 # followed the old default
            assert got[1] == ("b", "premium")                 # override untouched
            assert got[2] == ("c", "free")                    # would duplicate (c, premium) -> skipped
            assert got[3] == ("c", "premium")
            d = audit[-1]["details"]
            assert d["server_type_old"] == "free" and d["server_type_new"] == "premium"
            assert d["server_rows_changed"] == 1 and d["server_rows_kept_own_type"] == 3

    asyncio.run(go())


def test_toggle_and_delete_clear_routing_caches(monkeypatch, noop_audit):
    _, deleted = _patch(monkeypatch, noop_audit)

    async def go():
        async with make_db() as db:
            m, _rows = await _machine_with_rows(db)
            deleted.clear()
            await am.toggle_machine_active(m.id, db, "tok")
            assert set(deleted) == ROUTING_CACHES
            deleted.clear()
            await am.delete_machine(m.id, db, "tok")
            assert set(deleted) == ROUTING_CACHES

    asyncio.run(go())


def test_redis_failure_does_not_break_a_saved_change(monkeypatch, noop_audit):
    noop_audit(am)

    async def boom(_pattern):
        raise ConnectionError("redis down")

    monkeypatch.setattr(am, "delete_cache", boom)

    async def go():
        async with make_db() as db:
            m = PhysicalMachine(name="n", ip_address="2.2.2.2", server_type="free", max_capacity=10)
            db.add(m)
            await db.commit()
            await am.update_machine(m.id, am.MachineUpdate(max_capacity=20), db, "tok")
            stored = (await db.execute(
                select(PhysicalMachine.max_capacity).where(PhysicalMachine.id == m.id))).scalar()
            assert stored == 20

    asyncio.run(go())


def test_load_balancer_sees_new_capacity_immediately(monkeypatch, noop_audit):
    """A full server becomes selectable the moment its capacity is raised."""
    _patch(monkeypatch, noop_audit)

    async def go():
        async with make_db() as db:
            m = PhysicalMachine(name="m", ip_address="1.1.1.1", server_type="free", max_capacity=100)
            db.add(m)
            await db.flush()
            s = VPNServer(name="m", ip_address="1.1.1.1", app_name="appA", server_type="premium",
                          max_capacity=100, is_active=True, physical_machine_id=m.id,
                          cpu_usage=10, ram_usage=10, ping_latency_ms=20, load_score=5)
            db.add(s)
            await db.flush()
            db.add_all([VPNUserSession(server_id=s.id, user_id=f"u{i}", device_ip="1.1.1.1") for i in range(100)])
            await db.commit()

            engine = DecisionEngine(db)
            assert await engine._load_servers("appA", None) == []          # 100/100: full

            await am.update_machine(
                m.id, am.MachineUpdate(name="m", ip_address="1.1.1.1", server_type="free", max_capacity=1000),
                db, "tok")
            after = await engine._load_servers("appA", None)
            assert len(after) == 1 and after[0]["max_capacity"] == 1000
            assert after[0]["server"].server_type == "premium"             # premium untouched

    asyncio.run(go())
