"""
Regression tests for the 2026-09-24 sync_server_sessions() batching fix
(app/tasks.py): updates and deletes for continuing/disconnected OpenVPN
sessions must now happen in ONE batched query each instead of one query per
user, WITHOUT changing the resulting database state.

Uses a real (sync, file-based-in-memory) SQLite database and SQLAlchemy's
real event system to count actual SQL statements issued — not a mock — so
the query-count claims are verified, not assumed.
"""
from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.models import VPNServer, VPNUserSession
from app.tasks import sync_server_sessions


@pytest.fixture
def db():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def server(db):
    s = VPNServer(name="s", ip_address="1.1.1.1", app_name="appA", server_type="free",
                  max_capacity=1000, is_active=True, peak_users=0)
    db.add(s)
    db.commit()
    return s


def _count_statements(db):
    """Context-manager-free statement counter using SQLAlchemy's real event hooks."""
    counter = {"n": 0}

    def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        counter["n"] += 1

    engine = db.get_bind()
    event.listen(engine, "before_cursor_execute", before_cursor_execute)
    return counter, engine, before_cursor_execute


def test_all_new_users_creates_sessions_unchanged(db, server):
    active_users = {(f"u{i}", "1.2.3.4"): {"config_tag": None, "bytes_received": 100, "bytes_sent": 50}
                    for i in range(5)}
    sync_server_sessions(db, server, active_users)
    db.commit()

    sessions = db.query(VPNUserSession).filter(VPNUserSession.server_id == server.id).all()
    assert len(sessions) == 5
    assert all(s.protocol == "openvpn" and s.bytes_received == 100 for s in sessions)
    assert server.peak_users == 5


def test_continuing_users_get_updated_bytes_correctly(db, server):
    # Seed 3 existing openvpn sessions.
    sync_server_sessions(db, server, {
        ("u1", "1.1.1.1"): {"bytes_received": 10, "bytes_sent": 5},
        ("u2", "1.1.1.1"): {"bytes_received": 20, "bytes_sent": 10},
        ("u3", "1.1.1.1"): {"bytes_received": 30, "bytes_sent": 15},
    })
    db.commit()
    ids_before = {(s.user_id, s.device_ip): s.id for s in db.query(VPNUserSession).all()}

    # Same 3 users, still connected, with NEW byte counts (this exercises the batched update path).
    sync_server_sessions(db, server, {
        ("u1", "1.1.1.1"): {"bytes_received": 111, "bytes_sent": 55},
        ("u2", "1.1.1.1"): {"bytes_received": 222, "bytes_sent": 66},
        ("u3", "1.1.1.1"): {"bytes_received": 333, "bytes_sent": 77},
    })
    db.commit()

    rows = {(s.user_id, s.device_ip): s for s in db.query(VPNUserSession).all()}
    assert len(rows) == 3, "no rows should be added or removed — all 3 were still connected"
    assert rows[("u1", "1.1.1.1")].bytes_received == 111 and rows[("u1", "1.1.1.1")].bytes_sent == 55
    assert rows[("u2", "1.1.1.1")].bytes_received == 222 and rows[("u2", "1.1.1.1")].bytes_sent == 66
    assert rows[("u3", "1.1.1.1")].bytes_received == 333 and rows[("u3", "1.1.1.1")].bytes_sent == 77
    # Same underlying rows were updated in place, not replaced.
    ids_after = {k: v.id for k, v in rows.items()}
    assert ids_after == ids_before


def test_disconnected_users_are_removed_correctly(db, server):
    sync_server_sessions(db, server, {
        ("u1", "1.1.1.1"): {"bytes_received": 1, "bytes_sent": 1},
        ("u2", "1.1.1.1"): {"bytes_received": 1, "bytes_sent": 1},
        ("u3", "1.1.1.1"): {"bytes_received": 1, "bytes_sent": 1},
    })
    db.commit()

    # u2 disconnects; u1 and u3 remain.
    sync_server_sessions(db, server, {
        ("u1", "1.1.1.1"): {"bytes_received": 1, "bytes_sent": 1},
        ("u3", "1.1.1.1"): {"bytes_received": 1, "bytes_sent": 1},
    })
    db.commit()

    remaining = {(s.user_id, s.device_ip) for s in db.query(VPNUserSession).filter(VPNUserSession.server_id == server.id).all()}
    assert remaining == {("u1", "1.1.1.1"), ("u3", "1.1.1.1")}


def test_disconnect_never_touches_shadowsocks_rows_on_same_server(db, server):
    """The batched delete adds an explicit protocol='openvpn' filter — verify
    it still never removes shadowsocks sessions, same as the original code."""
    ss = VPNUserSession(server_id=server.id, user_id="ss-user", device_ip="9.9.9.9", protocol="shadowsocks")
    db.add(ss)
    ov = VPNUserSession(server_id=server.id, user_id="ov-user", device_ip="1.1.1.1", protocol="openvpn")
    db.add(ov)
    db.commit()

    # OpenVPN's routing table now shows NO users at all (both would look "disconnected").
    sync_server_sessions(db, server, {})
    db.commit()

    remaining = db.query(VPNUserSession).filter(VPNUserSession.server_id == server.id).all()
    assert len(remaining) == 1 and remaining[0].protocol == "shadowsocks", \
        "shadowsocks session must survive an openvpn-only sync even when its user isn't in active_users"


def test_mixed_new_continuing_and_disconnected_in_one_call(db, server):
    sync_server_sessions(db, server, {
        ("stays", "1.1.1.1"):      {"bytes_received": 1, "bytes_sent": 1},
        ("disconnects", "1.1.1.1"): {"bytes_received": 1, "bytes_sent": 1},
    })
    db.commit()

    sync_server_sessions(db, server, {
        ("stays", "1.1.1.1"): {"bytes_received": 999, "bytes_sent": 888},
        ("new", "1.1.1.1"):   {"bytes_received": 5, "bytes_sent": 5},
    })
    db.commit()

    rows = {(s.user_id, s.device_ip): s for s in db.query(VPNUserSession).filter(VPNUserSession.server_id == server.id).all()}
    assert set(rows.keys()) == {("stays", "1.1.1.1"), ("new", "1.1.1.1")}
    assert rows[("stays", "1.1.1.1")].bytes_received == 999
    assert rows[("new", "1.1.1.1")].bytes_received == 5


def test_peak_users_tracking_unchanged(db, server):
    sync_server_sessions(db, server, {(f"u{i}", "1.1.1.1"): {"bytes_received": 0, "bytes_sent": 0} for i in range(10)})
    db.commit()
    assert server.peak_users == 10

    # Drop to 3 users — peak must NOT decrease.
    sync_server_sessions(db, server, {(f"u{i}", "1.1.1.1"): {"bytes_received": 0, "bytes_sent": 0} for i in range(3)})
    db.commit()
    assert server.peak_users == 10


def test_query_count_no_longer_scales_with_user_count(db, server):
    """The actual point of this fix: prove the number of SQL statements for
    updating N continuing users stays roughly constant, instead of growing
    1-for-1 with N as it did before."""
    N = 50
    sync_server_sessions(db, server, {(f"u{i}", "1.1.1.1"): {"bytes_received": 0, "bytes_sent": 0} for i in range(N)})
    db.commit()

    counter, engine, hook = _count_statements(db)
    try:
        # All N users "continuing" with new byte values — this is exactly the
        # path that used to issue N individual UPDATE statements.
        sync_server_sessions(db, server, {(f"u{i}", "1.1.1.1"): {"bytes_received": i, "bytes_sent": i} for i in range(N)})
        db.commit()
    finally:
        event.remove(engine, "before_cursor_execute", hook)

    print(f"SQL statements issued for {N} continuing users: {counter['n']}")
    # Old code: 1 (fetch existing) + N (individual updates) + 1 (final count) + commit-related
    #           = at least N+2 query-level statements, i.e. >= 52 here.
    # New code: 1 (fetch existing) + 1 (bulk update) + 1 (final count) = ~3-5 regardless of N.
    assert counter["n"] < N, (
        f"expected statement count to stay roughly constant (well under {N}), "
        f"got {counter['n']} — the batching fix may not be taking effect"
    )

    rows = {(s.user_id, s.device_ip): s.bytes_received for s in db.query(VPNUserSession).filter(VPNUserSession.server_id == server.id).all()}
    assert rows == {(f"u{i}", "1.1.1.1"): i for i in range(N)}, "batched update must still write correct per-row values"
