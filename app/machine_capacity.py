"""
Machine-level capacity.

One physical machine can be finalized for several apps; each (machine, app) pair
is its own VPNServer row, and each row used to be checked against the machine's
FULL capacity using only that app's sessions. A machine shared by 5 apps could
therefore hold ~5x its capacity before any single app saw it as full, and no
"capacity reached" alert ever fired.

This module is the single place that answers "how many users are on this
machine, in total, across every app?":

  * load_machine_loads()            async, used by the routing decision
                                    (decision_engine._load_server_tiers).
  * check_machine_capacity_alerts() sync, used by the Celery task
                                    `machine_capacity_check` (every ~60s).

Rules:
  * Total = every session on every VPNServer row of the machine (both
    protocols, all apps, active or not: sessions are physical connections).
  * Capacity = PhysicalMachine.max_capacity (the number shown in the admin
    panel). The per-app rows carry a copy of it.
  * Full = total >= capacity (and capacity > 0, same convention as before).

MACHINE_CAPACITY_ENABLED=false is the kill switch: routing and alerts go back to
the previous per-app behaviour exactly, and the Celery task does nothing.
"""
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, Optional, Tuple

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from app.config import settings
from app.models import Notification, PhysicalMachine, VPNServer, VPNUserSession

logger = logging.getLogger(__name__)

ALERT_TYPE = "capacity_reached"


async def load_machine_loads(
    db: AsyncSession, machine_ids: Iterable[int]
) -> Dict[int, Tuple[int, int]]:
    """{machine_id: (capacity, total_sessions)} for the given machines.

    One grouped query. Machines with no VPNServer rows are simply absent (a
    machine only ever matters here because one of its rows was just loaded).
    """
    ids = list(machine_ids)
    if not ids:
        return {}

    result = await db.execute(
        select(
            PhysicalMachine.id,
            PhysicalMachine.max_capacity,
            func.count(VPNUserSession.id),
        )
        .select_from(PhysicalMachine)
        .join(VPNServer, VPNServer.physical_machine_id == PhysicalMachine.id)
        .outerjoin(VPNUserSession, VPNUserSession.server_id == VPNServer.id)
        .where(PhysicalMachine.id.in_(ids))
        .group_by(PhysicalMachine.id, PhysicalMachine.max_capacity)
    )
    return {mid: (cap or 0, total or 0) for mid, cap, total in result.all()}


def check_machine_capacity_alerts(db: Session, now: Optional[datetime] = None) -> int:
    """Create a 'capacity_reached' notification for every active machine whose
    total users have reached its capacity. Returns how many were created.

    Anti-spam (the old per-app alert re-fired after every "mark all read"):
      * nothing is created while an UNREAD capacity alert exists for that IP, and
      * nothing is created if any capacity alert for that IP was created within
        the last CAPACITY_ALERT_REPEAT_MINUTES (even if it has been read).

    Short and local on purpose: one grouped query, a couple of tiny lookups,
    one commit. No network calls, so the connection is never idle in a
    transaction (PostgreSQL kills those after 30s on this deployment).
    """
    if not settings.MACHINE_CAPACITY_ENABLED:
        return 0

    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=max(0, int(settings.CAPACITY_ALERT_REPEAT_MINUTES)))

    apps_with_users = func.count(
        func.distinct(case((VPNUserSession.id.isnot(None), VPNServer.app_name)))
    )
    total_users = func.count(VPNUserSession.id)

    full_machines = db.execute(
        select(
            PhysicalMachine.name,
            PhysicalMachine.ip_address,
            PhysicalMachine.max_capacity,
            total_users,
            apps_with_users,
        )
        .select_from(PhysicalMachine)
        .join(VPNServer, VPNServer.physical_machine_id == PhysicalMachine.id)
        .outerjoin(VPNUserSession, VPNUserSession.server_id == VPNServer.id)
        .where(PhysicalMachine.is_active == True, PhysicalMachine.max_capacity > 0)  # noqa: E712
        .group_by(
            PhysicalMachine.id,
            PhysicalMachine.name,
            PhysicalMachine.ip_address,
            PhysicalMachine.max_capacity,
        )
        .having(total_users >= PhysicalMachine.max_capacity)
    ).all()

    created = 0
    for name, ip, capacity, total, apps in full_machines:
        blocked = db.execute(
            select(Notification.id)
            .where(
                Notification.type == ALERT_TYPE,
                Notification.server_ip == ip,
                (Notification.is_read == False) | (Notification.created_at >= cutoff),  # noqa: E712
            )
            .limit(1)
        ).first()
        if blocked:
            continue

        db.add(Notification(
            type=ALERT_TYPE,
            server_id=None,
            server_name=name,
            server_ip=ip,
            app_name=None,
            message=(
                f"Server '{name}' ({ip}) has reached its maximum capacity of {capacity} "
                f"users ({total} active across {apps} app(s))."
            ),
            is_read=False,
        ))
        created += 1

    if created:
        db.commit()
    else:
        db.rollback()   # nothing to keep: end the read transaction right away
    return created


def run_machine_capacity_check() -> int:
    """Celery entry point. Never raises: a failure here must not affect the
    monitoring tasks that share this worker."""
    if not settings.MACHINE_CAPACITY_ENABLED:
        return 0
    db = None
    try:
        from app.database import SyncSessionLocal

        db = SyncSessionLocal()
        created = check_machine_capacity_alerts(db)
        if created:
            logger.warning("machine_capacity_check: %d capacity alert(s) created", created)
        return created
    except Exception as e:
        if db is not None:
            try:
                db.rollback()
            except Exception:
                pass
        logger.error("machine_capacity_check failed: %r", e)
        return 0
    finally:
        if db is not None:
            try:
                db.close()
            except Exception:
                pass
