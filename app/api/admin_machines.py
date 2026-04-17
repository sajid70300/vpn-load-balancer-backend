"""
Admin API - Physical Machine Management

Step 1 of the 2-step server flow:
  Server Management (this file) → create bare machines (IP, location, capacity, type).
  AppConfigure (admin_apps.py)  → finalize a machine for an app with protocol config
                                   which creates the actual VPNServer rows.

This separation means:
  - The same physical IP is registered ONCE here.
  - It can be finalized for multiple apps with completely different protocol settings.
  - The decision engine only ever touches VPNServer rows (unchanged).
"""

from fastapi import APIRouter, Depends, Query, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, delete
from pydantic import BaseModel
from typing import Optional

from app.database import get_db
from app.models import PhysicalMachine, VPNServer
from app.auth import verify_api_key
from app.audit import audit_log

router = APIRouter(prefix="/admin/machines", tags=["Admin - Physical Machines"])


# ─── Pydantic schemas ─────────────────────────────────────────────────────────

class MachineCreate(BaseModel):
    name:               str
    ip_address:         str
    server_type:        str = "free"        # free | premium
    server_city:        Optional[str] = None
    server_country:     Optional[str] = None
    flag_image_url:     Optional[str] = None
    max_capacity:       int = 100
    monitoring_api_url: Optional[str] = None
    is_active:          bool = True


class MachineUpdate(BaseModel):
    name:               Optional[str] = None
    ip_address:         Optional[str] = None
    server_type:        Optional[str] = None
    server_city:        Optional[str] = None
    server_country:     Optional[str] = None
    flag_image_url:     Optional[str] = None
    max_capacity:       Optional[int] = None
    monitoring_api_url: Optional[str] = None
    is_active:          Optional[bool] = None


def _machine_out(m: PhysicalMachine, finalized_apps: list[str] = None) -> dict:
    return {
        "id":                  m.id,
        "name":                m.name,
        "ip_address":          m.ip_address,
        "server_type":         m.server_type,
        "server_city":         m.server_city,
        "server_country":      m.server_country,
        "flag_image_url":      m.flag_image_url,
        "max_capacity":        m.max_capacity,
        "monitoring_api_url":  m.monitoring_api_url,
        "is_active":           m.is_active,
        "created_at":          m.created_at,
        "updated_at":          m.updated_at,
        "finalized_apps":      finalized_apps or [],  # which apps this machine is finalized for
    }


# ─── Endpoints ────────────────────────────────────────────────────────────────

@router.get("/")
async def list_machines(
    skip:        int            = Query(0, ge=0),
    limit:       int            = Query(200, ge=1, le=500),
    search:      Optional[str]  = None,
    server_type: Optional[str]  = Query(None, pattern="^(free|premium)$"),
    is_active:   Optional[bool] = None,
    db:          AsyncSession   = Depends(get_db),
    token:       str            = Depends(verify_api_key),
):
    """
    List all bare physical machines.
    Also returns which apps each machine has been finalized for.
    """
    conditions = []
    if server_type:
        conditions.append(PhysicalMachine.server_type == server_type)
    if is_active is not None:
        conditions.append(PhysicalMachine.is_active == is_active)
    if search:
        pat = f"%{search}%"
        conditions.append(
            PhysicalMachine.name.ilike(pat) |
            PhysicalMachine.ip_address.ilike(pat) |
            PhysicalMachine.server_city.ilike(pat)
        )

    q = select(PhysicalMachine).order_by(PhysicalMachine.name)
    if conditions:
        q = q.where(and_(*conditions))

    result = await db.execute(q)
    machines = result.scalars().all()

    # For each machine, find which apps it has been finalized for
    # Single query: get all VPNServer rows with physical_machine_id in our set
    machine_ids = [m.id for m in machines]
    finalized: dict[int, list[str]] = {m.id: [] for m in machines}

    if machine_ids:
        vs_result = await db.execute(
            select(VPNServer.physical_machine_id, VPNServer.app_name)
            .where(
                and_(
                    VPNServer.physical_machine_id.in_(machine_ids),
                    VPNServer.app_name.isnot(None),
                )
            )
            .distinct()
        )
        for pm_id, app_name in vs_result.all():
            if app_name and app_name not in finalized[pm_id]:
                finalized[pm_id].append(app_name)

    out = [_machine_out(m, finalized[m.id]) for m in machines]
    total = len(out)
    return {"total": total, "skip": skip, "limit": limit, "machines": out[skip: skip + limit]}


@router.get("/{machine_id}")
async def get_machine(
    machine_id: int,
    db:         AsyncSession = Depends(get_db),
    token:      str          = Depends(verify_api_key),
):
    result = await db.execute(select(PhysicalMachine).where(PhysicalMachine.id == machine_id))
    m = result.scalar_one_or_none()
    if not m:
        raise HTTPException(status_code=404, detail="Machine not found")

    vs_result = await db.execute(
        select(VPNServer.app_name)
        .where(
            and_(
                VPNServer.physical_machine_id == machine_id,
                VPNServer.app_name.isnot(None),
            )
        )
        .distinct()
    )
    finalized_apps = [row[0] for row in vs_result.all() if row[0]]
    return _machine_out(m, finalized_apps)


@router.post("/", status_code=status.HTTP_201_CREATED)
async def create_machine(
    payload: MachineCreate,
    db:      AsyncSession = Depends(get_db),
    token:   str          = Depends(verify_api_key),
):
    """
    Register a new bare physical machine.
    IP must be unique across all machines.
    Protocol configuration is added later in AppConfigure.
    """
    existing = await db.execute(
        select(PhysicalMachine).where(PhysicalMachine.ip_address == payload.ip_address)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=400,
            detail=f"A machine with IP '{payload.ip_address}' already exists."
        )

    m = PhysicalMachine(
        name               = payload.name,
        ip_address         = payload.ip_address,
        server_type        = payload.server_type,
        server_city        = payload.server_city,
        server_country     = payload.server_country,
        flag_image_url     = payload.flag_image_url,
        max_capacity       = payload.max_capacity,
        monitoring_api_url = payload.monitoring_api_url,
        is_active          = payload.is_active,
    )
    db.add(m)
    await db.commit()
    await db.refresh(m)

    await audit_log(db, token, action="machine.create", resource_type="machine",
        resource_id=str(m.id),
        details={"name": m.name, "ip_address": m.ip_address})

    return _machine_out(m)


@router.put("/{machine_id}")
async def update_machine(
    machine_id: int,
    payload:    MachineUpdate,
    db:         AsyncSession = Depends(get_db),
    token:      str          = Depends(verify_api_key),
):
    """
    Update bare machine info.
    If ip_address changes, also updates the ip_address on all linked VPNServer rows.
    """
    result = await db.execute(select(PhysicalMachine).where(PhysicalMachine.id == machine_id))
    m = result.scalar_one_or_none()
    if not m:
        raise HTTPException(status_code=404, detail="Machine not found")

    # Check IP uniqueness if changing
    if payload.ip_address and payload.ip_address != m.ip_address:
        dup = await db.execute(
            select(PhysicalMachine).where(PhysicalMachine.ip_address == payload.ip_address)
        )
        if dup.scalar_one_or_none():
            raise HTTPException(status_code=400,
                detail=f"IP '{payload.ip_address}' is already used by another machine.")

    for field in ("name", "ip_address", "server_type", "server_city", "server_country",
                  "flag_image_url", "max_capacity", "monitoring_api_url", "is_active"):
        val = getattr(payload, field)
        if val is not None:
            setattr(m, field, val)

    # Propagate shared fields to all linked VPNServer rows
    vs_result = await db.execute(
        select(VPNServer).where(VPNServer.physical_machine_id == machine_id)
    )
    vpn_rows = vs_result.scalars().all()
    for row in vpn_rows:
        if payload.name         is not None: row.name         = payload.name
        if payload.ip_address   is not None: row.ip_address   = payload.ip_address
        if payload.server_type  is not None: row.server_type  = payload.server_type
        if payload.server_city  is not None: row.server_city  = payload.server_city
        if payload.server_country is not None: row.server_country = payload.server_country
        if payload.flag_image_url is not None: row.flag_image_url = payload.flag_image_url
        if payload.max_capacity is not None: row.max_capacity = payload.max_capacity
        if payload.monitoring_api_url is not None: row.monitoring_api_url = payload.monitoring_api_url
        if payload.is_active    is not None: row.is_active    = payload.is_active

    await db.commit()
    await db.refresh(m)
    await audit_log(db, token, action="machine.update", resource_type="machine",
        resource_id=str(machine_id),
        details={"name": m.name, "ip_address": m.ip_address})

    vs_result2 = await db.execute(
        select(VPNServer.app_name)
        .where(and_(VPNServer.physical_machine_id == machine_id, VPNServer.app_name.isnot(None)))
        .distinct()
    )
    finalized_apps = [row[0] for row in vs_result2.all() if row[0]]
    return _machine_out(m, finalized_apps)


@router.delete("/{machine_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_machine(
    machine_id: int,
    db:         AsyncSession = Depends(get_db),
    token:      str          = Depends(verify_api_key),
):
    """
    Delete a bare machine. Because VPNServer has ON DELETE CASCADE on physical_machine_id,
    all finalized VPNServer rows (and their sessions) are also removed automatically.
    """
    result = await db.execute(select(PhysicalMachine).where(PhysicalMachine.id == machine_id))
    m = result.scalar_one_or_none()
    if not m:
        raise HTTPException(status_code=404, detail="Machine not found")

    name = m.name
    await db.delete(m)
    await db.commit()

    await audit_log(db, token, action="machine.delete", resource_type="machine",
        resource_id=str(machine_id),
        details={"name": name})


@router.post("/{machine_id}/toggle-active")
async def toggle_machine_active(
    machine_id: int,
    db:         AsyncSession = Depends(get_db),
    token:      str          = Depends(verify_api_key),
):
    """
    Toggle is_active on the machine AND all its linked VPNServer rows atomically.
    """
    result = await db.execute(select(PhysicalMachine).where(PhysicalMachine.id == machine_id))
    m = result.scalar_one_or_none()
    if not m:
        raise HTTPException(status_code=404, detail="Machine not found")

    new_state = not m.is_active
    m.is_active = new_state

    vs_result = await db.execute(
        select(VPNServer).where(VPNServer.physical_machine_id == machine_id)
    )
    for row in vs_result.scalars().all():
        row.is_active = new_state

    await db.commit()
    await audit_log(db, token, action="machine.toggle_active", resource_type="machine",
        resource_id=str(machine_id),
        details={"name": m.name, "is_active": new_state})

    return {"machine_id": machine_id, "is_active": new_state}