"""
Admin API - Session Management
Includes: view sessions, disconnect users, bulk operations
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete
from sqlalchemy.orm import selectinload

from app.database import get_db
from app.models import VPNServer, VPNUserSession
from app.auth import verify_api_key

router = APIRouter(prefix="/admin", tags=["Admin - Sessions"])


@router.get("/servers/{server_id}/sessions")
async def get_server_sessions(
    server_id: int,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key)
):
    """Get all active user sessions for a specific server"""
    
    # Check if server exists
    server_result = await db.execute(select(VPNServer).where(VPNServer.id == server_id))
    server = server_result.scalar_one_or_none()
    
    if not server:
        raise HTTPException(status_code=404, detail="Server not found")
    
    # Get sessions
    result = await db.execute(
        select(VPNUserSession).where(VPNUserSession.server_id == server_id)
        .order_by(VPNUserSession.connected_time.desc())
    )
    sessions = result.scalars().all()
    
    return {
        "server_id": server.id,
        "server_name": server.name,
        "total_sessions": len(sessions),
        "sessions": [
            {
                "id": s.id,
                "user_id": s.user_id,
                "device_ip": s.device_ip,
                "config_tag": s.config_tag,
                "connected_time": s.connected_time,
                "bytes_received": s.bytes_received,
                "bytes_sent": s.bytes_sent,
                "total_bandwidth_mb": round((s.bytes_received + s.bytes_sent) / 1024 / 1024, 2)
            }
            for s in sessions
        ]
    }


@router.delete("/sessions/{session_id}")
async def delete_session(
    session_id: int,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key)
):
    """Delete/disconnect a specific user session"""
    
    result = await db.execute(select(VPNUserSession).where(VPNUserSession.id == session_id))
    session = result.scalar_one_or_none()
    
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    
    user_id = session.user_id
    server_id = session.server_id
    
    await db.delete(session)
    await db.commit()
    
    return {
        "message": f"Session for user '{user_id}' deleted successfully",
        "session_id": session_id,
        "server_id": server_id
    }


@router.delete("/sessions/bulk-delete")
async def bulk_delete_sessions(
    session_ids: list[int],
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key)
):
    """Disconnect multiple user sessions at once"""
    
    if not session_ids:
        raise HTTPException(status_code=400, detail="No session IDs provided")
    
    result = await db.execute(
        delete(VPNUserSession).where(VPNUserSession.id.in_(session_ids))
    )
    
    await db.commit()
    
    return {
        "message": f"Successfully deleted {result.rowcount} sessions",
        "deleted_count": result.rowcount
    }


@router.delete("/servers/{server_id}/sessions/clear")
async def clear_server_sessions(
    server_id: int,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key)
):
    """Disconnect all users from a specific server"""
    
    # Check if server exists
    server_result = await db.execute(select(VPNServer).where(VPNServer.id == server_id))
    server = server_result.scalar_one_or_none()
    
    if not server:
        raise HTTPException(status_code=404, detail="Server not found")
    
    result = await db.execute(
        delete(VPNUserSession).where(VPNUserSession.server_id == server_id)
    )
    
    await db.commit()
    
    return {
        "message": f"Cleared all sessions from server '{server.name}'",
        "server_id": server_id,
        "deleted_sessions": result.rowcount
    }