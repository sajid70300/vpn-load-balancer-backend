"""
API Router Registration
Imports and exports all API routers
"""

from app.api import public, admin_servers, admin_sessions, admin_stats, admin_metrics, admin_apps, admin_machines, admin_notifications

# Export all routers
__all__ = [
    "public",
    "admin_servers",
    "admin_sessions",
    "admin_stats",
    "admin_metrics",
    "admin_apps",
    "admin_machines",
    "admin_notifications",
]