from celery import Celery
from celery.schedules import crontab

# Import config at module level to avoid circular imports
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from app.config import settings

# Create Celery app
celery_app = Celery(
    'vpn_load_balancer',
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND
)

# Configuration
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=300,
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
)

# Scheduled tasks
celery_app.conf.beat_schedule = {
    'monitor-vpn-every-18-seconds': {
        'task': 'monitor_vpn',
        'schedule': 18.0,
    },
    'fetch-metrics-every-10-seconds': {
        'task': 'monitor_metrics',
        'schedule': 10.0,
    },
    'cleanup-stale-shadowsocks-every-5-minutes': {
        'task': 'cleanup_shadowsocks',
        'schedule': 300.0,  # every 5 minutes
    },
    'update-geoip-databases-weekly': {
        'task': 'update_geoip',
        'schedule': crontab(hour=3, minute=0, day_of_week=2),  # every Tuesday at 3am UTC
    },
}

# Register tasks
from app.tasks import monitor_vpn_status, monitor_all_server_metrics, cleanup_stale_shadowsocks_sessions, update_geoip_databases

@celery_app.task(name='monitor_vpn')
def monitor_vpn():
    monitor_vpn_status()

@celery_app.task(name='monitor_metrics')
def monitor_metrics():
    monitor_all_server_metrics()

@celery_app.task(name='cleanup_shadowsocks')
def cleanup_shadowsocks():
    cleanup_stale_shadowsocks_sessions()

@celery_app.task(name='update_geoip')
def update_geoip():
    update_geoip_databases()