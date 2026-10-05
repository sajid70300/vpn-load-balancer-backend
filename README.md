# VPN Load Balancer API

An intelligent VPN load balancing system with support for **OpenVPN** and **Shadowsocks** protocols, built with **FastAPI** and optimized for high-performance workloads.

## 🎯 Overview

This project provides a sophisticated VPN infrastructure management and load balancing solution. It intelligently distributes traffic across multiple VPN servers, implements protocol selection based on real-time metrics, enforces country and ISP policies, and provides comprehensive monitoring and audit capabilities.

## ✨ Key Features

### 🧠 Intelligent Decision Engine
- **Two-phase server & protocol selection** combining load scoring (CPU, RAM, ping, active sessions)
- **Automatic protocol selection** based on success rates and connection times
- **Policy-based routing** with country and ISP-level policy enforcement
- **Smart cooldown system** (soft/hard) for failed servers with country-level blocking

### 🔐 Multi-Protocol Support
- **OpenVPN** - Industry standard VPN protocol
- **Shadowsocks** - Lightweight proxy protocol
- **Same server architecture** - Both protocols run on the same VPN server row
- Automatic fallback between protocols

### 👥 Multi-Tenancy & Access Control
- **Multi-app support** - Multiple VPN applications/brands
- **Role-based access control** - Superadmin, Admin, User roles
- **User approval workflow** - First user becomes superadmin, others require approval

### 📊 Monitoring & Metrics
- **Real-time metrics collection** - CPU, RAM, ping, active sessions per server
- **Protocol-specific metrics** - Success rates, connection times by protocol, country, ASN
- **Server health monitoring** - Continuous health checks with configurable intervals
- **Celery-based background tasks** - Asynchronous monitoring and cleanup

### 🌍 Geographic & Network Policies
- **Country-based policies** - Restrict/prefer protocols by country
- **ISP-based policies** - Fine-grained policies by country + ASN combinations
- **Enforcement toggles** - Enable/disable policy enforcement globally
- **Policy override capabilities** - Admin controls

### 📝 Audit & Logging
- **Complete audit trail** - All admin actions logged with timestamps
- **Session tracking** - VPN user sessions with protocol and server details
- **Activity history** - Comprehensive event logging for compliance

### ⚙️ Advanced Configuration
- **Global settings** - Protocol mode, connection limits, policy enforcement, cooldown parameters
- **Per-server configuration** - Capacity limits, status, protocol support
- **Redis-backed caching** - Distributed cache for settings and session data
- **Database indexing** - Optimized queries with strategic indexes

## 🛠️ Tech Stack

- **Framework:** FastAPI (Python 3.11+)
- **Database:** PostgreSQL with SQLAlchemy ORM
- **Cache:** Redis (async with aioredis)
- **Task Queue:** Celery with Redis broker
- **API Documentation:** Swagger/OpenAPI, ReDoc
- **Web Server:** Uvicorn

## 📋 Prerequisites

- **Python 3.11+**
- **PostgreSQL 12+**
- **Redis 6+**
- **Operating System:** Linux/macOS/Windows (with WSL)

## 🚀 Quick Start

### 1. Clone & Setup Virtual Environment

```bash
# Create virtual environment
python -m venv myenv

# Activate (Windows)
myenv\Scripts\activate

# Activate (macOS/Linux)
source myenv/bin/activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Environment

Copy `.env.example` to `.env` and update with your settings:

```bash
# Database
DATABASE_URL=postgresql+asyncpg://user:password@localhost:5432/vpn_db
SYNC_DATABASE_URL=postgresql://user:password@localhost:5432/vpn_db

# Redis
REDIS_URL=redis://localhost:6379/0
CACHE_REDIS_URL=redis://localhost:6379/1

# Security
SECRET_KEY=your-secure-key-here
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=43200
API_KEY=your-api-key-here

# Celery
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/0

# Application
PROJECT_NAME="VPN Load Balancer API"
DEBUG=False
ALLOWED_ORIGINS=http://localhost:3000,http://localhost:5173
```

### 4. Start Services

**Terminal 1 - FastAPI Server:**
```bash
python main.py
```

**Terminal 2 - Celery Worker:**
```bash
celery -A celery_app worker --loglevel=info
```

**Terminal 3 - Celery Beat Scheduler:**
```bash
celery -A celery_app beat --loglevel=info
```

## 📚 API Documentation

Once running, access the interactive documentation:

- **Swagger UI:** http://localhost:8000/docs
- **ReDoc:** http://localhost:8000/redoc

### Public Endpoints

- `GET /v1/best_server` - Get best server (public selection)
- `GET /v1/best_server/auto` - Auto protocol selection
- `POST /v1/session/start` - Start VPN session
- `POST /v1/session/end` - End VPN session

### Admin Endpoints

- **Users:** `/admin/users/*` - User management
- **Servers:** `/admin/servers/*` - VPN server management
- **Sessions:** `/admin/sessions/*` - Session monitoring
- **Metrics:** `/admin/metrics/*` - Protocol & server metrics
- **Policies:** `/admin/policies/*` - Country & ISP policies
- **Settings:** `/admin/settings/*` - Global configuration
- **Audit:** `/admin/audit/*` - Audit logs
- **Machines:** `/admin/machines/*` - Machine/ASN management
- **Applications:** `/admin/apps/*` - Multi-app management
- **Notifications:** `/admin/notifications/*` - System notifications

## 📁 Project Structure

```
backend/
├── main.py                 # FastAPI app entry point
├── celery_app.py           # Celery configuration & tasks
├── migrate_db.py           # Database migration script
├── requirements.txt        # Python dependencies
├── .env                    # Environment variables (create from template)
│
└── app/
    ├── config.py           # Settings & configuration
    ├── database.py         # SQLAlchemy setup (async & sync)
    ├── models.py           # Database models (Users, Servers, Policies, etc.)
    ├── schemas.py          # Pydantic request/response schemas
    ├── auth.py             # JWT authentication & authorization
    ├── cache.py            # Redis cache operations
    ├── audit.py            # Audit logging
    ├── decision_engine.py   # Core algorithm for server & protocol selection
    ├── tasks.py            # Celery background tasks
    │
    └── api/
        ├── public.py           # Public VPN selection endpoints
        ├── admin_users.py       # User management
        ├── admin_servers.py     # Server management
        ├── admin_sessions.py    # Session management
        ├── admin_metrics.py     # Metrics & analytics
        ├── admin_policies.py    # Country & ISP policies
        ├── admin_settings.py    # Global settings
        ├── admin_audit.py       # Audit logs
        ├── admin_machines.py    # Machine/ASN management
        ├── admin_apps.py        # App/tenant management
        └── admin_notifications.py # System notifications
```

## 🔑 Core Concepts

### Decision Engine
The intelligent decision engine works in two phases:

**Phase 1:** Select best server based on comprehensive load scoring
- CPU utilization (normalized)
- RAM utilization (normalized)
- Network latency (ping)
- Active session count vs. capacity
- Smart cooldown avoidance

**Phase 2:** Choose protocol (OpenVPN or Shadowsocks)
- **Policy-first:** Apply country/ISP policies if enforce flags are ON
- **Auto-scoring:** Score protocols by success rate (70% weight) and connection time (30% weight)
- **Fallback:** Always provide fallback protocol if primary fails

### Cooldown System
Triggered when both protocols fail on a server for a specific country+ASN:
1. **Soft cooldown** (300 seconds) - First level, allow retries
2. **Hard cooldown** (3600 seconds) - Second level, stronger restriction
3. **Country-wide blocking** - If multiple ASNs from same country fail on same server

### Policies
Fine-grained control over protocol and server selection:
- **Country Policies:** Apply rules to all connections from a country
- **ISP Policies:** Override country policies for specific country + ASN combinations
- **Enforcement:** Toggle policy enforcement globally

## 🔄 Background Tasks (Celery)

Automatic monitoring runs in the background:

- **Monitor VPN** (every 18 sec) - Server health checks
- **Monitor Metrics** (every 10 sec) - Collect performance metrics
- **Cleanup Stale Sessions** (every 5 min) - Remove old session records

## 🔐 Security Features

- **JWT-based authentication** - Secure API access
- **API key support** - Application-level authentication
- **Role-based access control** - Granular permissions
- **Password hashing** - Secure credential storage
- **CORS configuration** - Control cross-origin requests
- **Audit logging** - Track all administrative actions

## 📊 Database Models

Key models:
- **DashboardUser** - Admin users with roles
- **App** - VPN applications/brands
- **VPNServer** - VPN server configurations (OpenVPN & Shadowsocks)
- **VPNUserSession** - Active user sessions
- **ProtocolMetrics** - Protocol performance metrics
- **CountryPolicy** - Country-level routing policies
- **ISPPolicy** - ISP-level routing policies
- **GlobalSettings** - System-wide configuration
- **AuditLog** - Action audit trail
- **Notification** - System notifications

## 🧪 Testing

```bash
# Run with development settings
DEBUG=True python main.py

# Test best server selection
curl http://localhost:8000/api/v1/best_server

# Test with specific country
curl "http://localhost:8000/api/v1/best_server?country=US&version=openvpn"
```

## 🐛 Troubleshooting

### Redis Connection Issues
```bash
# Check Redis is running on port 6379
# Update REDIS_URL and CELERY_BROKER_URL in .env
```

### Worker Issues
```bash
# Check Celery worker logs
celery -A celery_app worker --loglevel=debug
```

### Cache Issues
```bash
# Flush cache and restart
# Cache is automatically flushed on server startup
```

## 📖 Configuration Reference

### Global Settings

Manage via `/admin/settings/`:
- `protocol_mode` - "auto", "openvpn", or "shadowsocks"
- `disable_new_connections` - Block new connections
- `enforce_country_policies` - Enable country policies
- `enforce_isp_policies` - Enable ISP policies
- `cooldown_soft_seconds` - Soft cooldown duration (default: 300)
- `cooldown_hard_seconds` - Hard cooldown duration (default: 3600)
- `failure_rate_threshold` - Failure rate to trigger cooldown (default: 10%)

## 🚢 Production Deployment

1. **Set DEBUG=False** in `.env`
2. **Use strong SECRET_KEY** - Generate with: `python -c "import secrets; print(secrets.token_urlsafe())"`
3. **Use production database** - Configure PostgreSQL with backups
4. **Use production Redis** - Configure Redis persistence
5. **Run with production ASGI server:**
   ```bash
   gunicorn -w 4 -k uvicorn.workers.UvicornWorker main:app
   ```
6. **Use process manager** - Supervisor or systemd for process management
7. **Configure monitoring** - Health checks, error tracking, logging
8. **Enable HTTPS** - Use reverse proxy (Nginx) with SSL/TLS

## 📈 Connection Analytics (per-server statistics)

Per-server request / success / failure statistics (by country and protocol) for the dashboard's
**VPN Servers Analytics → server page** and the **Home Overview**.

**Design (built so it can never slow or fail a connection request):**

- The hot endpoints only do one *pipelined Redis `HINCRBY`* (`app/analytics.py`): `/v2/best_server/`
  counts after the response is sent (background task); `/v2/connection_feedback/` counts inline with a
  0.5 s hard cap. **No database access.** Every error is swallowed; `ANALYTICS_ENABLED=false` skips it all.
- A Celery task (`analytics_flush`, every 60 s, `app/analytics_tasks.py`) copies the Redis counters into
  three summary tables, idempotently (re-running a flush can never double count):
  `server_traffic_5m`, `server_traffic_hourly`, and `server_usage_5m` (live sessions + the capacity in
  force at that moment, so later capacity edits never rewrite history).
- `analytics_cleanup` (hourly) applies retention (`ANALYTICS_5M_RETENTION_DAYS=3`,
  `ANALYTICS_HOURLY_RETENTION_DAYS=60`, `ANALYTICS_USAGE_RETENTION_DAYS=30`).
- The read API (`app/api/admin_analytics.py`: `GET /admin/analytics/servers/{id}` and
  `GET /admin/analytics/overview`) reads only those tables and is cached in Redis for 15-60 s.
- Nothing in the routing / decision engine reads or writes these tables.

**What the numbers mean** (also shown in the UI):

| Metric | Meaning |
|---|---|
| Requests sent | Times the backend handed this server out (`/v2/best_server/`) — a request sent to the server, *not* a confirmed connection |
| Successful / Failed | Connection attempts the **client app reported** via `/v2/connection_feedback/` (not independently verified). One attempt = one protocol tried |
| Success rate | successful / (successful + failed) |

All timestamps are UTC. History starts on the day this was deployed (existing lifetime counters have no time dimension).

**Deploying it (in this order):**

1. `alembic upgrade head` — creates the 3 new tables (new empty tables only; instant, no locks on live tables).
   (If you skip this, the API's startup `create_all` creates them, but with several workers starting at once
   the migration is the safer route.)
2. Restart the API.
3. Restart **both** the Celery worker **and** Celery beat (new tasks + schedule).
4. Deploy the frontend build *after* the backend (it calls the new endpoints).

Check it: Celery logs `Task analytics_flush ... succeeded` every minute; `redis-cli -n 1 --scan --pattern 'an:*'`
shows the counter keys; the tables fill within ~1-2 minutes of traffic.

**Rollback / kill switch:** set `ANALYTICS_ENABLED=false` in `.env` and restart the API + Celery — recording
stops immediately and the dashboards simply show no new data. Removing the feature entirely is
`alembic downgrade a41f7c2d9e10` (drops only the 3 new tables).

### Graphs (time series) and custom ranges

`GET /admin/analytics/servers/{id}/series` feeds the 9 graphs on the per-server page (sessions, utilization,
requests, successful, failed, OpenVPN vs Shadowsocks requests and success/failure, country-wise requests and
success/failure). The report (`GET /admin/analytics/servers/{id}`) and the series accept the same
`period` (`1h|6h|24h|7d|30d|custom`), `from`/`to` (ISO, UTC, for `custom`), `country`, `protocol`, `scope`;
the series also takes `resolution` (seconds). **No new tables and no migration**: it reads the same
`server_traffic_5m`, `server_traffic_hourly` and `server_usage_5m` tables.

How the cost stays bounded as data grows (`app/analytics_series.py` holds the rules):

- **At most 300 graph points**, whatever the range: the bucket width grows with the range (5 min ... 1 day).
- **Top 5 countries + "Other"** per graph, so the response size does not depend on how many countries exist.
- **Fine data only while retained**: 5-minute buckets are used only inside `ANALYTICS_5M_RETENTION_DAYS`; older
  ranges automatically use the hourly table with >= 1 hour buckets. A custom range is clamped to
  `ANALYTICS_HOURLY_RETENTION_DAYS` (sessions/capacity history: `ANALYTICS_USAGE_RETENTION_DAYS`).
- **Same window as the cards**: the graph points always add up exactly to the card totals.
- Each request is two grouped scans of the narrow `(server_id, bucket_start)` index range plus one small
  sessions query, protected by a PostgreSQL `statement_timeout` (15 s), and cached in Redis (20-120 s).
- "No data" (before recording began, or no snapshot) is `null`, drawn as a gap; it is never shown as zero.
- A disabled server contributes no capacity; every snapshot keeps the capacity that applied at that time.

Measured on a real PostgreSQL 16 with ~5.8 million analytics rows (30 server rows, 40 countries, 60 days hourly,
3 days of 5-minute data), uncached: 1h-6h graphs 13-40 ms, 24h 40-170 ms, 7d 40-130 ms, 30d 150-300 ms,
worst case (58-day custom range, all apps of a server) about 0.5 s; the report cards 6-55 ms. The cost of one
request grows with *that server's* rows in the range (hours x countries x protocols), not with the table size.
To keep more history, raise the retention settings knowing hourly storage is roughly
`servers x countries x 2 x 24 x days` rows.

## 📄 License

Proprietary - VPN Load Balancer System

## 📞 Support

For issues or feature requests, please contact the development team.

---

**Built with ❤️ using FastAPI, PostgreSQL, and Redis**


## Related Projects

- [VPN Load Balancer Frontend](https://github.com/sajid70300/vpn-load-balancer-frontend.git)