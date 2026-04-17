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

## 📄 License

Proprietary - VPN Load Balancer System

## 📞 Support

For issues or feature requests, please contact the development team.

---

**Built with ❤️ using FastAPI, PostgreSQL, and Redis**


## Related Projects

- [VPN Load Balancer Frontend](https://github.com/sajid70300/vpn-load-balancer-frontend.git)