from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from app.config import settings
from app.database import Base, async_engine

from app.api import (
    public,
    admin_machines,
    admin_servers,
    admin_sessions,
    admin_stats,
    admin_metrics,
    admin_apps,
    admin_settings,
    admin_users,
    admin_audit,
    admin_notifications,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("✅ Database tables created")

    # Ensure notification index exists
    from sqlalchemy import text
    async with async_engine.begin() as conn:
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_notifications_unread
            ON notifications(is_read, created_at DESC)
        """))
    print("✅ DB indexes verified")

    # Flush stale global_settings cache on startup
    from app.cache import delete_cache
    await delete_cache("global_settings")
    print("✅ Settings cache flushed")

    print("✅ FastAPI server started")
    print("🚀 Decision Engine: READY (unified single-row server model)")
    print("🔌 Protocols: OpenVPN + Shadowsocks (both on same VPNServer row)")

    yield

    await async_engine.dispose()
    print("👋 Shutting down")


app = FastAPI(
    title="VPN Load Balancer API",
    description="Intelligent VPN Load Balancer with OpenVPN and Shadowsocks support",
    version="3.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    root_path="/api",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(public.router)
app.include_router(admin_machines.router)
app.include_router(admin_servers.router)
app.include_router(admin_sessions.router)
app.include_router(admin_stats.router)
app.include_router(admin_metrics.router)
app.include_router(admin_apps.router)
app.include_router(admin_settings.router)
app.include_router(admin_users.router)
app.include_router(admin_audit.router)
app.include_router(admin_notifications.router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, workers=1)








# # For deployment main.py should be as

# from fastapi import FastAPI
# from fastapi.middleware.cors import CORSMiddleware
# from fastapi.staticfiles import StaticFiles
# from fastapi.responses import FileResponse
# from contextlib import asynccontextmanager
# from app.config import settings
# from app.database import Base, async_engine
# import os

# from app.api import (
#     public,
#     admin_machines,
#     admin_servers,
#     admin_sessions,
#     admin_stats,
#     admin_metrics,
#     admin_apps,
#     admin_settings,
#     admin_users,
#     admin_audit,
#     admin_notifications,
# )


# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     async with async_engine.begin() as conn:
#         await conn.run_sync(Base.metadata.create_all)
#     print("✅ Database tables created")

#     from sqlalchemy import text
#     async with async_engine.begin() as conn:
#         await conn.execute(text("""
#             CREATE INDEX IF NOT EXISTS idx_notifications_unread
#             ON notifications(is_read, created_at DESC)
#         """))
#     print("✅ DB indexes verified")

#     from app.cache import delete_cache
#     await delete_cache("global_settings")
#     print("✅ Settings cache flushed")

#     print("✅ FastAPI server started")
#     print("🚀 Decision Engine: READY (unified single-row server model)")
#     print("🔌 Protocols: OpenVPN + Shadowsocks (both on same VPNServer row)")

#     yield

#     await async_engine.dispose()
#     print("👋 Shutting down")


# app = FastAPI(
#     title="VPN Load Balancer API",
#     description="Intelligent VPN Load Balancer with OpenVPN and Shadowsocks support",
#     version="3.0.0",
#     docs_url="/docs",
#     redoc_url="/redoc",
#     lifespan=lifespan,
# )

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=settings.cors_origins,
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # ── Serve React frontend static files (BEFORE API routers) ───
# FRONTEND_DIST = "/root/vpn_fastapi/vpn-load-balancer-frontend/dist"

# if os.path.exists(FRONTEND_DIST):
#     app.mount("/assets", StaticFiles(directory=f"{FRONTEND_DIST}/assets"), name="assets")

#     @app.get("/", include_in_schema=False)
#     async def serve_index():
#         return FileResponse(f"{FRONTEND_DIST}/index.html")

# # ── API routes ────────────────────────────────────────────────
# app.include_router(public.router)
# app.include_router(admin_machines.router)
# app.include_router(admin_servers.router)
# app.include_router(admin_sessions.router)
# app.include_router(admin_stats.router)
# app.include_router(admin_metrics.router)
# app.include_router(admin_apps.router)
# app.include_router(admin_settings.router)
# app.include_router(admin_users.router)
# app.include_router(admin_audit.router)
# app.include_router(admin_notifications.router)

# # ── SPA catch-all (AFTER API routers) ────────────────────────
# if os.path.exists(FRONTEND_DIST):
#     @app.get("/{full_path:path}", include_in_schema=False)
#     async def serve_spa(full_path: str):
#         file_path = f"{FRONTEND_DIST}/{full_path}"
#         if os.path.exists(file_path) and os.path.isfile(file_path):
#             return FileResponse(file_path)
#         return FileResponse(f"{FRONTEND_DIST}/index.html")


# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run("main:app", host="0.0.0.0", port=9000, reload=True, workers=1)