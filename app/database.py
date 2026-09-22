from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import sessionmaker, declarative_base
from app.config import settings

# Async for API
async_engine = create_async_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,
    pool_size=20,          # per worker; 4 uvicorn workers = 80 total baseline,
                           # 140 total including overflow (below).
                           # NOTE: this was previously 40 here (160+ total
                           # across 4 workers) — a 4x mismatch against the
                           # original comment's intent, since each worker
                           # process gets its own independent pool. Fixed
                           # 2026-07-22 after this caused Postgres connection
                           # exhaustion (200/200, max_connections) alongside
                           # another project sharing the same instance.
                           # Raised again 2026-09-22: real production traffic
                           # was observed needing ~106 concurrent connections
                           # (52 active + 54 idle-in-transaction, most of the
                           # latter brief — normal Redis-call overhead inside
                           # get_best_server()/_load_servers(), not a leak) —
                           # more than the previous 100-total pool could serve,
                           # causing QueuePool TimeoutError / 500s under load.
                           # At the same time, pg_stat_activity showed only
                           # ~107 of the 200 max_connections in use system-wide
                           # (the other project sharing this instance uses very
                           # few), so there was real headroom to draw on.
                           # 140 total leaves ~60 connections of margin under
                           # the CONFIRMED max_connections=200 (the "raised to
                           # 300" note above was not reflected in `SHOW
                           # max_connections` when checked on 2026-09-22 —
                           # verify/reconcile that separately from this fix).
                           # This raises capacity for the current load; it does
                           # not shorten how long each request holds a
                           # connection — see get_best_server()'s Redis calls
                           # in decision_engine.py for that follow-up.
    max_overflow=15,       # extra burst connections under spike traffic, per worker
    pool_timeout=10,       # fail fast after 10s instead of hanging forever
    pool_recycle=1800,     # recycle connections every 30min to avoid stale ones
)
AsyncSessionLocal = async_sessionmaker(async_engine, class_=AsyncSession, expire_on_commit=False)

# Sync for Celery
sync_engine = create_engine(
    settings.SYNC_DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=5,
    pool_timeout=10,
    pool_recycle=1800,
)
SyncSessionLocal = sessionmaker(bind=sync_engine)

Base = declarative_base()


async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except:
            await session.rollback()
            raise