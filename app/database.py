from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import sessionmaker, declarative_base
from app.config import settings

# Async for API
async_engine = create_async_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,
    pool_size=15,          # per worker; 4 uvicorn workers = 60 total baseline,
                           # 100 total including overflow (below).
                           # NOTE: this was previously 40 here (160+ total
                           # across 4 workers) — a 4x mismatch against the
                           # original comment's intent, since each worker
                           # process gets its own independent pool. Fixed
                           # 2026-07-22 after this caused Postgres connection
                           # exhaustion (200/200, max_connections) alongside
                           # another project sharing the same instance.
                           # Set a bit above the strict minimum (10) since
                           # max_connections is also being raised to 300
                           # around the same time — see alembic/ops notes.
    max_overflow=10,       # extra burst connections under spike traffic, per worker
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