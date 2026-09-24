"""
Test setup — runs BEFORE any app module is imported.

Safety: connection settings are force-overridden with dummy values (real
environment variables beat the .env file), so a test run can never connect to
the real database or Redis, whatever .env contains. All tests use throw-away
in-memory SQLite databases.

Run from the backend/ folder:
    pip install -r requirements-dev.txt
    python -m pytest tests -q
"""
import math
import os
import sys
from contextlib import asynccontextmanager

for _k, _v in {
    "DATABASE_URL": "postgresql+asyncpg://test:test@127.0.0.1:1/test",
    "SYNC_DATABASE_URL": "postgresql://test:test@127.0.0.1:1/test",
    "REDIS_URL": "redis://127.0.0.1:1/0",
    "CACHE_REDIS_URL": "redis://127.0.0.1:1/1",
    "CELERY_BROKER_URL": "redis://127.0.0.1:1/0",
    "CELERY_RESULT_BACKEND": "redis://127.0.0.1:1/0",
    "API_KEY": "test-key",
}.items():
    os.environ[_k] = _v

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.database import Base
from app import models  # noqa: F401  (registers tables on Base.metadata)


def _register_floor(dbapi_conn, _record):
    # Postgres has floor(); older SQLite builds don't.
    dbapi_conn.create_function("floor", 1, lambda x: None if x is None else math.floor(x))


@asynccontextmanager
async def make_db():
    """A fresh empty in-memory database with all tables; yields an AsyncSession."""
    engine = create_async_engine(
        "sqlite+aiosqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False}
    )
    event.listen(engine.sync_engine, "connect", _register_floor)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with session_factory() as session:
            yield session
    finally:
        await engine.dispose()


@pytest.fixture(autouse=True)
def _clear_server_list_cache():
    """_load_servers()'s and get_protocol_decision_for_server()'s caches
    (decision_engine._server_list_cache / _single_server_cache) are
    module-level dicts, deliberately shared across DecisionEngine instances
    within one process (that's the point of the fix). But that also means
    they persist across DIFFERENT tests in the same pytest run unless
    cleared — without this, two unrelated tests that happen to both use
    app_name "appA" (or the same server IP) within the same ~10 real-time
    seconds could see each other's cached (and by then wrong) data. Global
    and autouse so every test file gets a clean cache, not just ones that
    remembered to ask for it.
    """
    import app.decision_engine as de
    de._server_list_cache.clear()
    de._single_server_cache.clear()
    yield
    de._server_list_cache.clear()
    de._single_server_cache.clear()


@pytest.fixture
def noop_audit(monkeypatch):
    """Silence audit logging in a router module and record its calls."""
    calls = []

    async def _audit(db, token, **kw):
        calls.append(kw)

    def apply(module):
        monkeypatch.setattr(module, "audit_log", _audit)
        return calls

    return apply
