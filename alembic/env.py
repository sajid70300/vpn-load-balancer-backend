import os
import sys
import time
from logging.config import fileConfig

from sqlalchemy import pool, text
from sqlalchemy.exc import OperationalError, DBAPIError

from alembic import context

# ── Make the app package importable ──────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── Use the SAME engine and models as the running app — single source of truth.
# No DB URL is duplicated in alembic.ini; it always comes from app/config.py (.env).
from app.database import Base, sync_engine
from app import models  # noqa: F401  (import registers all model classes on Base.metadata)

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Autogenerate compares the live DB against this metadata.
target_metadata = Base.metadata

# ── Lock-safety settings ──────────────────────────────────────────────────────
# Learned directly from a 2026-07-01 production incident: a schema change must
# NEVER block indefinitely on a lock against a live, busy table. If it can't
# get the lock quickly, back off and retry a bounded number of times instead
# of hanging and queuing up real traffic behind it.
LOCK_TIMEOUT_SQL = "5s"
MAX_ATTEMPTS = 12
RETRY_DELAY_SECONDS = 5


def run_migrations_offline() -> None:
    """Emit SQL to stdout without touching a real database (rarely used here)."""
    url = str(sync_engine.url)
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """
    Run migrations against the real database, with a bounded lock wait and
    automatic retry on lock contention — never hangs indefinitely.
    """
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with sync_engine.connect() as connection:
                context.configure(connection=connection, target_metadata=target_metadata)
                with context.begin_transaction():
                    connection.execute(text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_SQL}'"))
                    context.run_migrations()
            return  # success
        except (OperationalError, DBAPIError) as e:
            msg = str(e).lower()
            if "lock timeout" in msg or "canceling statement" in msg:
                print(
                    f"   \u23f3 Table busy (attempt {attempt}/{MAX_ATTEMPTS}), "
                    f"retrying in {RETRY_DELAY_SECONDS}s..."
                )
                time.sleep(RETRY_DELAY_SECONDS)
                continue
            raise  # a real error — don't retry blindly

    raise RuntimeError(
        f"Could not acquire the lock after {MAX_ATTEMPTS} attempts "
        f"(~{MAX_ATTEMPTS * RETRY_DELAY_SECONDS}s). Nothing was left half-applied — "
        f"safe to investigate (check pg_stat_activity) and re-run later."
    )


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()