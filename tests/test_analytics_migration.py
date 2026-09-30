"""The analytics Alembic migration must match the models, be re-runnable, and keep a single head."""
import importlib.util
import os

import pytest
from alembic.config import Config
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models import ServerTraffic5m, ServerTrafficHourly, ServerUsage5m  # noqa: F401

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MIGRATION = os.path.join(BACKEND, "alembic", "versions", "c5a9e3d17b42_add_connection_analytics_tables.py")
NEW_TABLES = ["server_traffic_5m", "server_traffic_hourly", "server_usage_5m"]


def load_migration():
    spec = importlib.util.spec_from_file_location("analytics_migration", MIGRATION)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def engine():
    return create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})


def run_migration(engine, fn_name):
    mod = load_migration()
    with engine.begin() as conn:
        with Operations.context(MigrationContext.configure(conn)):
            getattr(mod, fn_name)()


def shape(insp, table):
    cols = {c["name"]: (str(c["type"]).split("(")[0], c["nullable"]) for c in insp.get_columns(table)}
    pk = tuple(insp.get_pk_constraint(table)["constrained_columns"])
    idx = {i["name"]: tuple(i["column_names"]) for i in insp.get_indexes(table)}
    return cols, pk, idx


def test_migration_creates_exactly_what_the_models_define(engine):
    run_migration(engine, "upgrade")
    from_migration = inspect(engine)

    model_engine = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
    Base.metadata.create_all(model_engine)
    from_models = inspect(model_engine)

    for table in NEW_TABLES:
        assert shape(from_migration, table) == shape(from_models, table), table


def test_migration_touches_nothing_else_and_only_adds_the_three_tables(engine):
    before = set(inspect(engine).get_table_names())
    run_migration(engine, "upgrade")
    assert set(inspect(engine).get_table_names()) - before == set(NEW_TABLES)


def test_running_upgrade_twice_or_after_create_all_is_harmless(engine):
    run_migration(engine, "upgrade")
    run_migration(engine, "upgrade")                       # already there -> skipped, no error
    other = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
    Base.metadata.create_all(other)                        # the API's startup create_all ran first
    run_migration(other, "upgrade")
    assert set(NEW_TABLES) <= set(inspect(other).get_table_names())


def test_downgrade_drops_only_the_new_tables_and_is_safe_to_repeat(engine):
    Base.metadata.create_all(engine)
    others = set(inspect(engine).get_table_names()) - set(NEW_TABLES)
    run_migration(engine, "downgrade")
    assert set(inspect(engine).get_table_names()) == others
    run_migration(engine, "downgrade")                     # nothing to drop -> no error


def test_the_migration_chain_has_a_single_head_and_this_is_it():
    cfg = Config(os.path.join(BACKEND, "alembic.ini"))
    cfg.set_main_option("script_location", os.path.join(BACKEND, "alembic"))
    script = ScriptDirectory.from_config(cfg)
    assert script.get_heads() == ["c5a9e3d17b42"]
    assert script.get_revision("c5a9e3d17b42").down_revision == "a41f7c2d9e10"
