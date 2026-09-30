"""add connection analytics tables

Revision ID: c5a9e3d17b42
Revises: a41f7c2d9e10
Create Date: 2026-09-30 12:00:00.000000

Three NEW, standalone tables for the per-server connection analytics:
  * server_traffic_5m      counters per 5-minute bucket / server / country / protocol
  * server_traffic_hourly  the same counters rolled up per hour
  * server_usage_5m        live sessions + the capacity in force, per server, every ~5 min

Safe on a live database:
  * Only CREATE TABLE / CREATE INDEX on brand-new empty tables — no existing table,
    column, index or row is touched, so there is nothing to lock against live traffic.
  * No foreign keys (statistics must survive server deletion; no cascade cost).
  * Nothing in the routing / decision-engine path reads or writes these tables.
  * Idempotent: a table that already exists (e.g. the API's startup create_all ran
    first) is skipped, so running this migration never fails on "already exists".
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c5a9e3d17b42'
down_revision: Union[str, Sequence[str], None] = 'a41f7c2d9e10'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_table(name: str) -> bool:
    return sa.inspect(op.get_bind()).has_table(name)


def _traffic_table(name: str, index_name: str) -> None:
    if _has_table(name):
        return
    op.create_table(
        name,
        sa.Column('bucket_start', sa.DateTime(timezone=True), nullable=False),
        sa.Column('server_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.Column('country', sa.String(length=2), nullable=False),
        sa.Column('protocol', sa.String(length=12), nullable=False),
        sa.Column('assigned', sa.Integer(), server_default='0', nullable=False),
        sa.Column('success', sa.Integer(), server_default='0', nullable=False),
        sa.Column('failed', sa.Integer(), server_default='0', nullable=False),
        sa.PrimaryKeyConstraint('bucket_start', 'server_id', 'country', 'protocol'),
    )
    op.create_index(index_name, name, ['server_id', 'bucket_start'], unique=False)


def upgrade() -> None:
    """Upgrade schema."""
    _traffic_table('server_traffic_5m', 'ix_server_traffic_5m_server_bucket')
    _traffic_table('server_traffic_hourly', 'ix_server_traffic_hourly_server_bucket')

    if not _has_table('server_usage_5m'):
        op.create_table(
            'server_usage_5m',
            sa.Column('bucket_start', sa.DateTime(timezone=True), nullable=False),
            sa.Column('server_id', sa.Integer(), autoincrement=False, nullable=False),
            sa.Column('active_sessions', sa.Integer(), server_default='0', nullable=False),
            sa.Column('openvpn_sessions', sa.Integer(), server_default='0', nullable=False),
            sa.Column('shadowsocks_sessions', sa.Integer(), server_default='0', nullable=False),
            sa.Column('max_capacity', sa.Integer(), server_default='0', nullable=False),
            sa.Column('is_active', sa.Boolean(), server_default=sa.true(), nullable=False),
            sa.PrimaryKeyConstraint('bucket_start', 'server_id'),
        )
        op.create_index('ix_server_usage_5m_server_bucket', 'server_usage_5m',
                        ['server_id', 'bucket_start'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    # Drops only the three new tables (and their indexes).
    for table, index in (
        ('server_usage_5m', 'ix_server_usage_5m_server_bucket'),
        ('server_traffic_hourly', 'ix_server_traffic_hourly_server_bucket'),
        ('server_traffic_5m', 'ix_server_traffic_5m_server_bucket'),
    ):
        if _has_table(table):
            op.drop_index(index, table_name=table)
            op.drop_table(table)
