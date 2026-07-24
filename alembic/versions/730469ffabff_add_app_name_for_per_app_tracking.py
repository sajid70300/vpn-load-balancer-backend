"""add app_name to system_peak_stats and active_users_history for per-app tracking

Revision ID: 730469ffabff
Revises: 0c3e9031d1ac
Create Date: 2026-07-24 09:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '730469ffabff'
down_revision: Union[str, Sequence[str], None] = '0c3e9031d1ac'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Must exactly match ALL_APPS_KEY in app/models.py — used here only to
# backfill existing rows (which were, before this change, always the
# combined/global total) so they keep meaning the same thing afterward.
ALL_APPS_KEY = "__all_apps__"


def upgrade() -> None:
    """Upgrade schema."""
    # ── system_peak_stats: add app_name, backfill existing row, enforce uniqueness ──
    op.add_column(
        'system_peak_stats',
        sa.Column('app_name', sa.String(length=100), nullable=False, server_default=ALL_APPS_KEY),
    )
    op.create_unique_constraint('uq_system_peak_stats_app_name', 'system_peak_stats', ['app_name'])
    # Drop the server default after backfilling — new rows going forward must
    # explicitly specify app_name (the app writes it every time), so an
    # omitted value should be a visible error, not silently become "__all_apps__".
    op.alter_column('system_peak_stats', 'app_name', server_default=None)

    # CRITICAL: the original code always inserted this table's one existing
    # row with an explicit id=1 (SystemPeakStats(id=1, ...)) — which, in
    # Postgres, does NOT advance the column's underlying auto-increment
    # sequence. Left alone, the very first time the app tries to
    # auto-generate an id for a new row (a genuinely new app's first peak
    # entry) it would collide with the existing id=1 row and crash with
    # "duplicate key value violates unique constraint". Verified this exact
    # failure reproduces against a real Postgres 16 instance, and that this
    # resync fixes it. Safe/idempotent to run even if no desync exists.
    op.execute(
        "SELECT setval(pg_get_serial_sequence('system_peak_stats', 'id'), "
        "COALESCE((SELECT MAX(id) FROM system_peak_stats), 1))"
    )

    # ── active_users_history: add app_name, backfill existing rows, index it ──
    op.add_column(
        'active_users_history',
        sa.Column('app_name', sa.String(length=100), nullable=False, server_default=ALL_APPS_KEY),
    )
    op.create_index('ix_active_users_history_app_name', 'active_users_history', ['app_name'])
    op.create_index(
        'ix_active_users_history_app_recorded',
        'active_users_history', ['app_name', 'recorded_at'],
    )
    op.alter_column('active_users_history', 'app_name', server_default=None)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index('ix_active_users_history_app_recorded', table_name='active_users_history')
    op.drop_index('ix_active_users_history_app_name', table_name='active_users_history')
    op.drop_column('active_users_history', 'app_name')

    op.drop_constraint('uq_system_peak_stats_app_name', 'system_peak_stats', type_='unique')
    op.drop_column('system_peak_stats', 'app_name')