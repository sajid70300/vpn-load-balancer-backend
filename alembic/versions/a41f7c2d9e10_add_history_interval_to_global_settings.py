"""add history_interval_minutes to global_settings

Revision ID: a41f7c2d9e10
Revises: 730469ffabff
Create Date: 2026-09-18 12:00:00.000000

Adds the admin-configurable interval (in minutes) at which the Celery snapshot
task records active-user history for the dashboard graphs.

Safe on a live database:
  * global_settings is a single-row table.
  * A constant server default makes this a metadata-only change on PostgreSQL 11+
    (no table rewrite); existing rows read as 30, which is exactly the
    previously hard-coded snapshot interval — behaviour is unchanged until an
    admin changes the value.
  * The ORM maps the column as `deferred`, so the routing / decision-engine
    query never touches it, whether or not this migration has run yet.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a41f7c2d9e10'
down_revision: Union[str, Sequence[str], None] = '730469ffabff'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'global_settings',
        sa.Column('history_interval_minutes', sa.Integer(), nullable=False, server_default='30'),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('global_settings', 'history_interval_minutes')
