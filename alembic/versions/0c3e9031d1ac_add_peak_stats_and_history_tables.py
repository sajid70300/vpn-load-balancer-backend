"""add system_peak_stats and active_users_history tables

Revision ID: 0c3e9031d1ac
Revises: db977cad812e
Create Date: 2026-07-22 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0c3e9031d1ac'
down_revision: Union[str, Sequence[str], None] = 'db977cad812e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # ### two new, standalone tables — no existing table is touched ###
    op.create_table(
        'system_peak_stats',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('peak_users', sa.Integer(), nullable=False),
        sa.Column('peak_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_table(
        'active_users_history',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('recorded_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.Column('total_users', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(op.f('ix_active_users_history_id'), 'active_users_history', ['id'], unique=False)
    op.create_index(op.f('ix_active_users_history_recorded_at'), 'active_users_history', ['recorded_at'], unique=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    # ### reverses the above — drops only the two new tables, nothing else ###
    op.drop_index(op.f('ix_active_users_history_recorded_at'), table_name='active_users_history')
    op.drop_index(op.f('ix_active_users_history_id'), table_name='active_users_history')
    op.drop_table('active_users_history')
    op.drop_table('system_peak_stats')
    # ### end Alembic commands ###