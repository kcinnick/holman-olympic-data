"""remove event column from olympics_medals

Revision ID: c93c59a41c5a
Revises: 
Create Date: 2024-12-05 19:07:23.398340

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'c93c59a41c5a'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # Remove the 'event' column from 'olympics_medals' table
    op.drop_column('olympics_medals', 'event')


def downgrade():
    # Re-add the 'event' column to 'olympics_medals' table
    op.add_column('olympics_medals', sa.Column('event', sa.String(length=100)))
