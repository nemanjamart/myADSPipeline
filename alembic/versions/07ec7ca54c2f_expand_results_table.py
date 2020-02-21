"""expand results table

Revision ID: 07ec7ca54c2f
Revises: 4d32cf06f0dd
Create Date: 2020-02-20 12:18:49.152203

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '07ec7ca54c2f'
down_revision = '4d32cf06f0dd'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('results', sa.Column('setup_id', sa.Integer))


def downgrade():
    op.drop_column('results', 'setup_id')
