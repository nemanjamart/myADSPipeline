"""split last sent

Revision ID: 5224ac0b32ba
Revises: 07ec7ca54c2f
Create Date: 2020-08-20 17:12:23.775384

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5224ac0b32ba'
down_revision = '07ec7ca54c2f'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('authors', sa.Column('last_sent_daily', sa.UTCDateTime))
    op.add_column('authors', sa.Column('last_sent_weekly', sa.UTCDateTime))


def downgrade():
    op.drop_column('authors', 'last_sent_daily')
    op.drop_column('authors', 'last_sent_weekly')
