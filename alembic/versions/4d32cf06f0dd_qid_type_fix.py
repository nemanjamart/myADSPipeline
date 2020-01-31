"""qid type fix

Revision ID: 4d32cf06f0dd
Revises: 782cd22eccdf
Create Date: 2020-01-31 13:45:11.886780

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = '4d32cf06f0dd'
down_revision = '782cd22eccdf'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('results', 'qid', existing_type=sa.Integer, type_=sa.String(32))


def downgrade():
    op.alter_column('results', 'qid',
                    existing_type=sa.String(32),
                    type_=sa.Integer,
                    postgresql_using="qid::integer")
