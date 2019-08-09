"""initial

Revision ID: 782cd22eccdf
Revises: 
Create Date: 2019-08-06 14:29:16.421330

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import Column, Integer, ARRAY, String, Text
from adsputils import get_date, UTCDateTime


# revision identifiers, used by Alembic.
revision = '782cd22eccdf'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('storage',
                    Column('key', String(255), primary_key=True),
                    Column('value', Text),
                    )

    op.create_table('authors',
                    Column('id', Integer, primary_key=True),
                    Column('created', UTCDateTime, default=get_date()),
                    Column('last_sent', UTCDateTime),
                    )

    op.create_table('results',
                    Column('id', Integer, primary_key=True),
                    Column('user_id', Integer),
                    Column('qid', Integer),
                    Column('results', ARRAY(String)),
                    Column('created', UTCDateTime, default=get_date()),
                    )

def downgrade():
    op.drop_table('storage')
    op.drop_table('authors')
    op.drop_table('results')
