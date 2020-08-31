"""split last sent

Revision ID: 5224ac0b32ba
Revises: 07ec7ca54c2f
Create Date: 2020-08-20 17:12:23.775384

"""
from alembic import op
import sqlalchemy as sa
from adsputils import UTCDateTime


# revision identifiers, used by Alembic.
revision = '5224ac0b32ba'
down_revision = '07ec7ca54c2f'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('authors', sa.Column('last_sent_daily', UTCDateTime))
    op.add_column('authors', sa.Column('last_sent_weekly', UTCDateTime))

    # Data migration: takes a few steps...
    # Declare ORM table views. Note that the view contains old and new columns!
    t_authors = sa.Table(
        'authors',
        sa.MetaData(),
        sa.Column('id', sa.Integer),
        sa.Column('last_sent', UTCDateTime),     # old column
        sa.Column('last_sent_daily', UTCDateTime),  # two new columns
        sa.Column('last_sent_weekly', UTCDateTime),
    )
    # Use Alchemy's connection and transaction to noodle over the data
    connection = op.get_bind()
    # Select all existing last sent dates that need migrating
    results = connection.execute(sa.select([
        t_authors.c.id,
        t_authors.c.last_sent,
    ])).fetchall()
    # Iterate over all selected data tuples
    for id_, last_sent_ in results:
        # Update the new columns
        connection.execute(t_authors.update().where(t_authors.c.id == id_).values(
            last_sent_daily=last_sent_,
            last_sent_weekly=last_sent_,
        ))

    op.drop_column('authors', 'last_sent')


def downgrade():
    op.add_column('authors', sa.Column('last_sent', UTCDateTime))

    # data migration
    t_authors = sa.Table(
        'authors',
        sa.MetaData(),
        sa.Column('id', sa.Integer),
        sa.Column('last_sent', UTCDateTime),
        sa.Column('last_sent_daily', UTCDateTime),
    )

    connection = op.get_bind()
    results = connection.execute(sa.select([
        t_authors.c.id,
        t_authors.c.last_sent_daily,
    ])).fetchall()

    for id_, last_sent_daily_ in results:
        connection.execute(t_authors.update().where(t_authors.c.id == id_).values(
            last_sent=last_sent_daily_,
        ))

    op.drop_column('authors', 'last_sent_daily')
    op.drop_column('authors', 'last_sent_weekly')
