from alembic import op

revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None


def upgrade():
    op.execute('ALTER TABLE device_latest ALTER COLUMN device_id TYPE varchar(96) COLLATE "C"')


def downgrade():
    op.execute(
        'ALTER TABLE device_latest ALTER COLUMN device_id TYPE varchar(96) COLLATE "default"'
    )
