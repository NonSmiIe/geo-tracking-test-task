from alembic import op

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(
        "device_latest_position", "device_latest", ["position"], postgresql_using="gist"
    )


def downgrade():
    op.drop_index("device_latest_position", table_name="device_latest")
