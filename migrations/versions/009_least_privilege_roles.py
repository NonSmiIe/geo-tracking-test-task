import os

from alembic import op

revision = "009"
down_revision = "008"
branch_labels = None
depends_on = None


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def upgrade():
    password = literal(os.environ.get("FLEET_APP_PASSWORD", "local-demo-password"))
    op.execute("""
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'fleet_app') THEN
        CREATE ROLE fleet_app LOGIN;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'fleet_monitor') THEN
        CREATE ROLE fleet_monitor LOGIN;
    END IF;
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO fleet_app, fleet_monitor', current_database());
END $$
""")
    op.execute(f"ALTER ROLE fleet_app PASSWORD {password}")
    op.execute(f"ALTER ROLE fleet_monitor PASSWORD {password}")
    op.execute("GRANT pg_monitor TO fleet_monitor")
    op.execute("GRANT USAGE ON SCHEMA public TO fleet_app")
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO fleet_app")
    op.execute("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO fleet_app")
    op.execute("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO fleet_app")
    op.execute(
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO fleet_app"
    )
    op.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT EXECUTE ON FUNCTIONS TO fleet_app")


def downgrade():
    op.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON TABLES FROM fleet_app")
    op.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON FUNCTIONS FROM fleet_app")
    op.execute("REVOKE ALL ON ALL TABLES IN SCHEMA public FROM fleet_app")
    op.execute("REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM fleet_app")
    op.execute("REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM fleet_app")
    op.execute("REVOKE USAGE ON SCHEMA public FROM fleet_app")
