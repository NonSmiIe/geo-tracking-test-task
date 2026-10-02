from alembic import op

revision = "010"
down_revision = "009"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
CREATE TABLE zone_membership (
    device_id varchar(96) COLLATE "C" NOT NULL,
    zone_id uuid NOT NULL REFERENCES geozones (id) ON DELETE CASCADE,
    zone_version integer NOT NULL,
    entered_at timestamptz NOT NULL,
    PRIMARY KEY (device_id, zone_id)
)
""")
    op.execute("CREATE INDEX zone_membership_zone ON zone_membership (zone_id)")
    op.execute("""
CREATE TABLE zone_events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id varchar(96) NOT NULL,
    zone_id uuid NOT NULL,
    zone_version integer NOT NULL,
    device_id varchar(96) COLLATE "C" NOT NULL,
    kind varchar(8) NOT NULL CONSTRAINT zone_event_kind CHECK (kind IN ('entered', 'exited')),
    at timestamptz NOT NULL,
    dwell_us bigint,
    topic_id varchar(32) NOT NULL,
    partition integer NOT NULL,
    "offset" bigint NOT NULL,
    CONSTRAINT zone_event_origin UNIQUE (topic_id, partition, "offset", zone_id)
)
""")
    op.execute("CREATE INDEX zone_events_owner ON zone_events (user_id, id)")


def downgrade():
    op.execute("DROP TABLE zone_events")
    op.execute("DROP TABLE zone_membership")
