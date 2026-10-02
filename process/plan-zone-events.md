---
title: Plan: zone entry and exit events (P1)
date: 2026-10-02
kind: plan
status: building
---

# Plan: zone entry and exit events (P1)

From [2026-10-02-0940-product-lens](ideas/2026-10-02-0940-product-lens.md), triaged as P1 in [ideas](ideas.md). Settled context: [decisions](decisions.md).

## Semantics

- A **membership** row means the device is inside this *version* of the zone, judged by its newest fresh sample.
- The zone version bumps only when matching changes: centre, radius or `active`. Today it bumps on any change, a rename included; that is fixed so a rename does not re-announce every device inside.
- An edit, a pause or a resume makes old memberships stale. They are ignored and replaced, so a device inside a resumed or moved zone gets one `entered`, and nothing claims it left a zone that was only edited.
- A deleted zone cascades its memberships away. Events stay as history, so there is no foreign key on events.
- `inside_report` stays unchanged: the decision against entry-only alerts holds. `zone_event` is an additional frame type.

## Data

- `zone_membership(device_id COLLATE "C", zone_id → geozones ON DELETE CASCADE, zone_version, entered_at)`, primary key `(device_id, zone_id)`, plus an index on `zone_id` for the cascade.
- `zone_events(id identity, user_id, zone_id, zone_version, device_id, kind entered|exited, at, dwell_us, topic_id, partition, offset)`, with `UNIQUE (topic_id, partition, offset, zone_id)` and an index on `(user_id, id)`.

## Processor, inside the existing batch transaction

1. The upsert runs first, as now. Its row locks order a zombie owner and a new owner per device.
2. Read membership for devices with fresh samples, joined with `geozones`: valid means active and the same version.
3. For each device, walk its fresh samples in timestamp order against the `MATCH_SQL` result. Entered = matched − held; exited = held − matched. An exit carries its dwell.
4. Write the membership diff (delete exited and stale rows, insert entered), and the events with their Kafka position.
5. Replayed records (offset ≤ persisted) read their events back by `(topic_id, partition, offset)` and re-emit them, the same rule as positions.
6. Deliver `zone_event` frames on the owner's alerts subject, within the byte-bounded frames.

## API and UI

- `GET /zone-events?after=<id>&limit=` for the owner returns items and a cursor. This is the backfill after a reconnect, which `inside_report` never had.
- The UI activity feed is driven by `zone_event`: entered and exited with dwell, backfilled from the endpoint on load. The client-side `EPISODE_GAP_MS` heuristic is deleted.

## Proof

- **Tests on PostGIS, Kafka and NATS:**
  - enter, then exit with dwell, on the socket and the endpoint;
  - in and out inside one batch;
  - a replay after a crash between commit and publish re-emits the same events;
  - a pause gives no exit, and a resume with the device inside gives `entered`;
  - a rename gives no event;
  - a delete cascades;
  - a zombie owner.
- **Measurement:** 300k × 300 s against D2's spread, with the statement-time delta. The benchmark's coverage zone gives a 300k `entered` burst in the first 5 s, which is the worst case the lens named.

Open: retention of `zone_events` (daily partitions, or a delete job). → [questions](questions.md)
