---
title: Devil's advocate, first pass
date: 2026-10-02
kind: research
---

## Fleetline: 10 questions it can't yet answer with evidence

Ranked by how much a wrong answer would hurt credibility. None of these repeats the three items already open in `questions.md`.

**1. Can one forged report freeze a device forever?** (high)
- `Report.timestamp` only checks that a time zone is present (`schemas.py`). There is no upper bound and no device authentication on `/ingest`.
- A device's watermark only moves forward: the stale filter in `processor.py` plus `WHERE device.reported_at < excluded.reported_at`.
- So one report dated year 2100 for `truck-1` makes every later real report stale. That device's position and alerts stop silently for good, and anyone can send it.
- "Device clocks are trusted" is written as a semantic, but its cost isn't stated.
- **Evidence:** a test that sends a future-dated report, then a valid one, and asserts the valid one is still persisted. Then decide on a skew bound, rejection or quarantine.

**2. Does "202 means durable" survive a 30-minute outage?** (high)
- `KAFKA_LOG_RETENTION_MS: 1800000`, the replication factor is 1 (hard-coded in `ensure_topic`), there is one broker, and Kafka's default flush leaves data in page cache.
- If processors are down or lagging for more than 30 minutes, Kafka deletes reports that were already acknowledged, and nothing reports it.
- **Evidence:** stop processors, ingest for longer than retention, restart them, and reconcile acked against committed. Also add a lag-versus-retention alarm.

**3. What happens to processors when NATS blips?** (high)
- `run()` only catches `DATABASE_ERRORS`. A `nats.flush()` timeout or a `CommitFailedError` after a rebalance revokes partitions both propagate, and the process exits.
- `docker-compose.yml` has no `restart:` policy, so replicas die off one by one while ingest keeps returning 202 into a 30-minute log (see 2).
- **Evidence:** restart NATS mid-run at 100k, and separately SIGKILL one processor, then count surviving replicas and reconcile. `evidence/database-fault.json` only covers a DB fault at low load.

**4. Is the "zombie owner" guard ever exercised, or does the zombie just crash?** (high)
- `CLAUDE.md` and the README say the monotonic upsert protects against a zombie during a rebalance.
- In practice, the zombie's offset commit fails (see 3). Two owners reading the same watermark without a row lock both emit alerts, so duplicate alerts are possible.
- **Evidence:** force a rebalance (scale processors 4→6→2) under load, then count duplicate `(device, ts, zone)` alerts and processor exits.

**5. Does the ingest-survives-DB-outage claim survive a load balancer?** (medium)
- The README says that when the database is down, "ingest keeps accepting into Kafka".
- But `/health/ready` returns 503 when PostgreSQL or NATS is down (`api/health.py`). Any load balancer or orchestrator honouring readiness drains every api instance, which kills ingest. This matters because G6 adds an LB.
- **Evidence:** stop `db` behind the planned LB and measure ingest. Likely fix: separate readiness per role.

**6. How many dashboards can it serve, not how many devices?** (high)
- Every benchmark uses four sessions. The cap is 128 sessions per worker, and a world view subscribes to the full stream of about 20k positions/s.
- A real customer opens 200 tabs. Gateway CPU, the NATS egress multiplier and evictions are all unmeasured, and the "every session reconciled" result says nothing about N viewers.
- **Evidence:** sweep sessions 4→64→256 (mixing world and city views) at 100k devices. Record evictions, gateway CPU and p99, and find the ceiling.

**7. What happens when the partition count changes?** (medium)
- 24 partitions caps processors at 24.
- Adding partitions changes `hash(device_id) % n`. During the transition, one device's reports sit on two partitions owned by two consumers. That breaks the "one owner per device" invariant `CLAUDE.md` calls load-bearing, and the watermark logic then drops earlier samples as stale, losing alerts.
- **Evidence:** a written repartition procedure (new topic plus drain-then-switch), or a test showing alert loss under `kafka-topics --alter`.

**8. What does a device resend after its socket drops?** (medium)
- Acks are a cumulative count, `{"type":"ack","count":n}` (`ingest.py`). Produce futures settle out of order across partitions, so a count can't tell a device which reports are durable.
- A real device either resends everything (safe, but the protocol doesn't say so) or guesses wrong. If it resends an unacked sample after a later one has committed, that sample is now stale and its alert is lost.
- **Evidence:** a written client contract (ack per sequence number or highest contiguous index), plus a test of disconnect mid-window.

**9. Does PostgreSQL stay flat for hours, not 15 minutes?** (medium)
- `device_latest` takes about 20k upserts/s, and each one changes `position`. If position is in a GiST index, as `/devices/latest` and insights imply, that rules out HOT updates (Postgres's cheaper in-place updates), so you get index churn, bloat and autovacuum pressure.
- `/insights` runs a per-zone `count(*)` over the fleet on every call.
- The 900 s run cannot show vacuum debt.
- **Evidence:** a 6-hour soak recording `pg_stat_user_tables` dead tuples, table and index size, autovacuum runs, and p99 drift. Also an `/insights` latency run with 1,000 zones.

**10. Are the headline claims literally true?** (high, credibility)
- The README opens with "Every component that carries load scales horizontally", but its own Limits section admits single-instance Kafka, NATS and PostgreSQL.
- Latency is measured on one host against the generator's own clock, so there is no clock skew, and the generator shares 14 CPUs with the stack.
- The design page and README present 100k as "the measured envelope", while the 150k rung has failed only on Kafka memory.
- **Evidence:** a claims audit that checks each README sentence against a file or line, rewords the first paragraph to match Limits, and runs once with the generator on a separate machine (or states that it wasn't).

**Also missing from the product angle:** "alert on every fresh inside report" means one zone over 100k devices produces about 20k alerts/s for one user, and nothing is persisted (Limits: no replay). Nobody has asked whether an operator can act on that stream, or whether entry/exit transitions are the actual product.

Sources (read only, nothing modified): `geo_tracking/{processor.py,schemas.py,ingest.py,bus.py,api/health.py,spatial.py,insights.py}`, `docker-compose.yml`, `README.md`, `CLAUDE.md`, and the vault notes `vision.md`, `questions.md`, `decisions.md` and `loop-ledger.md`.
