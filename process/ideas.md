---
title: Fleetline hypothesis backlog
date: 2026-10-02
kind: note
---

# Fleetline hypothesis backlog

Ranked ideas waiting to be tried. Format: the idea, who proposed it, the expected gain, the cost, how to measure it, and its status (open / trying / kept → decisions / disproved → decisions). Charter: loop charter. Settled: [decisions](decisions.md).

## Open

Triage of [2026-10-02-0945-claims-audit-3](ideas/2026-10-02-0945-claims-audit-3.md), all accepted:

- **A1 · the README results table is generated from evidence JSON** by a script, not hand-copied. F8 and F9 were copy errors; one source makes them impossible. The script also writes the commit each run measured (U1). *accepted*
- **A2 · gateway `subscribed` only after NATS confirms the subscriptions** (`await nats.flush()`), so a dashboard never misses the first events after `subscribed` (U6, a code defect). *accepted*
- **A3 · gateway lines carry the socket's request id**, including evictions logged from the NATS callback: the connection holds its id (F23). *accepted*
- **A4 · design page sections 03, 04, 06, 09, 10 and 11 rewritten for one writer**: pool 1, batch 10,000, the real statement order, replays re-emitted, byte-bounded frames, 11.6 ms / 5.8 µs, HOT 98.3% (F2–F6, F19, F20, F22). *accepted*
- **A5 · README prose:** F1, F4, F7, F10–F18 and F21, F24, plus the per-socket limit and labels for U4, U5, U7. *accepted*
- **A6 · claims about the current build** wait for D2 at HEAD and fault reruns at HEAD (U1, U2). *accepted, D2 running*

Triage of [2026-10-02-0940-product-lens](ideas/2026-10-02-0940-product-lens.md):

- **P1 · zone entry and exit events with a durable event log.** Processor-owned `zone_membership` and `zone_events`, written in the batch transaction and keyed by Kafka position so a replay re-reads instead of recomputing. Delivered as frames on the alerts subject and through `GET /zone-events`. `inside_report` stays. *accepted, building next*. Proof: transition tests on PostGIS; a 300k crossing fixture with ground truth under a policy declared first.
- **P2 · dwell time and dwell alerts.** *accepted, after P1*
- **P3 · offline and back-online.** The marker goes through the partition, so the one-owner rule holds. *accepted, after P1*
- **P4 · polygon zones** as `area geography` with one predicate and an extended superset test. *accepted*
- **P5 · webhooks from the event log**, through a separate dispatcher. *accepted, after P1*
- **P6 · schedules and speed rules**, evaluated against the sample's own timestamp. *accepted, low priority*
- **P7 · track history.** *deferred:* about 0.5–1 PostgreSQL core does not fit the laptop at 300k; it needs a second store. → questions
- **P8 · organisations.** *deferred:* the largest blast radius; the brief allows mock identity. Device credentials stay the stated limit.

Triage of [2026-10-02-0935-devils-advocate](ideas/2026-10-02-0935-devils-advocate.md):

- **D1 · connections are not devices.** The headline is restated as reports/s through N connections, with the generator's shape beside it. Measure a per-connection ceiling: 20k sockets at batch-size 1 through the edge. *accepted, docs now; measurement queued*
- **D2 · one run per decision.** 3 runs of 300k at HEAD, reporting the spread. Decisions settled by one run (fetch window) are labelled single-run until an interleaved A/B exists. *accepted, next measurement*
- **D3 · durability only proven for graceful stops.** Add s10 Kafka SIGKILL and s11 PostgreSQL SIGKILL under load to the campaign; VM kill goes to questions (it needs the host). *accepted*
- **D5 · single points of failure in compose.** A 3-broker KRaft Kafka with RF 3 and min.insync 2, a 3-node NATS cluster, and 2 edges; kill a broker leader, a NATS node and an edge at 100k. Raises horizontal scale and failure tolerance. *accepted, large*
- **D6 · faults at 300k:** s2, s3, s5 and s9 at 300k with drain time. Failure tolerance in `vision.md` is lowered to 4 until then. *accepted*
- **D7 · latency scope.** README states exactly what the figure measures; the benchmark reports per-session percentiles beside the merged ones. *accepted, docs now*
- **D8 · dashboard count:** 300k with 100 mixed sessions; gateway CPU, evictions and slow consumers. *accepted*
- **D9 · steady state:** 300k for 2 h or more. *accepted, run overnight-style while agents work*
- **D10 · document truth:** waits for the claims auditor running now. *accepted*
- **D11 · spoofing next to the headline.** README states it beside the 300k claim. *accepted, docs now*

Triage of [2026-10-02-0845-review-bcce466](ideas/2026-10-02-0845-review-bcce466.md):

- **W1 · high: the writer parks outside `getmany`, so aiokafka stops rejoining after `max_poll_interval` and the watchdog stays quiet.** Verified in `group_coordinator.py:723`. The `consuming` gate is deleted and the writer always polls; aiokafka holds back records during a reassignment. *kept → `1cf61ae`*
- **W2 · high: position frames were unbounded.** One rule for every NATS frame: chunked by bytes (256 KiB), replacing `alert_frame_items`. Two chunking rules for one payload limit would drift. `MaxPayloadError` is not special-cased, because a frame cannot exceed the limit any more. *kept → `1cf61ae`*
- **W3 · medium: the drop-across-revoke branch** either never runs or skips offsets. Deleted, with its test. *kept → `1cf61ae`*
- **W4 · medium: revoke timeout leaked old batches into the next generation.** On timeout both stages are cancelled, whatever was in flight is dropped (replay covers it), and fresh stages start. In-flight accounting is reset whenever the stages restart. *kept → `1cf61ae`*
- **W5 · DB-error path.** (a) is moot after W4, because a write still running at handover is cancelled. (b) AIMD on failure is *rejected*: smaller batches raise the per-row cost on a database that is already degraded, and a statement past `statement_timeout` means the database is effectively down, which the `BatchesFailing` alert reports.
- **W6 · low: watchdog spans the outbox wait.** `polled` is re-stamped after the hand-off, so the watchdog bounds the writer alone; delivery is bounded by `PublishStalled`. *kept → `1cf61ae`*
- **W7 · low: shutdown versus assign.** Moot once the gate is gone. *closed*
- **W8 · batching has no floor.** A fetch window: `fetch_max_wait_ms` = `processor_batch_window_ms` (100 ms) and `fetch_min_bytes` = `fetch_max_bytes`. A batch is then min(window × rate, cap), and latency is bounded by the window. *disproved → `c459ea4`: batches doubled (999 rows), but the cost per row stayed about 18 µs, Kafka CPU doubled and p95 rose to 1,071 ms*

Triage of [2026-10-02-0835-security-lens](ideas/2026-10-02-0835-security-lens.md), all accepted:

- **X1 · critical: matches per report are unbounded.** 1,000 world-sized zones make one batch 2M match rows: a timeout, then a poison partition forever. Fix at the source, in zone CRUD: radius ≤ 500 km (schema plus DB CHECK), and a new or moved zone may overlap at most 50 of the user's active zones, which bounds per-point overlap. *kept → `8b05a54`*
- **X2 · high: gateway memory.** The queue budget is 128 × 8 MiB against a 320 MiB container. Fix: 1 MiB per connection, 32 MiB per NATS subscription, 8 dashboards per user, a 4 KiB WS frame limit on gateways, and viewport changes paced to 4/s per socket. *kept → `c862bbe`, as a gateway byte budget that evicts the largest backlog*
- **X3 · high: one body limit for every route**, in one ASGI middleware that replaces `bounded_body`; the edge enforces timeouts and per-IP and per-user rate limits. *kept → `c862bbe` (per-user only, since Docker NAT gives every client one IP)*
- **X4 · high: spoofing.** Clamp future timestamps to receive time, and state plainly in the README that ingestion is unauthenticated, with what a real design needs. *kept → `c862bbe`; clamp reversed → `e97b1e0` (it falsified real reports under clock skew; one 30 s reject bound instead)*
- **X5 · medium:** Grafana with no login form at all (provisioned, anonymous viewer); a non-superuser app role and a `pg_monitor` exporter role; `/stats` cached for 5 s; a NATS token; `no-new-privileges` on every container; the demo zone counted against the quota. *kept → `c862bbe`; demo quota rejected: one zone per user, already bounded*
- **X6 · medium: per-socket token bucket on `/ingest`.** A flooding socket starves others in the FIFO window. *open, after X1–X5*

Triage of [2026-10-02-0700-claims-audit-2](ideas/2026-10-02-0700-claims-audit-2.md), all accepted:

- **C1 · code: liveness was stamped by the supervisor, not by lanes, and nothing restarted a stuck processor.** Fix: each lane stamps its poll; the supervisor exits the process when the oldest lane is stale, so compose restarts it. *kept → `c3c2761`; ProcessorStalled deleted (it could never fire)*
- **C2 · code: a NATS slow-consumer drop left dashboards unaware.** Fix: the gateway resyncs the connections routed to the dropped subscription's subject. *kept → `c3c2761`*
- **C3 · code: `IngestShedding` divided HTTP refusals by all reports.** Fix: an HTTP-only denominator; promtool tests for every one of the 11 alerts. *kept → `c3c2761`*
- **C4 · text:** PG18; processor pool 4; alert latency gap up to about 12 ms; faults fired at 62–71 s; the "newest report per device" wording; published ports; generator HTTP transport; demo endpoints; policy v4; unsupported prose removed or backed by a stored probe. *kept → `c3c2761`; `evidence/capacity/upsert-probe.txt`*

Triage of [2026-10-02-0425-scale-architect](ideas/2026-10-02-0425-scale-architect.md), all accepted except where noted. Measured budget: about 130 µs CPU per report (api 46, db 32, processor 18, gateway 12, edge 7.5, kafka 7–17). The target for 1M on 14 cores is about 55.

- **S1 · framed `/ingest`.** One WebSocket message carries an array of reports, decoded in one msgspec pass that gives the same rejections; a whole-frame reject keeps the ack prefix exact. The generator frames too. Expect api 46 → about 20 µs. *kept → `85b0ff9`; 300k passes*
- **S2 · confluent-kafka producer with `murmur2_random`** (pinned by a test against aiokafka's partitioner), lz4, linger 20 ms. Expect api about −12 µs and broker about −40%. *kept → `ceae966`; api 40 → 26 µs per report*
- **S3 · `COLLATE "C"` on `device_id`; drop `pool_pre_ping` in processors** (18k BEGIN, 9k ROLLBACK per 9k batches). *built → `cf5fd30`*
- **S4 · one transaction per owned partition, concurrently; uvloop in processors.** *lanes built → `cf5fd30`; uvloop open*
- **S5 · in-processor zone candidate index guarded by a zone epoch.** Zero gain on the benchmark fixture; realistic fleets only. *open*
- **S6 · generator and observer CPU via getrusage; sink run at 1M before any server rung.** *with S1*
- **S7 · Citus re-shaped:** distribute on `kafka_partition`, with a shard per partition. The decided shape (distribute on `device_id`) gave no gain, because matching never touches `device_latest`, and it made every batch a 2PC. It pays only past one primary (about 3M devices). Built after S1–S4, as a horizontal-shape proof, not a speed claim. *decision revised*
- **Profiling:** py-spy inside a Linux container with `SYS_PTRACE` (macOS needs root). *with S1*

Triage of [2026-10-02-0330-review-0f4bb6c](ideas/2026-10-02-0330-review-0f4bb6c.md), all accepted, before O1:

- **W1 · high: a cancelled `Window` waiter leaks permits and kills a socket's ack callback.** Fix: `wake` drops done futures without taking permits; the waiter removes itself only if still queued; a test that cancels then releases in the same step. *kept → `f7b3aae`*
- **P1 · medium: a replay loses alerts for a device with several samples in the batch.** Root cause: replay was detected by equality with the newest stored row. Correct design: a `consumer_progress(topic, partition, offset)` row written in the batch transaction; records at or below it are persisted-but-maybe-unpublished and are all re-emitted, records above go through the watermark. Also closes FA-5 (ambiguous commit). *kept → `f7b3aae`, decisions*
- **B1 · benchmark hangs:** a lane past the reconnect cap blocks `put(None)`; an observer whose subscribe keeps failing ignores `stop`; delivery must be judged deduplicated (per-device last timestamp), with duplicates counted. *kept → `f7b3aae`*
- **S1 · `zone_occupancy` repeats its SELECT in two branches**; use the single `IS NULL OR = ANY` predicate. 003 downgrade must restore 002's `grid_cells`. Model lacks `fillfactor=70` (SQLAlchemy 2.0 has no table storage option; the migration owns it). *kept → `f7b3aae`*
- **S2 · `plan_cache_mode=force_custom_plan` is a global knob only two queries need.** Scope it: `SET` on `zone_occupancy`, `SET LOCAL` in the snapshot transaction. *kept → `f7b3aae`*
- **S3 · zones wider than ~16° seq-scan `device_latest`** (no index behind `&&` since H2). Measure at 1M rows before deciding. *open*

Triage of the SRE lens ([2026-10-02-0325-sre-lens](ideas/2026-10-02-0325-sre-lens.md)), all accepted, in this order:

- **O1 · Prometheus exposition on every process; delete the NATS scatter-gather.** Histograms with a time base replace the 4096-sample deques; processor gets its own HTTP port; benchmark reads Prometheus. Blind spots it closes: silent instance dropout, lag reported only by live owners, no pool/503/edge-queue numbers. *kept → `5b4840f`; per-role registries*
- **O2 · `observability` compose profile:** Prometheus (dns_sd per role), Grafana provisioned dashboard, kafka-exporter (group lag independent of processors), postgres-exporter, nats-exporter, HAProxy prometheus-exporter. *kept → `5b4840f` (core services, not a profile)*
- **O3 · alert rules + `promtool test rules`.** *kept → `5b4840f`, 11 alerts, 5 test cases*
- **O4 · runbook per alert in README.** *kept → `5b4840f`; also DA7 repartitioning*
- **O5 · JSON logs, rebalance listener, eviction logs, request ids.** *kept → `a2aaaa8`/`7df40fd` (request ids open)*
- **O6 · processor liveness (last poll age) + k8s manifests with kubeconform.** *liveness kept → `5b4840f`, watchdog `c3c2761`; manifests kept → `58d314f`*
- **O7 · claim defect:** README said gateway readiness checks NATS. *fixed → `5b4840f`*

Triage of the review of 157f5ff ([2026-10-02-0400-review-157f5ff](ideas/2026-10-02-0400-review-157f5ff.md)) and of the failure architect's plan ([2026-10-02-0405-failure-architect](ideas/2026-10-02-0405-failure-architect.md)), all accepted:

- **R1–R3 · ingest window.** A failed send leaked a slot; a flush hung after a failure; HTTP starved behind sockets. Fix: one FIFO semaphore as the single admission point, release on every exit, close the socket immediately on failure. *in progress*
- **R4 · commit.** `IllegalStateError` on a rebalance crashed the processor; the publish retry re-sent buffered frames. Fix: commit only assigned partitions, publish once, then flush until a deadline and exit for a restart past it. *in progress*
- **FA-1 · harness.**
  - The generator must reconnect and resend from the ack prefix.
  - Observers must survive a close.
  - Durability is judged from PostgreSQL rows and Kafka group offsets, not process counters.
  - Delivery is judged as a set, with duplicates counted.
  - Latency is judged per 10 s bucket, with the fault window excused.
  - *open, before G5 runs*
- **FA-2 · PID 1.** The processor is PID 1 with no SIGTERM handling, so every stop is a SIGKILL and the consumer never leaves the group. Fix: `init: true` and a SIGTERM → cancel handler. *in progress*
- **FA-3 · asyncpg timeout.** There is no client-side `command_timeout`, so a frozen database hangs processors. *in progress*
- **FA-4 · readiness and sessions.** A NATS blip marks gateways down and `shutdown-sessions` kills every dashboard. Fix: drop `shutdown-sessions`; gateway readiness is liveness, since NATS reconnects by itself. *in progress*
- **FA-5 · ambiguous commit.** If the DB connection dies after COMMIT, a replay finds the batch stale, so its live events are lost and the commit is never counted. Documented loss; it needs a test with a proxy. *open*
- **FA-6 · graceful shutdown.** uvicorn's graceful shutdown is 15 s, longer than docker's 10 s stop timeout. Align with `stop_grace_period`. *in progress*
- **G5 scenarios S1–S8:** NATS outage, Kafka restart, rolling processor restart, zombie pause, DB kill, DB freeze, api kill, gateway kill. *open, after FA-1*

Triage of [2026-10-02-0310-devils-advocate](ideas/2026-10-02-0310-devils-advocate.md) (all 10 accepted):

- **DA1 · defect, high: a future-dated report freezes a device forever.** Fix: ingest rejects timestamps beyond now + a 5-minute skew bound with 422, in one place (`schemas.Report`). Add a test. *next, before H1 measurement*
- **DA2 · defect, high: Kafka retention of 30 minutes can delete acked reports during a processor outage.** *kept → `ae23741`: 24 h / 1 GiB per partition, log on the volume, progress by topic ID; proved across a Kafka recreation*
- **DA3 · defect, high: a processor exits on a NATS flush failure or a rebalance `CommitFailedError`, and there is no restart policy.** Fix: publishing retries after the DB commit until NATS accepts (before the offset commit), `CommitFailedError` is treated as a lost partition and the loop continues, and every service gets `restart: unless-stopped`. Prove it with a NATS restart and a processor SIGKILL at 100k. *open*
- **DA4 · semantics: a zombie during a rebalance can emit duplicate alerts.** Inherent to at-least-once. Document it, then measure duplicates while scaling processors 4→6→2 under load. *open*
- **DA5 · defect, medium: `/health/ready` on api depends on PostgreSQL and NATS, so a load balancer would drain ingest during a DB outage.** Fix: readiness checks only what ingest needs (Kafka); the database is reported separately. *with H1/G6*
- **DA6 · gap: dashboard-count ceiling unmeasured.** Sweep 4→64→256 sessions at 100k. *open, after H1*
- **DA7 · gap: repartitioning procedure.** Write it into the README (new topic, drain, switch) instead of `--alter`. *open*
- **DA8 · defect, medium: the cumulative ack count does not tell a device which reports are durable.** Fix: ack the highest contiguous sequence index per connection. Add a test for a disconnect mid-window. *open*
- **DA9 · gap: soak behaviour of `device_latest` (HOT updates, bloat, vacuum).** Ties into H2. *open*
- **DA10 · claims.** Triage of [2026-10-02-0330-claims-auditor](ideas/2026-10-02-0330-claims-auditor.md):
  - **Fixed in code by H1 (`c911c03`):** F1 (load balancer), F2 (configurable replication factor), F4 (api now scales by replicas). The text is still to fix.
  - **Code defects:** F5, the `/ingest` socket ignoring `produce_window` (fix together with DA8); F7, `migrate` has no limits; F8, the frame `decode()` per recipient (perf, plus wording).
  - **Wording, owed in one docs pass after the next measurements:** F3 (partitions), F6 (the zones arrow comes from api), F9 (alert latency columns), F10 (rounding), U1 ("to a dashboard WebSocket client"), U2 (record `docker info` in results), U3 ("count and identity checksum"), U4, U5 (a whole-run batch histogram), U6 (extrapolation, not an upper bound; DB growth accelerates), U7 (measure bytes per session), U8 (commit the 60-zone probe as evidence), U9 ("not measured"), U10 (a processor kill under load → G5).
  - **Fixture realism (U6):** 99 of the 100 zones sit at one point, so each report matches exactly one zone. Add a realistic zone-density fixture before claiming DB scaling. *open*

Source for H1–H4: [2026-10-02-0250-performance-architect](ideas/2026-10-02-0250-performance-architect.md).

- **H1 Split the gateway out of api; SO_REUSEPORT per worker; 6–8 workers.** Perf architect. Evidence: ingest per worker is skewed 2.2×, and one loop carries about half the 21 MB/s fanout. Expected ceiling gain 1.5–2×. Low cost. Measure at 150k: per-worker max/mean ≤ 1.2, hot-loop lag p99, CPU-ms per report. *open, first*
- **H2 Drop the GiST index on `device_latest` + fillfactor 70 → HOT upserts; snapshots and insights move to a tile column.** Perf architect. Expected 30–50% less DB CPU per report. Read `n_tup_hot_upd` first. Measure at 200k. *open*
- **H3 confluent-kafka producer with the `murmur2_random` partitioner** (anything else breaks device→partition). Expected 15–25% API ingest CPU. Measure at 150k, then a 900 s run. *open*
- **H4a msgspec Report with identical rejections; H4b array frames on /ingest** (a protocol change, labelled as such). 10–20% and 20–40%. *open*
- **Horizontal-scale gap** (found by Denis): no LB in front of api, Kafka RF hard-coded to 1, NATS single URL, PostgreSQL unsharded. *open; Citus decided — see decisions.md, after H1*
- Rated weak by the perf architect: one SQL round trip per batch, Kafka bundling (conflicts with the per-device key), uvloop (already on), partition/processor counts.

- **Find the ceiling before optimizing.** Me. Gain: a known bottleneck. Cost: one 150k and one 200k rung. Measure: acceptance v3 plus `docker stats`. At 100k the API used 2.3 cores of a 4-CPU limit, PostgreSQL 0.9 of 3, and processors were nearly idle, so the API is the first suspect. *open*
- **Generator against an ack-only sink.** Charter. Gain: separates the generator's own limit from the server's. Cost: a tiny sink script. *open*
