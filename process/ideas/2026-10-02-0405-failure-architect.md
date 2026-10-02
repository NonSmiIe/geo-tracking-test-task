---
title: Failure architect, G5 campaign plan
date: 2026-10-02
kind: plan
---

# G5 failure campaign: Fleetline under 100k / 20k rps / 300 s

**Fix the harness first.** Four of the eight scenarios below fail for reasons in the harness, not the server:
- The generator never reconnects. A closed `/ingest` socket ends its lane for the rest of the run, and every later report on that lane becomes `generator_dropped` (generator.py:159-186). The resend-from-ack-`n` contract has never run live.
- Observers stop on the first close (benchmark.py:63).
- Counters exist per process. A restarted processor or gateway starts again at 0. `merge` only adds up the instances still running (metrics.py:52-59), so `committed == acked` and `no_session_evicted` turn into noise.
- `consumer_lag` only covers partitions some processor currently owns (processor.py:34-40). While a partition has no owner it reads 0, which wrongly passes `consumer_lag_drained`.
- `inject` runs one docker command at a time (benchmark.py:108-128). A `stop` that takes 10 s pushes every later event back, so read the real times from `faults[].at_seconds`.

**How a fault run should be judged.** These replace the current checks; they do not sit beside them.
1. **Durability.** The generator writes out the last acked timestamp for each device. Then `device_latest` holds every prefix device with exactly that `reported_at`. Kafka's group offsets equal its end offsets, read with `kafka-consumer-groups --describe`, not the gauge.
2. **Delivery.** Observers keep a set of `(device, ts)`, not a checksum. The observed set must contain all acked reports, except a gap only in the fault window of an ephemeral path. Duplicates are counted, reported and capped at one batch per affected owner; they are not required to be zero.
3. **Latency.** p95 is judged per 10 s bucket. The budget is suspended from the fault until recovery. Recovery means p95 < 1 s within 30 s of the last `start`, or 60 s for kafka and db.
4. **Resources.** Containers that were restarted are left out of `memory_stable`.
5. **Workload.** `transport_errors` and `http_503` are allowed when the report was resent and acked later.

## Scenarios, highest expected value first

**S1. NATS outage.** `--fault 90:stop:geo-tracking-test-task-nats-1 --fault 100:start:geo-tracking-test-task-nats-1`
- *Must hold:* no acked report is lost, there are no duplicate positions, the dashboards stay connected, and lag drains within 60 s.
- *Predicted defects:*
  - Gateway readiness fails (gateway_app.py:47-51). HAProxy then marks both gateways down and `on-marked-down shutdown-sessions` (haproxy.cfg:19) closes every dashboard. A 2 s NATS blip becomes a total UI outage.
  - `publish` resends the whole batch after a partial buffer, an `OutboundBufferLimitError` or a flush timeout (processor.py:88-98). Messages that were already buffered go out twice after reconnect.
- *Acceptance:*
  - `not_closed` fails correctly.
  - `no_session_evicted` wrongly passes, because HAProxy closes the sessions, not the eviction counter.
  - The final `metrics()` can come back with no `roles`, so benchmark.py:200 raises KeyError.

**S2. Kafka broker restart.** `--fault 90:kill:…-kafka-1 --fault 92:start:…-kafka-1`
- *Must hold:* every acked report is in `device_latest`, devices reconnect and resend from their ack count, and there are no extra commits beyond what was in flight.
- *Predicted defects:*
  - api readiness fails (api/health.py:17-24). With `shutdown-sessions` that cuts all 64 ingest sockets.
  - Separately, the first failed produce closes the socket with 1011 (ingest.py:89-91, 108, 120). Reports produced after it can still become durable without being acked, so committed > acked.
- *Acceptance:* `exact_population` and `no_loss_or_overload` fail. The pass condition should be acked ⊆ committed, with committed − acked ≤ the in-flight window.

**S3. Rolling processor restart.** `--fault 60:restart:…-processor-1 --fault 85:restart:…-processor-2 --fault 110:restart:…-processor-3 --fault 135:restart:…-processor-4`
- *Must hold:* each restart stalls its partitions for 5 s or less, lag drains, and the final state is exact.
- *Predicted defects:*
  - The processor runs as PID 1 with no SIGTERM handler and no `init` (docker-compose.yml:129, processor.py:170-172). The kernel ignores SIGTERM for PID 1, so every stop waits 10 s and ends in SIGKILL.
  - The finally block never runs, so the consumer never leaves the group. Each restart then costs the 10 s stop plus the 10 s session timeout, followed by a replay.
- *Acceptance:* `every_acked_report_committed` fails wrongly (counter reset). `consumer_lag_drained` can pass wrongly while partitions have no owner.

**S4. Zombie owner.** `--fault 120:pause:…-processor-1 --fault 135:unpause:…-processor-1` (15 s is longer than the 10 s session timeout)
- *Must hold:* the monotonic upsert keeps state correct, and duplicates stay within one batch per zombie.
- *Predicted defect:* the zombie read its watermarks before the pause, so it treats its in-flight batch as fresh. It re-publishes positions and alerts and counts `reports_committed` a second time (processor.py:52-59). `CommitFailedError` is then swallowed (processor.py:125-127).
- *Acceptance:* `committed == acked` and the checksum checks fail wrongly. They should count duplicates instead (DA4).

**S5. Database crash.** `--fault 120:kill:…-db-1 --fault 125:start:…-db-1`
- *Must hold:* the final `device_latest` is exact, and lag drains within 60 s after the db is healthy again.
- *Predicted defect:* if the kill lands on COMMIT, the commit can succeed while the client sees an error. The processor seeks back (processor.py:113-119), the replay finds those rows stale, and they are never published or counted. That is a live loss, plus committed < acked.
- *Acceptance:* `no_failed_batches` fails wrongly. In a fault run it should be required to be above 0 and bounded.

**S6. Database freeze.** `--fault 200:pause:…-db-1 --fault 215:unpause:…-db-1`
- *Predicted defect:* the only timeout is server-side `statement_timeout`. asyncpg has no `command_timeout` (db.py:12-21), so all four processors hang silently, and REST hangs past its 1 s checkout limit.
- *Must hold:* lag recovers within 60 s.

**S7. api replica kill.** `--fault 100:kill:…-api-2 --fault 105:start:…-api-2`
- *Must hold:* 16 sockets reconnect and resend from their ack count, and there is no loss.
- *Predicted defect:*
  - This exercises the ack-prefix contract (DA8) and the generator's missing reconnect (generator.py:159-186).
  - A `stop` variant shows uvicorn's `--timeout-graceful-shutdown 15` (Dockerfile) is longer than docker's 10 s stop timeout, so the drain is always cut short.

**S8. Gateway kill.** `--fault 100:kill:…-gateway-1 --fault 110:start:…-gateway-1`
- *Must hold:* sessions on the surviving gateway reconcile exactly with zero evictions. Sessions that were on gateway-1 reconnect, and only their gap is excused.
- *Predicted defect:* not severe. The restarted container's NATS reconnect and `/ws` routing after it comes back still need checking.
- *Acceptance:* the eviction count from the restarted gateway is reset.

## Needs a test or config change, not a docker action
- **Kafka retention (DA2).** Retention is 30 min and checked every 5 min, so a 300 s run cannot reach it. Use a topic with `retention.ms` of a few seconds plus a processor outage.
- **Partitions and one-way loss.** `docker network disconnect` needs two arguments, but `inject` passes `split(":",2)` as one argv. Asymmetric loss and latency need `tc netem`.
- **Exact ambiguous commit.** Use a TCP proxy that drops the connection after COMMIT is sent.
- **Crash between DB commit and NATS publish.** README documents this as a live loss. Prove it with an exception injected at processor.py:86.
- **Partial NATS buffer, then flush timeout.** Unit test with a fake client against processor.py:88-98.
- **NATS slow consumer on the gateway.** At `pending_bytes_limit` (gateway.py:100) messages are dropped silently.
- **Disk full** on the Kafka log dir or pgdata.
- **Kafka RF > 1 failover.** Not possible with one broker.

The task was read-only, so I modified nothing and wrote nothing to the vault; this campaign plan still needs to go there.
