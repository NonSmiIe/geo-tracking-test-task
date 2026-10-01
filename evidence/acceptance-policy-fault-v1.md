# Fault-run acceptance policy v1

Declared on October 2, 2026, before the first G5 fault run. It applies to `scripts/benchmark.py` runs that pass `--fault`; runs without faults stay on [v4](acceptance-policy-v4.md).

A fault kills, restarts or pauses one container mid-run. What must still hold:

- **Faults applied.** Every `docker` action exited 0.
- **Nothing acknowledged is lost.** Every scheduled report is acknowledged, after resends: devices reconnect and resend from their durable prefix. PostgreSQL ends holding every device's newest acknowledged report, by count and by the sum of timestamps. This is the durability verdict. Process counters are not used, because a restarted process resets them.
- **The pipeline recovers.** Consumer lag from the broker drains to 0.
- **Dashboards.** A session that never closed missed no event: deduplicated count and checksum equal the acknowledged set. A session that the fault closed must reconnect and receive events after its last closure. Live events published while it was disconnected are not replayed, by design: it gets a fresh snapshot.

Not judged, but recorded in each file: latency (a fault stalls delivery for its duration), duplicates (at-least-once), memory of restarted containers, failed batches and publish retries.
