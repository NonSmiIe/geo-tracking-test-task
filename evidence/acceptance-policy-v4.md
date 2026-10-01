# Operational acceptance policy v4

Declared on October 2, 2026, before the first run whose counters come from Prometheus. It replaces [v3](acceptance-policy-v3.md) for runs from that commit on; v3 results stay as recorded.

Scope, population, rate, cadence, latency and resources are unchanged from v3. What changes:

- **Source of pipeline counters.** Totals are read from Prometheus, which scrapes every api, gateway and processor replica on its own port every 5 s, before the run and 12 s after consumer lag reaches 0. They are no longer a NATS scatter-gather, where a stalled process silently dropped out of the sum.
- **Consumer lag** comes from the broker's group offsets (kafka-exporter), so it includes partitions no processor owns. An absent lag series is a failure, never a 0.
- **Counters never reset.** The sum of `resets()` over the run window must be 0. Without that check, a restarted process would make the delta under-count.
- **No NATS slow-consumer error** on any gateway, in addition to no eviction and no failed batch.
- **Delivery is judged deduplicated.** Each session keeps a per-device bitmap of report indexes. A repeated event is counted under `duplicates` and excluded from the count and checksum, since delivery is at least once. Count and checksum must still equal the acknowledged set exactly, so a missing event still fails.
