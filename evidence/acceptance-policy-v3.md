# Operational acceptance policy v3

Declared on October 2, 2026, before the first 100,000-device run on the Kafka/NATS architecture. It replaces [policy v2](acceptance-policy.md) for runs of that architecture; v2 results stay as recorded.

Scope: the default Compose stack (4 API workers, 4 processors, 24 Kafka partitions) and `scripts/benchmark.py`, 100 active zones, four dashboard sessions: two for the owner of a coverage zone holding every device, one for another user with a world viewport, and one for that user with a small probe viewport.

- **Population.** Every expected report is scheduled, sent and acknowledged by the API, with zero generator drops, rejections or transport errors.
- **Rate.** Acknowledged rate over the whole run, including drain, is at least 99% of the offered rate.
- **Cadence.** Scheduling p99 ≤ 100 ms, at most 0.1% of reports more than 100 ms late, maximum < 1 s.
- **Pipeline.** Processors commit exactly as many fresh reports as were acknowledged. Consumer lag ends at 0, no batch fails and no dashboard session is evicted.
- **Delivery.** Each world session receives every acknowledged position, verified by count and 64-bit identity checksum. Both owner sessions receive every alert; the other user receives none. The probe session receives exactly the positions whose map tile falls inside its subscribed tiles.
- **Latency.** Position and alert delivery p95 < 1 s, measured from the scheduled report timestamp.
- **Resources.** Every container stays below 90% of its memory limit. Memory growth from the second-quarter median to the last-quarter median is at most 64 MiB, which excludes JVM and cache warm-up. Samples are taken every 10 s and at least four are required.
