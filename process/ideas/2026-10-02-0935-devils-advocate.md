---
title: Devil's advocate after one writer per processor
date: 2026-10-02
kind: audit
---

# Devil's advocate (raw)

Triage: [ideas](../ideas.md) · [questions](../questions.md). Read-only; line numbers are `README.md` unless another file is named.

1. **"300,000 devices" are 64 concentrator sockets.** The generator runs 8 processes × 8 connections × frames of 100 reports (`generator.py:87,90,176`), about 940 reports/s per socket. The edge allows `maxconn 20000` and 4,096 per server. *Status:* missing. *Settle:* 20k sockets at batch-size 1, find the per-connection ceiling, and restate the claim as reports/s over N connections.
2. **Reproducibility.** 2 of 11 runs at 300k passed (s1 753 ms, one-writer 876 ms). `rung-300k-secured` (981/1,000 ms) failed and is missing from the README table. 876 ms predates `1cf61ae`, `e1b83e1` and `10afc79`. The window revert is a single A/B inside a 753–1,082 ms spread. *Status:* weak. *Settle:* 3–5 runs at HEAD with the spread; interleaved A/B.
3. **"Durable" is a single-broker page cache.** acks=all with RF 1 and no flush settings. The faults are `docker restart`, a graceful SIGTERM. Retention at 300k is about 2 h. *Status:* weak. *Settle:* SIGKILL Kafka and PostgreSQL under load, kill the VM, then RF 3 with min.insync 2 and a leader kill.
4. **N hosts or N containers?** Everything ran on one laptop. H1's ceiling gain is not demonstrated. 24 partitions cap processors; PostgreSQL is one writer; Citus is not built; 1M is a budget. *Status:* missing. *Settle:* load generator on a second machine; the Citus proof bar.
5. **SPOFs:** one Kafka, NATS, PostgreSQL and edge. k8s covers only the stateless tiers. *Status:* missing. *Settle:* 3-broker Kafka, NATS cluster and 2 edges in compose; kill one of each at 100k.
6. **Faults at the claimed load:** the campaign ran at 100k. vision rates failure tolerance 5/5 while listing "a fault during 300k" as a gap. *Status:* weak, inflated. *Settle:* s2, s3, s5 and s9 at 300k, with drain time and p95 recovery.
7. **What latency measures:** observer receipt minus report timestamp on the shared host clock, merged across 4 sessions, 3 of them world views. No browser, network or TLS. *Settle:* per-session reporting plus a probe through a real network hop.
8. **How many dashboards:** 4 sessions in every run. The cap is 256 in compose. A world view is about 1.7 MB/s at 20k/s; 60k/s is unmeasured. *Settle:* 300k with 50–250 mixed sessions; gateway CPU, evictions and slow consumers.
9. **Steady state:** 300k ran 300 s only. Autovacuum, checkpoints, WAL, segment rolls and retention never came into play. *Settle:* 300k for 2 h or more.
10. **Which document is the truth.** The design page contradicts README and code:
    - pool 2 versus 1;
    - batch 2,000 versus 10,000;
    - upsert 16 ms versus 11.6 ms per 2,000 rows;
    - commit-to-publish crash "loses those frames" versus "loses no live event".

    *Settle:* the claims auditor over both documents.

Settled and not re-raised: alerts on every inside report, Kafka + NATS, no PgBouncer, liveness-only health. Unauthenticated ingest means anyone can trigger any user's alerts by spoofing a `device_id`; that should sit next to the headline claim.
