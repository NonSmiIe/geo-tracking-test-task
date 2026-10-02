---
title: Fleetline open questions
date: 2026-10-02
kind: note
---

# Fleetline open questions

Hard questions that have no evidence-backed answer yet. Close one only with a link to an evidence file or code. Charter: loop charter.

## Open

- **Does the service scale horizontally across hosts?** (Denis, 2026-10-02) Partly, by design: api and processors are stateless or partition-owned. But there is no load balancer in front of api, Kafka RF is hard-coded to 1, NATS takes a single URL and PostgreSQL is a single writer. → G6.
- **Why did Kafka memory grow by more than 64 MiB over the 150k rung?** Heap is fixed at 768m, so the growth is page cache, direct buffers or off-heap. Find it before claiming 150k.
- **Where exactly is the ceiling on this laptop, and what saturates first?** The perf architect predicts the hottest api event loop at about 150k devices. The 150k rung is running.

## Вопросы Денису

_(пусто)_

- **Does Fleetline scale across hosts, not containers?** (devil's advocate, 2026-10-02, D4.) Every measurement ran on one laptop. To close it: the load generator on a second machine, then the Citus proof bar from `decisions.md`. Open until an evidence file exists.
- **What does an ack survive?** (D3.) A container SIGKILL is testable here; a Docker VM or host crash with RF 1 loses whatever sits only in the page cache. To close it: RF 3 with min.insync 2 (D5), plus a VM kill run.
