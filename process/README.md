# How Fleetline was built

These are the working notes of the build, kept as they were written: plans before the work, audits after inspecting it, adversarial reviews, the decision log and the hand-offs between sessions. They are dated and they are history. Where a note disagrees with the code, the code, [`README.md`](../README.md) and [`evidence/`](../evidence) are current; the note shows what was believed at the time and why it changed.

## Read in this order

**1 October: first implementation (10k devices, PostGIS only)**

- [research-2026-10-01](research-2026-10-01.md): the assignment's requirements and the architecture evidence behind the first design.
- [plan-2026-10-01](plan-2026-10-01.md): the adversarial implementation plan.
- [audit-2026-10-01](audit-2026-10-01.md): what the first load run broke and what was fixed.
- [handoff-live-implementation](handoff-live-implementation.md): mid-build checkpoint.
- [note-2026-10-02](note-2026-10-02.md): the acceptance policy, declared before the run it judged.
- [report-2026-10-02](report-2026-10-02.md) with [its evidence](report-2026-10-02/): the 10k submission, verified.

**2 October: review, then the rewrite to 100k+ (Kafka ingest, NATS fanout)**

- [audit-2026-10-02](audit-2026-10-02.md): a hard review of that submission. It found a real denial-of-service bug and produced a rework list.
- [plan-2026-10-02](plan-2026-10-02.md): why the first design stopped at about 4k reports/s, and the rewrite.
- [vision](vision.md): what "ideal" means on each axis, with the current level and its evidence.
- [loop-ledger](loop-ledger.md): one entry per improvement round through the night, in Russian.
- [ideas](ideas.md): the ranked hypothesis backlog. Every idea is kept, tried, or disproved with a measurement.
- [ideas/](ideas/): the rotating review lenses that fed the backlog: performance, scale, failure, SRE, security, product, a devil's advocate, three claims audits checking the README against evidence, and code reviews of individual commits.
- [decisions](decisions.md): settled choices, with the rejected alternatives and what would reopen each one.
- [questions](questions.md): questions that had no evidence-backed answer yet.
- [plan-zone-events](plan-zone-events.md): the zone entry/exit events plan. It was built, then taken out of master (kept on `feature/zone-events`) because its membership read cost 92 ms per call under load.
- [handoff-2026-10-02](handoff-2026-10-02.md): where the overnight loop stopped.
- [report-final-pass](report-final-pass.md): the final cleanup, the 168-test suite and the runtime check.
