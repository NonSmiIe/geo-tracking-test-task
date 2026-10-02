# Verification results

The final operational acceptance run passed on October 2, 2026. The service accepted every scheduled report, every subscribed session reconciled its expected stream, and memory, queues and database connections stayed within the declared budgets.

## Sustained load

| Measurement | Result |
| --- | --- |
| Devices | 10,000, moving every five seconds |
| Duration including drain | 900.028 seconds |
| Scheduled / accepted | 1,800,000 / 1,800,000 |
| Accepted rate | 1,999.94 reports/s |
| Generator drops / HTTP rejections / transport failures | 0 / 0 / 0 |
| End-to-end delivery p50 / p95 / p99 | 34 / 92 / 127 ms |
| Maximum delivery latency | 362 ms |
| Scheduling p99 / maximum | 2 / 141 ms |
| Reports scheduled over 100 ms late | 175, or 0.00972% |
| Producer queue high-water | 385 reports |
| Application ingress high-water | 256 reports |
| Application peak container memory | 120.2 MiB of 512 MiB |
| Database peak container memory | 159.3 MiB of 1 GiB |
| Application first-to-last quarter median growth | 4 MiB |
| Database first-to-last quarter median growth | 10.6 MiB |

Fixture: 100 active zones, one 10 km coverage zone around Riga and 99 distant zones; two sessions for the coverage-zone owner and one for another user. Each of the three sessions received and reconciled all 1,800,000 location reports. Both owner sessions received and reconciled all 1,800,000 private alerts; the other user received zero of those alerts. Reconciliation uses counts and 64-bit report-identity checksums. Latency is measured from the scheduled timestamp, including producer and server waiting.

Hardware: Apple M4 Pro, native ARM64 Docker; application limited to two CPUs and 512 MiB, PostgreSQL limited to two CPUs and 1 GiB. PostgreSQL 17.6, PostGIS 3.6.4, Python 3.12.13. These results establish this deployment's measured envelope; higher report rates, 100 dashboard sessions and horizontal multi-process deployment have not been established by this campaign.

Raw final measurements: [baseline.json](baseline.json). The [operational acceptance policy](acceptance-policy.md) was recorded before the fresh run. Process counters in the raw API samples are cumulative and include previous runs and the deliberate database-fault probe; the benchmark's population and checksums are isolated by its unique device prefix.

## Earlier runs retained

- [Initial run](baseline-initial.json): 128 producer connections; 316 generator drops, 1,799,684 accepted reports, delivery p95 1,103 ms. Failed. All submitted reports still reconciled.
- [Second run](baseline-second.json): 256 producer connections; all 1,800,000 reports accepted and reconciled, delivery p95 131 ms. Failed its earlier zero-timer-warning gate because 126 reports were scheduled over 100 ms late. The architecture review identified that gate as an unjustified hard timing constraint; this result was preserved and never rescored as passed.
- Final fresh run used explicit sustained-rate, percentile/tail cadence, loss, delivery and resource criteria. It passed every criterion.

## Correctness and failure recovery

41 tests passed against real PostGIS in an isolated `geo_test` database. They cover metre-based containment, boundary tolerance, high latitude, antimeridian positions, variable radii/global outliers, ownership, duplicate/stale reports, intermediate zone crossings, multiple sessions, disconnects, blocked writers, handshake races, output rollback, aggregate HTTP-body limits, database errors/recovery, restart watermarks, processor failure, SDK structured-output contracts and benchmark acceptance semantics.

The actual container database was stopped and restarted. Readiness, ingestion and CRUD returned 503 during the outage; readiness and ingestion recovered to 200 without an application restart. [Fault evidence](database-fault.json).

The final live `scripts/drive.py` check passed: inside-then-outside alert preservation, both owner sessions, sibling survival after disconnect and private CRUD. Lint, formatting, both JavaScript syntax checks and the reusable skill validator passed. A GitHub Actions template is provided at `ci/github-actions.yml`; these results come from local checks, not remote CI.

## Spatial query proof

The populated query spike uses 10,000 zones and 200 incoming positions. The old maximum-radius query took 1.638 ms with varied radii but 2,090.025 ms with one global-radius outlier. The replacement radius-bucket query took 13.6 ms and 25.123 ms respectively. It trades some ordinary-case overhead for isolation from the large-radius pathology and retains the exact geography check.

Execution plans: [current query](spatial-query.json), [earlier query](spatial-legacy.json). This is a query spike, not a claim of sustained throughput with 10,000 zones. Fixtures were rolled back after the probe.

## Actual served browser verification

[Browser results](ui/served-verification-mupp28rp.json) used the real served application with no source route overlays. Verified 30,005 stored devices, one fleet canvas, 60 rendered fleet rows, desktop 1440 px and mobile 390 px without horizontal overflow, keyboard focus preservation, selected-view accessibility, search/inspector, private three-session delivery, exact microsecond ordering, synchronized zone create/edit/pause/resume/delete, Escape-to-search behavior and real occupancy insights.

Screenshots: [desktop](ui/served-desktop-mupp28rp.png), [mobile](ui/served-mobile-mupp28rp.png). Earlier source-overlay results are retained separately and are not substituted for this check.

## Assistant verification boundary

The real OpenAI SDK and strict response schema were tested with a deterministic HTTP test provider. Private context, invalid centres/radii, concurrency limits and the absence of automatic zone mutations were verified. No live model request was made because no API key was configured. Setting `GEO_OPENAI_API_KEY` enables the actual provider integration; the UI otherwise reports it unavailable.

Notifications remain live and ephemeral, with a documented commit-to-fanout crash window. Deployment uses one application process with local connection state; adding workers requires a different shared-routing architecture.
