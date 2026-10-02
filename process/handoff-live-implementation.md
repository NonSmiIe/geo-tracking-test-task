---
title: Geo tracking live implementation checkpoint
date: 2026-10-01
kind: handoff
status: open
seq: 4
---

The standalone implementation and actual served UI are complete; verification and final packaging remain active. The current source passes 38 real-PostGIS tests and lint/format checks. Do not stop at the first smoke result or restart the running app while its sustained benchmark is active.

Plan: [plan-2026-10-01](plan-2026-10-01.md). Audit: [audit-2026-10-01](audit-2026-10-01.md). Repository: this repository.

## State

- New local Git repository initialized on master, no commits yet and no remote. All task code is in this standalone directory. Local deliverable needs a clean committed task branch after final checks.
- Two-container Docker stack on localhost8097; app2CPUs/512MiB, DB2CPUs/1GiB; native ARM64 AppleM4Pro, PostgreSQL17.6/PostGIS3.6.4.
- Core: bounded HTTP admission and16MiB aggregate retained bodies,4096 pending reports,200-report/25ms processor, repeatable-read PostGIS matching, latest-state upsert then commit/fanout. Output4000matches/1MiB budget beforecommit; socket queues2MiB/128frames. Frames now retain immutable UTF8bytes, decoded only at socket send.
- Radius-bucket multicolumnGiST replaces the failed maximum-radius query. Fullplans in evidence/spatial-query.json and spatial-legacy.json.200report/10kzone global-outlier spike25.123ms versus2090.025ms previously.
- Correct all-session routing, private zone CRUD, owner-only zones_changed notifications, handshake reservations, one writer/socket, slow-client eviction, graceful processor shutdown.
- Added fleet freshness/private occupancy insights, real optional OpenAI structured zone drafts (read-only; selectedcentre/radius validated;2calls in flight; finite timeout, no retries). No key supplied and no live model test; actual SDK tested through deterministic provider.
- Actual database-stop test found raw OSError bypass; DATABASE_ERRORS now covers SQLAlchemy/OSError/asyncpg errors. Real final fault probe confirms503 ready/ingest/CRUD duringDBdown then200 ready/ingest afterrestart withoutapprestart. evidence/database-fault.json.
- Impeccable UI agent built self-hosted assets, onecanvas fleet/cluster rendering, bounded60rows/80alerts, search/inspector/follow, private geofence CRUD, operationsbrief, assistantpreview, darktheme/mobile. Correct microsecond ordering and async socket/identity guards. Independent review foundfocusloss and wrongaria-selected; bothfixed.
- Actual served no-overlay browser check: evidence/ui/served-verification-mupp28rp.json and unique served screenshots;30,005 devices/60rows/1canvas;1440/390 nooverflow,3sessions/privatezones,exactmicrosecondordering, cross-tabzonechanges,keyboardfocus,Escape/slash,realbrief. Earlier source-overlay screenshots had interpretation conflicts; use the uniquely named actual-served evidence and DOM results.
- Reusable scripts/drive.py passed live3session test. scripts/ui_verify.js runs via mcp__playwright__browser_run_code_unsafe({filename:absolutePath}); it removesoverlays and seedsoneuniquedevice onfreshDB. Repo-local .agents/skills/drive-fleet validated and symlinked into ~/.codex/skills/drive-fleet.
- README, PRODUCT/DESIGN,Impeccable design.json,CIworkflow and dependencylock exist. README links evidence/verification.md, which must still be written with final measuredresults. RawJSONevidence is tracked; progressJSONL ignored.

## Active measurement

First900s run at128 producerconcurrency failed:1,800,000scheduled,1,799,684accepted,316gendropped,allsubmittedframesreconciled;deliveryp951103ms. Preserved in evidence/baseline-initial.json and ignored progresslog.

Second900s run at256 producerconcurrency is running as execsession24006, output evidence/baseline.json and baseline-progress.jsonl. At730s:1,460,156scheduled,all attempted accepted exceptnormalinflight,zero drops/rejections,queuehigh641;HTTPp9594ms;phasep993ms/max150ms,126reports flaggedlate>100ms. The current script incorrectly treats any100ms timerwarning as an absoluteworkloadfailure; originalplan didnot require hard-RT zerojitter. Adversarial agent is independently assessing objective cadence criteria. Preserve this run's rawresult. If criteria change, predeclare them and run a fresh900s test; do not silently rescore a failed measurement as passed.

## Next

1. Finish secondrun and preserve rawresult as baseline-second.json if timer-onlygate fails.
2. Resolve cadence methodology from adversarial review. Keep exactscheduledpopulation,acceptedpopulation,deliverychecksum/count,p95<1s,and boundedresourceproof; retain100mswarningdiagnostics. Anychangedcriterion needs documentedrationale and a freshindependentrun.
3. Run final sustainedmeasurement, plus shorthigher-rate/fanout sweeps if useful; publish measuredlimits and client-bottleneck qualifications. No applicationchanges unless actualbugsremain.
4. Final lint/format/nodechecks,driveskillvalidation,actualserverhealth and localGitcommit. Write evidence/verification.md and vaultclosingreport,updateplancheckboxes,copy durableevidence into sameunit,then handuser appURL/source/evidence.

## Traps

- Approvalpolicy nownever/fullaccess: do not pass sandbox_permissions.
- Build/run tests with --no-deps toavoidrecreatingdatabase duringload. Apprebuild closesallWS; freeze app duringbenchmark.
- Only one appworker; local routing is deliberately enforced by entrypoint.
- User explicitly authorized bothagents and extraUI/AI/features. No externalmessages orGitHubpublishing authorized.
