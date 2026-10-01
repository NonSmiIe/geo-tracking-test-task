# Fault-run acceptance policy v2

Declared on October 2, 2026, before rerunning S1 (NATS restart). It replaces [fault-v1](acceptance-policy-fault-v1.md) for runs from that commit on; fault-v1 results stay as recorded.

One change. A gateway whose NATS connection was lost now sends every dashboard `{"type":"resync"}` after reconnecting, because core NATS drops whatever was published while the gateway was resubscribing. A session that receives `resync` is judged like one that was closed: it must receive events after its last resync. A session that saw neither a closure nor a resync must still have missed nothing. Under fault-v1, S1 found exactly that gap: open sessions silently missed events.
