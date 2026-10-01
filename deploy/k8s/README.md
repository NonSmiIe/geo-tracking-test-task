# Kubernetes

`kubectl apply -k deploy/k8s` deploys the three stateless tiers: api, gateway and processor, plus a migration Job and the ingress. The stateful tiers come from their operators:

| Dependency | Expected source | Address in `config.yaml` |
| --- | --- | --- |
| Kafka, 3 brokers, topic RF 3 | Strimzi cluster `fleetline` | `fleetline-kafka-bootstrap.kafka:9092` |
| NATS, 3-node cluster | the NATS Helm chart | `nats://nats.nats:4222` |
| PostgreSQL 18 + PostGIS 3 | CloudNativePG; the Secret `fleetline-database` holds `GEO_DATABASE_URL` | — |
| Prometheus | kube-prometheus-stack; scrape `api`, `gateway` and `processor-metrics` | `prometheus-operated.monitoring:9090` |

What the manifests encode, and why:

- **Liveness only, no readiness probe.** It matches compose. A readiness check that depends on Kafka took every saturated replica out of rotation at 300k devices (see the main README).
- **`terminationGracePeriodSeconds: 45`.** Processors finish their in-flight batch and commit offsets before releasing partitions on SIGTERM, and the publish deadline is 30 s.
- **A processor PDB with `maxUnavailable: 1`.** Each eviction is one rebalance.
- **No HPA on processors.** Partitions (24) cap useful replicas, and lag, not CPU, is the signal. Scale them with KEDA's Kafka scaler on group `processors`, capped at the partition count.
- **An HPA on api by CPU.** Ingest cost is CPU per report.
- **The ingress routes `/ws` to gateways** with hour-long timeouts, and refuses `/metrics`, the same as the HAProxy edge.

Validate without a cluster:

```bash
docker run --rm -v "$PWD/deploy/k8s:/k:ro" registry.k8s.io/kubectl:v1.34.1 kustomize /k > /tmp/fleetline.yaml
docker run --rm -i ghcr.io/yannh/kubeconform:v0.7.0 -strict -summary < /tmp/fleetline.yaml
```
