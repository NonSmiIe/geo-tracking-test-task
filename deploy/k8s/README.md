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
- **Processors scale on Kafka lag with KEDA**, not on CPU. The `ScaledObject` targets 20,000 records of lag per replica, about 3 s of one processor's share of 300k devices (an estimate: 7.5k reports/s per processor is the offered load, not a measured capacity), with at least 2 replicas. `allowIdleConsumers: false` caps replicas at the topic's partition count, so `maxReplicaCount` never has to repeat it. Every scale event is a rebalance, so scale-up can double every 30 s but scale-down waits 5 minutes and removes at most 2 pods a minute. The scaler reads bootstrap, group and topic from the same ConfigMap keys the processors use. KEDA itself must be installed in the cluster.
- **An HPA on api by CPU.** Ingest cost is CPU per report.
- **The ingress routes `/ws` to gateways** with hour-long timeouts, and refuses `/metrics`, the same as the HAProxy edge.

Validate without a cluster:

```bash
docker run --rm -v "$PWD/deploy/k8s:/k:ro" registry.k8s.io/kubectl:v1.34.1 kustomize /k > /tmp/fleetline.yaml
docker run --rm -i ghcr.io/yannh/kubeconform:v0.7.0 -strict -summary -schema-location default \
  -schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json' < /tmp/fleetline.yaml
```
