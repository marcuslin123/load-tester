# Distributed Load Testing Platform — Design

**Status:** Approved
**Author:** Marcus Lin

---

## 1. Purpose

A platform that generates HTTP load against a target service from a fleet of
containerized workers, coordinated by a central orchestrator, with real-time
metrics and automatic fleet scaling.

The engineering goals are to build and operate a real distributed system:
container orchestration, service-to-service streaming, closed-loop autoscaling,
and production-grade observability.

### Scope tiers

**Layer 1 — the deliverable (~4 weeks).** A user writes a YAML test file and runs
one command. The platform provisions a fleet of containerized workers, generates
HTTP load against a target, streams metrics back in real time, autoscales the
fleet during the run, and surfaces everything in Grafana.

**Near-term (Week 5).** gRPC target-protocol adapter. Separated from the long-tail
stretch list because it reuses the protobuf and streaming work already done in
Layer 1, making it unusually cheap to add.

**Stretch, in priority order.** Terraform for EKS provisioning · CI/CD via GitHub
Actions · per-worker CPU/memory reporting · WebSocket adapter · web dashboard ·
scripted multi-step scenarios · historical result storage · orchestrator HA.

**Explicitly out of scope for Layer 1.** Web UI, test scheduling, historical
result storage, scripted multi-step scenarios, WebSocket adapter, orchestrator
high availability.

---

## 2. Architecture

```
   test.yaml
      │
      ▼
┌──────────────────────────────────────┐        ┌──────────────┐
│           ORCHESTRATOR               │        │ Fleet Mgr    │
│                                      │◄──────►│ (interface)  │
│  ┌────────────┐   ┌───────────────┐  │        ├──────────────┤
│  │ Test       │   │ Autoscaler    │  │        │ DockerDriver │
│  │ Controller │◄─►│ (control loop)│  │        │ K8sDriver    │
│  └────────────┘   └───────────────┘  │        └──────────────┘
│  ┌────────────────────────────────┐  │
│  │ Aggregator (merged histograms) │  │
│  └────────────────────────────────┘  │
│           /metrics  (Prometheus)     │
└───────▲──────────────────────────────┘
        │  gRPC bidirectional stream
        │  ▲ up: metric batches + heartbeat + health
        │  ▼ down: assign / rebalance / stop
   ┌────┴────┬─────────┬─────────┐
   │Worker 1 │Worker 2 │Worker N │   ← containers (Compose / K8s pods)
   └────┬────┴────┬────┴────┬────┘
        └─────────┼─────────┘
                  ▼
          ┌───────────────┐
          │ TARGET APP    │  ← own Go echo server (latency/error injection)
          └───────────────┘

Prometheus scrapes orchestrator ──► Grafana dashboards
```

---

## 3. Components

### Orchestrator (`cmd/orchestrator`)

A single Go binary. Responsibilities:

- Parse and validate the test file.
- Ask the Fleet Manager for the initial worker count.
- Assign load slices to workers over gRPC.
- Merge incoming metric deltas into fleet-wide histograms and counters.
- Run the autoscaling control loop.
- Expose `/metrics` for Prometheus.
- Print a final summary and exit non-zero if thresholds were violated.

### Worker (`cmd/worker`)

A single Go binary running in a container. On startup it **dials out** to the
orchestrator (address supplied via environment variable), registers, and opens
one long-lived bidirectional gRPC stream. It receives an assignment, spawns
goroutines to generate load, aggregates results locally, and ships metric deltas
every second.

### Fleet Manager

A Go interface with two implementations, so the orchestrator never knows which
environment it is running in:

```go
type FleetManager interface {
    Scale(ctx context.Context, desired int) error
    List(ctx context.Context) ([]WorkerInfo, error)
    Teardown(ctx context.Context) error
}
```

- `DockerDriver` — uses the Docker Engine API to create and remove containers.
- `K8sDriver` — patches a Deployment's replica count via `client-go`.

### Protocol Adapter

The abstraction that keeps additional target protocols cheap:

```go
type Protocol interface {
    Execute(ctx context.Context) Result
}
// Result{Latency, StatusCode, Err, BytesRead}
```

- `http.go` → `HTTPProtocol` (Layer 1)
- `grpc.go` → `GRPCProtocol` (Week 5) — unary calls, measure round-trip
- `ws.go` → `WSProtocol` (later; requires new metric semantics, see §11)

Adding a protocol means one new file and zero changes to orchestrator or worker
plumbing.

### Target App (`cmd/target`)

A small Go server with configurable artificial latency, error rate, and a
CPU-burn endpoint. **This is not optional.** It serves two essential purposes:

1. **Correctness oracle.** Responses with known fixed latency let you verify that
   measured percentiles match injected latency before you trust any number the
   platform reports.
2. **Autoscaling demonstration.** Deployed on AWS with a Horizontal Pod
   Autoscaler, it lets you observe the *target's* autoscaling react to your
   generated load — distinct from your orchestrator scaling its own fleet.

---

## 4. Test definition format

```yaml
name: checkout-api-stress
target:
  protocol: http             # http | grpc (only http valid in Layer 1)
  url: http://target:8080/api/checkout
  method: POST
  headers:
    Content-Type: application/json
  body: '{"item_id": 42}'
load:
  model: constant-vus        # or: constant-rate
  virtual_users: 50000
  duration: 5m
  ramp_up: 30s
fleet:
  min_workers: 2
  max_workers: 30
  autoscale: true
thresholds:                  # non-zero exit code if violated
  p99_latency: 500ms
  error_rate: 0.01
```

The `protocol` field is present from Layer 1 even though only `http` is accepted,
so adding gRPC in Week 5 is not a breaking schema change. A `grpc` target
substitutes `address`, `service`, `method`, and `message` for `url`/`body`.

### Load models

- **`constant-vus` (closed model).** N virtual users each loop
  request → response → request. Throughput is an *output*: it drops when the
  target slows down.
- **`constant-rate` (open model).** Fire at exactly R requests/second regardless
  of whether prior responses have returned. Throughput is an *input*.

The open model is where **coordinated omission** must be handled. A naive
generator that waits for a slow response before sending the next request silently
under-reports latency, because the requests it *should* have sent during the
stall are never measured. The fix: schedule each request against its *intended*
send time and measure latency from that timestamp, not from actual dispatch.

---

## 5. Metrics pipeline

### Why not stream per-request events

At 50k RPS across 20 workers, one message per request is 1M messages/second over
gRPC. The metrics pipeline would become the bottleneck and the platform would
effectively be load testing itself.

### Design

1. Each worker maintains an **HDR histogram** of latencies in memory, plus
   counters: requests sent, succeeded, failed, status-code buckets, bytes read.
2. Every second it serializes a **delta** and pushes one small message upstream.
3. The orchestrator **merges** histograms additively.

Merged histograms are load-bearing here: **percentiles cannot be averaged.** The
mean of two workers' p99 values is not the fleet p99. Merging histogram buckets
produces a mathematically correct global percentile.

### Backpressure

The worker's internal metrics channel is buffered and **drops oldest on full**,
incrementing a `dropped_samples` counter that is reported upstream. Metric
reporting must never block load generation.

---

## 6. Autoscaling

Per-worker capacity is not knowable in advance — it depends on instance size,
target latency, and response size. The orchestrator therefore *measures* it.

Control loop:

1. **Provision** `ceil(target_load / assumed_per_worker_capacity)` workers, with a
   floor of `min_workers`.
2. **Every 5 seconds, evaluate saturation per worker:** is its *achieved* rate
   meeting its *assigned* rate? This ratio is the only saturation signal in
   Layer 1. Per-worker CPU and memory are a stretch goal (§1) and are
   deliberately not required here — achieved-versus-assigned rate is sufficient
   to detect a saturated worker, and it needs no host metrics plumbing.
3. **Scale up** if fleet-wide achieved throughput is below 90% of target for two
   consecutive intervals and `workers < max_workers`. Scale by the deficit ratio,
   not one worker at a time.
4. **Rebalance** on any membership change: recompute per-worker slices and push
   new assignments down the existing streams.
5. **Cooldown** of 15 seconds between scaling actions to prevent thrash — new
   containers need time to warm up.
6. **Scale to zero** on test completion.

### Two kinds of autoscaling, deliberately both present

- **Fleet autoscaling (this platform).** Orchestrator-driven, reacting to a load
  deficit — the difference between assigned and achieved request rate.
- **Target autoscaling (observed).** The target app runs under a Kubernetes HPA.
  Generating sustained load causes the HPA to add pods, and the platform's
  metrics capture the target's latency recovering as it scales. This demonstrates
  the use case a real engineer would buy this tool for: validating that their
  autoscaling policy actually works.

### Documented ceiling

Roughly 28k practical concurrent connections per worker, bounded by ephemeral
port exhaustion (~64k ports minus `TIME_WAIT` churn). Reaching this limit and
resolving it by scaling horizontally is expected, and worth documenting in the
README with the measured numbers.

---

## 7. Observability

The orchestrator exposes `/metrics` containing fleet-wide series **plus
per-worker series labeled with `worker_id`**. Prometheus scrapes only the
orchestrator; it never discovers individual workers. This avoids needing service
discovery configuration in both Compose and Kubernetes, while preserving
per-worker visibility.

Grafana dashboard panels:

- Requests/second over time
- p50 / p95 / p99 latency
- Error rate
- Status-code distribution
- Active worker count (scaling events appear as steps)
- Per-worker RPS contribution
- Dropped samples

Prometheus and Grafana run as containers in the same Compose file, so
`docker compose up` brings up the entire stack including dashboards.

---

## 8. Failure modes

| Failure | Handling |
|---|---|
| Worker dies mid-test | Stream breaks → orchestrator marks it dead, redistributes its slice, asks Fleet Manager for a replacement. Last-reported metrics are retained. |
| Worker hangs (no heartbeat) | 10-second heartbeat timeout → treated as dead. |
| Target unreachable at start | Pre-flight single request; abort with a clear error rather than reporting a 100% error rate. |
| Scale-up fails (quota/capacity) | Log the failure, continue at current fleet size, and record that target load was unmet. Never silently report as if it succeeded. |
| Orchestrator dies | Test is lost. Accepted for Layer 1 — see §11. |
| Thresholds violated | Non-zero exit code, making the tool usable in CI. |

---

## 9. Testing strategy

**Unit.** Histogram merge correctness; percentile math; load-slice distribution;
autoscaler decisions (table-driven against synthetic saturation inputs); rate
scheduler timing under simulated clock.

**Integration.** In-process orchestrator plus N in-process workers against
`cmd/target` configured with known fixed latency. Assert that measured p99
matches injected latency within tolerance. This is the correctness oracle for the
entire metrics pipeline.

**End-to-end.** `docker compose up`, run a test, assert Prometheus contains the
expected series.

**Fault injection.** `docker kill` a worker mid-test; assert the run completes and
the fleet self-heals.

---

## 10. Repository layout

```
cmd/
  orchestrator/
  worker/
  target/
internal/
  config/       # YAML parsing + validation
  fleet/        # FleetManager interface, docker/, k8s/
  protocol/     # Protocol interface, http.go
  metrics/      # histograms, merge, prometheus exporter
  autoscale/    # control loop
  loadgen/      # VU scheduler, rate scheduler
proto/          # loadtest.proto
deploy/
  compose/      # docker-compose.yml, prometheus.yml, grafana dashboards
  k8s/          # deployments, services, RBAC
  terraform/    # stretch: EKS provisioning
docs/
  design.md     # this document
```

---

## 11. Engineering decisions and rejected alternatives

### Coordination model — centralized orchestrator

**Rejected:** Raft-based leader election among peer workers; NATS or Redis message
bus.

Raft would consume the entire project timeline on its own. A message bus adds an
operational dependency and injects queue-hop latency into real-time metric
streaming. A single orchestrator is a deliberate single point of failure, and the
failure cost is low: a load test is a short-lived, re-runnable job, so losing one
costs five minutes rather than data integrity.

### Internal transport — gRPC bidirectional streaming

**Rejected:** HTTP polling; REST with webhooks in both directions; raw WebSocket;
message queue.

The orchestrator needs four things from each worker connection: continuous metric
push, arbitrary-time command push (assign, rebalance, stop), immediate death
detection, and a typed contract. One gRPC bidirectional stream provides all four
in a single mechanism — a broken TCP stream *is* the liveness signal, so no
separate health-check endpoint is needed, and protobuf generates both client and
server code from one schema.

HTTP polling would require the orchestrator to know every worker's address
(service discovery in both environments), deliver stale metrics, delay commands
until the next poll, and need a separate liveness check. Bidirectional REST would
require both sides to be reachable servers, which is painful for pods behind
ephemeral IPs. Raw WebSocket is a reasonable fit but would mean hand-rolling
framing, serialization, and schema versioning that protobuf provides for free.

### Connection direction — workers dial the orchestrator

**Rejected:** orchestrator dials workers.

This eliminates service discovery entirely. Workers need one environment
variable, and the same code works identically under Compose and Kubernetes with
no DNS or headless-service configuration.

### Target protocol — HTTP in Layer 1, gRPC in Week 5

**Rejected:** building HTTP, gRPC, and WebSocket simultaneously.

Note this is a *separate* concern from the internal transport above: here gRPC is
a protocol being load tested, not the platform's own plumbing.

gRPC is promoted ahead of the general stretch list because by Week 2 the project
already has a `.proto`, generated Go code, and a working streaming client — so
`GRPCProtocol` reuses existing knowledge at low marginal cost, and the
orchestrator itself is a convenient gRPC server to test against.

WebSocket stays a later stretch because it is not request/response. "Latency" and
"requests per second" need to be redefined (messages sent? round-trip echo
time?), and the metrics pipeline, histograms, and both load models currently
assume a request/response cycle. That is genuine design work, not an additive
adapter.

### Metric granularity — pre-aggregated 1-second histogram deltas

**Rejected:** streaming per-request events.

Per-request streaming would be roughly 1M messages/second at target scale, making
the tool bottleneck on itself.

### Percentile computation — merged HDR histograms

**Rejected:** averaging per-worker percentiles; sampling raw latencies.

Averaging percentiles is mathematically invalid. Histograms merge exactly.

### Fleet control — `FleetManager` interface with two drivers

**Rejected:** Docker-only, or Kubernetes-only.

Compose provides a fast local development loop; Kubernetes provides the
deployment experience the project exists to demonstrate. The interface means the
orchestrator is byte-identical across both.

### Kubernetes environments — kind locally, EKS on AWS

These are not competing choices. EKS *is* Kubernetes with an AWS-managed control
plane: same API, same manifests, same `kubectl`. The `K8sDriver` is written once
and runs against both; moving to EKS means pointing `kubeconfig` at a different
cluster. kind is the daily development loop; EKS is the Week-4 cloud validation
run, torn down afterward.

### Scaling mechanism — orchestrator-driven replica count

**Rejected:** relying on the Kubernetes HPA to scale the worker fleet.

An HPA cannot know the *test's* target load; it only observes CPU after the fact.
The orchestrator scales on the load deficit, which is the actual signal. The HPA
is still demonstrated — on the target app, per §6.

### HTTP client — `net/http` with a tuned `Transport`

**Rejected:** `fasthttp`.

The standard library is sufficient for the first pass, and premature optimization
would obscure where the real bottlenecks are. `fasthttp` is a documented
follow-up if a measured ceiling justifies it.

### Prometheus scrape topology — scrape the orchestrator only

**Rejected:** scraping each worker directly.

Ephemeral workers would require service discovery configuration per environment.
Re-exporting worker metrics with `worker_id` labels gives equivalent visibility
with no additional configuration.

### Orchestrator high availability — not built

HA would require leader election (Raft, or a Kubernetes lease), replication of
in-flight histogram state to standbys, and worker re-dial-and-resume logic. That
is a multi-week project whose cost is not justified by the failure cost of a
re-runnable job. The documented path if it were needed: leader election via a
Kubernetes lease plus periodic checkpointing of merged histograms.

---

## 12. Four-week plan

| Week | Deliverable |
|---|---|
| 1 | Repo scaffold, `cmd/target`, config parsing, single-process HTTP load generator with correct histograms. Verifiable: measured latency matches injected latency. |
| 2 | `loadtest.proto`, orchestrator and worker with bidirectional streaming, metric aggregation across two local workers. |
| 3 | Dockerfiles, `DockerDriver`, Compose stack with Prometheus and Grafana, dashboards, multi-worker run. |
| 4 | Autoscaler control loop, `K8sDriver` on kind, fault-injection tests, one EKS deployment to capture real numbers, README with architecture diagram. |
| 5 | `GRPCProtocol` target adapter (near-term goal, not part of Layer 1 "done"). |

Week 1 is deliberately single-process so that the Go learning curve is fought
without distributed-systems complexity layered on top.

---

## 13. Risks and mitigations

**Go learning curve.** Mitigated by completing the prerequisite work before
Week 1 (Go fundamentals, goroutines/channels/context, a small gRPC streaming
exercise), and by keeping Week 1 single-process.

**Local resource ceiling.** A laptop caps the fleet at roughly 5–8 workers. Real
numbers come from the Week-4 EKS run, not local testing.

**Measuring the wrong thing.** Mitigated by validating against the known-latency
target app before trusting any reported number, and by explicitly handling
coordinated omission in the open load model.

**AWS cost overrun.** The EKS control plane bills **$0.10/hour even with zero
worker nodes** (~$73/month if left running). Mitigation: a teardown checklist
executed at the end of every cloud session —

1. `eksctl delete cluster --name <name>` (or `terraform destroy`)
2. Confirm no EC2 instances, load balancers, or NAT gateways remain
3. Confirm EBS volumes and Elastic IPs are released
4. Check the AWS Billing console the following day

**Scope creep into later layers.** The stretch list exists to park ideas, not to
build them. Layer 1 is done when the Week-4 deliverables are complete.

---

## 14. Resume framing

Placeholders `X`, `Y`, and the throughput figures must be replaced with numbers
actually measured during the Week-4 EKS run.

```
Distributed Load Testing Platform | Go, gRPC, Docker, Kubernetes, AWS, Prometheus, Grafana

- Orchestrated X containerized workers in Go via gRPC streaming to generate Y
  concurrent requests against target services, validating autoscaling policies
  under sustained load
- Designed auto-scaling fleet management abstracted over Docker and EKS,
  dynamically provisioning workers based on real-time load demand and worker
  health status
- Instrumented end-to-end observability with Prometheus metrics and Grafana
  dashboards, capturing latency percentiles (p50/p95/p99), throughput, and error
  rates across distributed workers
```
