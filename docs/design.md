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

### Language choice — Go

Go is a strong fit for this infrastructure project because goroutines and
channels map naturally to concurrent load generation, worker coordination, and
streaming gRPC connections. Its standard library provides solid HTTP and
networking primitives, while static compilation produces portable binaries that
are simple to package into Docker and Kubernetes deployments.

Go also emphasizes readability and consistent software engineering through
tools such as `gofmt`, `go test`, `go vet`, race detection, fuzzing, and built-in
module management. This is particularly valuable in an AI-assisted workflow,
where the important skill is not only generating code quickly, but reviewing,
verifying, and maintaining it over time. The learning curve is intentional
because understanding Go concurrency and systems programming is a core goal of
the project.

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
  model: constant-rate       # or: constant-vus
  rate: 50000
  max_in_flight: 10000
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

For `constant-vus`, the caller owns the run context and its total duration,
including ramp-up. The first virtual user starts immediately, the last starts at
the end of the ramp, and each started user remains active until cancellation.
Requests aborted specifically because the run context ended are excluded from
metrics: they are neither target successes nor target failures. Results that
completed before cancellation are still recorded.

For `constant-rate`, `max_in_flight` is required. An absolute scheduler computes
each intended arrival from the run start; `ramp_up` linearly increases the rate
from zero to the configured target. A non-blocking semaphore admits requests up
to the in-flight limit. Arrivals rejected at that limit increment a separate
`unmet_demand` counter rather than becoming target failures or waiting in a
client-side queue. Workers also report exact admitted-arrival counts so the
orchestrator can distinguish generated requests, max-in-flight rejection, and
scheduler lag. Admitted request latency is measured from intended arrival
through response completion.

The open model is where **coordinated omission** must be handled. A naive
generator that waits for a slow response before sending the next request silently
under-reports latency, because the requests it *should* have sent during the
stall are never measured. The fix: schedule each request against its *intended*
send time and measure latency from that timestamp, not from actual dispatch.

### Local CLI execution

`cmd/loadtest` joins the local configuration, protocol, scheduler, and metrics
packages. Before starting the timed run, it sends one unmeasured configured
request with a five-second timeout by default. Transport, DNS, TLS, timeout, and
response-read failures abort setup, while an HTTP 4xx or 5xx still proves that
the target is reachable. The timeout is configurable with
`-preflight-timeout`.

The exact configured request is useful for validating the route and method, but
it can have side effects for operations such as POST or DELETE. Local runs
should therefore target disposable environments. A future version may support
a separate health URL, disabled preflight, and idempotency headers; silently
changing the configured method to GET or HEAD could validate the wrong route.

Normal completion prints a text summary and evaluates enabled p99 latency and
error-rate thresholds. Equality passes and a zero threshold disables that
check. Any unmet demand, dropped metric samples, or a run with no completed
requests fails because the result is incomplete or unreliable. Ctrl+C cancels
scheduling and in-flight requests, prints an `INTERRUPTED` partial summary
without threshold evaluation, and exits with the conventional status 130.

---

## 5. Metrics pipeline

### Why not stream per-request events

At 50k RPS across 20 workers, one message per request is 1M messages/second over
gRPC. The metrics pipeline would become the bottleneck and the platform would
effectively be load testing itself.

### Design

1. One collector goroutine per worker continuously drains a buffered channel and
   owns the live metrics state.
2. It records every request latency, including failed attempts, in an **HDR
   histogram**, plus counters for requests sent, succeeded, failed, transport
   errors, 4xx responses, 5xx responses, exact status codes, and bytes read.
3. Every second it snapshots and resets that state, then pushes the resulting
   **delta** upstream.
4. The orchestrator **merges** histograms and counters additively.

Distributed workers do not send preflight requests. An assignment creates one
run controller that owns the scheduler, collector, absolute reporting clock,
and revision transitions. Closed and open schedulers remain anchored to the
assignment's original `starts_at`: rebalances start already-due work immediately
and skip past open-model arrivals instead of replaying them or restarting ramp.

Active workers report at one-second boundaries measured from `starts_at` and
flush a final partial interval at the deadline. A revision change cancels and
drains the previous scheduler, flushes a partial delta tagged with that old
revision, and then applies the replacement without resetting the collector or
per-run delta sequence. An explicit final-revision marker closes the old
revision even when that flush lands exactly on a one-second boundary. Zero-load
workers remain idle and send no metric deltas.

Histogram payloads use raw HDR V2 compressed bytes. The orchestrator validates
the worker, run, known assignment revision, interval, strict sequence, counter
invariants, encoding, and histogram observation count before atomically merging
a delta. Exact duplicates are idempotent; gaps, conflicting duplicates,
malformed deltas, and missing final deltas become integrity violations rather
than producing a trustworthy-looking partial result.

At the absolute deadline, the orchestrator waits for final deltas from every
worker with nonzero load in the latest revision. It finishes as soon as all
arrive or after a two-second grace period, lists missing workers as integrity
violations, evaluates the merged snapshot over the configured duration, and
prints the same summary and exit statuses as the local CLI.

Merged histograms are load-bearing here: **percentiles cannot be averaged.** The
mean of two workers' p99 values is not the fleet p99. Merging histogram buckets
produces a mathematically correct global percentile.

### Backpressure

The worker's internal metrics channel is buffered and **drops the incoming sample
when full**, incrementing a `dropped_samples` counter that is reported upstream.
Metric reporting must never block load generation.

---

## 6. Autoscaling

Per-worker capacity is not knowable in advance — it depends on instance size,
target latency, and response size. The orchestrator therefore *measures* it.
Fleet autoscaling is valid only for `constant-rate`; configuration rejects it
for `constant-vus`.

Control loop:

1. **Provision** `min_workers`; capacity is measured rather than assumed.
   Initial provisioning failure is a setup failure. Docker running state and
   gRPC readiness remain separate, so the orchestrator waits up to a
   configurable worker startup timeout (30 seconds by default) for registration.
2. **Every 5 seconds, evaluate the exact ramp window.** Expected arrivals come
   from the absolute schedule. Active workers shard one global arrival sequence,
   so uneven rate splits still produce each fleet-wide deadline exactly once.
   The controller allows three seconds for delivery, then reads only
   deltas whose declared interval ended by the evaluation boundary; late data
   cannot leak into a later window. Scheduler coverage is
   `(admitted_arrivals + unmet_demand) / expected_arrivals`: admitted arrivals
   became requests, while unmet demand accounts for arrivals that could not.
   During the healthy-fleet windows used for scaling, unmet demand represents
   global concurrency rejection and any remaining deficit is worker scheduler
   lag. Worker-loss windows are handled separately by replacement reconciliation.
   A delayed scheduler dispatches at most one stale arrival before advancing to
   the first current deadline owned by its shard. It does not burst through every
   missed deadline: those omissions remain scheduler lag, while burst catch-up
   would distort both the configured rate and later control windows.
   Response completions are not a fleet-capacity signal because a slow target
   must not cause worker scaling. Final evaluation compares total expected
   arrivals with total admitted plus already-recorded unmet demand exactly once,
   so load the fleet never generated fails the result without mutating the
   controller's cumulative inputs.
3. **Replace before adding capacity.** Reconcile the current desired count and
   wait for replacement workers to register before evaluating a fresh capacity
   window. Missed arrivals remain unmet demand, but a temporary worker death
   does not permanently inflate the desired fleet. The provider replaces the
   exact stable worker ID even when its old container is still running but its
   gRPC session is gone. Previously registered workers are replaced immediately;
   newly created workers retain the normal startup timeout before replacement.
4. **Scale up** when scheduler coverage is below 90% for two consecutive
   intervals and the fleet is below `max_workers`. The 90% action threshold
   tolerates brief scheduling and reporting noise; any unmet demand still fails
   the final result. Compute the deficit-ratio count, cap one action at twice the
   current count, and clamp it to `max_workers`. Zero coverage uses the same
   two-times fallback.
5. **Rebalance** on membership change and start a 15-second cooldown only after
   a successful capacity increase. New workers receive the remaining absolute
   run window rather than restarting the test. Future revisions queue in
   activation order. At activation, the old revision stops scheduling without
   canceling admitted requests; those requests drain into a revision-specific
   collector while the new revision starts immediately. Metric sequence numbers
   remain ordered, but a ready newer-revision delta may precede a late old final
   delta so one stalled request cannot hide current autoscaler windows.
6. **Do not scale down during a run.** The supported load shape only ramps up
   and then remains constant; it does not fluctuate downward. Removing workers
   would add churn without following a real demand reduction. Scale directly to
   zero during teardown.

`max_in_flight` remains a global test limit divided across active workers, so
adding workers does not raise total allowed concurrency. Max-in-flight rejection
is reported as unmet demand rather than treated as a scaling trigger; raising
that user-selected ceiling is the remedy when it is too low. Horizontal scaling
instead distributes scheduling, connection handling, and response processing
across more worker processes.

The orchestrator selects `manual` or `docker` fleet ownership through operational
flags rather than the test YAML. Manual mode accepts externally started workers
and cannot be combined with autoscaling. A Docker-managed fixed run reconciles
exactly `min_workers`; an autoscaled run starts there and grows as needed. Piece
20 adds a Kubernetes driver behind the same interface.

Mid-run fleet-manager errors preserve the existing workers, become final-result
violations, and retry at the next evaluation without starting cooldown. The
controller exports desired and registered workers, expected and admitted rates,
scheduler coverage, deficient intervals, scale actions, and fleet errors. Logs
include the inputs and reason for each decision.

### Two kinds of autoscaling, deliberately both present

- **Fleet autoscaling (this platform).** Orchestrator-driven, reacting when the
  worker fleet cannot process the configured arrival schedule fast enough.
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

A custom pull collector reads one immutable view containing cloned cumulative
fleet and per-worker snapshots plus active worker IDs on each scrape. Fleet and
worker snapshots are cloned under one aggregator lock acquisition, so a scrape
cannot mix accepted-delta versions, while Prometheus export does not duplicate
mutable metric state. It converts the HDR latency distribution into a classic
Prometheus histogram with cumulative buckets at 1ms, 5ms, 10ms, 25ms,
50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, and 10s. Histogram count and the
HDR-derived sum retain the same bounded approximation as the source histogram.
Fixed-boundary counts conservatively include only HDR bars whose quantized upper
value is at or below `le`, so a boundary bar may be deferred to the next bucket
under the existing three-significant-figure approximation but is never counted
below its possible upper value. Native histograms remain a future option once
their Go client API is stable.

Fleet series cover run metadata, active workers, requests, failures by transport,
client, and server kind, exact HTTP status codes, response bytes, dropped
samples, unmet demand, and request duration. Equivalent `loadtester_worker_*`
series use `worker_id`; registered workers remain exported with their cumulative
values and an active value of zero until the orchestrator exits, including when
they disconnect before emitting a first delta.
Succeeded requests, requests per second, error rate, and latency percentiles are
derived in PromQL instead of duplicated as mutable gauges.

The orchestrator serves gRPC on `:9090` and metrics plus `/healthz` on `:9091`.
The health endpoint reports only that both process listeners are ready; target
and worker health remain separate concerns. Failure to bind either listener is a
setup failure, and both listeners stop with the run.

Grafana dashboard panels:

- Requests/second over time
- p50 / p95 / p99 latency
- Error rate
- Status-code distribution
- Active worker count (scaling events appear as steps)
- Per-worker RPS contribution
- Dropped samples
- Unmet demand

The provisioned **Load Tester Overview** dashboard opens with compact current
RPS, error rate, active worker, dropped sample, and unmet demand stats. Detailed
time-series panels show fleet RPS; p50, p95, and p99 latency; total error rate
plus transport, client, and server breakdowns; exact HTTP status codes; stacked
per-worker RPS; and active workers over time. The demo dashboard draws a fixed
100ms latency threshold matching its committed configuration. Status codes and
per-worker contribution are stacked so changes in composition remain visible
without hiding total throughput.

Prometheus and Grafana run as containers in the same Compose file. A committed
five-minute constant-rate demo starts automatically against two fixed workers,
`worker-a` and `worker-b`; dynamic worker provisioning remains a later piece.
Prometheus scrapes every second and retains data in a named volume. Grafana has
its Prometheus data source and read-only dashboard provisioned with local-only
`admin` credentials. Dashboard JSON is committed and loaded automatically, so a
clean checkout requires no manual Grafana setup. After the bounded orchestrator
run exits, workers stop load generation and continue their normal reconnect
backoff while Prometheus and Grafana remain available for inspection.

---

## 8. Failure modes

| Failure | Handling |
|---|---|
| Worker dies mid-test | Stream breaks -> orchestrator retains complete deltas, records an integrity failure, and redistributes the load immediately. Later fleet drivers may provision a replacement. |
| Worker hangs (no activity) | Three missed heartbeat intervals -> close the stream and use the same recovery path as a broken connection. |
| Target unreachable at start | Pre-flight single request; abort with a clear error rather than reporting a 100% error rate. |
| Scale-up fails (quota/capacity) | Log the failure, continue at current fleet size, record a result violation, and retry at the next evaluation. Never silently report success. |
| Generated schedule overlaps | Final admitted arrivals above the absolute schedule -> fail the result as an integrity violation. |
| Orchestrator dies | Test is lost. Accepted for Layer 1 — see §11. |
| Thresholds violated | Non-zero exit code, making the tool usable in CI. |

---

## 9. Testing strategy

**Unit.** Histogram merge correctness; percentile math; load-slice distribution;
autoscaler decisions (table-driven against synthetic saturation inputs);
controller timing and fleet failures with fake time and a fake manager; exact
admitted-arrival accounting; rate scheduler timing under simulated clock.

**Integration.** In-process orchestrator plus N in-process workers against
`cmd/target` configured with known fixed latency. Assert that measured p99
matches injected latency within tolerance. This is the correctness oracle for the
entire metrics pipeline.

The Phase 1 oracle runs the local pipeline for three seconds with 20 virtual
users against a target injecting 50ms latency and a 20% error rate. Accepted
ranges are 50–65ms for p50, 50–80ms for p95, 50–100ms for p99, 320–480
requests/second, and 15–25% measured errors. Histogram merge equivalence uses
the same real HTTP observations in three batch collectors and one combined
collector, so merged and directly aggregated percentiles must match exactly.

**End-to-end.** `docker compose up`, run a test, assert Prometheus contains the
expected series.

The Piece 19 Docker test applies an optional per-worker CPU limit so one initial
worker is reproducibly under-provisioned. It verifies scale-up,
scheduler-coverage recovery, and complete teardown without depending on the host
being naturally saturated.

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

The versioned `loadtest.v1.WorkerControl.Connect` stream uses typed `oneof`
envelopes. Workers send registration, one-second metric deltas, and heartbeats;
the orchestrator sends registration acknowledgment, complete versioned load
assignments, and stop commands. Workers supply a process-stable ID, and the
orchestrator acknowledges it with the heartbeat interval. Assignments contain
absolute start and deadline timestamps plus a monotonically increasing revision,
so rebalances replace state idempotently without restarting the test window.
The assignment target is itself a protocol `oneof`, allowing the planned gRPC
target adapter to be added without changing the assignment's field numbers.

Metric deltas identify the worker, run, assignment revision, interval, and
sequence number. Counters and status-code counts travel alongside a compact HDR
V2 encoded histogram, preserving exact mergeability without sending individual
request observations. Heartbeats report worker state, the applied assignment
revision, and in-flight requests. Protobuf field numbers are permanent within
`v1`: new fields may be added, but existing numbers are never renumbered or
reused.

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

The worker requires `ORCHESTRATOR_ADDR`; `WORKER_ID` optionally overrides the
host or container name used as its process-stable identity. A registration-only
orchestrator accepts the first stream message, rejects empty or duplicate IDs,
acknowledges the worker with a three-second heartbeat interval, and removes the
session when its stream closes. Registered workers send sequenced `IDLE`
heartbeats through one outbound writer, establishing the send serialization that
later metric deltas reuse.

Streaming RPCs are recreated by the application after connection loss. Retryable
transport, EOF, deadline, and duplicate-ID failures use jittered exponential
backoff from 500ms to 10s and reset after registration succeeds. Invalid
arguments and authentication or permission failures are permanent. Local Phase
2 transport is plaintext inside a trusted development network; external TLS and
authentication remain deployment concerns.

The orchestrator reads the validated load-test file at startup and waits for
`fleet.min_workers` before creating a run. One coordinator goroutine serializes
membership changes and assignment revisions. The initial assignment starts one
second in the future, and every rebalance preserves the run ID, original start,
and original deadline so added workers receive only the remaining test window.

Workers are ordered by ID before quotient-and-remainder load splitting, making
remainder placement deterministic without dropping demand. Constant-rate load
is divided among at most `min(worker count, rate, max_in_flight)` active workers;
surplus workers receive an explicit zero-load replacement assignment. A worker
validates and retains only complete, non-stale assignments and reports the
accepted run ID and revision in later heartbeats. Piece 12 leaves workers idle;
Piece 13 applies those assignments to load generation and metric reporting.

Joining workers trigger rebalances, but assignment delivery failures and worker
departures do not redistribute load until Piece 14 adds failure detection and
recovery. Failed sends remove the affected active session, and workers joining
after the original deadline remain connected but idle.

Piece 14 makes `min_workers` a startup gate rather than a permanent quorum. Once
the run starts, a broken stream, failed assignment send, or three intervals with
no valid worker message removes that session and triggers a replacement revision
across every healthy worker, even when the fleet has fallen below the configured
minimum. Any valid heartbeat or metric delta proves liveness; malformed traffic
does not extend the deadline. Closely spaced
departures are coalesced before the coordinator reads a fresh membership
snapshot. An empty revision keeps an all-workers-dead run open for replacements
until the original deadline.

Complete metric deltas accepted before a failure remain in the fleet aggregate,
but a nonzero-load worker's unreported partial interval is never reconstructed.
Its loss is an integrity violation, while a zero-load worker departure is only a
membership event. For open load, the orchestrator records a defensible lower
bound on unmet demand: the additional absolute-schedule arrivals that could not
be assigned between failure detection and successful replacement-assignment
dispatch. Closed load has no prescribed arrival rate, but still fails integrity
when an active worker dies.

A reconnecting process keeps its stable worker ID. Registration returns the next
expected metric sequence so the fresh collector cannot conflict with previously
accepted deltas; the worker then receives the newest assignment revision. The
failure violation remains because the old collector's partial interval was lost.
Piece 14 redistributes to available and manually started workers only. Docker and
Kubernetes replacement provisioning remains behind the Fleet Manager introduced
in Pieces 16 and 20, with automated capacity recovery added by Piece 19.

Piece 15 packages the orchestrator, worker, and target through named targets in
one root multi-stage `Dockerfile`. Shared dependency layers feed separate static
Go build stages that honor Docker's target OS and architecture. Each final image
uses the same distroless non-root runtime in local and production environments,
contains only its binary and required runtime certificates, and receives standard
OCI source, version, revision, and title labels. The worker's registration version
is injected from the same build version argument.

Images do not embed environment-specific addresses or configuration. The
orchestrator receives a read-only YAML mount and its existing `-config` flag; the
worker keeps `ORCHESTRATOR_ADDR` and `WORKER_ID`; the target keeps `-addr`.
Container-to-container traffic uses Docker DNS, while a worker dialing a host
orchestrator uses the runtime-provided `host.docker.internal` mapping. A
`.dockerignore` keeps unrelated local state out of the context, and a smoke script
verifies image metadata, non-root execution, target health, a complete
three-container run, host dialing, cleanup, and an amd64 cross-build. Compose,
registries, fleet drivers, and separate debug images are outside Piece 15.

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

### Metrics overflow — drop the incoming sample

**Rejected:** blocking producers; locking to drop the oldest sample; a custom
lock-free ring buffer.

On a full channel, a non-blocking send drops the incoming sample and increments
`dropped_samples`. This sacrifices recency to guarantee that metrics bookkeeping
cannot delay load generation or distort its timing; preserving the newest sample
is not worth adding contention or queue complexity.

### Metrics collection — one consumer goroutine

**Rejected:** updating shared metrics behind a producer-side mutex; sharding
collectors before measuring a bottleneck.

One goroutine continuously drains the result channel and owns the histogram and
counters, keeping request goroutines free of metrics locks. One-second delta
snapshots provide near-real-time reporting. Collector sharding remains a
measured follow-up if this single consumer becomes a demonstrated bottleneck.

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

Piece 16 introduces the provider-neutral `FleetManager` and `WorkerInfo` types,
then implements `DockerDriver` with the official Docker Go SDK behind a narrow,
fakeable client interface. `WorkerInfo` carries a stable worker ID, a
provider-specific ID such as a container ID or pod UID, and a normalized state.
The driver receives an existing network, worker image, orchestrator address, run
ID, and optional host-gateway mapping. It owns only worker containers bearing its
project, run, and worker labels; the caller owns the network, orchestrator, and
target.

`Scale(n)` means exactly `n` managed containers must be running. Worker IDs use
deterministic numbered slots, missing slots are created in ascending order, and
scale-down removes the highest slots first. Repeating the same desired count is
idempotent. A stopped container remains observable through `List`, does not
satisfy the desired running count, and is replaced in the same slot on the next
`Scale` call. The driver does not install a Docker restart policy or reconcile
without being called; automatic replacement belongs to Piece 19.

`Scale` waits for Docker's running state, not gRPC registration. The orchestrator
separately decides whether enough workers registered, which keeps infrastructure
state separate from application connectivity. A partially failed scale-up rolls
back only the containers created by that call. Scale-down and teardown attempt
every requested removal and join any errors. Driver operations are serialized;
`Replace` recreates one exact stable worker slot. Managed cleanup gives
scale-to-zero and teardown separate bounded contexts before closing the client.
teardown is idempotent, permanently closes scaling with `ErrClosed`, and leaves
`List` available for an empty post-cleanup observation.

The Docker driver requires its configured image to exist locally; registry pulls
and authentication remain outside Piece 16. Unit tests use the fake client for
scaling, rollback, filtering, ordering, cancellation, and terminal-state cases.
An opt-in Docker integration test independently verifies exact running-container
counts and eventual orchestrator registration, then proves repeated teardown and
post-teardown `Scale` cannot create another container.

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
