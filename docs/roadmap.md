# Roadmap

The build order for the distributed load testing platform, broken into pieces
small enough to finish and verify one at a time. The [design spec](design.md)
explains *what* the system is and *why* each decision was made; this document is
the order to build it in.

**Status:** plan of record. Progress lives in `git log`, not in this file.

## How to read this

Each piece has:

- **Goal** — the one thing that exists when the piece is finished.
- **Why here** — what forces this piece to come before the next one.
- **Done when** — the observable check that proves it works. If this cannot be
  demonstrated, the piece is not finished, regardless of how much code exists.
- **Teaches** — the concepts the piece exercises, so the learning is tied to
  something concrete rather than studied in the abstract.

Two rules that make the order meaningful:

1. **No piece is started before its predecessor's "Done when" passes.** The
   dependencies are real, not stylistic — Piece 9 cannot detect a broken metrics
   pipeline if Piece 2 does not yet provide a known-correct answer to compare
   against.
2. **Each phase ends in a gate.** A gate is a piece whose verification covers
   the whole phase. Failing a gate means fixing the phase, not starting the next
   one.

Anything in the stretch list of the spec (§1) is out of scope here. This roadmap
covers Layer 1 plus the gRPC target adapter.

---

## Phase 1 — A single process that measures correctly

Deliberately not distributed. One binary, no containers, no gRPC. The point of
this phase is to earn the right to trust a latency number, because every later
phase multiplies whatever this phase produces — including its errors.

Corresponds to spec §12, Week 1.

### Piece 1 — Toolchain and repo scaffold

**Goal:** An empty but buildable Go module, committed, with `go build ./...` and
`go test ./...` both succeeding on nothing.

**Why here:** Everything else needs a module path to import from. Settling
`github.com/marcuslin123/load-tester` now means no import rewrites later.

**Done when:** `go build ./... && go vet ./... && go test ./...` all exit zero,
and the commit is pushed.

**Teaches:** `go mod`, Go's package and directory conventions.

Go is not installed yet — `brew install go` is part of this piece.

### Piece 2 — Target app, the correctness oracle

**Goal:** `cmd/target` — an HTTP server with configurable injected latency,
configurable error rate, and a CPU-burn endpoint.

**Why here:** This comes *before* the load generator, which is the
counterintuitive part. A load generator tested against a real API can only be
checked for plausibility; tested against a server that always takes exactly 50ms,
it can be checked for correctness. Building the ruler before the thing being
measured is the whole reason this piece is first.

It also earns its place twice: in Phase 4 it runs under a Kubernetes HPA so the
*target's* autoscaling can be observed reacting to generated load (spec §6).

**Done when:** `curl` against `/echo?latency=250ms` takes ~250ms; a request with
`error_rate=1.0` returns 503 every time; `/burn?ms=50` measurably consumes CPU.

**Teaches:** `net/http` handlers, flags, query parsing, `time.Duration`.

### Piece 3 — Test file parsing and validation

**Goal:** `internal/config` — parse the YAML format in spec §4 into structs, apply
defaults, and reject invalid files with a message naming the offending field.

**Why here:** Every later piece is configured by this struct. Defining it now
means the schema stops moving.

**Done when:** The example file from spec §4 round-trips into a struct with every
field correct, and a table of malformed files each produce an error naming the bad
field. A bare `duration: 30` is rejected rather than silently read as 30
nanoseconds.

**Teaches:** Struct tags, custom unmarshalling, table-driven tests.

The `fleet:` block is parsed and validated here even though nothing consumes it
until Phase 4, so the file format never has to break.

### Piece 4 — HTTP protocol adapter

**Goal:** `internal/protocol` — the `Protocol` interface from spec §3, and one
implementation that fires an HTTP request and returns
`Result{Latency, StatusCode, BytesRead, Err}`.

**Why here:** Both schedulers and the metrics layer consume this shape, so its
signature has to settle before either is written. Defining the interface now, with
only one implementation behind it, is what makes the gRPC adapter in Phase 5 a
single new file.

**Done when:** A request against the Piece 2 target reports the injected latency
and the right status code; 20 sequential requests open at most a couple of TCP
connections, proving the response body is drained and connections are reused.

**Teaches:** Go interfaces, `http.Transport` tuning, errors as return values,
`context` for cancellation.

The connection-reuse check matters more than it looks. A generator that discards
connections measures its own TCP handshakes and burns through ephemeral ports —
the ceiling documented in spec §6.

### Piece 5 — Metrics collection and merging

**Goal:** `internal/metrics` — an HDR histogram plus counters, fed by a
non-blocking channel, producing an immutable snapshot that can be merged with
other snapshots.

**Why here:** The schedulers need somewhere to put results. More importantly the
merge operation is the load-bearing assumption of the entire distributed design
(spec §5) — if merging is wrong, Phase 2 produces confidently wrong fleet
percentiles.

**Done when:** Merging a histogram of 989 samples at 10ms with one of 11 samples
at 100ms yields a p99 of ~100ms — *not* the 55ms that averaging the two p99s
would give. Offering results to a full buffer never blocks and increments a
dropped counter instead.

**Teaches:** Channels, `sync.Mutex`, `sync/atomic`, why percentiles are not
averageable.

Non-blocking is a correctness property, not a performance one: a generator that
stalls on its own bookkeeping reports latencies it caused itself.

### Piece 6 — Closed-model scheduler (`constant-vus`)

**Goal:** `internal/loadgen` — N virtual users, each looping
request → response → request, with ramp-up.

**Why here:** It is the simpler of the two models and shares the sink and
cancellation plumbing the open model needs. Throughput here is an *output*.

**Done when:** 8 virtual users produce 8 concurrent in-flight requests and never
more; the run stops promptly on context cancellation with every goroutine
accounted for; a long ramp-up demonstrably keeps most users from starting.

**Teaches:** Goroutines, `sync.WaitGroup`, `context` cancellation, timers.

### Piece 7 — Open-model scheduler (`constant-rate`)

**Goal:** A scheduler that fires at R requests/second regardless of whether prior
responses have returned, with a required bound on in-flight requests.

**Why here:** It depends on Piece 6's plumbing, and it is where the subtlest bug
in load testing lives.

**Done when:** Against a target far too slow to serve the requested rate,
reported latency includes delay from the intended arrival time, and arrivals
rejected at the in-flight bound surface as separate unmet demand rather than
disappearing or being blamed on the target.

**Teaches:** Absolute versus accumulated scheduling, semaphores via buffered
channels, coordinated omission.

The trap: measuring latency from actual dispatch instead of *intended* send time
makes a stalled target look fast, because the requests that should have been sent
during the stall are never recorded (spec §4). Latency must be measured from the
intended send time.

### Piece 8 — CLI, summary, and thresholds

**Goal:** `cmd/loadtest` — read a config file, run it, print a summary, and exit
non-zero when a threshold is violated.

**Why here:** It joins Pieces 3–7 into something runnable by hand, which makes
the next piece's verification possible.

**Done when:** A run against a fast target prints `PASS` and exits 0; the same
config against a slow target prints the violated threshold and exits 1; an
unreachable target fails fast with a clear message instead of reporting a 100%
error rate (spec §8).

**Teaches:** `flag`, exit codes, signal handling, wiring packages together.

Distinct exit codes are what make the tool usable in CI, and CI usability is a
concrete thing to point at in an interview.

### Piece 9 — Oracle integration test **(Phase 1 gate)**

**Goal:** An automated test asserting that measured percentiles match the
target's injected latency.

**Why here:** This is the gate. Everything after this phase assumes the numbers
mean something.

**Done when:** Against a 50ms target, p50/p95/p99 all land near 50ms and none
below it; closed-model throughput matches Little's Law (virtual users ÷ latency);
measured error rate matches injected error rate; and three separate runs merged
together produce the same distribution as one combined run.

**Teaches:** Integration testing, `httptest`, choosing tolerances for
timing-dependent assertions.

That last assertion is the one Phase 2 rests on: merging per-worker snapshots
must equal measuring the whole thing at once.

---

## Phase 2 — Two processes talking

Adds distribution and nothing else. Still no containers, still no autoscaling —
both processes run locally as plain binaries, so a failure here is a
gRPC-or-aggregation failure and cannot be anything else.

Corresponds to spec §12, Week 2.

### Piece 10 — Protocol definition and generated code

**Goal:** `proto/loadtest.proto` plus generated Go — registration, assignments,
metric deltas, and heartbeats over one bidirectional stream.

**Why here:** The generated types are the vocabulary the next three pieces speak.

**Done when:** `protoc` regenerates cleanly from a committed, scripted command,
and the generated package compiles.

**Teaches:** Protobuf schema design, code generation, semantic versioning of a
wire format.

Requires installing `protoc` and the Go plugins. Committing the generation
command matters — regenerating by half-remembered incantation is how wire formats
drift.

### Piece 11 — Worker dials out and registers

**Goal:** `cmd/worker` — on startup, dial the orchestrator address from an
environment variable, register, and hold one long-lived stream open.

**Why here:** Direction of connection is a deliberate choice (spec §11): workers
dial *in*, so neither Docker nor Kubernetes needs service discovery, and the
orchestrator needs no inbound route to each worker. Establishing it here means
Phases 3 and 4 inherit it for free.

**Done when:** A worker started against a running orchestrator appears in the
orchestrator's log as registered, and a worker started against nothing retries
with backoff instead of crashing.

**Teaches:** gRPC clients, streams, connection lifecycle, retry with backoff.

### Piece 12 — Orchestrator assigns load slices

**Goal:** `cmd/orchestrator` — accept worker streams, divide the configured load
across registered workers, and push assignments down the stream.

**Why here:** With workers able to connect, they now need to be told what to do.

**Done when:** A test for 100 virtual users across two workers assigns 50 each;
an uneven split distributes the remainder rather than dropping it; assignments
recompute and re-push when a third worker joins mid-run.

**Teaches:** gRPC servers, concurrent map access, integer division edge cases.

### Piece 13 — Metric deltas flow upstream and merge

**Goal:** Workers ship one aggregated delta per second; the orchestrator merges
them into fleet-wide histograms and prints the same summary format as Piece 8.

**Why here:** It closes the loop. Piece 5's merge is finally used for its actual
purpose.

**Done when:** A two-worker run against the Piece 2 target reports a fleet p99
matching the injected latency, and total requests equals the sum of both workers'
counts.

**Teaches:** Histogram serialization, delta versus cumulative reporting, stream
multiplexing.

One second, pre-aggregated, is a deliberate ceiling (spec §5): at 50k RPS across
20 workers, per-request messages would be a million messages a second and the
platform would be load testing itself.

### Piece 14 — Worker death and recovery **(Phase 2 gate)**

**Goal:** Detect a dead worker, redistribute its slice, and finish the run.

**Why here:** The gate for the phase, and the piece that makes the distributed
design honest — a fleet that only works when nothing fails is not a fleet.

**Done when:** Killing one of two workers mid-run causes the orchestrator to mark
it dead within the heartbeat timeout, reassign its load to the survivor, retain
the dead worker's already-reported metrics, and complete the run.

**Teaches:** Stream error handling, heartbeat timeouts, why a broken stream is
itself a liveness signal.

This is where the choice of one bidirectional stream pays off: metric push,
command push, and liveness all ride the same connection, so a broken stream *is*
the death notification and no separate health check is needed (spec §11).

---

## Phase 3 — Containers and observability

The fleet becomes real containers, and the metrics become visible. No autoscaling
yet — worker count is fixed and set by hand, so container problems stay separable
from control-loop problems.

Corresponds to spec §12, Week 3.

### Piece 15 — Container images

**Goal:** Dockerfiles for orchestrator, worker, and target.

**Why here:** The Docker driver cannot start containers that do not exist.

**Done when:** All three images build; a container run by hand behaves exactly as
the local binary did, including a worker in a container dialing the orchestrator
on the host.

**Teaches:** Multi-stage builds, static linking, image size, container
networking.

### Piece 16 — Docker fleet driver

**Goal:** `internal/fleet` — the `FleetManager` interface from spec §3 and a
`DockerDriver` that creates and removes worker containers via the Docker Engine
API.

**Why here:** The interface must exist before there are two implementations of
it. Writing `DockerDriver` first and `K8sDriver` in Phase 4 against the same
interface is what lets the orchestrator stay ignorant of its environment.

**Done when:** `Scale(3)` produces three running worker containers that register
themselves; `Scale(1)` removes two; `Teardown` leaves nothing behind. Verified
with `docker ps`.

**Teaches:** The Docker Engine API, interface-based design, resource cleanup.

`Teardown` leaving nothing behind is worth being strict about — orphaned
containers on a laptop are an annoyance, but the same bug against AWS is a bill.

### Piece 17 — Prometheus export and the Compose stack

**Goal:** An orchestrator `/metrics` endpoint carrying fleet-wide series plus
per-worker series labeled `worker_id`, and a Compose file bringing up the whole
stack.

**Why here:** Dashboards need a scrapeable source.

**Done when:** `docker compose up` starts orchestrator, target, Prometheus, and
Grafana; Prometheus shows the orchestrator as an up target; the expected series
are queryable with per-worker labels present.

**Teaches:** The Prometheus client library, metric types, labels and cardinality,
Compose networking.

Prometheus scrapes only the orchestrator, never individual workers (spec §7).
Workers are short-lived and unaddressable, so re-exporting their series through
the orchestrator keeps per-worker visibility without any service discovery
configuration.

### Piece 18 — Grafana dashboards **(Phase 3 gate)**

**Goal:** A provisioned dashboard with the panels from spec §7, committed as JSON
so it appears automatically.

**Why here:** The gate: it proves the whole pipeline end to end, from a request
leaving a worker to a pixel on a graph.

**Done when:** `docker compose up` on a clean checkout yields a working dashboard
with no manual clicking, and a running test visibly moves every panel.

**Teaches:** Grafana provisioning, PromQL, dashboards as code.

Provisioning matters for a resume project specifically: a reviewer who has to
hand-build a dashboard will never see it. This is also where the screenshot for
the README comes from.

---

## Phase 4 — Autoscaling and Kubernetes

The fleet sizes itself, the same orchestrator runs unmodified against Kubernetes,
and real numbers get captured.

Corresponds to spec §12, Week 4.

### Piece 19 — Autoscaler control loop

**Goal:** `internal/autoscale` — the loop from spec §6: measure achieved versus
assigned rate, scale by the deficit ratio, respect a cooldown, rebalance on
membership change.

**Why here:** It needs a working fleet driver (Piece 16) and trustworthy
throughput measurements (Piece 13) to act on.

**Done when:** Table-driven tests over synthetic saturation inputs produce the
expected decisions, including no-scale inside the cooldown window and no scale
past `max_workers`. Live against Docker, an under-provisioned run scales up and
converges on the target rate.

**Teaches:** Control loops, hysteresis and thrash avoidance, testing
time-dependent logic without sleeping.

Per-worker capacity is deliberately *measured* rather than assumed — it depends
on instance size, target latency, and response size, so no constant would be
right. Scaling by the deficit ratio rather than one worker at a time is what keeps
convergence from taking minutes.

### Piece 20 — Kubernetes fleet driver

**Goal:** A `K8sDriver` that scales a Deployment's replica count via `client-go`,
plus the manifests and RBAC to run the stack on `kind`.

**Why here:** With the interface settled and the autoscaler working against
Docker, this swaps the driver and nothing else.

**Done when:** The same test file, unchanged, runs on `kind` with the autoscaler
driving replica count. `kubectl get pods` shows pods appearing and disappearing.
No orchestrator logic changed to make this work — only the driver selection.

**Teaches:** `client-go`, Deployments, ServiceAccounts and RBAC, in-cluster
configuration.

"No orchestrator logic changed" is the claim the interface exists to support, and
it is worth confirming rather than assuming.

### Piece 21 — Observing the target's own autoscaling

**Goal:** The target deployed under a Horizontal Pod Autoscaler, with a run that
captures the HPA adding pods and target latency recovering.

**Why here:** It needs both a working platform and a Kubernetes environment.

**Done when:** Sustained load against the CPU-burn endpoint drives the HPA to add
pods; the Grafana dashboard shows latency degrading and then recovering as
capacity arrives.

**Teaches:** HPA mechanics, resource requests and limits, metrics-server.

This is the piece that demonstrates the *use case* rather than the tool: proving
someone's autoscaling policy actually works is the reason a real engineer would
run this. It is also the second of the two distinct kinds of autoscaling in the
project (spec §6) — the fleet scaling itself, and the target scaling under load —
and being able to tell them apart cleanly is worth more in an interview than
either alone.

### Piece 22 — Fault injection under Kubernetes

**Goal:** Confirm Piece 14's recovery still holds when the fleet is real pods
being deleted.

**Why here:** Recovery was proven against local processes. Pod deletion is a
different failure shape — the replica controller is also reacting.

**Done when:** `kubectl delete pod` on a worker mid-run completes the run with
load redistributed, and the dashboard shows the dip and recovery. No metrics from
before the failure are lost.

**Teaches:** Kubernetes pod lifecycle, graceful shutdown and `SIGTERM`,
distinguishing failure from intentional scale-down.

### Piece 23 — EKS run and measured numbers

**Goal:** One short deployment to EKS producing the real numbers the README and
resume will cite.

**Why here:** A laptop caps the fleet at roughly 5–8 workers, so the honest
numbers can only come from real infrastructure (spec §13).

**Done when:** Peak worker count, peak requests/second, and observed latency
percentiles are recorded from an actual run — measured, not estimated — and the
teardown checklist in spec §13 is complete with a billing check the following
day.

**Teaches:** EKS, `eksctl`, cloud cost awareness, instance sizing.

Two hard constraints. The EKS control plane bills **$0.10/hour even with zero
worker nodes**, so the cluster is created for a session and destroyed at the end
of it. And every number that reaches the resume comes from this run — placeholders
in the resume draft exist to be replaced with these figures, not to be quietly
guessed.

### Piece 24 — README **(Phase 4 gate, Layer 1 done)**

**Goal:** A short README: what it is, architecture diagram, quickstart, measured
results, link to the design doc.

**Why here:** It needs Piece 23's numbers and a dashboard screenshot to exist.
This is the gate that closes Layer 1.

**Done when:** Someone who has never seen the project can clone it, run
`docker compose up`, and get a working test with dashboards, following only the
README. The measured numbers are present with no placeholders left.

**Teaches:** Technical writing, diagramming, documenting a known ceiling
honestly.

The README stays short and the design doc stays long — reasoning and rejected
alternatives belong in `design.md`, not in the front door. Documenting the
~28k-connections-per-worker ceiling and how horizontal scaling addressed it is
worth more than omitting it (spec §6).

---

## Phase 5 — gRPC target adapter

Not required for Layer 1 "done", but the highest-value next piece, because it
proves the `Protocol` abstraction was real.

Corresponds to spec §12, Week 5.

### Piece 25 — `GRPCProtocol`

**Goal:** A second `Protocol` implementation making unary gRPC calls, selected by
`protocol: grpc` in the test file.

**Why here:** It requires everything else to exist, and it is the cheapest way to
show the interface earns its keep.

**Done when:** A gRPC target can be load tested with the same orchestrator,
worker, autoscaler, and dashboards, changed only by the test file — one new file
in `internal/protocol` and zero changes to orchestrator or worker plumbing.

**Teaches:** gRPC clients as a load target, reflection or generated stubs,
extension via interfaces.

WebSocket is deliberately *not* next. It is not request/response, so latency and
requests-per-second would both need redefining, and it would change the metrics
semantics rather than reuse them (spec §11).

---

## Ordering at a glance

| Phase | Pieces | Gate | Nothing distributed until |
|---|---|---|---|
| 1 — Measure correctly | 1–9 | Piece 9: measured latency matches injected latency | — |
| 2 — Two processes | 10–14 | Piece 14: fleet survives a worker death | Phase 1 passes |
| 3 — Containers and dashboards | 15–18 | Piece 18: `docker compose up` yields live dashboards | Phase 2 passes |
| 4 — Autoscaling and Kubernetes | 19–24 | Piece 24: README with measured numbers — Layer 1 done | Phase 3 passes |
| 5 — gRPC targets | 25 | Same stack, new protocol, one new file | Layer 1 done |

Each phase adds exactly one kind of complexity: Phase 1 adds none, Phase 2 adds
the network, Phase 3 adds containers, Phase 4 adds self-adjustment and a second
environment. When something breaks, the phase says what kind of problem it is.
