# Piece 24A README Design

**Date:** 2026-08-25
**Status:** Ready for review

## Purpose

Create the project's first `README.md` as a concise, recruiter-friendly front
door. It must let a new reader understand the system quickly and run the local
Docker Compose demo without consulting another document.

Piece 24A documents only behavior already proven locally. Piece 24B will replace
the cloud-benchmark status note with measured EKS results after Piece 23.

## Audience And Style

The primary audience is a software engineering or infrastructure recruiter who
may spend about two minutes scanning the repository. A developer evaluating the
project must still be able to run it from the quickstart.

Keep every section short. Put implementation reasoning, rejected alternatives,
and detailed protocol behavior in `docs/design.md`, not the README. Avoid badge
walls, marketing language, estimated performance claims, and empty benchmark
tables.

## README Structure

### 1. Opening

- Title: `Distributed Load Testing Platform`
- One sentence explaining that Go workers are coordinated over gRPC to generate
  and measure HTTP load.
- One compact technology line: Go, gRPC, Docker, Kubernetes, Prometheus, Grafana.
- A real screenshot of the locally provisioned `Load Tester Overview` dashboard.
- Four capability bullets:
  - open and closed models with coordinated-omission-aware scheduling;
  - distributed worker orchestration and additive histogram aggregation;
  - Docker and Kubernetes fleet management with worker recovery;
  - Prometheus metrics and provisioned Grafana dashboards.

Do not claim an AWS deployment or a throughput ceiling before Piece 23 measures
it.

### 2. Architecture

Use a GitHub-native Mermaid diagram based on the approved layered-system option.
It must show:

```text
YAML scenario -> Orchestrator -> gRPC worker fleet -> Target service
                       |                 |
                       +---- metrics ----+----> Prometheus -> Grafana
```

The rendered diagram should distinguish control messages from generated target
traffic and metric deltas. A short caption states that the orchestrator schedules
demand and manages fleet size, while workers execute requests and stream deltas
back for aggregation.

### 3. Quickstart

Require only Git and Docker with Compose. Document these commands directly rather
than adding a Makefile or README-only wrapper:

```bash
git clone https://github.com/marcuslin123/load-tester.git
cd load-tester
docker compose up --build --detach
docker compose logs --follow orchestrator
docker compose down --volumes
```

Tell the reader to open `http://localhost:3000` and sign in with `admin` / `admin`.
Explain in one sentence that the committed demo runs for five minutes at 200 RPS
with two workers against the included target service. Make the log-follow command
optional so it does not obscure the dashboard path.

### 4. How It Works

Use at most four short bullets:

1. Parse and validate the YAML scenario.
2. Schedule open- or closed-model demand and assign it across workers.
3. Execute requests while streaming counters and histograms over gRPC.
4. Merge deltas for the final result and export live Prometheus metrics.

### 5. Verification

Document:

```bash
go test ./...
./scripts/test-compose.sh
```

One sentence explains that the Compose smoke test proves service health, worker
contributions, live Prometheus series, dashboard provisioning, representative
Grafana queries, and cleanup.

### 6. Cloud Benchmark Status

Do not include placeholders or estimated values. Use a concise status note:

> Local Docker Compose and kind validation are complete. The reproducible EKS
> benchmark and measured cloud results are tracked in Piece 23.

Piece 24B replaces this note with the measured topology, worker count, sustained
RPS, latency percentiles, errors, unmet demand, dropped samples, duration, and
cost after the EKS run.

### 7. Documentation And Scope

Link to `docs/design.md` for detailed architecture and trade-offs, and
`docs/roadmap.md` for project progression. Add one concise scope statement: the
current target adapter is HTTP and the orchestrator is intentionally a single
process; gRPC target support is the next layer.

## Dashboard Screenshot

Store the image at `docs/images/load-tester-overview.png`. Capture the real local
Compose dashboard after its panels contain live data. The screenshot must show
the dashboard title and the important request-rate, latency, error, worker, and
unmet-demand panels without exposing unrelated browser or desktop content.

Do not label the local screenshot as an EKS result. Piece 24B may add a separate
cloud-run screenshot rather than silently changing the meaning of this image.

## Roadmap Update

Briefly record that Piece 24 is delivered in two passes:

- Piece 24A creates the local, benchmark-honest README.
- Piece 24B adds measured EKS results and closes the Phase 4 gate after Piece 23.

Piece 24 must remain incomplete until no benchmark placeholders or pending status
notes remain.

## Verification Plan

1. Start from a clean Compose state and execute the README quickstart exactly as
   written.
2. Confirm the target, orchestrator, two workers, Prometheus, and Grafana reach
   their expected states.
3. Confirm the dashboard displays live data before capturing the screenshot.
4. Run `./scripts/test-compose.sh` and `go test ./...`.
5. Check every relative link and referenced file.
6. Render the README on GitHub-compatible Markdown and verify the Mermaid diagram,
   screenshot, headings, and code blocks are readable on narrow and wide layouts.

Add a small contract test only if it protects a real invariant such as referenced
files, quickstart commands, or the absence of fabricated benchmark values. Do not
test prose wording for its own sake.

## Out Of Scope

- Provisioning or running EKS.
- Publishing measured cloud performance numbers.
- Changing scheduler, fleet, metrics, or Compose behavior.
- Adding a Makefile, documentation site, Helm chart, or new dashboard.
- Repeating the detailed design document in the README.
