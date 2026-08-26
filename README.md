# Distributed Load Testing Platform

A distributed load generator that coordinates Go workers over gRPC to produce
and measure HTTP traffic.

**Stack:** Go · gRPC · Docker · Kubernetes · Prometheus · Grafana

![Load Tester Overview dashboard](docs/images/load-tester-overview.png)

## Capabilities

- Open and closed load models; the open constant-rate model uses coordinated-omission-aware scheduling.
- Distributed worker orchestration with additive histogram aggregation.
- Docker and Kubernetes fleet management with worker failure recovery.
- Prometheus metrics and provisioned Grafana dashboards.

## Architecture

```mermaid
flowchart LR
    C["YAML scenario"] --> O["Orchestrator<br/>schedule · scale · aggregate"]
    O -->|"assignments over gRPC"| W["Worker fleet"]
    W -->|"HTTP load"| T["Target service"]
    W -->|"metric deltas over gRPC"| O
    O -->|"metrics endpoint"| P["Prometheus"]
    P --> G["Grafana"]
```

The orchestrator schedules demand and manages fleet size. Workers execute
requests and stream metric deltas back for aggregation; Prometheus scrapes the
resulting metrics for Grafana.

## Quickstart

Requires Git and Docker with Docker Compose.

```bash
git clone https://github.com/marcuslin123/load-tester.git
cd load-tester
docker compose up --build --detach
```

Open [Grafana](http://localhost:3000) and sign in with `admin` / `admin`. The
committed demo runs for five minutes at 200 RPS with two workers against the
included target service.

Optionally follow the final result:

```bash
docker compose logs --follow orchestrator
```

Remove the local stack and its Prometheus data:

```bash
docker compose down --volumes
```

## How It Works

1. Parse and validate the YAML scenario.
2. Schedule open- or closed-model demand across workers.
3. Execute requests while streaming counters and histograms over gRPC.
4. Merge metric deltas for the final result and live Prometheus export.

## Verification

```bash
go test ./...
./scripts/test-compose.sh
```

The Compose smoke test verifies service health, worker contributions, live
Prometheus series, provisioned Grafana queries, and cleanup.

## Cloud Benchmark

Local Docker Compose and kind validation are complete. The reproducible EKS
benchmark and measured cloud results are tracked in Piece 23.

## Documentation

- [Design document](docs/design.md): architecture, invariants, and trade-offs.
- [Roadmap](docs/roadmap.md): incremental milestones and validation gates.

The current target adapter is HTTP, and the orchestrator is intentionally a
single process. A gRPC target adapter is the next project layer.
