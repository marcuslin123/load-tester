# Target App Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `cmd/target`, a small HTTP service that injects known latency, probabilistic failures, and bounded CPU work for load-generator validation.

**Architecture:** Put all request behavior in `internal/targetapp.NewHandler`, where `httptest` can exercise it without opening a network port. Keep `cmd/target/main.go` responsible only for parsing the listen-address flag and starting `http.Server`.

**Tech Stack:** Go 1.26.5 standard library (`net/http`, `net/http/httptest`, `time`, `math/rand`, `flag`).

## Global Constraints

- Use no third-party dependencies.
- `GET /health` returns HTTP 200.
- `GET /echo?latency=<duration>&error_rate=<0..1>` waits for the requested duration and then returns either HTTP 200 or HTTP 503.
- `GET /burn?ms=<integer>` performs CPU work for approximately the requested number of milliseconds and returns HTTP 200.
- Invalid, negative, or out-of-range query values return HTTP 400.
- Bound latency and CPU-burn inputs to 10 seconds so an accidental request cannot tie up the local service indefinitely.

---

### Task 1: Testable Target HTTP Handler

**Files:**
- Create: `internal/targetapp/handler_test.go`
- Create: `internal/targetapp/handler.go`

**Interfaces:**
- Consumes: standard `http.Handler` and URL query strings.
- Produces: `func NewHandler() http.Handler`.

- [x] **Step 1: Write failing handler tests**

Add tests that call `NewHandler()` through `httptest` and assert:

```go
func TestHealthReturnsOK(t *testing.T)
func TestEchoWaitsForInjectedLatency(t *testing.T)
func TestEchoAlwaysFailsAtErrorRateOne(t *testing.T)
func TestEchoRejectsInvalidParameters(t *testing.T)
func TestBurnConsumesRequestedTime(t *testing.T)
func TestBurnRejectsInvalidMilliseconds(t *testing.T)
```

Use a 20ms request with a 15ms lower bound for timing assertions, and table-driven cases for malformed values.

- [x] **Step 2: Run the tests and verify RED**

Run:

```bash
env GOCACHE="$PWD/.cache/go-build" go test ./internal/targetapp
```

Expected: compilation fails because `NewHandler` does not exist.

- [x] **Step 3: Implement the minimal handler**

Create a `http.ServeMux` with `/health`, `/echo`, and `/burn`. Parse latency with `time.ParseDuration`, parse error rate with `strconv.ParseFloat`, and parse burn milliseconds with `strconv.Atoi`. Return HTTP 400 for invalid values; otherwise wait, select a status using `rand.Float64`, or spin until a deadline.

- [x] **Step 4: Run the tests and verify GREEN**

Run:

```bash
env GOCACHE="$PWD/.cache/go-build" go test ./internal/targetapp
```

Expected: all handler tests pass.

### Task 2: Target Binary

**Files:**
- Create: `cmd/target/main.go`

**Interfaces:**
- Consumes: `targetapp.NewHandler()`.
- Produces: a `target` executable with `-addr` defaulting to `:8080`.

- [x] **Step 1: Add the binary wiring**

Parse `-addr`, construct an `http.Server` with sensible read-header timeout, log the address, and return a fatal error if `ListenAndServe` exits unexpectedly.

- [x] **Step 2: Verify the full Go module**

Run:

```bash
env GOCACHE="$PWD/.cache/go-build" go test ./...
env GOCACHE="$PWD/.cache/go-build" go vet ./...
env GOCACHE="$PWD/.cache/go-build" go build ./...
```

Expected: all commands exit zero.

- [x] **Step 3: Verify the roadmap acceptance checks manually**

Start `go run ./cmd/target`, then check `/health`, `/echo?latency=250ms`, `/echo?error_rate=1.0`, and `/burn?ms=50` with `curl`. Confirm statuses are 200, 200, 503, and 200, and confirm the timed endpoints take approximately their requested durations.

- [x] **Step 4: Commit and push**

Stage the plan, handler, tests, and binary. Commit as `add target app`, push `main`, and confirm the worktree is clean and synchronized with `origin/main`.
