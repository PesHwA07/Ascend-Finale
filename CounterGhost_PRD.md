# CounterGhost — Product Requirements Document
### Distributed Counter Integrity After Partial Node State Loss
**Hackathon build window:** 24 hours · **Stack:** Go (single language) · **Storage:** SQLite

---

## 1. Problem Statement (Restated)

A counter sharded across multiple nodes (e.g., remaining inventory, rate-limit usage) is periodically
aggregated into a global total. If a node crashes and restarts having lost **part** of its local counter
state, the global aggregate silently undercounts — and this looks identical to a legitimate reset from the
outside. The system must:

1. Distinguish genuine partial state loss from a normal reset.
2. Recover the exact correct global count after such loss.
3. Correctly handle pre-crash deltas that arrive late (after restart) — applying each exactly once,
   without losing the contribution or double-counting a replay.

This is a **correctness demonstration under simulated failure**, not a production throughput/HA system.

---

## 2. Goals vs. Non-Goals

### 2.1 MVP Goals (must-have — this is what gets graded)
| # | Goal | Why it's core |
|---|---|---|
| G1 | Multiple simulated nodes, each maintaining a local shard of a global counter | Establishes the sharded-counter baseline |
| G2 | Durable, uniquely-identified operations (deltas) — not just aggregate values | This is the entire correctness spine; without it, loss vs. reset is undetectable |
| G3 | Deterministic, seeded crash injection causing **partial** (not total) local state loss | Must be reproducible for live demo and grading |
| G4 | Node epoch tracking — a restart is a new incarnation, not a silent continuation | Lets the system tell fresh state from stale/incomplete state |
| G5 | Reconciliation engine: detect missing operations vs. the durable log, replay only those, and correct the global count | The actual "fix" the problem statement demands |
| G6 | Delayed pre-crash delta replay handling — apply-exactly-once via operation ID dedup | Explicitly required by the problem statement's second scenario |
| G7 | Machine-checkable final assertion (`expected == recovered`, `duplicates_ignored == N`) | Explicitly required — "machine-checkable," not just visually plausible |
| G8 | Live, real-time demo view (WebSocket-driven, embedded dashboard) | Hackathon judging is live — must be watchable, not just log output |

### 2.2 Non-Goals for MVP (explicitly deferred — see Section 9)
- Kafka/Redpanda-backed message delivery
- Postgres, Prometheus, Grafana, Docker Compose multi-service orchestration
- React/TypeScript frontend build pipeline
- Multi-region / real network simulation
- Authentication, multi-tenant counters, horizontal scaling beyond demo node count

---

## 3. Architecture (MVP)

Single Go binary. No external services. No Docker required to run.

```
+--------------------------------------------------------------+
|                         Go binary                              |
|                                                                |
|  Simulation Core (goroutines)                                 |
|   - N simulated nodes (each a goroutine + local SQLite file)   |
|   - Operation log per node (durable, unique operation_id)      |
|   - Crash/partial-loss injector (deterministic, seeded)        |
|   - Epoch manager                                              |
|                                                                |
|  Reconciliation Engine                                          |
|   - Manifest comparison (processed ranges vs. authoritative)   |
|   - Missing-operation detection & replay                       |
|   - Duplicate/replay rejection (operation_id dedup)             |
|                                                                |
|  Invariant Checker                                              |
|   - Runs after every state change                               |
|   - Emits pass/fail per invariant (see Section 7)               |
|                                                                |
|  Event Bus (Go channel, fan-out)                                |
|      |                                                          |
|      v                                                          |
|  WebSocket server (net/http + gorilla/websocket)                 |
|      |                                                          |
|      v                                                          |
|  Embedded dashboard (go:embed HTML/JS/CSS — no build step)      |
+--------------------------------------------------------------+
```

**Why this shape:** one command to run (`go run .`), no live-demo infra risk, real goroutine concurrency
for node behavior, and the correctness core (simulation + reconciliation + invariants) never crosses a
language or serialization boundary.

---

## 4. Local Deployment (How to Run)

This project has **zero external dependencies** — no Docker, no database server, no broker, no frontend
build step. The only requirement is a Go toolchain. This section exists so a mentor, judge, or teammate
can run and verify the project themselves without needing this document explained to them.

**Note on Docker/cloud (decision recorded, updated):** Judging is confirmed **in-person, live on the
presenter's laptop**, so the primary demo path remains the direct binary execution described above —
that stays the lowest-risk option and is what actually runs during judging. However, a Docker image is
prepared **in advance** (Section 4.6) purely as a rehearsed answer if a judge or mentor asks
"could you deploy this?" — it is not the runtime used for the live demo itself, to avoid introducing
daemon/build-time risk into the judged run.

### 4.1 Prerequisites
| Requirement | Version | Notes |
|---|---|---|
| Go | 1.21+ | `go version` to check |
| SQLite driver | `modernc.org/sqlite` (pure Go, no CGo) | Preferred over `mattn/go-sqlite3` specifically to avoid requiring a C compiler on the judge/demo machine |
| Browser | Any modern browser | Only needed to view the live dashboard; not required to run `go test` |

### 4.2 Build & Run
```bash
# clone / unzip the project, then from the project root:
go mod tidy              # fetch dependencies (one-time, needs network)
go build -o counterghost .
./counterghost            # starts simulation + dashboard server
```
or, for local iteration without a separate build step:
```bash
go run .
```
Dashboard is served at **http://localhost:8080** by default (configurable — see 4.3).

### 4.3 Configuration flags
| Flag | Default | Purpose |
|---|---|---|
| `-port` | `8080` | Dashboard/WebSocket HTTP port |
| `-nodes` | `3` | Number of simulated nodes |
| `-seed` | `42` | RNG seed for any randomized behavior — keeps demo runs reproducible |
| `-db-dir` | `./data` | Where per-node SQLite files are created |
| `-reset` | `false` | Wipe `./data` on startup for a clean demo run |

Example for a clean, reproducible demo start:
```bash
./counterghost -reset -nodes 3 -seed 42 -port 8080
```

### 4.4 Running correctness tests independently of the dashboard
This is the fallback path from Section 12's risk table — judges can verify correctness even if the
live dashboard is skipped entirely:
```bash
go test ./... -v
```
Expect all 6 invariant tests (Section 7) and both demo scenarios (Section 8) to pass, with the final
assertion block printed to stdout.

### 4.5 Troubleshooting
| Symptom | Likely cause | Fix |
|---|---|---|
| `bind: address already in use` | Port 8080 taken by another process | Run with `-port 8081` (or any free port) |
| `go: module lookup disabled by GOFLAGS=-mod=mod` / dependency fetch fails | No network access at run time | Run `go mod tidy` once beforehand with network available; not needed again offline afterward |
| Dashboard loads but shows no live updates | WebSocket blocked by a browser extension or corporate proxy | Try a different browser or an incognito window with extensions disabled |
| `database is locked` errors in logs | Multiple writers hitting the same SQLite file directly instead of through the node's single writer path | Confirm `-db-dir` isn't being shared across two running instances; each node writes to its own file |
| Stale data from a previous run confuses a fresh demo | `./data` wasn't cleared | Restart with `-reset` |

**Pre-demo checklist (run this the night before, not the morning of):**
1. `go build -o counterghost .` succeeds with no network connection (proves dependencies are already cached).
2. `./counterghost -reset -nodes 3 -seed 42` boots cleanly and the dashboard loads in a fresh browser tab.
3. `go test ./... -v` passes completely, offline.
4. Run Scenario A and B once from the dashboard, then restart with `-reset` so the actual judging run starts from a known-clean state.

### 4.6 Optional Docker Packaging (prepared in advance — not the primary demo path)

Built and rehearsed ahead of time so it's ready to show immediately if asked "could you deploy this?" —
this is **not** what runs during the actual judged demo (see Section 4's note above); it exists purely
as a rehearsed, working answer to a deployability question.

**Dockerfile (multi-stage, no CGO needed):**
```dockerfile
# ---- Build stage ----
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /counterghost .

# ---- Final stage ----
FROM gcr.io/distroless/static-debian12
WORKDIR /app
COPY --from=builder /counterghost /app/counterghost
VOLUME ["/app/data"]
EXPOSE 8080
ENTRYPOINT ["/app/counterghost", "-db-dir=/app/data"]
```

**Build and run:**
```bash
docker build -t counterghost:latest .
docker run --rm -p 8080:8080 -v "$(pwd)/data:/app/data" counterghost:latest -reset -nodes 3 -seed 42
```

**Why this shape:**
- `distroless/static` as the final base image — no shell, no package manager, smallest reasonable attack
  surface and image size, appropriate because the binary genuinely has zero runtime dependencies
  (pure-Go SQLite driver, `go:embed` dashboard assets baked in).
- CGO disabled deliberately — keeps the binary fully static, which is exactly what makes the distroless
  final stage possible in the first place.
- The `-db-dir=/app/data` flag is fixed in `ENTRYPOINT`, with a named volume so per-node SQLite files
  can persist across container restarts or be inspected from the host if a mentor wants to see the
  durable operation log directly.

**Pre-hackathon checklist for this artifact specifically:**
1. `docker build -t counterghost:latest .` succeeds without errors, well before judging starts.
2. `docker run` (command above) boots cleanly and the dashboard is reachable at `localhost:8080`.
3. Keep this rehearsed but **do not swap it in as the live-demo runtime** — it is insurance, not the plan.

---

## 5. Data Model

### 5.1 Operation (Delta)
```go
type Operation struct {
    OperationID   string    // durable UUID, generated once, never regenerated on retry
    CounterID     string    // e.g. "inventory:sku-42"
    NodeID        string    // originating node
    Epoch         int64     // node incarnation at time of creation
    Sequence      int64     // monotonic per-node-epoch ordering
    Amount        int64     // +/- delta
    CreatedAt     time.Time
}
```
`OperationID` is the unique key enforced at the SQLite layer (`PRIMARY KEY` / `UNIQUE` constraint) —
this is what makes duplicate-replay rejection a storage-level guarantee, not just application logic.

### 5.2 Node Manifest
```go
type Manifest struct {
    NodeID                  string
    Epoch                   int64
    HighestContiguousSeq    int64
    ProcessedRanges         [][2]int64   // e.g. [[1,800],[802,842]] — gap at 801 = evidence of loss
    LocalValue              int64
    ProcessedOperationHash  string       // cheap integrity check across the set
}
```
The manifest — not the bare local value — is what the reconciler compares against the authoritative log.
A gap in `ProcessedRanges` is the actual evidence distinguishing "lost data" from "legitimately never
had it."

### 5.3 Reconciliation Result
```go
type ReconciliationResult struct {
    NodeID              string
    MissingOperationIDs []string
    RecoveredValue      int64
    DuplicatesIgnored   int
}
```

---

## 6. Core Algorithm (MVP Logic)

1. **Normal operation:** API assigns each new operation a UUID + current node epoch/sequence, writes it
   to the node's durable SQLite operation log, then applies it to the in-memory local counter.
2. **Crash injection (seeded, deterministic):** Delete a contiguous or scattered subset of a node's local
   operation rows directly (simulating partial state loss), then restart that node's goroutine with a
   **new epoch**.
3. **Manifest publish:** Restarted node publishes its manifest — processed ranges now show a gap.
4. **Reconciliation:**
   `Missing = AuthoritativeOperations(old epoch) − LocalProcessedOperations(node)`
   `RecoveredValue = LocalValue + Σ amount(op) for op in Missing`
5. **Replay of delayed pre-crash deltas:** Any delta arriving with an `OperationID` already present in
   the durable log is rejected at the constraint layer and counted in `DuplicatesIgnored` — never
   re-applied to the counter.
6. **Invariant check:** Run after every state transition (see Section 7); dashboard reflects live
   pass/fail.

---

## 7. Correctness Invariants (Machine-Checked)

| # | Invariant | Assertion |
|---|---|---|
| I1 | No duplicate contribution | `count(operation_id applied) ≤ 1` for every operation |
| I2 | No acknowledged operation disappears | Every operation in a trusted global count remains discoverable in durable storage |
| I3 | Recovery is monotonic | Replaying the same missing operation twice cannot increase the count twice |
| I4 | Epochs are unique per node | A restarted node cannot silently overwrite its previous incarnation's identity |
| I5 | Global count is reproducible | Recomputing from the durable operation set independently matches the live aggregate |
| I6 | Reconciliation is convergent/idempotent | `Reconcile(Reconcile(state)) == Reconcile(state)` |

The **final deliverable output** must include an explicit assertion block, e.g.:
```
expected_global_count     = 120
recovered_global_count    = 120
duplicate_operations_ignored = 8
ASSERT expected == recovered   -> PASS
```

---

## 8. Required Demo Scenarios

### Scenario A — Naive aggregation fails, reconciliation fixes it
1. Seed 3 nodes with independent operations → global = 100.
2. Issue 20 more ops to Node A → global = 120.
3. Crash Node A, deleting 8 of its local operation records → naive local value = 52, naive global = 112.
4. **Show the wrong number on screen.**
5. Trigger reconciliation live → missing 8 operation IDs detected → replayed → Node A = 60 → global = 120.
6. **Show the corrected number on screen, invariant panel flips to green.**

### Scenario B — Delayed replay after recovery must not double-count
1. Continuing from Scenario A's healthy state (global = 120).
2. Re-deliver the same 8 pre-crash operation IDs (simulating delayed network delivery).
3. System must reject all 8 as duplicates (`DuplicatesIgnored = 8`), global remains 120.
4. **Show the duplicate-rejection counter incrementing live, global count staying flat.**

Both scenarios must be triggerable **on demand via the live dashboard** (buttons/endpoints), not only
as a canned startup script — judges should be able to ask "do it again" and see the same result.

---

## 9. Future / Additional Goals (Post-MVP — only after Section 2.1 is fully working and demoed)

These are explicitly **out of scope until the core MVP is proven and rehearsed**. Do not start these
during the 24-hour window unless there is verified slack time after Section 8's scenarios are working
and rehearsed end-to-end.

| Priority | Addition | Value it adds |
|---|---|---|
| P1 | Kafka/Redpanda-backed delivery variant | Shows the same reconciliation logic composes with real streaming infra |
| P2 | Multi-scenario chaos mode (random churn + random partial loss, not just seeded) | Demonstrates robustness beyond the two scripted cases |
| P3 | Prometheus metrics + Grafana panel | Production-observability story for judges who ask "how would this run in prod" |
| P4 | Persisted historical run comparison (did we get faster/more reliable across runs) | Nice-to-have credibility signal |
| P5 | More counter types (multi-field invariants, not just scalar sum) | Bridges toward the broader WriteFork-style problem, shows extensibility |
| P6 | Auth / multi-tenant counters | Only relevant if reframing as a product, not just a proof |
| — | ~~Containerized (Docker) variant~~ | **Done ahead of time** — see Section 4.6. Prepared as a rehearsed deployability answer, not used as the live-demo runtime. Cloud hosting itself remains deferred unless the demo format changes to remote/async. |

---

## 10. 24-Hour Build Plan

| Hours | Focus | Exit criteria |
|---|---|---|
| 0–1 | Repo/module setup, SQLite schema, project skeleton | `go run .` boots, empty dashboard loads |
| 1–4 | Operation model + node simulation (apply logic, local counter) | Unit tests: applying N ops gives correct local sum |
| 4–7 | Crash/partial-loss injector + epoch manager | Can seed a crash deterministically; manifest shows a gap |
| 7–10 | Reconciliation engine | Scenario A passes as a `go test`, before any UI exists |
| 10–12 | Replay/duplicate handling | Scenario B passes as a `go test` |
| 12–14 | Invariant checker + explicit assertion output | CLI run prints the pass/fail block from Section 7 |
| 14–17 | Event bus + WebSocket + embedded dashboard | Live counters update in browser during a scripted run |
| 17–19 | Wire Scenario A + B to on-demand dashboard buttons | Judges can trigger both scenarios live, repeatedly |
| 19–21 | Edge-case hardening (double crash, crash during reconciliation, empty node) | No panics; invariants still hold under stress |
| 21–23 | Rehearse the full demo narrative 2–3 times end-to-end | Demo runs cleanly without your intervention beyond clicking buttons |
| 23–24 | Buffer, final polish, backup plan (recorded run in case live demo hiccups) | Ready state — nothing risky touched after this point |

**Rule of thumb:** correctness logic (hours 0–14) must be tested and passing via `go test` **before**
any dashboard work starts. If time runs short, cut dashboard polish, never cut invariant coverage.

---

## 11. Hackathon Evaluation / Judging Alignment

To make review easy for judges, the demo should explicitly map back to the problem statement's own
success criteria:

| Problem statement requirement | Where it's shown |
|---|---|
| "Crash a node with partial local counter loss" | Scenario A, step 3 |
| "Correctly detecting and correcting the resulting global count rather than accepting the undercount" | Scenario A, steps 4–6 |
| "Delayed pre-crash counter deltas may arrive again" | Scenario B, step 2 |
| "Recovery must use durable delta identity... to restore the exact global count" | Section 6, step 4; Section 5.1 |
| "Without either losing the missing contribution or double-counting replayed deltas" | Scenario A (no loss) + Scenario B (no double-count), shown back-to-back |
| Distinguish genuine partial loss from normal reset | Section 5.2 manifest/processed-ranges gap detection |

**Talking point for the review:** open with the *naive* LWW-style/aggregate-only failure first (show the
wrong number), then show your system catching and fixing it — this mirrors exactly how the problem
statement frames the danger ("looks identical to a normal reset... silently undercounted").

---

## 12. Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Live demo network/browser hiccup during judging | Have a recorded backup run of both scenarios ready to play |
| Reconciliation bug only surfaces under real goroutine timing | Add a deterministic "single-threaded mode" flag for demo reliability, keep concurrent mode for stress tests |
| Running out of time before dashboard is done | Correctness (`go test` green on all 6 invariants) is the real deliverable; a CLI-only demo with clear printed output is an acceptable fallback |
| SQLite file locking across goroutines | Use one SQLite connection per node (separate files) rather than shared DB, or serialize writes via a single writer goroutine per node |

---

## 13. Definition of Done (MVP)

- [ ] All 6 invariants (Section 7) pass as automated `go test` cases
- [ ] Scenario A and B both runnable on-demand from the live dashboard
- [ ] Final assertion block printed/displayed exactly as shown in Section 7
- [ ] No external services required to run (`go run .` is sufficient)
- [ ] Project builds and runs fully offline after one initial `go mod tidy` (Section 4)
- [ ] Demo rehearsed end-to-end at least twice without manual intervention beyond clicking buttons
