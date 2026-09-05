# CounterGhost — PRD Analysis & Structured Summary

---

## Objective

Build a **single Go binary** that simulates sharded counters across multiple nodes, demonstrates how partial state loss on crash silently corrupts the global count, and then **detects and corrects** that corruption — proving correctness with machine-checkable assertions and a live WebSocket dashboard. All within a 24-hour hackathon window.

---

## Functional Requirements

| # | Requirement | Source |
|---|---|---|
| F1 | **Multiple simulated nodes** — each a goroutine with its own SQLite file, maintaining a local shard of a global counter | PRD G1 |
| F2 | **Durable, uniquely-identified operations (deltas)** — every counter mutation is a UUID-keyed `Operation` struct persisted to SQLite before being applied | PRD G2, §5.1 |
| F3 | **Deterministic, seeded crash injection** — delete a subset of a node's operation rows (partial loss, not total), reproducible via seed | PRD G3 |
| F4 | **Epoch tracking** — every node restart increments an epoch; a restarted node is a new incarnation, not a silent continuation | PRD G4 |
| F5 | **Node manifest** — after restart, node publishes a manifest with `ProcessedRanges` that shows gaps (evidence of lost operations) | PRD §5.2 |
| F6 | **Reconciliation engine** — compare manifest against authoritative log, detect missing operations, replay only those, correct the global count | PRD G5, §6 step 4 |
| F7 | **Delayed pre-crash delta replay handling** — reject duplicates at the SQLite constraint layer, count them in `DuplicatesIgnored` | PRD G6, §6 step 5 |
| F8 | **Machine-checkable final assertion** — print/display `expected == recovered → PASS`, `duplicates_ignored == N` | PRD G7, §7 |
| F9 | **Live WebSocket dashboard** — embedded HTML/JS/CSS via `go:embed`, real-time updates, invariant panel | PRD G8 |
| F10 | **Scenario A** — Naive aggregation fails → reconciliation fixes it (on-demand from dashboard) | PRD §8 |
| F11 | **Scenario B** — Delayed replay after recovery must not double-count (on-demand from dashboard) | PRD §8 |
| F12 | **6 correctness invariants** (I1–I6) — machine-checked after every state transition | PRD §7 |
| F13 | **CLI flags** — `-port`, `-nodes`, `-seed`, `-db-dir`, `-reset` | PRD §4.3 |
| F14 | Both scenarios must be **triggerable on-demand** (buttons), not just a canned startup script | PRD §8 |

---

## Non-Functional Requirements

| Category | Requirement |
|---|---|
| **Stack** | Go single binary, SQLite (pure Go driver `modernc.org/sqlite`), no CGo |
| **Zero external deps** | No Docker, no DB server, no broker, no frontend build step required to run |
| **Reproducibility** | Seeded RNG (`-seed 42`) makes every demo run deterministic |
| **Offline capable** | After one `go mod tidy`, everything runs fully offline |
| **Deployment** | `go run .` or `go build -o counterghost . && ./counterghost` |
| **Correctness > Polish** | If time is short, cut dashboard polish, never cut invariant coverage |
| **Docker** | Optional, pre-built as insurance — NOT the live demo runtime |

---

## Explicit Deliverables

1. ✅ All 6 invariants (I1–I6) pass as automated `go test` cases
2. ✅ Scenario A and B both runnable on-demand from the live dashboard
3. ✅ Final assertion block printed/displayed exactly as in §7
4. ✅ No external services required (`go run .` is sufficient)
5. ✅ Builds and runs fully offline after one initial `go mod tidy`
6. ✅ Demo rehearsed end-to-end at least twice

---

## Key Data Structures (from PRD §5)

### Operation (Delta)
```
OperationID   string    // durable UUID, never regenerated on retry
CounterID     string    // e.g. "inventory:sku-42"
NodeID        string    // originating node
Epoch         int64     // node incarnation at creation time
Sequence      int64     // monotonic per-node-epoch ordering
Amount        int64     // +/- delta
CreatedAt     time.Time
```

### Node Manifest
```
NodeID                  string
Epoch                   int64
HighestContiguousSeq    int64
ProcessedRanges         [][2]int64   // gaps = evidence of loss
LocalValue              int64
ProcessedOperationHash  string
```

### ReconciliationResult
```
NodeID              string
MissingOperationIDs []string
RecoveredValue      int64
DuplicatesIgnored   int
```

---

## Core Algorithm (PRD §6)

1. **Normal op** → assign UUID + epoch/sequence → write to SQLite → apply to in-memory counter
2. **Crash injection** → delete subset of operation rows → restart goroutine with new epoch
3. **Manifest publish** → restarted node shows gap in ProcessedRanges
4. **Reconciliation** → `Missing = Authoritative(old epoch) − Local(node)` → `RecoveredValue = LocalValue + Σ(missing amounts)`
5. **Replay rejection** → duplicate OperationID rejected at SQLite UNIQUE constraint → counted in DuplicatesIgnored
6. **Invariant check** → runs after every state transition

---

## Architecture Summary

```
Single Go Binary
├── Simulation Core (goroutines)
│   ├── N simulated nodes (goroutine + SQLite file each)
│   ├── Operation log per node (durable, unique operation_id)
│   ├── Crash/partial-loss injector (deterministic, seeded)
│   └── Epoch manager
├── Reconciliation Engine
│   ├── Manifest comparison
│   ├── Missing-operation detection & replay
│   └── Duplicate/replay rejection
├── Invariant Checker
│   └── Runs after every state change, emits pass/fail
├── Event Bus (Go channel, fan-out)
├── WebSocket server (net/http + gorilla/websocket)
└── Embedded dashboard (go:embed HTML/JS/CSS)
```

---

## Ambiguities, Gaps & Questions for You

### 1. Authoritative Log Location
The PRD says reconciliation compares against an "authoritative" operation log. **Where does this live?**

- **Option A:** A separate "coordinator" SQLite database that receives copies of every operation from all nodes (acts as the source of truth).
- **Option B:** The authoritative log is the **union of all nodes' durable logs** — reconciliation queries across all node DBs.
- **Option C:** Each node sends operations to a central in-memory event bus, which also durably logs them to a coordinator DB.

> The PRD's phrasing ("authoritative operations from old epoch") suggests a central log. **My read: Option A or C — a coordinator DB.** Need your confirmation.

### 2. Counter ID Scope
The PRD mentions `CounterID = "inventory:sku-42"` — is there **one global counter** for the demo, or multiple? Scenario A says "global = 100" which suggests a single counter.

> **My read:** Single counter for MVP demo. Multiple counter support is not needed.

### 3. Operation Distribution in Scenario A
"Seed 3 nodes with independent operations → global = 100. Issue 20 more ops to Node A → global = 120."

> Does "20 more ops to Node A" mean 20 ops of amount +1 each? Or could they be varied amounts summing to 20? **My read:** simplest is 20 ops × +1 = 20.

### 4. "ProcessedOperationHash" in Manifest
This is described as a "cheap integrity check." Is this critical for MVP or a nice-to-have?

> **My read:** Implement it (it's a simple hash over operation IDs), but it's secondary to the gap-detection logic.

### 5. Event Bus Design
The PRD shows an "Event Bus (Go channel, fan-out)" — is this just for feeding the WebSocket/dashboard, or does the reconciliation engine also consume from it?

> **My read:** The event bus is primarily for the dashboard (UI updates). The reconciliation engine queries SQLite directly.

---

## Proposed Step-by-Step Build Plan

Based on the PRD's 24-hour plan (§10), adapted for our incremental checkpoint workflow:

### Phase 1: Foundation (Hours 0–1)
1. **Go module + project skeleton** — `go mod init`, directory structure, main.go stub
2. **SQLite schema** — `operations` table, `manifests` table, per-node DB files
3. **Config flags** — `-port`, `-nodes`, `-seed`, `-db-dir`, `-reset`
4. **Exit criteria:** `go run .` boots, creates DB files, serves empty dashboard page

### Phase 2: Operation Model + Node Simulation (Hours 1–4)
5. **Operation struct + DB layer** — CRUD for operations, UNIQUE constraint on OperationID
6. **Node goroutine** — applies operations to local counter, writes to its SQLite
7. **Global aggregation** — sum across all nodes' local values
8. **Exit criteria:** Unit tests — applying N ops gives correct local + global sum

### Phase 3: Crash Injection + Epoch Manager (Hours 4–7)
9. **Epoch manager** — tracks incarnations per node, increments on restart
10. **Crash injector** — seeded, deterministic deletion of operation rows from a node's DB
11. **Manifest generation** — node publishes manifest with ProcessedRanges after restart
12. **Exit criteria:** Can crash a node, see gap in manifest, epoch incremented

### Phase 4: Reconciliation Engine (Hours 7–10)
13. **Authoritative log** — coordinator/central DB that stores all operations
14. **Missing operation detection** — compare manifest ranges against authoritative log
15. **Recovery** — replay missing operations, correct local and global counts
16. **Exit criteria:** Scenario A passes as `go test`

### Phase 5: Replay/Duplicate Handling (Hours 10–12)
17. **Delayed delta replay** — simulate re-delivery of pre-crash operations
18. **Dedup at SQLite constraint layer** — reject duplicates, count in DuplicatesIgnored
19. **Exit criteria:** Scenario B passes as `go test`

### Phase 6: Invariant Checker (Hours 12–14)
20. **6 invariants (I1–I6)** — implement checks, run after every state transition
21. **Assertion output** — print the machine-checkable pass/fail block
22. **Exit criteria:** All 6 invariants pass as `go test`, assertion block prints correctly

### Phase 7: Dashboard + WebSocket (Hours 14–17)
23. **Event bus** — Go channel fan-out for state changes
24. **WebSocket server** — `net/http` + `gorilla/websocket`
25. **Embedded HTML/JS/CSS** — `go:embed`, live-updating counters + invariant panel
26. **Exit criteria:** Dashboard shows live counter updates during a scripted run

### Phase 8: Wire Scenarios to Dashboard (Hours 17–19)
27. **Scenario A button** — triggers crash + reconciliation flow from UI
28. **Scenario B button** — triggers delayed replay from UI
29. **Exit criteria:** Both scenarios triggerable on-demand, repeatedly

### Phase 9: Edge-Case Hardening (Hours 19–21)
30. **Double crash, crash during reconciliation, empty node** — no panics, invariants hold
31. **Exit criteria:** Stress tests pass

### Phase 10: Rehearsal + Polish (Hours 21–24)
32. **Full demo rehearsal** 2–3 times end-to-end
33. **Dockerfile** (pre-built, tested, not used for live demo)
34. **Buffer** — no risky changes after hour 23

---

> [!IMPORTANT]
> **Before I proceed:** Please review this analysis and confirm:
> 1. Is my understanding of the overall system correct?
> 2. Do you have answers for the 5 questions in the "Ambiguities" section?
> 3. Does the build plan ordering look right to you?
