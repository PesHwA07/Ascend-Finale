# CounterGhost — Full 24-Hour Implementation Plan

## Current Status (Complete ✅)
- All 6 invariants passing (I1-I6)
- Scenario A+B working end-to-end
- Single-page dashboard with manual controls
- 29 tests, all green
- Everything merged to `main`

---

## Phase 1: Core Infrastructure (Hours 0–6)

> Foundation for everything else — Event Bus, WebSocket, Agents, Two-Page Dashboard

---

### 1.1 Event Bus

**What:** Central pub/sub system for all system events. Every component publishes here, dashboard subscribes.

#### [NEW] `internal/events/bus.go`
- `EventBus` struct with Go channels + fan-out to multiple subscribers
- Event types enum:
  - `NodeDown` / `NodeUp` — Sentinel detects health changes
  - `DivergenceDetected` — Reconciler spots mismatch
  - `ReconciliationStarted` / `ReconciliationComplete` — repair lifecycle
  - `ReplayRejected` — duplicate operation blocked
  - `OperationApplied` — new op written
  - `ProjectionRebuilt` — full rebuild completed
  - `ChaosExperimentRun` — chaos scenario executed
- `Subscribe() <-chan Event` — returns read-only channel
- `Publish(Event)` — thread-safe broadcast
- `History(n int) []Event` — last N events (for dashboard initial load)

---

### 1.2 WebSocket Server

**What:** Real-time push to browser. Replaces 2s HTTP polling.

#### [NEW] `internal/server/websocket.go`
- `GET /ws` endpoint using `gorilla/websocket`
- On connect: send last 50 events from `EventBus.History()`
- Subscribes to EventBus, pushes JSON events to all connected clients
- Handles: connect, ping/pong keepalive, graceful disconnect
- Message format:
  ```json
  {
    "type": "node_down",
    "node_id": "node-0",
    "timestamp": "2026-09-05T14:32:05Z",
    "data": { "epoch": 2, "old_local_value": 40 }
  }
  ```

#### [MODIFY] `go.mod`
- Add `github.com/gorilla/websocket`

#### [MODIFY] `internal/server/server.go`
- Accept `*events.EventBus` parameter
- Publish events from existing API handlers (apply, crash, reconcile, replay)
- Wire WebSocket route

---

### 1.3 Sentinel Agent (Autonomous Health Monitor)

**What:** Goroutine that polls node health every 2s, detects crashes automatically.

#### [NEW] `internal/agents/sentinel.go`
```go
type SentinelAgent struct {
    nodes     []*node.Node
    statusMap map[string]bool  // node_id → was_healthy
    eventBus  *events.EventBus
    interval  time.Duration    // default 2s
}
```
- `Start(ctx context.Context)` — ticker loop
- Detects transitions: alive→dead → publishes `NodeDown`
- Detects transitions: dead→alive → publishes `NodeUp`
- Dashboard shows: `[14:32:05] SENTINEL: Node-0 detected DOWN`
- **No HTTP calls** — direct `node.IsAlive()` check (in-process)

---

### 1.4 Reconciler Agent (Autonomous Self-Healing)

**What:** Goroutine that detects divergence and auto-repairs without manual clicks.

#### [NEW] `internal/agents/reconciler.go`
```go
type ReconcilerAgent struct {
    simulation *simulation.Simulation
    eventBus   *events.EventBus
    threshold  int64           // minimum divergence to trigger (default 0)
    interval   time.Duration   // periodic check every 5s
}
```
- `Start(ctx context.Context)` — ticker loop + event listener
- **Periodic check (every 5s):** compare auth vs naive, if divergence > threshold → reconcile
- **Event-driven:** also reconcile immediately on `NodeUp` event
- Publishes `ReconciliationStarted` → does work → publishes `ReconciliationComplete`
- Queries across **ALL epochs** for the target node
- Filters by **node_id only** — never touches other nodes' data
- Dashboard shows autonomous repair happening in real-time

---

### 1.5 Two-Page Dashboard

**What:** Split into Chaos Control Panel (Page 1) + Agent Workflow Dashboard (Page 2).

#### [MODIFY] `dashboard/index.html` → Chaos Control Panel
- **Purpose:** Judges cause failures here
- Per-node controls: Crash, Seed, manual Reconcile, manual Replay
- Node status cards with live epoch/localVal (WebSocket-fed)
- Global counters (naive vs auth)
- Scenario A & B buttons
- Navigation: link to Agent Dashboard
- **Agent toggle:** switch to enable/disable autonomous healing

#### [NEW] `dashboard/agents.html` → Agent Workflow Dashboard
- **Purpose:** Judges watch the system heal itself
- **Live topology map:** 3 node circles (green=alive, red=crashed, animated transitions)
- **Agent activity log:** WebSocket-fed real-time log with color-coded badges
  - SENTINEL entries in blue
  - RECONCILER entries in green
  - ERRORS in red
- **Divergence display:** large number showing auth - naive (pulses red when > 0)
- **Metrics panel:** reconciliation count, avg time-to-repair, duplicates rejected
- **Invariant panel:** 6 invariants with live pass/fail
- **Assertion block:** appears after Scenario B
- Navigation: link back to Chaos Panel

---

### 1.6 Audit Trail Viewer

**What:** Scrollable list of operations from coordinator DB, proving the event log is the source of truth.

#### [NEW] API: `GET /api/operations?node_id=X&limit=50`
- Returns recent operations from coordinator DB
- Filterable by node_id

#### Dashboard section (on Agent Dashboard)
- Scrollable table: timestamp, operation_id (truncated UUID), node_id, epoch, amount
- Auto-refreshes via WebSocket (new ops pushed as `OperationApplied` events)
- Judges can verify: "yes, operation X was really there before the crash"

---

### Phase 1 Exit Criteria
- ✅ WebSocket pushes events to dashboard in real-time
- ✅ Sentinel auto-detects crashes within 3 seconds
- ✅ Reconciler auto-repairs divergence without manual clicks
- ✅ Two pages work: chaos on Page 1 → response visible on Page 2
- ✅ All existing tests still pass

---

## Phase 2: High-Value Differentiators (Hours 6–14)

> What makes this stand out from "just another counter project"

---

### 2.1 Multi-Counter Support

**Time: ~3 hours**

**What:** Support multiple independent counters (e.g., `inventory:sku-42`, `rate_limit:user-123`).

#### [MODIFY] `internal/model/model.go`
- Add `CounterID string` field to `Operation` struct

#### [MODIFY] `internal/db/db.go`
- Add `counter_id` column to operations table
- Update all queries to group by counter_id
- `SumAmountsByCounter()` — returns map[counterID]int64

#### [MODIFY] `internal/node/node.go`
- `ApplyDelta` accepts counter_id parameter
- Per-counter local values: `localValues map[string]int64`

#### [MODIFY] `internal/simulation/simulation.go`
- `NaiveGlobalByCounter()` / `AuthoritativeGlobalByCounter()`

#### Dashboard
- Per-counter breakdown display:
  ```
  inventory:sku-42  →  Node0: 45  Node1: 32  Node2: 28  Global: 105
  inventory:sku-43  →  Node0: 12  Node1:  8  Node2:  5  Global:  25
  ```
- Counter selector dropdown on Chaos Panel

**Why:** Shows this handles real-world complexity, not just one toy number.

---

### 2.2 Projection Rebuild

**Time: ~1 hour**

**What:** Button to wipe all local state and rebuild entirely from the event log.

#### [NEW] Function: `simulation.RebuildFromLog(nodeID string)`
- Delete all operations from node's local DB
- Reset localVal to 0
- Query all operations for this node from coordinator
- Replay them all via INSERT OR IGNORE
- Return: `{ before: 60, wiped_to: 0, rebuilt_to: 60, ops_replayed: 60 }`

#### [NEW] API: `POST /api/rebuild`
- Takes `{ "node_id": "node-0" }`
- Returns rebuild result

#### Dashboard
- "🔄 Rebuild from Event Log" button
- Animated progress: `localVal: 60 → 0 → rebuilding... → 60 ✅`

**Why:** Dramatic demo moment proving the event log is the single source of truth.

---

### 2.3 Temporal Queries (Time Travel)

**Time: ~2 hours**

**What:** Replay the event log up to a specific timestamp to see "what was the state at time T?"

#### [NEW] API: `GET /api/state-at?timestamp=2026-09-05T14:32:05Z`
- Filter operations: `created_at <= timestamp`
- Compute per-node sums from filtered ops
- Return: `{ timestamp, per_node: [{node_id, value}], global }`

#### [MODIFY] `internal/db/db.go`
- `SumAmountsBeforeTimestamp(db, nodeID, timestamp)` query

#### Dashboard
- Timestamp input / slider on Agent Dashboard
- "🔍 Show State at This Time" button
- Display: "At 14:32:05, global count was 112 (Node 0: 45, Node 1: 32, Node 2: 35)"
- Comparison: "Current state: 120 (+8 ops since then)"

**Why:** Event sourcing's superpower. Judges can say "show me the state before the crash" → instant answer.

---

### 2.4 Chaos Experiment Library

**Time: ~2 hours**

**What:** Predefined, scriptable chaos experiments with auto-execution.

#### [NEW] `internal/chaos/experiments.go`
```go
type Experiment struct {
    Name        string `json:"name"`
    Description string `json:"description"`
    Steps       []Step `json:"steps"`
}

type Step struct {
    Action      string `json:"action"`     // "seed", "crash", "reconcile", "replay", "wait"
    NodeID      string `json:"node_id"`
    Count       int    `json:"count"`
    DurationMs  int    `json:"duration_ms"` // for "wait" steps
}
```

#### [NEW] Built-in experiments:
1. **"Partial Loss + Auto-Repair"** — seed → crash → wait 10s → verify auto-repair
2. **"Double Crash"** — crash node-0 → crash node-1 → verify both auto-repair
3. **"Crash During High Load"** — start seeding 100 ops while crashing a node
4. **"Delayed Replay Storm"** — crash → reconcile → replay 3 times → verify no duplicates
5. **"Full Rebuild Test"** — seed → wipe → rebuild → verify identical

#### [NEW] API: `GET /api/experiments` + `POST /api/experiments/run`

#### Dashboard
- Dropdown: select experiment
- "▶ Run Experiment" button
- Step-by-step progress display with pass/fail per step

**Why:** Shows systematic resilience testing, not just "I clicked things randomly."

---

### 2.5 Metrics Dashboard with Charts

**Time: ~3 hours**

**What:** Live charts making correctness visible.

#### Tech: Chart.js (CDN link, no build step needed)

#### Chart 1: Divergence Over Time
- X-axis: last 5 minutes (scrolling window)
- Y-axis: `authoritative - naive`
- Spikes on crash, drops to 0 after reconciliation
- Green line when 0, red when > 0

#### Chart 2: Time-to-Repair
- Bar chart: each crash incident → seconds until divergence returned to 0
- Target line at 10s

#### Chart 3: Operations Per Second
- Real-time throughput line chart
- Shows system continues processing during recovery

#### Data source
- WebSocket events feed chart data points
- EventBus publishes periodic `MetricsSnapshot` events (every 1s)

**Why:** Makes the abstract (correctness) concrete (visual numbers going down).

---

### 2.6 Enhanced Audit Trail

**Time: ~1 hour**

#### Enhancements over basic version:
- **Filters:** by node, by counter_id, by time range
- **Search:** by operation_id (paste UUID)
- **Export:** "📥 Download as JSON" button (last 1000 ops)
- **Grouping:** toggle between flat list and grouped-by-counter view

---

### Phase 2 Exit Criteria
- ✅ Multiple counters tracked and displayed independently
- ✅ Projection rebuild proves event log is source of truth
- ✅ Temporal queries show state at any past timestamp
- ✅ At least 3 predefined chaos experiments runnable from dashboard
- ✅ Charts show divergence spike and recovery in real-time

---

## Phase 3: Polish & Narrative (Hours 14–20)

---

### 3.1 Demo Narrative Script

Write and practice a 5-minute demo flow:

1. "Here's our system running normally — 3 nodes, all healthy, divergence = 0"
2. "We're tracking two counters: `inventory:sku-42` and `rate_limit:user-123`"
3. "Now I'll crash Node 0 — watch the Agent Dashboard"
4. *Don't touch anything*
5. "Sentinel detected the crash within 2 seconds"
6. "Reconciler found 8 missing operations, replaying them now..."
7. "Divergence back to zero — system healed itself"
8. "Now let's replay the same 8 ops — all rejected as duplicates"
9. "Let me time-travel: here's the state 30 seconds before the crash"
10. "And finally — Projection Rebuild: I'll wipe all local state... rebuild from event log... exact same result"
11. "Assertion block: expected == recovered, duplicates_ignored == 8 — PASS"

**Practice 3–4 times.**

---

### 3.2 Future Work Documentation

#### [MODIFY] `README.md` — add "Future Work" section:
- Multi-tenant isolation (tenant_id partitioning)
- Snapshotting for fast replay (hourly checkpoints)
- Dead letter queue for unrepairable operations
- Automated rollback on SLI degradation
- Kafka/Redpanda-backed event log for high throughput
- Prometheus + Grafana observability stack

---

### 3.3 Edge Case Hardening

Test and fix these scenarios:
- **Double crash:** crash node-0, then crash node-1 before node-0 recovers
- **Crash during reconciliation:** node dies mid-repair (idempotent replay handles this)
- **Empty node crash:** node with zero operations
- **High load + crash:** apply 100 ops/sec while crashing nodes
- **All nodes crash:** crash all 3, verify coordinator still has everything

---

### 3.4 Backup Demo Video

- Record 3-minute screen capture of full Scenario A + B running
- Include both dashboard pages side-by-side
- Save locally as fallback if live demo has technical issues

---

## Phase 4: Rest & Rehearsal (Hours 20–24)

- **Stop coding at hour 20** — no new features
- **Sleep 6–8 hours**
- **Rehearse demo 2–3 times** with both pages open
- **Prep environment:** close tabs, kill background processes, test `go run .`

---

## Final Feature Set

| Category | Features |
|---|---|
| **Core Correctness** | 6 invariants, Scenario A+B, automated reconciliation, Sentinel monitoring |
| **Infrastructure** | Event Bus, WebSocket real-time push, two-page dashboard |
| **Differentiators** | Multi-counter, projection rebuild, temporal queries, chaos experiments, Chart.js metrics |
| **Polish** | Demo narrative, future work docs, edge case hardening, backup video |

---

## New Package Map

```
internal/
├── agents/
│   ├── sentinel.go          # Health monitoring agent
│   └── reconciler.go        # Auto-repair agent
├── events/
│   └── bus.go               # Central event bus (pub/sub)
├── chaos/
│   └── experiments.go       # Predefined chaos experiment definitions + executor
├── config/config.go
├── db/db.go                 # + counter_id, temporal query support
├── model/model.go           # + CounterID, Event types
├── node/node.go             # + multi-counter local values
├── server/
│   ├── server.go            # + EventBus wiring, new endpoints
│   └── websocket.go         # WebSocket handler
└── simulation/
    ├── simulation.go        # + per-counter aggregation
    ├── crash.go
    ├── reconcile.go         # + RebuildFromLog
    └── ...tests...
```

---

## Build Order

| Step | Phase | Branch | Est. Time |
|---|---|---|---|
| 1 | Event Bus | `feature/event-bus` | 1 hr |
| 2 | WebSocket | `feature/event-bus` | 45 min |
| 3 | Sentinel Agent | `feature/agents` | 1 hr |
| 4 | Reconciler Agent | `feature/agents` | 1.5 hrs |
| 5 | Two-Page Dashboard | `feature/dashboard-v2` | 2.5 hrs |
| 6 | Audit Trail | `feature/dashboard-v2` | 1 hr |
| 7 | Multi-Counter | `feature/multi-counter` | 3 hrs |
| 8 | Projection Rebuild | `feature/projection-rebuild` | 1 hr |
| 9 | Temporal Queries | `feature/temporal` | 2 hrs |
| 10 | Chaos Experiments | `feature/chaos-library` | 2 hrs |
| 11 | Chart.js Metrics | `feature/metrics` | 3 hrs |
| 12 | Enhanced Audit | `feature/metrics` | 1 hr |
| 13 | Edge Case Tests | `feature/hardening` | 2 hrs |
| 14 | Polish + Merge | `main` | 1.5 hrs |
| | **Total** | | **~23 hrs** |

---

## Open Questions

> [!IMPORTANT]
> 1. **gorilla/websocket vs nhooyr.io/websocket?** gorilla is more common but technically archived. nhooyr is newer. Recommend gorilla for simplicity.
> 2. **Chart.js via CDN or embedded?** CDN needs internet at demo time. Recommend CDN since demo venue will have WiFi, and it keeps the binary small.
> 3. **Agent toggle?** Should the Chaos Panel have a "Manual Mode / Agent Mode" switch so judges can see both manual and autonomous flows?
> 4. **Multi-counter default counters?** Suggest: `inventory:sku-42`, `inventory:sku-43`, `rate_limit:api-gateway` as the 3 demo counters.
