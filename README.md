# 👻 CounterGhost

**Distributed Counter Integrity After Partial Node State Loss**

CounterGhost demonstrates how a sharded counter system detects, recovers, and **proves correctness** after a node crashes and silently loses part of its local state — a problem that makes naive aggregation silently wrong.

Built as a single Go binary. No external services. No Docker. No databases to install.

[![Go](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go&logoColor=white)](#) [![SQLite](https://img.shields.io/badge/SQLite-Pure_Go-003B57?style=flat&logo=sqlite&logoColor=white)](#) [![License](https://img.shields.io/badge/Ascend-Hackathon-blueviolet?style=flat)](#)

---

## 🎯 The Problem

A counter is sharded across multiple nodes. Each node tracks its own local count. The **global count = sum of all locals**. What happens when a node crashes and loses some (not all) of its operations?

| Failure Mode | What Goes Wrong |
|---|---|
| **Silent data loss** | Naive global sum reports the wrong total — and nobody knows |
| **Phantom reset** | The loss looks identical to a legitimate counter reset |
| **Duplicate replay** | Pre-crash operations that arrive late could double-count |
| **No divergence signal** | No way to tell if `auth ≠ naive` without a second source of truth |

CounterGhost solves all four with a **durable operation log**, **epoch-tracked node incarnations**, a **two-phase reconciliation engine**, and **autonomous self-healing agents**.

---

## ⚡ Quick Start

### Prerequisites
| Requirement | Version | Check |
|---|---|---|
| Go | 1.21+ | `go version` |
| Browser | Any modern | For the live dashboard |

### Build & Run
```bash
git clone https://github.com/PesHwA07/Ascend-Finale.git
cd Ascend-Finale
go mod tidy
go build -o counterghost.exe .      # Windows
# go build -o counterghost .        # Linux/Mac
./counterghost.exe -reset
```

Dashboard opens at **http://localhost:8080**

### Run Tests
```bash
go test ./... -v -count=1
```

---

## 📋 Version History

### v1.0 — Core Correctness Engine
The foundation: prove that a distributed counter can detect crash-induced data loss, repair itself, and reject duplicate replays.

| Feature | Description |
|---|---|
| **Node Simulation** | 3 SQLite-backed nodes with independent operation logs |
| **Dual-Write Architecture** | Every op writes to both node DB + coordinator DB |
| **Crash Injection** | Seeded RNG deletes random ops from a node's local DB |
| **Epoch Management** | Each crash increments the node's epoch (generation counter) |
| **Two-Phase Reconciliation** | Sync-up (node→coord) + Repair (coord→node) |
| **Delayed Replay / Dedup** | `INSERT OR IGNORE` on UUID rejects duplicate deliveries |
| **6 Machine-Checked Invariants** | All run as `go test` cases (see Invariants section) |
| **Interactive Dashboard** | Single-page Chaos Panel with Scenario A/B buttons |
| **7 REST API Endpoints** | Full programmatic control of the simulation |

### v2.0 — Autonomous Agents & Observability
Added self-healing agents, real-time event streaming, and advanced correctness features that go beyond basic crash-recover.

| Feature | Description |
|---|---|
| **Event Bus** | Thread-safe in-process pub/sub for all system events |
| **WebSocket Push** | Real-time event streaming to dashboard (no polling needed) |
| **Sentinel Agent** | Background goroutine that polls node health every 5s, publishes `node_down`/`node_up` events |
| **Reconciler Agent** | Background goroutine that checks for divergence every 10s, auto-triggers reconciliation |
| **Two-Page Dashboard** | Chaos Panel (control) + Agent Dashboard (monitoring) |
| **System Architecture Flowchart** | Live topology showing nodes → coordinator → agents |
| **Projection Rebuild** | Nuclear option: wipe node DB completely, replay ALL ops from coordinator log |
| **Temporal Queries** | "Time-travel" — query the global counter at any past timestamp |
| **Audit Trail API** | List all coordinator operations for transparency |
| **Chaos Experiments** | 3 scripted multi-step experiments with pass/fail assertions |
| **Chart.js Divergence Chart** | Live 3-line chart (naive, auth, divergence) with rolling 60-point window |
| **3 New API Endpoints** | `POST /api/rebuild`, `GET /api/operations`, `GET /api/temporal` |

### v3.0 — Production-Grade Patterns _(current)_
Added production-ready data pipeline patterns and infrastructure scaffolding for Kafka + Postgres migration.

| Feature | Description |
|---|---|
| **Transactional Outbox** | Atomic op + outbox write in one SQLite transaction; OutboxSyncer drains async |
| **OutboxSyncer Agent** | 3rd autonomous agent — syncs outbox → coordinator every 3s |
| **Audit Logging** | Every `apply`, `crash`, `reconcile`, `rebuild` action recorded with timestamp + details |
| **Dead Letter Queue** | Outbox entries failing 5+ times moved to DLQ for manual inspection |
| **3 New API Endpoints** | `GET /api/audit`, `GET /api/dlq`, `GET /api/outbox` |
| **Docker Compose** | Kafka (KRaft) + Postgres 16 + Kafka UI scaffolding for Phase 2 migration |
| **Postgres Schema** | Production-grade schema with JSONB, partial indexes, GIN indexes |

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                         Go Binary                                │
│                                                                  │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐                    │
│  │  node-0  │   │  node-1  │   │  node-2  │   ← Per-node       │
│  │ (SQLite) │   │ (SQLite) │   │ (SQLite) │     SQLite files    │
│  └────┬─────┘   └────┬─────┘   └────┬─────┘                    │
│       │              │              │                            │
│       └──────────────┼──────────────┘                            │
│                      │ dual-write                                │
│              ┌───────▼────────┐                                  │
│              │ Coordinator DB │  ← Source of truth (SQLite)      │
│              │ (operation log)│                                   │
│              └───────┬────────┘                                  │
│                      │                                           │
│       ┌──────────────┼──────────────┐                            │
│       │              │              │                            │
│  ┌────▼─────┐  ┌─────▼─────┐  ┌────▼──────┐                    │
│  │ Sentinel │  │Reconciler │  │ Event Bus │  ← v2.0 agents     │
│  │ (5s poll)│  │ (10s poll)│  │ (pub/sub) │                     │
│  └──────────┘  └───────────┘  └─────┬─────┘                    │
│                                     │                            │
│              ┌──────────────────────▼──────────────┐            │
│              │ HTTP Server + WebSocket              │            │
│              │ ┌──────────┐  ┌───────────────────┐ │            │
│              │ │ REST API │  │ WebSocket Push    │ │            │
│              │ │ 10 endpts│  │ Real-time events  │ │            │
│              │ └──────────┘  └───────────────────┘ │            │
│              └──────────────────────┬──────────────┘            │
│                                     │ go:embed                   │
│              ┌──────────────────────▼──────────────┐            │
│              │ Dashboard (HTML/CSS/JS)              │            │
│              │ ┌────────────┐  ┌─────────────────┐ │            │
│              │ │Chaos Panel │  │Agent Dashboard  │ │            │
│              │ │ (control)  │  │ (monitoring)    │ │            │
│              │ └────────────┘  └─────────────────┘ │            │
│              └─────────────────────────────────────┘            │
└──────────────────────────────────────────────────────────────────┘
```

### Data Flow
1. **Apply** → Op gets a UUID, writes to both node DB and coordinator DB
2. **Crash** → Random ops deleted from node DB; coordinator retains all
3. **Detect** → Sentinel polls health; Reconciler checks `naive ≠ auth`
4. **Repair** → Coordinator ops replayed to node via `INSERT OR IGNORE`
5. **Verify** → `naive == auth` again; invariants hold

---

## 📁 Project Structure

```
.
├── main.go                              # Entry point, go:embed, agent wiring
├── dashboard/
│   ├── index.html                       # Chaos Panel (control center)
│   └── agents.html                      # Agent Dashboard (monitoring) [v2]
├── internal/
│   ├── config/config.go                 # CLI flag parsing
│   ├── model/model.go                   # Core types: Operation, ReconciliationResult
│   ├── db/db.go                         # SQLite CRUD, schema, temporal queries
│   ├── events/bus.go                    # Event bus: pub/sub + event types [v2]
│   ├── agents/                          # Autonomous agents [v2]
│   │   ├── sentinel.go                  # Health monitor (node_down/node_up)
│   │   └── reconciler.go               # Auto-reconciliation on divergence
│   ├── node/
│   │   ├── node.go                      # Node engine: ApplyDelta, epochs, state
│   │   └── node_test.go                 # 12 node-level tests
│   ├── server/
│   │   ├── server.go                    # HTTP server, 10 API endpoints
│   │   └── websocket.go                # WebSocket hub + broadcast [v2]
│   └── simulation/
│       ├── simulation.go                # Simulation manager, dual aggregation
│       ├── crash.go                     # Crash injection, manifest, delayed replay
│       ├── reconcile.go                 # Two-phase reconciliation engine
│       ├── rebuild.go                   # Projection rebuild from coordinator log [v2]
│       ├── temporal.go                  # Time-travel temporal queries [v2]
│       ├── simulation_test.go           # Aggregation + lookup tests
│       ├── crash_test.go                # 7 crash/manifest tests
│       ├── reconcile_test.go            # 5 reconciliation tests
│       └── invariant_test.go            # 6 invariants + Scenario A+B
├── CounterGhost_PRD.md                  # Product requirements document
├── go.mod / go.sum                      # Dependencies (pure Go SQLite)
└── README.md                            # This file
```

---

## 🔧 Configuration

| Flag | Default | Purpose |
|---|---|---|
| `-port` | `8080` | Dashboard HTTP port |
| `-nodes` | `3` | Number of simulated nodes |
| `-seed` | `42` | RNG seed for deterministic crash injection |
| `-db-dir` | `./data` | Where per-node SQLite files are stored |
| `-reset` | `false` | **Wipe all data** and start fresh on startup |

---

## 🌐 API Reference

### Core Endpoints (v1)
| Method | Path | Description |
|---|---|---|
| `GET` | `/api/health` | Health check |
| `GET` | `/api/state` | All node states + naive/authoritative globals |
| `POST` | `/api/apply` | Apply N increment operations to a node |
| `POST` | `/api/crash` | Simulate partial state loss on a node |
| `GET` | `/api/manifest` | Compute a node's operation manifest (shows gaps) |
| `POST` | `/api/reconcile` | Detect and repair missing operations |
| `POST` | `/api/replay` | Simulate delayed re-delivery (dedup test) |

### Advanced Endpoints (v2)
| Method | Path | Description |
|---|---|---|
| `POST` | `/api/rebuild` | **Projection rebuild**: wipe node DB, replay ALL from coordinator |
| `GET` | `/api/operations` | **Audit trail**: list all coordinator operations |
| `GET` | `/api/temporal?as_of=` | **Time-travel**: query global counter at any past timestamp |
| `WS` | `/ws` | **WebSocket**: real-time event stream to dashboard |

### Example: Temporal Query
```bash
# What was the global counter 30 seconds ago?
GET /api/temporal?as_of=-30s

# What was the global counter at a specific time?
GET /api/temporal?as_of=2024-09-06T01:00:00Z
```

---

## 🎬 Demo Scenarios

### Scenario A — Crash & Reconcile
1. **Seed** 3 nodes with 40 ops each → global = 120
2. **Crash** node-0, deleting 8 ops → naive = 112 (WRONG), auth = 120 (correct)
3. **Reconcile** → replays 8 missing ops → naive = auth = 120 ✅

### Scenario B — Delayed Replay (Idempotency Proof)
1. After reconciliation, **re-deliver** the same 8 crashed operations
2. System **rejects all 8** as duplicates (`INSERT OR IGNORE`)
3. Global count stays at 120, `new_inserts = 0` ✅

### Chaos Experiments (v2)

| Experiment | What It Proves |
|---|---|
| **🌪 Triple Crash** | Crash ALL 3 nodes → reconcile all → all converge to auth value |
| **🔄 Rebuild Proof** | Seed → crash → full projection rebuild from coordinator → exact value match |
| **⏰ Time Travel** | Seed → snapshot timestamp → add more ops → temporal query returns old value |

---

## ✅ Correctness Invariants

All 6 invariants are machine-checked as `go test` cases:

| # | Invariant | What It Checks |
|---|---|---|
| **I1** | No duplicate contribution | `count(operation_id) ≤ 1` for every op in every DB |
| **I2** | No acknowledged op disappears | Coordinator retains 100% of ops after any crash |
| **I3** | Recovery is monotonic | Double reconcile doesn't inflate count |
| **I4** | Epochs are unique per node | Crash always increments epoch: 1 → 2 → 3 |
| **I5** | Global count reproducible | `naive == auth == recomputed from log` |
| **I6** | Reconciliation idempotent | `Reconcile(Reconcile(s)) == Reconcile(s)` |

```
========================================
  COUNTERGHOST ASSERTION BLOCK
========================================
  expected_global_count        = 120
  recovered_global_count       = 120
  duplicate_operations_ignored = 8
  ASSERT expected == recovered   -> PASS
  ASSERT post_replay == expected -> PASS
  ASSERT dups_ignored == deleted -> PASS
========================================
```

---

## 🧠 Key Design Decisions

| Decision | Rationale |
|---|---|
| **One SQLite file per node** | No write contention between nodes; each node owns its file exclusively |
| **Coordinator as source of truth** | The event log is always complete; local DBs are projections of it |
| **Manifests are derived, not stored** | Computed on-demand from operation log to avoid drift risk |
| **UUID operation IDs** | `{node_id}-epoch{N}-seq{M}` — generated once, never regenerated; enables dedup at storage layer |
| **Seeded RNG for crash injection** | Same seed = same crash = deterministic, reproducible demos |
| **Two-phase reconciliation** | Sync-up (node→coord) + Repair (coord→node) handles all failure modes |
| **Event Bus (v2)** | Decouples event producers from consumers; agents and WebSocket subscribe independently |
| **Autonomous agents (v2)** | Sentinel detects, Reconciler heals — no manual intervention needed |
| **Projection rebuild (v2)** | Proves event log is a complete history; can reconstruct any node from scratch |
| **Pure Go SQLite** | `modernc.org/sqlite` — no CGo, no C compiler — builds anywhere Go runs |

---

## 📊 Test Suite

```
29+ tests total — all passing

internal/node          12 tests   Node engine, apply, epoch, state management
internal/simulation    17 tests   Crash, manifest, reconciliation, invariants, scenarios
```

Run with verbose output:
```bash
go test ./... -v -count=1
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| **Language** | Go 1.21+ |
| **Storage** | SQLite via `modernc.org/sqlite` (pure Go, no CGo) |
| **WebSocket** | `gorilla/websocket` |
| **Frontend** | Vanilla HTML/CSS/JS (embedded via `go:embed`) |
| **Charting** | Chart.js 4.4.4 (CDN) |
| **Fonts** | Inter + JetBrains Mono (Google Fonts) |
| **Dependencies** | Zero external services — single binary |

---

## 🗺️ Dashboard Pages

### Page 1: Chaos Panel (`http://localhost:8080`)
The **control center** — inject faults, run scenarios, and observe the system's response.
- Global counter cards (naive vs auth vs drift)
- Per-node cards with Seed, Crash, Reconcile, Replay, Rebuild buttons
- Demo Scenarios (A & B) with one-click execution
- Chaos Experiments (Triple Crash, Rebuild Proof, Time Travel)
- Real-time event log

### Page 2: Agent Dashboard (`http://localhost:8080/agents.html`)
The **monitoring view** — watch autonomous agents detect and repair faults in real-time.
- System architecture flowchart (nodes → coordinator → agents)
- Live divergence panel with peak tracking
- Chart.js line chart (naive, auth, divergence over time)
- Agent activity log with SENTINEL/RECONCILER/OUTBOX_SYNCER badges
- Invariant verification panel (I1–I6)

---

## 🔒 v3 Production Patterns (current)

### Transactional Outbox
Every `ApplyDelta` now writes **both** the operation and an outbox entry in a **single atomic SQLite transaction**. The OutboxSyncer agent asynchronously drains outbox entries to the coordinator, guaranteeing eventual consistency even if the coordinator is temporarily unreachable.

```
Node DB Transaction:
  ┌─────────────────────────────────┐
  │ INSERT INTO operations ...      │ ← counter operation
  │ INSERT INTO operation_outbox ...│ ← outbox entry
  │ [COMMIT]                        │ ← both succeed or both fail
  └─────────────────────────────────┘
       │
       ▼ (async, every 3s)
  OutboxSyncer → Coordinator DB
```

### Audit Trail
Every state-mutating API call (`apply`, `crash`, `reconcile`, `rebuild`) is recorded in the `audit_log` table with timestamp, action, resource, and details. Query via `GET /api/audit`.

### Dead Letter Queue
Operations that fail outbox sync 5+ times are moved to the `dead_letter_queue` table for manual inspection instead of being retried forever. Query via `GET /api/dlq`.

### New v3 API Endpoints
| Method | Path | Description |
|---|---|---|
| `GET` | `/api/audit` | Query audit trail (filter by action, resource_id) |
| `GET` | `/api/dlq` | View dead letter queue entries |
| `GET` | `/api/outbox` | View outbox sync stats per node |

---

## 🚀 Production Roadmap

### Phase 2: Kafka + Postgres (Docker Compose ready)
> **Status**: Scaffolding complete (`docker-compose.yml` + `migrations/001_initial.sql`)

| Component | What | Why |
|---|---|---|
| **Kafka** (KRaft) | Replace coordinator SQLite with Kafka topic | Multi-consumer, replayability, `acks=all` durability |
| **Postgres** 16 | Replace per-node SQLite files | Concurrent writers (MVCC), PITR, JSONB manifests |
| **Storage Interface** | `OperationStore` interface in Go | Swap SQLite ↔ Postgres without changing business logic |

```bash
# Start the production stack
docker-compose up -d

# Kafka UI at http://localhost:8090
# Postgres at localhost:5432 (counterghost/counterghost_dev)
```

### Phase 3: Service Mesh (Istio)
| Feature | What It Solves |
|---|---|
| **mTLS** | Encrypt all service-to-service communication |
| **Circuit Breakers** | Prevent cascading failures when coordinator is overloaded |
| **Retries with Backoff** | Centralized retry policy across all services |
| **Distributed Tracing** | End-to-end latency visibility (Node → Coordinator → Postgres) |
| **Rate Limiting** | Per-service quotas to prevent overload |

### Phase 4: Horizontal Scaling
| Feature | What It Enables |
|---|---|
| **Sharded Coordinators** | Consistent hashing: node → coordinator shard |
| **Multi-Region** | us-east-1, us-west-2, eu-west-1 with cross-region Kafka |
| **Prometheus + Grafana** | `counterghost_operations_total`, divergence gauge, outbox depth |

---

## 📜 License

Built for the Ascend Hackathon.

