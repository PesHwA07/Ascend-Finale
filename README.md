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
┌─────────────────────────────────────────────────────────────────────────┐
│                          CounterGhost Cluster                          │
│                                                                         │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐           │
│  │    Node 0       │  │    Node 1       │  │    Node 2       │          │
│  │  ┌───────────┐ │  │  ┌───────────┐ │  │  ┌───────────┐ │          │
│  │  │ Postgres  │ │  │  │ Postgres  │ │  │  │ Postgres  │ │          │
│  │  │ (local)   │ │  │  │ (local)   │ │  │  │ (local)   │ │          │
│  │  │ +outbox   │ │  │  │ +outbox   │ │  │  │ +outbox   │ │          │
│  │  └─────┬─────┘ │  │  └─────┬─────┘ │  │  └─────┬─────┘ │          │
│  └────────┼────────┘  └────────┼────────┘  └────────┼────────┘          │
│           │                    │                    │                    │
│           └────────────────────┼────────────────────┘                    │
│                    atomic tx   │  (op + outbox entry)                    │
│                                │                                         │
│              ┌─────────────────▼──────────────────┐                     │
│              │    Kafka (KRaft — no ZooKeeper)     │                     │
│              │    ┌───────────────────────────┐   │                     │
│              │    │ Topic: counter.operations  │   │                     │
│              │    │ Partitions: 3 │ RF: 3      │   │                     │
│              │    │ acks=all │ idempotent      │   │                     │
│              │    └───────────────────────────┘   │                     │
│              └─────────────────┬──────────────────┘                     │
│                                │                                         │
│           ┌────────────────────┼─────────────────────┐                  │
│           │                    │                     │                  │
│  ┌────────▼────────┐  ┌───────▼────────┐  ┌─────────▼──────────┐      │
│  │   Coordinator    │  │  Audit Store   │  │  Dead Letter Queue │      │
│  │   (Postgres)     │  │  (Postgres)    │  │    (Postgres)      │      │
│  │  operation log   │  │  audit_log     │  │  failed ops → DLQ  │      │
│  │  JSONB manifests │  │  JSONB details │  │  manual inspection │      │
│  └────────┬─────────┘  └────────────────┘  └────────────────────┘      │
│           │                                                              │
│  ┌────────┼──────────────────────────────────┐                          │
│  │        │                                  │                          │
│  │  ┌─────▼──────┐ ┌───────────┐ ┌──────────▼───┐                     │
│  │  │  Sentinel  │ │Reconciler │ │OutboxSyncer  │   ← 3 Autonomous   │
│  │  │  (5s poll) │ │ (10s poll)│ │  (3s poll)   │     Agents          │
│  │  └────────────┘ └───────────┘ └──────────────┘                     │
│  └───────────────────────────────────────────────┘                      │
│                                │                                         │
│              ┌─────────────────▼──────────────────┐                     │
│              │  Event Bus (in-process pub/sub)     │                     │
│              └─────────────────┬──────────────────┘                     │
│                                │                                         │
│              ┌─────────────────▼──────────────────┐                     │
│              │  HTTP Server + WebSocket             │                     │
│              │  13 REST endpoints │ real-time push  │                     │
│              └─────────────────┬──────────────────┘                     │
│                                │ go:embed                                │
│              ┌─────────────────▼──────────────────┐                     │
│              │  Dashboard (HTML/CSS/JS)             │                     │
│              │  Chaos Panel │ Agent Monitor         │                     │
│              └────────────────────────────────────┘                     │
└─────────────────────────────────────────────────────────────────────────┘

External Infrastructure (Docker Compose):
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ Kafka 3.7    │  │ Postgres 16  │  │  Kafka UI    │
│ KRaft mode   │  │  Alpine      │  │  :8090       │
│ :9092        │  │  :5432       │  │  (optional)  │
└──────────────┘  └──────────────┘  └──────────────┘
```

### Data Flow
1. **Apply** → Op + outbox entry written in one atomic Postgres transaction
2. **Outbox Sync** → OutboxSyncer drains outbox to Kafka topic every 3s
3. **Consume** → Coordinator consumes from Kafka, writes to Postgres coordinator DB
4. **Crash** → Node state lost; Kafka retains all operations immutably
5. **Detect** → Sentinel polls health; Reconciler checks `naive ≠ auth`
6. **Repair** → Kafka ops replayed to node via idempotent insert
7. **Audit** → Every mutation logged to `audit_log` table (JSONB details)
8. **DLQ** → Operations failing 5+ syncs moved to dead letter queue
9. **Verify** → `naive == auth` again; all 6 invariants hold

---

## 📁 Project Structure

```
.
├── main.go                              # Entry point, go:embed, agent wiring
├── docker-compose.yml                   # Kafka + Postgres scaffolding [v3]
├── dashboard/
│   ├── index.html                       # Chaos Panel (control center)
│   └── agents.html                      # Agent Dashboard (monitoring) [v2]
├── migrations/
│   └── 001_initial.sql                  # Postgres schema (Phase 2 ready) [v3]
├── internal/
│   ├── config/config.go                 # CLI flag parsing
│   ├── model/model.go                   # Core types: Operation, ReconciliationResult
│   ├── db/                              # Storage layer
│   │   ├── db.go                        # SQLite CRUD, schema, temporal queries
│   │   ├── audit.go                     # Audit log table + CRUD [v3]
│   │   ├── outbox.go                    # Transactional outbox pattern [v3]
│   │   └── dlq.go                       # Dead letter queue [v3]
│   ├── events/bus.go                    # Event bus: pub/sub + event types [v2]
│   ├── agents/                          # Autonomous agents [v2+]
│   │   ├── sentinel.go                  # Health monitor (5s poll) [v2]
│   │   ├── reconciler.go               # Auto-reconciliation (10s poll) [v2]
│   │   └── outbox_syncer.go            # Outbox → coordinator sync (3s poll) [v3]
│   ├── node/
│   │   ├── node.go                      # Node engine: transactional outbox ApplyDelta
│   │   └── node_test.go                 # 12 node-level tests
│   ├── server/
│   │   ├── server.go                    # HTTP server, 13 API endpoints + audit middleware
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
├── counterghost-agent-architecture.md   # Phase 2 architecture deep dive
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

### Production Endpoints (v3)
| Method | Path | Description |
|---|---|---|
| `GET` | `/api/audit` | **Audit log**: query all state-mutating actions (filter by `action`, `resource_id`, `limit`) |
| `GET` | `/api/dlq` | **Dead letter queue**: view operations that failed outbox sync 5+ times |
| `GET` | `/api/outbox` | **Outbox stats**: sync status per node (or all nodes if no `node_id` param) |

### Example: Temporal Query
```bash
# What was the global counter 30 seconds ago?
GET /api/temporal?as_of=-30s

# What was the global counter at a specific time?
GET /api/temporal?as_of=2024-09-06T01:00:00Z
```

### Example: Audit & DLQ
```bash
# Show all crash actions
GET /api/audit?action=crash&limit=10

# Show dead letter queue
GET /api/dlq

# Show outbox sync stats for node-0
GET /api/outbox?node_id=node-0
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

## 🔒 Production Patterns

### Transactional Outbox
Every `ApplyDelta` writes **both** the operation and an outbox entry in a **single atomic transaction**. The OutboxSyncer agent asynchronously drains outbox entries to Kafka, guaranteeing exactly-once delivery even if the broker is temporarily unreachable.

```
Node Postgres Transaction:
  ┌─────────────────────────────────┐
  │ INSERT INTO operations ...      │ ← counter operation
  │ INSERT INTO operation_outbox ...│ ← outbox entry
  │ [COMMIT]                        │ ← both succeed or both fail
  └─────────────────────────────────┘
       │
       ▼ (async, every 3s)
  OutboxSyncer → Kafka → Coordinator Postgres
```

### Kafka Event Log
Operations flow through a Kafka topic (`counter.operations`) with:
- **KRaft mode** — no ZooKeeper dependency
- **3 partitions** — parallel consumption per node
- **`acks=all`** — no data loss, every write acknowledged by all replicas
- **Idempotent producer** — exactly-once semantics

### Audit Trail
Every state-mutating API call (`apply`, `crash`, `reconcile`, `rebuild`) is recorded in the `audit_log` table with timestamp, action, resource, and JSONB details. Query via `GET /api/audit`.

### Dead Letter Queue
Operations that fail outbox sync 5+ times are moved to the `dead_letter_queue` table for manual inspection instead of being retried forever. Query via `GET /api/dlq`.

### Postgres Schema Design
Production-grade schema with:
- **TIMESTAMPTZ** for all temporal fields (not TEXT)
- **JSONB** columns for manifests and audit details (indexed, queryable)
- **Partial indexes** on outbox (`WHERE synced = FALSE`) — only scan unsynced rows
- **GIN indexes** on JSONB fields for fast detail queries
- **Unique constraints** on `(node_id, epoch, sequence)` — database-level dedup

```bash
# Start the full production stack
docker-compose up -d

# Services:
#   Kafka     → localhost:9092
#   Postgres  → localhost:5432 (counterghost/counterghost_dev)
#   Kafka UI  → localhost:8090 (visual topic inspector)
```

---

## 🚀 Scaling Architecture

### Service Mesh (Istio)
| Feature | What It Solves |
|---|---|
| **mTLS** | Encrypt all service-to-service communication |
| **Circuit Breakers** | Prevent cascading failures when coordinator is overloaded |
| **Retries with Backoff** | Centralized retry policy across all services |
| **Distributed Tracing** | End-to-end latency visibility (Node → Kafka → Postgres) |
| **Rate Limiting** | Per-service quotas to prevent overload |

### Horizontal Scaling
| Feature | What It Enables |
|---|---|
| **Sharded Coordinators** | Consistent hashing: node → coordinator shard |
| **Multi-Region** | us-east-1, us-west-2, eu-west-1 with cross-region Kafka |
| **Prometheus + Grafana** | `counterghost_operations_total`, divergence gauge, outbox depth |
| **Multi-Tenant Isolation** | `tenant_id` partitioning across all tables |
| **Snapshotting** | Hourly checkpoints for fast replay (skip full log scan) |

---

## 📜 License

Built for the Ascend Hackathon.


