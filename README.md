# 👻 CounterGhost

**Distributed Counter Integrity After Partial Node State Loss**

CounterGhost demonstrates how a sharded counter system detects, recovers, and proves correctness after a node crashes and silently loses part of its local state — a problem that makes naive aggregation silently wrong.

---

## 🎯 The Problem

A counter is sharded across multiple nodes. Each node tracks its own local count. The global count is the sum of all locals. **What happens when a node crashes and loses some (not all) of its operations?**

- **Naive systems report the wrong total** — and nobody knows it's wrong
- The loss **looks identical to a legitimate reset** from the outside
- Pre-crash operations that arrive late could **double-count** if replayed carelessly

CounterGhost solves all three problems with a **durable operation log**, **epoch-tracked node incarnations**, and a **two-phase reconciliation engine**.

---

## ⚡ Quick Start

### Prerequisites
| Requirement | Version | Notes |
|---|---|---|
| Go | 1.21+ | `go version` to check |
| Browser | Any modern | For the live dashboard |

### Build & Run
```bash
git clone https://github.com/PesHwA07/Ascend-Finale.git
cd Ascend-Finale
go mod tidy
go build -o counterghost .
./counterghost -reset -nodes 3 -seed 42
```

Dashboard opens at **http://localhost:8080**

### Run Tests (Correctness Verification)
```bash
go test ./... -v
```

All 6 invariants and both demo scenarios pass as automated `go test` cases.

---

## 🏗️ Architecture

Single Go binary. No external services. No Docker required.

```
┌──────────────────────────────────────────────────────┐
│                    Go Binary                          │
│                                                       │
│  ┌─────────────────────────────────────────────────┐  │
│  │ Simulation Core                                  │  │
│  │  • 3 nodes (each with own SQLite file)          │  │
│  │  • UUID-identified operations (durable log)      │  │
│  │  • Seeded crash injector (deterministic)         │  │
│  │  • Epoch manager (tracks node incarnations)      │  │
│  └─────────────────────────────────────────────────┘  │
│                        │                               │
│  ┌─────────────────────────────────────────────────┐  │
│  │ Reconciliation Engine                            │  │
│  │  • Sync-up: node → coordinator (fix dual-write)  │  │
│  │  • Repair: coordinator → node (replay missing)   │  │
│  │  • Dedup: INSERT OR IGNORE (UUID constraint)     │  │
│  └─────────────────────────────────────────────────┘  │
│                        │                               │
│  ┌─────────────────────────────────────────────────┐  │
│  │ Invariant Checker (6 machine-checked assertions) │  │
│  └─────────────────────────────────────────────────┘  │
│                        │                               │
│  ┌─────────────────────────────────────────────────┐  │
│  │ Embedded Dashboard (go:embed, no build step)     │  │
│  └─────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
.
├── main.go                          # Entry point, flags, go:embed
├── dashboard/
│   └── index.html                   # Full interactive dashboard (embedded)
├── internal/
│   ├── config/config.go             # CLI flag parsing
│   ├── model/model.go               # Core types: Operation, Manifest, ReconciliationResult
│   ├── db/db.go                     # SQLite CRUD, schema, pool tuning
│   ├── node/
│   │   ├── node.go                  # Node engine: ApplyDelta, epochs, local state
│   │   └── node_test.go             # 12 node-level tests
│   ├── server/server.go             # HTTP server, 7 API endpoints
│   └── simulation/
│       ├── simulation.go            # Simulation manager, dual aggregation
│       ├── simulation_test.go       # Aggregation + lookup tests
│       ├── crash.go                 # Crash injection, manifest, delayed replay
│       ├── crash_test.go            # 7 crash/manifest tests
│       ├── reconcile.go             # Two-phase reconciliation engine
│       ├── reconcile_test.go        # 5 reconciliation tests
│       └── invariant_test.go        # 6 invariants + Scenario A+B end-to-end
├── CounterGhost_PRD.md              # Full product requirements document
└── go.mod / go.sum                  # Dependencies (pure Go SQLite, no CGo)
```

---

## 🔧 Configuration

| Flag | Default | Purpose |
|---|---|---|
| `-port` | `8080` | Dashboard HTTP port |
| `-nodes` | `3` | Number of simulated nodes |
| `-seed` | `42` | RNG seed for deterministic crash injection |
| `-db-dir` | `./data` | Where per-node SQLite files are stored |
| `-reset` | `false` | Wipe data directory on startup |

---

## 🌐 API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/health` | Health check |
| `GET` | `/api/state` | All node states + naive/authoritative globals |
| `POST` | `/api/apply` | Apply N operations to a node |
| `POST` | `/api/crash` | Simulate partial state loss on a node |
| `GET` | `/api/manifest` | Compute a node's manifest (shows gaps) |
| `POST` | `/api/reconcile` | Detect and repair missing operations |
| `POST` | `/api/replay` | Simulate delayed re-delivery (dedup test) |

---

## 🎬 Demo Scenarios

### Scenario A — Crash & Reconcile
1. **Seed** 3 nodes with operations → global = 120
2. **Crash** node-0, deleting 8 operations → naive = 112 (WRONG), auth = 120 (correct)
3. **Reconcile** → replays 8 missing ops → naive = auth = 120 ✅

### Scenario B — Delayed Replay (No Double-Count)
1. After reconciliation, **re-deliver** the same 8 crashed operations
2. System **rejects all 8** as duplicates (UUID dedup via `INSERT OR IGNORE`)
3. Global count stays at 120, `duplicates_ignored = 8` ✅

### Assertion Block Output
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

## ✅ Correctness Invariants (PRD §7)

All 6 invariants are machine-checked as `go test` cases:

| # | Invariant | What It Checks |
|---|---|---|
| **I1** | No duplicate contribution | `count(operation_id) ≤ 1` for every op |
| **I2** | No acknowledged op disappears | Coordinator retains all ops after crash |
| **I3** | Recovery is monotonic | Double reconcile doesn't inflate count |
| **I4** | Epochs are unique per node | Crash always increments: 1 < 2 < 3 |
| **I5** | Global count reproducible | naive == auth == recomputed from log |
| **I6** | Reconciliation idempotent | `Reconcile(Reconcile(s)) == Reconcile(s)` |

---

## 🧠 Key Design Decisions

| Decision | Rationale |
|---|---|
| **One SQLite file per node** | No write contention between nodes; each node owns its file |
| **Manifests are derived, not stored** | Computed on-demand from operation log; no drift risk |
| **UUID operation IDs** | Generated once, never regenerated; enables dedup at storage layer |
| **Seeded RNG for crash injection** | Same seed = same crash = deterministic, reproducible demos |
| **Two-phase reconciliation** | Sync-up (node→coord) + repair (coord→node) handles all failure modes |
| **Pure Go SQLite (`modernc.org/sqlite`)** | No CGo, no C compiler needed — builds anywhere Go runs |

---

## 📊 Test Suite

```
29 tests total — all passing

internal/node          12 tests   Node engine, apply, epoch, state management
internal/simulation    17 tests   Crash, manifest, reconciliation, invariants, scenarios
```

Run with verbose output:
```bash
go test ./... -v -count=1
```

---

## 🛠️ Tech Stack

- **Language:** Go 1.21+
- **Storage:** SQLite (via `modernc.org/sqlite` — pure Go, no CGo)
- **Frontend:** Vanilla HTML/CSS/JS (embedded via `go:embed`)
- **Fonts:** Inter + JetBrains Mono (Google Fonts)
- **Dependencies:** Zero external services

---

## 📜 License

Built for the Ascend Hackathon.
