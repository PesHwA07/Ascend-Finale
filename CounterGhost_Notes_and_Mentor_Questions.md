# CounterGhost — Tech Stack Justification Notes & Mentor Questions

---

## PART 1: Tech Stack Decision Notes (for notebook / documentation)

Format for each decision: **What we chose → What we rejected → Why → Performance/trade-off honesty.**
Use this section to defend choices if judges ask "why didn't you use X."

---

### 1.1 Language: Go (not Python, not a Go+Python split)

**Chosen:** Go, single language for the entire codebase.

**Rejected:** Python-only, and a Go+Python split (Go for logic, Python for reporting).

**Why:**
- The problem is fundamentally about **concurrent, independent node behavior racing against crash/replay
  events**. Go's goroutines + channels model this natively — each simulated node is a real concurrently
  running unit, not a scripted illusion of concurrency.
- Static typing catches structural bugs (e.g., epoch as `int64` vs accidentally treated as `string`)
  at compile time — valuable because this project's entire value proposition is *correctness*, so
  compile-time safety directly serves the goal, not just convenience.
- A Go+Python split was considered (Go for core, Python for reporting) but rejected because it adds a
  **serialization seam** (JSON boundary) with no correctness benefit for a 24-hour build — every hour
  spent maintaining a schema contract between two languages is an hour not spent hardening the
  reconciliation logic itself.
- Single static binary (`go build`) means **zero deployment risk during live judging** — no venv, no
  dependency resolution, no "it worked on my machine."

**Performance honesty:**
- Go's goroutines are lightweight (KBs of stack, not OS threads) — we can simulate many nodes cheaply,
  which is irrelevant at demo scale (3–5 nodes) but is a legitimate answer if asked "does this scale."
- Python would have been **faster to prototype** the reconciliation algorithm in (less boilerplate,
  no explicit error handling per call) — this is a real cost we accepted deliberately, trading iteration
  speed for compile-time safety and demo-day reliability.
- We are **not** claiming Go is "faster" in a way that matters here — raw runtime performance is not the
  bottleneck for a hackathon-scale demo; the actual justification is concurrency model fit +
  single-binary deployment risk reduction, and we should say that plainly if asked, not oversell a
  performance narrative that isn't the real reason.

---

### 1.2 Storage: SQLite (not Postgres, not pure JSONL, not in-memory-only)

**Chosen:** SQLite, one file per simulated node.

**Rejected:** PostgreSQL (external service), pure JSONL files, in-memory + periodic snapshot only.

**Why:**
- We need **real unique constraints and transactional writes** to make "duplicate operation rejected"
  a storage-level guarantee, not just an application-level `if` check that could have a bug. SQLite gives
  us `PRIMARY KEY`/`UNIQUE` enforcement and ACID transactions with zero external service.
- Pure JSONL would require us to **hand-roll uniqueness checking** (scan-and-compare) — more code,
  more surface for the exact kind of subtle bug this project is supposed to prevent.
- In-memory-only would defeat the premise entirely — the whole scenario requires a **crash to actually
  lose data that was durably recorded**, which needs real durability to simulate honestly, not fake it.
- SQLite runs embedded in the binary — **no separate service to start, configure, or fail during a live
  demo.** This was the deciding factor over Postgres: Postgres gives us nothing SQLite doesn't for this
  scale, but adds a service dependency that can break during judging.

**Performance honesty:**
- SQLite serializes writes per file (one writer at a time per database file). This is **a real
  limitation**, not a strength — we mitigate it by giving each simulated node its **own SQLite file**,
  so nodes don't contend with each other; only concurrent writers *within* a single node's goroutine need
  to be serialized, which we handle by funneling all writes for a node through one writer path.
- This would **not** be an adequate choice for a real production sharded counter service at real
  throughput — we should say so directly if asked. It is the right choice for *proving the correctness
  algorithm* at demo scale, not for a claim about production readiness.

---

### 1.3 Delivery/messaging: Direct function calls + Go channels (not Kafka/Redpanda)

**Chosen:** In-process event simulation — operations, crashes, and delayed replays are triggered via
direct function calls and channel sends within the same binary.

**Rejected:** Kafka or Redpanda as the message transport.

**Why:**
- A real broker's entire job is to **guarantee delivery** — but our demo's entire job is to **deliberately
  violate delivery guarantees on command** (drop this, delay that, replay this exact one twice). Fighting
  a reliable broker to make it act unreliably on a precise schedule is strictly harder than simulating it
  directly, for no benefit to the correctness proof.
- Removes an entire service dependency (broker process, topic setup, consumer group coordination) from
  the live-demo risk surface.

**Performance/positioning honesty:**
- This is explicitly flagged in our PRD as **Future Goal P1** — we are not claiming this generalizes to
  production message delivery, only that the *reconciliation and invariant logic* we're proving would
  compose with a real broker later. We should be upfront that the MVP intentionally isolates the
  algorithm from transport-layer concerns to prove it cleanly first.

---

### 1.4 Live view: WebSocket + `go:embed` dashboard (not React/TypeScript, not Grafana)

**Chosen:** A minimal HTML/JS dashboard embedded directly into the Go binary via `go:embed`, updated
live over a WebSocket connection.

**Rejected:** A React/TypeScript frontend with a separate build pipeline; Prometheus + Grafana.

**Why:**
- `go:embed` means the dashboard ships **inside the single binary** — no `npm install`, no build step,
  no second process to keep alive during judging.
- We only need to display a handful of live-updating numbers and an event log — this does not need a
  component framework's complexity budget.
- Grafana/Prometheus are built for **long-running observability of a live production system** — our
  "observation window" is a few minutes of a live-triggered demo scenario, which a lightweight custom
  view serves better and more legibly for an audience than a general-purpose metrics dashboard would.

**Performance/positioning honesty:**
- This is the **weakest-engineered part of the stack by design** — it's presentation, not the graded
  correctness core, and we deliberately spent the least engineering rigor here. If asked "why does the
  UI look basic," the honest answer is: we prioritized proving correctness over building a polished
  frontend, and that trade-off was intentional given 24 hours.

---

### 1.5 Docker & cloud hosting (prepared in advance as a backup, not the live-demo runtime)

**Chosen:** Direct binary execution as the primary/live-demo path; a Docker image built and rehearsed
in advance purely as insurance if asked "could you deploy this?"

**Rejected as the live-demo runtime:** Running the demo itself through Docker.

**Why:** Judging is confirmed in-person, live on the presenter's laptop — for the *judged run itself*,
Docker adds failure surface (daemon uptime, image build time, port/volume mapping) with no
corresponding benefit, since SQLite is embedded and there's no broker to isolate. But "could this be
deployed" is a very likely mentor/judge question for a distributed-systems project, so we built and
tested the Dockerfile ahead of time rather than answering the question hypothetically — it's a
multi-stage build (`golang:alpine` → `distroless/static`), fully static since the SQLite driver is
pure Go and CGO is disabled.

**Performance/positioning honesty:** the two decisions aren't in tension — using the binary directly
for the live demo minimizes judged-run risk, while having the container already built and verified
means we can answer a deployability question with a live `docker run` on the spot instead of a
"yes, theoretically" answer. Cloud hosting itself is still deferred (Section 9 equivalent), since
nothing about in-person judging requires a public URL.

### 1.6 Summary table for quick reference

| Decision | Chosen | Rejected | Core reason |
|---|---|---|---|
| Language | Go (single) | Python; Go+Python split | Concurrency fit + zero cross-language seam in correctness-critical code |
| Storage | SQLite | Postgres; JSONL; in-memory only | Real constraints/transactions with zero external service |
| Messaging | In-process simulation | Kafka/Redpanda | Need to violate delivery guarantees on command, not rely on them |
| Live view | go:embed + WebSocket | React/TS; Grafana | Zero build pipeline, zero extra process, sufficient for the actual need |

---

## PART 2: Questions to Ask Mentors

Organize these by category so you can pick the most valuable ones if time with a mentor is short.
Lead with the **correctness-design questions** — those are the highest-value use of an expert's time,
since the tech-stack questions are mostly already justified above.

### A. Correctness & algorithm design (highest priority — ask these first)

1. **"Is epoch-per-restart alone a sufficient fencing mechanism, or are there known failure modes
   (e.g., epoch rollback, clock skew equivalents) we should guard against even in a simulated
   environment?"** — We want to know if there's a classic pitfall in epoch-based fencing that our
   simple "increment on restart" approach might miss.

2. **"Our loss-vs-reset detection relies on comparing processed-ranges/manifests against an
   authoritative durable log. Is there a known failure case where this comparison itself can be
   ambiguous — e.g., can a node lose the *manifest* itself and not just the operations?"** — We want to
   pressure-test whether our detection mechanism has a blind spot at the meta-level (losing the evidence
   of evidence).

3. **"Is `Reconcile(Reconcile(state)) == Reconcile(state)` (idempotent reconciliation) sufficient to
   claim correctness, or should we be proving a stronger property (e.g., convergence regardless of the
   order multiple reconciliation triggers fire in a real concurrent system)?"** — This tests whether our
   invariant list (Section 6 of PRD) is actually complete or just necessary-but-not-sufficient.

4. **"Would you recommend property-based/fuzz testing (e.g., randomized crash timing and randomized
   partial-loss patterns) as a stronger correctness argument than our two seeded scenarios, given our
   time constraints?"** — We want an expert's view on whether two deterministic scenarios are convincing
   enough or whether we should spend remaining time on broader randomized coverage instead of polish.

### B. Architecture & trade-offs (ask if time remains)

5. **"Given we chose SQLite with one file per node to avoid write contention — is there a lighter-weight
   alternative you'd recommend for this exact use case (simulate durability + crash-induced partial
   loss) that we haven't considered?"** — Open invitation for an alternative we might not know about.

6. **"Is there a real-world system (Kafka Streams state stores, DynamoDB, Stripe's idempotency keys,
   etc.) whose actual production design we should benchmark our approach against, to strengthen our
   demo narrative?"** — Grounds our design in a known industry precedent if one maps closely.

### C. Presentation & judging strategy (ask closer to demo prep)

7. **"From a judging perspective, is showing the *naive failure first* (wrong number on screen) before
   the fix more convincing than just showing the correct end state — or does that risk looking like we
   built the bug on purpose?"** — Tactical framing question about how to present the demo narrative.

8. **"If we run short on time, is a `go test`-verified correctness proof with a plain CLI output more
   valuable to show than a partially-finished live dashboard?"** — Direct ask for a mentor's prioritization
   opinion under real time pressure, since this is exactly the trade-off flagged in our PRD's risk section.

### D. Stretch-goal sanity check (only if MVP is done early)

9. **"If we have spare time for one Future Goal (see PRD Section 8), which would most strengthen the
   project's credibility to a technical judge: a Kafka-backed variant, randomized chaos testing, or
   extending to multi-field invariants (closer to the WriteFork-style problem)?"** — Useful to get an
   outside opinion on which stretch goal has the best effort-to-credibility ratio, since we may only have
   time for one.

---

### How to use this during the mentor session
- Open with your 30-second problem summary, then go straight to **Section A, question 2** — it's the
  single most likely place a real gap could exist in the design, and getting it addressed early gives
  you the most time to react if the mentor flags something.
- Keep Section D for the very end, and only if the mentor explicitly asks "what's next" or you've
  finished the MVP walkthrough with time to spare.
