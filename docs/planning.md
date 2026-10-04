# ce-stream — implementation plan

**Status:** Phases 0–5 done (MySQL 9.x JSON + Avro, E2E, harden, perf). **Phase 6 un-deferred for MongoDB only** ([#5](https://github.com/AxialDB/ce-stream/issues/5), 2026-10-01); other engines stay deferred. Lean OSS MVP landed in-repo — see [`oss-readiness.md`](oss-readiness.md). Maintained by [AxialDB](https://axialdb.com/) ([releases](https://github.com/AxialDB/releases)).

**Product:** Database change logs → **CloudEvents 1.0** (include-list, resume, Kafka optional). Not a warehouse ELT tool; not Debezium/Kafka Connect.

**Constraints (locked for v1):**

- Language: **Rust** (if crate spike fails on MySQL 9.x → **stop and discuss**; no silent Go/Java pivot)
- MySQL: **9.x only** (local lab / eval instance on port 3306)
- Capture: **ROW binlog** via sidecar client (prefer replica); not in-mysqld plugins for v1
- Capture concurrency: **single ordered reader** per source (one dump stream / GTID order). Do not parallelize binlog read. Optional sink worker pool + backpressure = Phase 4.
- Delivery: **library + CLI** (CLI is a thin host; embed in client process is supported). Ops default = sidecar/CLI. One `server_id` per dump client.
- Interchange: CloudEvents 1.0 **structured JSON** (default) with **full before/after** row images; optional `sink.format=avro` (Phase 5)
- Resume: **GTID** (confirmed ON on lab instance)
- Transport: **TLS** required for spike gate

**Lab instance (2026-08-01 check):** `log_bin=ON`, `binlog_format=ROW`, `binlog_row_image=FULL`, `gtid_mode=ON`, `enforce_gtid_consistency=ON`.

---

## Phase 0 — Spec freeze (docs)

- CloudEvent `type` / `source` / `subject` / `data` / extension attrs (`gtid`, `file`, `pos`, …)
- Include-list semantics (client-side filter; full binlog still exists on server)
- Checkpoint format (adapter + JSON payload)
- Sink contracts: `stdout`, `http` (more later)
- Explicit non-goals: Kafka Connect, warehouse load, MySQL &lt; 9, triggers

**Exit:** This doc + README agree; no code required beyond scaffold.

---

## Phase 1 — Binlog spike (gate for Rust)

Spike against **local MySQL 9.x** (lab or eval instance):

| Check | Pass criteria |
|-------|----------------|
| Connect as replica client | Auth + optional TLS |
| ROW events | INSERT/UPDATE/DELETE decoded for a test table |
| GTID resume | Kill/restart; no silent gap (or documented at-least-once) |
| Include-list | Unrelated tables ignored quickly |
| Throughput smoke | Catch-up on modest traffic without mysqld collapse |

**Candidate crates:** `mysql-binlog-connector-rust`, `mysql_cdc` (pick one or thin-wrap).

**Exit:** Short spike note in `docs/spike-mysql-binlog.md` (what worked, gaps owned by us). If both crates fail hard on 9.x → **stop and discuss with owner** (Rust-only policy).

**Result (2026-08-01):** TLS + ROW I/U/D + GTID + include-list **PASS** on MySQL 9.7 via git `mysql-binlog-connector-rust` + `rustls`. **Blocked without workaround:** empty `Latest` / auto-fetch uses removed `SHOW MASTER STATUS`. Decision: patch/vendor to **`SHOW BINARY LOG STATUS` only** (9.x; no older-version fallback) — **not** a language pivot.

**Patched (2026-08-01):** vendored under `vendor/mysql-binlog-connector-rust` (`PATCHES.md`). `Latest` verified. GTID-only workaround no longer required for spike.

**Do not** build sinks/product polish until Phase 2 MVP exit is met (done below).

---

## Phase 2 — MySQL source MVP

Wire `ce-stream-mysql`:

1. Map ROW events → `ce_stream_core::CloudEvent`
2. `FileCheckpointStore` (GTID / file+pos)
3. Include-list from `ce-stream.toml`
4. `ce-stream` CLI: load config → run source → `StdoutSink`

**Exit:** `cargo run -p ce-stream-cli -- --config …` prints CloudEvents for included tables; resume after restart.

**Result (2026-08-01):** MVP exit met on lab — stdout CloudEvents for `ce_stream_spike.t1`, file checkpoint GTID under `.ce-stream/`. Column names are `col_N` until server `binlog_row_metadata=FULL`.

---

## Phase 3 — Sinks + ops (E2E path)

- Prefer real column names: document / require `binlog_row_metadata=FULL` on capture source
- HTTP sink (POST CloudEvents JSON)
- Health / lag logging (`tracing`)
- Config validation + clear errors
- systemd / Docker example (optional)
- Basic integration test (testcontainers or scripted local MySQL 9.x)

**Exit:** Documented "replica + include-list + HTTP webhook" happy path; scripted E2E green.

**Result (2026-08-02):** HTTP `HttpSink` (`application/cloudevents+json`), config validation, `ce_stream::health` logs, [`docs/ops-e2e.md`](ops-e2e.md), [`docs/library.md`](library.md), `scripts/e2e-http.ps1`, `deploy/ce-stream.service`, `embed_callback` example.

---

## Phase 4 — Hardening

- At-least-once vs exactly-once honesty in docs
- Backpressure if sink slow (bounded queue; slow the reader — do not drop)
- Signal-only mode (no before/after images) for low-impact consumers
- Impact notes: capture on replica preferred; measure primary if not

**Exit:** v0.1 tag suitable for early OSS users (JSON CloudEvents E2E complete).

**Result (2026-08-02):** Checkpoint advances only after sink Ok; bounded `queue_capacity` backpressure; `payload_mode=full|signal`; [`docs/delivery.md`](delivery.md); prod continuous runbook in [`docs/ops-e2e.md`](ops-e2e.md); health includes `lag_ms`.

---

## Phase 4.5 — Performance harness (before Avro)

Location: [`scripts/perf/`](../scripts/perf/). Spec: [`docs/perf-harness.md`](perf-harness.md).

**Scenarios:** frequent changes (baseline), choking sink, burst catch-up, signal vs full, include-list noise, restart mid-stream.

**Order of work:** mock HTTP sink → loadgen + runner → baseline → choke (no drops) → burst/signal → restart smoke → comparison worksheet.

**Comparison:** measure our events/sec, lag, catch-up, choke behavior on lab MySQL 9.x. Optional side-by-side with Debezium on the **same** loadgen when available; Fivetran mostly qualitative / impact notes (see perf doc). Not a marketing gate.

**Exit:** Documented how to run; JSON summary artifacts; baseline + choke + sustained green on lab.

**Lab status (2026-08-02):** JSON baseline ~458 eps; choke no drops; sustained keep-up PASS at 300/600, FAIL at 1200 (ceiling ~600-650). Avro baseline ~495 eps (~15% fewer bytes); choke no drops; sustained 1000/s FAIL keep-up (second-half ~741 eps). See perf-harness.md.

---

## Phase 5 — Avro encoding (after E2E + perf harness)

**Only after** Phases 3–4 E2E (JSON CloudEvents) is implemented and tested.

- Optional sink / encoder: same logical CloudEvent → Avro binary (schema story TBD)
- Does not change capture model or replace JSON as the default interchange
- Schema Registry (if any) is part of this phase's design, not earlier

**Exit:** Documented optional Avro path; JSON remains default.

**Result (2026-08-02):** Implemented — `sink.format = json|avro`, schema [`schemas/cloudevent-v1.avsc`](../schemas/cloudevent-v1.avsc), docs [`avro.md`](avro.md). No Schema Registry yet. Perf runners accept `-Format avro`. Lab Avro numbers in [`perf-harness.md`](perf-harness.md).

---

## Phase 6 — Multi-DB adapters — **MongoDB shipped in v0.4.0; others deferred**

Same `ChangeSource` / CloudEvent envelope:

| Adapter | Mechanism | Status |
|---------|-----------|--------|
| `ce-stream-mongo` | Change streams, MongoDB 8.0+ replica set | **Shipped** in v0.4.0 ([#5](https://github.com/AxialDB/ce-stream/issues/5), [`issues/5.md`](issues/5.md)) |
| `ce-stream-postgres` | Logical decoding / replication protocol (sketch) | Deferred |
| `ce-stream-sqlite` | update hooks / session (different shape, sketch) | Deferred |
| MSSQL | Prefer consuming **CES** if available; not a binlog clone (sketch) | Deferred |

**Exit (MongoDB):** `ce-stream-mongo` behind the same CLI `source.adapter = "mongo"`, released as v0.4.0. MySQL output unchanged.

**Decision (2026-08-02):** Park Phase 6. v1 stays **MySQL 9.x only**. Traits may remain adapter-shaped so a future second engine can plug in without redesign, but no Postgres/SQLite/MSSQL work until explicitly pulled back.

**Decision (2026-10-01):** Pull MongoDB back from Phase 6 ([#5](https://github.com/AxialDB/ce-stream/issues/5)). AxialDB needs MongoDB CDC without Kafka or Debezium, and change streams fit the existing `ChangeSource` and commit-boundary model. Scope: MongoDB 8.0+ replica sets, no sharded clusters. Postgres, SQLite, and MSSQL stay deferred.

---

## Out of scope (until explicitly pulled in)

- Other DB engines besides MongoDB (Phase 6 — deferred)
- Competing with Fivetran on quietness as a product claim (measure; don't market until proven)
- In-process MySQL transmit/trans observers as primary capture
- AxialDB coupling (optional subscriber later; separate repo)
- Parallel multi-dump on one binlog for "more throughput"
- Schema Registry / typed per-table Avro (optional later)

---

## Suggested order

```text
Phase 0 → 1 (spike) → 2 (MVP) → 3 (HTTP E2E) → 4 (harden)
  → 4.5 (perf harness) → 5 (Avro)  [done]
  → OSS readiness / v0.1 tag
  → v0.2.0 (#1 Gate 0) → v0.3.0 (#3 live include list)
  → Phase 6: MongoDB (#5, v0.4.0); other DBs only if un-deferred
```

**Now:** tag v0.4.0 after merge, then publish to crates.io in the order in [`releasing.md`](releasing.md#cratesio). Postgres, SQLite, and MSSQL stay deferred.
