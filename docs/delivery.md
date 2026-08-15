# Delivery semantics (Phase 4 + Issue #1)

**Status:** Commit-boundary delivery + `delivery_unit` (v0.2.0). At-least-once + backpressure + `payload_mode`. Encoding: JSON default; optional Avro ([`avro.md`](avro.md)). Other DB engines deferred ([`planning.md`](planning.md)).

ce-stream is **at-least-once**, not exactly-once.

## Commit-boundary emit (v0.2.0)

Regardless of `delivery_unit`, the MySQL adapter **buffers** row and DDL events until **XID**. Consumers see **nothing mid-transaction**.

After XID, delivery shape depends on `source.delivery_unit`:

| `delivery_unit` | After XID | Checkpoint |
|-----------------|-----------|------------|
| `row` (default) | M DDL CloudEvents + N row CloudEvents, in order | After all M+N sink acks |
| `transaction` | One `CommittedTransaction` envelope | After one sink ack |

If the process crashes **after** some post-XID deliveries but **before** checkpoint, the **entire commit** is redelivered on restart (duplicates acceptable).

## What “issued” means

1. Sink callback / HTTP POST returns **Ok** (HTTP 2xx).
2. **Then** GTID checkpoint is written to disk (`.ce-stream/checkpoint.json`).

If the process crashes after a successful HTTP POST but before the checkpoint flush, the same GTID may be delivered again after restart. Consumers should be **idempotent** (dedupe on business key and/or CloudEvent `id` / `gtid`).

## What we do not claim

- Exactly-once end-to-end delivery
- Global order after a fan-out bus (Kafka/NATS/…)
- Zero duplicates across crashes

## Backpressure

The binlog reader pushes into a **bounded** queue (`source.queue_capacity`, default 64). When the sink is slow, `blocking_send` stalls the reader (does **not** drop events). MySQL may see the dump client slow down; prefer a replica and size the webhook accordingly.

## Signal vs full payload

| `payload_mode` | `data` contents |
|----------------|-----------------|
| `full` (default) | `op` + `before` / `after` images |
| `signal` | `{ "op": "...", "signal": true }` only |

Signal mode reduces payload size and sink cost; you lose row images.

## Encoding (JSON vs Avro)

Default sink encoding is **JSON**. Set `sink.format = "avro"` for optional binary Avro (same logical payload).

| `delivery_unit` | `format=json` | `format=avro` |
|-----------------|---------------|---------------|
| `row` | CloudEvents structured JSON per row | `ce-stream.cloudevent.v1` per row |
| `transaction` | One JSON envelope per commit | `ce-stream.committed-transaction.v1` per commit |

See [`avro.md`](avro.md). Checkpoint and delivery semantics are unchanged.
