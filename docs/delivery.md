# Delivery semantics (Phase 4 + Issue #1)

**Status:** Commit-boundary delivery + `delivery_unit` (v0.2.0). At-least-once + backpressure + `payload_mode`. Encoding: JSON default; optional Avro ([`avro.md`](avro.md)). Values: [`values.md`](values.md). Other DB engines deferred ([`planning.md`](planning.md)).

ce-stream is **at-least-once**, not exactly-once.

## What a source delivers

Every source delivers one thing at a time ([#19](https://github.com/AxialDB/ce-stream/issues/19)). In order with each other:

| Delivery | What it is | Moves the checkpoint |
|----------|------------|----------------------|
| `Commit` | One committed transaction, or one part of it | The transaction or its last part |
| `Progress` | The stream passed a position with nothing in it for this consumer | Yes |
| `Schema` | The definition of a table. It may come between two parts of a transaction | No |

**`Progress`** comes from a commit with nothing for the include list, and from a position the source learns on its own: Mongo's idle resume token, PostgreSQL's keepalive when no transaction is open and everything delivered is confirmed, and a PostgreSQL message of the consumer's (non-transactional, under one of its `message_prefixes`).

**`Schema`** comes before a table's first commit on a session. MySQL delivers it again when the columns change, since its table map repeats in every transaction and signals nothing. PostgreSQL delivers it every time the server announces the table, identical or not. MongoDB has no schema and delivers none.

**Controls** are a transaction's lifecycle events, read before its rows, and go with its last part: `Dropped`, `Renamed`, `Truncated`, `DefinitionChanged`, `DatabaseDropped`, `Invalidated`, and `Message` (a consumer's transactional message, under one of its `message_prefixes`). The consumer decides what stops its own work.

On PostgreSQL, `Invalidated` means an update or delete arrived without the old row, since the table left `REPLICA IDENTITY FULL`. The row is not delivered and the stream goes on for the other tables. A drop or a rename sends nothing on that stream.

## Commit-boundary emit (v0.2.0)

Regardless of `delivery_unit`, an adapter **buffers** row and DDL events until the transaction **ends**. Consumers see **nothing mid-transaction**. A transaction larger than one part is delivered in parts ([#16](https://github.com/AxialDB/ce-stream/issues/16)), when it has committed on MySQL and PostgreSQL.

Where a transaction ends (v0.4.3, [#13](https://github.com/AxialDB/ce-stream/issues/13)). The server writes a transaction in one of two shapes:

| Shape in the binlog | Ends at | Delivered as |
|---------------------|---------|--------------|
| `BEGIN`, rows, `Xid` (InnoDB) | the `Xid` event | the rows |
| `BEGIN`, rows, `COMMIT` query (a table without transactions, such as MyISAM) | the `COMMIT` | the rows |
| `BEGIN`, `CREATE TABLE ... START TRANSACTION`, rows, `Xid` (`CREATE TABLE ... SELECT`) | the `Xid` event | the DDL statement and the rows |
| One `Query` event: a DDL statement (`ALTER TABLE`, `TRUNCATE TABLE`, `DROP TABLE`, `CREATE DATABASE`, ...) | that event | the DDL statement, no rows |
| One `Query` event: any other statement (`GRANT`, `ANALYZE TABLE`) | that event | a `Progress`; its GTID enters the executed set |
| `XA START`, rows, `XA END`, `XA PREPARE`, later `XA COMMIT` | the `XA COMMIT` | the rows, at that position |
| `XA START`, rows, `XA END`, `XA COMMIT ... ONE PHASE` | that commit | the rows |

**XA.** A transaction's rows are delivered when it commits. The rows are kept in memory from `XA PREPARE` and dropped at `XA ROLLBACK`. The prepared GTID stays out of the executed set until its `XA COMMIT` is delivered, so a restart between the two makes the server send the prepared transaction again; nothing is stored on disk. An `XA COMMIT` whose rows were never read (prepared before capture first started, or its prepare purged) stops the stream with an error, since they cannot be recovered. An XA transaction is held whole until it ends, since a part cannot be taken back.

PostgreSQL does this by itself: a prepared transaction is decoded at `COMMIT PREPARED`.

Not delivered: a transaction the binlog ends with `ROLLBACK`. The stream goes on past it.

After the end of the transaction ("XID" below), delivery shape depends on `source.delivery_unit`:

| `delivery_unit` | After XID | Checkpoint |
|-----------------|-----------|------------|
| `row` (default) | M DDL CloudEvents + N row CloudEvents, in order | After all M+N sink acks |
| `transaction` | One `Delivery::Commit` | After one sink ack |

If the process crashes **after** some post-XID deliveries but **before** checkpoint, the **entire commit** is redelivered on restart (duplicates acceptable).

## What “issued” means

1. Sink callback / HTTP POST returns **Ok** (HTTP 2xx).
2. **Then** the checkpoint is written (`.ce-stream/checkpoint.json`), to a temporary file and renamed, so a crash leaves the old one or the new one.

With `checkpoint = "on_confirm"` step 2 waits for the consumer's `Confirm::confirm` instead ([`library.md`](library.md#confirming-what-is-durable)).

If the process crashes after a successful HTTP POST but before the checkpoint, the same commit may be delivered again after restart. Consumers should be **idempotent** (dedupe on business key and/or CloudEvent `id`, `gtid`, or `lsn`).

**PostgreSQL's slot.** The server forgets what lies behind the slot, so the slot moves only to a checkpoint that has reached the disk, at most once a second. It never moves past what a restart would read. A checkpoint behind its slot is `HistoryLost`, as is a missing or invalidated slot, and one from another system or timeline is its own error.

## What we do not claim

- Exactly-once end-to-end delivery
- Global order after a fan-out bus (Kafka/NATS/…)
- Zero duplicates across crashes

## Backpressure

An adapter's reader pushes into a **bounded** queue (`source.queue_capacity`, default 64, and `source.queue_max_bytes`, default 32 MB). When the sink is slow, the reader stalls (it does **not** drop events). MySQL may see the dump client slow down; prefer a replica and size the webhook accordingly. On PostgreSQL a task apart from the reader keeps the connection alive, sending a status update at a quarter of `wal_sender_timeout`, at most 10 seconds apart, whatever the sink does.

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
| `transaction` | One JSON `Delivery` | `ce-stream.committed-transaction.v4`, `ce-stream.progress.v1`, `ce-stream.table-schema.v1` |

See [`avro.md`](avro.md). Checkpoint and delivery semantics are unchanged.

## Live include list (v0.3.0)

Row events are filtered by a cloneable [`IncludeList`](library.md#live-include-list) snapshotted at the start of each transaction (MySQL: its GTID; PostgreSQL: its Begin). Changing the list never splits a transaction. A fully filtered commit is a `Progress`, so the position still advances. MySQL DDL Query events are not filtered. There is no ack; the next transaction uses the new list.

A PostgreSQL table whose partition parent is on the list passes as well; each row says its root ([`library.md`](library.md#postgresql)).
