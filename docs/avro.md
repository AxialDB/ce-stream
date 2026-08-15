# Optional Avro encoding (Phase 5 + Issue #1)

**Status:** Done. JSON remains default. Lab perf: [`perf-harness.md`](perf-harness.md).

JSON remains the **default**. Avro is an optional sink encoding of the same logical payload.

## Config

```toml
[source]
delivery_unit = "row"        # default — one CloudEvent per message after commit
# delivery_unit = "transaction"  # one CommittedTransaction envelope per commit

[sink]
kind = "http"   # or stdout
url = "http://127.0.0.1:18080/events"
format = "avro" # default: json
```

### Row mode (`delivery_unit = row`)

| `format` | HTTP `Content-Type` | Body | Schema header |
|----------|---------------------|------|---------------|
| `json` (default) | `application/cloudevents+json` | Structured-mode JSON | — |
| `avro` | `application/cloudevents+avro` | Single Avro datum (no OCF) | `x-ce-stream-avro-schema: ce-stream.cloudevent.v1` |

### Transaction mode (`delivery_unit = transaction`)

| `format` | HTTP `Content-Type` | Body | Schema header |
|----------|---------------------|------|---------------|
| `json` (default) | `application/json` | `CommittedTransaction` JSON | — |
| `avro` | `application/ce-stream.committed-transaction+avro` | Single Avro datum (no OCF) | `x-ce-stream-avro-schema: ce-stream.committed-transaction.v1` |

Stdout + Avro prints **one base64 line per message** (binary on a TTY is hostile).

## Schemas

Published copies (keep in sync):

- [`schemas/cloudevent-v1.avsc`](../schemas/cloudevent-v1.avsc) — row CloudEvents
- [`schemas/committed-transaction-v1.avsc`](../schemas/committed-transaction-v1.avsc) — transaction envelopes
- Embedded under [`crates/ce-stream-core/schemas/`](../crates/ce-stream-core/schemas/)

### `ce-stream.cloudevent.v1`

- Envelope fields are Avro strings
- Variable CDC payload stays in `data_json` / `extensions_json` as JSON text

### `ce-stream.committed-transaction.v1`

Field order matches Rust `CommittedTransaction`:

1. `gtid` — commit GTID
2. `gtid_set_after` — executed set after commit (checkpoint watermark)
3. `ddl` — array of `{ schema, query }` (DDL in binlog order)
4. `events` — array of nested `CloudEvent` records (row changes)

## Library

```rust
use ce_stream_core::avro_encode::{
    decode_cloudevent, decode_committed_transaction,
    encode_cloudevent, encode_committed_transaction,
};
use ce_stream_core::{HttpSink, SinkFormat};

let bytes = encode_cloudevent(&event)?;
let txn_bytes = encode_committed_transaction(&commit)?;
let sink = HttpSink::with_format(url, SinkFormat::Avro)?;
sink.post_committed_transaction(&commit).await?;
```

## Perf

Same harness as JSON (`-Format avro`). Lab (2026-08-02): baseline ~495 eps / ~345 KB (vs JSON ~458 / ~408 KB); choke no drops; sustained 1000/s FAIL keep-up (~741 eps). Details in [`perf-harness.md`](perf-harness.md).
