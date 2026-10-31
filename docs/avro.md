# Optional Avro encoding (Phase 5 + Issue #1)

**Status:** Done. JSON remains default. Lab perf: [`perf-harness.md`](perf-harness.md).

JSON remains the **default**. Avro is an optional sink encoding of the same logical payload.

## Config

```toml
[source]
delivery_unit = "row"        # default — one CloudEvent per message after commit
# delivery_unit = "transaction"  # one Delivery at a time: commit, progress, table definition

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
| `json` (default) | `application/json` | `Delivery` JSON | — |
| `avro` | one per delivery, below | Single Avro datum (no OCF) | `x-ce-stream-avro-schema`, below |

| Delivery | HTTP `Content-Type` | Schema header |
|----------|---------------------|---------------|
| `Commit` | `application/ce-stream.committed-transaction+avro` | `ce-stream.committed-transaction.v4` |
| `Progress` | `application/ce-stream.progress+avro` | `ce-stream.progress.v1` |
| `Schema` | `application/ce-stream.table-schema+avro` | `ce-stream.table-schema.v1` |

Versions 1 to 3 of the commit are read, not written.

Stdout + Avro prints **one base64 line per message** (binary on a TTY is hostile).

## Schemas

Published copies (keep in sync):

- [`schemas/cloudevent-v1.avsc`](../schemas/cloudevent-v1.avsc) — row CloudEvents
- [`schemas/committed-transaction-v4.avsc`](../schemas/committed-transaction-v4.avsc) — commits, from every adapter
- [`schemas/progress-v1.avsc`](../schemas/progress-v1.avsc) — a position passed
- [`schemas/table-schema-v1.avsc`](../schemas/table-schema-v1.avsc) — a table's definition
- [`schemas/committed-transaction-v1.avsc`](../schemas/committed-transaction-v1.avsc), [`v2`](../schemas/committed-transaction-v2.avsc), [`v3`](../schemas/committed-transaction-v3.avsc) — read only
- Embedded under [`crates/ce-stream-core/schemas/`](../crates/ce-stream-core/schemas/)

### `ce-stream.cloudevent.v1`

- Envelope fields are Avro strings
- Variable CDC payload stays in `data_json` / `extensions_json` as JSON text

### `ce-stream.committed-transaction.v4`

v3 without the `gtid` fields, with `position` for every adapter. See the schema file for its fields.

## Library

```rust
use ce_stream_core::avro_encode::{encode_cloudevent, encode_delivery};
use ce_stream_core::{HttpSink, SinkFormat};

let bytes = encode_cloudevent(&event)?;
let (delivery_bytes, schema_id, content_type) = encode_delivery(&delivery)?;
let sink = HttpSink::with_format(url, SinkFormat::Avro)?;
sink.post_delivery(&delivery).await?;
```

## Perf

Same harness as JSON (`-Format avro`). Lab (2026-08-02): baseline ~495 eps / ~345 KB (vs JSON ~458 / ~408 KB); choke no drops; sustained 1000/s FAIL keep-up (~741 eps). Details in [`perf-harness.md`](perf-harness.md).
