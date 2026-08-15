# Using ce-stream as a library

**Status:** Supported. MySQL 9.x only for v1; other adapters deferred.

The CLI is a thin host. Embed capture in your own process with the same single ordered reader.

## Crates

- `ce-stream-core` - `CloudEvent`, `CommittedTransaction`, `ChangeSource`, `Sink`, `CheckpointStore`
- `ce-stream-mysql` - `MysqlBinlogSource`, `FileCheckpointStore`, `validate_capture_gates`

## Minimal callback (row mode, in-process push)

```rust
use ce_stream_core::source::{ChangeSource, DeliveryUnit, SourceConfig};
use ce_stream_core::event::TableRef;
use ce_stream_mysql::{MysqlBinlogSource, MysqlSourceOptions};

// inside an async fn on a tokio runtime:
let mut source = MysqlBinlogSource {
    options: MysqlSourceOptions {
        host: "127.0.0.1".into(),
        port: 3306,
        user: "ce_stream".into(),
        password: "...".into(),
        server_id: 19001,
        tls: true,
    },
    config: SourceConfig {
        source_id: "mysql://127.0.0.1:3306/app".into(),
        include_tables: vec![TableRef::new("demo_perf", "orders")],
        payload_mode: Default::default(),
        queue_capacity: 64,
        delivery_unit: DeliveryUnit::Row, // default
    },
    checkpoint: None,
    checkpoint_store: None, // or Some(Box::new(FileCheckpointStore { ... }))
    skip_gate_check: false,
};

source
    .run(|ev| {
        // your pub/sub, queue, or business logic
        println!("{}", serde_json::to_string(&ev)?);
        Ok(())
    })
    .await?;
```

## Transaction mode (AxialDB default)

Use `delivery_unit: DeliveryUnit::Transaction` and `run_transactions`:

```rust
use ce_stream_core::source::{ChangeSource, DeliveryUnit, SourceConfig};

source.config.delivery_unit = DeliveryUnit::Transaction;

source
    .run_transactions(|txn| {
        // one envelope per commit: txn.gtid, txn.ddl, txn.events
        Ok(())
    })
    .await?;
```

Row mode and transaction mode share the same commit-boundary buffering — nothing is emitted before XID.

## Capture gates

Before reading the binlog, ce-stream validates MySQL settings required for safe ROW capture:

- `binlog_format=ROW`
- `binlog_row_image=FULL`
- `binlog_row_metadata=FULL`
- `gtid_mode=ON`
- `enforce_gtid_consistency=ON` (warn if off)

Call explicitly or rely on the default check in `run` / `run_transactions`:

```rust
use ce_stream_mysql::validate_capture_gates;

let report = validate_capture_gates(&source.options.connection_url()).await?;
for w in report.warnings {
    tracing::warn!("gate: {w}");
}
```

Set `skip_gate_check: true` on `MysqlBinlogSource` only for lab environments.

## Rules

- One dump client / unique `server_id` per process (do not start two readers with the same id).
- Capture stays single-threaded and ordered; fan-out in your callback or downstream bus.
- Prefer TLS; prefer reading a replica; set `binlog_row_metadata=FULL` for column names.
- Optional Avro: `HttpSink::with_format(url, SinkFormat::Avro)` or encode via `ce_stream_core::avro_encode` — see [`avro.md`](avro.md).

See also [`ops-e2e.md`](ops-e2e.md) for the HTTP sidecar path and [`delivery.md`](delivery.md) for commit-boundary semantics.
