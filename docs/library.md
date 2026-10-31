# Using ce-stream as a library

**Status:** Supported. MySQL 9.x, MongoDB 8.0+, and PostgreSQL 17+ ([#19](https://github.com/AxialDB/ce-stream/issues/19), not released). Other adapters are deferred.

The CLI is a thin host. Embed capture in your own process with the same single ordered reader. The examples here compile: `cargo test --doc -p ce-stream-cli`.

## Crates

- `ce-stream-core` - `Delivery`, `CloudEvent`, `ChangeSource`, `Confirm`, `SeedCut`, `GateReport`, `IncludeList`, `Sink`, `CheckpointStore`, `FileCheckpointStore`
- `ce-stream-mysql` - `MysqlBinlogSource`, `MysqlSeedCut`
- `ce-stream-mongo` - `MongoChangeStreamSource` for a MongoDB 8.0+ replica set
- `ce-stream-postgres` - `PostgresSource`, `PgSeedCut`, `create_slot`, `drop_slot`, `slot_figures`, `catalog_tables`
- `ce-stream-cli` - the `ce-stream` binary (`cargo install ce-stream-cli`)

## Row mode

One callback per row, for a sink. A table definition is a CloudEvent of its own (`io.ce-stream.schema`). A position passed is not delivered in this mode; it still moves the checkpoint.

```rust,no_run
use ce_stream_core::event::TableRef;
use ce_stream_core::source::{ChangeSource, DeliveryUnit, SourceConfig};
use ce_stream_core::{FileCheckpointStore, IncludeList};
use ce_stream_mysql::{new_confirm, MysqlBinlogSource, MysqlSourceOptions};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
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
        include_tables: vec![TableRef::new("demo", "orders")],
        delivery_unit: DeliveryUnit::Row,
        ..Default::default()
    },
    checkpoint: None,
    checkpoint_store: Some(Box::new(FileCheckpointStore {
        path: ".ce-stream/checkpoint.json".into(),
    })),
    skip_gate_check: false,
    include: IncludeList::new(), // seeded from include_tables at run() unless mutated
    confirm: new_confirm(),
};

source
    .run(|event| {
        println!("{}", event.subject);
        Ok(())
    })
    .await?;
# Ok(())
# }
```

## Transaction mode

One `Delivery` at a time, whatever the source:

```rust,no_run
use ce_stream_core::delivery::Delivery;
use ce_stream_core::source::ChangeSource;

# async fn example<S: ChangeSource>(mut source: S) -> ce_stream_core::error::Result<()> {
source
    .run_deliveries(|delivery| {
        match delivery {
            // One commit, or one part of it.
            Delivery::Commit(txn) => println!("{} rows", txn.events.len()),
            // The stream passed a position with nothing in it for you.
            Delivery::Progress { position } => println!("at {}", position.after),
            // A table's definition. It moves nothing.
            Delivery::Schema(schema) => println!("{}", schema.subject.as_subject()),
        }
        Ok(())
    })
    .await
# }
```

Nothing is delivered before its transaction commits, in either mode. See [`delivery.md`](delivery.md).

## Positions

An embedder compares positions without knowing the host: `source.covers(at, target)` is true when a stream that has delivered everything up to `at` has delivered everything up to `target`. `at` is a position you received: a commit's, or a `Progress`. A part that is not the last has no position after it, and answers `Err`.

| Source | `at` | `after` |
|--------|------|---------|
| MySQL | `{ "gtid" }` | `{ "gtid": "<executed set>" }` |
| MongoDB | `{ "cluster_time", ... }` | `{ "resume_token", "cluster_time" }` |
| PostgreSQL | `{ "lsn", "xid" }`; a mark: `{ "lsn", "prefix", "content" }` | `{ "lsn" }` |

## Confirming what is durable

By default the callback's `Ok` is the confirmation. A consumer that makes a delivery durable after its callback has returned sets `SourceConfig::checkpoint` to `OnConfirm`, and calls `confirm` when it has:

```rust,no_run
use ce_stream_core::delivery::Delivery;
use ce_stream_core::source::ChangeSource;

# async fn example<S: ChangeSource>(mut source: S) -> ce_stream_core::error::Result<()> {
let confirm = source.confirm();
source
    .run_deliveries(move |delivery| {
        if let Delivery::Progress { position } = &delivery {
            // ... once it is on your disk:
            confirm.confirm(position)?;
        }
        Ok(())
    })
    .await
# }
```

A position behind the last confirmed one is a no-op. A part that is not the last cannot be confirmed.

## Seeding

A `SeedCut` is built from a connection you supply to the primary, under your own account, never the stream's. The copy is yours; ce-stream never reads source rows.

```rust,no_run
use ce_stream_core::delivery::SeedCut;
use ce_stream_postgres::{PgSeedCut, PostgresOptions};

# async fn example(primary: PostgresOptions) -> ce_stream_core::error::Result<()> {
// The prefix must be among the stream's message_prefixes.
let cut = PgSeedCut::new(primary, "my_marks");
let mark = cut.mark().await?;
cut.wait_settled(&mark).await?;
// A snapshot taken now contains every transaction committed at or before the
// mark. Copy under it, then apply the stream from the mark on.
# Ok(())
# }
```

`MysqlSeedCut` reads `gtid_executed`, and its `wait_settled` returns at once, since a GTID set names finished transactions.

## PostgreSQL

The slot is yours to make and drop. A missing or invalidated slot is `Error::HistoryLost`, never a quiet new slot, and so is a checkpoint behind the slot.

```rust,no_run
use ce_stream_core::source::{ChangeSource, DeliveryUnit, SourceConfig};
use ce_stream_core::{delivery::CheckpointMode, event::TableRef};
use ce_stream_postgres::{
    create_slot, new_confirm, slot_figures, PostgresOptions, PostgresSource, PostgresStreamOptions,
};

# async fn example() -> ce_stream_core::error::Result<()> {
let options = PostgresOptions {
    host: "127.0.0.1".into(),
    port: 5432,
    user: "ce_stream".into(), // LOGIN and REPLICATION; it reads no rows
    password: "...".into(),
    database: "app".into(),
    tls: true,
    tls_ca: Some("ca.pem".into()), // without one, TLS encrypts and checks nothing
};
if slot_figures(&options, "app_slot").await?.is_none() {
    create_slot(&options, "app_slot").await?;
}
let mut source = PostgresSource {
    options,
    stream: PostgresStreamOptions {
        slot: "app_slot".into(),
        publication: "ce_stream".into(),
        message_prefixes: vec!["my_marks".into()],
        min_server_version: (17, 0),
    },
    config: SourceConfig {
        source_id: "postgres://127.0.0.1:5432/app".into(),
        include_tables: vec![TableRef::new("public", "orders")],
        delivery_unit: DeliveryUnit::Transaction,
        checkpoint: CheckpointMode::OnConfirm,
        ..Default::default()
    },
    checkpoint: None,
    checkpoint_store: None,
    skip_gate_check: false,
    include: Default::default(),
    confirm: new_confirm(),
};
source.run_deliveries(|_delivery| Ok(())).await
# }
```

- A subject is `schema.table`: one stream is one database, which goes in `source_id`.
- `TableSchema.identity` and the controls carry the relation OID, which survives a rename.
- A row of a partition has the partition as its subject. With its parent on the include list, the row says so in the extensions `partition_root` and `partition_root_oid`.
- A drop or a rename sends nothing on the stream. `catalog_tables` runs the query that shows them, on a connection you supply.
- The embedder installs a `rustls` crypto provider before connecting with TLS.

## Live include list

Clone the handle **before** `run` / `run_deliveries`. Updates do **not** end the stream.

```rust,no_run
use ce_stream_core::{event::TableRef, IncludeList};

# fn example(handle: IncludeList) {
handle.insert(TableRef::new("demo", "items"));
handle.remove(&TableRef::new("demo", "orders"));
handle.replace([TableRef::new("demo", "items")]);
# }
```

- **When it applies:** the start of the next transaction. Best-effort; **no ack**.
- Mid-transaction updates never split the current transaction.
- A commit with nothing for the list is a `Progress`, so the position still advances.
- Empty `include_tables` at start still means all tables. After start, removing the last table or `replace([])` means no row events.
- MySQL DDL Query events are not filtered by this list.

## Capture gates

Every source has `gate()`, a `GateReport { failures, warnings }` whose entries name the object and the fix. A source about to run refuses on failures unless `skip_gate_check` is set, which is for lab environments only.

```rust,no_run
use ce_stream_core::source::ChangeSource;

# async fn example<S: ChangeSource>(source: S) -> ce_stream_core::error::Result<()> {
let report = source.gate().await?;
for warning in &report.warnings {
    println!("gate: {warning}");
}
# Ok(())
# }
```

The server checks of each source are in [`ops-e2e.md`](ops-e2e.md).

## Rules

- One stream per slot or `server_id` (do not start two readers with the same one).
- Capture stays single-threaded and ordered; fan-out in your callback or downstream bus.
- Prefer TLS; prefer reading a replica.
- Optional Avro: `HttpSink::with_format(url, SinkFormat::Avro)` or encode via `ce_stream_core::avro_encode` — see [`avro.md`](avro.md).

See also [`ops-e2e.md`](ops-e2e.md) for the HTTP sidecar path, [`delivery.md`](delivery.md) for commit-boundary semantics, and [`values.md`](values.md) for the values.
