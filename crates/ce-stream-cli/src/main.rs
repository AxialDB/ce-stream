use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use ce_stream_core::delivery::{has_resume_payload, CheckpointMode, Delivery};
use ce_stream_core::event::{CloudEvent, PayloadMode, SinkFormat, TableRef};
use ce_stream_core::source::{ChangeSource, DeliveryUnit, SourceConfig};
use ce_stream_core::transaction::{CommittedTransaction, SourcePosition};
use ce_stream_core::{CheckpointStore, FileCheckpointStore, HttpSink, Sink, StdoutSink};
use ce_stream_mongo::{
    note_cluster_time, open_seed_cursor, read_seed_batch, FullDocumentMode,
    MongoChangeStreamSource, MongoCheckpoint, MongoSourceOptions,
};
use ce_stream_mysql::{MysqlBinlogSource, MysqlSourceOptions};
use ce_stream_postgres::{PostgresOptions, PostgresSource, PostgresStreamOptions};
use clap::Parser;
use serde::Deserialize;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "ce-stream",
    about = "CloudEvents change streams from database logs"
)]
struct Args {
    /// Path to config TOML
    #[arg(short, long, default_value = "ce-stream.toml")]
    config: PathBuf,

    /// Stop after N CloudEvents (0 = run forever). Smoke/CI only - not for production.
    #[arg(long, default_value_t = 0)]
    max_events: u64,

    /// Skip MySQL capture gate checks (lab escape hatch only).
    #[arg(long)]
    skip_gate_check: bool,
}

#[derive(Debug, Deserialize)]
struct FileConfig {
    source: SourceSection,
    checkpoint: CheckpointSection,
    sink: SinkSection,
}

#[derive(Debug, Deserialize)]
struct SourceSection {
    adapter: String,
    source_id: String,
    #[serde(default)]
    host: String,
    #[serde(default)]
    port: u16,
    #[serde(default)]
    user: String,
    #[serde(default)]
    password: String,
    #[serde(default)]
    server_id: u64,
    #[serde(default = "default_true")]
    tls: bool,
    /// MongoDB connection string. Required when adapter = mongo.
    #[serde(default)]
    uri: String,
    /// Database the change stream is opened on.
    #[serde(default)]
    database: String,
    /// required (post-images) | update_lookup
    #[serde(default = "default_full_document")]
    full_document: String,
    /// Copy included collections before tailing, fenced at cluster time T.
    #[serde(default)]
    seed: bool,
    include_tables: Vec<String>,
    /// full | signal
    #[serde(default = "default_payload_mode")]
    payload_mode: String,
    /// row (default) | transaction — see docs/issues/1.md
    #[serde(default = "default_delivery_unit")]
    delivery_unit: String,
    /// Bounded queue; reader blocks when full (backpressure).
    #[serde(default = "default_queue_capacity")]
    queue_capacity: usize,
    /// Bytes of row data the queue to the sink holds at most (issue #16).
    #[serde(default = "default_queue_max_bytes")]
    queue_max_bytes: usize,
    /// A long transaction is delivered in parts of at most this many rows.
    #[serde(default = "default_part_max_rows")]
    part_max_rows: usize,
    /// ... and at most this many bytes of row data.
    #[serde(default = "default_part_max_bytes")]
    part_max_bytes: usize,
    /// on_ok (default) | on_confirm: the checkpoint moves once the sink has the
    /// delivery, inside the callback or after it. on_confirm needs transaction delivery.
    #[serde(default = "default_checkpoint_mode")]
    checkpoint: String,
    /// PostgreSQL: a PEM file of roots. Without one TLS encrypts and checks nothing.
    #[serde(default)]
    tls_ca: Option<PathBuf>,
    /// PostgreSQL: the replication slot.
    #[serde(default)]
    slot: String,
    /// PostgreSQL: the publication.
    #[serde(default = "default_publication")]
    publication: String,
    /// PostgreSQL: prefixes of logical messages to deliver.
    #[serde(default)]
    message_prefixes: Vec<String>,
    /// PostgreSQL: the lowest server version the gate takes, such as "17.5".
    #[serde(default = "default_min_server_version")]
    min_server_version: String,
    /// PostgreSQL: make the slot when it does not exist. Otherwise a missing
    /// slot is an error: the changes since it went are gone.
    #[serde(default)]
    create_slot: bool,
}

#[derive(Debug, Deserialize)]
struct CheckpointSection {
    path: PathBuf,
}

#[derive(Debug, Deserialize)]
struct SinkSection {
    kind: String,
    #[serde(default)]
    url: Option<String>,
    /// json (default) | avro
    #[serde(default = "default_sink_format")]
    format: String,
}

fn default_true() -> bool {
    true
}

fn default_payload_mode() -> String {
    "full".into()
}

fn default_delivery_unit() -> String {
    "row".into()
}

fn default_queue_max_bytes() -> usize {
    ce_stream_core::source::DEFAULT_QUEUE_MAX_BYTES
}

fn default_queue_capacity() -> usize {
    64
}

fn default_part_max_rows() -> usize {
    ce_stream_core::source::DEFAULT_PART_MAX_ROWS
}

fn default_part_max_bytes() -> usize {
    ce_stream_core::source::DEFAULT_PART_MAX_BYTES
}

fn default_sink_format() -> String {
    "json".into()
}

fn default_full_document() -> String {
    "required".into()
}

fn default_checkpoint_mode() -> String {
    "on_ok".into()
}

fn default_publication() -> String {
    "ce_stream".into()
}

fn default_min_server_version() -> String {
    "17".into()
}

enum OutSink {
    Stdout(StdoutSink),
    Http(HttpSink),
}

#[tokio::main]
async fn main() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    if let Err(err) = run().await {
        tracing::error!(error = %err, "ce-stream failed");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    let raw = tokio::fs::read_to_string(&args.config).await.map_err(|e| {
        format!(
            "read config {}: {e} (copy ce-stream.toml.example)",
            args.config.display()
        )
    })?;
    let cfg: FileConfig = toml::from_str(&raw).map_err(|e| format!("config parse error: {e}"))?;
    validate_config(&cfg)?;

    if args.max_events > 0 {
        tracing::warn!(
            max_events = args.max_events,
            "max-events is for smoke/CI only; omit it in production"
        );
    }

    let include_tables = cfg
        .source
        .include_tables
        .iter()
        .map(|s| parse_table_ref(s))
        .collect::<Result<Vec<_>, _>>()?;

    let payload_mode = parse_payload_mode(&cfg.source.payload_mode)?;
    let delivery_unit = parse_delivery_unit(&cfg.source.delivery_unit)?;
    let sink_format = parse_sink_format(&cfg.sink.format)?;
    let checkpoint_mode = parse_checkpoint_mode(&cfg.source.checkpoint)?;

    let out = match cfg.sink.kind.as_str() {
        "stdout" => OutSink::Stdout(StdoutSink::new(sink_format)),
        "http" => {
            let url = cfg
                .sink
                .url
                .clone()
                .ok_or("sink.url is required when sink.kind = \"http\"")?;
            OutSink::Http(HttpSink::with_format(url, sink_format)?)
        }
        other => {
            return Err(format!("unsupported sink.kind: {other} (use stdout|http)").into());
        }
    };

    let store = FileCheckpointStore {
        path: cfg.checkpoint.path.clone(),
    };
    let mut checkpoint = store.load().await?;

    tracing::info!(
        adapter = %cfg.source.adapter,
        tables = ?cfg.source.include_tables,
        payload_mode = %cfg.source.payload_mode,
        delivery_unit = %cfg.source.delivery_unit,
        sink = %cfg.sink.kind,
        sink_format = %cfg.sink.format,
        "ce-stream starting (continuous unless max-events set)"
    );
    if sink_format == SinkFormat::Avro && delivery_unit == DeliveryUnit::Transaction {
        tracing::info!(
            schema = ce_stream_core::avro_encode::committed_transaction_schema_id(
                &CommittedTransaction::default()
            ),
            "transaction avro: commits in this schema; progress and table definitions in their own"
        );
    }

    let config = SourceConfig {
        part_max_rows: cfg.source.part_max_rows,
        part_max_bytes: cfg.source.part_max_bytes,
        source_id: cfg.source.source_id.clone(),
        include_tables: include_tables.clone(),
        payload_mode,
        queue_capacity: cfg.source.queue_capacity,
        queue_max_bytes: cfg.source.queue_max_bytes,
        delivery_unit,
        checkpoint: checkpoint_mode,
    };
    let confirming = checkpoint_mode == CheckpointMode::OnConfirm;

    if cfg.source.adapter == "postgres" {
        let options = PostgresOptions {
            host: cfg.source.host.clone(),
            port: cfg.source.port,
            user: cfg.source.user.clone(),
            password: cfg.source.password.clone(),
            database: cfg.source.database.clone(),
            tls: cfg.source.tls,
            tls_ca: cfg.source.tls_ca.clone(),
        };
        if cfg.source.create_slot
            && ce_stream_postgres::slot_figures(&options, &cfg.source.slot)
                .await?
                .is_none()
        {
            let at = ce_stream_postgres::create_slot(&options, &cfg.source.slot).await?;
            tracing::info!(slot = %cfg.source.slot, %at, "made the replication slot");
        }
        let mut source = PostgresSource {
            options,
            stream: PostgresStreamOptions {
                slot: cfg.source.slot.clone(),
                publication: cfg.source.publication.clone(),
                message_prefixes: cfg.source.message_prefixes.clone(),
                min_server_version: parse_server_version(&cfg.source.min_server_version)?,
            },
            config,
            checkpoint,
            checkpoint_store: Some(Box::new(store)),
            skip_gate_check: args.skip_gate_check,
            include: Default::default(),
            confirm: ce_stream_postgres::new_confirm(),
        };
        return capture(
            &mut source,
            delivery_unit,
            &out,
            sink_format,
            args.max_events,
            confirming,
        )
        .await;
    }

    if cfg.source.adapter == "mongo" {
        let full_document = parse_full_document(&cfg.source.full_document)?;
        if cfg.source.seed {
            checkpoint = seed_mongo(
                &cfg,
                &include_tables,
                &out,
                payload_mode,
                delivery_unit,
                sink_format,
                &store,
                checkpoint,
            )
            .await?;
        }
        let mut source = MongoChangeStreamSource {
            options: MongoSourceOptions {
                uri: cfg.source.uri.clone(),
                database: cfg.source.database.clone(),
                full_document,
            },
            config,
            checkpoint,
            checkpoint_store: Some(Box::new(store)),
            skip_gate_check: args.skip_gate_check,
            include: Default::default(),
            confirm: ce_stream_mongo::new_confirm(),
        };
        return capture(
            &mut source,
            delivery_unit,
            &out,
            sink_format,
            args.max_events,
            confirming,
        )
        .await;
    }

    tracing::info!(
        "tip: set binlog_row_metadata=FULL on MySQL for real column names; prefer a replica host"
    );
    let mut source = MysqlBinlogSource {
        options: MysqlSourceOptions {
            host: cfg.source.host,
            port: cfg.source.port,
            user: cfg.source.user,
            password: cfg.source.password,
            server_id: cfg.source.server_id,
            tls: cfg.source.tls,
        },
        config,
        checkpoint,
        checkpoint_store: Some(Box::new(store)),
        skip_gate_check: args.skip_gate_check,
        include: Default::default(),
        confirm: ce_stream_mysql::new_confirm(),
    };
    capture(
        &mut source,
        delivery_unit,
        &out,
        sink_format,
        args.max_events,
        confirming,
    )
    .await
}

/// What the sink now has, as a position to confirm. A part that is not the last
/// and a table definition have none.
fn confirmable(delivery: &Delivery) -> Option<&SourcePosition> {
    let position = match delivery {
        Delivery::Commit(txn) => txn.position.as_ref(),
        Delivery::Progress { position } => Some(position),
        Delivery::Schema(_) => None,
    }?;
    has_resume_payload(position).then_some(position)
}

async fn capture<S: ChangeSource>(
    source: &mut S,
    delivery_unit: DeliveryUnit,
    out: &OutSink,
    sink_format: SinkFormat,
    max: u64,
    confirming: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let emitted = Arc::new(AtomicU64::new(0));
    let emitted_cb = Arc::clone(&emitted);
    let started = Instant::now();
    let handle = tokio::runtime::Handle::current();
    let confirm = confirming.then(|| source.confirm());

    let run_result = if delivery_unit == DeliveryUnit::Row {
        source
            .run(|ev: CloudEvent| {
                emit_row(out, &handle, &ev)?;
                let n = emitted_cb.fetch_add(1, Ordering::SeqCst) + 1;
                log_row_health(n, &ev, &started);
                if max > 0 && n >= max {
                    return Err(ce_stream_core::Error::Source(format!(
                        "reached max_events={max}"
                    )));
                }
                Ok(())
            })
            .await
    } else {
        source
            .run_deliveries(|delivery| {
                emit_delivery(out, &handle, &delivery, sink_format)?;
                if let (Some(confirm), Some(position)) = (&confirm, confirmable(&delivery)) {
                    confirm.confirm(position)?;
                }
                let n = emitted_cb.fetch_add(1, Ordering::SeqCst) + 1;
                if n == 1 || n.is_multiple_of(100) {
                    let rows_in_commit = match &delivery {
                        Delivery::Commit(txn) => txn.events.len(),
                        _ => 0,
                    };
                    tracing::info!(
                        target: "ce_stream::health",
                        deliveries_total = n,
                        rows_in_commit,
                        uptime_secs = started.elapsed().as_secs(),
                        "capture health"
                    );
                }
                if max > 0 && n >= max {
                    return Err(ce_stream_core::Error::Source(format!(
                        "reached max_events={max}"
                    )));
                }
                Ok(())
            })
            .await
    };

    run_result.or_else(|e| {
        if e.to_string().contains("reached max_events=") {
            tracing::info!(
                target: "ce_stream::health",
                count = emitted.load(Ordering::SeqCst),
                uptime_secs = started.elapsed().as_secs(),
                "stopped at max_events"
            );
            Ok(())
        } else {
            Err(e)
        }
    })?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn seed_mongo(
    cfg: &FileConfig,
    tables: &[TableRef],
    out: &OutSink,
    payload_mode: PayloadMode,
    delivery_unit: DeliveryUnit,
    sink_format: SinkFormat,
    store: &FileCheckpointStore,
    existing: Option<ce_stream_core::Checkpoint>,
) -> Result<Option<ce_stream_core::Checkpoint>, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(cp) = &existing {
        if let Some(stored) = MongoCheckpoint::from_checkpoint(cp) {
            if !stored.resume_token.is_null() {
                tracing::info!("checkpoint has a resume token; skipping seed");
                return Ok(existing);
            }
        }
    }
    let client = mongodb::Client::with_uri_str(&cfg.source.uri).await?;
    let cluster_time = note_cluster_time(&client).await?;
    tracing::info!(t = cluster_time.t, i = cluster_time.i, "seed fence");
    let handle = tokio::runtime::Handle::current();
    let payload = MongoCheckpoint {
        resume_token: serde_json::Value::Null,
        cluster_time: Some(cluster_time),
        seed_cluster_time: Some(cluster_time),
    }
    .to_checkpoint()
    .payload;
    for table in tables {
        let mut cursor = open_seed_cursor(&client, &table.database, &table.table).await?;
        loop {
            let events = read_seed_batch(
                &mut cursor,
                &cfg.source.source_id,
                table,
                cluster_time,
                payload_mode,
            )
            .await?;
            if events.is_empty() {
                break;
            }
            if delivery_unit == DeliveryUnit::Row {
                for ev in &events {
                    emit_row(out, &handle, ev)?;
                }
            } else {
                let txn = CommittedTransaction {
                    part: None,
                    position: Some(SourcePosition {
                        adapter: "mongo".into(),
                        at: payload.clone(),
                        after: payload.clone(),
                    }),
                    events,
                    ..Default::default()
                };
                emit_delivery(out, &handle, &Delivery::Commit(txn), sink_format)?;
            }
        }
    }
    let cp = MongoCheckpoint {
        resume_token: serde_json::Value::Null,
        cluster_time: Some(cluster_time),
        seed_cluster_time: Some(cluster_time),
    }
    .to_checkpoint();
    store.save(&cp).await?;
    Ok(Some(cp))
}

fn emit_row(
    out: &OutSink,
    handle: &tokio::runtime::Handle,
    ev: &CloudEvent,
) -> Result<(), ce_stream_core::Error> {
    tokio::task::block_in_place(|| match out {
        OutSink::Stdout(s) => handle.block_on(s.emit(ev)),
        OutSink::Http(s) => handle.block_on(s.emit(ev)),
    })
}

fn emit_delivery(
    out: &OutSink,
    handle: &tokio::runtime::Handle,
    delivery: &Delivery,
    format: SinkFormat,
) -> Result<(), ce_stream_core::Error> {
    tokio::task::block_in_place(|| match out {
        OutSink::Stdout(_) => match format {
            SinkFormat::Json => {
                let line = serde_json::to_string(delivery)
                    .map_err(|e| ce_stream_core::Error::Sink(e.to_string()))?;
                println!("{line}");
                Ok(())
            }
            SinkFormat::Avro => {
                let (bytes, _, _) = ce_stream_core::avro_encode::encode_delivery(delivery)?;
                let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes);
                println!("{b64}");
                Ok(())
            }
        },
        OutSink::Http(s) => handle.block_on(s.post_delivery(delivery)),
    })
}

fn log_row_health(n: u64, ev: &CloudEvent, started: &Instant) {
    let gtid = ev
        .extensions
        .get("gtid")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let lag_ms = event_lag_ms(ev);
    if n == 1 || n.is_multiple_of(100) {
        tracing::info!(
            target: "ce_stream::health",
            events_total = n,
            last_gtid = %gtid,
            subject = %ev.subject,
            lag_ms,
            uptime_secs = started.elapsed().as_secs(),
            "capture health"
        );
    }
}

fn event_lag_ms(ev: &CloudEvent) -> i64 {
    if let Ok(t) = chrono::DateTime::parse_from_rfc3339(&ev.time) {
        let now = chrono::Utc::now();
        return (now - t.with_timezone(&chrono::Utc)).num_milliseconds();
    }
    -1
}

fn parse_payload_mode(s: &str) -> Result<PayloadMode, String> {
    match s.trim().to_ascii_lowercase().as_str() {
        "full" => Ok(PayloadMode::Full),
        "signal" => Ok(PayloadMode::Signal),
        other => Err(format!(
            "source.payload_mode must be full|signal, got {other}"
        )),
    }
}

fn parse_delivery_unit(s: &str) -> Result<DeliveryUnit, String> {
    match s.trim().to_ascii_lowercase().as_str() {
        "row" => Ok(DeliveryUnit::Row),
        "transaction" => Ok(DeliveryUnit::Transaction),
        other => Err(format!(
            "source.delivery_unit must be row|transaction, got {other}"
        )),
    }
}

fn parse_sink_format(s: &str) -> Result<SinkFormat, String> {
    match s.trim().to_ascii_lowercase().as_str() {
        "json" => Ok(SinkFormat::Json),
        "avro" => Ok(SinkFormat::Avro),
        other => Err(format!("sink.format must be json|avro, got {other}")),
    }
}

fn parse_full_document(s: &str) -> Result<FullDocumentMode, String> {
    match s.trim().to_ascii_lowercase().as_str() {
        "required" => Ok(FullDocumentMode::Required),
        "update_lookup" => Ok(FullDocumentMode::UpdateLookup),
        other => Err(format!(
            "source.full_document must be required|update_lookup, got {other}"
        )),
    }
}

fn parse_checkpoint_mode(s: &str) -> Result<CheckpointMode, String> {
    match s.trim().to_ascii_lowercase().as_str() {
        "on_ok" => Ok(CheckpointMode::OnOk),
        "on_confirm" => Ok(CheckpointMode::OnConfirm),
        other => Err(format!(
            "source.checkpoint must be on_ok|on_confirm, got {other}"
        )),
    }
}

/// "17" or "17.5".
fn parse_server_version(s: &str) -> Result<(u32, u32), String> {
    let bad = || format!("source.min_server_version must be like 17 or 17.5, got {s}");
    let (major, minor) = s.trim().split_once('.').unwrap_or((s.trim(), "0"));
    Ok((
        major.parse().map_err(|_| bad())?,
        minor.parse().map_err(|_| bad())?,
    ))
}

type ValidateFn = fn(&FileConfig) -> Result<(), String>;

/// Every adapter the CLI has, and what its section must hold.
const ADAPTERS: &[(&str, ValidateFn)] = &[
    ("mysql", validate_mysql),
    ("mongo", validate_mongo),
    ("postgres", validate_postgres),
];

fn validate_config(cfg: &FileConfig) -> Result<(), String> {
    let Some((_, validate)) = ADAPTERS
        .iter()
        .find(|(name, _)| *name == cfg.source.adapter)
    else {
        let names: Vec<&str> = ADAPTERS.iter().map(|(name, _)| *name).collect();
        return Err(format!(
            "unsupported source.adapter: {} (one of {})",
            cfg.source.adapter,
            names.join(", ")
        ));
    };
    validate(cfg)?;
    if parse_checkpoint_mode(&cfg.source.checkpoint)? == CheckpointMode::OnConfirm
        && parse_delivery_unit(&cfg.source.delivery_unit)? == DeliveryUnit::Row
    {
        return Err("source.checkpoint = on_confirm needs delivery_unit = transaction".into());
    }
    if cfg.source.include_tables.is_empty() {
        return Err("source.include_tables must list at least one database.table".into());
    }
    if cfg.source.source_id.trim().is_empty() {
        return Err("source.source_id must not be empty (CloudEvents source)".into());
    }
    if cfg.source.part_max_rows == 0 || cfg.source.part_max_bytes == 0 {
        return Err("source.part_max_rows and source.part_max_bytes must be >= 1".into());
    }
    if cfg.source.queue_max_bytes == 0 {
        return Err("source.queue_max_bytes must be >= 1".into());
    }
    if cfg.source.queue_capacity == 0 {
        return Err("source.queue_capacity must be >= 1".into());
    }
    parse_payload_mode(&cfg.source.payload_mode)?;
    parse_delivery_unit(&cfg.source.delivery_unit)?;
    parse_sink_format(&cfg.sink.format)?;
    match cfg.sink.kind.as_str() {
        "stdout" => {}
        "http" => {
            if cfg
                .sink
                .url
                .as_ref()
                .map(|u| u.trim().is_empty())
                .unwrap_or(true)
            {
                return Err("sink.url is required when sink.kind = \"http\"".into());
            }
        }
        other => {
            return Err(format!("unsupported sink.kind: {other} (use stdout|http)"));
        }
    }
    Ok(())
}

fn validate_mysql(cfg: &FileConfig) -> Result<(), String> {
    if cfg.source.host.trim().is_empty() {
        return Err("source.host must not be empty".into());
    }
    if cfg.source.port == 0 {
        return Err("source.port must be > 0".into());
    }
    if cfg.source.user.trim().is_empty() {
        return Err("source.user must not be empty".into());
    }
    if cfg.source.server_id == 0 {
        return Err("source.server_id must be a unique non-zero replica id".into());
    }
    if !cfg.source.tls {
        tracing::warn!("source.tls=false; TLS is recommended for production capture");
    }
    Ok(())
}

fn validate_postgres(cfg: &FileConfig) -> Result<(), String> {
    for (key, value) in [
        ("host", &cfg.source.host),
        ("user", &cfg.source.user),
        ("database", &cfg.source.database),
        ("slot", &cfg.source.slot),
        ("publication", &cfg.source.publication),
    ] {
        if value.trim().is_empty() {
            return Err(format!("source.{key} must not be empty"));
        }
    }
    if cfg.source.port == 0 {
        return Err("source.port must be > 0".into());
    }
    parse_server_version(&cfg.source.min_server_version)?;
    if !cfg.source.tls {
        tracing::warn!("source.tls=false; TLS is recommended for production capture");
    }
    Ok(())
}

fn validate_mongo(cfg: &FileConfig) -> Result<(), String> {
    if cfg.source.uri.trim().is_empty() {
        return Err("source.uri must be a MongoDB connection string".into());
    }
    if cfg.source.database.trim().is_empty() {
        return Err("source.database must be the database to watch".into());
    }
    parse_full_document(&cfg.source.full_document)?;
    for entry in &cfg.source.include_tables {
        let (db, _) = entry.split_once('.').ok_or_else(|| {
            format!("include_tables entry must be database.collection, got {entry}")
        })?;
        if db != cfg.source.database {
            return Err(format!(
                "include_tables entry {entry} is not in source.database {}",
                cfg.source.database
            ));
        }
    }
    Ok(())
}

fn parse_table_ref(s: &str) -> Result<TableRef, String> {
    let (db, table) = s
        .split_once('.')
        .ok_or_else(|| format!("include_tables entry must be database.table, got {s}"))?;
    if db.is_empty() || table.is_empty() {
        return Err(format!("invalid include_tables entry: {s}"));
    }
    Ok(TableRef::new(db, table))
}
