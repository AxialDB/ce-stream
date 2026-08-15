use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use ce_stream_core::event::{CloudEvent, PayloadMode, SinkFormat, TableRef};
use ce_stream_core::source::{ChangeSource, DeliveryUnit, SourceConfig};
use ce_stream_core::{CheckpointStore, HttpSink, Sink, StdoutSink};
use ce_stream_mysql::{FileCheckpointStore, MysqlBinlogSource, MysqlSourceOptions};
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
    host: String,
    port: u16,
    user: String,
    password: String,
    server_id: u64,
    #[serde(default = "default_true")]
    tls: bool,
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

fn default_queue_capacity() -> usize {
    64
}

fn default_sink_format() -> String {
    "json".into()
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
    let checkpoint = store.load().await?;
    if let Some(cp) = &checkpoint {
        tracing::info!(gtid = ?cp.payload.get("gtid"), "loaded checkpoint");
    } else {
        tracing::info!("no checkpoint; starting from Latest");
    }

    tracing::info!(
        adapter = %cfg.source.adapter,
        host = %cfg.source.host,
        port = cfg.source.port,
        server_id = cfg.source.server_id,
        tls = cfg.source.tls,
        tables = ?cfg.source.include_tables,
        payload_mode = %cfg.source.payload_mode,
        delivery_unit = %cfg.source.delivery_unit,
        queue_capacity = cfg.source.queue_capacity,
        sink = %cfg.sink.kind,
        sink_format = %cfg.sink.format,
        "ce-stream starting (continuous unless max-events set)"
    );
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
        config: SourceConfig {
            source_id: cfg.source.source_id,
            include_tables,
            payload_mode,
            queue_capacity: cfg.source.queue_capacity,
            delivery_unit,
        },
        checkpoint,
        checkpoint_store: Some(Box::new(store)),
        skip_gate_check: args.skip_gate_check,
    };

    let emitted = Arc::new(AtomicU64::new(0));
    let max = args.max_events;
    let emitted_cb = Arc::clone(&emitted);
    let started = Instant::now();
    let handle = tokio::runtime::Handle::current();
    let sink_format_for_txn = sink_format;

    let run_result = if delivery_unit == DeliveryUnit::Row {
        source
            .run(|ev: CloudEvent| {
                emit_row(&out, &handle, &ev)?;

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
            .run_transactions(|txn| {
                emit_transaction(&out, &handle, &txn, sink_format_for_txn)?;

                let n = emitted_cb.fetch_add(1, Ordering::SeqCst) + 1;
                if n == 1 || n % 100 == 0 {
                    tracing::info!(
                        target: "ce_stream::health",
                        commits_total = n,
                        last_gtid = %txn.gtid,
                        rows_in_commit = txn.events.len(),
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

fn emit_transaction(
    out: &OutSink,
    handle: &tokio::runtime::Handle,
    txn: &ce_stream_core::CommittedTransaction,
    format: SinkFormat,
) -> Result<(), ce_stream_core::Error> {
    tokio::task::block_in_place(|| match out {
        OutSink::Stdout(_) => match format {
            SinkFormat::Json => {
                let line = serde_json::to_string(txn)
                    .map_err(|e| ce_stream_core::Error::Sink(e.to_string()))?;
                println!("{line}");
                Ok(())
            }
            SinkFormat::Avro => {
                let bytes = ce_stream_core::avro_encode::encode_committed_transaction(txn)?;
                let b64 = base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    bytes,
                );
                println!("{b64}");
                Ok(())
            }
        },
        OutSink::Http(s) => handle.block_on(s.post_committed_transaction(txn)),
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

fn validate_config(cfg: &FileConfig) -> Result<(), String> {
    if cfg.source.adapter != "mysql" {
        return Err(format!(
            "unsupported source.adapter: {} (v1 supports mysql only)",
            cfg.source.adapter
        ));
    }
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
    if cfg.source.include_tables.is_empty() {
        return Err("source.include_tables must list at least one database.table".into());
    }
    if cfg.source.source_id.trim().is_empty() {
        return Err("source.source_id must not be empty (CloudEvents source)".into());
    }
    if cfg.source.queue_capacity == 0 {
        return Err("source.queue_capacity must be >= 1".into());
    }
    parse_payload_mode(&cfg.source.payload_mode)?;
    parse_delivery_unit(&cfg.source.delivery_unit)?;
    parse_sink_format(&cfg.sink.format)?;
    if !cfg.source.tls {
        tracing::warn!("source.tls=false; TLS is recommended for production capture");
    }
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

fn parse_table_ref(s: &str) -> Result<TableRef, String> {
    let (db, table) = s
        .split_once('.')
        .ok_or_else(|| format!("include_tables entry must be database.table, got {s}"))?;
    if db.is_empty() || table.is_empty() {
        return Err(format!("invalid include_tables entry: {s}"));
    }
    Ok(TableRef::new(db, table))
}
