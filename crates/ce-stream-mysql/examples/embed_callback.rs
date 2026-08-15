//! Minimal embed example: print CloudEvents from a callback (library push).
//!
//! ```text
//! cargo run -p ce-stream-mysql --example embed_callback
//! ```
//! Set CE_STREAM_* env vars or edit defaults below.

use ce_stream_core::event::TableRef;
use ce_stream_core::source::{ChangeSource, SourceConfig};
use ce_stream_mysql::{MysqlBinlogSource, MysqlSourceOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let host = std::env::var("CE_STREAM_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let user = std::env::var("CE_STREAM_USER").unwrap_or_else(|_| "ce_stream".into());
    let password = std::env::var("CE_STREAM_PASSWORD")
        .map_err(|_| "set CE_STREAM_PASSWORD (MySQL user password for the capture account)")?;
    let table = std::env::var("CE_STREAM_TABLE").unwrap_or_else(|_| "ce_stream_spike.t1".into());
    let (db, tbl) = table
        .split_once('.')
        .ok_or("CE_STREAM_TABLE must be db.table")?;

    let mut source = MysqlBinlogSource {
        options: MysqlSourceOptions {
            host,
            port: 3306,
            user,
            password,
            server_id: 19199,
            tls: true,
        },
        config: SourceConfig {
            source_id: "mysql://127.0.0.1:3306/embed-example".into(),
            include_tables: vec![TableRef::new(db, tbl)],
            payload_mode: Default::default(),
            queue_capacity: 64,
            delivery_unit: Default::default(),
        },
        checkpoint: None,
        checkpoint_store: None,
        skip_gate_check: true,
    };

    let mut n = 0u32;
    source
        .run(|ev| {
            println!("{}", serde_json::to_string(&ev)?);
            n += 1;
            if n >= 1 {
                return Err(ce_stream_core::Error::Source("done".into()));
            }
            Ok(())
        })
        .await
        .map_err(|e| e.to_string())
        .or_else(|e| if e.contains("done") { Ok(()) } else { Err(e) })?;
    Ok(())
}
