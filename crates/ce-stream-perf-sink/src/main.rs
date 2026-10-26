//! Mock HTTP sink for perf harness: configurable delay, receive counters.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Json;
use axum::Router;
use clap::Parser;
use serde::Serialize;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "ce-stream-perf-sink")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:18081")]
    listen: String,

    /// Artificial handling delay per POST (choke scenario).
    #[arg(long, default_value_t = 0)]
    delay_ms: u64,

    /// After N successful POST responses, hang on the next request (crash harness).
    /// The same as `--stall-at N+1`.
    #[arg(long, default_value_t = 0)]
    stall_after: u64,

    /// Hang on the Nth POST, 1 being the first, and on every one after it (crash
    /// harness). Unlike `--stall-after`, it can hang the very first delivery.
    #[arg(long, default_value_t = 0, conflicts_with = "stall_after")]
    stall_at: u64,
}

struct AppState {
    delay: Duration,
    /// 0: never hang.
    stall_at: u64,
    /// POSTs that arrived, answered or not.
    arrived: AtomicU64,
    received: AtomicU64,
    bytes: AtomicU64,
    stalled: AtomicU64,
}

#[derive(Serialize)]
struct Stats {
    /// POSTs answered.
    received: u64,
    bytes: u64,
    delay_ms: u64,
    /// POSTs that arrived and hang.
    stalled: u64,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    let args = Args::parse();
    let addr: SocketAddr = args
        .listen
        .parse()
        .unwrap_or_else(|e| panic!("invalid --listen {}: {e}", args.listen));

    let stall_at = if args.stall_at > 0 {
        args.stall_at
    } else if args.stall_after > 0 {
        args.stall_after + 1
    } else {
        0
    };
    let state = Arc::new(AppState {
        delay: Duration::from_millis(args.delay_ms),
        stall_at,
        arrived: AtomicU64::new(0),
        received: AtomicU64::new(0),
        bytes: AtomicU64::new(0),
        stalled: AtomicU64::new(0),
    });

    let app = Router::new()
        .route("/events", post(events))
        .route("/stats", get(stats))
        .route("/reset", post(reset))
        .with_state(state.clone());

    tracing::info!(%addr, delay_ms = args.delay_ms, stall_at, "perf sink listening");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("bind {addr}: {e}"));
    axum::serve(listener, app).await.expect("server error");
}

async fn events(State(state): State<Arc<AppState>>, body: Bytes) -> impl IntoResponse {
    let number = state.arrived.fetch_add(1, Ordering::SeqCst) + 1;
    if state.stall_at > 0 && number >= state.stall_at {
        state.stalled.fetch_add(1, Ordering::SeqCst);
        tracing::info!(
            number,
            stall_at = state.stall_at,
            "stalling POST for crash harness"
        );
        std::future::pending::<()>().await;
    }
    if !state.delay.is_zero() {
        tokio::time::sleep(state.delay).await;
    }
    state.received.fetch_add(1, Ordering::SeqCst);
    state.bytes.fetch_add(body.len() as u64, Ordering::SeqCst);
    StatusCode::OK
}

async fn stats(State(state): State<Arc<AppState>>) -> Json<Stats> {
    Json(Stats {
        received: state.received.load(Ordering::SeqCst),
        bytes: state.bytes.load(Ordering::SeqCst),
        delay_ms: state.delay.as_millis() as u64,
        stalled: state.stalled.load(Ordering::SeqCst),
    })
}

async fn reset(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    state.arrived.store(0, Ordering::SeqCst);
    state.received.store(0, Ordering::SeqCst);
    state.bytes.store(0, Ordering::SeqCst);
    state.stalled.store(0, Ordering::SeqCst);
    StatusCode::OK
}
