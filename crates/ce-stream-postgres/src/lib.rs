//! PostgreSQL logical replication source: `pgoutput` over an own protocol layer,
//! with no libpq. See `docs/issues/19.md`.
//!
//! The embedder installs a `rustls` crypto provider before connecting with TLS.

mod assemble;
mod catalog;
mod gate;
mod lsn;
mod options;
mod pgoutput;
mod position;
mod seed;
mod slots;
mod source;
mod values;
mod wire;

pub use catalog::{catalog_tables, CatalogColumn, CatalogPartition, CatalogTable};
pub use gate::validate_capture_gates;
pub use lsn::Lsn;
pub use options::{PostgresOptions, PostgresStreamOptions};
pub use position::{covers_position, ADAPTER};
pub use seed::PgSeedCut;
pub use slots::{create_slot, drop_slot, slot_figures, SlotFigures};
pub use source::{new_confirm, PostgresSource};
pub use values::{host_type, text_to_json};
pub use wire::ServerError;
