//! Where to connect, and what the stream reads.

/// A connection to one database. A stream, a seed cut, and a catalog query each
/// take their own: the stream's account needs only `REPLICATION`, and a seed
/// cut needs the primary.
#[derive(Clone)]
pub struct PostgresOptions {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub tls: bool,
    /// A PEM file of roots. With one the server's certificate and name are
    /// verified; without one TLS encrypts and checks nothing.
    pub tls_ca: Option<std::path::PathBuf>,
}

impl std::fmt::Debug for PostgresOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresOptions")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("database", &self.database)
            .field("tls", &self.tls)
            .field("tls_ca", &self.tls_ca)
            .finish_non_exhaustive()
    }
}

/// What the stream reads.
#[derive(Debug, Clone)]
pub struct PostgresStreamOptions {
    /// The slot. The source never makes one on its own: a missing slot is
    /// history lost. See [`crate::create_slot`].
    pub slot: String,
    pub publication: String,
    /// Prefixes of the consumer's own logical messages. Non-transactional
    /// messages ignore publications, so every other prefix is dropped.
    pub message_prefixes: Vec<String>,
    /// The lowest major and minor server version the gate takes.
    pub min_server_version: (u32, u32),
}

impl Default for PostgresStreamOptions {
    fn default() -> Self {
        Self {
            slot: String::new(),
            publication: "ce_stream".into(),
            message_prefixes: Vec::new(),
            min_server_version: (17, 0),
        }
    }
}
