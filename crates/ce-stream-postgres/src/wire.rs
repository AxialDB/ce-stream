//! The connection: TLS, startup, SCRAM-SHA-256, simple query, CopyBoth frames.
//! No libpq, so that the Windows build stays pure Rust.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use ce_stream_core::error::{Error, Result};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::lsn::Lsn;
use crate::options::PostgresOptions;

const PROTOCOL_3_0: i32 = 196_608;
const SSL_REQUEST: i32 = 80_877_103;

/// The settings every connection starts with, so that the text the server
/// sends has one format. Converting to UTF-8 is the server's work.
pub(crate) const PINNED_SETTINGS: &[(&str, &str)] = &[
    ("client_encoding", "UTF8"),
    ("DateStyle", "ISO"),
    ("TimeZone", "UTC"),
    ("IntervalStyle", "postgres"),
    ("extra_float_digits", "3"),
    ("bytea_output", "hex"),
    ("lc_monetary", "C"),
];

pub(crate) trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

fn source_error(what: &str, err: impl std::fmt::Display) -> Error {
    Error::Source(format!("{what}: {err}"))
}

/// One message: its tag and its payload.
pub(crate) async fn read_message<R: AsyncRead + Unpin>(r: &mut R) -> Result<(u8, Vec<u8>)> {
    let tag = r
        .read_u8()
        .await
        .map_err(|e| source_error("connection closed", e))?;
    let len = r
        .read_i32()
        .await
        .map_err(|e| source_error("connection closed", e))? as usize;
    if len < 4 {
        return Err(Error::Source(format!("bad message length {len}")));
    }
    let mut payload = vec![0u8; len - 4];
    r.read_exact(&mut payload)
        .await
        .map_err(|e| source_error("connection closed", e))?;
    Ok((tag, payload))
}

pub(crate) fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 5);
    out.push(tag);
    out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// A server's error: its SQLSTATE and its text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerError {
    pub code: String,
    pub message: String,
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.message, self.code)
    }
}

pub(crate) fn parse_error(payload: &[u8]) -> ServerError {
    let mut code = String::new();
    let mut message = String::new();
    let mut rest = payload;
    while let Some(&field) = rest.first() {
        if field == 0 {
            break;
        }
        let end = rest.iter().position(|&b| b == 0).unwrap_or(rest.len());
        let text = String::from_utf8_lossy(&rest[1..end]).into_owned();
        match field {
            b'C' => code = text,
            b'M' => message = text,
            _ => {}
        }
        rest = &rest[(end + 1).min(rest.len())..];
    }
    ServerError { code, message }
}

/// A server error that means the changes are gone: a slot that is missing or
/// invalidated. Never a reason to make a new slot quietly.
pub(crate) fn map_server_error(what: &str, err: ServerError) -> Error {
    // 42704 undefined_object, a missing slot. 55000 object_not_in_prerequisite_state
    // with this text, an invalidated slot.
    let invalidated = err.code == "55000"
        && (err.message.contains("can no longer get changes")
            || err.message.contains("has been invalidated")
            || err
                .message
                .contains("cannot read from logical replication slot"));
    if (err.code == "42704" && err.message.contains("replication slot")) || invalidated {
        Error::HistoryLost(format!("{what}: {err}"))
    } else {
        Error::Source(format!("{what}: {err}"))
    }
}

fn hmac256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes any key length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut salted = salt.to_vec();
    salted.extend_from_slice(&1u32.to_be_bytes());
    let mut u = hmac256(password, &salted);
    let mut t = u;
    for _ in 1..iterations {
        u = hmac256(password, &u);
        for (a, b) in t.iter_mut().zip(u.iter()) {
            *a ^= b;
        }
    }
    t
}

async fn scram<S: Io>(stream: &mut S, mechanisms: &[u8], password: &str) -> Result<()> {
    let offered = String::from_utf8_lossy(mechanisms).replace('\0', " ");
    if !offered.contains("SCRAM-SHA-256") {
        return Err(Error::Source(format!(
            "server offers no SCRAM-SHA-256 (offered: {offered})"
        )));
    }
    let b64 = base64::engine::general_purpose::STANDARD;
    let client_nonce = b64.encode(rand::random::<[u8; 18]>());
    let client_first_bare = format!("n=,r={client_nonce}");
    let initial = format!("n,,{client_first_bare}");
    let mut body = b"SCRAM-SHA-256\0".to_vec();
    body.extend_from_slice(&(initial.len() as i32).to_be_bytes());
    body.extend_from_slice(initial.as_bytes());
    write_all(stream, &frame(b'p', &body)).await?;

    let server_first = read_auth(stream, 11).await?;
    let mut nonce = None;
    let mut salt = None;
    let mut iterations = None;
    for part in server_first.split(',') {
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };
        match key {
            "r" => nonce = Some(value.to_string()),
            "s" => {
                salt = Some(
                    b64.decode(value)
                        .map_err(|e| source_error("SCRAM salt", e))?,
                )
            }
            "i" => {
                iterations = Some(
                    value
                        .parse::<u32>()
                        .map_err(|e| source_error("SCRAM iterations", e))?,
                )
            }
            _ => {}
        }
    }
    let (Some(nonce), Some(salt), Some(iterations)) = (nonce, salt, iterations) else {
        return Err(Error::Source(
            "SCRAM: incomplete server first message".into(),
        ));
    };
    if !nonce.starts_with(&client_nonce) {
        return Err(Error::Source(
            "SCRAM: server nonce does not extend the client's".into(),
        ));
    }
    let salted = pbkdf2_sha256(password.as_bytes(), &salt, iterations);
    let client_key = hmac256(&salted, b"Client Key");
    let stored_key: [u8; 32] = Sha256::digest(client_key).into();
    let client_final_bare = format!("c=biws,r={nonce}");
    let auth_message = format!("{client_first_bare},{server_first},{client_final_bare}");
    let signature = hmac256(&stored_key, auth_message.as_bytes());
    let proof: Vec<u8> = client_key
        .iter()
        .zip(signature.iter())
        .map(|(a, b)| a ^ b)
        .collect();
    let client_final = format!("{client_final_bare},p={}", b64.encode(proof));
    write_all(stream, &frame(b'p', client_final.as_bytes())).await?;

    let server_final = read_auth(stream, 12).await?;
    let server_key = hmac256(&salted, b"Server Key");
    let expected = b64.encode(hmac256(&server_key, auth_message.as_bytes()));
    if server_final != format!("v={expected}") {
        return Err(Error::Source("SCRAM: server signature mismatch".into()));
    }
    Ok(())
}

async fn read_auth<S: Io>(stream: &mut S, expected: i32) -> Result<String> {
    let (tag, payload) = read_message(stream).await?;
    if tag == b'E' {
        return Err(Error::Source(format!(
            "authentication: {}",
            parse_error(&payload)
        )));
    }
    let code = payload
        .get(..4)
        .map(|b| i32::from_be_bytes(b.try_into().expect("four bytes")));
    if tag != b'R' || code != Some(expected) {
        return Err(Error::Source(format!(
            "authentication: unexpected message {:?}",
            tag as char
        )));
    }
    Ok(String::from_utf8_lossy(&payload[4..]).into_owned())
}

async fn write_all<S: AsyncWrite + Unpin>(stream: &mut S, bytes: &[u8]) -> Result<()> {
    stream
        .write_all(bytes)
        .await
        .map_err(|e| source_error("connection write", e))?;
    stream
        .flush()
        .await
        .map_err(|e| source_error("connection write", e))
}

/// Encryption without a CA checks nothing about the server, as MySQL's
/// `ssl-mode=required` does.
#[derive(Debug)]
struct NoVerify(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls_pki_types::CertificateDer<'_>,
        _intermediates: &[rustls_pki_types::CertificateDer<'_>],
        _server_name: &rustls_pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls_pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls_pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

fn tls_config(options: &PostgresOptions) -> Result<rustls::ClientConfig> {
    let provider = rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .ok_or_else(|| Error::Config("no TLS crypto provider is installed".into()))?;
    let builder = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| source_error("TLS", e))?;
    Ok(match &options.tls_ca {
        Some(path) => {
            use rustls_pki_types::pem::PemObject;
            let mut roots = rustls::RootCertStore::empty();
            let certs = rustls_pki_types::CertificateDer::pem_file_iter(path)
                .map_err(|e| source_error("tls_ca", e))?;
            for cert in certs {
                let cert = cert.map_err(|e| source_error("tls_ca", e))?;
                roots.add(cert).map_err(|e| source_error("tls_ca", e))?;
            }
            builder.with_root_certificates(roots).with_no_client_auth()
        }
        None => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerify(provider)))
            .with_no_client_auth(),
    })
}

async fn open_stream(options: &PostgresOptions) -> Result<Box<dyn Io>> {
    let mut tcp = TcpStream::connect((options.host.as_str(), options.port))
        .await
        .map_err(|e| source_error(&format!("connect to {}:{}", options.host, options.port), e))?;
    tcp.set_nodelay(true)
        .map_err(|e| source_error("connect", e))?;
    if !options.tls {
        return Ok(Box::new(tcp));
    }
    let mut request = Vec::with_capacity(8);
    request.extend_from_slice(&8i32.to_be_bytes());
    request.extend_from_slice(&SSL_REQUEST.to_be_bytes());
    write_all(&mut tcp, &request).await?;
    let answer = tcp
        .read_u8()
        .await
        .map_err(|e| source_error("TLS request", e))?;
    if answer != b'S' {
        return Err(Error::Source(format!(
            "{}:{} does not accept TLS",
            options.host, options.port
        )));
    }
    let name = rustls_pki_types::ServerName::try_from(options.host.clone())
        .map_err(|e| source_error("TLS server name", e))?;
    let connector = tokio_rustls::TlsConnector::from(Arc::new(tls_config(options)?));
    let tls = connector
        .connect(name, tcp)
        .await
        .map_err(|e| source_error("TLS handshake", e))?;
    Ok(Box::new(tls))
}

/// Rows of a simple query, as text.
pub type Rows = Vec<Vec<Option<String>>>;

pub(crate) fn startup_body(options: &PostgresOptions, replication: bool) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&PROTOCOL_3_0.to_be_bytes());
    let mut settings = vec![
        ("user", options.user.as_str()),
        ("database", options.database.as_str()),
        ("application_name", "ce-stream"),
    ];
    if replication {
        settings.push(("replication", "database"));
    }
    settings.extend_from_slice(PINNED_SETTINGS);
    for (key, value) in settings {
        body.extend_from_slice(key.as_bytes());
        body.push(0);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);
    body
}

pub(crate) struct Connection {
    pub(crate) stream: Box<dyn Io>,
}

impl Connection {
    /// `replication`: a logical replication connection, which also takes
    /// ordinary SQL.
    pub(crate) async fn connect(options: &PostgresOptions, replication: bool) -> Result<Self> {
        let mut stream = open_stream(options).await?;
        let body = startup_body(options, replication);
        let mut message = ((body.len() + 4) as i32).to_be_bytes().to_vec();
        message.extend_from_slice(&body);
        write_all(&mut stream, &message).await?;
        loop {
            let (tag, payload) = read_message(&mut stream).await?;
            match tag {
                b'R' => {
                    let code = payload
                        .get(..4)
                        .map(|b| i32::from_be_bytes(b.try_into().expect("four bytes")));
                    match code {
                        Some(0) => {}
                        Some(10) => scram(&mut stream, &payload[4..], &options.password).await?,
                        other => {
                            return Err(Error::Source(format!(
                                "unsupported authentication request {other:?}; use scram-sha-256"
                            )))
                        }
                    }
                }
                b'E' => {
                    return Err(Error::Source(format!(
                        "connect to {}:{}: {}",
                        options.host,
                        options.port,
                        parse_error(&payload)
                    )))
                }
                b'S' | b'K' | b'N' => {}
                b'Z' => return Ok(Self { stream }),
                other => {
                    return Err(Error::Source(format!(
                        "unexpected message {:?} at startup",
                        other as char
                    )))
                }
            }
        }
    }

    /// `Err` with the server's error kept apart, for a caller that reads it.
    pub(crate) async fn try_query(
        &mut self,
        sql: &str,
    ) -> Result<std::result::Result<Option<Rows>, ServerError>> {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        write_all(&mut self.stream, &frame(b'Q', &body)).await?;
        let mut rows = Vec::new();
        let mut error = None;
        loop {
            let (tag, payload) = read_message(&mut self.stream).await?;
            match tag {
                // CopyBoth: START_REPLICATION was accepted.
                b'W' => return Ok(Ok(None)),
                b'T' | b'C' | b'N' | b'S' | b'I' => {}
                b'D' => rows.push(parse_row(&payload)?),
                b'E' => error = Some(parse_error(&payload)),
                b'Z' => {
                    return Ok(match error {
                        Some(err) => Err(err),
                        None => Ok(Some(rows)),
                    })
                }
                other => {
                    return Err(Error::Source(format!(
                        "unexpected message {:?} answering a query",
                        other as char
                    )))
                }
            }
        }
    }

    pub(crate) async fn query(&mut self, sql: &str) -> Result<Rows> {
        match self.try_query(sql).await? {
            Ok(Some(rows)) => Ok(rows),
            Ok(None) => Err(Error::Source(format!("{sql}: unexpected CopyBoth"))),
            Err(err) => Err(map_server_error(sql, err)),
        }
    }
}

fn parse_row(payload: &[u8]) -> Result<Vec<Option<String>>> {
    let truncated = || Error::Source("truncated row".into());
    let count = i16::from_be_bytes(
        payload
            .get(..2)
            .ok_or_else(truncated)?
            .try_into()
            .expect("two bytes"),
    );
    let mut rest = &payload[2..];
    let mut row = Vec::with_capacity(count.max(0) as usize);
    for _ in 0..count {
        let len = i32::from_be_bytes(
            rest.get(..4)
                .ok_or_else(truncated)?
                .try_into()
                .expect("four bytes"),
        );
        rest = &rest[4..];
        if len < 0 {
            row.push(None);
        } else {
            let text = rest.get(..len as usize).ok_or_else(truncated)?;
            row.push(Some(String::from_utf8_lossy(text).into_owned()));
            rest = &rest[len as usize..];
        }
    }
    Ok(row)
}

/// A string literal for SQL.
pub(crate) fn quote_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// An identifier for SQL or a replication command.
pub(crate) fn quote_ident(text: &str) -> String {
    format!("\"{}\"", text.replace('"', "\"\""))
}

fn pg_now_micros() -> i64 {
    const PG_EPOCH_UNIX_MICROS: i64 = 946_684_800_000_000;
    let unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    unix.as_micros() as i64 - PG_EPOCH_UNIX_MICROS
}

/// Standby status update: written, flushed, and applied are all the confirmed
/// position, since nothing short of durable is confirmed.
pub(crate) fn status_update(lsn: Lsn) -> Vec<u8> {
    let mut body = Vec::with_capacity(34);
    body.push(b'r');
    for _ in 0..3 {
        body.extend_from_slice(&lsn.0.to_be_bytes());
    }
    body.extend_from_slice(&pg_now_micros().to_be_bytes());
    body.push(0);
    frame(b'd', &body)
}

/// One CopyData message of the stream.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum StreamFrame {
    XLog(Vec<u8>),
    KeepAlive { wal_end: Lsn, reply_requested: bool },
}

pub(crate) fn parse_stream_frame(payload: &[u8]) -> Result<StreamFrame> {
    let truncated = || Error::Source("truncated stream frame".into());
    let lsn_at = |at: usize| -> Result<Lsn> {
        Ok(Lsn(u64::from_be_bytes(
            payload
                .get(at..at + 8)
                .ok_or_else(truncated)?
                .try_into()
                .expect("eight bytes"),
        )))
    };
    match payload.first() {
        Some(b'w') => Ok(StreamFrame::XLog(
            payload.get(25..).ok_or_else(truncated)?.to_vec(),
        )),
        Some(b'k') => Ok(StreamFrame::KeepAlive {
            wal_end: lsn_at(1)?,
            reply_requested: *payload.get(17).ok_or_else(truncated)? != 0,
        }),
        other => Err(Error::Source(format!("unknown stream frame {other:?}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options() -> PostgresOptions {
        PostgresOptions {
            host: "h".into(),
            port: 5432,
            user: "u".into(),
            password: "p".into(),
            database: "d".into(),
            tls: false,
            tls_ca: None,
        }
    }

    #[test]
    fn startup_pins_the_text_formats_and_asks_for_replication_only_when_told() {
        let text = |replication| {
            String::from_utf8_lossy(&startup_body(&options(), replication)).into_owned()
        };
        for wanted in [
            "client_encoding\0UTF8",
            "DateStyle\0ISO",
            "TimeZone\0UTC",
            "IntervalStyle\0postgres",
            "extra_float_digits\x003",
            "bytea_output\0hex",
            "lc_monetary\0C",
        ] {
            assert!(text(false).contains(wanted), "{wanted}");
        }
        assert!(text(true).contains("replication\0database"));
        assert!(!text(false).contains("replication"));
    }

    #[test]
    fn scram_key_derivation_matches_a_known_vector() {
        // RFC 7914 section 11 gives PBKDF2-HMAC-SHA256 for "passwd"/"salt", 1 iteration.
        let out = pbkdf2_sha256(b"passwd", b"salt", 1);
        assert_eq!(out[..8], [0x55, 0xac, 0x04, 0x6e, 0x56, 0xe3, 0x08, 0x9f]);
    }

    #[test]
    fn a_missing_or_invalidated_slot_is_history_lost() {
        let lost = |code: &str, message: &str| {
            matches!(
                map_server_error(
                    "start",
                    ServerError {
                        code: code.into(),
                        message: message.into()
                    }
                ),
                Error::HistoryLost(_)
            )
        };
        assert!(lost("42704", "replication slot \"s\" does not exist"));
        assert!(lost(
            "55000",
            "can no longer get changes from replication slot \"s\""
        ));
        assert!(!lost("42P01", "relation does not exist"));
        assert!(!lost("55000", "some other state"));
    }

    #[test]
    fn stream_frames_parse() {
        let mut keepalive = vec![b'k'];
        keepalive.extend_from_slice(&0x1234u64.to_be_bytes());
        keepalive.extend_from_slice(&0u64.to_be_bytes());
        keepalive.push(1);
        assert_eq!(
            parse_stream_frame(&keepalive).unwrap(),
            StreamFrame::KeepAlive {
                wal_end: Lsn(0x1234),
                reply_requested: true
            }
        );
        let mut xlog = vec![b'w'];
        xlog.extend_from_slice(&[0; 24]);
        xlog.extend_from_slice(b"data");
        assert_eq!(
            parse_stream_frame(&xlog).unwrap(),
            StreamFrame::XLog(b"data".to_vec())
        );
        assert!(parse_stream_frame(b"z").is_err());
        assert!(parse_stream_frame(b"k").is_err());
    }

    #[test]
    fn a_status_update_carries_the_position_three_times() {
        let framed = status_update(Lsn(7));
        assert_eq!(framed[0], b'd');
        assert_eq!(framed[5], b'r');
        for at in [6, 14, 22] {
            assert_eq!(framed[at..at + 8], 7u64.to_be_bytes());
        }
    }

    #[test]
    fn quoting() {
        assert_eq!(quote_literal("a'b"), "'a''b'");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    }
}
