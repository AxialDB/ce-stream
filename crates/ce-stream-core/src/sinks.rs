//! Built-in sinks: stdout and HTTP (JSON or optional Avro).

use async_trait::async_trait;
use tracing::{debug, info};

use crate::avro_encode::{self, CONTENT_TYPE_AVRO, CONTENT_TYPE_COMMITTED_TXN_AVRO, SCHEMA_ID};
use crate::error::{Error, Result};
use crate::event::{CloudEvent, SinkFormat};
use crate::transaction::CommittedTransaction;
use crate::Sink;

pub struct StdoutSink {
    format: SinkFormat,
}

impl StdoutSink {
    pub fn new(format: SinkFormat) -> Self {
        Self { format }
    }
}

impl Default for StdoutSink {
    fn default() -> Self {
        Self::new(SinkFormat::Json)
    }
}

#[async_trait]
impl Sink for StdoutSink {
    async fn emit(&self, event: &CloudEvent) -> Result<()> {
        match self.format {
            SinkFormat::Json => {
                let line = serde_json::to_string(event).map_err(|e| Error::Sink(e.to_string()))?;
                println!("{line}");
            }
            SinkFormat::Avro => {
                // Binary on a TTY is hostile; emit one base64 line per event.
                let bytes = avro_encode::encode_cloudevent(event)?;
                let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes);
                println!("{b64}");
            }
        }
        Ok(())
    }
}

/// POST CloudEvents to a webhook URL (JSON or Avro).
pub struct HttpSink {
    client: reqwest::Client,
    url: String,
    format: SinkFormat,
    /// Extra headers (e.g. Authorization).
    headers: Vec<(String, String)>,
}

impl HttpSink {
    pub fn new(url: impl Into<String>) -> Result<Self> {
        Self::with_format(url, SinkFormat::Json)
    }

    pub fn with_format(url: impl Into<String>, format: SinkFormat) -> Result<Self> {
        let url = url.into();
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(Error::Config(format!(
                "sink.url must be http(s), got: {url}"
            )));
        }
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| Error::Sink(e.to_string()))?;
        Ok(Self {
            client,
            url,
            format,
            headers: Vec::new(),
        })
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// POST a JSON body (e.g. [`CommittedTransaction`] in transaction delivery mode).
    pub async fn post_json<T: serde::Serialize>(&self, body: &T) -> Result<()> {
        let mut req = self
            .client
            .post(&self.url)
            .header("content-type", "application/json")
            .json(body);
        for (k, v) in &self.headers {
            req = req.header(k, v);
        }
        let resp = req.send().await.map_err(|e| Error::Sink(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(Error::Sink(format!(
                "HTTP {status} from {}: {}",
                self.url,
                truncate(&text, 200)
            )));
        }
        debug!(%status, url = %self.url, "http json post ok");
        Ok(())
    }

    /// POST one committed transaction envelope (JSON or Avro depending on sink format).
    pub async fn post_committed_transaction(&self, txn: &CommittedTransaction) -> Result<()> {
        match self.format {
            SinkFormat::Json => self.post_json(txn).await,
            SinkFormat::Avro => {
                let body = avro_encode::encode_committed_transaction(txn)?;
                let schema_id = avro_encode::committed_transaction_schema_id(txn);
                let mut req = self
                    .client
                    .post(&self.url)
                    .header("content-type", CONTENT_TYPE_COMMITTED_TXN_AVRO)
                    .header("x-ce-stream-avro-schema", schema_id)
                    .body(body);
                for (k, v) in &self.headers {
                    req = req.header(k, v);
                }
                let resp = req.send().await.map_err(|e| Error::Sink(e.to_string()))?;
                let status = resp.status();
                if !status.is_success() {
                    let text = resp.text().await.unwrap_or_default();
                    return Err(Error::Sink(format!(
                        "HTTP {status} from {}: {}",
                        self.url,
                        truncate(&text, 200)
                    )));
                }
                debug!(%status, url = %self.url, gtid = %txn.gtid, "http committed transaction avro post ok");
                Ok(())
            }
        }
    }
}

#[async_trait]
impl Sink for HttpSink {
    async fn emit(&self, event: &CloudEvent) -> Result<()> {
        let mut req = self.client.post(&self.url);
        match self.format {
            SinkFormat::Json => {
                let body = event.to_structured_json();
                req = req
                    .header("content-type", "application/cloudevents+json")
                    .json(&body);
            }
            SinkFormat::Avro => {
                let body = avro_encode::encode_cloudevent(event)?;
                req = req
                    .header("content-type", CONTENT_TYPE_AVRO)
                    .header("x-ce-stream-avro-schema", SCHEMA_ID)
                    .body(body);
            }
        }
        for (k, v) in &self.headers {
            req = req.header(k, v);
        }
        let resp = req.send().await.map_err(|e| Error::Sink(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(Error::Sink(format!(
                "HTTP {status} from {}: {}",
                self.url,
                truncate(&text, 200)
            )));
        }
        debug!(%status, url = %self.url, id = %event.id, format = ?self.format, "http sink ok");
        info!(
            target: "ce_stream::health",
            sink = "http",
            format = ?self.format,
            event_id = %event.id,
            subject = %event.subject,
            "event delivered"
        );
        Ok(())
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}...", &s[..max])
    }
}
