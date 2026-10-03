//! Connect-time checks. Pure helpers are unit-tested; the async check talks to the server.

use ce_stream_core::error::{Error, Result};
use ce_stream_core::event::TableRef;
use futures_util::StreamExt;
use mongodb::bson::{doc, Document};
use mongodb::Client;

use crate::options::FullDocumentMode;

pub struct GateReport {
    pub warnings: Vec<String>,
}

pub fn topology_ok(hello: &Document) -> Result<()> {
    if hello.get_str("msg").ok() == Some("isdbgrid") {
        return Err(Error::Source(
            "sharded clusters are not supported yet; v1 is a replica set".into(),
        ));
    }
    match hello.get_str("setName") {
        Ok(name) if !name.is_empty() => Ok(()),
        _ => Err(Error::Source(
            "standalone mongod has no change streams; start a replica set (a one-node set is enough)".into(),
        )),
    }
}

pub fn version_ok(version: &str) -> Result<()> {
    let major = version
        .split(['.', '-'])
        .next()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0);
    if major >= 8 {
        Ok(())
    } else {
        Err(Error::Source(format!(
            "MongoDB {version} is below the supported floor (8.0)"
        )))
    }
}

pub fn post_images_enabled(collection_info: &Document) -> bool {
    collection_info
        .get_document("options")
        .ok()
        .and_then(|o| o.get_document("changeStreamPreAndPostImages").ok())
        .and_then(|p| p.get_bool("enabled").ok())
        .unwrap_or(false)
}

/// `connectionStatus` with `showPrivileges: true`. No authenticated user means auth is off.
pub fn privileges_ok(status: &Document, db: &str) -> Result<()> {
    let auth = match status.get_document("authInfo") {
        Ok(auth) => auth,
        Err(_) => return Ok(()),
    };
    let users = auth.get_array("authenticatedUsers").ok();
    if users.map(|u| u.is_empty()).unwrap_or(true) {
        return Ok(());
    }
    let privs = auth
        .get_array("authenticatedUserPrivileges")
        .map_err(|_| Error::Source("authenticated session did not report privileges".into()))?;
    let mut find = false;
    let mut change_stream = false;
    for entry in privs {
        let Some(entry) = entry.as_document() else {
            continue;
        };
        let Ok(resource) = entry.get_document("resource") else {
            continue;
        };
        if !resource_covers(resource, db) {
            continue;
        }
        let Ok(actions) = entry.get_array("actions") else {
            continue;
        };
        for action in actions {
            match action.as_str() {
                Some("find") => find = true,
                Some("changeStream") => change_stream = true,
                _ => {}
            }
        }
    }
    if find && change_stream {
        Ok(())
    } else {
        Err(Error::Source(format!(
            "capture user needs find and changeStream on database {db}"
        )))
    }
}

fn resource_covers(resource: &Document, db: &str) -> bool {
    if resource.get_bool("anyResource").ok() == Some(true)
        || resource.get_bool("cluster").ok() == Some(true)
    {
        return true;
    }
    matches!(resource.get_str("db"), Ok(name) if name.is_empty() || name == db)
}

pub fn oplog_window_secs(oldest_t: u32, newest_t: u32) -> u32 {
    newest_t.saturating_sub(oldest_t)
}

pub async fn validate_capture_gates(
    client: &Client,
    db: &str,
    watched: &[TableRef],
    mode: FullDocumentMode,
) -> Result<GateReport> {
    let mut warnings = Vec::new();
    let hello = client
        .database("admin")
        .run_command(doc! { "hello": 1 })
        .await
        .map_err(|e| Error::Source(format!("hello: {e}")))?;
    topology_ok(&hello)?;

    let info = client
        .database("admin")
        .run_command(doc! { "buildInfo": 1 })
        .await
        .map_err(|e| Error::Source(format!("buildInfo: {e}")))?;
    let version = info
        .get_str("version")
        .map_err(|_| Error::Source("buildInfo has no version".into()))?;
    version_ok(version)?;

    let status = client
        .database("admin")
        .run_command(doc! { "connectionStatus": 1, "showPrivileges": true })
        .await
        .map_err(|e| Error::Source(format!("connectionStatus: {e}")))?;
    privileges_ok(&status, db)?;

    if mode == FullDocumentMode::Required {
        for table in watched.iter().filter(|t| t.database == db) {
            let mut cursor = client
                .database(db)
                .list_collections()
                .filter(doc! { "name": &table.table })
                .await
                .map_err(|e| Error::Source(format!("listCollections: {e}")))?;
            let info = cursor
                .next()
                .await
                .transpose()
                .map_err(|e| Error::Source(format!("listCollections: {e}")))?
                .ok_or_else(|| {
                    Error::Source(format!("collection {db}.{} does not exist", table.table))
                })?;
            let options_doc = mongodb::bson::to_document(&info.options).unwrap_or_default();
            let wrapped = doc! { "options": options_doc };
            if !post_images_enabled(&wrapped) {
                return Err(Error::Source(format!(
                    "enable changeStreamPreAndPostImages on {db}.{} or use update_lookup",
                    table.table
                )));
            }
        }
    }

    match oplog_bounds(client).await {
        Ok((oldest, newest)) => {
            let secs = oplog_window_secs(oldest, newest);
            warnings.push(format!("oplog window is {secs}s"));
        }
        Err(msg) => warnings.push(msg),
    }
    Ok(GateReport { warnings })
}

async fn oplog_bounds(client: &Client) -> std::result::Result<(u32, u32), String> {
    let oplog = client.database("local").collection::<Document>("oplog.rs");
    let oldest = oplog
        .find_one(doc! {})
        .sort(doc! { "$natural": 1 })
        .await
        .map_err(|e| format!("oplog window not checked ({e})"))?
        .ok_or_else(|| "oplog window not checked (oplog is empty)".to_string())?;
    let newest = oplog
        .find_one(doc! {})
        .sort(doc! { "$natural": -1 })
        .await
        .map_err(|e| format!("oplog window not checked ({e})"))?
        .ok_or_else(|| "oplog window not checked (oplog is empty)".to_string())?;
    let oldest_t = timestamp_secs(&oldest)?;
    let newest_t = timestamp_secs(&newest)?;
    Ok((oldest_t, newest_t))
}

fn timestamp_secs(entry: &Document) -> std::result::Result<u32, String> {
    entry
        .get_timestamp("ts")
        .map(|ts| ts.time)
        .map_err(|_| "oplog window not checked (entry has no ts)".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::doc;

    #[test]
    fn topology_and_version() {
        assert!(topology_ok(&doc! { "setName": "rs0" }).is_ok());
        assert!(topology_ok(&doc! { "ismaster": true }).is_err());
        assert!(topology_ok(&doc! { "msg": "isdbgrid", "setName": "rs0" }).is_err());
        assert!(version_ok("8.0.32").is_ok());
        assert!(version_ok("8.3.11").is_ok());
        assert!(version_ok("9.0.2").is_ok());
        assert!(version_ok("7.0.43").is_err());
    }

    #[test]
    fn privileges_and_post_images() {
        let open =
            doc! { "authInfo": { "authenticatedUsers": [], "authenticatedUserPrivileges": [] } };
        assert!(privileges_ok(&open, "app").is_ok());
        let ok = doc! {
            "authInfo": {
                "authenticatedUsers": [ { "user": "cdc", "db": "admin" } ],
                "authenticatedUserPrivileges": [ {
                    "resource": { "db": "app", "collection": "" },
                    "actions": [ "find", "changeStream" ]
                } ]
            }
        };
        assert!(privileges_ok(&ok, "app").is_ok());
        let missing = doc! {
            "authInfo": {
                "authenticatedUsers": [ { "user": "cdc", "db": "admin" } ],
                "authenticatedUserPrivileges": [ {
                    "resource": { "db": "app", "collection": "" },
                    "actions": [ "find" ]
                } ]
            }
        };
        assert!(privileges_ok(&missing, "app").is_err());
        let info = doc! { "name": "orders", "options": { "changeStreamPreAndPostImages": { "enabled": true } } };
        assert!(post_images_enabled(&info));
        assert!(!post_images_enabled(&doc! { "name": "orders" }));
        assert_eq!(oplog_window_secs(100, 250), 150);
    }
}
