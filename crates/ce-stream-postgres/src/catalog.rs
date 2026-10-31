//! What the catalog says of the included tables. A drop or a rename sends nothing
//! on the stream, so the embedder asks here, on a connection of its own.

use ce_stream_core::error::Result;
use serde::{Deserialize, Serialize};

use crate::options::PostgresOptions;
use crate::wire::{quote_literal, Connection};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogColumn {
    pub name: String,
    pub attnum: i32,
    pub atttypid: u32,
    pub atttypmod: i32,
    /// The `xmin` of its `pg_attribute` row: changes when the column does.
    pub xmin: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogPartition {
    pub oid: u32,
    pub schema: String,
    pub name: String,
    pub relreplident: String,
    pub relfilenode: u32,
    pub in_publication: bool,
    /// The `xmin` of its `pg_inherits` row.
    pub inherits_xmin: Option<String>,
    /// The `xmin` of its `pg_type` row.
    pub type_xmin: Option<String>,
}

/// One included table. A table that no longer exists has no entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogTable {
    pub oid: u32,
    pub schema: String,
    pub name: String,
    pub relreplident: String,
    pub relpersistence: String,
    pub relkind: String,
    pub relispartition: bool,
    pub relfilenode: u32,
    pub primary_key: Vec<String>,
    pub in_publication: bool,
    pub columns: Vec<CatalogColumn>,
    /// For a partitioned parent, every table below it.
    pub partitions: Vec<CatalogPartition>,
}

pub(crate) fn catalog_sql(oids: &[u32], publication: &str) -> String {
    let list = oids
        .iter()
        .map(|oid| format!("{oid}::oid"))
        .collect::<Vec<_>>()
        .join(",");
    let publication = quote_literal(publication);
    format!(
        "SELECT json_build_object(
            'oid', c.oid::bigint, 'schema', n.nspname, 'name', c.relname,
            'relreplident', c.relreplident, 'relpersistence', c.relpersistence,
            'relkind', c.relkind, 'relispartition', c.relispartition,
            'relfilenode', c.relfilenode::bigint,
            'primary_key', COALESCE((
                SELECT json_agg(a.attname ORDER BY k.ord)
                FROM pg_index i
                CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                WHERE i.indrelid = c.oid AND i.indisprimary), '[]'::json),
            'in_publication', EXISTS (
                SELECT 1 FROM pg_publication_tables t
                WHERE t.pubname = {publication}
                  AND t.schemaname = n.nspname AND t.tablename = c.relname),
            'columns', COALESCE((
                SELECT json_agg(json_build_object(
                    'name', a.attname, 'attnum', a.attnum, 'atttypid', a.atttypid::bigint,
                    'atttypmod', a.atttypmod, 'xmin', a.xmin::text) ORDER BY a.attnum)
                FROM pg_attribute a
                WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped), '[]'::json),
            'partitions', COALESCE((
                SELECT json_agg(json_build_object(
                    'oid', p.oid::bigint, 'schema', pn.nspname, 'name', p.relname,
                    'relreplident', p.relreplident, 'relfilenode', p.relfilenode::bigint,
                    'in_publication', EXISTS (
                        SELECT 1 FROM pg_publication_tables t
                        WHERE t.pubname = {publication}
                          AND t.schemaname = pn.nspname AND t.tablename = p.relname),
                    'inherits_xmin', (SELECT h.xmin::text FROM pg_inherits h WHERE h.inhrelid = p.oid),
                    'type_xmin', (SELECT y.xmin::text FROM pg_type y WHERE y.oid = p.reltype))
                    ORDER BY p.oid)
                FROM pg_partition_tree(c.oid) tree
                JOIN pg_class p ON p.oid = tree.relid
                JOIN pg_namespace pn ON pn.oid = p.relnamespace
                WHERE tree.relid <> c.oid), '[]'::json))::text
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.oid IN ({list})"
    )
}

pub async fn catalog_tables(
    options: &PostgresOptions,
    oids: &[u32],
    publication: &str,
) -> Result<Vec<CatalogTable>> {
    if oids.is_empty() {
        return Ok(Vec::new());
    }
    let mut conn = Connection::connect(options, false).await?;
    let rows = conn.query(&catalog_sql(oids, publication)).await?;
    let mut tables = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(Some(text)) = row.into_iter().next() {
            tables.push(serde_json::from_str(&text)?);
        }
    }
    Ok(tables)
}
