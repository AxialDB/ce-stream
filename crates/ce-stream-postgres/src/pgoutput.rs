//! `pgoutput`, protocol version 1, after
//! <https://www.postgresql.org/docs/17/protocol-logicalrep-message-formats.html>.
//!
//! Streaming (2) and two-phase (3) are not requested, so a transaction arrives
//! whole at its commit, and a prepared one at `COMMIT PREPARED`.

use ce_stream_core::error::{Error, Result};

use crate::lsn::Lsn;

/// A column of a Relation message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationColumn {
    pub key: bool,
    pub name: String,
    pub type_oid: u32,
    pub typmod: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TupleValue {
    Null,
    /// A TOASTed value the update did not touch: not sent.
    Unchanged,
    Text(Vec<u8>),
}

pub type Tuple = Vec<TupleValue>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Begin {
        final_lsn: Lsn,
        xid: u32,
    },
    Commit {
        lsn: Lsn,
        end_lsn: Lsn,
    },
    Origin,
    Relation {
        oid: u32,
        namespace: String,
        name: String,
        replica_identity: u8,
        columns: Vec<RelationColumn>,
    },
    Type {
        oid: u32,
        namespace: String,
        name: String,
    },
    Insert {
        relation: u32,
        new: Tuple,
    },
    Update {
        relation: u32,
        /// The whole old row, under `REPLICA IDENTITY FULL`.
        old: Option<Tuple>,
        /// Only the key: another identity.
        key: Option<Tuple>,
        new: Tuple,
    },
    Delete {
        relation: u32,
        old: Option<Tuple>,
        key: Option<Tuple>,
    },
    Truncate {
        relations: Vec<u32>,
    },
    Logical {
        transactional: bool,
        lsn: Lsn,
        prefix: String,
        content: Vec<u8>,
    },
}

struct Cursor<'a>(&'a [u8]);

fn truncated() -> Error {
    Error::Source("pgoutput: truncated message".into())
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.0.len() < n {
            return Err(truncated());
        }
        let (head, tail) = self.0.split_at(n);
        self.0 = tail;
        Ok(head)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn i16(&mut self) -> Result<i16> {
        Ok(i16::from_be_bytes(
            self.take(2)?.try_into().expect("two bytes"),
        ))
    }

    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(
            self.take(4)?.try_into().expect("four bytes"),
        ))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(self.i32()? as u32)
    }

    fn lsn(&mut self) -> Result<Lsn> {
        Ok(Lsn(u64::from_be_bytes(
            self.take(8)?.try_into().expect("eight bytes"),
        )))
    }

    fn string(&mut self) -> Result<String> {
        let end = self.0.iter().position(|&b| b == 0).ok_or_else(truncated)?;
        let text = String::from_utf8_lossy(&self.0[..end]).into_owned();
        self.0 = &self.0[end + 1..];
        Ok(text)
    }

    fn tuple(&mut self) -> Result<Tuple> {
        let count = self.i16()?;
        let mut values = Vec::with_capacity(count.max(0) as usize);
        for _ in 0..count {
            values.push(match self.u8()? {
                b'n' => TupleValue::Null,
                b'u' => TupleValue::Unchanged,
                b't' => {
                    let len = self.i32()?;
                    TupleValue::Text(self.take(len.max(0) as usize)?.to_vec())
                }
                other => {
                    return Err(Error::Source(format!(
                        "pgoutput: unknown column kind {:?}",
                        other as char
                    )))
                }
            });
        }
        Ok(values)
    }
}

pub fn decode(data: &[u8]) -> Result<Message> {
    let mut c = Cursor(data);
    let tag = c.u8()?;
    let message = match tag {
        b'B' => {
            let final_lsn = c.lsn()?;
            let _commit_time = c.take(8)?;
            Message::Begin {
                final_lsn,
                xid: c.u32()?,
            }
        }
        b'C' => {
            let _flags = c.u8()?;
            let lsn = c.lsn()?;
            let end_lsn = c.lsn()?;
            let _commit_time = c.take(8)?;
            Message::Commit { lsn, end_lsn }
        }
        b'O' => {
            let _lsn = c.lsn()?;
            let _name = c.string()?;
            Message::Origin
        }
        b'R' => {
            let oid = c.u32()?;
            let namespace = c.string()?;
            let name = c.string()?;
            let replica_identity = c.u8()?;
            let count = c.i16()?;
            let mut columns = Vec::with_capacity(count.max(0) as usize);
            for _ in 0..count {
                columns.push(RelationColumn {
                    key: c.u8()? & 1 != 0,
                    name: c.string()?,
                    type_oid: c.u32()?,
                    typmod: c.i32()?,
                });
            }
            Message::Relation {
                oid,
                namespace,
                name,
                replica_identity,
                columns,
            }
        }
        b'Y' => Message::Type {
            oid: c.u32()?,
            namespace: c.string()?,
            name: c.string()?,
        },
        b'I' => {
            let relation = c.u32()?;
            match c.u8()? {
                b'N' => {}
                other => {
                    return Err(Error::Source(format!(
                        "pgoutput: Insert expects N, got {:?}",
                        other as char
                    )))
                }
            }
            Message::Insert {
                relation,
                new: c.tuple()?,
            }
        }
        b'U' => {
            let relation = c.u32()?;
            let mut old = None;
            let mut key = None;
            let mut kind = c.u8()?;
            if kind == b'O' {
                old = Some(c.tuple()?);
                kind = c.u8()?;
            } else if kind == b'K' {
                key = Some(c.tuple()?);
                kind = c.u8()?;
            }
            if kind != b'N' {
                return Err(Error::Source(format!(
                    "pgoutput: Update expects N, got {:?}",
                    kind as char
                )));
            }
            Message::Update {
                relation,
                old,
                key,
                new: c.tuple()?,
            }
        }
        b'D' => {
            let relation = c.u32()?;
            match c.u8()? {
                b'O' => Message::Delete {
                    relation,
                    old: Some(c.tuple()?),
                    key: None,
                },
                b'K' => Message::Delete {
                    relation,
                    old: None,
                    key: Some(c.tuple()?),
                },
                other => {
                    return Err(Error::Source(format!(
                        "pgoutput: Delete expects O or K, got {:?}",
                        other as char
                    )))
                }
            }
        }
        b'T' => {
            let count = c.i32()?;
            let _options = c.u8()?;
            let mut relations = Vec::with_capacity(count.max(0) as usize);
            for _ in 0..count {
                relations.push(c.u32()?);
            }
            Message::Truncate { relations }
        }
        b'M' => {
            let flags = c.u8()?;
            let lsn = c.lsn()?;
            let prefix = c.string()?;
            let len = c.i32()?;
            Message::Logical {
                transactional: flags & 1 != 0,
                lsn,
                prefix,
                content: c.take(len.max(0) as usize)?.to_vec(),
            }
        }
        other => {
            return Err(Error::Source(format!(
                "pgoutput: unknown message {:?}",
                other as char
            )))
        }
    };
    if !c.0.is_empty() {
        return Err(Error::Source(format!(
            "pgoutput: {} bytes left after {:?}",
            c.0.len(),
            tag as char
        )));
    }
    Ok(message)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Messages as the server sends them, for tests here and in the stream.
    pub(crate) fn tuple_bytes(values: &[Option<&str>]) -> Vec<u8> {
        let mut out = (values.len() as i16).to_be_bytes().to_vec();
        for value in values {
            match value {
                None => out.push(b'n'),
                Some("\u{1}unchanged") => out.push(b'u'),
                Some(text) => {
                    out.push(b't');
                    out.extend_from_slice(&(text.len() as i32).to_be_bytes());
                    out.extend_from_slice(text.as_bytes());
                }
            }
        }
        out
    }

    #[test]
    fn begin_and_commit() {
        let mut begin = vec![b'B'];
        begin.extend_from_slice(&0x10u64.to_be_bytes());
        begin.extend_from_slice(&0i64.to_be_bytes());
        begin.extend_from_slice(&7i32.to_be_bytes());
        assert_eq!(
            decode(&begin).unwrap(),
            Message::Begin {
                final_lsn: Lsn(0x10),
                xid: 7
            }
        );
        let mut commit = vec![b'C', 0];
        commit.extend_from_slice(&0x10u64.to_be_bytes());
        commit.extend_from_slice(&0x18u64.to_be_bytes());
        commit.extend_from_slice(&0i64.to_be_bytes());
        assert_eq!(
            decode(&commit).unwrap(),
            Message::Commit {
                lsn: Lsn(0x10),
                end_lsn: Lsn(0x18)
            }
        );
    }

    #[test]
    fn relation_with_key_flags_and_typmod() {
        let mut data = vec![b'R'];
        data.extend_from_slice(&16420i32.to_be_bytes());
        data.extend_from_slice(b"public\0orders\0f");
        data.extend_from_slice(&2i16.to_be_bytes());
        data.push(1);
        data.extend_from_slice(b"id\0");
        data.extend_from_slice(&23i32.to_be_bytes());
        data.extend_from_slice(&(-1i32).to_be_bytes());
        data.push(0);
        data.extend_from_slice(b"price\0");
        data.extend_from_slice(&1700i32.to_be_bytes());
        data.extend_from_slice(&655_366i32.to_be_bytes());
        let Message::Relation {
            oid,
            namespace,
            name,
            replica_identity,
            columns,
        } = decode(&data).unwrap()
        else {
            panic!("expected a relation");
        };
        assert_eq!(
            (oid, namespace.as_str(), name.as_str()),
            (16420, "public", "orders")
        );
        assert_eq!(replica_identity, b'f');
        assert!(columns[0].key && !columns[1].key);
        assert_eq!(columns[1].typmod, 655_366);
    }

    #[test]
    fn an_update_has_the_old_row_and_the_new_one() {
        let mut data = vec![b'U'];
        data.extend_from_slice(&1i32.to_be_bytes());
        data.push(b'O');
        data.extend(tuple_bytes(&[Some("1"), Some("big")]));
        data.push(b'N');
        data.extend(tuple_bytes(&[Some("1"), Some("\u{1}unchanged")]));
        let Message::Update { old, key, new, .. } = decode(&data).unwrap() else {
            panic!("expected an update");
        };
        assert_eq!(key, None);
        assert_eq!(old.unwrap()[1], TupleValue::Text(b"big".to_vec()));
        assert_eq!(new[1], TupleValue::Unchanged);
    }

    #[test]
    fn an_update_without_an_old_row() {
        let mut data = vec![b'U'];
        data.extend_from_slice(&1i32.to_be_bytes());
        data.push(b'N');
        data.extend(tuple_bytes(&[None]));
        let Message::Update { old, key, new, .. } = decode(&data).unwrap() else {
            panic!("expected an update");
        };
        assert!(old.is_none() && key.is_none());
        assert_eq!(new, vec![TupleValue::Null]);
    }

    #[test]
    fn a_delete_by_key_or_by_old_row() {
        let mut data = vec![b'D'];
        data.extend_from_slice(&1i32.to_be_bytes());
        data.push(b'K');
        data.extend(tuple_bytes(&[Some("1")]));
        assert!(matches!(
            decode(&data).unwrap(),
            Message::Delete {
                old: None,
                key: Some(_),
                ..
            }
        ));
    }

    #[test]
    fn truncate_lists_its_relations() {
        let mut data = vec![b'T'];
        data.extend_from_slice(&2i32.to_be_bytes());
        data.push(0);
        data.extend_from_slice(&1i32.to_be_bytes());
        data.extend_from_slice(&2i32.to_be_bytes());
        assert_eq!(
            decode(&data).unwrap(),
            Message::Truncate {
                relations: vec![1, 2]
            }
        );
    }

    #[test]
    fn a_logical_message() {
        let mut data = vec![b'M', 1];
        data.extend_from_slice(&0x20u64.to_be_bytes());
        data.extend_from_slice(b"axialdb_ddl\0");
        data.extend_from_slice(&3i32.to_be_bytes());
        data.extend_from_slice(&[0, 255, 7]);
        assert_eq!(
            decode(&data).unwrap(),
            Message::Logical {
                transactional: true,
                lsn: Lsn(0x20),
                prefix: "axialdb_ddl".into(),
                content: vec![0, 255, 7]
            }
        );
    }

    #[test]
    fn nonsense_is_an_error_not_a_guess() {
        assert!(decode(b"Z").is_err());
        assert!(decode(b"").is_err());
        assert!(decode(b"B\0").is_err());
        let mut trailing = vec![b'T'];
        trailing.extend_from_slice(&0i32.to_be_bytes());
        trailing.extend_from_slice(&[0, 9]);
        assert!(decode(&trailing).is_err());
    }
}
