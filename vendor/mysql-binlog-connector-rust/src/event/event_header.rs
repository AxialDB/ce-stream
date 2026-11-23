use std::io::{Cursor, Read, Seek, SeekFrom};

use byteorder::{LittleEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};

use crate::{binlog_error::BinlogError, constants, ext::cursor_ext::CursorExt};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct EventHeader {
    pub timestamp: u32,
    pub event_type: u8,
    pub server_id: u32,
    pub event_length: u32,
    pub next_event_position: u32,
    pub event_flags: u16,
}

/// The common header of a rows event, with the partition information MySQL
/// writes for a partitioned table (8.0 and later, WL#12168).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowsEventHeader {
    pub table_id: u64,
    pub column_count: usize,
    pub included_columns: Vec<bool>,
    /// The partition the rows are in: the partition's place in the table's
    /// definition when the event was logged, subpartitions counted as
    /// `partition * subpartitions + subpartition`. `None` for a table that is
    /// not partitioned, or a server that does not write it.
    pub partition_id: Option<u16>,
    /// For an update rows event, the partition the rows left.
    pub source_partition_id: Option<u16>,
}

impl EventHeader {
    pub fn parse<S: Read + Seek>(stream: &mut S) -> Result<Self, BinlogError> {
        // refer: https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Log__event__header.html
        let mut buf = [0u8; constants::EVENT_HEADER_LENGTH];
        stream.read_exact(&mut buf)?;

        let mut cursor = Cursor::new(&buf);
        Ok(Self {
            timestamp: cursor.read_u32::<LittleEndian>()?,
            event_type: cursor.read_u8()?,
            server_id: cursor.read_u32::<LittleEndian>()?,
            event_length: cursor.read_u32::<LittleEndian>()?,
            next_event_position: cursor.read_u32::<LittleEndian>()?,
            event_flags: cursor.read_u16::<LittleEndian>()?,
        })
    }

    // Parse the common header for rows events:
    // WriteRows / UpdateRows / DeleteRows
    // ExtWriteRows / ExtUpdateRows / ExtDeleteRows
    //
    // `is_update` says the event is an update rows event, whose partition
    // information also carries the partition the rows left.
    pub fn parse_rows_event_common_header(
        cursor: &mut Cursor<&Vec<u8>>,
        row_event_version: u8,
        is_update: bool,
    ) -> Result<RowsEventHeader, BinlogError> {
        let table_id = cursor.read_u48::<LittleEndian>()?;
        let _flags = cursor.read_u16::<LittleEndian>()?;

        let mut partition_id = None;
        let mut source_partition_id = None;
        // ExtWriteRows/ExtUpdateRows/ExtDeleteRows, version 2, MySQL only: a
        // variable header whose length counts its own two bytes, holding
        // type-tagged extra row information (MySQL's Rows_event constructor in
        // rows_event.cpp). Type 0 is NDB: a length byte that counts itself,
        // then that many bytes less one. Type 1 is PART (WL#12168): the
        // partition the rows are in, two bytes, and for an update rows event
        // the partition they left, two more. An unknown type ends the reading.
        if row_event_version == 2 {
            let var_header_len = cursor.read_u16::<LittleEndian>()? as u64;
            let end = cursor.position() + var_header_len.saturating_sub(2);
            while cursor.position() < end {
                match cursor.read_u8()? {
                    0 => {
                        let ndb_len = cursor.read_u8()? as i64;
                        cursor.seek(SeekFrom::Current((ndb_len - 1).max(0)))?;
                    }
                    1 => {
                        partition_id = Some(cursor.read_u16::<LittleEndian>()?);
                        if is_update {
                            source_partition_id = Some(cursor.read_u16::<LittleEndian>()?);
                        }
                    }
                    _ => break,
                }
            }
            if cursor.position() > end {
                return Err(BinlogError::UnexpectedData(
                    "rows event extra row information runs past its header".into(),
                ));
            }
            cursor.set_position(end);
        }

        let column_count = cursor.read_packed_number()?;
        let included_columns = cursor.read_bits(column_count, false)?;

        Ok(RowsEventHeader {
            table_id,
            column_count,
            included_columns,
            partition_id,
            source_partition_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// table_id 7, flags 1, then the version-2 variable header, then one
    /// column with the included bit set.
    fn rows_header(var_header: &[u8]) -> Vec<u8> {
        let mut bytes = vec![7, 0, 0, 0, 0, 0, 1, 0];
        let len = (var_header.len() + 2) as u16;
        bytes.extend_from_slice(&len.to_le_bytes());
        bytes.extend_from_slice(var_header);
        bytes.push(1); // column count
        bytes.push(0b1); // included columns
        bytes
    }

    fn parse(bytes: &Vec<u8>, version: u8, is_update: bool) -> RowsEventHeader {
        let mut cursor = Cursor::new(bytes);
        let header = EventHeader::parse_rows_event_common_header(&mut cursor, version, is_update)
            .expect("header");
        assert_eq!(
            cursor.available(),
            0,
            "the header reads exactly to the columns"
        );
        header
    }

    #[test]
    fn no_extra_row_information() {
        let header = parse(&rows_header(&[]), 2, false);
        assert_eq!(header.table_id, 7);
        assert_eq!(header.included_columns, vec![true]);
        assert_eq!(header.partition_id, None);
        assert_eq!(header.source_partition_id, None);
    }

    #[test]
    fn partition_of_a_write_and_a_delete() {
        let header = parse(&rows_header(&[1, 3, 0]), 2, false);
        assert_eq!(header.partition_id, Some(3));
        assert_eq!(header.source_partition_id, None);
    }

    #[test]
    fn partitions_of_an_update() {
        let header = parse(&rows_header(&[1, 3, 0, 1, 0]), 2, true);
        assert_eq!(header.partition_id, Some(3));
        assert_eq!(header.source_partition_id, Some(1));
    }

    #[test]
    fn ndb_information_before_the_partition() {
        // NDB: type 0, length 4 counting itself, format byte, two data bytes.
        let header = parse(
            &rows_header(&[0, 4, 9, 0xAA, 0xBB, 1, 258u16 as u8, 1]),
            2,
            false,
        );
        assert_eq!(header.partition_id, Some(258));
    }

    #[test]
    fn unknown_type_ends_the_reading() {
        let header = parse(&rows_header(&[5, 0xFF, 0xFF]), 2, false);
        assert_eq!(header.partition_id, None);
        assert_eq!(header.included_columns, vec![true]);
    }

    #[test]
    fn version_one_has_no_variable_header() {
        let mut bytes = vec![7, 0, 0, 0, 0, 0, 1, 0];
        bytes.push(1);
        bytes.push(0b1);
        let header = parse(&bytes, 1, false);
        assert_eq!(header.partition_id, None);
    }

    #[test]
    fn information_past_the_header_is_an_error() {
        // The variable header says 3 bytes (one after the length), but PART
        // needs two more.
        let bytes = vec![7, 0, 0, 0, 0, 0, 1, 0, 3, 0, 1, 3, 0, 1, 0b1];
        let mut cursor = Cursor::new(&bytes);
        let err = EventHeader::parse_rows_event_common_header(&mut cursor, 2, false)
            .expect_err("runs past");
        assert!(err.to_string().contains("runs past"), "{err}");
    }
}
