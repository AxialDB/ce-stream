# Patches applied on top of apecloud/mysql-binlog-connector-rust @ f7cca8ec

`ce-stream-mysql-binlog` is the fork that [ce-stream](https://github.com/AxialDB/ce-stream) vendors for its MySQL source. Its changes are in ce-stream's [`CHANGELOG.md`](https://github.com/AxialDB/ce-stream/blob/main/CHANGELOG.md).

## 0.1.0

1. `src/command/command_util.rs` — `show master status` → `show binary log status`
   (MySQL 8.4+ / 9.x; ce-stream does not support older MySQL.)

2. `src/lib.rs` — `#![allow(dead_code)]` to silence unused protocol-field warnings from upstream.

## 0.2.0 (with ce-stream v0.6.0)

3. `src/event/event_header.rs`, `src/event/{write,update,delete}_rows_event.rs` — the partition id of a rows event:
   `partition_id` on `WriteRowsEvent`, `UpdateRowsEvent` and `DeleteRowsEvent`, and `source_partition_id` on `UpdateRowsEvent`.
   `EventHeader::parse_rows_event_common_header` takes `is_update: bool` and returns `RowsEventHeader`. **Breaking** for struct literals and callers of that function.

4. `src/event/xa_prepare_event.rs` — `XaPrepareEvent::one_phase` was inverted. It is true for `XA COMMIT ... ONE PHASE` and false for `XA PREPARE`.

5. `src/column/json/json_binary.rs`, `src/column/json/json_formatter.rs` — a negative `TIME` inside a `JSON` value is the negative of the whole packed value, not of the hour alone.
   New `JsonFormatter::value_negative_time`, whose default calls `value_time` with the hour negated.
