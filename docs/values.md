# Values

What a value of each host type looks like in a row image (`data.before`, `data.after`). The contract for every adapter ([#19](https://github.com/AxialDB/ce-stream/issues/19)).

**Text is delivered as valid UTF-8 holding the characters the source holds.** A source whose text cannot be delivered that way is refused by the gate. A value that cannot be decoded is an error naming the column, never a `hex:` string.

**Types travel apart from the values.** A value alone does not say what it is: `1767323045000000` can be an instant or a large number. MySQL and PostgreSQL say it in a `Schema` delivery, as each column's `host_type`. A MongoDB collection has no columns, so each row says it in `types` ([below](#mongodb)).

| Kind | JSON | MySQL | PostgreSQL | MongoDB |
|------|------|-------|------------|---------|
| Whole number | number | `TINYINT` to `BIGINT`, `YEAR`, `BIT` | `smallint`, `integer`, `bigint` | Int32, Int64 |
| Floating point | number; text for what JSON has no number for | `FLOAT`, `DOUBLE` | `real`, `double precision`: `NaN`, `Infinity`, `-Infinity` stay strings | Double, likewise |
| Boolean | `true`, `false` | `TINYINT(1)` is a whole number: MySQL has no boolean in the binlog | `boolean` | Boolean |
| Decimal | string, exact | `DECIMAL` | `numeric` (`NaN` included), `money` under `lc_monetary=C` | Decimal128 |
| Date, time | string in the host's fixed format | `DATE`, `TIME` | `date`, `time`, `timetz`, `interval` under `IntervalStyle=postgres` | |
| Date-time with no zone | string | `DATETIME` | `timestamp`, under `DateStyle=ISO` | |
| An instant | number: microseconds since the epoch, UTC | `TIMESTAMP` | `timestamptz`; `infinity` and `-infinity` stay strings | Date |
| Text | string | `CHAR`, `VARCHAR`, `TEXT`, from the column's charset; `ENUM` and `SET` as their labels, a `SET` joined by commas | `text`, `varchar`, `char`, `name`, enums, `uuid`, `inet`, `xml` | String, Symbol, ObjectId as 24 hex characters |
| JSON | string, as the host prints it | `JSON`, with the key order and spacing MySQL returns | `json`, `jsonb` | |
| Nested | object, array | | | Object, Array, holding values of these same forms |
| Binary | string `hex:<lowercase hex>` | `BINARY`, `VARBINARY`, `BLOB` | `bytea` | Binary |
| Other | string, as the host prints it | | arrays and every other type, under the pinned settings | Regex, internal Timestamp, MinKey, and the rest, in extended JSON |
| `NULL` | `null` | | | Null |

## MySQL

`binlog_row_metadata = FULL` gives each column's charset. These are decoded, each matched against MySQL's own `CONVERT` on every byte sequence: `utf8mb4`, `utf8mb3`, `ascii`, `latin1`, `latin2`, `latin7`, `cp1250`, `cp1251`, `cp1256`, `cp1257`, `koi8r`, `macroman`, `cp932`, `euckr`, `gbk`. A column in any other charset is left out of the row, so that one unused column does not make a table unusable. A consumer that needs it refuses it by its own catalog.

## PostgreSQL

Every connection starts with these settings, so the server's text has one format:

| Setting | Value |
|---------|-------|
| `client_encoding` | `UTF8`: the server converts from the database's encoding |
| `DateStyle` | `ISO` |
| `TimeZone` | `UTC` |
| `IntervalStyle` | `postgres` |
| `extra_float_digits` | `3` |
| `bytea_output` | `hex` |
| `lc_monetary` | `C` |

The gate refuses a `SQL_ASCII` database: it declares no encoding, and one byte that is not UTF-8 ends the replication connection at that change.

## MongoDB

BSON strings are UTF-8. A collection has no schema, and two documents can hold different types in one field, so MongoDB delivers no `Schema`. Each row has `types` beside its image, for the values whose form does not show their type:

```json
{
  "op": "insert",
  "after": {
    "_id": "65a1b2c3d4e5f60718293a4b",
    "qty": 7,
    "price": "12.50",
    "at": 1767323045000000,
    "photo": "hex:0102ff",
    "note": "a",
    "customer": {"name": "b", "since": 1704067200000000},
    "items": ["x", 1704067200000000]
  },
  "types": {
    "_id": "objectId",
    "price": "decimal",
    "at": "date",
    "photo": "binary",
    "customer": {"since": "date"},
    "items": [null, "date"]
  }
}
```

- **`types` mirrors the shape of the document**, not dotted paths, since a field name can hold a dot. A nested object or array appears only when something in it has an entry, and `null` holds the place of an array element with none. A row with nothing to say has no `types`.
- **No entry:** a string, a boolean, a null, and a number. `7` is whole and `7.0` is a double.
- **Entries:** `objectId`, `decimal`, `date`, `binary` (`binary:04` for a subtype other than generic), `symbol`, `regex`, `javascript`, `javascriptWithScope`, `timestamp`, `minKey`, `maxKey`, `undefined`, `dbPointer`, and `double` for `NaN` and the infinities, which are strings.
- **A date past what microseconds hold** is a string of its milliseconds, `9223372036854775807 ms`.
- **A delete** has the document key as `before`, with `types` for it. `payload_mode = "signal"` has neither.
