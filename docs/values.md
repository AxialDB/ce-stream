# Values

What a value of each host type looks like in a row image (`data.before`, `data.after`). The contract for every adapter ([#19](https://github.com/AxialDB/ce-stream/issues/19)).

**Text is delivered as valid UTF-8 holding the characters the source holds.** A source whose text cannot be delivered that way is refused by the gate. A value that cannot be decoded is an error naming the column, never a `hex:` string.

| Kind | JSON | MySQL | PostgreSQL |
|------|------|-------|------------|
| Whole number | number | `TINYINT` to `BIGINT`, `YEAR`, `BIT` | `smallint`, `integer`, `bigint` |
| Floating point | number; text for what JSON has no number for | `FLOAT`, `DOUBLE` | `real`, `double precision`: `NaN`, `Infinity`, `-Infinity` stay strings |
| Boolean | `true`, `false` | `TINYINT(1)` is a whole number: MySQL has no boolean in the binlog | `boolean` |
| Decimal | string, exact | `DECIMAL` | `numeric` (`NaN` included), `money` under `lc_monetary=C` |
| Date, time | string in the host's fixed format | `DATE`, `TIME` | `date`, `time`, `timetz`, `interval` under `IntervalStyle=postgres` |
| Date-time with no zone | string | `DATETIME` | `timestamp`, under `DateStyle=ISO` |
| An instant | number: microseconds since the epoch, UTC | `TIMESTAMP` | `timestamptz`; `infinity` and `-infinity` stay strings |
| Text | string | `CHAR`, `VARCHAR`, `TEXT`, from the column's charset; `ENUM` and `SET` as their labels, a `SET` joined by commas | `text`, `varchar`, `char`, `name`, enums, `uuid`, `inet`, `xml` |
| JSON | string, as the host prints it | `JSON`, with the key order and spacing MySQL returns | `json`, `jsonb` |
| Binary | string `hex:<lowercase hex>` | `BINARY`, `VARBINARY`, `BLOB` | `bytea` |
| Other | string, as the host prints it | | arrays and every other type, under the pinned settings |
| `NULL` | `null` | | |

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

BSON strings are UTF-8. Values are canonical extended JSON: a type tag stands beside the value, since a collection has no schema to read it by (`{"$numberLong": "7"}`, `{"$date": {"$numberLong": "1700000000000"}}`, `{"$binary": {...}}`). A document's `data` has no `hex:` strings and no bare 64-bit numbers.
