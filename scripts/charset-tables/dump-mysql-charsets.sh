#!/usr/bin/env bash
# What MySQL itself makes of each byte sequence, per character set, as the table
# `tests/charsets.rs` in ce-stream-mysql holds the decoders against.
#
#   MYSQL="mysql -h127.0.0.1 -P3310 -uroot -pSECRET" OUT=/tmp/charset-tables \
#     bash scripts/charset-tables/dump-mysql-charsets.sh
#
# Each output line is `<charset> <hex of the bytes> <hex of the utf8mb4 text>`.
# A sequence MySQL calls invalid is left out: it cannot be stored in the column.
# Needs a scratch database; drops it again.
set -euo pipefail

: "${MYSQL:?set MYSQL to the mysql command line}"
OUT="${OUT:-/tmp/charset-tables}"
DB=ce_stream_charset_tables
mkdir -p "$OUT"

M="$MYSQL --silent --skip-column-names --default-character-set=utf8mb4"

$M -e "DROP DATABASE IF EXISTS $DB; CREATE DATABASE $DB; CREATE TABLE $DB.seq (h VARCHAR(8) PRIMARY KEY)" 2>/dev/null

load() { # the sequences in table seq: one byte, or one and two bytes
  $M -e "TRUNCATE TABLE $DB.seq" 2>/dev/null
  $M "$DB" 2>/dev/null <<SQL
INSERT INTO seq
WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < 255)
SELECT LPAD(HEX(i), 2, '0') FROM n;
SQL
  if [ "$1" = two ]; then
    $M "$DB" 2>/dev/null <<SQL
SET SESSION cte_max_recursion_depth = 70000;
INSERT INTO seq
WITH RECURSIVE n(i) AS (SELECT 32768 UNION ALL SELECT i + 1 FROM n WHERE i < 65535)
SELECT LPAD(HEX(i), 4, '0') FROM n;
SQL
  fi
}

dump() { # charset: the sequences that hold exactly one character in it
  local cs=$1
  $M "$DB" 2>/dev/null <<SQL
SELECT '$cs', h, HEX(CONVERT(CONVERT(UNHEX(h) USING $cs) USING utf8mb4))
FROM seq
WHERE CONVERT(UNHEX(h) USING $cs) IS NOT NULL
  AND CHAR_LENGTH(CONVERT(UNHEX(h) USING $cs)) = 1;
SQL
}

: > "$OUT/single.txt"
load one
for cs in armscii8 ascii cp1250 cp1251 cp1256 cp1257 cp850 cp852 cp866 dec8 geostd8 greek \
          hebrew hp8 keybcs2 koi8r koi8u latin1 latin2 latin5 latin7 macce macroman swe7 tis620; do
  dump "$cs" >> "$OUT/single.txt"
done

: > "$OUT/double.txt"
load two
for cs in big5 cp932 eucjpms euckr gb18030 gb2312 gbk sjis ujis; do
  dump "$cs" >> "$OUT/double.txt"
done

$M -e "SET SESSION group_concat_max_len = 100000; SELECT CHARACTER_SET_NAME, GROUP_CONCAT(ID ORDER BY ID) FROM information_schema.COLLATIONS GROUP BY CHARACTER_SET_NAME ORDER BY 1" 2>/dev/null > "$OUT/collations.txt"

$M -e "DROP DATABASE $DB" 2>/dev/null
wc -l "$OUT"/*.txt
