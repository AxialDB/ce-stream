# PostgreSQL crash harness

`kill -9` ce-stream under concurrent writers with `checkpoint = "on_confirm"`, restart it, and check that every committed row arrived. A row may arrive twice when a kill falls between the sink and the checkpoint; none may be missing. Linux, with `jq`.

The server has `wal_level = logical`, a database, and a capture role with `LOGIN` and `REPLICATION`:

```bash
export CE_STREAM_BIN=target/debug/ce-stream PSQL=/usr/lib/postgresql/17/bin/psql PGPORT=5432
export PGDATABASE=ce_stream_crash CAPTURE_USER=ce_stream
export PGPASSWORD='<superuser password>' CE_STREAM_PASSWORD='<capture role password>'
scripts/crash-harness/postgres/run-crash-harness.sh
```

`WRITERS`, `PER_WRITER`, and `KILLS` size the run. Artifacts land under `out/<timestamp>/` (gitignored), including a config with the capture password; never commit them.

The figure to watch besides the gap is "commits starting exactly where the one before ended". Such commits are why positions compare by a commit's end ([`docs/issues/19.md`](../../../docs/issues/19.md)), so a run with none has not tested that.
