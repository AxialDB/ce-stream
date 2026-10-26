# MySQL crash harness

MySQL 9.x lab regression for [issue #1](https://github.com/AxialDB/ce-stream/issues/1).

**How to run:** see the parent [`../README.md`](../README.md) for Windows, WSL, and AxialDB wrapper commands.

## Scenario

```text
1. Preflight: binlog_row_metadata=FULL, ROW format, log_bin ON
2. Warmup:    one autocommit INSERT -> checkpoint GTID G0
3. Crash:     3-row START TRANSACTION ... COMMIT; kill ce-stream after sink receives 1 event
              assert checkpoint still G0
4. Restart:  same checkpoint file -> binlog replays txn -> sink receives full commit
              assert checkpoint advances past G0
```

Uses `ce-stream-perf-sink --stall-at N` to simulate a crash in the middle of a delivery: the sink answers the deliveries before the Nth and hangs on it, and the harness kills ce-stream once it hangs. Row mode hangs on row 2; transaction mode hangs on the one envelope, so nothing of the transaction is answered.

With `--part-max-rows N` / `-PartMaxRows N` ([issue #16](https://github.com/AxialDB/ce-stream/issues/16)) the transaction has `2N+1` rows unless `--txn-rows` says otherwise, and is delivered in parts of N rows. The sink answers the whole first part (N rows in row mode, one envelope in transaction mode) and hangs on the first delivery of the second part (`--stall-at N+1` or `--stall-at 2`), so the kill falls after a whole part was taken. The checkpoint must not have moved, and the restart delivers the whole transaction again from part 1.

## Scripts

| File | Purpose |
|------|---------|
| `run-crash-harness.ps1` | Windows host + `-Wsl` delegate |
| `run-crash-harness.sh` | Linux / WSL bash |
| `install-linux.sh` | Build + install to `~/.local/bin` |
| `_common.ps1` / `_common.sh` | Shared helpers |

Artifacts: `out/<timestamp>/` (checkpoint, TOML, logs, `summary.json`) - gitignored.

## Failure modes

| Symptom | Likely cause |
|---------|----------------|
| `binlog_row_metadata must be FULL` | Set in MySQL `my.ini` / `mysqld.cnf`, restart service |
| `Set CE_STREAM_PASSWORD` | Export password before run |
| `Pass -MysqlDefaults or set MYSQL_DEFAULTS_FILE` | Point at admin mysql client defaults file |
| Checkpoint advanced after partial delivery | Pre-v0.2.0 per-row checkpoint bug |
| Redelivery count < 3 (row mode) | GTID resume or binlog replay issue |

## Related

- [`docs/ops-e2e.md`](../../docs/ops-e2e.md)
- `crates/ce-stream-mysql/tests/txn_checkpoint.rs`
