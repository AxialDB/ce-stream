---
name: Bug report
about: Something is broken or incorrect
title: ""
labels: bug
assignees: ""
---

## Summary

A clear description of the bug.

## Environment

- ce-stream version / commit:
- OS:
- Source and its version (MySQL 9.x, MongoDB 8.0+ replica set, or PostgreSQL 17+):
- Capture host role (primary / replica / standby):
- Sink (`stdout` / `http`) and `format` (`json` / `avro`):

## Config (redact secrets)

Paste relevant `ce-stream.toml` sections with passwords removed.

## Steps to reproduce

1.
2.
3.

## Expected vs actual

-

## Logs

Relevant `RUST_LOG=info` / `ce_stream::health` lines (redact secrets).
