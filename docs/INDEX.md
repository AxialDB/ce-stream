# Documentation index

| Doc | Audience | Notes |
|-----|----------|--------|
| [`../README.md`](../README.md) | Everyone | Start here ([AxialDB](https://axialdb.com/) maintained) |
| [`../CONTRIBUTING.md`](../CONTRIBUTING.md) | Contributors | Bugs, PRs, Discussions |
| [`../SECURITY.md`](../SECURITY.md) | Security reports | Private advisory path |
| [`../CHANGELOG.md`](../CHANGELOG.md) | Everyone | Release notes |
| [`releases/v0.4.2.md`](releases/v0.4.2.md) | Everyone | **v0.4.2** release notes (Mongo server-side include filter) |
| [`releases/v0.4.0.md`](releases/v0.4.0.md) | Everyone | v0.4.0 release notes (MongoDB change streams) |
| [`releases/v0.3.0.md`](releases/v0.3.0.md) | Everyone | v0.3.0 release notes (live include list) |
| [`releases/v0.2.0.md`](releases/v0.2.0.md) | Everyone | v0.2.0 release notes (Gate 0) |
| [`releasing.md`](releasing.md) | Maintainers | Issue → branch → PR → tag workflow; GitHub Release |
| [`issues/11.md`](issues/11.md) | Maintainers | Mongo resume after an event the filter leaves out (#11, with #9 in v0.4.2) |
| [`issues/9.md`](issues/9.md) | Maintainers | Mongo server-side include filter (#9, pending tag v0.4.2) |
| [`issues/5.md`](issues/5.md) | Maintainers | MongoDB change stream source (#5, pending tag v0.4.0) |
| [`issues/3.md`](issues/3.md) | Maintainers | Live include list (closes #3) |
| [`issues/1.md`](issues/1.md) | Maintainers | Gate 0 plan (closes #1) |
| [`planning.md`](planning.md) | Maintainers | Phase status; Phase 6: MongoDB in v0.4.0, other DBs **deferred** |
| [`oss-readiness.md`](oss-readiness.md) | Maintainers | Lean OSS MVP (historical plan; see releasing.md for v0.2) |
| [`ops-e2e.md`](ops-e2e.md) | Operators | Continuous capture → HTTP |
| [`delivery.md`](delivery.md) | Operators / consumers | At-least-once, backpressure, payload modes |
| [`library.md`](library.md) | Embedders | In-process `ChangeSource` |
| [`avro.md`](avro.md) | Integrators | Optional `sink.format=avro` |
| [`../scripts/crash-harness/README.md`](../scripts/crash-harness/README.md) | Maintainers | Gate 0 crash regression (how to run) |
| [`../scripts/crash-harness/mysql/README.md`](../scripts/crash-harness/mysql/README.md) | Maintainers | MySQL adapter details |
| [`perf-harness.md`](perf-harness.md) | Maintainers | Lab scenarios + JSON/Avro results |
| [`spike-mysql-binlog.md`](spike-mysql-binlog.md) | Maintainers | Historical Phase 1 gate notes |

**v1 product:** MySQL 9.x and MongoDB 8.0+ ([#5](https://github.com/AxialDB/ce-stream/issues/5), v0.4.0). Other engines are deferred (not scheduled).
