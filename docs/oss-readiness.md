# OSS readiness

**Maintainer:** [AxialDB](https://axialdb.com/) vendor ([releases](https://github.com/AxialDB/releases)).

Lean OSS MVP (v0.1) is in-repo: `LICENSE`, `NOTICE`, `CONTRIBUTING.md`, `SECURITY.md`, issue/PR templates, `CHANGELOG.md`, Discussions forms, CI (`fmt`/`clippy`/tests, **no MySQL**).

**v0.2.0 delivery:** GitHub Release binaries from [`.github/workflows/release.yml`](../../.github/workflows/release.yml) — **Linux x86_64** and **Windows x86_64** as equal first-class targets. Tag `v*` publishes archives + `SHA256SUMS`. Crash harness and live capture stay **lab-only**. crates.io still later.

The rest of this file is the original v0.1 gap analysis (historical). Prefer [`releasing.md`](releasing.md) for how to cut a release.

---

**Purpose:** Close the gap between ce-stream today and what credible Rust DB projects show at the GitHub root — **without** copying diesel/sea-orm ceremony on day one.

**Maintainer:** [AxialDB](https://axialdb.com/) vendor ([releases](https://github.com/AxialDB/releases)). Project is open-source; AxialDB is the creating/maintaining org, not a hard runtime dependency.

**MVP status (2026-08-02):** Implemented in-repo — `LICENSE`, `NOTICE`, README credit, `CONTRIBUTING.md`, `SECURITY.md`, issue/PR templates, CI workflow, `CHANGELOG.md`, install-from-git docs, Discussion forms under `.github/DISCUSSION_TEMPLATE/` (`q-a`, `ideas`, `general`). **Manual:** confirm Discussion category slugs match those filenames; tag `v0.1.0` when you cut the release; crates.io publish later.

---

## Recommended model: lean OSS MVP

Do **not** start with Discord, CoC bureaucracy, Dependabot sprawl, docs websites, or dual-license bikeshedding. Match **sqlx-ish minimum**, not diesel-max.

### In scope for first public cut (MVP)

| Item | Why enough | Status |
|------|------------|--------|
| `LICENSE` (Apache-2.0 file matching Cargo) | Legal clarity; peers always have this | done |
| README (outsider + **AxialDB** links) | First impression | done |
| Short `CONTRIBUTING.md` | Fork → PR; bugs = Issues; questions = Discussions | done |
| Enable GitHub **Discussions** + category forms | No Discord yet | Discussions on; forms in `.github/DISCUSSION_TEMPLATE/` (push if not on remote yet) |
| One bug **issue template** + short **PR template** | Useful reports without process theater | done |
| CI: `fmt` + `clippy` + `test` (no live MySQL on every PR) | Trust signal | done (`.github/workflows/ci.yml`) |
| `CHANGELOG.md` + tag `v0.1.0` | Release hygiene | changelog done; **tag when ready** |
| Publish crates **or** document `cargo install --git` until publish | Delivery path | install-from-git documented |

**Tracker:** GitHub only (Issues / PRs / Discussions). No Jira for public OSS.

**Security (minimal):** one paragraph in README or tiny `SECURITY.md` — “email maintainers / use GitHub private advisory; do not file public Issues for vulns.” Full diesel-style policy later.

### Explicitly later (not MVP)

- Code of conduct (add when first external contributors show up, or copy Covenant in 10 minutes then)
- Discord, Funding.yml, Dependabot, cargo-deny
- Docker Compose quickstart, release binaries, docs site
- Rich example gallery, FAQ site, dual MIT/Apache unless you care
- Schema Registry / multi-DB

### How people send requests (MVP)

```text
Question / “how do I…?”  →  Discussions
Bug                       →  Issue (template)
Feature idea              →  Discussion first; Issue only if you accept it onto the roadmap
Code                      →  PR from a fork
Security                  →  Private advisory / email
```

You triage in GitHub. No second system.

---

## Peer survey (reference)

**Surveyed peers (2026-08-02):**

| Project | Stars (approx) | Why peer |
|---------|----------------|----------|
| [transact-rs/sqlx](https://github.com/transact-rs/sqlx) | ~17k | Async Rust SQL toolkit; MySQL driver; library + CLI |
| [diesel-rs/diesel](https://github.com/diesel-rs/diesel) | ~14k | Mature ORM; strong governance (SECURITY, CoC, Discussions) |
| [SeaQL/sea-orm](https://github.com/SeaQL/sea-orm) | ~10k | Polished README/docs site; COMMUNITY + CONTRIBUTING + crates.io |

Also glanced at [Qovery/Replibyte](https://github.com/Qovery/Replibyte) (~4k) as a **CLI/tool** sibling (Docker compose demos, website docs) — useful for delivery packaging, less for crate governance.

---

## What peers put at the GitHub root

Common root surface (all three libraries share most of this):

| Root artifact | sqlx | diesel | sea-orm | ce-stream today |
|---------------|------|--------|---------|-----------------|
| README with badges (CI, crates.io, docs) | yes | yes | yes | partial (no badges/crates) |
| Dual `LICENSE-APACHE` + `LICENSE-MIT` (or clear single license file) | dual | dual | dual | **missing files** (Cargo says Apache-2.0) |
| `CHANGELOG.md` (or changelog/) | yes | yes | yes | **missing** |
| `CONTRIBUTING.md` | yes | yes | yes | **missing** |
| Code of conduct | (org/discord) | `code_of_conduct.md` | org `.github` | **missing** |
| `SECURITY.md` | — | yes | — | **missing** |
| `.github/` workflows + issue/PR templates | yes | yes | yes | **missing** |
| `examples/` at root or nearby | yes | yes | yes | one embed example only |
| FAQ / versions / community pointers | FAQ | Discussions | COMMUNITY, VERSIONS, external docs site | planning docs only |
| CLI as first-class crate | `sqlx-cli` | `diesel_cli` | `sea-orm-cli` | `ce-stream-cli` (ok) |
| Workspace split by concern | sqlx-mysql/… | diesel_* | sea-orm-* | crates/ (ok shape) |

**README pattern they share:** one-liner + badges → install → minimal usage → link out to docs.rs / website → license + contribution blurb. Not a dump of internal phase status.

**Delivery pattern:**

- **Libraries:** publish to **crates.io**; docs on **docs.rs**; optional CLI via `cargo install`.
- **Tools (Replibyte):** binary + Docker + compose recipes + marketing site. We already lean library+CLI; Docker is optional polish, not required for v0.1 library parity.

**Communication pattern:**

- Discord and/or GitHub Discussions for questions (not every question as an Issue).
- Issue templates for bugs; feature ideas often routed to Discussions (diesel is explicit about this).
- SECURITY via private advisory / email (diesel model is the clearest).
- Funding.yml / sponsors when mature (defer for us).

---

## Gap analysis (ce-stream)

**We already have (keep):**

- Clear product docs under `docs/` (ops, delivery, library, avro, perf, spike, planning)
- Workspace layout (`core` / `mysql` / `cli`) similar to peers
- Example config, systemd unit, perf harness, Apache-2.0 declared in Cargo

**Must-have for “same level” at v0.1 announce:**

1. **License files on disk** matching Cargo (`LICENSE` or dual MIT/Apache like peers).
2. **README rewrite for outsiders** — problem → install → 30-second run → doc map; badges when CI/crates exist; no machine-local lab paths in public docs/scripts.
3. **`CHANGELOG.md`** starting at `0.1.0` (Keep a Changelog style).
4. **`CONTRIBUTING.md`** — how to build, lab MySQL 9.x expectations, PR norms, where to ask questions.
5. **`.github/workflows`** — at least `cargo check` + `cargo test` (unit); optional clippy/fmt.
6. **Issue + PR templates** — bug report fields (MySQL version, ce-stream version, config redacted).
7. **crates.io publish plan** — which crates are public (`ce-stream-core`, `ce-stream-mysql`, binary crate); versions aligned; `repository` / `homepage` / `readme` metadata.
8. **docs.rs** — crate-level rustdoc; README points at docs.rs for API, `docs/` for ops.

**Should-have soon after v0.1:**

9. **`SECURITY.md`** — supported versions (latest only is fine); private report channel.
10. **Code of conduct** — Contributor Covenant (root or org `.github`).
11. **GitHub Discussions** enabled; CONTRIBUTING says “questions → Discussions”.
12. **Richer `examples/`** — HTTP sink callback, signal mode, Avro decode consumer sketch.
13. **`cargo install ce-stream`** path documented; optional GitHub Release binaries later.
14. **Docker Compose demo** (MySQL 9 + ce-stream + mock webhook) — Replibyte-style “try in one command” without requiring AxialDB lab.

**Nice-to-have / later:**

15. External docs site (SeaORM-level) — overkill until traffic justifies it; keep Markdown in-repo.
16. Discord — only if community volume needs it.
17. Dependabot / cargo-deny in CI (diesel has deny.toml).
18. FAQ.md for recurring MySQL 9 / GTID / column-name questions.
19. Schema Registry / multi-DB — explicitly **not** OSS-readiness; product deferred items.

---

## Recommended workstreams (ordered)

### A. Legal + root hygiene (1–2 days)

- [ ] Add `LICENSE` (Apache-2.0) or dual MIT/Apache files
- [ ] Align all crate `license` fields; add `readme`, `keywords`, `categories` for crates.io
- [ ] Root `.gitignore` / ensure secrets (`ce-stream.toml` passwords) never published
- [ ] Rewrite README for public audience; link planning as “internals”

### B. Governance + communication (MVP)

- [ ] Short `CONTRIBUTING.md` (no CoC required yet)
- [ ] Tiny security note (README paragraph or short `SECURITY.md`)
- [ ] Bug issue template + PR template
- [ ] Enable Discussions; document channels in README

### C. Delivery + CI (2–4 days)

- [ ] CI workflow: fmt, clippy, test (no live MySQL required for default PR gate)
- [ ] Optional nightly/scheduled job with MySQL 9 service container for E2E
- [ ] Tag `v0.1.0`; publish crates; attach CLI binary or document `cargo install --git` interim
- [ ] `CHANGELOG.md` entry for 0.1.0

### D. Docs packaging (1–2 days)

- [ ] `docs/INDEX.md` (or README “Documentation” section) as the public map
- [ ] Promote ops/delivery/library/avro/perf; demote spike/planning to “history / maintainers”
- [ ] Ensure rustdoc on public APIs (`CloudEvent`, `MysqlBinlogSource`, sinks)

### E. Try-it packaging (optional, 2–3 days)

- [ ] `docker-compose.yml`: MySQL 9.x + ce-stream + tiny webhook
- [ ] One-page `docs/quickstart.md`

---

## Explicit non-goals for this plan

- Matching peer **star counts** or Discord size
- Multi-DB adapters (Phase 6 deferred)
- Website / Pro admin panel / sponsorship program
- Replacing Debezium in marketing copy

---

**Success for MVP:** outsider understands the product + AxialDB stewardship in 30s; can build/run; can file a bug or PR; CI is green; license is clear. No Discord, no docs site, no multi-DB.
