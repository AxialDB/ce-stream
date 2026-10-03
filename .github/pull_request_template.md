## Summary

What does this PR change and why?

## Checklist

- [ ] `cargo fmt --all`
- [ ] `cargo clippy -p ce-stream-core -p ce-stream-mysql -p ce-stream-mongo -p ce-stream-cli -p ce-stream-perf-sink --no-deps -- -D warnings`
- [ ] `cargo test -p ce-stream-core -p ce-stream-mysql -p ce-stream-mongo`
- [ ] Docs updated if behavior or config changed
- [ ] Scope change (new source engine or un-deferred phase): `docs/planning.md` decision plus scope lines in README, CONTRIBUTING, `docs/INDEX.md`, Ideas form ([`docs/releasing.md`](../docs/releasing.md))
- [ ] No secrets committed (`ce-stream.toml` stays gitignored)

## Notes for reviewers

-
