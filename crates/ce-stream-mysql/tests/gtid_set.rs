use ce_stream_mysql::ExecutedSet;

#[test]
fn round_trip_preserves_holes() {
    let set = ExecutedSet::from_set_string("abc:1-10:12-15").unwrap();
    assert_eq!(set.to_set_string(), "abc:1-10:12-15");
}

#[test]
fn add_committed_fills_hole() {
    let mut set = ExecutedSet::from_set_string("abc:1-10:12-15").unwrap();
    set.add_committed("abc:11").unwrap();
    assert_eq!(set.to_set_string(), "abc:1-15");
}

#[test]
fn handshake_unions_purged_baseline_and_checkpoint() {
    let uuid = "49b103aa-6814-11f1-aab9-144fd7d9c421";
    let out = ExecutedSet::binlog_handshake_gtid_set(
        &format!("{uuid}:4105-4105"),
        Some(&format!("{uuid}:1-4104")),
        &format!("{uuid}:1-242"),
    )
    .unwrap();
    assert_eq!(out, format!("{uuid}:1-4105"));
}

#[test]
fn duplicate_add_committed_is_idempotent() {
    let mut set = ExecutedSet::from_set_string("abc:1-10").unwrap();
    set.add_committed("abc:5").unwrap();
    assert_eq!(set.to_set_string(), "abc:1-10");
    set.add_committed("abc:11").unwrap();
    assert_eq!(set.to_set_string(), "abc:1-11");
}
