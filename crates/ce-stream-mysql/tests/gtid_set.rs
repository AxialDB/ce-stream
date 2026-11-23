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
fn handshake_is_baseline_and_checkpoint_when_the_purge_is_inside() {
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
fn handshake_is_history_lost_when_the_server_purged_past_the_checkpoint() {
    let uuid = "49b103aa-6814-11f1-aab9-144fd7d9c421";
    let err = ExecutedSet::binlog_handshake_gtid_set(
        &format!("{uuid}:1-123"),
        Some(&format!("{uuid}:1-100")),
        &format!("{uuid}:1-183"),
    )
    .unwrap_err();
    assert!(
        matches!(err, ce_stream_core::Error::HistoryLost(_)),
        "{err}"
    );
    assert_eq!(
        ce_stream_mysql::purged_missing(
            &format!("{uuid}:1-123"),
            Some(&format!("{uuid}:1-100")),
            &format!("{uuid}:1-183")
        )
        .unwrap(),
        format!("{uuid}:124-183")
    );
}

#[test]
fn duplicate_add_committed_is_idempotent() {
    let mut set = ExecutedSet::from_set_string("abc:1-10").unwrap();
    set.add_committed("abc:5").unwrap();
    assert_eq!(set.to_set_string(), "abc:1-10");
    set.add_committed("abc:11").unwrap();
    assert_eq!(set.to_set_string(), "abc:1-11");
}
