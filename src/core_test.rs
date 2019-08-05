use std::ops::Bound;

use crate::core::{Delta, Entry, Value};
use crate::vlog;

#[test]
fn test_delta_new_upsert() {
    let delta: Delta<i32> = Delta::new_upsert(vlog::Delta::new_native(100), 200);
    assert_eq!(delta.clone().into_diff(), Some(100));
    assert_eq!(delta.to_seqno(), 200);
    assert_eq!(delta.to_seqno_state(), (true, 200));

    match delta.clone().into_upserted() {
        Some((d, seqno)) => {
            assert_eq!(d.into_native_delta(), Some(100));
            assert_eq!(seqno, 200);
        }
        None => assert!(false),
    }
    assert_eq!(delta.into_deleted(), None);
}

#[test]
fn test_delta_new_delete() {
    let delta: Delta<i32> = Delta::new_delete(300);
    assert_eq!(delta.clone().into_diff(), None);
    assert_eq!(delta.to_seqno(), 300);
    assert_eq!(delta.to_seqno_state(), (false, 300));

    match delta.clone().into_upserted() {
        Some(_) => assert!(false),
        _ => (),
    }
    assert_eq!(delta.into_deleted(), Some(300));
}

#[test]
fn test_value_new_upsert() {
    let value: Value<i32> = Value::new_upsert(vlog::Value::new_native(100), 200);
    assert_eq!(value.to_native_value(), Some(100));
    assert_eq!(value.is_deleted(), false);
}

#[test]
fn test_value_new_delete() {
    let value: Value<i32> = Value::new_delete(300);
    assert_eq!(value.to_native_value(), None);
    assert_eq!(value.is_deleted(), true);
}

#[test]
fn test_entry_new() {
    // testcase1 new
    let value = Box::new(Value::new_upsert(vlog::Value::new_native(10), 1000));
    let mut entry1 = Entry::new(100, value);
    // verify latest entry
    assert_eq!(entry1.as_deltas().len(), 0);
    verify_latest(&entry1, 100, Some(10), 1000, false);
    // verify versions
    let mut vers = entry1.versions();
    let entry = vers.next().expect("expected valid entry");
    verify_version(&entry, 100, Some(10), 1000, false);
    assert!(vers.next().is_none());

    // testcase2 upsert
    let value = Box::new(Value::new_upsert(vlog::Value::new_native(20), 1001));
    let entry2 = Entry::new(100, value);
    entry1.prepend_version(entry2, false /*lsm*/);
    // verify latest entry
    assert_eq!(entry1.as_deltas().len(), 0);
    verify_latest(&entry1, 100, Some(20), 1001, false);
    // verify versions
    let mut vers = entry1.versions();
    let entry = vers.next().expect("expected valid entry");
    verify_version(&entry, 100, Some(20), 1001, false);
    assert!(vers.next().is_none());

    // testcase3 purge noop
    let entry1 = entry1.purge(Bound::Included(1000)).unwrap();
    // verify latest entry
    assert_eq!(entry1.as_deltas().len(), 0);
    verify_latest(&entry1, 100, Some(20), 1001, false);
    // verify versions
    let mut vers = entry1.versions();
    let entry = vers.next().expect("expected valid entry");
    verify_version(&entry, 100, Some(20), 1001, false);
    assert!(vers.next().is_none());

    // testcase4 actual purge
    assert!(entry1.purge(Bound::Included(1002)).is_none());
    assert_eq!(entry1.as_deltas().len(), 0);
}

#[test]
fn test_entry_new_lsm() {
    // testcase1 new
    let value = Box::new(Value::new_upsert(vlog::Value::new_native(10), 1000));
    let mut entry1 = Entry::new(100, value);
    // verify latest entry
    assert_eq!(entry1.as_deltas().len(), 0);
    verify_latest(&entry1, 100, Some(10), 1000, false);
    // verify versions
    let mut vers = entry1.versions();
    let entry = vers.next().expect("expected valid entry");
    verify_version(&entry, 100, Some(10), 1000, false);
    assert!(vers.next().is_none());

    // testcase2 upsert
    let value = Box::new(Value::new_upsert(vlog::Value::new_native(20), 1001));
    let entry2 = Entry::new(100, value);
    entry1.prepend_version(entry2, true /*lsm*/);
    // verify latest entry
    assert_eq!(entry1.as_deltas().len(), 1);
    verify_latest(&entry1, 100, Some(20), 1001, false);
    // verify versions
    let mut vers = entry1.versions();
    let mut entry = vers.next().expect("expected valid entry");
    verify_version(&entry, 100, Some(20), 1001, false);
    entry = vers.next().expect("expected valid entry");
    verify_version(&entry, 100, Some(10), 1000, false);
    assert!(vers.next().is_none());

    // testcase3 delete
    entry1.delete(1002);
    // verify latest entry
    assert_eq!(entry1.as_deltas().len(), 2);
    verify_latest(&entry1, 100, None, 1002, true);
    // verify versions
    let mut vers = entry1.versions();
    let mut entry = vers.next().expect("expected valid entry");
    verify_version(&entry, 100, None, 1002, true);
    entry = vers.next().expect("expected valid entry");
    verify_version(&entry, 100, Some(20), 1001, false);
    entry = vers.next().expect("expected valid entry");
    verify_version(&entry, 100, Some(10), 1000, false);
    assert!(vers.next().is_none());

    // testcase4 upsert
    let value = Box::new(Value::new_upsert(vlog::Value::new_native(30), 1003));
    let entry3 = Entry::new(100, value);
    entry1.prepend_version(entry3, true /*lsm*/);
    // verify latest entry
    assert_eq!(entry1.as_deltas().len(), 3);
    verify_latest(&entry1, 100, Some(30), 1003, false);
    // verify versions
    let mut vers = entry1.versions();
    let mut entry = vers.next().expect("expected valid entry");
    verify_version(&entry, 100, Some(30), 1003, false);
    entry = vers.next().expect("expected valid entry");
    verify_version(&entry, 100, None, 1002, true);
    entry = vers.next().expect("expected valid entry");
    verify_version(&entry, 100, Some(20), 1001, false);
    entry = vers.next().expect("expected valid entry");
    verify_version(&entry, 100, Some(10), 1000, false);
    assert!(vers.next().is_none());

    // testcase5 purge noop
    let entry1 = entry1.purge(Bound::Included(1000)).unwrap();
    // verify latest entry
    assert_eq!(entry1.as_deltas().len(), 2);
    verify_latest(&entry1, 100, Some(30), 1003, false);
    // verify versions
    let mut vers = entry1.versions();
    let mut entry = vers.next().expect("expected valid entry");
    verify_version(&entry, 100, Some(30), 1003, false);
    entry = vers.next().expect("expected valid entry");
    verify_version(&entry, 100, None, 1002, true);
    entry = vers.next().expect("expected valid entry");
    verify_version(&entry, 100, Some(20), 1001, false);
    assert!(vers.next().is_none());

    // testcase5 purge
    let entry1 = entry1.purge(Bound::Included(1002)).unwrap();
    // verify latest entry
    assert_eq!(entry1.as_deltas().len(), 0);
    verify_latest(&entry1, 100, Some(30), 1003, false);
    // verify versions
    let mut vers = entry1.versions();
    let mut entry = vers.next().expect("expected valid entry");
    verify_version(&entry, 100, Some(30), 1003, false);
    assert!(vers.next().is_none());

    assert!(entry1.purge(Bound::Included(1004)).is_none());
}

fn verify_version(e: &Entry<i32, i32>, key: i32, val: Option<i32>, seq: u64, del: bool) {
    assert_eq!(e.to_key(), key);
    assert_eq!(e.as_key(), &key);
    assert_eq!(e.to_native_value(), val);
    assert_eq!(e.to_seqno(), seq);
    assert_eq!(e.to_seqno_state(), (!del, seq));
    assert_eq!(e.is_deleted(), del);
}

fn verify_latest(e: &Entry<i32, i32>, key: i32, val: Option<i32>, seq: u64, del: bool) {
    assert_eq!(e.to_key(), key);
    assert_eq!(e.as_key(), &key);
    assert_eq!(e.to_native_value(), val);
    assert_eq!(e.to_seqno(), seq);
    assert_eq!(e.to_seqno_state(), (!del, seq));
    assert_eq!(e.is_deleted(), del);
}
