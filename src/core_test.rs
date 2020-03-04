use std::ops::Bound;

use super::*;

use crate::{
    core::{Delta, Entry, Value},
    vlog,
};

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
    let v = Box::new(vlog::Value::new_native(100));
    let value: Value<i32> = Value::new_upsert(v, 200);
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
    let value = Value::new_upsert(Box::new(vlog::Value::new_native(10)), 1000);
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
    let value = Value::new_upsert(Box::new(vlog::Value::new_native(20)), 1001);
    let entry2 = Entry::new(100, value);
    entry1.prepend_version(entry2, false /*lsm*/).unwrap();
    // verify latest entry
    assert_eq!(entry1.as_deltas().len(), 0);
    verify_latest(&entry1, 100, Some(20), 1001, false);
    // verify versions
    let mut vers = entry1.versions();
    let entry = vers.next().expect("expected valid entry");
    verify_version(&entry, 100, Some(20), 1001, false);
    assert!(vers.next().is_none());

    // testcase3 purge noop
    let entry1 = {
        let cutoff = Cutoff::new_lsm(Bound::Included(1000));
        entry1.purge(cutoff).unwrap()
    };
    // verify latest entry
    assert_eq!(entry1.as_deltas().len(), 0);
    verify_latest(&entry1, 100, Some(20), 1001, false);
    // verify versions
    let mut vers = entry1.versions();
    let entry = vers.next().expect("expected valid entry");
    verify_version(&entry, 100, Some(20), 1001, false);
    assert!(vers.next().is_none());

    // testcase4 actual purge
    let entry = {
        let cutoff = Cutoff::new_lsm(Bound::Included(1002));
        entry1.purge(cutoff)
    };
    assert!(entry.is_none());
}

#[test]
fn test_entry_new_lsm() {
    // testcase1 new
    let value = Value::new_upsert(Box::new(vlog::Value::new_native(10)), 1000);
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
    let value = Value::new_upsert(Box::new(vlog::Value::new_native(20)), 1001);
    let entry2 = Entry::new(100, value);
    entry1.prepend_version(entry2, true /*lsm*/).unwrap();
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
    entry1.delete(1002).unwrap();
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
    let value = Value::new_upsert(Box::new(vlog::Value::new_native(30)), 1003);
    let entry3 = Entry::new(100, value);
    entry1.prepend_version(entry3, true /*lsm*/).unwrap();
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
    let entry1 = {
        let cutoff = Cutoff::new_lsm(Bound::Excluded(1000));
        entry1.purge(cutoff).unwrap()
    };
    assert_eq!(entry1.as_deltas().len(), 3);
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

    // testcase6 purge noop
    let entry1 = {
        let cutoff = Cutoff::new_lsm(Bound::Included(1000));
        entry1.purge(cutoff).unwrap()
    };
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

    // testcase7 purge
    let entry1 = {
        let cutoff = Cutoff::new_lsm(Bound::Included(1002));
        entry1.purge(cutoff).unwrap()
    };
    // verify latest entry
    assert_eq!(entry1.as_deltas().len(), 0);
    verify_latest(&entry1, 100, Some(30), 1003, false);
    // verify versions
    let mut vers = entry1.versions();
    let entry = vers.next().expect("expected valid entry");
    verify_version(&entry, 100, Some(30), 1003, false);
    assert!(vers.next().is_none());

    let entry = {
        let cutoff = Cutoff::new_lsm(Bound::Included(1004));
        entry1.purge(cutoff)
    };
    assert!(entry.is_none());
}

#[test]
fn test_entry_tombstone_purge1() {
    let value = Value::new_upsert(Box::new(vlog::Value::new_native(10)), 1000);
    let mut entry = Entry::new(100, value);
    {
        let v = Box::new(vlog::Value::new_native(20));
        let value = Value::new_upsert(v, 1001);
        let e = Entry::new(100, value);
        entry.prepend_version(e, true /*lsm*/).unwrap();
    }
    entry.delete(1002).unwrap();

    let cutoff = Cutoff::new_tombstone(Bound::Excluded(1002));
    let ent = entry.clone().purge(cutoff).unwrap();
    assert_eq!(ent.as_deltas().len(), 2);
    let mut vers = ent.versions();
    let mut e = vers.next().expect("expected valid ent");
    verify_version(&e, 100, None, 1002, true);
    e = vers.next().expect("expected valid ent");
    verify_version(&e, 100, Some(20), 1001, false);
    e = vers.next().expect("expected valid ent");
    verify_version(&e, 100, Some(10), 1000, false);
    assert!(vers.next().is_none());

    let cutoff = Cutoff::new_tombstone(Bound::Included(1002));
    assert!(entry.clone().purge(cutoff).is_none());

    let cutoff = Cutoff::new_tombstone(Bound::Unbounded);
    assert!(entry.clone().purge(cutoff).is_none());
}

#[test]
fn test_entry_tombstone_purge2() {
    let value = Value::new_upsert(Box::new(vlog::Value::new_native(10)), 1000);
    let mut entry = Entry::new(100, value);
    {
        let v = Box::new(vlog::Value::new_native(20));
        let value = Value::new_upsert(v, 1001);
        let e = Entry::new(100, value);
        entry.prepend_version(e, true /*lsm*/).unwrap();
    }
    entry.delete(1002).unwrap();
    {
        let v = Box::new(vlog::Value::new_native(30));
        let value = Value::new_upsert(v, 1003);
        let e = Entry::new(100, value);
        entry.prepend_version(e, true /*lsm*/).unwrap();
    }

    let cutoffs = vec![
        Bound::Excluded(1002),
        Bound::Included(1002),
        Bound::Excluded(1003),
        Bound::Included(1003),
        Bound::Unbounded,
    ];
    for cutoff in cutoffs.into_iter() {
        let cutoff = Cutoff::new_tombstone(cutoff);
        let ent = entry.clone().purge(cutoff).unwrap();
        assert_eq!(ent.as_deltas().len(), 3);
        let mut vers = ent.versions();
        let mut e = vers.next().expect("expected valid ent");
        verify_version(&e, 100, Some(30), 1003, false);
        e = vers.next().expect("expected valid ent");
        verify_version(&e, 100, None, 1002, true);
        e = vers.next().expect("expected valid ent");
        verify_version(&e, 100, Some(20), 1001, false);
        e = vers.next().expect("expected valid ent");
        verify_version(&e, 100, Some(10), 1000, false);
        assert!(vers.next().is_none());
    }
}

#[test]
fn test_entry_mono_purge1() {
    let value = Value::new_upsert(Box::new(vlog::Value::new_native(10)), 1000);
    let mut entry = Entry::new(100, value);
    {
        let v = Box::new(vlog::Value::new_native(20));
        let value = Value::new_upsert(v, 1001);
        let e = Entry::new(100, value);
        entry.prepend_version(e, true /*lsm*/).unwrap();
    }

    let cutoff = Cutoff::new_mono();
    let ent = entry.clone().purge(cutoff).unwrap();
    assert_eq!(ent.as_deltas().len(), 0);
    assert_eq!(ent.to_seqno(), 1001);
    assert_eq!(ent.to_native_value().unwrap(), 20);

    let cutoff = Cutoff::new_mono();
    let ent = entry.clone().purge(cutoff).unwrap();
    assert_eq!(ent.as_deltas().len(), 0);
    assert_eq!(ent.to_seqno(), 1001);
    assert_eq!(ent.to_native_value().unwrap(), 20);

    let cutoff = Cutoff::new_mono();
    let ent = entry.clone().purge(cutoff).unwrap();
    assert_eq!(ent.as_deltas().len(), 0);
}

#[test]
fn test_entry_mono_purge2() {
    let value = Value::new_upsert(Box::new(vlog::Value::new_native(10)), 1000);
    let mut entry = Entry::new(100, value);
    {
        let v = Box::new(vlog::Value::new_native(20));
        let value = Value::new_upsert(v, 1001);
        let e = Entry::new(100, value);
        entry.prepend_version(e, true /*lsm*/).unwrap();
    }

    let cutoff = Cutoff::new_mono();
    let ent = entry.clone().purge(cutoff).unwrap();
    assert_eq!(ent.as_deltas().len(), 0);
}

#[test]
fn test_entry_mono_purge3() {
    let value = Value::new_upsert(Box::new(vlog::Value::new_native(10)), 1000);
    let mut entry = Entry::new(100, value);
    {
        let v = Box::new(vlog::Value::new_native(20));
        let value = Value::new_upsert(v, 1001);
        let e = Entry::new(100, value);
        entry.prepend_version(e, true /*lsm*/).unwrap();
    }
    entry.delete(1002).unwrap();

    let cutoff = Cutoff::new_mono();
    let ent = entry.clone().purge(cutoff);
    assert_eq!(ent.is_none(), true);
}

#[test]
fn test_entry_filter_within() {
    // version1 - upsert
    let value = Value::new_upsert_value(1000_i32, 10);
    let mut entry = Entry::new(100_i32, value);
    // version2 - delete
    let value = Value::new_upsert_value(2000_i32, 20);
    let entry2 = Entry::new(100_i32, value);
    // version3 - upsert
    let value = Value::new_upsert_value(3000_i32, 30);
    let entry3 = Entry::new(100_i32, value);
    // version4 - delete
    let value = Value::new_upsert_value(4000_i32, 40);
    let entry4 = Entry::new(100_i32, value);

    entry.prepend_version(entry2, true /*lsm*/).unwrap();
    entry.prepend_version(entry3, true /*lsm*/).unwrap();
    entry.prepend_version(entry4, true /*lsm*/).unwrap();

    let vers: Vec<Entry<i32, i32>> = entry.versions().collect();
    assert_eq!(vers.len(), 4);

    let verify = |entry: Entry<i32, i32>, ref_res: Vec<(i32, u64)>| {
        let vers: Vec<Entry<i32, i32>> = entry.versions().collect();
        assert_eq!(vers.len(), ref_res.len());
        let iter = vers
            .into_iter()
            .map(|e| (e.to_native_value().unwrap(), e.to_seqno()))
            .into_iter()
            .zip(ref_res.into_iter());
        for ((val, seqno), (ref_val, ref_seqno)) in iter {
            assert_eq!(val, ref_val);
            assert_eq!(seqno, ref_seqno);
        }
    };

    let (start, end) = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
    verify(
        entry.filter_within(start, end).unwrap(),
        [(4000, 40), (3000, 30), (2000, 20), (1000, 10)].to_vec(),
    );

    let (start, end) = (Bound::<u64>::Unbounded, Bound::Included(40_u64));
    verify(
        entry.filter_within(start, end).unwrap(),
        [(4000, 40), (3000, 30), (2000, 20), (1000, 10)].to_vec(),
    );

    let (start, end) = (Bound::<u64>::Unbounded, Bound::Excluded(40_u64));
    verify(
        entry.filter_within(start, end).unwrap(),
        [(3000, 30), (2000, 20), (1000, 10)].to_vec(),
    );

    let (start, end) = (Bound::Included(10_u64), Bound::<u64>::Unbounded);
    verify(
        entry.filter_within(start, end).unwrap(),
        [(4000, 40), (3000, 30), (2000, 20), (1000, 10)].to_vec(),
    );

    let (start, end) = (Bound::Excluded(10_u64), Bound::<u64>::Unbounded);
    verify(
        entry.filter_within(start, end).unwrap(),
        [(4000, 40), (3000, 30), (2000, 20)].to_vec(),
    );

    let (start, end) = (Bound::Included(10_u64), Bound::Included(40_u64));
    verify(
        entry.filter_within(start, end).unwrap(),
        [(4000, 40), (3000, 30), (2000, 20), (1000, 10)].to_vec(),
    );

    let (start, end) = (Bound::Included(10_u64), Bound::Excluded(40_u64));
    verify(
        entry.filter_within(start, end).unwrap(),
        [(3000, 30), (2000, 20), (1000, 10)].to_vec(),
    );

    let (start, end) = (Bound::Included(20_u64), Bound::Included(30_u64));
    verify(
        entry.filter_within(start, end).unwrap(),
        [(3000, 30), (2000, 20)].to_vec(),
    );

    let (start, end) = (Bound::Included(20_u64), Bound::Excluded(30_u64));
    verify(
        entry.filter_within(start, end).unwrap(),
        [(2000, 20)].to_vec(),
    );

    let (start, end) = (Bound::Excluded(20_u64), Bound::Included(30_u64));
    verify(
        entry.filter_within(start, end).unwrap(),
        [(3000, 30)].to_vec(),
    );

    let (start, end) = (Bound::Excluded(20_u64), Bound::Excluded(30_u64));
    assert!(entry.filter_within(start, end).is_none());

    let (start, end) = (Bound::Included(21_u64), Bound::Included(29_u64));
    assert!(entry.filter_within(start, end).is_none());

    let (start, end) = (Bound::Excluded(21_u64), Bound::Excluded(29_u64));
    assert!(entry.filter_within(start, end).is_none());
}

fn verify_version(
    e: &Entry<i32, i32>,
    key: i32,
    val: Option<i32>,
    seq: u64,
    del: bool, // is deleted
) {
    assert_eq!(e.to_key(), key);
    assert_eq!(e.as_key(), &key);
    assert_eq!(e.to_native_value(), val);
    assert_eq!(e.to_seqno(), seq);
    assert_eq!(e.to_seqno_state(), (!del, seq));
    assert_eq!(e.is_deleted(), del);
}

fn verify_latest(
    e: &Entry<i32, i32>,
    key: i32,
    val: Option<i32>,
    seq: u64,
    del: bool, // is deleted
) {
    assert_eq!(e.to_key(), key);
    assert_eq!(e.as_key(), &key);
    assert_eq!(e.to_native_value(), val);
    assert_eq!(e.to_seqno(), seq);
    assert_eq!(e.to_seqno_state(), (!del, seq));
    assert_eq!(e.is_deleted(), del);
}
