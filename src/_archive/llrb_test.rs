// TODO: write test case for iter_within for Llrb and Mvcc index.

use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use std::{convert::TryFrom, mem, ops::Bound};

use super::*;
use crate::{
    core::{CommitIterator, Index, Reader, Validate, Writer},
    error::Error,
    llrb::Llrb,
    scans,
    types::Empty,
    util,
};

include!("./ref_test.rs");

// TODO: repeatable randoms.

#[test]
fn test_node_size() {
    use crate::{core, vlog};

    assert_eq!(std::mem::size_of::<Node<i64, i64>>(), 80);

    assert_eq!(32, std::mem::size_of::<vlog::Value<Empty>>());
    assert_eq!(24, std::mem::size_of::<core::Value<Empty>>());
    assert_eq!(32, std::mem::size_of::<vlog::Delta<Empty>>());
    assert_eq!(48, std::mem::size_of::<core::Delta<Empty>>());
    assert_eq!(24, std::mem::size_of::<Vec<Empty>>());
    assert_eq!(48, std::mem::size_of::<Entry<Empty, Empty>>());
    assert_eq!(72, std::mem::size_of::<Node<Empty, Empty>>());

    assert_eq!(32, std::mem::size_of::<vlog::Value<i64>>());
    assert_eq!(24, std::mem::size_of::<core::Value<i64>>());
    assert_eq!(32, std::mem::size_of::<vlog::Delta<i64>>());
    assert_eq!(48, std::mem::size_of::<core::Delta<i64>>());
    assert_eq!(24, std::mem::size_of::<Vec<i64>>());
    assert_eq!(72, std::mem::size_of::<Entry<[u8; 20], i64>>());
    assert_eq!(96, std::mem::size_of::<Node<[u8; 20], i64>>());

    let key: [u8; 20] = Default::default();
    let value: Vec<u8> = vec![Default::default(); 400];
    let value = core::Value::new_upsert(Box::new(vlog::Value::new_native(value)), 0);
    let entry = Entry::new(key, value);
    let node: Node<[u8; 20], Vec<u8>> = entry.into();
    assert_eq!(520, node.footprint().unwrap());
}

#[test]
fn test_name() {
    let mut llrb: Box<Llrb<i32, Empty>> = Llrb::new("test-llrb");
    assert_eq!(llrb.to_name().unwrap(), "test-llrb".to_string());
    assert!(llrb.validate().is_ok());
}

#[test]
fn test_len() {
    let mut llrb: Box<Llrb<i32, Empty>> = Llrb::new("test-llrb");
    assert_eq!(llrb.len(), 0);
    assert!(llrb.validate().is_ok());
}

#[test]
fn test_stats() {
    let fns = |value: usize, rwl: spinlock::Stats, depths: LlrbDepth| Stats {
        name: "name".to_string(),
        entries: value,
        n_deleted: value,
        node_size: value,
        key_footprint: value as isize,
        tree_footprint: value as isize,
        rw_latch: rwl,
        blacks: Some(value),
        depths: Some(depths),
    };
    let fnl = |value: usize| spinlock::Stats {
        value: 0xC0FFEE,
        read_locks: value,
        write_locks: value,
        conflicts: value,
    };
    let fnd = |value: usize| LlrbDepth {
        samples: value,
        min: value,
        max: value,
        total: value,
        depths: [value as u64; 256],
    };
    // case 1
    let s1 = fns(1, fnl(1), fnd(1));
    let s2 = fns(2, fnl(2), fnd(2));
    let s = s1.merge(s2);
    assert_eq!(s.name.len(), 0);
    assert_eq!(s.entries, 3);
    assert_eq!(s.n_deleted, 3);
    assert_eq!(s.node_size, 1);
    assert_eq!(s.key_footprint, 3);
    assert_eq!(s.tree_footprint, 3);
    assert_eq!(s.rw_latch.read_locks, 3);
    assert_eq!(s.rw_latch.write_locks, 3);
    assert_eq!(s.rw_latch.conflicts, 3);
    assert_eq!(s.blacks, Some(1));
    assert_eq!(s.depths.as_ref().unwrap().samples, 3);
    assert_eq!(s.depths.as_ref().unwrap().min, 1);
    assert_eq!(s.depths.as_ref().unwrap().max, 2);
    assert_eq!(s.depths.as_ref().unwrap().total, 3);
    assert_eq!(
        s.depths.as_ref().unwrap().depths.to_vec(),
        [3; 256].to_vec()
    );
    // case 2
    let mut s1 = fns(1, fnl(1), fnd(1));
    s1.blacks = None;
    s1.depths = None;
    let s2 = fns(2, fnl(2), fnd(2));
    let s = s1.merge(s2);
    assert_eq!(s.name.len(), 0);
    assert_eq!(s.entries, 3);
    assert_eq!(s.n_deleted, 3);
    assert_eq!(s.node_size, 1);
    assert_eq!(s.key_footprint, 3);
    assert_eq!(s.tree_footprint, 3);
    assert_eq!(s.rw_latch.read_locks, 3);
    assert_eq!(s.rw_latch.write_locks, 3);
    assert_eq!(s.rw_latch.conflicts, 3);
    assert_eq!(s.blacks, Some(2));
    assert_eq!(s.depths.as_ref().unwrap().samples, 2);
    assert_eq!(s.depths.as_ref().unwrap().min, 2);
    assert_eq!(s.depths.as_ref().unwrap().max, 2);
    assert_eq!(s.depths.as_ref().unwrap().total, 2);
    assert_eq!(
        s.depths.as_ref().unwrap().depths.to_vec(),
        [2; 256].to_vec()
    );
    // case 3
    let s1 = fns(1, fnl(1), fnd(1));
    let mut s2 = fns(2, fnl(2), fnd(2));
    s2.blacks = None;
    s2.depths = None;
    let s = s1.merge(s2);
    assert_eq!(s.name.len(), 0);
    assert_eq!(s.entries, 3);
    assert_eq!(s.n_deleted, 3);
    assert_eq!(s.node_size, 1);
    assert_eq!(s.key_footprint, 3);
    assert_eq!(s.tree_footprint, 3);
    assert_eq!(s.rw_latch.read_locks, 3);
    assert_eq!(s.rw_latch.write_locks, 3);
    assert_eq!(s.rw_latch.conflicts, 3);
    assert_eq!(s.blacks, Some(1));
    assert_eq!(s.depths.as_ref().unwrap().samples, 1);
    assert_eq!(s.depths.as_ref().unwrap().min, 1);
    assert_eq!(s.depths.as_ref().unwrap().max, 1);
    assert_eq!(s.depths.as_ref().unwrap().total, 1);
    assert_eq!(
        s.depths.as_ref().unwrap().depths.to_vec(),
        [1; 256].to_vec()
    );
    // case 4
    let mut s1 = fns(1, fnl(1), fnd(1));
    s1.blacks = None;
    s1.depths = None;
    let mut s2 = fns(2, fnl(2), fnd(2));
    s2.blacks = None;
    s2.depths = None;
    let s = s1.merge(s2);
    assert_eq!(s.name.len(), 0);
    assert_eq!(s.entries, 3);
    assert_eq!(s.n_deleted, 3);
    assert_eq!(s.node_size, 1);
    assert_eq!(s.key_footprint, 3);
    assert_eq!(s.tree_footprint, 3);
    assert_eq!(s.rw_latch.read_locks, 3);
    assert_eq!(s.rw_latch.write_locks, 3);
    assert_eq!(s.rw_latch.conflicts, 3);
    assert!(s.blacks.is_none());
    assert!(s.depths.is_none());
}

#[test]
fn test_first_last() {
    let seed: u128 = random();
    for i in 0..500 {
        let seed = seed + (i * 10);
        let mut rng = SmallRng::from_seed(seed.to_le_bytes());
        let mut index: Box<Llrb<i64, i64>> = Llrb::new("test-llrb");

        let mut min_key = std::i64::MAX;
        let mut max_key = std::i64::MIN;
        let n_ops = rng.gen::<u8>();
        for _ in 0..n_ops {
            let key: i64 = rng.gen::<i64>().abs();
            let value: i64 = rng.gen();
            min_key = std::cmp::min(min_key, key);
            max_key = std::cmp::max(max_key, key);
            index.set(key, value).unwrap();
        }
        match (index.first(), n_ops) {
            (None, 0) => (),
            (Some(entry), _) => assert_eq!(entry.to_key(), min_key),
            _ => unreachable!(),
        }
        match (index.last(), n_ops) {
            (None, 0) => (),
            (Some(entry), _) => assert_eq!(entry.to_key(), max_key),
            _ => unreachable!(),
        }
    }
}

#[test]
fn test_lsm_sticky() {
    let missing_key = 0x123456789;
    let populate = |index: &mut Llrb<i64, i64>| -> i64 {
        for _ in 0..500 {
            let key: i64 = random::<i64>().abs();
            let value: i64 = random();
            index.set(key, value).unwrap();
        }
        let iter = index.iter().unwrap();
        let e = iter.skip(random::<u8>() as usize).next().unwrap();
        let key = e.unwrap().to_key();
        index.set(key.clone(), 9).unwrap();
        index.delete(&key).unwrap();
        index.set(key.clone(), 10).unwrap();
        index.delete(&key).unwrap();
        index.delete(&missing_key).ok();
        key
    };

    // without lsm
    let mut index: Box<Llrb<i64, i64>> = Llrb::new("test-llrb");
    let key = populate(&mut index);
    match index.get(&key) {
        Err(Error::KeyNotFound) => (),
        Err(err) => panic!("unexpected {:?}", err),
        Ok(e) => panic!("unexpected {}", e.to_seqno()),
    };
    match index.get(&missing_key) {
        Err(Error::KeyNotFound) => (),
        Err(err) => panic!("unexpected {:?}", err),
        Ok(e) => panic!("unexpected {}", e.to_seqno()),
    };

    // without lsm, with sticky
    let mut index: Box<Llrb<i64, i64>> = Llrb::new("test-llrb");
    index.set_sticky(true).unwrap();
    let key = populate(&mut index);
    match index.get(&key) {
        Err(Error::KeyNotFound) => (),
        Err(err) => panic!("unexpected {:?}", err),
        Ok(e) => {
            assert_eq!(e.is_deleted(), true);
            assert_eq!(e.to_seqno(), 504);

            let es: Vec<Entry<i64, i64>> = e.versions().collect();
            assert_eq!(es.len(), 1);
            assert_eq!(es[0].is_deleted(), true);
            assert_eq!(es[0].to_seqno(), 504);
        }
    };
    match index.get(&missing_key) {
        Err(Error::KeyNotFound) => (),
        Err(err) => panic!("unexpected {:?}", err),
        Ok(e) => {
            assert_eq!(e.is_deleted(), true);
            assert_eq!(e.to_seqno(), 505);

            let es: Vec<Entry<i64, i64>> = e.versions().collect();
            assert_eq!(es.len(), 1);
            assert_eq!(es[0].is_deleted(), true);
            assert_eq!(es[0].to_seqno(), 505);
        }
    };

    // with lsm
    let mut index: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
    index.set_sticky(true).unwrap();
    let key = populate(&mut index);
    match index.get(&key) {
        Err(Error::KeyNotFound) => (),
        Err(err) => panic!("unexpected {:?}", err),
        Ok(e) => {
            assert_eq!(e.is_deleted(), true);
            assert_eq!(e.to_seqno(), 504);

            let es: Vec<Entry<i64, i64>> = e.versions().collect();
            assert_eq!(es.len(), 5);
            let seqnos: Vec<u64> = es.iter().map(|e| e.to_seqno()).collect();
            let dels: Vec<bool> = es.iter().map(|e| e.is_deleted()).collect();
            let values: Vec<i64> = es
                // filter out values from versions
                .iter()
                .filter_map(|e| e.to_native_value())
                .collect();

            assert_eq!(&seqnos[..4], &[504, 503, 502, 501]);
            assert_eq!(dels, &[true, false, true, false, false]);
            assert_eq!(&values[..2], &[10, 9]);
            assert_eq!(es[0].is_deleted(), true);
            assert_eq!(es[0].to_seqno(), 504);
        }
    };
    match index.get(&missing_key) {
        Err(Error::KeyNotFound) => (),
        Err(err) => panic!("unexpected {:?}", err),
        Ok(e) => {
            assert_eq!(e.is_deleted(), true);
            assert_eq!(e.to_seqno(), 505);

            let es: Vec<Entry<i64, i64>> = e.versions().collect();
            assert_eq!(es.len(), 1);
            assert_eq!(es[0].is_deleted(), true);
            assert_eq!(es[0].to_seqno(), 505);
        }
    };

    assert!(index.validate().is_ok());
}

#[test]
fn test_n_deleted() {
    let missing_key = 0x123456789;
    let populate = |index: &mut Llrb<i64, i64>| {
        for _ in 0..500 {
            let key: i64 = random::<i64>().abs();
            let value: i64 = random();
            index.set(key, value).unwrap();
        }
        let keys = {
            let mut keys = vec![];
            for _ in 0..100 {
                let iter = index.iter().unwrap();
                let e = iter.skip(random::<u8>() as usize).next().unwrap();
                keys.push(e.unwrap().to_key());
            }
            keys
        };
        for key in keys.iter() {
            index.delete(key).unwrap();
        }
        assert!(index.delete(&missing_key).unwrap().is_none());
    };

    // without lsm
    let mut index: Box<Llrb<i64, i64>> = Llrb::new("test-llrb");
    populate(&mut index);
    assert_eq!(index.to_stats().unwrap().n_deleted, 0);

    // validate will make sure the that n_deleted count is correct.
    assert!(index.validate().is_ok());
}

#[test]
fn test_empty_set_cas() {
    let seed: u128 = random();
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let lsm: bool = rng.gen();
    let mut index: Box<Llrb<i64, i64>> = if lsm {
        Llrb::new_lsm("test-llrb")
    } else {
        Llrb::new("test-llrb")
    };

    match index.set_cas(10, 102, 10) {
        Err(Error::InvalidCAS(0)) => (),
        _ => panic!("expected error"),
    }

    match index.set_cas(10, 102, 0) {
        Ok(None) => (),
        Ok(Some(e)) => panic!("unexpected entry key:{}", e.to_key()),
        Err(err) => panic!("unexpected err:{:?}", err),
    }
}

#[test]
fn test_set() {
    let mut llrb: Box<Llrb<i64, i64>> = Llrb::new("test-llrb");
    let mut refns = RefNodes::new(false /*lsm*/, 10);

    assert!(llrb.set(2, 10).unwrap().is_none());
    refns.set(2, 10);
    assert!(llrb.set(1, 10).unwrap().is_none());
    refns.set(1, 10);
    assert!(llrb.set(3, 10).unwrap().is_none());
    refns.set(3, 10);
    assert!(llrb.set(6, 10).unwrap().is_none());
    refns.set(6, 10);
    assert!(llrb.set(5, 10).unwrap().is_none());
    refns.set(5, 10);
    assert!(llrb.set(4, 10).unwrap().is_none());
    refns.set(4, 10);
    assert!(llrb.set(8, 10).unwrap().is_none());
    refns.set(8, 10);
    assert!(llrb.set(0, 10).unwrap().is_none());
    refns.set(0, 10);
    assert!(llrb.set(9, 10).unwrap().is_none());
    refns.set(9, 10);
    assert!(llrb.set(7, 10).unwrap().is_none());
    refns.set(7, 10);

    assert_eq!(llrb.len(), 10);
    assert!(llrb.validate().is_ok());

    assert_eq!(refns.to_seqno(), llrb.to_seqno().unwrap());
    // test get
    for i in 0..10 {
        let entry = llrb.get(&i);
        let refn = refns.get(i);
        check_node(entry.ok(), refn);
    }
    // test iter
    {
        let (mut iter, mut iter_ref) = (llrb.iter().unwrap(), refns.iter());
        loop {
            let item = iter.next().transpose().unwrap();
            if check_node(item, iter_ref.next().cloned()) == false {
                break;
            }
        }
    }
    assert!(llrb.validate().is_ok());
}

#[test]
fn test_cas_lsm() {
    let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
    let mut refns = RefNodes::new(true /*lsm*/, 11);

    assert!(llrb.set(2, 100).unwrap().is_none());
    refns.set(2, 100);
    assert!(llrb.set(1, 100).unwrap().is_none());
    refns.set(1, 100);
    assert!(llrb.set(3, 100).unwrap().is_none());
    refns.set(3, 100);
    assert!(llrb.set(6, 100).unwrap().is_none());
    refns.set(6, 100);
    assert!(llrb.set(5, 100).unwrap().is_none());
    refns.set(5, 100);
    assert!(llrb.set(4, 100).unwrap().is_none());
    refns.set(4, 100);
    assert!(llrb.set(8, 100).unwrap().is_none());
    refns.set(8, 100);
    assert!(llrb.set(0, 100).unwrap().is_none());
    refns.set(0, 100);
    assert!(llrb.set(9, 100).unwrap().is_none());
    refns.set(9, 100);
    assert!(llrb.set(7, 100).unwrap().is_none());
    refns.set(7, 100);

    // repeated mutations on same key

    let entry = llrb.set_cas(0, 200, 8).unwrap();
    let refn = refns.set_cas(0, 200, 8);
    check_node(entry, refn);

    let entry = llrb.set_cas(5, 200, 5).unwrap();
    let refn = refns.set_cas(5, 200, 5);
    check_node(entry, refn);

    let entry = llrb.set_cas(6, 200, 4).unwrap();
    let refn = refns.set_cas(6, 200, 4);
    check_node(entry, refn);

    let entry = llrb.set_cas(9, 200, 9).unwrap();
    let refn = refns.set_cas(9, 200, 9);
    check_node(entry, refn);

    let entry = llrb.set_cas(0, 300, 11).unwrap();
    let refn = refns.set_cas(0, 300, 11);
    check_node(entry, refn);

    let entry = llrb.set_cas(5, 300, 12).unwrap();
    let refn = refns.set_cas(5, 300, 12);
    check_node(entry, refn);

    let entry = llrb.set_cas(9, 300, 14).unwrap();
    let refn = refns.set_cas(9, 300, 14);
    check_node(entry, refn);

    // create
    assert!(llrb.set_cas(10, 100, 0).ok().unwrap().is_none());
    assert!(refns.set_cas(10, 100, 0).is_none());
    // error create
    assert_eq!(llrb.set_cas(10, 100, 0).err(), Some(Error::InvalidCAS(18)));
    refns.set_cas(10, 100, 0);
    // error insert
    assert_eq!(llrb.set_cas(9, 400, 14).err(), Some(Error::InvalidCAS(17)));
    refns.set_cas(9, 400, 14);

    assert_eq!(llrb.len(), 11);
    assert!(llrb.validate().is_ok());

    assert_eq!(refns.to_seqno(), llrb.to_seqno().unwrap());
    // test get
    for i in 0..11 {
        let entry = llrb.get(&i);
        let refn = refns.get(i);
        check_node(entry.ok(), refn);
    }
    // test iter
    let (mut iter, mut iter_ref) = (llrb.iter().unwrap(), refns.iter());
    loop {
        let item = iter.next().transpose().unwrap();
        if check_node(item, iter_ref.next().cloned()) == false {
            break;
        }
    }
}

#[test]
fn test_delete() {
    let mut llrb: Box<Llrb<i64, i64>> = Llrb::new("test-llrb");
    let mut refns = RefNodes::new(false /*lsm*/, 11);

    assert!(llrb.set(2, 100).unwrap().is_none());
    refns.set(2, 100);
    assert!(llrb.set(1, 100).unwrap().is_none());
    refns.set(1, 100);
    assert!(llrb.set(3, 100).unwrap().is_none());
    refns.set(3, 100);
    assert!(llrb.set(6, 100).unwrap().is_none());
    refns.set(6, 100);
    assert!(llrb.set(5, 100).unwrap().is_none());
    refns.set(5, 100);
    assert!(llrb.set(4, 100).unwrap().is_none());
    refns.set(4, 100);
    assert!(llrb.set(8, 100).unwrap().is_none());
    refns.set(8, 100);
    assert!(llrb.set(0, 100).unwrap().is_none());
    refns.set(0, 100);
    assert!(llrb.set(9, 100).unwrap().is_none());
    refns.set(9, 100);
    assert!(llrb.set(7, 100).unwrap().is_none());
    refns.set(7, 100);

    // delete a missing node.
    assert!(llrb.delete(&10).unwrap().is_none());
    assert!(refns.delete(10).is_none());

    assert_eq!(llrb.len(), 10);
    assert!(llrb.validate().is_ok());

    assert_eq!(refns.to_seqno(), llrb.to_seqno().unwrap());
    // test iter
    //println!("start loop");
    {
        let (mut iter, mut iter_ref) = (llrb.iter().unwrap(), refns.iter());
        loop {
            let entry = iter.next().transpose().unwrap();
            let refn = iter_ref.next().cloned();
            //println!("entry: {} ref: {}", entry.is_some(), refn.is_some());
            if check_node(entry, refn) == false {
                break;
            }
        }
    }

    // delete all entry. and set new entries
    for i in 0..10 {
        let entry = llrb.delete(&i).unwrap();
        let refn = refns.delete(i);
        check_node(entry, refn);
    }
    assert_eq!(refns.to_seqno(), llrb.to_seqno().unwrap());
    assert_eq!(llrb.len(), 0);
    assert!(llrb.validate().is_ok());
    // test iter
    assert!(llrb.iter().unwrap().next().is_none());
}

#[test]
fn test_iter() {
    let mut llrb: Box<Llrb<i64, i64>> = Llrb::new("test-llrb");
    let mut refns = RefNodes::new(false /*lsm*/, 10);

    assert!(llrb.set(2, 10).unwrap().is_none());
    refns.set(2, 10);
    assert!(llrb.set(1, 10).unwrap().is_none());
    refns.set(1, 10);
    assert!(llrb.set(3, 10).unwrap().is_none());
    refns.set(3, 10);
    assert!(llrb.set(6, 10).unwrap().is_none());
    refns.set(6, 10);
    assert!(llrb.set(5, 10).unwrap().is_none());
    refns.set(5, 10);
    assert!(llrb.set(4, 10).unwrap().is_none());
    refns.set(4, 10);
    assert!(llrb.set(8, 10).unwrap().is_none());
    refns.set(8, 10);
    assert!(llrb.set(0, 10).unwrap().is_none());
    refns.set(0, 10);
    assert!(llrb.set(9, 10).unwrap().is_none());
    refns.set(9, 10);
    assert!(llrb.set(7, 10).unwrap().is_none());
    refns.set(7, 10);

    assert_eq!(llrb.len(), 10);
    assert!(llrb.validate().is_ok());

    assert_eq!(refns.to_seqno(), llrb.to_seqno().unwrap());
    // test iter
    let (mut iter, mut iter_ref) = (llrb.iter().unwrap(), refns.iter());
    loop {
        let item = iter.next().transpose().unwrap();
        match (item, iter_ref.next()) {
            (None, None) => break,
            (entry, Some(refn)) => check_node(entry, Some(refn.clone())),
            _ => panic!("invalid"),
        };
    }
    assert!(iter.next().is_none());
    assert!(iter.next().is_none());
}

#[test]
fn test_range() {
    let mut llrb: Box<Llrb<i64, i64>> = Llrb::new("test-llrb");
    let mut refns = RefNodes::new(false /*lsm*/, 10);

    assert!(llrb.set(2, 10).unwrap().is_none());
    refns.set(2, 10);
    assert!(llrb.set(1, 10).unwrap().is_none());
    refns.set(1, 10);
    assert!(llrb.set(3, 10).unwrap().is_none());
    refns.set(3, 10);
    assert!(llrb.set(6, 10).unwrap().is_none());
    refns.set(6, 10);
    assert!(llrb.set(5, 10).unwrap().is_none());
    refns.set(5, 10);
    assert!(llrb.set(4, 10).unwrap().is_none());
    refns.set(4, 10);
    assert!(llrb.set(8, 10).unwrap().is_none());
    refns.set(8, 10);
    assert!(llrb.set(0, 10).unwrap().is_none());
    refns.set(0, 10);
    assert!(llrb.set(9, 10).unwrap().is_none());
    refns.set(9, 10);
    assert!(llrb.set(7, 10).unwrap().is_none());
    refns.set(7, 10);

    assert_eq!(llrb.len(), 10);
    assert!(llrb.validate().is_ok());

    assert_eq!(refns.to_seqno(), llrb.to_seqno().unwrap());
    // test range
    for _ in 0..1_000 {
        let (low, high) = random_low_high(llrb.len());

        {
            let mut iter = llrb.range((low, high)).unwrap();
            let mut iter_ref = refns.range(low, high);
            loop {
                let item = iter.next().transpose().unwrap();
                match (item, iter_ref.next()) {
                    (None, None) => break,
                    (entry, Some(refn)) => check_node(entry, Some(refn.clone())),
                    _ => panic!("invalid"),
                };
            }
            assert!(iter.next().is_none());
            assert!(iter.next().is_none());
        }

        {
            let mut iter = llrb.reverse((low, high)).unwrap();
            let mut iter_ref = refns.reverse(low, high);
            loop {
                let item = iter.next().transpose().unwrap();
                match (item, iter_ref.next()) {
                    (None, None) => break,
                    (entry, Some(refn)) => check_node(entry, Some(refn.clone())),
                    _ => panic!("invalid"),
                };
            }
            assert!(iter.next().is_none());
            assert!(iter.next().is_none());
        }
    }
}

//TODO: enable this test case once str/String is added to types.rs.
//#[test]
//fn test_range_str() {
//    let mut llrb: Box<Llrb<&str, i64>> = Llrb::new("test-llrb");
//
//    assert!(llrb.set("key1", 10).unwrap().is_none());
//    assert!(llrb.set("key2", 11).unwrap().is_none());
//    assert!(llrb.set("key3", 12).unwrap().is_none());
//    assert!(llrb.set("key4", 13).unwrap().is_none());
//    assert!(llrb.set("key5", 14).unwrap().is_none());
//
//    assert_eq!(llrb.len(), 5);
//    assert!(llrb.validate().is_ok());
//
//    let r = ops::RangeInclusive::new("key2", "key4");
//    let mut iter = llrb.range(r).unwrap();
//    let entry = iter
//        .next()
//        .transpose()
//        .unwrap()
//        .expect("expected entry for key2");
//    assert_eq!(entry.to_key(), "key2");
//    assert_eq!(entry.to_native_value().unwrap(), 11);
//    let entry = iter
//        .next()
//        .transpose()
//        .unwrap()
//        .expect("expected entry for key3");
//    assert_eq!(entry.to_key(), "key3");
//    assert_eq!(entry.to_native_value().unwrap(), 12);
//    let entry = iter
//        .next()
//        .transpose()
//        .unwrap()
//        .expect("expected entry for key4");
//    assert_eq!(entry.to_key(), "key4");
//    assert_eq!(entry.to_native_value().unwrap(), 13);
//    assert!(iter.next().is_none());
//
//    let r = ops::RangeInclusive::new("key2", "key4");
//    let mut iter = llrb.reverse(r).unwrap();
//    let entry = iter
//        .next()
//        .transpose()
//        .unwrap()
//        .expect("expected entry for key4");
//    assert_eq!(entry.to_key(), "key4");
//    assert_eq!(entry.to_native_value().unwrap(), 13);
//    let entry = iter
//        .next()
//        .transpose()
//        .unwrap()
//        .expect("expected entry for key3");
//    assert_eq!(entry.to_key(), "key3");
//    assert_eq!(entry.to_native_value().unwrap(), 12);
//    let entry = iter
//        .next()
//        .transpose()
//        .unwrap()
//        .expect("expected entry for key2");
//    assert_eq!(entry.to_key(), "key2");
//    assert_eq!(entry.to_native_value().unwrap(), 11);
//    assert!(iter.next().is_none());
//}

#[test]
fn test_crud() {
    let size = 1000;
    let mut llrb: Box<Llrb<i64, i64>> = Llrb::new("test-llrb");
    let mut refns = RefNodes::new(false /*lsm*/, size);

    for _ in 0..100000 {
        let key: i64 = (random::<i64>() % (size as i64)).abs();
        let value: i64 = random();
        let op: i64 = (random::<i64>() % 3).abs();
        //println!("key {} value {} op {}", key, value, op);
        match op {
            0 => {
                let entry = llrb.set(key, value).unwrap();
                let refn = refns.set(key, value);
                check_node(entry, refn);
                false
            }
            1 => {
                let off: usize = key.try_into().unwrap();
                let refn = &refns.entries[off];
                let cas = if refn.versions.len() > 0 {
                    refn.to_seqno()
                } else {
                    0
                };

                let entry = llrb.set_cas(key, value, cas).ok().unwrap();
                let refn = refns.set_cas(key, value, cas);
                check_node(entry, refn);
                false
            }
            2 => {
                let entry = llrb.delete(&key).unwrap();
                let refn = refns.delete(key);
                check_node(entry, refn);
                true
            }
            op => panic!("unreachable {}", op),
        };

        assert!(llrb.validate().is_ok());
    }

    //println!("len {}", llrb.len());
    assert_eq!(refns.to_seqno(), llrb.to_seqno().unwrap());

    {
        // test iter
        let (mut iter, mut iter_ref) = (llrb.iter().unwrap(), refns.iter());
        loop {
            let item = iter.next().transpose().unwrap();
            if check_node(item, iter_ref.next().cloned()) == false {
                break;
            }
        }
    }

    // ranges and reverses
    for _ in 0..10000 {
        let (low, high) = random_low_high(size);
        //println!("test loop {:?} {:?}", low, high);

        {
            let mut iter = llrb.range((low, high)).unwrap();
            let mut iter_ref = refns.range(low, high);
            loop {
                let item = iter.next().transpose().unwrap();
                if check_node(item, iter_ref.next().cloned()) == false {
                    break;
                }
            }
        }

        {
            let mut iter = llrb.reverse((low, high)).unwrap();
            let mut iter_ref = refns.reverse(low, high);
            loop {
                let item = iter.next().transpose().unwrap();
                if check_node(item, iter_ref.next().cloned()) == false {
                    break;
                }
            }
        }
    }
}

#[test]
fn test_crud_lsm() {
    let size = 1000;
    let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
    let mut refns = RefNodes::new(true /*lsm*/, size as usize);

    for _i in 0..20000 {
        let key: i64 = (random::<i64>() % size).abs();
        let value: i64 = random();
        let op: i64 = (random::<i64>() % 3).abs();
        // println!("test_crud_lsm seqno:{} op:{} key:{}", _i + 1, op, key);
        match op {
            0 => {
                let entry = llrb.set(key, value).unwrap();
                let refn = refns.set(key, value);
                check_node(entry, refn);
                false
            }
            1 => {
                let off: usize = key.try_into().unwrap();
                let refn = &refns.entries[off];
                let cas = if refn.versions.len() > 0 {
                    refn.to_seqno()
                } else {
                    0
                };

                // println!("set_cas {} {}", key, cas);
                let entry = llrb.set_cas(key, value, cas).ok().unwrap();
                let refn = refns.set_cas(key, value, cas);
                check_node(entry, refn);
                false
            }
            2 => {
                let entry = llrb.delete(&key).unwrap();
                let refn = refns.delete(key);
                check_node(entry, refn);
                true
            }
            op => panic!("unreachable {}", op),
        };
        assert_eq!(llrb.to_seqno().unwrap(), refns.to_seqno());

        assert!(llrb.validate().is_ok());
    }

    // println!("len {}", llrb.len());
    assert_eq!(refns.to_seqno(), llrb.to_seqno().unwrap());

    {
        // test iter
        let (mut iter, mut iter_ref) = (llrb.iter().unwrap(), refns.iter());
        loop {
            let item = iter.next().transpose().unwrap();
            if check_node(item, iter_ref.next().cloned()) == false {
                break;
            }
        }
    }

    // ranges and reverses
    for _ in 0..3000 {
        let (low, high) = random_low_high(size as usize);
        // println!("test loop {:?} {:?}", low, high);

        {
            let mut iter = llrb.range((low, high)).unwrap();
            let mut iter_ref = refns.range(low, high);
            loop {
                let item = iter.next().transpose().unwrap();
                if check_node(item, iter_ref.next().cloned()) == false {
                    break;
                }
            }
        }

        {
            let mut iter = llrb.reverse((low, high)).unwrap();
            let mut iter_ref = refns.reverse(low, high);
            loop {
                let item = iter.next().transpose().unwrap();
                if check_node(item, iter_ref.next().cloned()) == false {
                    break;
                }
            }
        }
    }
}

#[test]
fn test_pw_scan() {
    let mut llrb: Box<Llrb<i32, i32>> = Llrb::new_lsm("test-llrb");

    // populate
    for key in 0..10000 {
        let value = (key + 1) * 100;
        assert!(llrb.set(key, value).unwrap().is_none());
    }

    assert_eq!(llrb.len(), 10000);
    assert_eq!(llrb.to_seqno().unwrap(), 10000);
    let seqno1 = llrb.to_seqno().unwrap();

    let mut iter = scans::SkipScan::new(llrb.to_reader().unwrap());
    iter.set_seqno_range(..=seqno1).unwrap();
    for (i, entry) in iter.enumerate() {
        let entry = entry.unwrap();
        let ref_key = i as i32;
        let ref_value = (ref_key + 1) * 100;
        assert_eq!(entry.to_key(), ref_key);
        assert_eq!(entry.to_native_value().unwrap(), ref_value);
    }

    // first-inject.
    for key in (0..1000).step_by(3) {
        let value = (key + 1) * 1000;
        assert!(llrb.set(key, value).unwrap().is_some());
    }
    assert_eq!(llrb.len(), 10000);
    assert_eq!(llrb.to_seqno().unwrap(), 10334);

    // skip scan after first-inject.
    let mut iter = scans::SkipScan::new(llrb.to_reader().unwrap());
    iter.set_seqno_range(..=seqno1).unwrap();
    for (i, entry) in iter.enumerate() {
        let entry = entry.unwrap();
        let ref_key = i as i32;
        let ref_value = (ref_key + 1) * 100;

        let vers: Vec<Entry<i32, i32>> = entry.versions().collect();
        assert_eq!(vers.len(), 1);

        assert_eq!(entry.to_key(), ref_key);
        assert_eq!(entry.to_native_value().unwrap(), ref_value);
    }

    // second-inject.
    for key in (0..1000).step_by(3) {
        let value = (key + 1) * 10000;
        assert!(llrb.set(key, value).unwrap().is_some());
    }
    for key in (0..1000).step_by(5) {
        let value = (key + 1) * 1000;
        assert!(llrb.set(key, value).unwrap().is_some());
    }

    assert_eq!(llrb.len(), 10000);
    assert_eq!(llrb.to_seqno().unwrap(), 10868);

    let seqno2 = llrb.to_seqno().unwrap();

    // third-inject.
    for key in (0..1000).step_by(15) {
        let value = (key + 1) * 100000;
        assert!(llrb.set(key, value).unwrap().is_some());
    }
    assert_eq!(llrb.len(), 10000);
    assert_eq!(llrb.to_seqno().unwrap(), 10935);

    // skip scan in-between.
    let mut iter = scans::SkipScan::new(llrb.to_reader().unwrap());
    iter.set_seqno_range((Bound::Excluded(seqno1), Bound::Included(seqno2)))
        .unwrap();
    for entry in iter {
        let entry = entry.unwrap();
        let key = entry.to_key();
        match key {
            0 => {
                let ref_key = 0_i32;
                let vers: Vec<Entry<i32, i32>> = entry.versions().collect();
                assert_eq!(entry.to_key(), ref_key);
                for (i, ver) in vers.into_iter().enumerate() {
                    match (i, ver.to_native_value().unwrap()) {
                        (0, value) => assert_eq!(value, 1000),
                        (1, value) => assert_eq!(value, 10000),
                        (2, value) => assert_eq!(value, 1000),
                        _ => unreachable!(),
                    }
                }
            }
            key if key % 15 == 0 => {
                let ref_key = key as i32;
                let vers: Vec<Entry<i32, i32>> = entry.versions().collect();
                assert_eq!(entry.to_key(), ref_key);
                for (i, ver) in vers.into_iter().enumerate() {
                    match (i, ver.to_native_value().unwrap()) {
                        (0, value) => assert_eq!(value, (key + 1) * 1000),
                        (1, value) => assert_eq!(value, (key + 1) * 10000),
                        (2, value) => assert_eq!(value, (key + 1) * 1000),
                        _ => unreachable!(),
                    }
                }
            }
            key if key % 3 == 0 => {
                let ref_key = key as i32;
                let vers: Vec<Entry<i32, i32>> = entry.versions().collect();
                assert_eq!(entry.to_key(), ref_key);
                for (i, ver) in vers.into_iter().enumerate() {
                    match (i, ver.to_native_value().unwrap()) {
                        (0, value) => assert_eq!(value, (key + 1) * 10000),
                        (1, value) => assert_eq!(value, (key + 1) * 1000),
                        _ => unreachable!(),
                    }
                }
            }
            key if key % 5 == 0 => {
                let ref_key = key as i32;
                let vers: Vec<Entry<i32, i32>> = entry.versions().collect();
                assert_eq!(entry.to_key(), ref_key);
                for (i, ver) in vers.into_iter().enumerate() {
                    match (i, ver.to_native_value().unwrap()) {
                        (0, value) => assert_eq!(value, (key + 1) * 1000),
                        _ => unreachable!(),
                    }
                }
            }
            key => {
                let vers: Vec<Entry<i32, i32>> = entry.versions().collect();
                panic!("unexpected key {} num-versions: {}", key, vers.len());
            }
        }
    }

    // skip scan final.
    let mut iter = scans::SkipScan::new(llrb.to_reader().unwrap());
    iter.set_seqno_range((Bound::Excluded(seqno2), Bound::Unbounded))
        .unwrap();
    let mut ref_key = 0;
    for entry in iter {
        let entry = entry.unwrap();
        let ref_value = (ref_key + 1) * 100000;
        assert_eq!(entry.to_key(), ref_key);
        assert_eq!(entry.to_native_value().unwrap(), ref_value);
        let vers: Vec<Entry<i32, i32>> = entry.versions().collect();
        assert_eq!(vers.len(), 1);
        ref_key += 15;
    }

    assert!(llrb.validate().is_ok());
}

#[test]
fn test_mvcc_conversion() {
    let seed: u128 = random();
    for i in 0..50 {
        let seed = seed + (i * 10);
        let mut rng = SmallRng::from_seed(seed.to_le_bytes());

        let lsm: bool = rng.gen();
        let sticky: bool = rng.gen();
        let spin: bool = rng.gen();

        let mut llrb: Box<Llrb<i64, i64>> = if lsm {
            Llrb::new_lsm("test-llrb")
        } else {
            Llrb::new("test-llrb")
        };
        llrb.set_sticky(sticky).unwrap();
        llrb.set_spinlatch(spin).unwrap();

        let n_ops = match rng.gen::<u8>() % 10 {
            0 => 0,
            1 => 1,
            _ => i64::abs(rng.gen::<i64>() % 60_000),
        };
        let key_max = 20_000;
        random_llrb(n_ops, key_max, seed, &mut llrb);
        println!(
            "index-config: lsm:{} sticky:{} spin:{} nops:{} key_max:{}",
            lsm, sticky, spin, n_ops, key_max
        );

        let mut refllrb: Box<Llrb<i64, i64>> = llrb.clone();
        let mut mvcc: Box<Mvcc<i64, i64>> = TryFrom::try_from(*llrb).unwrap();

        assert_eq!(mvcc.is_lsm(), refllrb.is_lsm());
        assert_eq!(mvcc.is_sticky(), refllrb.is_sticky());
        assert_eq!(mvcc.is_spin(), refllrb.is_spin());
        assert_eq!(mvcc.to_seqno().unwrap(), refllrb.to_seqno().unwrap());
        let (lstats, mstats) = (refllrb.to_stats().unwrap(), mvcc.to_stats().unwrap());
        assert_eq!(lstats.entries, mstats.entries);
        assert_eq!(lstats.n_deleted, mstats.n_deleted);
        assert_eq!(lstats.key_footprint, mstats.key_footprint);
        assert_eq!(lstats.tree_footprint, mstats.tree_footprint);
        {
            let mut liter = refllrb.iter().unwrap();
            let mut miter = mvcc.iter().unwrap();
            loop {
                match (liter.next(), miter.next()) {
                    (Some(Ok(lentry)), Some(Ok(mentry))) => {
                        check_node1(&lentry, &mentry);
                    }
                    (None, None) => break,
                    _ => unreachable!(),
                }
            }
        }

        let mut refmvcc: Box<Mvcc<i64, i64>> = mvcc.clone().unwrap();
        let mut llrb: Box<Llrb<i64, i64>> = TryFrom::try_from(*mvcc).unwrap();

        assert_eq!(refmvcc.is_lsm(), llrb.is_lsm());
        assert_eq!(refmvcc.is_sticky(), llrb.is_sticky());
        assert_eq!(refmvcc.is_spin(), llrb.is_spin());
        assert_eq!(refmvcc.to_seqno().unwrap(), llrb.to_seqno().unwrap());
        let (lstats, mstats) = (llrb.to_stats().unwrap(), refmvcc.to_stats().unwrap());
        assert_eq!(lstats.entries, mstats.entries);
        assert_eq!(lstats.n_deleted, mstats.n_deleted);
        assert_eq!(lstats.key_footprint, mstats.key_footprint);
        assert_eq!(lstats.tree_footprint, mstats.tree_footprint);
        {
            let mut liter = llrb.iter().unwrap();
            let mut miter = refmvcc.iter().unwrap();
            loop {
                match (liter.next(), miter.next()) {
                    (Some(Ok(lentry)), Some(Ok(mentry))) => {
                        check_node1(&lentry, &mentry);
                    }
                    (None, None) => break,
                    _ => unreachable!(),
                }
            }
        }
    }
}

#[test]
fn test_split() {
    let seed: u128 = random();
    // let seed: u128 = 329414500652693436592311104669973016490;
    println!("seed:{}", seed,);
    for i in 0..50 {
        let seed = seed + (i * 10);
        let mut rng = SmallRng::from_seed(seed.to_le_bytes());

        let lsm: bool = rng.gen();
        let sticky: bool = rng.gen();
        let spin: bool = rng.gen();

        let mut llrb: Box<Llrb<i64, i64>> = if lsm {
            Llrb::new_lsm("test-llrb")
        } else {
            Llrb::new("test-llrb")
        };
        llrb.set_sticky(sticky).unwrap();
        llrb.set_spinlatch(spin).unwrap();

        let n_ops = match rng.gen::<u8>() % 10 {
            0 => 0,
            1 => 1,
            _ => i64::abs(rng.gen::<i64>() % 60_000),
        };
        let key_max = 20_000;
        random_llrb(n_ops, key_max, seed, &mut llrb);
        println!(
            "index-config: lsm:{} sticky:{} spin:{} nops:{} key_max:{}",
            lsm, sticky, spin, n_ops, key_max,
        );

        let mut refllrb = llrb.clone();
        let (mut first, mut second) = llrb
            .split("first".to_string(), "second".to_string())
            .unwrap();
        assert_eq!(first.to_name().unwrap(), "first".to_string());
        assert_eq!(second.to_name().unwrap(), "second".to_string());
        let mut iter = first.iter().unwrap().chain(second.iter().unwrap());
        let mut refiter = refllrb.iter().unwrap();
        loop {
            match (iter.next(), refiter.next()) {
                (Some(Ok(lentry)), Some(Ok(mentry))) => {
                    check_node1(&lentry, &mentry);
                }
                (None, None) => break,
                _ => unreachable!(),
            }
        }
    }
}

#[test]
fn test_commit1() {
    let mut index1: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-index1");
    let mut index2: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-index2");
    let mut rindex: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-ref-index");

    index1
        .commit(
            CommitIter::new(
                index2.to_reader().unwrap(),
                (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded),
            ),
            |meta| meta.clone(),
        )
        .unwrap();
    check_commit_nodes(index1.as_mut(), rindex.as_mut());
}

#[test]
fn test_commit2() {
    let mut index1: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-index1");
    let mut index2: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-index2");
    let mut rindex: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-ref-index");

    index2.set(100, 200).unwrap();
    rindex.set(100, 200).unwrap();

    index1
        .commit(
            CommitIter::new(
                index2.to_reader().unwrap(),
                (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded),
            ),
            |meta| meta.clone(),
        )
        .unwrap();
    check_commit_nodes(index1.as_mut(), rindex.as_mut());
}

#[test]
fn test_commit3() {
    let seed: u128 = random();
    // let seed: u128 = 137122643011174645787755929141427491522;
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
    println!("seed {}", seed);

    for _i in 0..100 {
        let lsm: bool = rng.gen();
        let sticky: bool = lsm || true;

        let (mut index1, mut index2, mut rindex) = if lsm {
            (
                Llrb::<i64, i64>::new_lsm("test-index1"),
                Llrb::<i64, i64>::new_lsm("test-index2"),
                Llrb::<i64, i64>::new_lsm("test-ref-index"),
            )
        } else {
            (
                Llrb::<i64, i64>::new("test-index1"),
                Llrb::<i64, i64>::new("test-index2"),
                Llrb::<i64, i64>::new("test-ref-index"),
            )
        };
        index1.set_sticky(sticky).unwrap();
        index2.set_sticky(sticky).unwrap();
        rindex.set_sticky(sticky).unwrap();
        //  println!("index-config: lsm:{} sticky:{}", lsm, sticky);

        let n_ops = rng.gen::<usize>() % 1000;
        for _ in 0..n_ops {
            let key: i64 = rng.gen::<i64>().abs() % (n_ops as i64 * 3);
            let value: i64 = rng.gen();
            let op: i64 = (rng.gen::<i64>() % 2).abs();
            //  println!("target k:{} v:{} {}", key, value, op);
            match op {
                0 => {
                    index1.set(key, value).unwrap();
                    rindex.set(key, value).unwrap();
                }
                1 => {
                    index1.delete(&key).unwrap();
                    rindex.delete(&key).unwrap();
                }
                op => panic!("unreachable {}", op),
            };
        }
        index2.set_seqno(index1.to_seqno().unwrap()).unwrap();

        let n_ops = rng.gen::<usize>() % 1000;
        for _ in 0..n_ops {
            let key: i64 = rng.gen::<i64>().abs() % (n_ops as i64 * 3);
            let value: i64 = rng.gen();
            let op: i64 = (rng.gen::<i64>() % 2).abs();
            //  println!("commit k:{} v:{} {}", key, value, op);
            match op {
                0 => {
                    index2.set(key, value).unwrap();
                    rindex.set(key, value).unwrap();
                }
                1 => {
                    index2.delete(&key).unwrap();
                    rindex.delete(&key).unwrap();
                }
                op => panic!("unreachable {}", op),
            };
        }

        index1
            .commit(
                CommitIter::new(
                    index2.to_reader().unwrap(),
                    (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded),
                ),
                |meta| meta.clone(),
            )
            .unwrap();
        check_commit_nodes(index1.as_mut(), rindex.as_mut());
    }
}

fn check_commit_nodes(index: &mut Llrb<i64, i64>, rindex: &mut Llrb<i64, i64>) {
    // verify root index
    assert_eq!(index.seqno, rindex.seqno);
    assert_eq!(index.n_count, rindex.n_count);
    assert_eq!(index.n_deleted, rindex.n_deleted);
    assert_eq!(index.key_footprint, rindex.key_footprint);
    assert_eq!(index.tree_footprint, rindex.tree_footprint);

    // verify each entry
    let mut iter = index.iter().unwrap();
    let mut refiter = rindex.iter().unwrap();
    loop {
        let entry = iter.next().transpose().unwrap();
        let refentry = refiter.next().transpose().unwrap();
        let (entry, refentry) = match (entry, refentry) {
            (Some(entry), Some(refentry)) => (entry, refentry),
            (None, None) => break,
            _ => unreachable!(),
        };
        assert_eq!(entry.to_key(), refentry.to_key());
        let key = entry.to_key();
        assert_eq!(entry.to_seqno(), refentry.to_seqno(), "key {}", key);
        assert_eq!(entry.is_deleted(), refentry.is_deleted(), "key {}", key);
        assert_eq!(
            entry.to_native_value(),
            refentry.to_native_value(),
            "key {}",
            key
        );

        let mut di = entry.as_deltas().iter();
        let mut ri = refentry.as_deltas().iter();
        loop {
            let (delta, refdelta) = (di.next(), ri.next());
            let (delta, refdelta) = match (delta, refdelta) {
                (Some(delta), Some(refdelta)) => (delta, refdelta),
                (None, None) => break,
                _ => unreachable!(),
            };
            assert_eq!(delta.to_seqno(), refdelta.to_seqno(), "key {}", key);
            assert_eq!(delta.to_diff(), refdelta.to_diff(), "key {}", key);
            assert_eq!(delta.is_deleted(), refdelta.is_deleted(), "key {}", key);
            assert_eq!(
                delta.to_seqno_state(),
                refdelta.to_seqno_state(),
                "key {}",
                key
            );
        }
    }
}

#[test]
fn test_compact() {
    let seed: u128 = random();
    // let seed: u128 = 2726664888361513285714080784886255657;
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
    println!("seed {}", seed);

    for _i in 0..50 {
        let lsm: bool = rng.gen();
        let sticky: bool = lsm || true;

        let (mut index, mut rindex) = if lsm {
            (
                Llrb::<i64, i64>::new_lsm("test-index"),
                Llrb::<i64, i64>::new_lsm("test-ref-index"),
            )
        } else {
            (
                Llrb::<i64, i64>::new("test-index"),
                Llrb::<i64, i64>::new("test-ref-index"),
            )
        };
        index.set_sticky(sticky).unwrap();
        rindex.set_sticky(sticky).unwrap();
        let n_ops = rng.gen::<usize>() % 100_000;
        for _ in 0..n_ops {
            let key: i64 = rng.gen::<i64>().abs() % (n_ops as i64 / 2);
            let value: i64 = rng.gen();
            let op: i64 = (rng.gen::<i64>() % 2).abs();
            //  println!("target k:{} v:{} {}", key, value, op);
            match op {
                0 => {
                    index.set(key, value).unwrap();
                    rindex.set(key, value).unwrap();
                }
                1 => {
                    index.delete(&key).unwrap();
                    rindex.delete(&key).unwrap();
                }
                op => panic!("unreachable {}", op),
            };
        }

        let cutoff = match rng.gen::<u8>() % 3 {
            0 => Bound::Excluded(rng.gen::<u64>() % (n_ops as u64) / 2),
            1 => Bound::Included(rng.gen::<u64>() % (n_ops as u64) / 2),
            2 => Bound::Unbounded,
            _ => unreachable!(),
        };
        let cutoff = match rng.gen::<u8>() % 2 {
            0 => Cutoff::new_tombstone(cutoff),
            1 => Cutoff::new_lsm(cutoff),
            _ => unreachable!(),
        };
        println!(
            "index-config: lsm:{} sticky:{} n_ops:{} cutoff:{:?}",
            lsm, sticky, n_ops, cutoff
        );

        let count = index.compact(cutoff).unwrap();
        assert_eq!(count, rindex.to_stats().unwrap().entries);
        check_compact_nodes(index.as_mut(), rindex.as_mut(), cutoff);
    }
}

fn check_compact_nodes(
    index: &mut Llrb<i64, i64>,
    rindex: &mut Llrb<i64, i64>,
    cutoff: Cutoff, // either tombstone/lsm.
) {
    // verify root index
    assert_eq!(index.seqno, rindex.seqno);

    let mut n_count = 0;
    let mut n_deleted = 0;
    let mut key_footprint = 0;
    let mut tree_footprint = 0;

    let res: Vec<Entry<i64, i64>> = rindex
        .iter()
        .unwrap()
        .map(|e| e.unwrap())
        .filter_map(|e| e.purge(cutoff.clone()))
        .collect();

    {
        let mut iter = index.iter().unwrap();
        let mut refiter = res.into_iter();
        loop {
            let entry = iter.next().transpose().unwrap();
            let refn = refiter.next();
            let (entry, refn) = match (entry, refn) {
                (Some(entry), Some(refn)) => (entry, refn),
                (None, Some(refn)) => {
                    let (key, seqno) = (refn.to_key(), refn.to_seqno());
                    panic!("refn is {} {}", key, seqno)
                }
                (Some(refn), None) => {
                    let (key, seqno) = (refn.to_key(), refn.to_seqno());
                    panic!("refn is {} {}", key, seqno)
                }
                (None, None) => break,
            };
            check_node1(&entry, &refn);

            n_count += 1;
            if entry.is_deleted() {
                n_deleted += 1;
            }
            key_footprint += util::key_footprint(entry.as_key()).unwrap();
            tree_footprint += {
                let size = mem::size_of::<Node<i64, i64>>();
                let overhead: isize = size.try_into().unwrap();
                overhead + entry.footprint().unwrap()
            };
        }
    }

    assert_eq!(n_count, index.n_count);
    assert_eq!(n_deleted, index.n_deleted);
    assert_eq!(key_footprint, index.key_footprint);
    assert_eq!(
        tree_footprint, index.tree_footprint,
        "for n_count {} n_deleted {}",
        n_count, n_deleted
    );
}

#[test]
fn test_commit_iterator_scan() {
    let seed: u128 = random();
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let (n_ops, key_max) = (60_000_i64, 20_000);
    let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
    random_llrb(n_ops, key_max, seed, &mut llrb);

    for i in 0..20 {
        let from_seqno = match rng.gen::<i64>() % n_ops {
            n if n >= 0 && n % 2 == 0 => Bound::Included(n as u64),
            n if n >= 0 => Bound::Excluded(n as u64),
            _ => Bound::Unbounded,
        };
        let mut r = llrb.to_reader().unwrap();
        let mut ref_iter = r.iter().unwrap();
        let within = (from_seqno, Bound::Included(llrb.to_seqno().unwrap()));
        let mut count = 0;
        let mut iter = llrb.scan(within.clone()).unwrap();
        loop {
            match ref_iter.next() {
                Some(Ok(ref_entry)) => match ref_entry.filter_within(within.0, within.1) {
                    Some(ref_entry) => match iter.next() {
                        Some(Ok(entry)) => {
                            check_node1(&entry, &ref_entry);
                            count += 1;
                        }
                        Some(Err(err)) => panic!("{:?}", err),
                        None => unreachable!(),
                    },
                    None => continue,
                },
                Some(Err(err)) => panic!("{:?}", err),
                None => {
                    assert!(iter.next().is_none());
                    break;
                }
            }
        }
        println!("{} {:?} {}", i, within, count);
    }
}

#[test]
fn test_commit_iterator_scans1() {
    let seed: u128 = random();
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let (n_ops, key_max) = (60_000_i64, 20_000);
    let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
    random_llrb(n_ops, key_max, seed, &mut llrb);

    for i in 0..20 {
        let shards = rng.gen::<usize>() % 31 + 1;
        let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
        let iters = llrb.scans(shards, within.clone()).unwrap();

        let mut counts: Vec<usize> = vec![];
        for iter in iters.into_iter() {
            counts.push(iter.map(|_| 1).collect::<Vec<usize>>().into_iter().sum());
        }
        println!("{} {} {:?}", i, shards, counts);

        let avg = llrb.len() / shards;
        for (i, count) in counts.into_iter().enumerate() {
            assert!(
                ((count as f64) / (avg as f64)) > 0.10,
                "{} shard {} / {}",
                i,
                count,
                avg
            )
        }
    }
}

#[test]
fn test_commit_iterator_scans2() {
    let seed: u128 = random();
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let (n_ops, key_max) = (60_000_i64, 20_000);
    let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
    random_llrb(n_ops, key_max, seed, &mut llrb);

    for i in 0..20 {
        let shards = rng.gen::<usize>() % 31 + 1;
        let from_seqno = match rng.gen::<i64>() % n_ops {
            n if n >= 0 && n % 2 == 0 => Bound::Included(n as u64),
            n if n >= 0 => Bound::Excluded(n as u64),
            _ => Bound::Unbounded,
        };
        let mut r = llrb.to_reader().unwrap();
        let within = (from_seqno, Bound::Included(llrb.to_seqno().unwrap()));
        let mut iter = {
            let mut iters = llrb.scans(shards, within.clone()).unwrap();
            iters.reverse(); // make this to stack
            let w = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
            scans::FilterScans::new(iters, w)
        };
        let mut ref_iter = r.iter().unwrap();
        let mut count = 0;
        loop {
            match ref_iter.next() {
                Some(Ok(ref_entry)) => match ref_entry.filter_within(within.0, within.1) {
                    Some(ref_entry) => match iter.next() {
                        Some(Ok(entry)) => {
                            check_node1(&entry, &ref_entry);
                            count += 1;
                        }
                        Some(Err(err)) => panic!("{:?}", err),
                        None => unreachable!(),
                    },
                    None => continue,
                },
                Some(Err(err)) => panic!("{:?}", err),
                None => {
                    assert!(iter.next().is_none());
                    break;
                }
            }
        }
        println!("{} {:?} {}", i, within, count);
    }
}

#[test]
fn test_commit_iterator_range_scans() {
    use std::ops::Bound;

    let seed: u128 = random();
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let (n_ops, key_max) = (128_000_i64, 20_000);
    let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
    random_llrb(n_ops, key_max, seed, &mut llrb);

    for i in 1..33 {
        let (mut ranges, mut low_key) = (vec![], Bound::Unbounded);
        for high_key in (0..i).map(|j| (n_ops / i) * (j + 1)) {
            ranges.push((low_key, Bound::Excluded(high_key)));
            low_key = Bound::Included(high_key);
        }
        ranges.push((low_key, Bound::Unbounded));

        let from_seqno = match rng.gen::<i64>() % n_ops {
            n if n >= 0 && n % 2 == 0 => Bound::Included(n as u64),
            n if n >= 0 => Bound::Excluded(n as u64),
            _ => Bound::Unbounded,
        };
        let mut r = llrb.to_reader().unwrap();
        let within = (from_seqno, Bound::Included(llrb.to_seqno().unwrap()));
        let mut iter = {
            let mut iters = llrb.range_scans(ranges, within.clone()).unwrap();
            iters.reverse(); // make this to stack
            let w = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
            scans::FilterScans::new(iters, w)
        };
        let mut ref_iter = r.iter().unwrap();
        let mut count = 0;
        loop {
            match ref_iter.next() {
                Some(Ok(ref_entry)) => match ref_entry.filter_within(within.0, within.1) {
                    Some(ref_entry) => match iter.next() {
                        Some(Ok(entry)) => {
                            check_node1(&entry, &ref_entry);
                            count += 1;
                        }
                        Some(Err(err)) => panic!("{:?}", err),
                        None => unreachable!(),
                    },
                    None => continue,
                },
                Some(Err(err)) => panic!("{:?}", err),
                None => {
                    assert!(iter.next().is_none());
                    break;
                }
            }
        }
        println!("{} {:?} {}", i, within, count);
    }
}

fn random_llrb(n_ops: i64, key_max: i64, seed: u128, llrb: &mut Llrb<i64, i64>) {
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
    for _i in 0..n_ops {
        let key = (rng.gen::<i64>() % key_max).abs();
        let op = rng.gen::<usize>() % 3;
        //println!("key {} {} {} {}", key, llrb.to_seqno(), op);
        match op {
            0 => {
                let value: i64 = rng.gen();
                llrb.set(key, value).unwrap();
            }
            1 => {
                let value: i64 = rng.gen();
                {
                    let cas = match llrb.get(&key) {
                        Err(Error::KeyNotFound) => 0,
                        Err(_err) => unreachable!(),
                        Ok(e) => e.to_seqno(),
                    };
                    llrb.set_cas(key, value, cas).unwrap();
                }
            }
            2 => {
                llrb.delete(&key).unwrap();
            }
            _ => unreachable!(),
        }
    }
}

fn check_node1(entry: &Entry<i64, i64>, ref_entry: &Entry<i64, i64>) {
    //println!("check_node {} {}", entry.key(), ref_entry.key);
    assert_eq!(entry.to_key(), ref_entry.to_key(), "key");

    let key = entry.to_key();
    //println!("check-node value {:?}", entry.to_native_value());
    assert_eq!(
        entry.to_native_value(),
        ref_entry.to_native_value(),
        "key {}",
        key
    );
    assert_eq!(entry.to_seqno(), ref_entry.to_seqno(), "key {}", key);
    assert_eq!(entry.is_deleted(), ref_entry.is_deleted(), "key {}", key);
    assert_eq!(
        entry.as_deltas().len(),
        ref_entry.as_deltas().len(),
        "key {}",
        key
    );

    //println!("versions {} {}", n_vers, refn_vers);
    let mut vers = entry.versions();
    let mut ref_vers = ref_entry.versions();
    loop {
        match (vers.next(), ref_vers.next()) {
            (Some(e), Some(re)) => {
                assert_eq!(e.to_native_value(), re.to_native_value(), "key {}", key);
                assert_eq!(e.to_seqno(), re.to_seqno(), "key {} ", key);
                assert_eq!(e.is_deleted(), re.is_deleted(), "key {}", key);
            }
            (None, None) => break,
            (Some(e), None) => panic!("invalid entry {} {}", e.to_key(), e.to_seqno()),
            (None, Some(re)) => panic!("invalid entry {} {}", re.to_key(), re.to_seqno()),
        }
    }
}
