// TODO: write test case for iter_within for Llrb and Mvcc index.

use rand::prelude::random;

use std::ops::Bound;

use super::*;
use crate::{
    core::{Index, Reader, Writer},
    error::Error,
    llrb::Llrb,
    scans::SkipScan,
    types::Empty,
};

include!("./ref_test.rs");

// TODO: repeatable randoms.

#[test]
fn test_node_size() {
    assert_eq!(std::mem::size_of::<Node<i64, i64>>(), 80);
}

#[test]
fn test_id() {
    let llrb: Box<Llrb<i32, Empty>> = Llrb::new("test-llrb");
    assert_eq!(llrb.to_name(), "test-llrb".to_string());
    assert!(llrb.validate().is_ok());
}

#[test]
fn test_len() {
    let llrb: Box<Llrb<i32, Empty>> = Llrb::new("test-llrb");
    assert_eq!(llrb.len(), 0);
    assert!(llrb.validate().is_ok());
}

#[test]
fn test_lsm_sticky() {
    // without lsm
    let llrb: Box<Llrb<i64, i64>> = Llrb::new("test-llrb");
    for _ in 0..500 {
        let key: i64 = random::<i64>().abs();
        let value: i64 = random();
        llrb.set(key, value);
    }
    let key = {
        let mut iter = llrb.iter().unwrap();
        iter.skip_till(random::<u8>() as usize).next().unwrap().to_key()
    }
    llrb.set(key.clone(), 9);
    llrb.delete(&key);
    llrb.set(key.clone(), 10);
    llrb.delete(&key);
    let entry = llrb.get(&key).unwrap();
    let vers = entry.versions();
    let e = vers.next().unwrap()
    assert!(e.is_deleted());
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

    assert_eq!(refns.to_seqno(), llrb.to_seqno());
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

    let entry = llrb.set_cas(0, 200, 8).ok().unwrap();
    let refn = refns.set_cas(0, 200, 8);
    check_node(entry, refn);

    let entry = llrb.set_cas(5, 200, 5).ok().unwrap();
    let refn = refns.set_cas(5, 200, 5);
    check_node(entry, refn);

    let entry = llrb.set_cas(6, 200, 4).ok().unwrap();
    let refn = refns.set_cas(6, 200, 4);
    check_node(entry, refn);

    let entry = llrb.set_cas(9, 200, 9).ok().unwrap();
    let refn = refns.set_cas(9, 200, 9);
    check_node(entry, refn);

    let entry = llrb.set_cas(0, 300, 11).ok().unwrap();
    let refn = refns.set_cas(0, 300, 11);
    check_node(entry, refn);

    let entry = llrb.set_cas(5, 300, 12).ok().unwrap();
    let refn = refns.set_cas(5, 300, 12);
    check_node(entry, refn);

    let entry = llrb.set_cas(9, 300, 14).ok().unwrap();
    let refn = refns.set_cas(9, 300, 14);
    check_node(entry, refn);

    // create
    assert!(llrb.set_cas(10, 100, 0).ok().unwrap().is_none());
    assert!(refns.set_cas(10, 100, 0).is_none());
    // error create
    assert!(llrb.set_cas(10, 100, 0).err() == Some(Error::InvalidCAS));
    // error insert
    assert!(llrb.set_cas(9, 400, 14).err() == Some(Error::InvalidCAS));

    assert_eq!(llrb.len(), 11);
    assert!(llrb.validate().is_ok());

    assert_eq!(refns.to_seqno(), llrb.to_seqno());
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

    assert_eq!(refns.to_seqno(), llrb.to_seqno());
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
    assert_eq!(refns.to_seqno(), llrb.to_seqno());
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

    assert_eq!(refns.to_seqno(), llrb.to_seqno());
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

    assert_eq!(refns.to_seqno(), llrb.to_seqno());
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

//TODO: enable this test case once type_str is implemented.
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
    assert_eq!(refns.to_seqno(), llrb.to_seqno());

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
        assert_eq!(llrb.to_seqno(), refns.to_seqno());

        assert!(llrb.validate().is_ok());
    }

    // println!("len {}", llrb.len());
    assert_eq!(refns.to_seqno(), llrb.to_seqno());

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
    assert_eq!(llrb.to_seqno(), 10000);
    let seqno1 = llrb.to_seqno();

    let iter = SkipScan::new(llrb.to_reader().unwrap(), ..=seqno1);
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
    assert_eq!(llrb.to_seqno(), 10334);

    // skip scan after first-inject.
    let iter = SkipScan::new(llrb.to_reader().unwrap(), ..=seqno1);
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
    assert_eq!(llrb.to_seqno(), 10868);

    let seqno2 = llrb.to_seqno();

    // third-inject.
    for key in (0..1000).step_by(15) {
        let value = (key + 1) * 100000;
        assert!(llrb.set(key, value).unwrap().is_some());
    }
    assert_eq!(llrb.len(), 10000);
    assert_eq!(llrb.to_seqno(), 10935);

    // skip scan in-between.
    let r = (Bound::Excluded(seqno1), Bound::Included(seqno2));
    let iter = SkipScan::new(llrb.to_reader().unwrap(), r);
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
    let r = (Bound::Excluded(seqno2), Bound::Unbounded);
    let iter = SkipScan::new(llrb.to_reader().unwrap(), r);
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
