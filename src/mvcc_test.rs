use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use std::ops::Bound;

use crate::{
    core::{CommitIterator, Index, Reader, Result, Validate, Writer},
    error::Error,
    mvcc::Mvcc,
    scans::SkipScan,
    types::Empty,
};

include!("./ref_test.rs");

// TODO: repeatable randoms.

#[test]
fn test_id() {
    let mut mvcc: Box<Mvcc<i32, Empty>> = Mvcc::new("test-mvcc");
    assert_eq!(mvcc.to_name(), "test-mvcc".to_string());
    assert!(mvcc.validate().is_ok());
}

#[test]
fn test_len() {
    let mut mvcc: Box<Mvcc<i32, Empty>> = Mvcc::new("test-mvcc");
    assert_eq!(mvcc.len(), 0);
    assert!(mvcc.validate().is_ok());
}

#[test]
fn test_lsm_sticky() {
    let missing_key = 0x123456789;
    let populate = |index: &mut Mvcc<i64, i64>| -> i64 {
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
    let mut index: Box<Mvcc<i64, i64>> = Mvcc::new("test-mvcc");
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
    let mut index: Box<Mvcc<i64, i64>> = Mvcc::new("test-mvcc");
    index.set_sticky(true);
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
    let mut index: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc");
    index.set_sticky(true);
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
            let values: Vec<i64> = es.iter().filter_map(|e| e.to_native_value()).collect();

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
    let populate = |index: &mut Mvcc<i64, i64>| {
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
        index.delete(&missing_key);
    };

    // without lsm
    let mut index: Box<Mvcc<i64, i64>> = Mvcc::new("test-mvcc");
    populate(&mut index);
    assert_eq!(index.to_stats().n_deleted, 0);

    // validate will make sure the that n_deleted count is correct.
    assert!(index.validate().is_ok());
}

#[test]
fn test_set() {
    let mut mvcc: Box<Mvcc<i64, i64>> = Mvcc::new("test-mvcc");
    let mut refns = RefNodes::new(false /*lsm*/, 10);

    assert!(mvcc.set(2, 10).unwrap().is_none());
    refns.set(2, 10);
    assert!(mvcc.set(1, 10).unwrap().is_none());
    refns.set(1, 10);
    assert!(mvcc.set(3, 10).unwrap().is_none());
    refns.set(3, 10);
    assert!(mvcc.set(6, 10).unwrap().is_none());
    refns.set(6, 10);
    assert!(mvcc.set(5, 10).unwrap().is_none());
    refns.set(5, 10);
    assert!(mvcc.set(4, 10).unwrap().is_none());
    refns.set(4, 10);
    assert!(mvcc.set(8, 10).unwrap().is_none());
    refns.set(8, 10);
    assert!(mvcc.set(0, 10).unwrap().is_none());
    refns.set(0, 10);
    assert!(mvcc.set(9, 10).unwrap().is_none());
    refns.set(9, 10);
    assert!(mvcc.set(7, 10).unwrap().is_none());
    refns.set(7, 10);

    assert_eq!(mvcc.len(), 10);
    assert!(mvcc.validate().is_ok());

    assert_eq!(refns.to_seqno(), mvcc.to_seqno());
    // test get
    for i in 0..10 {
        let entry = mvcc.get(&i);
        let refn = refns.get(i);
        check_node(entry.ok(), refn);
    }
    // test iter
    let (mut iter, mut iter_ref) = (mvcc.iter().unwrap(), refns.iter());
    loop {
        let item = iter.next().transpose().unwrap();
        if check_node(item, iter_ref.next().cloned()) == false {
            break;
        }
    }
}

#[test]
fn test_cas_lsm() {
    let mut mvcc: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc");
    let mut refns = RefNodes::new(true /*lsm*/, 11);

    assert!(mvcc.set(2, 100).unwrap().is_none());
    refns.set(2, 100);
    assert!(mvcc.set(1, 100).unwrap().is_none());
    refns.set(1, 100);
    assert!(mvcc.set(3, 100).unwrap().is_none());
    refns.set(3, 100);
    assert!(mvcc.set(6, 100).unwrap().is_none());
    refns.set(6, 100);
    assert!(mvcc.set(5, 100).unwrap().is_none());
    refns.set(5, 100);
    assert!(mvcc.set(4, 100).unwrap().is_none());
    refns.set(4, 100);
    assert!(mvcc.set(8, 100).unwrap().is_none());
    refns.set(8, 100);
    assert!(mvcc.set(0, 100).unwrap().is_none());
    refns.set(0, 100);
    assert!(mvcc.set(9, 100).unwrap().is_none());
    refns.set(9, 100);
    assert!(mvcc.set(7, 100).unwrap().is_none());
    refns.set(7, 100);

    // repeated mutations on same key

    let node = mvcc.set_cas(0, 200, 8).ok().unwrap();
    let refn = refns.set_cas(0, 200, 8);
    check_node(node, refn);

    let node = mvcc.set_cas(5, 200, 5).ok().unwrap();
    let refn = refns.set_cas(5, 200, 5);
    check_node(node, refn);

    let node = mvcc.set_cas(6, 200, 4).ok().unwrap();
    let refn = refns.set_cas(6, 200, 4);
    check_node(node, refn);

    let node = mvcc.set_cas(9, 200, 9).ok().unwrap();
    let refn = refns.set_cas(9, 200, 9);
    check_node(node, refn);

    let node = mvcc.set_cas(0, 300, 11).ok().unwrap();
    let refn = refns.set_cas(0, 300, 11);
    check_node(node, refn);

    let node = mvcc.set_cas(5, 300, 12).ok().unwrap();
    let refn = refns.set_cas(5, 300, 12);
    check_node(node, refn);

    let node = mvcc.set_cas(9, 300, 14).ok().unwrap();
    let refn = refns.set_cas(9, 300, 14);
    check_node(node, refn);

    // create
    assert!(mvcc.set_cas(10, 100, 0).ok().unwrap().is_none());
    assert!(refns.set_cas(10, 100, 0).is_none());
    // error create
    assert_eq!(mvcc.set_cas(10, 100, 0).err(), Some(Error::InvalidCAS(18)));
    // error insert
    assert_eq!(mvcc.set_cas(9, 400, 14).err(), Some(Error::InvalidCAS(17)));

    assert_eq!(mvcc.len(), 11);
    assert!(mvcc.validate().is_ok());

    assert_eq!(refns.to_seqno(), mvcc.to_seqno());
    // test get
    for i in 0..11 {
        let entry = mvcc.get(&i);
        let refn = refns.get(i);
        check_node(entry.ok(), refn);
    }
    // test iter
    let (mut iter, mut iter_ref) = (mvcc.iter().unwrap(), refns.iter());
    loop {
        let item = iter.next().transpose().unwrap();
        if check_node(item, iter_ref.next().cloned()) == false {
            break;
        }
    }
}

#[test]
fn test_delete() {
    let mut mvcc: Box<Mvcc<i64, i64>> = Mvcc::new("test-mvcc");
    let mut refns = RefNodes::new(false /*lsm*/, 11);

    assert!(mvcc.set(2, 100).unwrap().is_none());
    refns.set(2, 100);
    assert!(mvcc.set(1, 100).unwrap().is_none());
    refns.set(1, 100);
    assert!(mvcc.set(3, 100).unwrap().is_none());
    refns.set(3, 100);
    assert!(mvcc.set(6, 100).unwrap().is_none());
    refns.set(6, 100);
    assert!(mvcc.set(5, 100).unwrap().is_none());
    refns.set(5, 100);
    assert!(mvcc.set(4, 100).unwrap().is_none());
    refns.set(4, 100);
    assert!(mvcc.set(8, 100).unwrap().is_none());
    refns.set(8, 100);
    assert!(mvcc.set(0, 100).unwrap().is_none());
    refns.set(0, 100);
    assert!(mvcc.set(9, 100).unwrap().is_none());
    refns.set(9, 100);
    assert!(mvcc.set(7, 100).unwrap().is_none());
    refns.set(7, 100);

    // delete a missing node.
    assert!(mvcc.delete(&10).unwrap().is_none());
    assert!(refns.delete(10).is_none());

    assert_eq!(mvcc.len(), 10);
    assert!(mvcc.validate().is_ok());

    assert_eq!(refns.to_seqno(), mvcc.to_seqno());
    // test iter
    {
        let (mut iter, mut iter_ref) = (mvcc.iter().unwrap(), refns.iter());
        loop {
            let item = iter.next().transpose().unwrap();
            if check_node(item, iter_ref.next().cloned()) == false {
                break;
            }
        }
    }

    // delete all entry. and set new entries
    for i in 0..10 {
        let node = mvcc.delete(&i).unwrap();
        let refn = refns.delete(i);
        check_node(node, refn);
    }
    assert_eq!(refns.to_seqno(), mvcc.to_seqno());
    assert_eq!(mvcc.len(), 0);
    assert!(mvcc.validate().is_ok());
    // test iter
    assert!(mvcc.iter().unwrap().next().is_none());
}

#[test]
fn test_iter() {
    let mut mvcc: Box<Mvcc<i64, i64>> = Mvcc::new("test-mvcc");
    let mut refns = RefNodes::new(false /*lsm*/, 10);

    assert!(mvcc.set(2, 10).unwrap().is_none());
    refns.set(2, 10);
    assert!(mvcc.set(1, 10).unwrap().is_none());
    refns.set(1, 10);
    assert!(mvcc.set(3, 10).unwrap().is_none());
    refns.set(3, 10);
    assert!(mvcc.set(6, 10).unwrap().is_none());
    refns.set(6, 10);
    assert!(mvcc.set(5, 10).unwrap().is_none());
    refns.set(5, 10);
    assert!(mvcc.set(4, 10).unwrap().is_none());
    refns.set(4, 10);
    assert!(mvcc.set(8, 10).unwrap().is_none());
    refns.set(8, 10);
    assert!(mvcc.set(0, 10).unwrap().is_none());
    refns.set(0, 10);
    assert!(mvcc.set(9, 10).unwrap().is_none());
    refns.set(9, 10);
    assert!(mvcc.set(7, 10).unwrap().is_none());
    refns.set(7, 10);

    assert_eq!(mvcc.len(), 10);
    assert!(mvcc.validate().is_ok());

    assert_eq!(refns.to_seqno(), mvcc.to_seqno());
    // test iter
    let (mut iter, mut iter_ref) = (mvcc.iter().unwrap(), refns.iter());
    loop {
        let item = iter.next().transpose().unwrap();
        match (item, iter_ref.next()) {
            (None, None) => break,
            (node, Some(refn)) => check_node(node, Some(refn.clone())),
            _ => panic!("invalid"),
        };
    }
    assert!(iter.next().is_none());
    assert!(iter.next().is_none());
}

#[test]
fn test_range() {
    let mut mvcc: Box<Mvcc<i64, i64>> = Mvcc::new("test-mvcc");
    let mut refns = RefNodes::new(false /*lsm*/, 10);

    assert!(mvcc.set(2, 10).unwrap().is_none());
    refns.set(2, 10);
    assert!(mvcc.set(1, 10).unwrap().is_none());
    refns.set(1, 10);
    assert!(mvcc.set(3, 10).unwrap().is_none());
    refns.set(3, 10);
    assert!(mvcc.set(6, 10).unwrap().is_none());
    refns.set(6, 10);
    assert!(mvcc.set(5, 10).unwrap().is_none());
    refns.set(5, 10);
    assert!(mvcc.set(4, 10).unwrap().is_none());
    refns.set(4, 10);
    assert!(mvcc.set(8, 10).unwrap().is_none());
    refns.set(8, 10);
    assert!(mvcc.set(0, 10).unwrap().is_none());
    refns.set(0, 10);
    assert!(mvcc.set(9, 10).unwrap().is_none());
    refns.set(9, 10);
    assert!(mvcc.set(7, 10).unwrap().is_none());
    refns.set(7, 10);

    assert_eq!(mvcc.len(), 10);
    assert!(mvcc.validate().is_ok());

    assert_eq!(refns.to_seqno(), mvcc.to_seqno());
    // test range
    for _ in 0..1_000 {
        let (low, high) = random_low_high(mvcc.len());

        {
            let mut iter = mvcc.range((low, high)).unwrap();
            let mut iter_ref = refns.range(low, high);
            loop {
                let item = iter.next().transpose().unwrap();
                match (item, iter_ref.next()) {
                    (None, None) => break,
                    (node, Some(refn)) => check_node(node, Some(refn.clone())),
                    _ => panic!("invalid"),
                };
            }
            assert!(iter.next().is_none());
            assert!(iter.next().is_none());
        }

        {
            //println!("{:?} {:?}", low, high);
            let mut iter = mvcc.reverse((low, high)).unwrap();
            let mut iter_ref = refns.reverse(low, high);
            loop {
                let item = iter.next().transpose().unwrap();
                match (item, iter_ref.next()) {
                    (None, None) => break,
                    (node, Some(refn)) => check_node(node, Some(refn.clone())),
                    _ => panic!("invalid"),
                };
            }
            assert!(iter.next().is_none());
            assert!(iter.next().is_none());
        }
    }
}

#[test]
fn test_crud() {
    let size = 1000;
    let mut mvcc: Box<Mvcc<i64, i64>> = Mvcc::new("test-mvcc");
    let mut refns = RefNodes::new(false /*lsm*/, size);

    for _ in 0..100000 {
        let key: i64 = (random::<i64>() % (size as i64)).abs();
        let value: i64 = random();
        let op: i64 = (random::<i64>() % 3).abs();
        //println!("key {} value {} op {}", key, value, op);
        match op {
            0 => {
                let node = mvcc.set(key, value).unwrap();
                let refn = refns.set(key, value);
                check_node(node, refn);
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

                let node = mvcc.set_cas(key, value, cas).ok().unwrap();
                let refn = refns.set_cas(key, value, cas);
                check_node(node, refn);
                false
            }
            2 => {
                let node = mvcc.delete(&key).unwrap();
                let refn = refns.delete(key);
                check_node(node, refn);
                true
            }
            op => panic!("unreachable {}", op),
        };

        assert!(mvcc.validate().is_ok(), "validate failed");
    }

    //println!("len {}", mvcc.len());

    assert_eq!(refns.to_seqno(), mvcc.to_seqno());
    // test iter
    {
        let (mut iter, mut iter_ref) = (mvcc.iter().unwrap(), refns.iter());
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
            let mut iter = mvcc.range((low, high)).unwrap();
            let mut iter_ref = refns.range(low, high);
            loop {
                let item = iter.next().transpose().unwrap();
                if check_node(item, iter_ref.next().cloned()) == false {
                    break;
                }
            }
        }

        {
            let mut iter = mvcc.reverse((low, high)).unwrap();
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
    let mut mvcc: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc");
    let mut refns = RefNodes::new(true /*lsm*/, size as usize);

    for _i in 0..20000 {
        let key: i64 = (random::<i64>() % size).abs();
        let value: i64 = random();
        let op: i64 = (random::<i64>() % 3).abs();
        //println!("op {} on {}", op, key);
        match op {
            0 => {
                let node = mvcc.set(key, value).unwrap();
                let refn = refns.set(key, value);
                check_node(node, refn);
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

                //println!("set_cas {} {}", key, seqno);
                let node = mvcc.set_cas(key, value, cas).ok().unwrap();
                let refn = refns.set_cas(key, value, cas);
                check_node(node, refn);
                false
            }
            2 => {
                let node = mvcc.delete(&key).unwrap();
                let refn = refns.delete(key);
                check_node(node, refn);
                true
            }
            op => panic!("unreachable {}", op),
        };

        assert!(mvcc.validate().is_ok(), "validate failed");
    }

    //println!("len {}", mvcc.len());

    assert_eq!(refns.to_seqno(), mvcc.to_seqno());
    // test iter
    {
        let (mut iter, mut iter_ref) = (mvcc.iter().unwrap(), refns.iter());
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
        //println!("test loop {:?} {:?}", low, high);
        {
            let mut iter = mvcc.range((low, high)).unwrap();
            let mut iter_ref = refns.range(low, high);
            loop {
                let item = iter.next().transpose().unwrap();
                if check_node(item, iter_ref.next().cloned()) == false {
                    break;
                }
            }
        }

        {
            let mut iter = mvcc.reverse((low, high)).unwrap();
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
    let mut index: Box<Mvcc<i32, i32>> = Mvcc::new_lsm("test-mvcc");

    // populate
    for key in 0..10000 {
        let value = (key + 1) * 100;
        assert!(index.set(key, value).unwrap().is_none());
    }

    assert_eq!(index.len(), 10000);
    assert_eq!(index.to_seqno(), 10000);
    let seqno1 = index.to_seqno();

    let mut iter = SkipScan::new(index.to_reader().unwrap());
    iter.set_seqno_range(..=seqno1);
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
        assert!(index.set(key, value).unwrap().is_some());
    }
    assert_eq!(index.len(), 10000);
    assert_eq!(index.to_seqno(), 10334);

    // skip scan after first-inject.
    let mut iter = SkipScan::new(index.to_reader().unwrap());
    iter.set_seqno_range(..=seqno1);
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
        assert!(index.set(key, value).unwrap().is_some());
    }
    for key in (0..1000).step_by(5) {
        let value = (key + 1) * 1000;
        assert!(index.set(key, value).unwrap().is_some());
    }

    assert_eq!(index.len(), 10000);
    assert_eq!(index.to_seqno(), 10868);

    let seqno2 = index.to_seqno();

    // third-inject.
    for key in (0..1000).step_by(15) {
        let value = (key + 1) * 100000;
        assert!(index.set(key, value).unwrap().is_some());
    }
    assert_eq!(index.len(), 10000);
    assert_eq!(index.to_seqno(), 10935);

    // skip scan in-between.
    let mut iter = SkipScan::new(index.to_reader().unwrap());
    iter.set_seqno_range((Bound::Excluded(seqno1), Bound::Included(seqno2)));
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
    let mut iter = SkipScan::new(index.to_reader().unwrap());
    iter.set_seqno_range((Bound::Excluded(seqno2), Bound::Unbounded));
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

    assert!(index.validate().is_ok());
}

#[test]
fn test_commit_iterator_scan() {
    let seed: u128 = random();
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let (n_ops, key_max) = (60_000_i64, 20_000);
    let mut mvcc: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc");
    random_mvcc(n_ops, key_max, seed, &mut mvcc);

    for i in 0..20 {
        let from_seqno = match rng.gen::<i64>() % n_ops {
            n if n >= 0 && n % 2 == 0 => Bound::Included(n as u64),
            n if n >= 0 => Bound::Excluded(n as u64),
            _ => Bound::Unbounded,
        };
        let mut iter = (&mut *mvcc).scan(from_seqno).unwrap();
        let mut r = mvcc.to_reader().unwrap();
        let mut ref_iter = r.iter().unwrap();
        let within = (from_seqno, Bound::Included(mvcc.to_seqno()));
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
fn test_commit_iterator_scans() {
    type MyIter = Box<dyn Iterator<Item = Result<Entry<i64, i64>>>>;

    let seed: u128 = random();
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let (n_ops, key_max) = (60_000_i64, 20_000);
    let mut mvcc: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc");
    random_mvcc(n_ops, key_max, seed, &mut mvcc);

    for i in 0..20 {
        let shards = rng.gen::<usize>() % 31 + 1;
        let from_seqno = match rng.gen::<i64>() % n_ops {
            n if n >= 0 && n % 2 == 0 => Bound::Included(n as u64),
            n if n >= 0 => Bound::Excluded(n as u64),
            _ => Bound::Unbounded,
        };
        let mut iters = (&mut *mvcc).scans(shards, from_seqno).unwrap();
        let iter: MyIter = Box::new(iters.remove(0));
        let mut iter = iters.drain(..).into_iter().fold(iter, |acc, iter| {
            Box::new(acc.chain(Box::new(iter) as MyIter)) as MyIter
        });

        let mut r = mvcc.to_reader().unwrap();
        let mut ref_iter = r.iter().unwrap();
        let within = (from_seqno, Bound::Included(mvcc.to_seqno()));
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

    type MyIter = Box<dyn Iterator<Item = Result<Entry<i64, i64>>>>;

    let seed: u128 = random();
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let (n_ops, key_max) = (128_000_i64, 20_000);
    let mut mvcc: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc");
    random_mvcc(n_ops, key_max, seed, &mut mvcc);

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
        let mut iters = (&mut *mvcc).range_scans(ranges, from_seqno).unwrap();
        let iter: MyIter = Box::new(iters.remove(0));
        let mut iter = iters.drain(..).into_iter().fold(iter, |acc, iter| {
            Box::new(acc.chain(Box::new(iter) as MyIter)) as MyIter
        });

        let mut r = mvcc.to_reader().unwrap();
        let mut ref_iter = r.iter().unwrap();
        let within = (from_seqno, Bound::Included(mvcc.to_seqno()));
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

fn random_mvcc(n_ops: i64, key_max: i64, seed: u128, mvcc: &mut Mvcc<i64, i64>) {
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
    for _i in 0..n_ops {
        let key = (rng.gen::<i64>() % key_max).abs();
        let op = rng.gen::<usize>() % 3;
        //println!("key {} {} {} {}", key, mvcc.to_seqno(), op);
        match op {
            0 => {
                let value: i64 = rng.gen();
                mvcc.set(key, value).unwrap();
            }
            1 => {
                let value: i64 = rng.gen();
                {
                    let cas = match mvcc.get(&key) {
                        Err(Error::KeyNotFound) => 0,
                        Err(_err) => unreachable!(),
                        Ok(e) => e.to_seqno(),
                    };
                    mvcc.set_cas(key, value, cas).unwrap();
                }
            }
            2 => {
                mvcc.delete(&key).unwrap();
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
