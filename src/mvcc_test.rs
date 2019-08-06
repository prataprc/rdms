use std::ops::Bound;

use rand::prelude::random;

use crate::core::{Index, Reader, Writer};
use crate::error::Error;
use crate::mvcc::Mvcc;
use crate::type_empty::Empty;

include!("./ref_test.rs");

// TODO: repeatable randoms.

#[test]
fn test_id() {
    let mvcc: Mvcc<i32, Empty> = Mvcc::new("test-mvcc");
    assert_eq!(mvcc.to_name(), "test-mvcc".to_string());
}

#[test]
fn test_len() {
    let mvcc: Mvcc<i32, Empty> = Mvcc::new("test-mvcc");
    assert_eq!(mvcc.len(), 0);
}

#[test]
fn test_set() {
    let mut mvcc: Mvcc<i64, i64> = Mvcc::new("test-mvcc");
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
    let mut mvcc: Mvcc<i64, i64> = Mvcc::new_lsm("test-mvcc");
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
    assert!(mvcc.set_cas(10, 100, 0).err() == Some(Error::InvalidCAS));
    // error insert
    assert!(mvcc.set_cas(9, 400, 14).err() == Some(Error::InvalidCAS));

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
    let mut mvcc: Mvcc<i64, i64> = Mvcc::new("test-mvcc");
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
    let mut mvcc: Mvcc<i64, i64> = Mvcc::new("test-mvcc");
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
    let mut mvcc: Mvcc<i64, i64> = Mvcc::new("test-mvcc");
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

#[test]
fn test_crud() {
    let size = 1000;
    let mut mvcc: Mvcc<i64, i64> = Mvcc::new("test-mvcc");
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
    let (mut iter, mut iter_ref) = (mvcc.iter().unwrap(), refns.iter());
    loop {
        let item = iter.next().transpose().unwrap();
        if check_node(item, iter_ref.next().cloned()) == false {
            break;
        }
    }

    // ranges and reverses
    for _ in 0..10000 {
        let (low, high) = random_low_high(size);
        //println!("test loop {:?} {:?}", low, high);

        let mut iter = mvcc.range((low, high)).unwrap();
        let mut iter_ref = refns.range(low, high);
        loop {
            let item = iter.next().transpose().unwrap();
            if check_node(item, iter_ref.next().cloned()) == false {
                break;
            }
        }

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

#[test]
fn test_crud_lsm() {
    let size = 1000;
    let mut mvcc: Mvcc<i64, i64> = Mvcc::new_lsm("test-mvcc");
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
    let (mut iter, mut iter_ref) = (mvcc.iter().unwrap(), refns.iter());
    loop {
        let item = iter.next().transpose().unwrap();
        if check_node(item, iter_ref.next().cloned()) == false {
            break;
        }
    }

    // ranges and reverses
    for _ in 0..3000 {
        let (low, high) = random_low_high(size as usize);
        //println!("test loop {:?} {:?}", low, high);

        let mut iter = mvcc.range((low, high)).unwrap();
        let mut iter_ref = refns.range(low, high);
        loop {
            let item = iter.next().transpose().unwrap();
            if check_node(item, iter_ref.next().cloned()) == false {
                break;
            }
        }

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
