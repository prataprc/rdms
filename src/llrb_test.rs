use std::ops::Bound;

use rand::prelude::random;

use crate::error::Error;
use crate::llrb::Llrb;
use crate::type_empty::Empty;

include!("./ref_test.rs");

// TODO: repeatable randoms.

#[test]
fn test_id() {
    let llrb: Llrb<i32, Empty> = Llrb::new("test-llrb");
    assert_eq!(llrb.id(), "test-llrb".to_string());
}

#[test]
fn test_seqno() {
    let mut llrb: Llrb<i32, Empty> = Llrb::new("test-llrb");
    assert_eq!(llrb.get_seqno(), 0);
    llrb.set_seqno(1234);
    assert_eq!(llrb.get_seqno(), 1234);
}

#[test]
fn test_len() {
    let llrb: Llrb<i32, Empty> = Llrb::new("test-llrb");
    assert_eq!(llrb.len(), 0);
}

#[test]
fn test_set() {
    let mut llrb: Llrb<i64, i64> = Llrb::new("test-llrb");
    let mut refns = RefNodes::new(false /*lsm*/, 10);

    assert!(llrb.set(2, 10).is_none());
    refns.set(2, 10);
    assert!(llrb.set(1, 10).is_none());
    refns.set(1, 10);
    assert!(llrb.set(3, 10).is_none());
    refns.set(3, 10);
    assert!(llrb.set(6, 10).is_none());
    refns.set(6, 10);
    assert!(llrb.set(5, 10).is_none());
    refns.set(5, 10);
    assert!(llrb.set(4, 10).is_none());
    refns.set(4, 10);
    assert!(llrb.set(8, 10).is_none());
    refns.set(8, 10);
    assert!(llrb.set(0, 10).is_none());
    refns.set(0, 10);
    assert!(llrb.set(9, 10).is_none());
    refns.set(9, 10);
    assert!(llrb.set(7, 10).is_none());
    refns.set(7, 10);

    assert_eq!(llrb.len(), 10);
    assert!(llrb.validate().is_ok());

    // test get
    for i in 0..10 {
        let entry = llrb.get(&i);
        let refn = refns.get(i);
        check_node(entry, refn);
    }
    // test iter
    let (mut iter, mut iter_ref) = (llrb.iter(), refns.iter());
    loop {
        if check_node(iter.next(), iter_ref.next().cloned()) == false {
            break;
        }
    }
}

#[test]
fn test_cas_lsm() {
    let mut llrb: Llrb<i64, i64> = Llrb::new_lsm("test-llrb");
    let mut refns = RefNodes::new(true /*lsm*/, 11);

    assert!(llrb.set(2, 100).is_none());
    refns.set(2, 100);
    assert!(llrb.set(1, 100).is_none());
    refns.set(1, 100);
    assert!(llrb.set(3, 100).is_none());
    refns.set(3, 100);
    assert!(llrb.set(6, 100).is_none());
    refns.set(6, 100);
    assert!(llrb.set(5, 100).is_none());
    refns.set(5, 100);
    assert!(llrb.set(4, 100).is_none());
    refns.set(4, 100);
    assert!(llrb.set(8, 100).is_none());
    refns.set(8, 100);
    assert!(llrb.set(0, 100).is_none());
    refns.set(0, 100);
    assert!(llrb.set(9, 100).is_none());
    refns.set(9, 100);
    assert!(llrb.set(7, 100).is_none());
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

    // test get
    for i in 0..11 {
        let entry = llrb.get(&i);
        let refn = refns.get(i);
        check_node(entry, refn);
    }
    // test iter
    let (mut iter, mut iter_ref) = (llrb.iter(), refns.iter());
    loop {
        if check_node(iter.next(), iter_ref.next().cloned()) == false {
            break;
        }
    }
}

#[test]
fn test_delete() {
    let mut llrb: Llrb<i64, i64> = Llrb::new("test-llrb");
    let mut refns = RefNodes::new(false /*lsm*/, 11);

    assert!(llrb.set(2, 100).is_none());
    refns.set(2, 100);
    assert!(llrb.set(1, 100).is_none());
    refns.set(1, 100);
    assert!(llrb.set(3, 100).is_none());
    refns.set(3, 100);
    assert!(llrb.set(6, 100).is_none());
    refns.set(6, 100);
    assert!(llrb.set(5, 100).is_none());
    refns.set(5, 100);
    assert!(llrb.set(4, 100).is_none());
    refns.set(4, 100);
    assert!(llrb.set(8, 100).is_none());
    refns.set(8, 100);
    assert!(llrb.set(0, 100).is_none());
    refns.set(0, 100);
    assert!(llrb.set(9, 100).is_none());
    refns.set(9, 100);
    assert!(llrb.set(7, 100).is_none());
    refns.set(7, 100);

    // delete a missing node.
    assert!(llrb.delete(&10).is_none());
    assert!(refns.delete(10).is_none());

    assert_eq!(llrb.len(), 10);
    assert!(llrb.validate().is_ok());

    // test iter
    {
        let (mut iter, mut iter_ref) = (llrb.iter(), refns.iter());
        loop {
            if check_node(iter.next(), iter_ref.next().cloned()) == false {
                break;
            }
        }
    }

    // delete all entry. and set new entries
    for i in 0..10 {
        let entry = llrb.delete(&i);
        let refn = refns.delete(i);
        check_node(entry, refn);
    }
    assert_eq!(llrb.len(), 0);
    assert!(llrb.validate().is_ok());
    // test iter
    assert!(llrb.iter().next().is_none());
}

#[test]
fn test_iter() {
    let mut llrb: Llrb<i64, i64> = Llrb::new("test-llrb");
    let mut refns = RefNodes::new(false /*lsm*/, 10);

    assert!(llrb.set(2, 10).is_none());
    refns.set(2, 10);
    assert!(llrb.set(1, 10).is_none());
    refns.set(1, 10);
    assert!(llrb.set(3, 10).is_none());
    refns.set(3, 10);
    assert!(llrb.set(6, 10).is_none());
    refns.set(6, 10);
    assert!(llrb.set(5, 10).is_none());
    refns.set(5, 10);
    assert!(llrb.set(4, 10).is_none());
    refns.set(4, 10);
    assert!(llrb.set(8, 10).is_none());
    refns.set(8, 10);
    assert!(llrb.set(0, 10).is_none());
    refns.set(0, 10);
    assert!(llrb.set(9, 10).is_none());
    refns.set(9, 10);
    assert!(llrb.set(7, 10).is_none());
    refns.set(7, 10);

    assert_eq!(llrb.len(), 10);
    assert!(llrb.validate().is_ok());

    // test iter
    let (mut iter, mut iter_ref) = (llrb.iter(), refns.iter());
    loop {
        match (iter.next(), iter_ref.next()) {
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
    let mut llrb: Llrb<i64, i64> = Llrb::new("test-llrb");
    let mut refns = RefNodes::new(false /*lsm*/, 10);

    assert!(llrb.set(2, 10).is_none());
    refns.set(2, 10);
    assert!(llrb.set(1, 10).is_none());
    refns.set(1, 10);
    assert!(llrb.set(3, 10).is_none());
    refns.set(3, 10);
    assert!(llrb.set(6, 10).is_none());
    refns.set(6, 10);
    assert!(llrb.set(5, 10).is_none());
    refns.set(5, 10);
    assert!(llrb.set(4, 10).is_none());
    refns.set(4, 10);
    assert!(llrb.set(8, 10).is_none());
    refns.set(8, 10);
    assert!(llrb.set(0, 10).is_none());
    refns.set(0, 10);
    assert!(llrb.set(9, 10).is_none());
    refns.set(9, 10);
    assert!(llrb.set(7, 10).is_none());
    refns.set(7, 10);

    assert_eq!(llrb.len(), 10);
    assert!(llrb.validate().is_ok());

    // test range
    for _ in 0..1_000 {
        let (low, high) = random_low_high(llrb.len());

        let mut iter = llrb.range((low, high));
        let mut iter_ref = refns.range(low, high);
        loop {
            match (iter.next(), iter_ref.next()) {
                (None, None) => break,
                (entry, Some(refn)) => check_node(entry, Some(refn.clone())),
                _ => panic!("invalid"),
            };
        }
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());

        let mut iter = llrb.reverse((low, high));
        let mut iter_ref = refns.reverse(low, high);
        loop {
            match (iter.next(), iter_ref.next()) {
                (None, None) => break,
                (entry, Some(refn)) => check_node(entry, Some(refn.clone())),
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
    let mut llrb: Llrb<i64, i64> = Llrb::new("test-llrb");
    let mut refns = RefNodes::new(false /*lsm*/, size);

    for _ in 0..100000 {
        let key: i64 = (random::<i64>() % (size as i64)).abs();
        let value: i64 = random();
        let op: i64 = (random::<i64>() % 3).abs();
        //println!("key {} value {} op {}", key, value, op);
        match op {
            0 => {
                let entry = llrb.set(key, value);
                let refn = refns.set(key, value);
                check_node(entry, refn);
                false
            }
            1 => {
                let off: usize = key.try_into().unwrap();
                let refn = &refns.entries[off];
                let cas = if refn.versions.len() > 0 {
                    refn.get_seqno()
                } else {
                    0
                };

                let entry = llrb.set_cas(key, value, cas).ok().unwrap();
                let refn = refns.set_cas(key, value, cas);
                check_node(entry, refn);
                false
            }
            2 => {
                let entry = llrb.delete(&key);
                let refn = refns.delete(key);
                check_node(entry, refn);
                true
            }
            op => panic!("unreachable {}", op),
        };

        assert!(llrb.validate().is_ok());
    }

    //println!("len {}", llrb.len());

    // test iter
    let (mut iter, mut iter_ref) = (llrb.iter(), refns.iter());
    loop {
        if check_node(iter.next(), iter_ref.next().cloned()) == false {
            break;
        }
    }

    // ranges and reverses
    for _ in 0..10000 {
        let (low, high) = random_low_high(size);
        //println!("test loop {:?} {:?}", low, high);

        let mut iter = llrb.range((low, high));
        let mut iter_ref = refns.range(low, high);
        loop {
            if check_node(iter.next(), iter_ref.next().cloned()) == false {
                break;
            }
        }

        let mut iter = llrb.reverse((low, high));
        let mut iter_ref = refns.reverse(low, high);
        loop {
            if check_node(iter.next(), iter_ref.next().cloned()) == false {
                break;
            }
        }
    }
}

#[test]
fn test_crud_lsm() {
    let size = 1000;
    let mut llrb: Llrb<i64, i64> = Llrb::new_lsm("test-llrb");
    let mut refns = RefNodes::new(true /*lsm*/, size as usize);

    for _i in 0..20000 {
        let key: i64 = (random::<i64>() % size).abs();
        let value: i64 = random();
        let op: i64 = (random::<i64>() % 3).abs();
        println!("op {} on {}", op, key);
        match op {
            0 => {
                let entry = llrb.set(key, value);
                let refn = refns.set(key, value);
                check_node(entry, refn);
                false
            }
            1 => {
                let off: usize = key.try_into().unwrap();
                let refn = &refns.entries[off];
                let cas = if refn.versions.len() > 0 {
                    refn.get_seqno()
                } else {
                    0
                };

                //println!("set_cas {} {}", key, seqno);
                let entry = llrb.set_cas(key, value, cas).ok().unwrap();
                let refn = refns.set_cas(key, value, cas);
                check_node(entry, refn);
                false
            }
            2 => {
                let entry = llrb.delete(&key);
                let refn = refns.delete(key);
                check_node(entry, refn);
                true
            }
            op => panic!("unreachable {}", op),
        };

        assert!(llrb.validate().is_ok());
    }

    //println!("len {}", llrb.len());

    // test iter
    let (mut iter, mut iter_ref) = (llrb.iter(), refns.iter());
    loop {
        if check_node(iter.next(), iter_ref.next().cloned()) == false {
            break;
        }
    }

    // ranges and reverses
    for _ in 0..3000 {
        let (low, high) = random_low_high(size as usize);
        //println!("test loop {:?} {:?}", low, high);

        let mut iter = llrb.range((low, high));
        let mut iter_ref = refns.range(low, high);
        loop {
            if check_node(iter.next(), iter_ref.next().cloned()) == false {
                break;
            }
        }

        let mut iter = llrb.reverse((low, high));
        let mut iter_ref = refns.reverse(low, high);
        loop {
            if check_node(iter.next(), iter_ref.next().cloned()) == false {
                break;
            }
        }
    }
}
