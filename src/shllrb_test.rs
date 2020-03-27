use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use std::{mem, ops::Bound};

use super::*;
use crate::{core::Reader, llrb_node::Node, scans, types::Empty, util};

include!("./ref_test.rs");

#[test]
fn test_name() {
    for shard in 0..999 {
        let name: ShardName = ("test-shllrb".to_string(), shard).into();
        assert_eq!(
            format!("{}", name),
            format!("test-shllrb-shard-{:03}", shard)
        );
        assert_eq!(
            format!("{:?}", name),
            format!("{}", format!("test-shllrb-shard-{:03}", shard))
        );
        let (name, shard): (String, usize) = name.try_into().unwrap();
        assert_eq!(name, "test-shllrb".to_string());
        assert_eq!(shard, shard);
    }
}

#[test]
fn test_len() {
    let config: Config = Default::default();
    let mut index: Box<ShLlrb<i32, Empty>> = ShLlrb::new("test-shllrb", config);
    assert_eq!(index.len().unwrap(), 0);
    assert!(index.validate().is_ok());
}

#[test]
fn test_lsm_sticky() {
    let missing_key = 0x123456789;
    let populate = |index: &mut ShLlrb<i64, i64>| -> i64 {
        let mut w = index.to_writer().unwrap();
        let mut r = index.to_reader().unwrap();

        for _ in 0..500 {
            let key: i64 = random::<i64>().abs();
            let value: i64 = random();
            w.set(key, value).unwrap();
        }
        let iter = r.iter().unwrap();
        let e = iter.skip(random::<u8>() as usize).next().unwrap();
        let key = e.unwrap().to_key();
        w.set(key.clone(), 9).unwrap();
        w.delete(&key).unwrap();
        w.set(key.clone(), 10).unwrap();
        w.delete(&key).unwrap();
        w.delete(&missing_key).ok();
        key
    };

    // without lsm
    let config: Config = Default::default();
    let mut index: Box<ShLlrb<i64, i64>> = ShLlrb::new("test-shllrb", config);
    let mut r = index.to_reader().unwrap();
    let key = populate(&mut index);
    match r.get(&key) {
        Err(Error::KeyNotFound) => (),
        Err(err) => panic!("unexpected {:?}", err),
        Ok(e) => panic!("unexpected {}", e.to_seqno()),
    };
    match r.get(&missing_key) {
        Err(Error::KeyNotFound) => (),
        Err(err) => panic!("unexpected {:?}", err),
        Ok(e) => panic!("unexpected {}", e.to_seqno()),
    };

    // without lsm, with sticky
    let mut config: Config = Default::default();
    config.set_sticky(true).unwrap();
    let mut index: Box<ShLlrb<i64, i64>> = ShLlrb::new("test-shllrb", config);
    let key = populate(&mut index);
    let mut r = index.to_reader().unwrap();
    match r.get(&key) {
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
    match r.get(&missing_key) {
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
    let mut config: Config = Default::default();
    config.set_lsm(true).unwrap().set_sticky(true).unwrap();
    let mut index: Box<ShLlrb<i64, i64>> = ShLlrb::new("test-shllrb", config);
    let key = populate(&mut index);
    let mut r = index.to_reader().unwrap();
    match r.get(&key) {
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
    match r.get(&missing_key) {
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
        let mut w = index.to_writer().unwrap();
        let mut r = index.to_reader().unwrap();

        for _ in 0..500 {
            let key: i64 = random::<i64>().abs();
            let value: i64 = random();
            w.set(key, value).unwrap();
        }
        let keys = {
            let mut keys = vec![];
            for _ in 0..100 {
                let iter = r.iter().unwrap();
                let e = iter.skip(random::<u8>() as usize).next().unwrap();
                keys.push(e.unwrap().to_key());
            }
            keys
        };
        for key in keys.iter() {
            w.delete(key).unwrap();
        }
        assert!(w.delete(&missing_key).unwrap().is_none());
    };

    // without lsm
    let mut index: Box<Llrb<i64, i64>> = Llrb::new("test-llrb");
    populate(&mut index);
    assert_eq!(index.to_stats().unwrap().n_deleted, 0);

    // validate will make sure the that n_deleted count is correct.
    assert!(index.validate().is_ok());
}

#[test]
fn test_set() {
    let config: Config = Default::default();
    let mut index: Box<ShLlrb<i64, i64>> = ShLlrb::new("test-shllrb", config);
    let mut refns = RefNodes::new(false /*lsm*/, 10);

    let mut w = index.to_writer().unwrap();

    assert!(w.set(2, 10).unwrap().is_none());
    refns.set(2, 10);
    assert!(w.set(1, 10).unwrap().is_none());
    refns.set(1, 10);
    assert!(w.set(3, 10).unwrap().is_none());
    refns.set(3, 10);
    assert!(w.set(6, 10).unwrap().is_none());
    refns.set(6, 10);
    assert!(w.set(5, 10).unwrap().is_none());
    refns.set(5, 10);
    assert!(w.set(4, 10).unwrap().is_none());
    refns.set(4, 10);
    assert!(w.set(8, 10).unwrap().is_none());
    refns.set(8, 10);
    assert!(w.set(0, 10).unwrap().is_none());
    refns.set(0, 10);
    assert!(w.set(9, 10).unwrap().is_none());
    refns.set(9, 10);
    assert!(w.set(7, 10).unwrap().is_none());
    refns.set(7, 10);

    assert_eq!(index.len().unwrap(), 10);
    assert!(index.validate().is_ok());

    let mut r = index.to_reader().unwrap();

    assert_eq!(refns.to_seqno(), index.to_seqno().unwrap());
    // test get
    for i in 0..10 {
        let entry = r.get(&i);
        let refn = refns.get(i);
        check_node(entry.ok(), refn);
    }
    // test iter
    {
        let (mut iter, mut iter_ref) = (r.iter().unwrap(), refns.iter());
        loop {
            let item = iter.next().transpose().unwrap();
            if check_node(item, iter_ref.next().cloned()) == false {
                break;
            }
        }
    }
    assert!(index.validate().is_ok());
}

#[test]
fn test_cas_lsm() {
    let mut config: Config = Default::default();
    config.set_lsm(true).unwrap();
    let mut index: Box<ShLlrb<i64, i64>> = ShLlrb::new("test-shllrb", config);
    let mut refns = RefNodes::new(true /*lsm*/, 11);

    let mut w = index.to_writer().unwrap();

    assert!(w.set(2, 100).unwrap().is_none());
    refns.set(2, 100);
    assert!(w.set(1, 100).unwrap().is_none());
    refns.set(1, 100);
    assert!(w.set(3, 100).unwrap().is_none());
    refns.set(3, 100);
    assert!(w.set(6, 100).unwrap().is_none());
    refns.set(6, 100);
    assert!(w.set(5, 100).unwrap().is_none());
    refns.set(5, 100);
    assert!(w.set(4, 100).unwrap().is_none());
    refns.set(4, 100);
    assert!(w.set(8, 100).unwrap().is_none());
    refns.set(8, 100);
    assert!(w.set(0, 100).unwrap().is_none());
    refns.set(0, 100);
    assert!(w.set(9, 100).unwrap().is_none());
    refns.set(9, 100);
    assert!(w.set(7, 100).unwrap().is_none());
    refns.set(7, 100);

    // repeated mutations on same key

    let entry = w.set_cas(0, 200, 8).unwrap();
    let refn = refns.set_cas(0, 200, 8);
    check_node(entry, refn);

    let entry = w.set_cas(5, 200, 5).unwrap();
    let refn = refns.set_cas(5, 200, 5);
    check_node(entry, refn);

    let entry = w.set_cas(6, 200, 4).unwrap();
    let refn = refns.set_cas(6, 200, 4);
    check_node(entry, refn);

    let entry = w.set_cas(9, 200, 9).unwrap();
    let refn = refns.set_cas(9, 200, 9);
    check_node(entry, refn);

    let entry = w.set_cas(0, 300, 11).unwrap();
    let refn = refns.set_cas(0, 300, 11);
    check_node(entry, refn);

    let entry = w.set_cas(5, 300, 12).unwrap();
    let refn = refns.set_cas(5, 300, 12);
    check_node(entry, refn);

    let entry = w.set_cas(9, 300, 14).unwrap();
    let refn = refns.set_cas(9, 300, 14);
    check_node(entry, refn);

    // create
    assert!(w.set_cas(10, 100, 0).ok().unwrap().is_none());
    assert!(refns.set_cas(10, 100, 0).is_none());
    // error create
    assert_eq!(w.set_cas(10, 100, 0).err(), Some(Error::InvalidCAS(18)));
    refns.set_cas(10, 100, 0);
    // error insert
    assert_eq!(w.set_cas(9, 400, 14).err(), Some(Error::InvalidCAS(17)));
    refns.set_cas(9, 400, 14);

    assert_eq!(index.len().unwrap(), 11);
    assert!(index.validate().is_ok());

    let mut r = index.to_reader().unwrap();

    assert_eq!(refns.to_seqno(), index.to_seqno().unwrap());
    // test get
    for i in 0..11 {
        let entry = r.get(&i);
        let refn = refns.get(i);
        check_node(entry.ok(), refn);
    }
    // test iter
    let (mut iter, mut iter_ref) = (r.iter().unwrap(), refns.iter());
    loop {
        let item = iter.next().transpose().unwrap();
        if check_node(item, iter_ref.next().cloned()) == false {
            break;
        }
    }
}

#[test]
fn test_delete() {
    let config: Config = Default::default();
    let mut index: Box<ShLlrb<i64, i64>> = ShLlrb::new("test-shllrb", config);
    let mut refns = RefNodes::new(false /*lsm*/, 11);

    let mut w = index.to_writer().unwrap();

    assert!(w.set(2, 100).unwrap().is_none());
    refns.set(2, 100);
    assert!(w.set(1, 100).unwrap().is_none());
    refns.set(1, 100);
    assert!(w.set(3, 100).unwrap().is_none());
    refns.set(3, 100);
    assert!(w.set(6, 100).unwrap().is_none());
    refns.set(6, 100);
    assert!(w.set(5, 100).unwrap().is_none());
    refns.set(5, 100);
    assert!(w.set(4, 100).unwrap().is_none());
    refns.set(4, 100);
    assert!(w.set(8, 100).unwrap().is_none());
    refns.set(8, 100);
    assert!(w.set(0, 100).unwrap().is_none());
    refns.set(0, 100);
    assert!(w.set(9, 100).unwrap().is_none());
    refns.set(9, 100);
    assert!(w.set(7, 100).unwrap().is_none());
    refns.set(7, 100);

    // delete a missing node.
    assert!(w.delete(&10).unwrap().is_none());
    assert!(refns.delete(10).is_none());

    let mut r = index.to_reader().unwrap();

    assert_eq!(index.len().unwrap(), 10);
    assert!(index.validate().is_ok());

    assert_eq!(refns.to_seqno(), index.to_seqno().unwrap());
    // test iter
    //println!("start loop");
    {
        let (mut iter, mut iter_ref) = (r.iter().unwrap(), refns.iter());
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
        let entry = w.delete(&i).unwrap();
        let refn = refns.delete(i);
        check_node(entry, refn);
    }
    assert_eq!(refns.to_seqno(), index.to_seqno().unwrap());
    assert_eq!(index.len().unwrap(), 0);
    assert!(index.validate().is_ok());
    // test iter
    assert!(r.iter().unwrap().next().is_none());
}

#[test]
fn test_iter() {
    let config: Config = Default::default();
    let mut index: Box<ShLlrb<i64, i64>> = ShLlrb::new("test-shllrb", config);
    let mut refns = RefNodes::new(false /*lsm*/, 10);

    let mut w = index.to_writer().unwrap();

    assert!(w.set(2, 10).unwrap().is_none());
    refns.set(2, 10);
    assert!(w.set(1, 10).unwrap().is_none());
    refns.set(1, 10);
    assert!(w.set(3, 10).unwrap().is_none());
    refns.set(3, 10);
    assert!(w.set(6, 10).unwrap().is_none());
    refns.set(6, 10);
    assert!(w.set(5, 10).unwrap().is_none());
    refns.set(5, 10);
    assert!(w.set(4, 10).unwrap().is_none());
    refns.set(4, 10);
    assert!(w.set(8, 10).unwrap().is_none());
    refns.set(8, 10);
    assert!(w.set(0, 10).unwrap().is_none());
    refns.set(0, 10);
    assert!(w.set(9, 10).unwrap().is_none());
    refns.set(9, 10);
    assert!(w.set(7, 10).unwrap().is_none());
    refns.set(7, 10);

    assert_eq!(index.len().unwrap(), 10);
    assert!(index.validate().is_ok());

    let mut r = index.to_reader().unwrap();

    assert_eq!(refns.to_seqno(), index.to_seqno().unwrap());
    // test iter
    let (mut iter, mut iter_ref) = (r.iter().unwrap(), refns.iter());
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
    let config: Config = Default::default();
    let mut index: Box<ShLlrb<i64, i64>> = ShLlrb::new("test-shllrb", config);
    let mut refns = RefNodes::new(false /*lsm*/, 10);

    let mut w = index.to_writer().unwrap();

    assert!(w.set(2, 10).unwrap().is_none());
    refns.set(2, 10);
    assert!(w.set(1, 10).unwrap().is_none());
    refns.set(1, 10);
    assert!(w.set(3, 10).unwrap().is_none());
    refns.set(3, 10);
    assert!(w.set(6, 10).unwrap().is_none());
    refns.set(6, 10);
    assert!(w.set(5, 10).unwrap().is_none());
    refns.set(5, 10);
    assert!(w.set(4, 10).unwrap().is_none());
    refns.set(4, 10);
    assert!(w.set(8, 10).unwrap().is_none());
    refns.set(8, 10);
    assert!(w.set(0, 10).unwrap().is_none());
    refns.set(0, 10);
    assert!(w.set(9, 10).unwrap().is_none());
    refns.set(9, 10);
    assert!(w.set(7, 10).unwrap().is_none());
    refns.set(7, 10);

    assert_eq!(index.len().unwrap(), 10);
    assert!(index.validate().is_ok());

    let mut r = index.to_reader().unwrap();

    assert_eq!(refns.to_seqno(), index.to_seqno().unwrap());
    // test range
    for _ in 0..1_000 {
        let (low, high) = random_low_high(index.len().unwrap());

        {
            let mut iter = r.range((low, high)).unwrap();
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
            let mut iter = r.reverse((low, high)).unwrap();
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

#[test]
fn test_crud() {
    let size = 1000;
    let config: Config = Default::default();
    let mut index: Box<ShLlrb<i64, i64>> = ShLlrb::new("test-shllrb", config);
    let mut refns = RefNodes::new(false /*lsm*/, size);

    let mut w = index.to_writer().unwrap();

    for _ in 0..100000 {
        let key: i64 = (random::<i64>() % (size as i64)).abs();
        let value: i64 = random();
        let op: i64 = (random::<i64>() % 3).abs();
        //println!("key {} value {} op {}", key, value, op);
        match op {
            0 => {
                let entry = w.set(key, value).unwrap();
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

                let entry = w.set_cas(key, value, cas).ok().unwrap();
                let refn = refns.set_cas(key, value, cas);
                check_node(entry, refn);
                false
            }
            2 => {
                let entry = w.delete(&key).unwrap();
                let refn = refns.delete(key);
                check_node(entry, refn);
                true
            }
            op => panic!("unreachable {}", op),
        };

        assert!(index.validate().is_ok());
    }

    //println!("len {}", index.len());
    assert_eq!(refns.to_seqno(), index.to_seqno().unwrap());

    let mut r = index.to_reader().unwrap();
    {
        // test iter
        let (mut iter, mut iter_ref) = (r.iter().unwrap(), refns.iter());
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
            let mut iter = r.range((low, high)).unwrap();
            let mut iter_ref = refns.range(low, high);
            loop {
                let item = iter.next().transpose().unwrap();
                if check_node(item, iter_ref.next().cloned()) == false {
                    break;
                }
            }
        }

        {
            let mut iter = r.reverse((low, high)).unwrap();
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
    let mut config: Config = Default::default();
    config.set_lsm(true).unwrap();
    let mut index: Box<ShLlrb<i64, i64>> = ShLlrb::new("test-shllrb", config);
    let mut refns = RefNodes::new(true /*lsm*/, size as usize);

    let mut w = index.to_writer().unwrap();

    for _i in 0..20000 {
        let key: i64 = (random::<i64>() % size).abs();
        let value: i64 = random();
        let op: i64 = (random::<i64>() % 3).abs();
        // println!("test_crud_lsm seqno:{} op:{} key:{}", _i + 1, op, key);
        match op {
            0 => {
                let entry = w.set(key, value).unwrap();
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
                let entry = w.set_cas(key, value, cas).ok().unwrap();
                let refn = refns.set_cas(key, value, cas);
                check_node(entry, refn);
                false
            }
            2 => {
                let entry = w.delete(&key).unwrap();
                let refn = refns.delete(key);
                check_node(entry, refn);
                true
            }
            op => panic!("unreachable {}", op),
        };
        assert_eq!(index.to_seqno().unwrap(), refns.to_seqno());

        assert!(index.validate().is_ok());
    }

    // println!("len {}", index.len());
    assert_eq!(refns.to_seqno(), index.to_seqno().unwrap());

    let mut r = index.to_reader().unwrap();
    {
        // test iter
        let (mut iter, mut iter_ref) = (r.iter().unwrap(), refns.iter());
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
            let mut iter = r.range((low, high)).unwrap();
            let mut iter_ref = refns.range(low, high);
            loop {
                let item = iter.next().transpose().unwrap();
                if check_node(item, iter_ref.next().cloned()) == false {
                    break;
                }
            }
        }

        {
            let mut iter = r.reverse((low, high)).unwrap();
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

//#[test]
//fn test_pw_scan() {
//}

#[test]
fn test_commit1() {
    let mut config: Config = Default::default();
    config.set_lsm(true).unwrap();
    let mut index1: Box<ShLlrb<i64, i64>> = ShLlrb::new("ti1", config.clone());
    let index2: Box<ShLlrb<i64, i64>> = ShLlrb::new("ti2", config.clone());
    let mut rindex: Box<ShLlrb<i64, i64>> = ShLlrb::new("tri", config.clone());

    index1
        .commit(
            core::CommitIter::new(index2, (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded)),
            |meta| meta.clone(),
        )
        .unwrap();
    check_commit_nodes(index1.as_mut(), rindex.as_mut());
}

#[test]
fn test_commit2() {
    let mut config: Config = Default::default();
    config.set_lsm(true).unwrap();
    let mut index1: Box<ShLlrb<i64, i64>> = ShLlrb::new("ti1", config.clone());
    let mut index2: Box<ShLlrb<i64, i64>> = ShLlrb::new("ti2", config.clone());
    let mut rindex: Box<ShLlrb<i64, i64>> = ShLlrb::new("tri", config.clone());

    {
        let mut w = index2.to_writer().unwrap();
        w.set(100, 200).unwrap();
    }

    let mut w = rindex.to_writer().unwrap();
    w.set(100, 200).unwrap();

    index1
        .commit(
            core::CommitIter::new(index2, (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded)),
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
        // println!("test case {}", _i);
        let lsm: bool = rng.gen();
        let sticky: bool = lsm || true;

        let mut config: Config = Default::default();
        config.set_lsm(lsm).unwrap().set_sticky(sticky).unwrap();
        let mut index1 = ShLlrb::<i64, i64>::new("ti1", config.clone());
        let mut index2 = ShLlrb::<i64, i64>::new("ti2", config.clone());
        let mut rindex = ShLlrb::<i64, i64>::new("tri", config.clone());

        //  println!("index-config: lsm:{} sticky:{}", lsm, sticky);

        let mut w = index1.to_writer().unwrap();
        let mut rw = rindex.to_writer().unwrap();
        let n_ops = rng.gen::<usize>() % 1000;
        for _ in 0..n_ops {
            let key: i64 = rng.gen::<i64>().abs() % (n_ops as i64 * 3);
            let value: i64 = rng.gen();
            let op: i64 = (rng.gen::<i64>() % 2).abs();
            //  println!("target k:{} v:{} {}", key, value, op);
            match op {
                0 => {
                    w.set(key, value).unwrap();
                    rw.set(key, value).unwrap();
                }
                1 => {
                    w.delete(&key).unwrap();
                    rw.delete(&key).unwrap();
                }
                op => panic!("unreachable {}", op),
            };
        }

        index2.set_seqno(index1.to_seqno().unwrap()).unwrap();
        let mut w = index2.to_writer().unwrap();
        let mut rw = rindex.to_writer().unwrap();

        let n_ops = rng.gen::<usize>() % 1000;
        for _ in 0..n_ops {
            let key: i64 = rng.gen::<i64>().abs() % (n_ops as i64 * 3);
            let value: i64 = rng.gen();
            let op: i64 = (rng.gen::<i64>() % 2).abs();
            //  println!("commit k:{} v:{} {}", key, value, op);
            match op {
                0 => {
                    w.set(key, value).unwrap();
                    rw.set(key, value).unwrap();
                }
                1 => {
                    w.delete(&key).unwrap();
                    rw.delete(&key).unwrap();
                }
                op => panic!("unreachable {}", op),
            };
        }
        mem::drop(w);

        index1
            .commit(
                core::CommitIter::new(index2, (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded)),
                |meta| meta.clone(),
            )
            .unwrap();
        check_commit_nodes(index1.as_mut(), rindex.as_mut());
    }
}

fn check_commit_nodes(index: &mut ShLlrb<i64, i64>, ref_index: &mut ShLlrb<i64, i64>) {
    // verify root index
    let (stats, ref_stats) = (index.to_stats().unwrap(), ref_index.to_stats().unwrap());
    assert_eq!(index.to_seqno().unwrap(), ref_index.to_seqno().unwrap());
    assert_eq!(stats.entries, ref_stats.entries);
    assert_eq!(stats.n_deleted, ref_stats.n_deleted);
    assert_eq!(stats.key_footprint, ref_stats.key_footprint);
    assert_eq!(stats.tree_footprint, ref_stats.tree_footprint);

    // verify each entry
    let mut index_r = index.to_reader().unwrap();
    let mut iter = index_r.iter().unwrap();

    let mut ref_index_r = ref_index.to_reader().unwrap();
    let mut refiter = ref_index_r.iter().unwrap();

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

        let mut ei = entry.versions();
        let mut ri = refentry.versions();
        loop {
            let (e, re) = (ei.next(), ri.next());
            let (e, re) = match (e, re) {
                (Some(e), Some(re)) => (e, re),
                (None, None) => break,
                _ => unreachable!(),
            };
            assert_eq!(e.to_seqno(), re.to_seqno(), "key {}", key);
            assert_eq!(e.to_native_value(), re.to_native_value(), "key {}", key);
            assert_eq!(e.is_deleted(), re.is_deleted(), "key {}", key);
            assert_eq!(e.to_seqno_state(), re.to_seqno_state(), "key {}", key);
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
        // println!("test case {}", _i);
        let lsm: bool = rng.gen();
        let sticky: bool = lsm || true;

        let mut config: Config = Default::default();
        config.set_lsm(lsm).unwrap().set_sticky(sticky).unwrap();
        let mut index = ShLlrb::<i64, i64>::new("ti1", config.clone());
        let mut rindex = ShLlrb::<i64, i64>::new("tri", config.clone());

        let mut w = index.to_writer().unwrap();
        let mut rw = rindex.to_writer().unwrap();

        let n_ops = rng.gen::<usize>() % 100_000;
        for _ in 0..n_ops {
            let key: i64 = rng.gen::<i64>().abs() % (n_ops as i64 / 2);
            let value: i64 = rng.gen();
            let op: i64 = (rng.gen::<i64>() % 2).abs();
            //  println!("target k:{} v:{} {}", key, value, op);
            match op {
                0 => {
                    w.set(key, value).unwrap();
                    rw.set(key, value).unwrap();
                }
                1 => {
                    w.delete(&key).unwrap();
                    rw.delete(&key).unwrap();
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
    index: &mut ShLlrb<i64, i64>,
    rindex: &mut ShLlrb<i64, i64>,
    cutoff: Cutoff, // either tombstone/lsm.
) {
    // verify root index
    assert_eq!(index.to_seqno().unwrap(), rindex.to_seqno().unwrap());

    let mut n_count = 0;
    let mut n_deleted = 0;
    let mut key_footprint = 0;
    let mut tree_footprint = 0;

    let mut rr = rindex.to_reader().unwrap();
    let res: Vec<Entry<i64, i64>> = rr
        .iter()
        .unwrap()
        .map(|e| e.unwrap())
        .filter_map(|e| e.purge(cutoff.clone()))
        .collect();

    {
        let mut r = index.to_reader().unwrap();
        let mut iter = r.iter().unwrap();
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

    let stats = index.to_stats().unwrap();
    assert_eq!(n_count, stats.entries);
    assert_eq!(n_deleted, stats.n_deleted);
    assert_eq!(key_footprint, stats.key_footprint);
    assert_eq!(
        tree_footprint, stats.tree_footprint,
        "for n_count {} n_deleted {}",
        n_count, n_deleted
    );
}

#[test]
fn test_commit_iterator_scan() {
    let seed: u128 = random();
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let (n_ops, key_max) = (60_000_i64, 20_000);
    let mut config: Config = Default::default();
    config.set_lsm(true).unwrap();
    let mut index: Box<ShLlrb<i64, i64>> = ShLlrb::new("test-shllrb", config);

    random_index(n_ops, key_max, seed, &mut index);

    for i in 0..20 {
        let from_seqno = match rng.gen::<i64>() % n_ops {
            n if n >= 0 && n % 2 == 0 => Bound::Included(n as u64),
            n if n >= 0 => Bound::Excluded(n as u64),
            _ => Bound::Unbounded,
        };
        let mut r = index.to_reader().unwrap();
        let mut ref_iter = r.iter().unwrap();
        let within = (from_seqno, Bound::Included(index.to_seqno().unwrap()));
        let mut count = 0;
        let mut iter = index.scan(within.clone()).unwrap();
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
    let seed: u128 = random();
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let (n_ops, key_max) = (60_000_i64, 20_000);
    let mut config: Config = Default::default();
    config.set_lsm(true).unwrap();
    let mut index: Box<ShLlrb<i64, i64>> = ShLlrb::new("test-shllrb", config);

    random_index(n_ops, key_max, seed, &mut index);

    for i in 0..20 {
        let shards = rng.gen::<usize>() % 31 + 1;
        let from_seqno = match rng.gen::<i64>() % n_ops {
            n if n >= 0 && n % 2 == 0 => Bound::Included(n as u64),
            n if n >= 0 => Bound::Excluded(n as u64),
            _ => Bound::Unbounded,
        };
        let mut r = index.to_reader().unwrap();
        let within = (from_seqno, Bound::Included(index.to_seqno().unwrap()));

        let mut iter = {
            let mut iters = index.scans(shards, within.clone()).unwrap();
            iters.reverse(); // convert this to stack
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
    let mut config: Config = Default::default();
    config.set_lsm(true).unwrap();
    let mut index: Box<ShLlrb<i64, i64>> = ShLlrb::new("test-shllrb", config);

    random_index(n_ops, key_max, seed, &mut index);

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
        let mut r = index.to_reader().unwrap();
        let within = (from_seqno, Bound::Included(index.to_seqno().unwrap()));
        let mut iter = {
            let mut iters = index.range_scans(ranges, within.clone()).unwrap();
            iters.reverse(); // convert this to stack
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

fn random_index(n_ops: i64, key_max: i64, seed: u128, index: &mut ShLlrb<i64, i64>) {
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let mut w = index.to_writer().unwrap();
    let mut r = index.to_reader().unwrap();

    for _i in 0..n_ops {
        let key = (rng.gen::<i64>() % key_max).abs();
        let op = rng.gen::<usize>() % 3;
        //println!("key {} {} {} {}", key, index.to_seqno(), op);
        match op {
            0 => {
                let value: i64 = rng.gen();
                w.set(key, value).unwrap();
            }
            1 => {
                let value: i64 = rng.gen();
                {
                    let cas = match r.get(&key) {
                        Err(Error::KeyNotFound) => 0,
                        Err(_err) => unreachable!(),
                        Ok(e) => e.to_seqno(),
                    };
                    w.set_cas(key, value, cas).unwrap();
                }
            }
            2 => {
                w.delete(&key).unwrap();
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

    //println!("versions {} {}", n_vers, refn_vers);
    let mut vers = entry.versions();
    let mut ref_vers = ref_entry.versions();

    let (mut n, mut ref_n) = (0, 0);
    loop {
        match (vers.next(), ref_vers.next()) {
            (Some(e), Some(re)) => {
                assert_eq!(e.to_native_value(), re.to_native_value(), "key {}", key);
                assert_eq!(e.to_seqno(), re.to_seqno(), "key {} ", key);
                assert_eq!(e.is_deleted(), re.is_deleted(), "key {}", key);
                n += 1;
                ref_n += 1;
            }
            (None, None) => break,
            (Some(e), None) => panic!("invalid entry {} {}", e.to_key(), e.to_seqno()),
            (None, Some(re)) => panic!("invalid entry {} {}", re.to_key(), re.to_seqno()),
        }
    }

    assert_eq!(n, ref_n, "key {}", key);
}
