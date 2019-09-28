use std::convert::TryInto;
use std::ops::Bound;
use std::thread;

use rand::prelude::random;
use rand::{rngs::SmallRng, Rng, SeedableRng};

use super::*;

use crate::core::{Index, IndexIter, Reader, Writer};
use crate::error::Error;
use crate::llrb::Llrb;
use crate::mvcc::{Mvcc, MvccReader, MvccWriter};
use crate::robt;
use crate::scans::SkipScan;

#[test]
fn test_lsm_get1() {
    // test case using 5 mvcc versions
    let seed: u128 = random();
    println!("seed {}", seed);
    let mut refi = Llrb::new_lsm("test-llrb");

    let (n_ops, key_max) = random_ops_keys(seed, 60, 20);
    println!("mvcc1 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc1: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc1");
    random_mvcc(n_ops, key_max, seed, &mut mvcc1, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 600, 200);
    println!("mvcc2 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc2: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc2");
    mvcc2.set_seqno(mvcc1.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc2, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 6_000, 2_000);
    println!("mvcc3 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc3: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc3");
    mvcc3.set_seqno(mvcc2.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc3, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 60_000, 20_000);
    println!("mvcc4 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc4: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc4");
    mvcc4.set_seqno(mvcc3.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc4, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 600_000, 200_000);
    println!("mvcc5 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc5: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc5");
    mvcc5.set_seqno(mvcc4.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc5, &mut refi);

    let seqno = mvcc5.to_seqno();

    let yget = y_get(
        getter(&*mvcc5, false),
        y_get(
            getter(&*mvcc4, false),
            y_get(
                getter(&*mvcc3, false),
                y_get(getter(&*mvcc2, false), getter(&*mvcc1, false)),
            ),
        ),
    );
    for entry in refi.iter().unwrap() {
        let entry = entry.unwrap();
        let key = entry.to_key();
        let e = yget(&key).unwrap();

        let (a, z) = (Bound::Unbounded, Bound::Included(seqno));
        let e = e.filter_within(a, z).unwrap();
        assert_eq!(entry.to_key(), e.to_key());
        assert_eq!(entry.to_seqno(), e.to_seqno(), "for key {}", key,);
        assert_eq!(entry.is_deleted(), e.is_deleted(), "for key {}", key);
        assert_eq!(entry.to_native_value(), e.to_native_value(), "key {}", key);
    }
}

#[test]
fn test_lsm_get2() {
    // test case using 2 robt version and 1 mvcc versions
    let seed: u128 = random();
    println!("seed {}", seed);
    let mut refi = Llrb::new_lsm("test-llrb");

    let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");

    let (n_ops, key_max) = random_ops_keys(seed, 60_000, 20_000);
    let n_ops = n_ops + 1;
    random_llrb(n_ops, key_max, seed, &mut llrb, &mut refi);
    let (name, delta_ok) = ("test_lsm_get2-1", false);
    let disk1 = {
        let w = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
        let iter = Box::new(SkipScan::new(&*llrb, w));
        random_robt(name, seed, delta_ok, iter)
    };
    println!("disk1 n_ops: {} key_max: {}", n_ops, key_max);

    let (n_ops, key_max) = random_ops_keys(seed, 120_000, 40_000);
    let n_ops = n_ops + 1;
    random_llrb(n_ops, key_max, seed, &mut llrb, &mut refi);
    let (name, delta_ok) = ("test_lsm_get2-2", false);
    let disk2 = {
        let w = (Bound::Excluded(disk1.to_seqno()), Bound::<u64>::Unbounded);
        let iter = Box::new(SkipScan::new(&*llrb, w));
        random_robt(name, seed, delta_ok, iter)
    };
    println!("disk2 n_ops: {} key_max: {}", n_ops, key_max);

    let (n_ops, key_max) = random_ops_keys(seed, 200_000, 60_000);
    let mut mvcc: Box<Mvcc<i64, i64>> = Mvcc::from_llrb(*llrb);
    random_mvcc(n_ops, key_max, seed, mvcc.as_mut(), &mut refi);
    println!("mvcc n_ops: {} key_max: {}", n_ops, key_max);

    let seqno = mvcc.to_seqno();
    let w = mvcc.to_writer().unwrap();
    let r = mvcc.to_reader().unwrap();
    let t_handle = {
        let (_, key_max) = random_ops_keys(seed, 400_000, 400_000);
        let n_ops = 400_000;
        thread::spawn(move || concurrent_write(n_ops, key_max, seed, r, w))
    };

    // println!("start verification mvcc seqno {}", seqno);
    let yget = y_get(
        getter(&*mvcc, false),
        y_get(getter(&disk2, false), getter(&disk1, false)),
    );
    let _start = std::time::SystemTime::now();
    for entry in refi.iter().unwrap() {
        let entry = entry.unwrap();
        let key = entry.to_key();
        let e = yget(&key).unwrap();

        let (a, z) = (Bound::Unbounded, Bound::Included(seqno));
        let e = e.filter_within(a, z).unwrap();
        assert_eq!(entry.to_key(), e.to_key());
        assert_eq!(entry.to_seqno(), e.to_seqno(), "for key {}", key,);
        assert_eq!(entry.is_deleted(), e.is_deleted(), "for key {}", key);
        assert_eq!(entry.to_native_value(), e.to_native_value(), "key {}", key);
        assert_eq!(entry.as_deltas().len(), e.as_deltas().len());
    }
    // println!("get elapsed {:?}", _start.elapsed().unwrap().as_nanos());
    t_handle.join().unwrap();
}

#[test]
#[ignore]
fn test_lsm_get_versions1() {
    // test case using 5 mvcc versions
    let seed: u128 = random();
    println!("seed {}", seed);
    let mut refi = Llrb::new_lsm("test-llrb");

    let (n_ops, key_max) = random_ops_keys(seed, 60, 20);
    println!("mvcc1 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc1: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc1");
    random_mvcc(n_ops, key_max, seed, &mut mvcc1, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 600, 200);
    println!("mvcc2 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc2: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc2");
    mvcc2.set_seqno(mvcc1.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc2, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 6_000, 2_000);
    println!("mvcc3 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc3: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc3");
    mvcc3.set_seqno(mvcc2.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc3, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 60_000, 20_000);
    println!("mvcc4 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc4: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc4");
    mvcc4.set_seqno(mvcc3.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc4, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 600_000, 200_000);
    let n_ops = n_ops + 1;
    println!("mvcc5 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc5: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc5");
    mvcc5.set_seqno(mvcc4.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc5, &mut refi);

    let seqno = mvcc5.to_seqno();

    let yget = y_get_versions(
        getter(&*mvcc5, true),
        y_get_versions(
            getter(&*mvcc4, true),
            y_get_versions(
                getter(&*mvcc3, true),
                y_get_versions(getter(&*mvcc2, true), getter(&*mvcc1, true)),
            ),
        ),
    );
    for entry in refi.iter().unwrap() {
        let entry = entry.unwrap();
        let key = entry.to_key();
        // println!("entry key {}", key);
        let e = yget(&key).unwrap();

        let (a, z) = (Bound::Unbounded, Bound::Included(seqno));
        let e = e.filter_within(a, z).unwrap();
        assert_eq!(entry.to_key(), e.to_key());
        assert_eq!(entry.to_seqno(), e.to_seqno(), "for key {}", key,);
        assert_eq!(entry.is_deleted(), e.is_deleted(), "for key {}", key);
        assert_eq!(entry.to_native_value(), e.to_native_value(), "key {}", key);
        assert_eq!(entry.as_deltas().len(), e.as_deltas().len());
        let iter = entry.to_deltas().into_iter().zip(e.to_deltas().into_iter());
        for (x, y) in iter {
            // println!("x-seqno {} y-seqno {}", x.to_seqno(), y.to_seqno());
            assert_eq!(x.to_seqno(), y.to_seqno());
            assert_eq!(x.is_deleted(), y.is_deleted());
            let (m, n) = (entry.to_native_value(), e.to_native_value());
            assert_eq!(m, n, "key {}", key);
        }
    }
}

#[test]
#[ignore]
fn test_lsm_get_versions2() {
    // test case using 2 robt version and 1 mvcc versions
    let seed: u128 = random();
    // let seed: u128 = 207831376735128016456730006479960249204;
    println!("seed {}", seed);
    let mut refi = Llrb::new_lsm("test-llrb");

    let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");

    let (n_ops, key_max) = random_ops_keys(seed, 60_000, 20_000);
    let n_ops = n_ops + 1;
    random_llrb(n_ops, key_max, seed, &mut llrb, &mut refi);
    let (name, delta_ok) = ("test_lsm_get_versions2-1", true);
    let disk1 = {
        let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
        let iter = Box::new(SkipScan::new(&*llrb, within));
        random_robt(name, seed, delta_ok, iter)
    };
    let d1_seqno = disk1.to_seqno();
    println!(
        "disk1 n_ops:{} key_max:{} seqno:{}",
        n_ops, key_max, d1_seqno
    );

    let (n_ops, key_max) = random_ops_keys(seed, 120_000, 40_000);
    let n_ops = n_ops + 1;
    random_llrb(n_ops, key_max, seed, &mut llrb, &mut refi);
    let (name, delta_ok) = ("test_lsm_get_versions2-2", true);
    let disk2 = {
        let within = (Bound::Excluded(d1_seqno), Bound::<u64>::Unbounded);
        let iter = Box::new(SkipScan::new(&*llrb, within));
        random_robt(name, seed, delta_ok, iter)
    };
    let d2_seqno = disk2.to_seqno();
    println!(
        "disk2 n_ops:{} key_max:{} seqno:{}",
        n_ops, key_max, d2_seqno
    );

    let (n_ops, key_max) = random_ops_keys(seed, 200_000, 60_000);
    let mut mvcc: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc");
    mvcc.set_seqno(d2_seqno);
    random_mvcc(n_ops, key_max, seed, mvcc.as_mut(), &mut refi);
    println!("mvcc n_ops: {} key_max: {}", n_ops, key_max);

    let seqno = mvcc.to_seqno();
    let w = mvcc.to_writer().unwrap();
    let r = mvcc.to_reader().unwrap();
    let t_handle = {
        let (_, key_max) = random_ops_keys(seed, 400_000, 400_000);
        let n_ops = 400_000;
        thread::spawn(move || concurrent_write(n_ops, key_max, seed, r, w))
    };

    // println!("start verification mvcc seqno {}", seqno);
    let yget = y_get_versions(
        getter(&*mvcc, true),
        y_get_versions(getter(&disk2, true), getter(&disk1, true)),
    );
    let _start = std::time::SystemTime::now();
    for entry in refi.iter().unwrap() {
        let entry = entry.unwrap();
        let key = entry.to_key();
        // println!("entry key {}", key);
        let e = yget(&key).unwrap();

        let (a, z) = (Bound::Unbounded, Bound::Included(seqno));
        let e = e.filter_within(a, z).unwrap();
        assert_eq!(entry.to_key(), e.to_key());
        assert_eq!(entry.to_seqno(), e.to_seqno(), "for key {}", key,);
        assert_eq!(entry.is_deleted(), e.is_deleted(), "for key {}", key);
        assert_eq!(entry.to_native_value(), e.to_native_value(), "key {}", key);
        assert_eq!(entry.as_deltas().len(), e.as_deltas().len());
        let iter = entry.to_deltas().into_iter().zip(e.to_deltas().into_iter());
        for (x, y) in iter {
            assert_eq!(x.to_seqno(), y.to_seqno());
            assert_eq!(x.is_deleted(), y.is_deleted());
            let (m, n) = (entry.to_native_value(), e.to_native_value());
            assert_eq!(m, n, "key {}", key);
        }
    }
    // println!("get elapsed {:?}", _start.elapsed().unwrap().as_nanos());
    t_handle.join().unwrap();
}

#[test]
fn test_lsm_iter1() {
    // test case using 5 mvcc versions
    let seed: u128 = random();
    println!("seed {}", seed);
    let mut refi = Llrb::new_lsm("test-llrb");

    let (n_ops, key_max) = random_ops_keys(seed, 60, 20);
    println!("mvcc1 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc1: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc1");
    random_mvcc(n_ops, key_max, seed, &mut mvcc1, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 600, 200);
    println!("mvcc2 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc2: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc2");
    mvcc2.set_seqno(mvcc1.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc2, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 6_000, 2_000);
    println!("mvcc3 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc3: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc3");
    mvcc3.set_seqno(mvcc2.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc3, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 60_000, 20_000);
    println!("mvcc4 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc4: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc4");
    mvcc4.set_seqno(mvcc3.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc4, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 600_000, 200_000);
    println!("mvcc5 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc5: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc5");
    mvcc5.set_seqno(mvcc4.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc5, &mut refi);

    let seqno = mvcc5.to_seqno();

    let revr = false;
    let yiter = y_iter(
        mvcc5.iter().unwrap(),
        y_iter(
            mvcc4.iter().unwrap(),
            y_iter(
                mvcc3.iter().unwrap(),
                y_iter(mvcc2.iter().unwrap(), mvcc1.iter().unwrap(), revr),
                revr,
            ),
            revr,
        ),
        revr,
    );
    let entries1: Vec<Result<Entry<i64, i64>>> = refi.iter().unwrap().collect();
    let entries2: Vec<Result<Entry<i64, i64>>> = yiter.collect();

    assert_eq!(entries1.len(), entries2.len());
    for (entry, e) in entries1.into_iter().zip(entries2.into_iter()) {
        let (entry, e) = (entry.unwrap(), e.unwrap());
        let key = entry.to_key();

        let (a, z) = (Bound::Unbounded, Bound::Included(seqno));
        let e = e.filter_within(a, z).unwrap();
        assert_eq!(entry.to_key(), e.to_key());
        assert_eq!(entry.to_seqno(), e.to_seqno(), "for key {}", key,);
        assert_eq!(entry.is_deleted(), e.is_deleted(), "for key {}", key);
        assert_eq!(entry.to_native_value(), e.to_native_value(), "key {}", key);
    }
}

#[test]
fn test_lsm_iter2() {
    // test case using 2 robt version and 1 mvcc versions
    let seed: u128 = random();
    println!("seed {}", seed);
    let mut refi = Llrb::new_lsm("test-llrb");

    let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");

    let (n_ops, key_max) = random_ops_keys(seed, 60_000, 20_000);
    let n_ops = n_ops + 1;
    random_llrb(n_ops, key_max, seed, &mut llrb, &mut refi);
    let (name, delta_ok) = ("test_lsm_iter2-1", false);
    let disk1 = {
        let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
        let iter = Box::new(SkipScan::new(&*llrb, within));
        random_robt(name, seed, delta_ok, iter)
    };
    let d1_seqno = disk1.to_seqno();
    println!(
        "disk1 n_ops:{} key_max:{} seqno:{}",
        n_ops, key_max, d1_seqno
    );

    let (n_ops, key_max) = random_ops_keys(seed, 120_000, 40_000);
    let n_ops = n_ops + 1;
    random_llrb(n_ops, key_max, seed, &mut llrb, &mut refi);
    let (name, delta_ok) = ("test_lsm_iter2-2", false);
    let disk2 = {
        let within = (Bound::Excluded(d1_seqno), Bound::<u64>::Unbounded);
        let iter = Box::new(SkipScan::new(&*llrb, within));
        random_robt(name, seed, delta_ok, iter)
    };
    println!("disk2 n_ops: {} key_max: {}", n_ops, key_max);
    let d2_seqno = disk2.to_seqno();

    let (n_ops, key_max) = random_ops_keys(seed, 200_000, 60_000);
    let mut mvcc: Box<Mvcc<i64, i64>> = Mvcc::from_llrb(*llrb);
    random_mvcc(n_ops, key_max, seed, mvcc.as_mut(), &mut refi);
    println!("mvcc n_ops: {} key_max: {}", n_ops, key_max);

    let seqno = mvcc.to_seqno();
    let w = mvcc.to_writer().unwrap();
    let r = mvcc.to_reader().unwrap();
    let t_handle = {
        let (_, key_max) = random_ops_keys(seed, 400_000, 400_000);
        let n_ops = 400_000;
        thread::spawn(move || concurrent_write(n_ops, key_max, seed, r, w))
    };

    // println!("start verification mvcc seqno {}", seqno);
    let within = (Bound::Excluded(d2_seqno), Bound::Included(seqno));
    let revr = false;
    let yiter = y_iter(
        Box::new(SkipScan::new(&*mvcc, within)),
        y_iter(disk2.iter().unwrap(), disk1.iter().unwrap(), revr),
        revr,
    );
    let entries1: Vec<Result<Entry<i64, i64>>> = refi.iter().unwrap().collect();
    let entries2: Vec<Result<Entry<i64, i64>>> = yiter.collect();

    assert_eq!(entries1.len(), entries2.len());
    let _start = std::time::SystemTime::now();
    for (entry, e) in entries1.into_iter().zip(entries2.into_iter()) {
        let (entry, e) = (entry.unwrap(), e.unwrap());
        let key = entry.to_key();

        // TODO
        // let (a, z) = (Bound::Unbounded, Bound::Included(seqno));
        // let e = e.filter_within(a, z).unwrap();
        assert_eq!(entry.to_key(), e.to_key());
        assert_eq!(entry.to_seqno(), e.to_seqno(), "for key {}", key,);
        assert_eq!(entry.is_deleted(), e.is_deleted(), "for key {}", key);
        assert_eq!(entry.to_native_value(), e.to_native_value(), "key {}", key);
    }
    // println!("get elapsed {:?}", _start.elapsed().unwrap().as_nanos());
    t_handle.join().unwrap();
}

#[test]
#[ignore]
fn test_lsm_iter_versions1() {
    // test case using 5 mvcc versions
    let seed: u128 = random();
    println!("seed {}", seed);
    let mut refi = Llrb::new_lsm("test-llrb");

    let (n_ops, key_max) = random_ops_keys(seed, 60, 20);
    println!("mvcc1 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc1: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc1");
    random_mvcc(n_ops, key_max, seed, &mut mvcc1, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 600, 200);
    println!("mvcc2 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc2: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc2");
    mvcc2.set_seqno(mvcc1.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc2, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 6_000, 2_000);
    println!("mvcc3 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc3: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc3");
    mvcc3.set_seqno(mvcc2.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc3, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 60_000, 20_000);
    println!("mvcc4 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc4: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc4");
    mvcc4.set_seqno(mvcc3.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc4, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 600_000, 200_000);
    let n_ops = n_ops + 1;
    println!("mvcc5 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc5: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc5");
    mvcc5.set_seqno(mvcc4.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc5, &mut refi);

    let seqno = mvcc5.to_seqno();

    let revr = false;
    let yiter = y_iter_versions(
        mvcc5.iter_with_versions().unwrap(),
        y_iter_versions(
            mvcc4.iter_with_versions().unwrap(),
            y_iter_versions(
                mvcc3.iter_with_versions().unwrap(),
                y_iter_versions(
                    mvcc2.iter_with_versions().unwrap(),
                    mvcc1.iter_with_versions().unwrap(),
                    revr,
                ),
                revr,
            ),
            revr,
        ),
        revr,
    );
    let entries1: Vec<Result<Entry<i64, i64>>> = refi.iter().unwrap().collect();
    let entries2: Vec<Result<Entry<i64, i64>>> = yiter.collect();

    let _start = std::time::SystemTime::now();
    for (entry, e) in entries1.into_iter().zip(entries2.into_iter()) {
        let (entry, e) = (entry.unwrap(), e.unwrap());
        let key = entry.to_key();
        // println!("entry key {}", key);

        let (a, z) = (Bound::Unbounded, Bound::Included(seqno));
        let e = e.filter_within(a, z).unwrap();
        assert_eq!(entry.to_key(), e.to_key());
        assert_eq!(entry.to_seqno(), e.to_seqno(), "for key {}", key,);
        assert_eq!(entry.is_deleted(), e.is_deleted(), "for key {}", key);
        assert_eq!(entry.to_native_value(), e.to_native_value(), "key {}", key);
        assert_eq!(entry.as_deltas().len(), e.as_deltas().len());
        let iter = entry.to_deltas().into_iter().zip(e.to_deltas().into_iter());
        for (x, y) in iter {
            // println!("x-seqno {} y-seqno {}", x.to_seqno(), y.to_seqno());
            assert_eq!(x.to_seqno(), y.to_seqno());
            assert_eq!(x.is_deleted(), y.is_deleted());
            let (m, n) = (entry.to_native_value(), e.to_native_value());
            assert_eq!(m, n, "key {}", key);
        }
    }
}

#[test]
#[ignore]
fn test_lsm_iter_versions2() {
    // test case using 2 robt version and 1 mvcc versions
    let seed: u128 = random();
    println!("seed {}", seed);
    let mut refi = Llrb::new_lsm("test-llrb");

    let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");

    let (n_ops, key_max) = random_ops_keys(seed, 60_000, 20_000);
    let n_ops = n_ops + 1;
    random_llrb(n_ops, key_max, seed, &mut llrb, &mut refi);
    let (name, delta_ok) = ("test_lsm_iter_versions2-1", true);
    let disk1 = {
        let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
        let iter = Box::new(SkipScan::new(&*llrb, within));
        random_robt(name, seed, delta_ok, iter)
    };
    let d1_seqno = disk1.to_seqno();
    println!(
        "disk1 n_ops:{} key_max:{} seqno:{}",
        n_ops, key_max, d1_seqno
    );

    let (n_ops, key_max) = random_ops_keys(seed, 120_000, 40_000);
    let n_ops = n_ops + 1;
    random_llrb(n_ops, key_max, seed, &mut llrb, &mut refi);
    let (name, delta_ok) = ("test_lsm_iter_versions2-2", true);
    let disk2 = {
        let within = (Bound::Excluded(d1_seqno), Bound::<u64>::Unbounded);
        let iter = Box::new(SkipScan::new(&*llrb, within));
        random_robt(name, seed, delta_ok, iter)
    };
    let d2_seqno = disk2.to_seqno();
    println!(
        "disk2 n_ops:{} key_max:{} d1_seqno:{}",
        n_ops, key_max, d2_seqno
    );

    let (n_ops, key_max) = random_ops_keys(seed, 200_000, 60_000);
    let mut mvcc: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc");
    mvcc.set_seqno(d2_seqno);
    random_mvcc(n_ops, key_max, seed, mvcc.as_mut(), &mut refi);
    println!("mvcc n_ops: {} key_max: {}", n_ops, key_max);

    let seqno = mvcc.to_seqno();
    let w = mvcc.to_writer().unwrap();
    let r = mvcc.to_reader().unwrap();
    let t_handle = {
        let (_, key_max) = random_ops_keys(seed, 400_000, 400_000);
        let n_ops = 400_000;
        thread::spawn(move || concurrent_write(n_ops, key_max, seed, r, w))
    };

    // println!("start verification mvcc seqno {}", seqno);
    let within = (Bound::Excluded(d2_seqno), Bound::Included(seqno));
    let revr = false;
    let yiter = y_iter_versions(
        Box::new(SkipScan::new(&*mvcc, within)),
        y_iter_versions(
            disk2.iter_with_versions().unwrap(),
            disk1.iter_with_versions().unwrap(),
            revr,
        ),
        revr,
    );
    let entries1: Vec<Result<Entry<i64, i64>>> = refi.iter().unwrap().collect();
    let entries2: Vec<Result<Entry<i64, i64>>> = yiter.collect();

    assert_eq!(entries1.len(), entries2.len());
    let _start = std::time::SystemTime::now();
    for (entry, e) in entries1.into_iter().zip(entries2.into_iter()) {
        let (entry, e) = (entry.unwrap(), e.unwrap());
        let key = entry.to_key();
        // println!("entry key {}", key);

        // TODO
        // let (a, z) = (Bound::Unbounded, Bound::Included(seqno));
        // let e = e.filter_within(a, z).unwrap();
        assert_eq!(entry.to_key(), e.to_key());
        assert_eq!(entry.to_seqno(), e.to_seqno(), "for key {}", key,);
        assert_eq!(entry.is_deleted(), e.is_deleted(), "for key {}", key);
        assert_eq!(entry.to_native_value(), e.to_native_value(), "key {}", key);
        assert_eq!(entry.as_deltas().len(), e.as_deltas().len());
        let iter = entry.to_deltas().into_iter().zip(e.to_deltas().into_iter());
        for (x, y) in iter {
            assert_eq!(x.to_seqno(), y.to_seqno());
            assert_eq!(x.is_deleted(), y.is_deleted());
            let (m, n) = (entry.to_native_value(), e.to_native_value());
            assert_eq!(m, n, "key {}", key);
        }
    }
    // println!("get elapsed {:?}", _start.elapsed().unwrap().as_nanos());
    t_handle.join().unwrap();
}

#[test]
fn test_lsm_range1() {
    let seed: u128 = random();
    println!("seed {}", seed);
    let mut refi = Llrb::new_lsm("test-llrb");

    let (n_ops, key_max) = random_ops_keys(seed, 60, 20);
    println!("mvcc1 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc1: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc1");
    random_mvcc(n_ops, key_max, seed, &mut mvcc1, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 600, 200);
    println!("mvcc2 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc2: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc2");
    mvcc2.set_seqno(mvcc1.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc2, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 6_000, 2_000);
    println!("mvcc3 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc3: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc3");
    mvcc3.set_seqno(mvcc2.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc3, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 60_000, 20_000);
    println!("mvcc4 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc4: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc4");
    mvcc4.set_seqno(mvcc3.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc4, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 600_000, 200_000);
    println!("mvcc5 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc5: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc5");
    mvcc5.set_seqno(mvcc4.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc5, &mut refi);

    let seqno = mvcc5.to_seqno();
    let r = (Bound::<i64>::Unbounded, Bound::<i64>::Unbounded);
    let key_max = refi.reverse(r).unwrap().next().unwrap().unwrap().to_key();
    let key_max = (key_max as usize) + 10;

    for _i in 0..1000 {
        let r = random_low_high(key_max);
        let revr = false;
        let yiter = y_iter(
            mvcc5.range(r.clone()).unwrap(),
            y_iter(
                mvcc4.range(r.clone()).unwrap(),
                y_iter(
                    mvcc3.range(r.clone()).unwrap(),
                    y_iter(
                        mvcc2.range(r.clone()).unwrap(),
                        mvcc1.range(r.clone()).unwrap(),
                        revr,
                    ),
                    revr,
                ),
                revr,
            ),
            revr,
        );
        let iter = refi.range(r.clone()).unwrap();
        let entries1: Vec<Result<Entry<i64, i64>>> = iter.collect();
        let entries2: Vec<Result<Entry<i64, i64>>> = yiter.collect();

        assert_eq!(entries1.len(), entries2.len());
        for (entry, e) in entries1.into_iter().zip(entries2.into_iter()) {
            let (entry, e) = (entry.unwrap(), e.unwrap());
            let key = entry.to_key();

            let (a, z) = (Bound::Unbounded, Bound::Included(seqno));
            let e = e.filter_within(a, z).unwrap();
            assert_eq!(entry.to_key(), e.to_key());
            assert_eq!(entry.to_seqno(), e.to_seqno(), "for key {}", key,);
            assert_eq!(entry.is_deleted(), e.is_deleted(), "for key {}", key);
            let (v1, v2) = (entry.to_native_value(), e.to_native_value());
            assert_eq!(v1, v2, "key {}", key);
        }
    }
}

#[test]
fn test_lsm_range2() {
    // test case using 2 robt version and 1 mvcc versions
    let seed: u128 = random();
    //let seed: u128 = 99443758465951354559679348532807295713;
    println!("seed {}", seed);
    let mut refi = Llrb::new_lsm("test-llrb");

    let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");

    let (n_ops, key_max) = random_ops_keys(seed, 60_000, 20_000);
    let n_ops = n_ops + 1;
    random_llrb(n_ops, key_max, seed, &mut llrb, &mut refi);
    let (name, delta_ok) = ("test_lsm_range2-1", false);
    let disk1 = {
        let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
        let iter = Box::new(SkipScan::new(&*llrb, within));
        random_robt(name, seed, delta_ok, iter)
    };
    let d1_seqno = disk1.to_seqno();
    println!(
        "disk1 n_ops:{} key_max:{} seqno:{}",
        n_ops, key_max, d1_seqno
    );

    let (n_ops, key_max) = random_ops_keys(seed, 120_000, 40_000);
    let n_ops = n_ops + 1;
    random_llrb(n_ops, key_max, seed, &mut llrb, &mut refi);
    let (name, delta_ok) = ("test_lsm_range2-2", false);
    let disk2 = {
        let within = (Bound::Excluded(d1_seqno), Bound::<u64>::Unbounded);
        let iter = Box::new(SkipScan::new(&*llrb, within));
        random_robt(name, seed, delta_ok, iter)
    };
    let d2_seqno = disk2.to_seqno();
    println!(
        "disk2 n_ops:{} key_max:{} seqno: {}",
        n_ops, key_max, d2_seqno
    );

    let (n_ops, key_max) = random_ops_keys(seed, 200_000, 60_000);
    let mut mvcc: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc");
    mvcc.set_seqno(d2_seqno);
    random_mvcc(n_ops, key_max, seed, mvcc.as_mut(), &mut refi);
    println!("mvcc n_ops: {} key_max: {}", n_ops, key_max);

    let seqno = mvcc.to_seqno();
    let w = mvcc.to_writer().unwrap();
    let r = mvcc.to_reader().unwrap();
    let t_handle = {
        let (_, key_max) = random_ops_keys(seed, 400_000, 400_000);
        let n_ops = 400_000;
        thread::spawn(move || concurrent_write(n_ops, key_max, seed, r, w))
    };

    let r = (Bound::<i64>::Unbounded, Bound::<i64>::Unbounded);
    let key_max = refi.reverse(r).unwrap().next().unwrap().unwrap().to_key();
    let key_max = (key_max as usize) + 10;

    // println!("start verification mvcc seqno {}", seqno);
    for _i in 0..1000 {
        let r = random_low_high(key_max);
        let revr = false;
        // println!("range bound {:?}", r);
        let yiter = y_iter_versions(
            mvcc.range(r.clone()).unwrap(),
            y_iter(
                disk2.range(r.clone()).unwrap(),
                disk1.range(r.clone()).unwrap(),
                revr,
            ),
            revr,
        );
        let iter = refi.range(r.clone()).unwrap();
        let entries1: Vec<Result<Entry<i64, i64>>> = iter.collect();
        let entries2: Vec<Entry<i64, i64>> = yiter
            .filter_map(|e| {
                let (a, z) = (Bound::Unbounded, Bound::Included(seqno));
                e.unwrap().filter_within(a, z)
            })
            .collect();

        assert_eq!(entries1.len(), entries2.len());
        let _start = std::time::SystemTime::now();
        for (entry, e) in entries1.into_iter().zip(entries2.into_iter()) {
            let entry = entry.unwrap();
            let key = entry.to_key();
            // println!("verify key {}", key);

            assert_eq!(entry.to_key(), e.to_key());
            assert_eq!(entry.to_seqno(), e.to_seqno(), "for key {}", key,);
            assert_eq!(entry.is_deleted(), e.is_deleted(), "for key {}", key);
            let (v1, v2) = (entry.to_native_value(), e.to_native_value());
            assert_eq!(v1, v2, "key {}", key);
        }
    }
    // println!("get elapsed {:?}", _start.elapsed().unwrap().as_nanos());
    t_handle.join().unwrap();
}

#[test]
#[ignore]
fn test_lsm_range_versions1() {
    // test case using 5 mvcc versions
    let seed: u128 = random();
    //let seed: u128 = 165139395464580006058585702679737837028;
    println!("seed {}", seed);
    let mut refi = Llrb::new_lsm("test-llrb");

    let (n_ops, key_max) = random_ops_keys(seed, 60, 20);
    println!("mvcc1 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc1: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc1");
    random_mvcc(n_ops, key_max, seed, &mut mvcc1, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 600, 200);
    println!("mvcc2 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc2: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc2");
    mvcc2.set_seqno(mvcc1.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc2, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 6_000, 2_000);
    println!("mvcc3 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc3: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc3");
    mvcc3.set_seqno(mvcc2.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc3, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 60_000, 20_000);
    println!("mvcc4 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc4: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc4");
    mvcc4.set_seqno(mvcc3.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc4, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 600_000, 200_000);
    let n_ops = n_ops + 1;
    println!("mvcc5 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc5: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc5");
    mvcc5.set_seqno(mvcc4.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc5, &mut refi);

    let seqno = mvcc5.to_seqno();
    let r = (Bound::<i64>::Unbounded, Bound::<i64>::Unbounded);
    let key_max = refi.reverse(r).unwrap().next().unwrap().unwrap().to_key();
    let key_max = (key_max as usize) + 10;

    for _i in 0..1000 {
        let r = random_low_high(key_max);
        let revr = false;
        let yiter = y_iter_versions(
            mvcc5.range_with_versions(r.clone()).unwrap(),
            y_iter_versions(
                mvcc4.range_with_versions(r.clone()).unwrap(),
                y_iter_versions(
                    mvcc3.range_with_versions(r.clone()).unwrap(),
                    y_iter_versions(
                        mvcc2.range_with_versions(r.clone()).unwrap(),
                        mvcc1.range_with_versions(r.clone()).unwrap(),
                        revr,
                    ),
                    revr,
                ),
                revr,
            ),
            revr,
        );
        let iter = refi.range(r.clone()).unwrap();
        let entries1: Vec<Result<Entry<i64, i64>>> = iter.collect();
        let entries2: Vec<Result<Entry<i64, i64>>> = yiter.collect();

        assert_eq!(entries1.len(), entries2.len());
        for (entry, e) in entries1.into_iter().zip(entries2.into_iter()) {
            let (entry, e) = (entry.unwrap(), e.unwrap());
            let key = entry.to_key();
            // println!("entry key {}", key);

            let (a, z) = (Bound::Unbounded, Bound::Included(seqno));
            let e = e.filter_within(a, z).unwrap();
            assert_eq!(entry.to_key(), e.to_key());
            assert_eq!(entry.to_seqno(), e.to_seqno(), "for key {}", key,);
            assert_eq!(entry.is_deleted(), e.is_deleted(), "for key {}", key);
            let (v1, v2) = (entry.to_native_value(), e.to_native_value());
            assert_eq!(v1, v2, "key {}", key);
            assert_eq!(entry.as_deltas().len(), e.as_deltas().len());
            let iter = entry.as_deltas().iter().zip(e.as_deltas().iter());
            for (x, y) in iter {
                // println!("x-seqno {} y-seqno {}", x.to_seqno(), y.to_seqno());
                assert_eq!(x.to_seqno(), y.to_seqno());
                assert_eq!(x.is_deleted(), y.is_deleted());
                let (m, n) = (entry.to_native_value(), e.to_native_value());
                assert_eq!(m, n, "key {}", key);
            }
        }
    }
}

#[test]
#[ignore]
fn test_lsm_range_versions2() {
    // test case using 2 robt version and 1 mvcc versions
    let seed: u128 = random();
    println!("seed {}", seed);
    let mut refi = Llrb::new_lsm("test-llrb");

    let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");

    let (n_ops, key_max) = random_ops_keys(seed, 60_000, 20_000);
    let n_ops = n_ops + 1;
    random_llrb(n_ops, key_max, seed, &mut llrb, &mut refi);
    let (name, delta_ok) = ("test_lsm_range_versions2-1", true);
    let disk1 = {
        let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
        let iter = Box::new(SkipScan::new(&*llrb, within));
        random_robt(name, seed, delta_ok, iter)
    };
    let d1_seqno = disk1.to_seqno();
    println!(
        "disk1 n_ops:{} key_max:{} seqno:{}",
        n_ops, key_max, d1_seqno
    );

    let (n_ops, key_max) = random_ops_keys(seed, 120_000, 40_000);
    let n_ops = n_ops + 1;
    random_llrb(n_ops, key_max, seed, &mut llrb, &mut refi);
    let (name, delta_ok) = ("test_lsm_range_versions2-2", true);
    let disk2 = {
        let within = (Bound::Excluded(d1_seqno), Bound::<u64>::Unbounded);
        let iter = Box::new(SkipScan::new(&*llrb, within));
        random_robt(name, seed, delta_ok, iter)
    };
    let d2_seqno = disk2.to_seqno();
    println!(
        "disk2 n_ops:{} key_max:{} seqno:{}",
        n_ops, key_max, d2_seqno
    );

    let (n_ops, key_max) = random_ops_keys(seed, 200_000, 60_000);
    let mut mvcc: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc");
    mvcc.set_seqno(d2_seqno);
    random_mvcc(n_ops, key_max, seed, mvcc.as_mut(), &mut refi);
    println!("mvcc n_ops: {} key_max: {}", n_ops, key_max);

    let seqno = mvcc.to_seqno();
    let w = mvcc.to_writer().unwrap();
    let r = mvcc.to_reader().unwrap();
    let t_handle = {
        let (_, key_max) = random_ops_keys(seed, 400_000, 400_000);
        let n_ops = 400_000;
        thread::spawn(move || concurrent_write(n_ops, key_max, seed, r, w))
    };

    let r = (Bound::<i64>::Unbounded, Bound::<i64>::Unbounded);
    let key_max = refi.reverse(r).unwrap().next().unwrap().unwrap().to_key();
    let key_max = (key_max as usize) + 10;

    for _i in 0..1000 {
        let r = random_low_high(key_max);
        let revr = false;
        // println!("start verification mvcc seqno {}", seqno);
        let yiter = y_iter_versions(
            mvcc.range_with_versions(r.clone()).unwrap(),
            y_iter_versions(
                disk2.range_with_versions(r.clone()).unwrap(),
                disk1.range_with_versions(r.clone()).unwrap(),
                revr,
            ),
            revr,
        );
        let iter = refi.range(r.clone()).unwrap();
        let entries1: Vec<Result<Entry<i64, i64>>> = iter.collect();
        let entries2: Vec<Entry<i64, i64>> = yiter
            .filter_map(|e| {
                let (a, z) = (Bound::Unbounded, Bound::Included(seqno));
                e.unwrap().filter_within(a, z)
            })
            .collect();

        let _start = std::time::SystemTime::now();
        assert_eq!(entries1.len(), entries2.len());
        for (entry, e) in entries1.into_iter().zip(entries2.into_iter()) {
            let entry = entry.unwrap();
            let key = entry.to_key();
            // println!("entry key {}", key);

            assert_eq!(entry.to_key(), e.to_key());
            assert_eq!(entry.to_seqno(), e.to_seqno(), "for key {}", key,);
            assert_eq!(entry.is_deleted(), e.is_deleted(), "for key {}", key);
            let (v1, v2) = (entry.to_native_value(), e.to_native_value());
            assert_eq!(v1, v2, "key {}", key);
            assert_eq!(entry.as_deltas().len(), e.as_deltas().len());
            let iter = entry.as_deltas().iter().zip(e.as_deltas().iter());
            for (x, y) in iter {
                assert_eq!(x.to_seqno(), y.to_seqno());
                assert_eq!(x.is_deleted(), y.is_deleted());
                let (m, n) = (entry.to_native_value(), e.to_native_value());
                assert_eq!(m, n, "key {}", key);
            }
        }
    }
    // println!("get elapsed {:?}", _start.elapsed().unwrap().as_nanos());
    t_handle.join().unwrap();
}

#[test]
fn test_lsm_reverse1() {
    let seed: u128 = random();
    //let seed: u128 = 220743249322234861290250598912930125896;
    println!("seed {}", seed);
    let mut refi = Llrb::new_lsm("test-llrb");

    let (n_ops, key_max) = random_ops_keys(seed, 60, 20);
    println!("mvcc1 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc1: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc1");
    random_mvcc(n_ops, key_max, seed, &mut mvcc1, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 600, 200);
    println!("mvcc2 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc2: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc2");
    mvcc2.set_seqno(mvcc1.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc2, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 6_000, 2_000);
    println!("mvcc3 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc3: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc3");
    mvcc3.set_seqno(mvcc2.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc3, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 60_000, 20_000);
    println!("mvcc4 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc4: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc4");
    mvcc4.set_seqno(mvcc3.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc4, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 600_000, 200_000);
    println!("mvcc5 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc5: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc5");
    mvcc5.set_seqno(mvcc4.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc5, &mut refi);

    let seqno = mvcc5.to_seqno();
    let r = (Bound::<i64>::Unbounded, Bound::<i64>::Unbounded);
    let key_max = refi.reverse(r).unwrap().next().unwrap().unwrap().to_key();
    let key_max = (key_max as usize) + 10;

    for _i in 0..1000 {
        let r = random_low_high(key_max);
        let revr = true;
        // println!("range bound: {:?}", r);
        let yiter = y_iter(
            mvcc5.reverse(r.clone()).unwrap(),
            y_iter(
                mvcc4.reverse(r.clone()).unwrap(),
                y_iter(
                    mvcc3.reverse(r.clone()).unwrap(),
                    y_iter(
                        mvcc2.reverse(r.clone()).unwrap(),
                        mvcc1.reverse(r.clone()).unwrap(),
                        revr,
                    ),
                    revr,
                ),
                revr,
            ),
            revr,
        );
        let iter = refi.reverse(r.clone()).unwrap();
        let entries1: Vec<Result<Entry<i64, i64>>> = iter.collect();
        let entries2: Vec<Result<Entry<i64, i64>>> = yiter.collect();

        assert_eq!(entries1.len(), entries2.len());
        for (entry, e) in entries1.into_iter().zip(entries2.into_iter()) {
            let (entry, e) = (entry.unwrap(), e.unwrap());
            let key = entry.to_key();
            // println!("verify key {}", key);

            let (a, z) = (Bound::Unbounded, Bound::Included(seqno));
            let e = e.filter_within(a, z).unwrap();
            assert_eq!(entry.to_key(), e.to_key());
            assert_eq!(entry.to_seqno(), e.to_seqno(), "for key {}", key,);
            assert_eq!(entry.is_deleted(), e.is_deleted(), "for key {}", key);
            let (v1, v2) = (entry.to_native_value(), e.to_native_value());
            assert_eq!(v1, v2, "key {}", key);
        }
    }
}

#[test]
fn test_lsm_reverse2() {
    // test case using 2 robt version and 1 mvcc versions
    let seed: u128 = random();
    println!("seed {}", seed);
    let mut refi = Llrb::new_lsm("test-llrb");

    let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");

    let (n_ops, key_max) = random_ops_keys(seed, 60_000, 20_000);
    let n_ops = n_ops + 1;
    random_llrb(n_ops, key_max, seed, &mut llrb, &mut refi);
    let (name, delta_ok) = ("test_lsm_reverse2-1", false);
    let disk1 = {
        let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
        let iter = Box::new(SkipScan::new(&*llrb, within));
        random_robt(name, seed, delta_ok, iter)
    };
    let d1_seqno = disk1.to_seqno();
    println!(
        "disk1 n_ops:{} key_max:{} seqno:{}",
        n_ops, key_max, d1_seqno
    );

    let (n_ops, key_max) = random_ops_keys(seed, 120_000, 40_000);
    let n_ops = n_ops + 1;
    random_llrb(n_ops, key_max, seed, &mut llrb, &mut refi);
    let (name, delta_ok) = ("test_lsm_reverse2-2", false);
    let disk2 = {
        let within = (Bound::Excluded(d1_seqno), Bound::<u64>::Unbounded);
        let iter = Box::new(SkipScan::new(&*llrb, within));
        random_robt(name, seed, delta_ok, iter)
    };
    let d2_seqno = disk2.to_seqno();
    println!(
        "disk2 n_ops:{} key_max:{} seqno:{}",
        n_ops, key_max, d2_seqno
    );

    let (n_ops, key_max) = random_ops_keys(seed, 200_000, 60_000);
    let mut mvcc: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc");
    mvcc.set_seqno(d2_seqno);
    random_mvcc(n_ops, key_max, seed, mvcc.as_mut(), &mut refi);
    println!("mvcc n_ops:{} key_max:{}", n_ops, key_max);

    let seqno = mvcc.to_seqno();
    let w = mvcc.to_writer().unwrap();
    let r = mvcc.to_reader().unwrap();
    let t_handle = {
        let (_, key_max) = random_ops_keys(seed, 400_000, 400_000);
        let n_ops = 400_000;
        thread::spawn(move || concurrent_write(n_ops, key_max, seed, r, w))
    };

    let r = (Bound::<i64>::Unbounded, Bound::<i64>::Unbounded);
    let key_max = refi.reverse(r).unwrap().next().unwrap().unwrap().to_key();
    let key_max = (key_max as usize) + 10;

    for _i in 0..1000 {
        let r = random_low_high(key_max);
        let revr = true;
        // println!("start verification mvcc seqno {}", seqno);
        let yiter = y_iter_versions(
            mvcc.reverse(r.clone()).unwrap(),
            y_iter(
                disk2.reverse(r.clone()).unwrap(),
                disk1.reverse(r.clone()).unwrap(),
                revr,
            ),
            revr,
        );
        let iter = refi.reverse(r.clone()).unwrap();
        let entries1: Vec<Result<Entry<i64, i64>>> = iter.collect();
        let entries2: Vec<Entry<i64, i64>> = yiter
            .filter_map(|e| {
                let (a, z) = (Bound::Unbounded, Bound::Included(seqno));
                e.unwrap().filter_within(a, z)
            })
            .collect();

        let _start = std::time::SystemTime::now();
        assert_eq!(entries1.len(), entries2.len());
        for (entry, e) in entries1.into_iter().zip(entries2.into_iter()) {
            let entry = entry.unwrap();
            let key = entry.to_key();

            assert_eq!(entry.to_key(), e.to_key());
            assert_eq!(entry.to_seqno(), e.to_seqno(), "for key {}", key,);
            assert_eq!(entry.is_deleted(), e.is_deleted(), "for key {}", key);
            let (v1, v2) = (entry.to_native_value(), e.to_native_value());
            assert_eq!(v1, v2, "key {}", key);
        }
    }
    // println!("get elapsed {:?}", _start.elapsed().unwrap().as_nanos());
    t_handle.join().unwrap();
}

#[test]
#[ignore]
fn test_lsm_reverse_versions1() {
    // test case using 5 mvcc versions
    let seed: u128 = random();
    println!("seed {}", seed);
    let mut refi = Llrb::new_lsm("test-llrb");

    let (n_ops, key_max) = random_ops_keys(seed, 60, 20);
    println!("mvcc1 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc1: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc1");
    random_mvcc(n_ops, key_max, seed, &mut mvcc1, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 600, 200);
    println!("mvcc2 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc2: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc2");
    mvcc2.set_seqno(mvcc1.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc2, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 6_000, 2_000);
    println!("mvcc3 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc3: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc3");
    mvcc3.set_seqno(mvcc2.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc3, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 60_000, 20_000);
    println!("mvcc4 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc4: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc4");
    mvcc4.set_seqno(mvcc3.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc4, &mut refi);

    let (n_ops, key_max) = random_ops_keys(seed, 600_000, 200_000);
    let n_ops = n_ops + 1;
    println!("mvcc5 n_ops: {} key_max: {}", n_ops, key_max);
    let mut mvcc5: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc5");
    mvcc5.set_seqno(mvcc4.to_seqno());
    random_mvcc(n_ops, key_max, seed, &mut mvcc5, &mut refi);

    let seqno = mvcc5.to_seqno();
    let r = (Bound::<i64>::Unbounded, Bound::<i64>::Unbounded);
    let key_max = refi.reverse(r).unwrap().next().unwrap().unwrap().to_key();
    let key_max = (key_max as usize) + 10;

    for _i in 0..1000 {
        let r = random_low_high(key_max);
        let revr = true;
        let yiter = y_iter_versions(
            mvcc5.reverse_with_versions(r.clone()).unwrap(),
            y_iter_versions(
                mvcc4.reverse_with_versions(r.clone()).unwrap(),
                y_iter_versions(
                    mvcc3.reverse_with_versions(r.clone()).unwrap(),
                    y_iter_versions(
                        mvcc2.reverse_with_versions(r.clone()).unwrap(),
                        mvcc1.reverse_with_versions(r.clone()).unwrap(),
                        revr,
                    ),
                    revr,
                ),
                revr,
            ),
            revr,
        );
        let iter = refi.reverse_with_versions(r.clone()).unwrap();
        let entries1: Vec<Result<Entry<i64, i64>>> = iter.collect();
        let entries2: Vec<Result<Entry<i64, i64>>> = yiter.collect();

        assert_eq!(entries1.len(), entries2.len());
        for (entry, e) in entries1.into_iter().zip(entries2.into_iter()) {
            let (entry, e) = (entry.unwrap(), e.unwrap());
            let key = entry.to_key();
            // println!("entry key {}", key);

            let (a, z) = (Bound::Unbounded, Bound::Included(seqno));
            let e = e.filter_within(a, z).unwrap();
            assert_eq!(entry.to_key(), e.to_key());
            assert_eq!(entry.to_seqno(), e.to_seqno(), "for key {}", key,);
            assert_eq!(entry.is_deleted(), e.is_deleted(), "for key {}", key);
            let (v1, v2) = (entry.to_native_value(), e.to_native_value());
            assert_eq!(v1, v2, "key {}", key);
            assert_eq!(entry.as_deltas().len(), e.as_deltas().len());
            let iter = entry.as_deltas().iter().zip(e.as_deltas().iter());
            for (x, y) in iter {
                // println!("x-seqno {} y-seqno {}", x.to_seqno(), y.to_seqno());
                assert_eq!(x.to_seqno(), y.to_seqno());
                assert_eq!(x.is_deleted(), y.is_deleted());
                let (m, n) = (entry.to_native_value(), e.to_native_value());
                assert_eq!(m, n, "key {}", key);
            }
        }
    }
}

#[test]
#[ignore]
fn test_lsm_reverse_versions2() {
    // test case using 2 robt version and 1 mvcc versions
    let seed: u128 = random();
    // let seed: u128 = 215456859976182285399953190877559503919;
    println!("seed {}", seed);
    let mut refi = Llrb::new_lsm("test-llrb");

    let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");

    let (n_ops, key_max) = random_ops_keys(seed, 60_000, 20_000);
    let n_ops = n_ops + 1;
    random_llrb(n_ops, key_max, seed, &mut llrb, &mut refi);
    let (name, delta_ok) = ("test_lsm_reverse_versions2-1", true);
    let disk1 = {
        let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
        let iter = Box::new(SkipScan::new(&*llrb, within));
        random_robt(name, seed, delta_ok, iter)
    };
    let d1_seqno = disk1.to_seqno();
    println!(
        "disk1 n_ops:{} key_max:{} seqno:{}",
        n_ops, key_max, d1_seqno
    );

    let (n_ops, key_max) = random_ops_keys(seed, 120_000, 40_000);
    let n_ops = n_ops + 1;
    random_llrb(n_ops, key_max, seed, &mut llrb, &mut refi);
    let (name, delta_ok) = ("test_lsm_reverse_versions2-2", true);
    let disk2 = {
        let within = (Bound::Excluded(d1_seqno), Bound::<u64>::Unbounded);
        let iter = Box::new(SkipScan::new(&*llrb, within));
        random_robt(name, seed, delta_ok, iter)
    };
    let d2_seqno = disk2.to_seqno();
    println!(
        "disk2 n_ops:{} key_max:{} seqno:{}",
        n_ops, key_max, d2_seqno
    );

    let (n_ops, key_max) = random_ops_keys(seed, 200_000, 60_000);
    let mut mvcc: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc");
    mvcc.set_seqno(d2_seqno);
    random_mvcc(n_ops, key_max, seed, mvcc.as_mut(), &mut refi);
    println!("mvcc n_ops: {} key_max: {}", n_ops, key_max);

    let seqno = mvcc.to_seqno();
    let w = mvcc.to_writer().unwrap();
    let r = mvcc.to_reader().unwrap();
    let t_handle = {
        let (_, key_max) = random_ops_keys(seed, 400_000, 400_000);
        let n_ops = 400_000;
        thread::spawn(move || concurrent_write(n_ops, key_max, seed, r, w))
    };

    let r = (Bound::<i64>::Unbounded, Bound::<i64>::Unbounded);
    let key_max = refi.reverse(r).unwrap().next().unwrap().unwrap().to_key();
    let key_max = (key_max as usize) + 10;

    println!("start verification mvcc seqno {}", seqno);
    for _i in 0..1000 {
        let r = random_low_high(key_max);
        // let r = (Bound::Unbounded, Bound::Included(110));
        // println!("range bound {:?}", r);
        let revr = true;
        let yiter = y_iter_versions(
            mvcc.reverse_with_versions(r.clone()).unwrap(),
            y_iter_versions(
                disk2.reverse_with_versions(r.clone()).unwrap(),
                disk1.reverse_with_versions(r.clone()).unwrap(),
                revr,
            ),
            revr,
        );
        let iter = refi.reverse_with_versions(r.clone()).unwrap();
        let entries1: Vec<Result<Entry<i64, i64>>> = iter.collect();
        let entries2: Vec<Entry<i64, i64>> = yiter
            .filter_map(|e| {
                let (a, z) = (Bound::Unbounded, Bound::Included(seqno));
                e.unwrap().filter_within(a, z)
            })
            .collect();

        let _start = std::time::SystemTime::now();
        assert_eq!(entries1.len(), entries2.len());
        for (entry, e) in entries1.into_iter().zip(entries2.into_iter()) {
            let entry = entry.unwrap();
            let key = entry.to_key();
            // println!("entry key {}", key);

            assert_eq!(entry.to_key(), e.to_key());
            assert_eq!(entry.to_seqno(), e.to_seqno(), "for key {}", key,);
            assert_eq!(entry.is_deleted(), e.is_deleted(), "for key {}", key);
            let (v1, v2) = (entry.to_native_value(), e.to_native_value());
            assert_eq!(v1, v2, "key {}", key);
            assert_eq!(entry.as_deltas().len(), e.as_deltas().len());
            let iter = entry.as_deltas().iter().zip(e.as_deltas().iter());
            for (x, y) in iter {
                assert_eq!(x.to_seqno(), y.to_seqno());
                assert_eq!(x.is_deleted(), y.is_deleted());
                let (m, n) = (entry.to_native_value(), e.to_native_value());
                assert_eq!(m, n, "key {}", key);
            }
        }
    }
    // println!("get elapsed {:?}", _start.elapsed().unwrap().as_nanos());
    t_handle.join().unwrap();
}

fn random_llrb(
    n_ops: i64,
    key_max: i64,
    seed: u128,
    llrb: &mut Llrb<i64, i64>,
    refi: &mut Llrb<i64, i64>, // reference index
) {
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
    for _i in 0..n_ops {
        let key = (rng.gen::<i64>() % key_max).abs();
        let op = rng.gen::<usize>() % 3;
        //println!(
        //    "llrb key {} {} {} {}",
        //    key,
        //    llrb.to_seqno(),
        //    refi.to_seqno(),
        //    op
        //);
        match op {
            0 => {
                let value: i64 = rng.gen();
                llrb.set(key, value).unwrap();
                refi.set(key, value).unwrap();
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
                {
                    let cas = match refi.get(&key) {
                        Err(Error::KeyNotFound) => 0,
                        Err(_err) => unreachable!(),
                        Ok(e) => e.to_seqno(),
                    };
                    refi.set_cas(key, value, cas).unwrap();
                }
            }
            2 => {
                llrb.delete(&key).unwrap();
                refi.delete(&key).unwrap();
            }
            _ => unreachable!(),
        }
    }
    println!("random_llrb {}", llrb.to_seqno());
}

fn random_mvcc(
    n_ops: i64,
    key_max: i64,
    seed: u128,
    mvcc: &mut Mvcc<i64, i64>,
    refi: &mut Llrb<i64, i64>, // reference index
) {
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
    for _i in 0..n_ops {
        let key = (rng.gen::<i64>() % key_max).abs();
        let op = rng.gen::<usize>() % 3;
        //println!(
        //    "mvcc key {} {} {} {}",
        //    key,
        //    mvcc.to_seqno(),
        //    refi.to_seqno(),
        //    op
        //);
        match op {
            0 => {
                let value: i64 = rng.gen();
                mvcc.set(key, value).unwrap();
                refi.set(key, value).unwrap();
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
                {
                    let cas = match refi.get(&key) {
                        Err(Error::KeyNotFound) => 0,
                        Err(_err) => unreachable!(),
                        Ok(e) => e.to_seqno(),
                    };
                    refi.set_cas(key, value, cas).unwrap();
                }
            }
            2 => {
                mvcc.delete(&key).unwrap();
                refi.delete(&key).unwrap();
            }
            _ => unreachable!(),
        }
    }
}

fn random_robt(
    name: &str,
    seed: u128,
    delta_ok: bool,
    iter: IndexIter<i64, i64>,
) -> robt::Snapshot<i64, i64> {
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
    let dir = {
        let mut dir = std::env::temp_dir();
        dir.push(name);
        dir.to_str().unwrap().to_string()
    };
    let mut config: robt::Config = Default::default();
    config.delta_ok = delta_ok;
    config.value_in_vlog = rng.gen();
    let b = robt::Builder::initial(&dir, "random_robt", config.clone()).unwrap();
    let app_meta = "heloo world".to_string();
    b.build(iter, app_meta.as_bytes().to_vec()).unwrap();

    robt::Snapshot::<i64, i64>::open(&dir, "random_robt").unwrap()
}

fn concurrent_write(
    n_ops: i64,
    key_max: i64,
    seed: u128,
    r: MvccReader<i64, i64>,
    mut w: MvccWriter<i64, i64>,
) {
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
    let _start = std::time::SystemTime::now();
    for _i in 0..n_ops {
        let key = (rng.gen::<i64>() % key_max).abs();
        let op = rng.gen::<usize>() % 3;
        // println!("concurrent key {} {}", key, op);
        match op {
            0 => {
                let value: i64 = rng.gen();
                match w.set(key, value) {
                    Err(err) => panic!("set err: {:?}", err),
                    _ => (),
                }
            }
            1 => {
                let value: i64 = rng.gen();
                let cas = match r.get(&key) {
                    Err(Error::KeyNotFound) => 0,
                    Err(_err) => unreachable!(),
                    Ok(e) => e.to_seqno(),
                };
                match w.set_cas(key, value, cas) {
                    Err(err) => panic!("set_cas cas:{} err:{:?}", cas, err),
                    _ => (),
                }
            }
            2 => {
                w.delete(&key).unwrap();
            }
            _ => unreachable!(),
        }
    }
    println!(
        "concurrent write elapsed {:?}",
        _start.elapsed().unwrap().as_nanos()
    );
}

fn random_ops_keys(seed: u128, ops_limit: i64, key_limit: i64) -> (i64, i64) {
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let n_ops_set: Vec<i64> = vec![
        0,
        ops_limit / 10,
        ops_limit / 100,
        ops_limit / 1000,
        ops_limit / 10000,
    ];
    let i = rng.gen::<usize>() % (n_ops_set.len() + 1);
    let n_ops = if i == n_ops_set.len() {
        10000 + (rng.gen::<u64>() % (ops_limit as u64))
    } else {
        n_ops_set[i] as u64
    };
    let n_ops = n_ops as i64;

    let max_key_set: Vec<i64> = vec![
        (key_limit / 10) + 1,
        (key_limit / 100) + 1,
        (key_limit / 1000) + 1,
        (key_limit / 10000) + 1,
    ];
    let i: usize = rng.gen::<usize>() % (max_key_set.len() + 1);
    let max_key = if i == max_key_set.len() {
        10000 + (rng.gen::<i64>() % key_limit)
    } else {
        max_key_set[i]
    };
    (n_ops, i64::max(i64::abs(max_key), n_ops / 10) + 1)
}

fn log_entry(e: &Entry<i64, i64>) {
    println!(
        "key: {} value: {:?}, seqno: {}, deleted: {}",
        e.to_key(),
        e.to_native_value(),
        e.to_seqno(),
        e.is_deleted()
    );
    for d in e.as_deltas() {
        println!("seqno: {}, deleted: {}", d.to_seqno(), d.is_deleted());
    }
}

fn random_low_high(size: usize) -> (Bound<i64>, Bound<i64>) {
    let size: u64 = size.try_into().unwrap();
    let low: i64 = (random::<u64>() % size) as i64;
    let high: i64 = (random::<u64>() % size) as i64;
    let low = match random::<u8>() % 3 {
        0 => Bound::Included(low),
        1 => Bound::Excluded(low),
        2 => Bound::Unbounded,
        _ => unreachable!(),
    };
    let high = match random::<u8>() % 3 {
        0 => Bound::Included(high),
        1 => Bound::Excluded(high),
        2 => Bound::Unbounded,
        _ => unreachable!(),
    };
    //println!("low_high {:?} {:?}", low, high);
    (low, high)
}
