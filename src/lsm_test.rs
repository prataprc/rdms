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
    let delta_ok = false;
    let name = "test_lsm_get2-1";
    let disk1 = {
        let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
        let iter = Box::new(SkipScan::new(&*llrb, within));
        random_robt(name, seed, delta_ok, iter)
    };
    println!("disk1 n_ops: {} key_max: {}", n_ops, key_max);

    let (n_ops, key_max) = random_ops_keys(seed, 120_000, 40_000);
    let n_ops = n_ops + 1;
    random_llrb(n_ops, key_max, seed, &mut llrb, &mut refi);
    let delta_ok = false;
    let name = "test_lsm_get2-2";
    let disk2 = {
        let within = (Bound::Excluded(disk1.to_seqno()), Bound::<u64>::Unbounded);
        let iter = Box::new(SkipScan::new(&*llrb, within));
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
        let (n_ops, key_max) = random_ops_keys(seed, 400_000, 400_000);
        println!("concurrent n_ops: {} key_max: {}", n_ops, key_max);
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
    // let seed: u128 = 204391140320403798395535363311690471999;
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
    }
}

#[test]
fn test_lsm_get_versions2() {
    // test case using 1 mvcc version and 2 robt versions
}

#[test]
fn test_lsm_iter() {
    // test case using 5 mvcc versions
    // test case using 1 mvcc version and 2 robt versions
}

#[test]
fn test_lsm_iter_versions() {
    // test case using 5 mvcc versions
    // test case using 1 mvcc version and 2 robt versions
}

#[test]
fn test_lsm_range() {
    // test case using 5 mvcc versions
    // test case using 1 mvcc version and 2 robt versions
}

#[test]
fn test_lsm_range_versions() {
    // test case using 5 mvcc versions
    // test case using 1 mvcc version and 2 robt versions
}

#[test]
fn test_lsm_reverse() {
    // test case using 5 mvcc versions
    // test case using 1 mvcc version and 2 robt versions
}

#[test]
fn test_lsm_reverse_versions() {
    // test case using 5 mvcc versions
    // test case using 1 mvcc version and 2 robt versions
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
                w.set(key, value).unwrap();
            }
            1 => {
                let value: i64 = rng.gen();
                let cas = match r.get(&key) {
                    Err(Error::KeyNotFound) => 0,
                    Err(_err) => unreachable!(),
                    Ok(e) => e.to_seqno(),
                };
                w.set_cas(key, value, cas).unwrap();
            }
            2 => {
                w.delete(&key).unwrap();
            }
            _ => unreachable!(),
        }
    }
    //println!(
    //    "concurrent write elapsed {:?}",
    //    _start.elapsed().unwrap().as_nanos()
    //);
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
