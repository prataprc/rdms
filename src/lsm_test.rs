use std::ops::Bound;
use std::sync::Arc;
use std::thread;

use rand::prelude::random;
use rand::{rngs::SmallRng, Rng, SeedableRng};

use super::*;

use crate::core::{Index, IndexIter, Reader};
use crate::error::Error;
use crate::llrb::Llrb;
use crate::mvcc::{Mvcc, MvccWriter};
use crate::robt;
use crate::scans::SkipScan;

#[test]
#[ignore]
fn test_lsm_get1() {
    // test case using 5 mvcc versions
    let seed: u128 = random();
    let mut refi = Llrb::new_lsm("test-llrb");

    let (n_ops, key_max) = (60, 20);
    let mut mvcc1: Box<Mvcc<i64, i64>> = Mvcc::new_lsm("test-mvcc");
    random_mvcc(n_ops, key_max, seed, &mut mvcc1, &mut refi);

    let (n_ops, key_max) = (600, 200);
    let mut mvcc2 = mvcc1.clone();
    random_mvcc(n_ops, key_max, seed, &mut mvcc2, &mut refi);

    let (n_ops, key_max) = (6_000, 2_000);
    let mut mvcc3 = mvcc2.clone();
    random_mvcc(n_ops, key_max, seed, &mut mvcc3, &mut refi);

    let (n_ops, key_max) = (60_000, 20_000);
    let mut mvcc4 = mvcc3.clone();
    random_mvcc(n_ops, key_max, seed, &mut mvcc4, &mut refi);

    let (n_ops, key_max) = (600_000, 200_000);
    let mut mvcc5 = mvcc4.clone();
    random_mvcc(n_ops, key_max, seed, &mut mvcc5, &mut refi);

    let yget = y_get(
        getter(&*mvcc5),
        y_get(
            getter(&*mvcc4),
            y_get(getter(&*mvcc3), y_get(getter(&*mvcc2), getter(&*mvcc1))),
        ),
    );
    for entry in refi.iter().unwrap() {
        let entry = entry.unwrap();
        let key = entry.to_key();
        let e = yget(&key).unwrap();
        assert_eq!(e.to_seqno(), entry.to_seqno(), "for key {}", key,);
    }
}

#[test]
#[ignore]
fn test_lsm_get2() {
    // test case using 2 robt version and 1 mvcc versions
    let seed: u128 = random();
    let mut refi = Llrb::new_lsm("test-llrb");

    let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
    let (n_ops, key_max) = (60_000, 20_000);
    random_llrb(n_ops, key_max, seed, &mut llrb, &mut refi);
    let delta_ok = false;
    let name = "test_lsm_get2-1";
    let disk1 = {
        let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
        let iter = Box::new(SkipScan::new(&*llrb, within));
        random_robt(name, seed, delta_ok, iter)
    };

    let (n_ops, key_max) = (120_000, 40_000);
    random_llrb(n_ops, key_max, seed, &mut llrb, &mut refi);
    let delta_ok = false;
    let name = "test_lsm_get2-2";
    let disk2 = {
        let within = (Bound::Excluded(disk1.to_seqno()), Bound::<u64>::Unbounded);
        let iter = Box::new(SkipScan::new(&*llrb, within));
        random_robt(name, seed, delta_ok, iter)
    };

    let (n_ops, key_max) = (200_000, 60_000);
    println!("before from_llrb");
    let mut mvcc1: Arc<Box<Mvcc<i64, i64>>> = Arc::new(Mvcc::from_llrb(*llrb));
    println!("after from_llrb, before final mvcc population");
    {
        let mvcc: &mut Box<Mvcc<i64, i64>> = Arc::get_mut(&mut mvcc1).unwrap();
        random_mvcc(n_ops, key_max, seed, &mut **mvcc, &mut refi);
    }
    let seqno = mvcc1.to_seqno();
    let t_handle = {
        let mvcc_w = Arc::get_mut(&mut mvcc1).unwrap().to_writer();
        let mvcc = Arc::clone(&mvcc1);
        let (n_ops, key_max) = (200_000, 60_000);
        thread::spawn(move || {
            concurrent_write(
                n_ops, key_max, seed, mvcc_w, // writer handle
                mvcc,
            )
        })
    };

    println!("start verification mvcc seqno {}", seqno);
    let yget = y_get(getter(&**mvcc1), y_get(getter(&disk2), getter(&disk1)));
    for entry in refi.iter().unwrap() {
        let entry = entry.unwrap();
        let key = entry.to_key();
        let e = yget(&key).unwrap();

        let e1 = mvcc1.get(&key);

        let (a, z) = (Bound::Unbounded, Bound::Included(seqno));
        let e = e.filter_within(a, z).unwrap();
        assert_eq!(entry.to_key(), e.to_key());
        assert_eq!(
            entry.to_seqno(),
            e.to_seqno(),
            "for key {} {:?}",
            key,
            e1.map(|e| e.to_seqno())
        );
        assert_eq!(entry.is_deleted(), e.is_deleted(), "for key {}", key);
        assert_eq!(entry.to_native_value(), e.to_native_value(), "key {}", key);
        assert_eq!(entry.as_deltas().len(), e.as_deltas().len());
    }

    println!("test_lsm_get2 finished get ops");
    t_handle.join().unwrap();
    println!("test_lsm_get2 finished concurrent writes sync");
}

#[test]
fn test_lsm_get_versions() {
    // test case using 5 mvcc versions
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
        let op = rng.gen::<usize>() % 1;
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
            //1 => {
            //    let value: i64 = rng.gen();
            //    {
            //        let cas = match mvcc.get(&key) {
            //            Err(Error::KeyNotFound) => 0,
            //            Err(_err) => unreachable!(),
            //            Ok(e) => e.to_seqno(),
            //        };
            //        mvcc.set_cas(key, value, cas).unwrap();
            //    }
            //    {
            //        let cas = match refi.get(&key) {
            //            Err(Error::KeyNotFound) => 0,
            //            Err(_err) => unreachable!(),
            //            Ok(e) => e.to_seqno(),
            //        };
            //        refi.set_cas(key, value, cas).unwrap();
            //    }
            //}
            //2 => {
            //    mvcc.delete(&key).unwrap();
            //    refi.delete(&key).unwrap();
            //}
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
    mut w: MvccWriter<i64, i64>, // writer handle into mvcc argument.
    mvcc: Arc<Box<Mvcc<i64, i64>>>,
) {
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
    for _i in 0..n_ops {
        let key = (rng.gen::<i64>() % key_max).abs();
        let op = rng.gen::<usize>() % 1;
        // println!("concurrent key {} {} {}", key, mvcc.to_seqno(), op);
        match op {
            0 => {
                let value: i64 = rng.gen();
                w.set(key, value).unwrap();
            }
            //1 => {
            //    let value: i64 = rng.gen();
            //    let cas = match mvcc.get(&key) {
            //        Err(Error::KeyNotFound) => 0,
            //        Err(_err) => unreachable!(),
            //        Ok(e) => e.to_seqno(),
            //    };
            //    w.set_cas(key, value, cas).unwrap();
            //}
            //2 => {
            //    w.delete(&key).unwrap();
            //}
            _ => unreachable!(),
        }
    }
    println!("finished concurrent writes to mvcc {}", mvcc.to_seqno());
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
