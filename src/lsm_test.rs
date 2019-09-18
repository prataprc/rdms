use rand::prelude::random;
use rand::{rngs::SmallRng, Rng, SeedableRng};

use super::*;

use crate::core::Reader;
use crate::error::Error;
use crate::llrb::Llrb;

#[test]
fn test_lsm_get() {
    // test case using 5 llrb versions
    let seed: u128 = random();
    let mut refi = Llrb::new("test-llrb");

    let (n_ops, key_max) = (60, 20);
    let mut llrb1: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
    random_llrb(n_ops, key_max, seed, &mut llrb1, &mut refi);

    let (n_ops, key_max) = (600, 200);
    let mut llrb2 = llrb1.clone();
    random_llrb(n_ops, key_max, seed, &mut llrb2, &mut refi);

    //let (n_ops, key_max) = (6_000, 2_000);
    //let mut llrb3 = llrb2.clone();
    //random_llrb(n_ops, key_max, seed, &mut llrb3, &mut refi);

    //let (n_ops, key_max) = (60_000, 20_000);
    //let mut llrb4 = llrb3.clone();
    //random_llrb(n_ops, key_max, seed, &mut llrb4, &mut refi);

    //let (n_ops, key_max) = (60_000, 20_000);
    //let mut llrb5 = llrb4.clone();
    //random_llrb(n_ops, key_max, seed, &mut llrb5, &mut refi);

    let yget = y_get(getter(&*llrb2), getter(&*llrb1));
    for entry in refi.iter().unwrap() {
        let entry = entry.unwrap();
        let key = entry.to_key();
        let e = yget(&key).unwrap();
        assert_eq!(e.to_seqno(), entry.to_seqno());
    }
}

#[test]
fn test_lsm_get_versions() {
    // test case using 5 llrb versions
    // test case using 3 llrb version and 2 robt versions
}

#[test]
fn test_lsm_iter() {
    // test case using 5 llrb versions
    // test case using 3 llrb version and 2 robt versions
}

#[test]
fn test_lsm_iter_versions() {
    // test case using 5 llrb versions
    // test case using 3 llrb version and 2 robt versions
}

#[test]
fn test_lsm_range() {
    // test case using 5 llrb versions
    // test case using 3 llrb version and 2 robt versions
}

#[test]
fn test_lsm_range_versions() {
    // test case using 5 llrb versions
    // test case using 3 llrb version and 2 robt versions
}

#[test]
fn test_lsm_reverse() {
    // test case using 5 llrb versions
    // test case using 3 llrb version and 2 robt versions
}

#[test]
fn test_lsm_reverse_versions() {
    // test case using 5 llrb versions
    // test case using 3 llrb version and 2 robt versions
}

fn test_lsm_skip_scan() {}

fn test_get<'a>(y: LsmGet<'a, i64, i64>, refi: Llrb<i64, i64>) {
    // TBD
}

fn test_iter<'a>(y: IndexIter<'a, i64, i64>, refi: IndexIter<'a, i64, i64>) {
    // TBD
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
        match rng.gen::<usize>() % 3 {
            0 => {
                let value: i64 = rng.gen();
                llrb.set(key, value).unwrap();
                refi.set(key, value);
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

//fn random_robt(
//    n_ops: u64,
//    key_max: u64,
//    refi: Llrb<i64, i64>, // reference index
//) -> Robt<i64, i64> {
//    // TBD
//}
