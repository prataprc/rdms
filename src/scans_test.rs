use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use super::*;
use crate::{
    core::{EphemeralIndex, Reader, Writer},
    error::Error,
    llrb::Llrb,
};

#[test]
fn test_skip_scan() {
    let seed: u128 = random();

    let (n_ops, key_max) = (6_000, 2_000);
    let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
    random_llrb(n_ops, key_max, seed, &mut llrb);

    let testcases = vec![
        (Bound::Included(0), Bound::Included(5100)),
        (Bound::Included(6000), Bound::Excluded(5100)),
        (Bound::Included(4000), Bound::Unbounded),
        (Bound::Excluded(5000), Bound::Included(5100)),
        (Bound::Excluded(5000), Bound::Excluded(5100)),
        (Bound::Excluded(5000), Bound::Unbounded),
        (Bound::Unbounded, Bound::Excluded(5100)),
        (Bound::Unbounded, Bound::Included(5100)),
        (Bound::Unbounded, Bound::Unbounded),
    ];
    for within in testcases {
        let r = llrb.to_reader().unwrap();
        let es: Vec<Entry<i64, i64>> = SkipScan::new(r, within)
            .map(|e| e.unwrap()) // unwrap
            .collect();
        for e in es {
            assert!(within.contains(&e.to_seqno()));
            for d in e.as_deltas() {
                assert!(within.contains(&d.to_seqno()));
            }
        }
    }

    let r = llrb.to_reader().unwrap();
    let within = (Bound::Included(5000), Bound::Included(5000));
    let es: Vec<Entry<i64, i64>> = SkipScan::new(r, within)
        .map(|e| e.unwrap()) // unwrap
        .collect();
    assert_eq!(es.len(), 1);
    assert_eq!(es[0].to_seqno(), 5000);

    let r = llrb.to_reader().unwrap();
    let within = (Bound::Included(5000), Bound::Excluded(5000));
    let es: Vec<Entry<i64, i64>> = SkipScan::new(r, within)
        .map(|e| e.unwrap()) // unwrap
        .collect();
    assert_eq!(es.len(), 0);

    let r = llrb.to_reader().unwrap();
    let within = (Bound::Excluded(5000), Bound::Included(5000));
    let es: Vec<Entry<i64, i64>> = SkipScan::new(r, within)
        .map(|e| e.unwrap()) // unwrap
        .collect();
    assert_eq!(es.len(), 0);

    let r = llrb.to_reader().unwrap();
    let within = (Bound::Excluded(5000), Bound::Excluded(5000));
    let es: Vec<Entry<i64, i64>> = SkipScan::new(r, within)
        .map(|e| e.unwrap()) // unwrap
        .collect();
    assert_eq!(es.len(), 0);

    let r = llrb.to_reader().unwrap();
    let within = (Bound::Included(5000), Bound::Excluded(5001));
    let es: Vec<Entry<i64, i64>> = SkipScan::new(r, within)
        .map(|e| e.unwrap()) // unwrap
        .collect();
    assert_eq!(es.len(), 1);
    assert_eq!(es[0].to_seqno(), 5000);

    let r = llrb.to_reader().unwrap();
    let within = (Bound::Excluded(5000), Bound::Included(5001));
    let es: Vec<Entry<i64, i64>> = SkipScan::new(r, within)
        .map(|e| e.unwrap()) // unwrap
        .collect();
    assert_eq!(es.len(), 1);
    assert_eq!(es[0].to_seqno(), 5001);

    let r = llrb.to_reader().unwrap();
    let within = (Bound::Excluded(5000), Bound::Excluded(5001));
    let es: Vec<Entry<i64, i64>> = SkipScan::new(r, within)
        .map(|e| e.unwrap()) // unwrap
        .collect();
    assert_eq!(es.len(), 0);
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
