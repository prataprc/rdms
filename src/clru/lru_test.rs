use arbitrary::{self, unstructured::Unstructured, Arbitrary};
use rand::{self, prelude::random, rngs::SmallRng, Rng, SeedableRng};

use std::thread;

use crate::{clru, dbs, llrb};

#[test]
fn test_lru() {
    let seed: u64 = random();
    let mut rng = SmallRng::seed_from_u64(seed);

    let n = [10_000, 1_000_000, 10_000_000][rng.gen::<usize>() % 3];
    let m = ((n as f64) * [0.01, 0.1, 100.0][rng.gen::<usize>() % 3]) as usize;
    let n_threads = [1, 2, 4, 8, 16, 32, 64][rng.gen::<usize>() % 7];
    let n_ops = n * [1, 2, 4, 8][rng.gen::<usize>() % 4];

    let index = populate_primary_index(seed, n);
    let lru = {
        let config = clru::Config::new(n_threads + 1, m);
        clru::Lru::from_config(config)
    };

    let mut handles = vec![];
    for thread_id in 0..n_threads {
        let seed = seed + ((thread_id as u64) * 100);
        let index = index.clone();
        let lru = lru.clone();

        let typ: AccessType = {
            let bytes = rng.gen::<[u8; 32]>();
            let mut uns = Unstructured::new(&bytes);
            uns.arbitrary().unwrap()
        };

        let keys = access_keys(thread_id, n_threads, seed, typ, index.clone(), m);
        let h = thread::spawn(move || with_lru(seed, index, lru, keys, n_ops));
        handles.push(h);
    }

    let mut statss = vec![];
    for h in handles {
        let stats = h.join().unwrap();
        println!("{:?}", stats);
        statss.push(stats)
    }
}

fn with_lru(
    seed: u64,
    index: llrb::Index<u128, u128>,
    mut lru: clru::Lru<u128, u128>,
    keys: Vec<u128>,
    n_ops: usize,
) -> CacheStat {
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut stats = CacheStat::default();

    for _i in 0..n_ops {
        let key = keys[rng.gen::<usize>() % keys.len()];
        match lru.get(&key) {
            Some(value) => {
                let ref_value = index.get(&key).unwrap();
                assert_eq!(ref_value.to_value().unwrap(), value);
                stats.hits += 1;
            }
            None => {
                let ref_value = index.get(&key).unwrap().to_value().unwrap();
                assert!(lru.set(key, ref_value).is_none());
                stats.misses += 1;
            }
        }
    }

    stats
}

fn populate_primary_index(seed: u64, n: usize) -> llrb::Index<u128, u128> {
    let mut rng = SmallRng::seed_from_u64(seed);
    let index: llrb::Index<u128, u128> = llrb::Index::new("primary_index", true);
    for _ in 0..n {
        loop {
            let (key, value) = (rng.gen(), rng.gen());
            match index.insert(key, value).unwrap() {
                dbs::Wr {
                    old_entry: None, ..
                } => break,
                _ => (),
            }
        }
    }

    index
}

fn access_keys(
    thread_id: usize,
    n_threads: usize,
    seed: u64,
    typ: AccessType,
    index: llrb::Index<u128, u128>,
    m: usize,
) -> Vec<u128> {
    let mut rng = SmallRng::seed_from_u64(seed);

    let iter = index.iter().unwrap();
    match typ {
        AccessType::Inclusive => iter
            .step_by(n_threads)
            .take(m)
            .map(|e| e.to_key())
            .collect(),
        AccessType::Exclusive => iter
            .skip(thread_id)
            .step_by(n_threads)
            .take(m)
            .map(|e| e.to_key())
            .collect(),
        AccessType::Overlap => {
            let mut keys: Vec<u128> = iter.take(m / 2).map(|e| e.to_key()).collect();
            keys.extend_from_slice(
                &index
                    .iter()
                    .unwrap()
                    .skip(thread_id)
                    .step_by(n_threads)
                    .take(m / 2)
                    .map(|e| e.to_key())
                    .collect::<Vec<u128>>(),
            );
            keys
        }
        AccessType::Random => {
            let keys_set: Vec<u128> = iter.map(|e| e.to_key()).collect();
            let mut keys = vec![];
            for _ in 0..m {
                loop {
                    let off = rng.gen::<usize>() % keys.len();
                    match keys.contains(&keys_set[off]) {
                        false => {
                            keys.push(keys_set[off]);
                            break;
                        }
                        true => (),
                    }
                }
            }
            keys
        }
    }
}

#[derive(Clone, Debug, Arbitrary)]
enum AccessType {
    Inclusive,
    Exclusive,
    Overlap,
    Random,
}

#[derive(Default, Debug)]
struct CacheStat {
    misses: usize,
    hits: usize,
}
