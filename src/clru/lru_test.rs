use arbitrary::{self, unstructured::Unstructured, Arbitrary};
use rand::{self, prelude::random, rngs::SmallRng, Rng, SeedableRng};

use std::{fmt, hash::Hash, thread};

use crate::{clru, dbs, llrb};

macro_rules! test_code {
    ($seed:expr, $keytype:ty) => {{
        let mut rng = SmallRng::seed_from_u64($seed);

        let n_threads = [1, 2, 4, 8, 16, 32, 64][rng.gen::<usize>() % 7];
        let m = match <$keytype>::MAX as usize {
            i if i < 65536 => (<$keytype>::MAX as usize) / n_threads,
            _ => [1000, 10_000, 100_000][rng.gen::<usize>() % 3],
        };
        let c = ((m as f64) * [0.01, 0.1, 1.0][rng.gen::<usize>() % 3]) as usize;

        let n = std::cmp::min(n_threads * m, <$keytype>::MAX as usize);
        let n_ops = (n_threads * m) * [1, 2, 4, 8][rng.gen::<usize>() % 4];
        let access_type: AccessType = {
            let bytes = rng.gen::<[u8; 32]>();
            let mut uns = Unstructured::new(&bytes);
            uns.arbitrary().unwrap()
        };

        println!(
            "test_lru_{}, seed:{} n:{} m:{}, c:{}, n_threads:{}, n_ops:{}, access:{:?}",
            stringify!($keytype),
            $seed,
            n,
            m,
            c,
            n_threads,
            n_ops,
            access_type,
        );

        let index = populate_primary_index::<$keytype>($seed, n);
        let lru = {
            let config = clru::Config::new(n_threads + 1, c);
            clru::Lru::from_config(config)
        };
        println!("test_lru_{} loaded index ...", stringify!($keytype));

        let mut handles = vec![];
        for thread_id in 0..n_threads {
            let seed = $seed + ((thread_id as u64) * 100);
            let index = index.clone();
            let lru = lru.clone();

            let keys = access_keys::<$keytype>(
                thread_id,
                n_threads,
                seed,
                access_type,
                index.clone(),
                m,
            );

            println!(
                "test_lru_{} thread-{} loaded key_set len:{}",
                stringify!($keytype),
                thread_id,
                keys.len()
            );
            assert!(keys.len() == m, "{} != {}", keys.len(), m);

            let h = thread::spawn(move || {
                with_lru::<$keytype>(thread_id, seed, index, lru, keys, n_ops)
            });
            handles.push(h);
        }

        let mut statss = vec![];
        for h in handles {
            let stats = h.join().unwrap();
            println!("test_lru_{} {:?}", stringify!($keytype), stats);
            statss.push(stats)
        }

        let lru_items = lru.len();
        let stats = lru.close().unwrap().unwrap();

        println!("lru len:{} stats:{:?}", lru_items, stats);
        validate(statss, stats, n_threads, n_ops);
    }};
}

#[test]
fn test_lru_u8() {
    let seed: u64 = [
        17107959159771477669,
        14991693275449717884,
        14750078202842248867,
        random(),
    ][random::<usize>() % 4];
    test_code!(seed, u8);
}

#[test]
fn test_lru_u16() {
    let seed: u64 = [12552994332855700723, random()][random::<usize>() % 2];
    test_code!(seed, u16);
}

#[test]
fn test_lru_u32() {
    let seed: u64 =
        [4838062309460413831, 682526681629544662, random()][random::<usize>() % 3];
    test_code!(seed, u32);
}

#[test]
fn test_lru_u128() {
    let seed: u64 = random();
    test_code!(seed, u128);
}

fn with_lru<K>(
    _thread_id: usize,
    seed: u64,
    index: llrb::Index<K, u128>,
    mut lru: clru::Lru<K, u128>,
    keys: Vec<K>,
    n_ops: usize,
) -> CacheStat
where
    K: Copy + Clone + PartialEq + Ord + Hash + fmt::Display + fmt::Debug,
{
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut stats = CacheStat::default();

    for _i in 0..n_ops {
        let key = keys[rng.gen::<usize>() % keys.len()];
        // println!("thread-{} {}", _thread_id, key);
        match lru.get(&key) {
            Some(value) => {
                // println!("thread-{} get-ok key:{}", _thread_id, key);
                let ref_value = index.get(&key).unwrap();
                assert_eq!(ref_value.to_value().unwrap(), value);
                stats.hits += 1;
            }
            None => {
                // println!("thread-{} get-no key:{}", _thread_id, key);
                let ref_value = index.get(&key).unwrap().to_value().unwrap();
                lru.set(key, ref_value);
                stats.misses += 1;
            }
        }
    }

    stats
}

fn populate_primary_index<K>(seed: u64, n: usize) -> llrb::Index<K, u128>
where
    K: Clone + Ord + dbs::Footprint,
    rand::distributions::Standard: rand::distributions::Distribution<K>,
{
    let mut rng = SmallRng::seed_from_u64(seed);
    let index: llrb::Index<K, u128> = llrb::Index::new("primary_index", true);
    for _ in 0..n {
        loop {
            let key: K = rng.gen();
            let value: u128 = rng.gen::<u128>();
            if let dbs::Wr {
                old_entry: None, ..
            } = index.insert(key, value).unwrap()
            {
                break;
            }
        }
    }

    index
}

fn access_keys<K>(
    thread_id: usize,
    n_threads: usize,
    seed: u64,
    access_type: AccessType,
    index: llrb::Index<K, u128>,
    m: usize,
) -> Vec<K>
where
    K: Copy + Clone + Default + PartialEq,
{
    let mut rng = SmallRng::seed_from_u64(seed);

    let iter = index.iter().unwrap();
    match access_type {
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
            let keys_set: Vec<K> = iter.take(m * n_threads).map(|e| e.to_key()).collect();
            let mut keys = vec![];
            while keys.len() < m {
                let off = rng.gen::<usize>() % keys_set.len();
                if !keys.contains(&keys_set[off]) {
                    keys.push(keys_set[off]);
                }
            }
            keys
        }
        AccessType::Random => {
            let keys_set: Vec<K> = iter.map(|e| e.to_key()).collect();
            let mut keys = vec![];
            while keys.len() < m {
                let off = rng.gen::<usize>() % keys_set.len();
                if !keys.contains(&keys_set[off]) {
                    keys.push(keys_set[off]);
                }
            }
            keys
        }
    }
}

#[derive(Copy, Clone, Debug, Arbitrary)]
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

fn validate(
    statss: Vec<CacheStat>,
    stats: crate::clru::Stats,
    n_threads: usize,
    n_ops: usize,
) {
    let mut n_misses = 0;
    for stats in statss.iter() {
        n_misses += stats.misses;
        assert_eq!(stats.misses + stats.hits, n_ops);
    }

    assert_eq!(n_ops * n_threads, stats.n_gets);
    assert_eq!(n_misses, stats.n_sets);

    assert_eq!(
        n_ops * n_threads,
        stats.n_evicted + stats.n_deleted + stats.n_access_gc
    )
}
