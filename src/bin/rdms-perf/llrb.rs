use rand::{rngs::SmallRng, Rng, SeedableRng};

use std::{fmt, thread, time};

use rdms::{
    db::{self, ToJson},
    llrb::Index,
    Result,
};

use crate::Opt;

pub fn perf<K>(opts: Opt) -> Result<()>
where
    K: 'static + Send + Sync + Clone + Ord + db::Footprint + fmt::Debug,
    rand::distributions::Standard: rand::distributions::Distribution<K>,
{
    let mut rng = SmallRng::from_seed(opts.seed.to_le_bytes());

    let index = Index::<K, u64>::new("rdms-llrb-perf", opts.spin);

    initial_load(&mut rng, opts.clone(), index.clone());

    let mut handles = vec![];
    for j in 0..opts.writers {
        let (mut opts, index) = (opts.clone(), index.clone());
        opts.reset_readops();
        let seed = opts.seed + ((j as u128) * 100);
        let h = thread::spawn(move || incr_load(j, seed, opts, index));
        handles.push(h);
    }
    for j in opts.writers..(opts.writers + opts.readers) {
        let (mut opts, index) = (opts.clone(), index.clone());
        opts.reset_writeops();
        let seed = opts.seed + ((j as u128) * 100);
        let h = thread::spawn(move || incr_load(j, seed, opts, index));
        handles.push(h);
    }

    for handle in handles.into_iter() {
        handle.join().unwrap()
    }

    let (elapsed, n) = {
        let start = time::Instant::now();
        let n: usize = index.iter().unwrap().map(|_| 1_usize).sum();
        assert!(n == index.len(), "{} != {}", n, index.len());
        (start.elapsed(), n)
    };
    println!("rdms-llrb: iterating {} items, took {:?}", n, elapsed);

    let (elapsed, n) = {
        let start = time::Instant::now();
        let n: usize = index.range(..).unwrap().map(|_| 1_usize).sum();
        assert!(n == index.len(), "{} != {}", n, index.len());
        (start.elapsed(), n)
    };
    println!("rdms-llrb: ranging {} items, took {:?}", n, elapsed);

    let (elapsed, n) = {
        let start = time::Instant::now();
        let n: usize = index.reverse(..).unwrap().map(|_| 1_usize).sum();
        assert!(n == index.len(), "{} != {}", n, index.len());
        (start.elapsed(), n)
    };
    println!("rdms-llrb: rev iterating {} items, took {:?}", n, elapsed);

    println!("rdms-llrb: index latest-seqno:{}", index.to_seqno());
    println!("rdms-llrb: index deleted_count:{}", index.deleted_count());

    println!("rdms-llrb: validating {} items in index", index.len());
    index.validate().unwrap();

    println!("rdms-llrb: stats {}", index.to_stats().unwrap().to_json());

    index.purge().unwrap();

    Ok(())
}

fn initial_load<K>(rng: &mut SmallRng, opts: Opt, index: Index<K, u64>)
where
    K: 'static + Send + Sync + Clone + Ord + db::Footprint,
    rand::distributions::Standard: rand::distributions::Distribution<K>,
{
    // initial load
    let start = time::Instant::now();
    for _i in 0..opts.loads {
        let (key, val) = (rng.gen::<K>(), rng.gen::<u64>());
        index.set(key, val).unwrap();
    }

    println!(
        "rdms-llrb loaded {} items in {:?}",
        opts.loads,
        start.elapsed()
    );
}

fn incr_load<K>(j: usize, seed: u128, opts: Opt, index: Index<K, u64>)
where
    K: 'static + Send + Sync + Clone + Ord + db::Footprint,
    rand::distributions::Standard: rand::distributions::Distribution<K>,
{
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let start = time::Instant::now();
    let total = opts.sets + opts.ins + opts.rems + opts.dels + opts.gets;
    let (mut sets, mut ins, mut rems, mut dels, mut gets) =
        (opts.sets, opts.ins, opts.rems, opts.dels, opts.gets);
    while (sets + ins + rems + dels + gets) > 0 {
        let key = rng.gen::<K>();
        match rng.gen::<usize>() % (sets + ins + rems + dels + gets) {
            op if opts.cas && op < sets => {
                let cas = rng.gen::<u64>() % (total as u64);
                let val = rng.gen::<u64>();
                index.set_cas(key, val, cas).unwrap();
                sets -= 1;
            }
            op if op < opts.sets => {
                let val = rng.gen::<u64>();
                index.set(key, val).unwrap();
                sets -= 1;
            }
            op if opts.cas && op < (opts.sets + opts.ins) => {
                let cas = rng.gen::<u64>() % (total as u64);
                let val = rng.gen::<u64>();
                index.insert_cas(key, val, cas).unwrap();
                ins -= 1;
            }
            op if op < (opts.sets + opts.ins) => {
                let val = rng.gen::<u64>();
                index.insert(key, val).unwrap();
                ins -= 1;
            }
            op if opts.cas && op < (opts.sets + opts.ins + opts.rems) => {
                let cas = rng.gen::<u64>() % (total as u64);
                index.remove_cas(&key, cas).unwrap();
                rems -= 1;
            }
            op if op < (opts.sets + opts.ins + opts.rems) => {
                index.remove(&key).unwrap();
                rems -= 1;
            }
            op if opts.cas && op < (opts.sets + opts.ins + opts.rems + opts.dels) => {
                let cas = rng.gen::<u64>() % (total as u64);
                index.delete_cas(&key, cas).unwrap();
                dels -= 1;
            }
            op if op < (opts.sets + opts.ins + opts.rems + opts.dels) => {
                index.delete(&key).unwrap();
                dels -= 1;
            }
            _op => {
                index.get(&key).ok();
                gets -= 1;
            }
        }
    }

    println!(
        concat!(
            "rdms-llrb: incremental-{} for (sets:{} ins:{} rems:{} dels:{} gets:{}) ",
            "operations took {:?}",
        ),
        j,
        opts.sets,
        opts.ins,
        opts.rems,
        opts.dels,
        opts.gets,
        start.elapsed()
    );
}
