use rand::{rngs::SmallRng, Rng, SeedableRng};
use serde::Deserialize;

use std::{fmt, result, thread, time};

use rdms::{
    db::{self, ToJson},
    llrb::Index,
};

use crate::{load_profile, Generate, Opt};

const DEFAULT_KEY_SIZE: usize = 16;
const DEFAULT_VAL_SIZE: usize = 16;

#[derive(Clone, Deserialize)]
pub struct Profile {
    key_type: String, // u64, binary
    key_size: usize,
    value_type: String, // u64, binary
    value_size: usize,
    spin: bool,
    cas: bool,
    loads: usize,
    sets: usize,
    ins: usize,
    rems: usize,
    dels: usize,
    gets: usize,
    writers: usize,
    readers: usize,
    validate: bool,
}

impl Generate<u64> for Profile {
    fn gen_key(&self, rng: &mut SmallRng) -> u64 {
        rng.gen::<u64>()
    }

    fn gen_value(&self, rng: &mut SmallRng) -> u64 {
        rng.gen::<u64>()
    }
}

impl Generate<db::Binary> for Profile {
    fn gen_key(&self, rng: &mut SmallRng) -> db::Binary {
        let key = rng.gen::<u64>();
        let size = self.key_size;
        let val = format!("{:0width$}", key, width = size).as_bytes().to_vec();
        db::Binary { val }
    }

    fn gen_value(&self, rng: &mut SmallRng) -> db::Binary {
        let val = rng.gen::<u64>();
        let size = self.value_size;
        let val = format!("{:0width$}", val, width = size).as_bytes().to_vec();
        db::Binary { val }
    }
}

impl Default for Profile {
    fn default() -> Profile {
        Profile {
            key_type: "u64".to_string(),
            key_size: DEFAULT_KEY_SIZE,
            value_type: "u64".to_string(),
            value_size: DEFAULT_VAL_SIZE,
            spin: false,
            cas: false,
            loads: 1_000_000,
            sets: 1_000_000,
            ins: 0,
            rems: 100_000,
            dels: 0,
            gets: 1_000_000,
            writers: 1,
            readers: 1,
            validate: true,
        }
    }
}

impl Profile {
    fn reset_read_ops(&mut self) {
        self.gets = 0;
    }

    fn reset_write_ops(&mut self) {
        self.sets = 0;
        self.ins = 0;
        self.rems = 0;
        self.dels = 0;
    }
}

pub fn perf(opts: Opt) -> result::Result<(), String> {
    let profile: Profile =
        toml::from_str(&load_profile(&opts)?).map_err(|e| e.to_string())?;

    let (kt, vt) = (&profile.key_type, &profile.value_type);

    match (kt.as_str(), vt.as_str()) {
        ("u64", "u64") => load_and_spawn::<u64, u64>(opts, profile),
        ("u64", "binary") => load_and_spawn::<u64, db::Binary>(opts, profile),
        ("binary", "binary") => load_and_spawn::<db::Binary, db::Binary>(opts, profile),
        (_, _) => unreachable!(),
    }
}

fn load_and_spawn<K, V>(opts: Opt, p: Profile) -> result::Result<(), String>
where
    K: 'static + Send + Sync + Clone + Ord + db::Footprint + fmt::Debug,
    V: 'static + Send + Sync + db::Diff + db::Footprint,
    <V as db::Diff>::Delta: Send + Sync + db::Footprint,
    Profile: Generate<K> + Generate<V>,
{
    let mut rng = SmallRng::from_seed(opts.seed.to_le_bytes());

    let index = Index::<K, V>::new("rdms-llrb-perf", p.spin);

    initial_load(&mut rng, p.clone(), index.clone())?;

    let mut handles = vec![];
    for j in 0..p.writers {
        let (mut p, index) = (p.clone(), index.clone());
        p.reset_read_ops();
        let seed = opts.seed + ((j as u128) * 100);
        let h = thread::spawn(move || incr_load(j, seed, p, index));
        handles.push(h);
    }
    for j in p.writers..(p.writers + p.readers) {
        let (mut p, index) = (p.clone(), index.clone());
        p.reset_write_ops();
        let seed = opts.seed + ((j as u128) * 100);
        let h = thread::spawn(move || incr_load(j, seed, p, index));
        handles.push(h);
    }

    for handle in handles.into_iter() {
        handle.join().unwrap().unwrap()
    }

    print!("rdms-perf: iterating ... ");
    let (elapsed, n) = {
        let start = time::Instant::now();
        let n: usize = index.iter().unwrap().map(|_| 1_usize).sum();
        assert!(n == index.len(), "{} != {}", n, index.len());
        (start.elapsed(), n)
    };
    println!("{} items, took {:?}", n, elapsed);

    print!("rdms-perf: ranging ... ");
    let (elapsed, n) = {
        let start = time::Instant::now();
        let n: usize = index.range(..).unwrap().map(|_| 1_usize).sum();
        assert!(n == index.len(), "{} != {}", n, index.len());
        (start.elapsed(), n)
    };
    println!("{} items, took {:?}", n, elapsed);

    print!("rdms-perf: reverse iter ... ");
    let (elapsed, n) = {
        let start = time::Instant::now();
        let n: usize = index.reverse(..).unwrap().map(|_| 1_usize).sum();
        assert!(n == index.len(), "{} != {}", n, index.len());
        (start.elapsed(), n)
    };
    println!("{} items, took {:?}", n, elapsed);

    println!("rdms-perf: index latest-seqno:{}", index.to_seqno());
    println!("rdms-perf: index deleted_count:{}", index.deleted_count());

    println!("rdms-perf: stats {}", index.to_stats().unwrap().to_json());

    if p.validate {
        print!("rdms-perf: validating {} items in index ... ", index.len());
        index.validate().unwrap();
        println!("ok");
    }

    index.purge().unwrap();

    Ok(())
}

fn initial_load<K, V>(
    rng: &mut SmallRng,
    p: Profile,
    index: Index<K, V>,
) -> result::Result<(), String>
where
    K: 'static + Send + Sync + Clone + Ord + db::Footprint,
    V: 'static + Send + Sync + db::Diff + db::Footprint,
    <V as db::Diff>::Delta: Send + Sync + db::Footprint,
    Profile: Generate<K> + Generate<V>,
{
    let start = time::Instant::now();
    for _i in 0..p.loads {
        index.set(p.gen_key(rng), p.gen_value(rng)).unwrap();
    }

    println!(
        "rdms-perf: loaded {} items in {:?}",
        p.loads,
        start.elapsed()
    );

    Ok(())
}

fn incr_load<K, V>(
    j: usize,
    seed: u128,
    p: Profile,
    index: Index<K, V>,
) -> result::Result<(), String>
where
    K: 'static + Send + Sync + Clone + Ord + db::Footprint,
    V: 'static + Send + Sync + db::Diff + db::Footprint,
    <V as db::Diff>::Delta: Send + Sync + db::Footprint,
    Profile: Generate<K> + Generate<V>,
{
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let start = time::Instant::now();
    let total = p.sets + p.ins + p.rems + p.dels + p.gets;
    let (mut sets, mut ins, mut rems, mut dels, mut gets) =
        (p.sets, p.ins, p.rems, p.dels, p.gets);

    while (sets + ins + rems + dels + gets) > 0 {
        let key: K = p.gen_key(&mut rng);
        match rng.gen::<usize>() % (sets + ins + rems + dels + gets) {
            op if p.cas && op < sets => {
                let cas = rng.gen::<u64>() % (total as u64);
                let val: V = p.gen_value(&mut rng);
                index.set_cas(key, val, cas).unwrap();
                sets -= 1;
            }
            op if op < p.sets => {
                let val: V = p.gen_value(&mut rng);
                index.set(key, val).unwrap();
                sets -= 1;
            }
            op if p.cas && op < (p.sets + p.ins) => {
                let cas = rng.gen::<u64>() % (total as u64);
                let val: V = p.gen_value(&mut rng);
                index.insert_cas(key, val, cas).unwrap();
                ins -= 1;
            }
            op if op < (p.sets + p.ins) => {
                let val: V = p.gen_value(&mut rng);
                index.insert(key, val).unwrap();
                ins -= 1;
            }
            op if p.cas && op < (p.sets + p.ins + p.rems) => {
                let cas = rng.gen::<u64>() % (total as u64);
                index.remove_cas(&key, cas).unwrap();
                rems -= 1;
            }
            op if op < (p.sets + p.ins + p.rems) => {
                index.remove(&key).unwrap();
                rems -= 1;
            }
            op if p.cas && op < (p.sets + p.ins + p.rems + p.dels) => {
                let cas = rng.gen::<u64>() % (total as u64);
                index.delete_cas(&key, cas).unwrap();
                dels -= 1;
            }
            op if op < (p.sets + p.ins + p.rems + p.dels) => {
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
            "rdms-perf: incremental-{} for (sets:{} ins:{} rems:{} dels:{} gets:{}) ",
            "operations took {:?}",
        ),
        j,
        p.sets,
        p.ins,
        p.rems,
        p.dels,
        p.gets,
        start.elapsed()
    );

    Ok(())
}
