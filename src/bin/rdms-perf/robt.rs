use rand::{rngs::SmallRng, Rng, SeedableRng};

use std::{fmt, result, thread, time};

use rdms::{
    db::{self, ToJson},
    llrb::Index as Mdb,
    robt::Index,
};

use crate::{get_property, load_profile, Generate, Key, Opt, Value};

// to_name, to_index_location, to_vlog_location, len, to_root, to_seqno, to_app_metadata
// to_stats, to_bitmap, is_compacted, validate

const DEFAULT_KEY_SIZE: i64 = 16;
const DEFAULT_VAL_SIZE: i64 = 16;

#[derive(Clone)]
pub struct Profile {
    key: Key,
    value: Value,
    bitmap: String,
    incr: bool,
    compact: bool,
    versions: bool
    loads: usize,
    gets: usize,
    iter: usize,
    reverse: usize,
    readers: usize,
    validate: bool,
}

impl Default for Profile {
    fn default() -> Profile {
        Profile {
            key: (String, usize),
            value: (String, usize),
            bitmap: "nobitmap",
            incr: false,
            compact: false,
            versiosn: false,
            loads: 1_000_000,
            gets: 1_000_000,
            iter: false,
            reverse: false,
            readers: 1,
            validate: true,
        }
    }
}

impl Profile {
    fn from_toml(v: toml::Value) -> result::Result<Profile, String> {
        let p: Profile = Default::default();

        let key = {
            match get_property!(v, "key_type", as_str, p.key.to_type()) {
                "u64" => Ok(Key::U64),
                "string" => {
                    let s = get_property!(v, "key_size", as_integer, DEFAULT_KEY_SIZE);
                    Ok(Key::String(s as usize))
                }
                typ => Err(format!("invalid type {}", typ)),
            }?
        };
        let value = {
            match get_property!(v, "value_type", as_str, p.value.to_type()) {
                "u64" => Ok(Value::U64),
                "string" => {
                    let s = get_property!(v, "value_size", as_integer, DEFAULT_VAL_SIZE);
                    Ok(Value::String(s as usize))
                }
                typ => Err(format!("invalid type {}", typ)),
            }?
        };

        let p = Profile {
            key,
            value,
            cas: get_property!(v, "cas", as_bool, p.cas),
            loads: get_property!(v, "loads", as_integer, p.loads as i64) as usize,
            sets: get_property!(v, "sets", as_integer, p.sets as i64) as usize,
            ins: get_property!(v, "ins", as_integer, p.ins as i64) as usize,
            rems: get_property!(v, "rems", as_integer, p.rems as i64) as usize,
            dels: get_property!(v, "dels", as_integer, p.dels as i64) as usize,
            gets: get_property!(v, "gets", as_integer, p.gets as i64) as usize,
            writers: get_property!(v, "writers", as_integer, p.writers as i64) as usize,
            readers: get_property!(v, "readers", as_integer, p.loads as i64) as usize,
            validate: get_property!(v, "validate", as_bool, p.validate),
        };

        Ok(p)
    }

    fn reset_writeops(&mut self) {
        self.sets = 0;
        self.ins = 0;
        self.rems = 0;
        self.dels = 0;
    }

    fn reset_readops(&mut self) {
        self.gets = 0;
    }
}

pub fn perf(opts: Opt) -> result::Result<(), String> {
    let profile: Profile =
        Profile::from_toml(load_profile(&opts)?).expect("invalid profile properties");

    match (&profile.key, &profile.value) {
        (Key::U64, Value::U64) => load_and_spawn::<u64, u64>(opts, profile),
        //(Key::U64, Value::String(_)) => load_and_spawn::<u64, String>(opts, profile),
        //(Key::String(_), Value::U64) => load_and_spawn::<String, u64>(opts, profile),
        //(Key::String(_), Value::String(_)) => {
        //    load_and_spawn::<String, String>(opts, profile)
        //}
        (_, _) => unreachable!(),
    }
}

fn load_and_spawn<K, V>(opts: Opt, p: Profile) -> result::Result<(), String>
where
    K: 'static + Send + Sync + Clone + Ord + db::Footprint + fmt::Debug,
    V: 'static + Send + Sync + db::Diff + db::Footprint,
    <V as db::Diff>::Delta: Send + Sync + db::Footprint,
    Key: Generate<K>,
    Value: Generate<V>,
{
    let mut rng = SmallRng::from_seed(opts.seed.to_le_bytes());

    let index = Index::<K, V>::new("rdms-llrb-perf", p.spin);

    initial_load(&mut rng, p.clone(), index.clone())?;

    let mut handles = vec![];
    for j in 0..p.writers {
        let (mut p, index) = (p.clone(), index.clone());
        p.reset_readops();
        let seed = opts.seed + ((j as u128) * 100);
        let h = thread::spawn(move || incr_load(j, seed, p, index));
        handles.push(h);
    }
    for j in p.writers..(p.writers + p.readers) {
        let (mut p, index) = (p.clone(), index.clone());
        p.reset_writeops();
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
    Key: Generate<K>,
    Value: Generate<V>,
{
    let start = time::Instant::now();
    for _i in 0..p.loads {
        index.set(p.key.gen(rng), p.value.gen(rng)).unwrap();
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
    Key: Generate<K>,
    Value: Generate<V>,
{
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let start = time::Instant::now();
    let total = p.sets + p.ins + p.rems + p.dels + p.gets;
    let (mut sets, mut ins, mut rems, mut dels, mut gets) =
        (p.sets, p.ins, p.rems, p.dels, p.gets);

    while (sets + ins + rems + dels + gets) > 0 {
        let key: K = p.key.gen(&mut rng);
        match rng.gen::<usize>() % (sets + ins + rems + dels + gets) {
            op if p.cas && op < sets => {
                let cas = rng.gen::<u64>() % (total as u64);
                let val: V = p.value.gen(&mut rng);
                index.set_cas(key, val, cas).unwrap();
                sets -= 1;
            }
            op if op < p.sets => {
                let val: V = p.value.gen(&mut rng);
                index.set(key, val).unwrap();
                sets -= 1;
            }
            op if p.cas && op < (p.sets + p.ins) => {
                let cas = rng.gen::<u64>() % (total as u64);
                let val: V = p.value.gen(&mut rng);
                index.insert_cas(key, val, cas).unwrap();
                ins -= 1;
            }
            op if op < (p.sets + p.ins) => {
                let val: V = p.value.gen(&mut rng);
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
