use rand::{rngs::SmallRng, Rng, SeedableRng};

use std::{
    collections::BTreeMap,
    fmt, result,
    time::{self, SystemTime},
};

use crate::{get_property, load_profile, Generate, Key, Opt, Value};
use rdms::db;

const DEFAULT_KEY_SIZE: i64 = 16;
const DEFAULT_VAL_SIZE: i64 = 16;

#[derive(Clone)]
pub struct Profile {
    key: Key,
    value: Value,
    loads: usize,
    sets: usize,
    rems: usize,
    gets: usize,
}

impl Default for Profile {
    fn default() -> Profile {
        Profile {
            key: Key::default(),
            value: Value::default(),
            loads: 1_000_000,
            sets: 1_000_000,
            rems: 100_000,
            gets: 1_000_000,
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
            loads: get_property!(v, "loads", as_integer, p.loads as i64) as usize,
            sets: get_property!(v, "sets", as_integer, p.sets as i64) as usize,
            rems: get_property!(v, "rems", as_integer, p.rems as i64) as usize,
            gets: get_property!(v, "gets", as_integer, p.gets as i64) as usize,
        };

        Ok(p)
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
    let mut index: BTreeMap<K, V> = BTreeMap::new();
    initial_load(&mut rng, p.clone(), &mut index)?;
    incr_load(opts.seed, p.clone(), &mut index)?;

    print!("rdms-perf: iterating ... ");
    let (elapsed, n) = {
        let start = time::Instant::now();
        let n: usize = index.iter().map(|_| 1_usize).sum();
        assert!(n == index.len(), "{} != {}", n, index.len());
        (start.elapsed(), n)
    };
    println!("{} items, took {:?}", n, elapsed);

    print!("rdms-perf: ranging ... ");
    let (elapsed, n) = {
        let start = time::Instant::now();
        let n: usize = index.range(..).map(|_| 1_usize).sum();
        assert!(n == index.len(), "{} != {}", n, index.len());
        (start.elapsed(), n)
    };
    println!("{} items, took {:?}", n, elapsed);

    print!("rdms-perf: reverse iter ... ");
    let (elapsed, n) = {
        let start = time::Instant::now();
        let n: usize = index.range(..).rev().map(|_| 1_usize).sum();
        assert!(n == index.len(), "{} != {}", n, index.len());
        (start.elapsed(), n)
    };
    println!("{} items, took {:?}", n, elapsed);

    Ok(())
}

fn initial_load<K, V>(
    rng: &mut SmallRng,
    p: Profile,
    index: &mut BTreeMap<K, V>,
) -> result::Result<(), String>
where
    K: 'static + Send + Sync + Clone + Ord + db::Footprint,
    V: 'static + Send + Sync + db::Diff + db::Footprint,
    <V as db::Diff>::Delta: Send + Sync + db::Footprint,
    Key: Generate<K>,
    Value: Generate<V>,
{
    let start = SystemTime::now();

    for _i in 0..p.loads {
        index.insert(p.key.gen(rng), p.value.gen(rng));
    }

    println!(
        "rdms-perf: loaded {} items in {:?}",
        p.loads,
        start.elapsed()
    );

    Ok(())
}

fn incr_load<K, V>(
    seed: u128,
    p: Profile,
    index: &mut BTreeMap<K, V>,
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
    let (mut sets, mut rems, mut gets) = (p.sets, p.rems, p.gets);

    while (sets + rems + gets) > 0 {
        let key: K = p.key.gen(&mut rng);
        match rng.gen::<usize>() % (sets + rems + gets) {
            op if op < sets => {
                index.insert(key, p.value.gen(&mut rng));
                sets -= 1;
            }
            op if op < (sets + rems) => {
                index.remove(&key);
                rems -= 1;
            }
            op if op < (sets + rems + gets) => {
                index.get(&key);
                gets -= 1;
            }
            _ => unreachable!(),
        }
    }

    println!(
        concat!(
            "rdms-btreemap: incremental for (sets:{} rems:{} gets:{}) ",
            "operations took {:?}",
        ),
        p.sets,
        p.rems,
        p.gets,
        start.elapsed()
    );

    Ok(())
}
