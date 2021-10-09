use rand::{rngs::SmallRng, Rng, SeedableRng};

use std::{
    collections::BTreeMap,
    fmt, result,
    time::{self, SystemTime},
};

use crate::{get_property, load_profile, Generate, Opt};
use rdms::db;

const DEFAULT_KEY_SIZE: i64 = 16;
const DEFAULT_VAL_SIZE: i64 = 16;

#[derive(Clone)]
pub struct Profile {
    key: (String, usize),   // u64, binary
    value: (String, usize), // u64, binary
    loads: usize,
    sets: usize,
    rems: usize,
    gets: usize,
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
        let size = self.key.1;
        db::Binary(format!("{:0width$}", key, width = size).as_bytes().to_vec())
    }

    fn gen_value(&self, rng: &mut SmallRng) -> db::Binary {
        let val = rng.gen::<u64>();
        let size = self.value.1;
        db::Binary(format!("{:0width$}", val, width = size).as_bytes().to_vec())
    }
}

impl Default for Profile {
    fn default() -> Profile {
        Profile {
            key: ("u64".to_string(), 0),
            value: ("u64".to_string(), 0),
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

        let key = (
            get_property!(v, "key_type", as_str, &p.key.0).to_string(),
            get_property!(v, "key_size", as_integer, DEFAULT_KEY_SIZE) as usize,
        );
        let value = (
            get_property!(v, "value_type", as_str, &p.value.0).to_string(),
            get_property!(v, "value_size", as_integer, DEFAULT_VAL_SIZE) as usize,
        );

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

    let (kt, vt) = (&profile.key.0, &profile.value.0);

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
    Profile: Generate<K> + Generate<V>,
{
    let start = SystemTime::now();

    for _i in 0..p.loads {
        index.insert(p.gen_key(rng), p.gen_value(rng));
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
    Profile: Generate<K> + Generate<V>,
{
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let start = time::Instant::now();
    let (mut sets, mut rems, mut gets) = (p.sets, p.rems, p.gets);

    while (sets + rems + gets) > 0 {
        let key: K = p.gen_key(&mut rng);
        match rng.gen::<usize>() % (sets + rems + gets) {
            op if op < sets => {
                index.insert(key, p.gen_value(&mut rng));
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
