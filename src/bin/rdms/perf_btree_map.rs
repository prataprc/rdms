use rand::{rngs::SmallRng, Rng, SeedableRng};
use serde::Deserialize;

use std::{
    collections::BTreeMap,
    fmt, result,
    time::{self, SystemTime},
};

use crate::cmd_perf::{Generate, Opt};
use rdms::{db, util};

const DEFAULT_KEY_SIZE: usize = 16;
const DEFAULT_VAL_SIZE: usize = 16;

#[derive(Clone, Deserialize)]
pub struct Profile {
    key_type: String, // u64, binary
    key_size: usize,
    value_type: String, // u64, binary
    value_size: usize,
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
        let (key, size) = (rng.gen::<u64>(), self.key_size);
        let val = format!("{:0width$}", key, width = size).as_bytes().to_vec();
        db::Binary { val }
    }

    fn gen_value(&self, rng: &mut SmallRng) -> db::Binary {
        let (val, size) = (rng.gen::<u64>(), self.value_size);
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
            loads: 1_000_000,
            sets: 1_000_000,
            rems: 100_000,
            gets: 1_000_000,
        }
    }
}

pub fn perf(opts: Opt) -> result::Result<(), String> {
    let profile: Profile =
        util::files::load_toml(&opts.profile).map_err(|e| e.to_string())?;

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
    let mut index: BTreeMap<K, V> = BTreeMap::new();
    initial_load(&mut rng, p.clone(), &mut index)?;
    incr_load(opts.seed, p.clone(), &mut index)?;

    print!("rdms: iterating ... ");
    let (elapsed, n) = {
        let start = time::Instant::now();
        let n: usize = index.iter().map(|_| 1_usize).sum();
        assert!(n == index.len(), "{} != {}", n, index.len());
        (start.elapsed(), n)
    };
    println!("{} items, took {:?}", n, elapsed);

    print!("rdms: ranging ... ");
    let (elapsed, n) = {
        let start = time::Instant::now();
        let n: usize = index.range(..).map(|_| 1_usize).sum();
        assert!(n == index.len(), "{} != {}", n, index.len());
        (start.elapsed(), n)
    };
    println!("{} items, took {:?}", n, elapsed);

    print!("rdms: reverse iter ... ");
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

    println!("rdms: loaded {} items in {:?}", p.loads, start.elapsed());

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
