use cbordata::{FromCbor, IntoCbor};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use serde::Deserialize;

use std::{ffi, fmt, hash::Hash, result, thread, time};

use rdms::{
    db::{self, ToJson},
    llrb::{self, Index as Mdb},
    robt, Result,
};

use crate::{load_profile, Generate, Opt};

// to_name, to_index_location, to_vlog_location, len, to_root, to_seqno, to_app_metadata
// to_stats, to_bitmap, is_compacted, validate

const DEFAULT_KEY_SIZE: usize = 16;
const DEFAULT_VAL_SIZE: usize = 16;

#[derive(Clone, Deserialize)]
pub struct Profile {
    key_type: String, // u64, binary
    key_size: usize,
    value_type: String, // u64, binary
    value_size: usize,
    bitmap: String,
    initial: Initial,
    incrs: Vec<Incremental>,
    load: Load,
}

#[derive(Clone, Deserialize)]
struct Initial {
    sets: usize,
    ins: usize,
    rems: usize,
    dels: usize,
    robt: InitialConfig,
}

impl Default for Initial {
    fn default() -> Initial {
        Initial {
            sets: 1_000_000,
            ins: 1_000_000,
            rems: 100_000,
            dels: 0,
            robt: InitialConfig::default(),
        }
    }
}

#[derive(Clone, Deserialize)]
struct InitialConfig {
    name: String,
    dir: String,
    z_blocksize: usize,
    m_blocksize: usize,
    v_blocksize: usize,
    delta_ok: bool,
    value_in_vlog: bool,
    flush_queue_size: usize,
}

impl Default for InitialConfig {
    fn default() -> InitialConfig {
        InitialConfig {
            name: "rdms-robt-perf".to_string(),
            dir: "".to_string(),
            z_blocksize: 4096,
            m_blocksize: 4096,
            v_blocksize: 4096,
            delta_ok: true,
            value_in_vlog: true,
            flush_queue_size: 64,
        }
    }
}

#[derive(Clone, Deserialize)]
struct Incremental {
    name: String,
    sets: usize,
    ins: usize,
    rems: usize,
    dels: usize,
    compact: bool,
    compact_name: String,
}

impl Default for Incremental {
    fn default() -> Incremental {
        Incremental {
            name: "rdms-robt-perf-incr1".to_string(),
            sets: 1_000_000,
            ins: 1_000_000,
            rems: 100_000,
            dels: 0,
            compact: true,
            compact_name: "rdms-robt-perf-compact1".to_string(),
        }
    }
}

#[derive(Clone, Deserialize)]
struct Load {
    gets: usize,
    get_versions: usize,
    iter: bool,
    iter_versions: bool,
    reverse: bool,
    reverse_versions: bool,
    readers: usize,
    validate: bool,
}

impl Default for Load {
    fn default() -> Load {
        Load {
            gets: 1_000_000,
            get_versions: 0,
            iter: true,
            iter_versions: false,
            reverse: true,
            reverse_versions: false,
            readers: 1,
            validate: true,
        }
    }
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
            bitmap: "nobitmap".to_string(),
            initial: Initial::default(),
            incrs: vec![Incremental::default()],
            load: Load::default(),
        }
    }
}

impl Profile {
    fn to_initial_config(&self) -> robt::Config {
        let mut config = robt::Config::new(
            self.initial.robt.dir.as_ref(),
            &self.initial.robt.name.as_ref(),
        );
        config.z_blocksize = self.initial.robt.z_blocksize;
        config.m_blocksize = self.initial.robt.m_blocksize;
        config.v_blocksize = self.initial.robt.v_blocksize;
        config.delta_ok = self.initial.robt.delta_ok;
        config.value_in_vlog = self.initial.robt.value_in_vlog;
        config.flush_queue_size = self.initial.robt.flush_queue_size;

        config
    }
}

pub fn perf(opts: Opt) -> result::Result<(), String> {
    let profile: Profile =
        toml::from_str(&load_profile(&opts)?).map_err(|e| e.to_string())?;

    let (kt, vt) = (&profile.key_type, &profile.value_type);

    match (kt.as_str(), vt.as_str()) {
        //("u64", "u64") => load_and_spawn::<u64, u64>(opts, profile),
        //("u64", "binary") => load_and_spawn::<u64, db::Binary>(opts, profile),
        //("binary", "binary") => load_and_spawn::<db::Binary, db::Binary>(opts, profile),
        (_, _) => unreachable!(),
    }
}

//fn load_and_spawn<K, V>(opts: Opt, p: Profile) -> result::Result<(), String>
//where
//    K: 'static + Send + Sync + Clone + Ord + db::Footprint + fmt::Debug,
//    V: 'static + Send + Sync + db::Diff + db::Footprint,
//    <V as db::Diff>::Delta: Send + Sync + db::Footprint,
//    Key: Generate<K>,
//    Value: Generate<V>,
//{
//    let mut rng = SmallRng::from_seed(opts.seed.to_le_bytes());
//
//    let index = Index::<K, V>::new("rdms-llrb-perf", p.spin);
//
//    initial_load(&mut rng, p.clone(), index.clone())?;
//
//    let mut handles = vec![];
//    for j in 0..p.writers {
//        let (mut p, index) = (p.clone(), index.clone());
//        p.reset_readops();
//        let seed = opts.seed + ((j as u128) * 100);
//        let h = thread::spawn(move || incr_load(j, seed, p, index));
//        handles.push(h);
//    }
//    for j in p.writers..(p.writers + p.readers) {
//        let (mut p, index) = (p.clone(), index.clone());
//        p.reset_writeops();
//        let seed = opts.seed + ((j as u128) * 100);
//        let h = thread::spawn(move || incr_load(j, seed, p, index));
//        handles.push(h);
//    }
//
//    for handle in handles.into_iter() {
//        handle.join().unwrap().unwrap()
//    }
//
//    print!("rdms-perf: iterating ... ");
//    let (elapsed, n) = {
//        let start = time::Instant::now();
//        let n: usize = index.iter().unwrap().map(|_| 1_usize).sum();
//        assert!(n == index.len(), "{} != {}", n, index.len());
//        (start.elapsed(), n)
//    };
//    println!("{} items, took {:?}", n, elapsed);
//
//    print!("rdms-perf: ranging ... ");
//    let (elapsed, n) = {
//        let start = time::Instant::now();
//        let n: usize = index.range(..).unwrap().map(|_| 1_usize).sum();
//        assert!(n == index.len(), "{} != {}", n, index.len());
//        (start.elapsed(), n)
//    };
//    println!("{} items, took {:?}", n, elapsed);
//
//    print!("rdms-perf: reverse iter ... ");
//    let (elapsed, n) = {
//        let start = time::Instant::now();
//        let n: usize = index.reverse(..).unwrap().map(|_| 1_usize).sum();
//        assert!(n == index.len(), "{} != {}", n, index.len());
//        (start.elapsed(), n)
//    };
//    println!("{} items, took {:?}", n, elapsed);
//
//    println!("rdms-perf: index latest-seqno:{}", index.to_seqno());
//    println!("rdms-perf: index deleted_count:{}", index.deleted_count());
//
//    println!("rdms-perf: stats {}", index.to_stats().unwrap().to_json());
//
//    if p.validate {
//        print!("rdms-perf: validating {} items in index ... ", index.len());
//        index.validate().unwrap();
//        println!("ok");
//    }
//
//    index.purge().unwrap();
//
//    Ok(())
//}

fn initial_index<K, V, B>(
    seed: u128,
    p: &Profile,
    bitmap: B,
) -> result::Result<(), String>
where
    K: Clone + Ord + Hash + db::Footprint + fmt::Debug + IntoCbor,
    V: Clone + db::Footprint + fmt::Debug + IntoCbor + db::Diff + IntoCbor,
    <V as db::Diff>::Delta: db::Footprint + IntoCbor + FromCbor,
    B: db::Bloom,
    rand::distributions::Standard: rand::distributions::Distribution<K>,
    rand::distributions::Standard: rand::distributions::Distribution<V>,
{
    let appmd = "rdms-robt-perf-initial".as_bytes().to_vec();
    let p_init = p.initial.clone();
    let mdb = llrb::load_index(
        seed,
        p_init.sets,
        p_init.ins,
        p_init.rems,
        p_init.dels,
        None,
    );
    let seqno = Some(mdb.to_seqno());

    let elapsed = {
        let start = time::Instant::now();
        let config = p.to_initial_config();

        let mut build: robt::Builder<K, V> =
            robt::Builder::initial(config, appmd.to_vec()).unwrap();
        build
            .build_index(
                mdb.iter().unwrap().map(|e: db::Entry<K, V>| Ok(e)),
                bitmap,
                seqno,
            )
            .unwrap();
        start.elapsed()
    };

    println!("Took {:?} for initial build {} items", elapsed, mdb.len());
    Ok(())
}

fn incr_index<K, V, B>(seed: u128, p: &Profile, bitmap: B) -> result::Result<(), String>
where
    K: Clone + Ord + Hash + db::Footprint + fmt::Debug + IntoCbor + FromCbor,
    V: Clone + db::Diff + db::Footprint + fmt::Debug + IntoCbor + FromCbor,
    <V as db::Diff>::Delta: db::Footprint + IntoCbor + FromCbor,
    B: Clone + db::Bloom,
    rand::distributions::Standard: rand::distributions::Distribution<K>,
    rand::distributions::Standard: rand::distributions::Distribution<V>,
{
    let mut index = {
        let dir: &ffi::OsStr = p.initial.robt.dir.as_ref();
        robt::Index::open(dir, &p.initial.robt.name).unwrap()
    };
    let config = p.to_initial_config();

    for (i, p_incr) in p.incrs.iter().enumerate() {
        let mut config = config.clone();
        config.name = p_incr.name.clone();

        let appmd = format!("rdms-robt-perf-incremental-{}", i)
            .as_bytes()
            .to_vec();
        let seqno = Some(index.to_seqno());

        let mdb = llrb::load_index(
            seed,
            p_incr.sets,
            p_incr.ins,
            p_incr.rems,
            p_incr.dels,
            seqno,
        );

        let elapsed = {
            let start = time::Instant::now();
            let mut build = index
                .try_clone()
                .unwrap()
                .incremental(&config.dir, &config.name, appmd)
                .unwrap();
            build
                .build_index(
                    mdb.iter().unwrap().map(|e: db::Entry<K, V>| Ok(e)),
                    bitmap.clone(),
                    seqno,
                )
                .unwrap();
            start.elapsed()
        };
        println!(
            "Took {:?} for incremental build {} items",
            elapsed,
            index.len()
        );

        index = if p_incr.compact {
            config.name = p_incr.compact_name.clone();
            let start = time::Instant::now();
            let cindex = index
                .compact(config, bitmap.clone(), db::Cutoff::Mono)
                .unwrap();
            let elapsed = start.elapsed();
            println!(
                "Took {:?} for compact build {} items",
                elapsed,
                cindex.len()
            );
            cindex
        } else {
            index
        }
    }
    Ok(())
}

//{
//    let mut handles = vec![];
//    for i in 0..n_readers {
//        let prefix = prefix.to_string();
//        let (cnf, mdb, appmd) = (config.clone(), mdb.clone(), appmd.to_vec());
//        let seed = seed + ((i as u128) * 10);
//        handles.push(thread::spawn(move || {
//            read_thread::<K, B>(prefix, i, seed, cnf, mdb, appmd)
//        }));
//    }
//
//    for handle in handles.into_iter() {
//        handle.join().unwrap();
//    }
//
//    mdb
//}

//fn initial_load<K, V>(
//    rng: &mut SmallRng,
//    p: Profile,
//    index: Index<K, V>,
//) -> result::Result<(), String>
//where
//    K: 'static + Send + Sync + Clone + Ord + db::Footprint,
//    V: 'static + Send + Sync + db::Diff + db::Footprint,
//    <V as db::Diff>::Delta: Send + Sync + db::Footprint,
//    Key: Generate<K>,
//    Value: Generate<V>,
//{
//    let start = time::Instant::now();
//    for _i in 0..p.loads {
//        index.set(p.key.gen(rng), p.value.gen(rng)).unwrap();
//    }
//
//    println!(
//        "rdms-perf: loaded {} items in {:?}",
//        p.loads,
//        start.elapsed()
//    );
//
//    Ok(())
//}

fn read_load<K, V, B>(
    j: usize,
    seed: u128,
    p: Profile,
    mut index: robt::Index<K, V, B>,
) -> result::Result<(), String>
where
    K: 'static + Send + Sync + Clone + Ord + db::Footprint + FromCbor,
    V: 'static + Send + Sync + db::Diff + db::Footprint + FromCbor,
    <V as db::Diff>::Delta: Send + Sync + db::Footprint + FromCbor,
    B: db::Bloom,
    Profile: Generate<K>,
{
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let start = time::Instant::now();
    let total = p.load.gets + p.load.get_versions;
    let (mut gets, mut getvers) = (p.load.gets, p.load.get_versions);

    while (gets + getvers) > 0 {
        let key: K = p.gen_key(&mut rng);
        match rng.gen::<usize>() % (gets + getvers) {
            op if op < gets => {
                index.get(&key).ok();
                gets -= 1;
            }
            _op => {
                index.get_versions(&key).ok();
            }
        }
    }

    println!(
        "rdms-perf: read-load-{} for (gets:{} get_versions:{}) operations took {:?}",
        j,
        p.load.gets,
        p.load.get_versions,
        start.elapsed()
    );

    if p.load.iter {
        do_iter(j, "iter", index.iter(..).unwrap())
    }
    if p.load.iter_versions {
        do_iter(j, "iter_versions", index.iter_versions(..).unwrap())
    }
    if p.load.reverse {
        do_iter(j, "reverse", index.reverse(..).unwrap())
    }
    if p.load.reverse_versions {
        do_iter(j, "reverse_versions", index.reverse(..).unwrap())
    }

    Ok(())
}

fn do_iter<I, K, V>(j: usize, prefix: &str, iter: I)
where
    V: db::Diff,
    I: Iterator<Item = Result<db::Entry<K, V>>>,
{
    let start = time::Instant::now();
    let len: usize = iter
        .map(|e| {
            e.unwrap();
            1
        })
        .sum();
    println!(
        "rdms-perf: read-load-{} took {:?} to {} {} items",
        j,
        start.elapsed(),
        prefix,
        len
    );
}
