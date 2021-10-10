use rand::{rngs::SmallRng, Rng, SeedableRng};
use serde::Deserialize;

use std::{fmt, result, thread, time};

use rdms::{
    db::{self, ToJson},
    llrb::Index as Mdb,
    robt::Index,
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
    iter: usize,
    iter_versions: usize,
    reverse: usize,
    reverse_versions: usize,
    readers: usize,
    validate: bool,
}

impl Default for Load {
    fn default() -> Load {
        Load {
            gets: 1_000_000,
            get_versions: 0,
            iter: 1,
            iter_versions: 0,
            reverse: 1,
            reverse_versions: 0,
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
        db::Binary(format!("{:0width$}", key, width = size).as_bytes().to_vec())
    }

    fn gen_value(&self, rng: &mut SmallRng) -> db::Binary {
        let (val, size) = (rng.gen::<u64>(), self.value_size);
        db::Binary(format!("{:0width$}", val, width = size).as_bytes().to_vec())
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

//fn initial_index<K, B>(p: &Profile, config: Config) -> Result<Config, String> {
//    let appmd = "rdms-robt-perf-initial".as_bytes().to_vec();
//    let mdb = llrb::load_index(seed, p.sets, p.ins, p.rems, p.dels, None);
//    let seqno = Some(mdb.to_seqno());
//
//    let elapsed = {
//        let start = time::Instance::now(),
//
//        let mut build = Builder::initial(config.clone(), appmd.to_vec()).unwrap();
//        build
//            .build_index(mdb.iter().unwrap().map(|e| Ok(e)), bitmap, seqno)
//            .unwrap();
//        start.elapsed()
//    };
//
//    println!("Took {} to build {} items", elapsed, mdb.len())
//    Ok(config)
//}

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

//fn incr_load<K, V>(
//    j: usize,
//    seed: u128,
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
//    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
//
//    let start = time::Instant::now();
//    let total = p.sets + p.ins + p.rems + p.dels + p.gets;
//    let (mut sets, mut ins, mut rems, mut dels, mut gets) =
//        (p.sets, p.ins, p.rems, p.dels, p.gets);
//
//    while (sets + ins + rems + dels + gets) > 0 {
//        let key: K = p.key.gen(&mut rng);
//        match rng.gen::<usize>() % (sets + ins + rems + dels + gets) {
//            op if p.cas && op < sets => {
//                let cas = rng.gen::<u64>() % (total as u64);
//                let val: V = p.value.gen(&mut rng);
//                index.set_cas(key, val, cas).unwrap();
//                sets -= 1;
//            }
//            op if op < p.sets => {
//                let val: V = p.value.gen(&mut rng);
//                index.set(key, val).unwrap();
//                sets -= 1;
//            }
//            op if p.cas && op < (p.sets + p.ins) => {
//                let cas = rng.gen::<u64>() % (total as u64);
//                let val: V = p.value.gen(&mut rng);
//                index.insert_cas(key, val, cas).unwrap();
//                ins -= 1;
//            }
//            op if op < (p.sets + p.ins) => {
//                let val: V = p.value.gen(&mut rng);
//                index.insert(key, val).unwrap();
//                ins -= 1;
//            }
//            op if p.cas && op < (p.sets + p.ins + p.rems) => {
//                let cas = rng.gen::<u64>() % (total as u64);
//                index.remove_cas(&key, cas).unwrap();
//                rems -= 1;
//            }
//            op if op < (p.sets + p.ins + p.rems) => {
//                index.remove(&key).unwrap();
//                rems -= 1;
//            }
//            op if p.cas && op < (p.sets + p.ins + p.rems + p.dels) => {
//                let cas = rng.gen::<u64>() % (total as u64);
//                index.delete_cas(&key, cas).unwrap();
//                dels -= 1;
//            }
//            op if op < (p.sets + p.ins + p.rems + p.dels) => {
//                index.delete(&key).unwrap();
//                dels -= 1;
//            }
//            _op => {
//                index.get(&key).ok();
//                gets -= 1;
//            }
//        }
//    }
//
//    println!(
//        concat!(
//            "rdms-perf: incremental-{} for (sets:{} ins:{} rems:{} dels:{} gets:{}) ",
//            "operations took {:?}",
//        ),
//        j,
//        p.sets,
//        p.ins,
//        p.rems,
//        p.dels,
//        p.gets,
//        start.elapsed()
//    );
//
//    Ok(())
//}
