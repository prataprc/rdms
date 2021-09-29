use lmdb::{self, Cursor, Transaction};
use rand::{rngs::SmallRng, Rng, SeedableRng};

use std::{io, result, sync::Arc, thread, time};

use crate::{get_property, load_profile, Opt};

const DEFAULT_KEY_SIZE: i64 = 16;
const DEFAULT_VAL_SIZE: i64 = 16;

#[derive(Clone)]
pub struct Profile {
    name: String,
    dir: String,
    key_size: usize,
    val_size: usize,
    load_batch: usize,
    loads: usize,
    sets: usize,
    rems: usize,
    gets: usize,
    writers: usize,
    readers: usize,
}

impl Default for Profile {
    fn default() -> Profile {
        Profile {
            name: "perf-lmdb".to_string(),
            dir: String::default(),
            key_size: DEFAULT_KEY_SIZE as usize,
            val_size: DEFAULT_VAL_SIZE as usize,
            load_batch: 100_000,
            loads: 1_000_000,
            sets: 1_000_000,
            rems: 100_000,
            gets: 1_000_000,
            writers: 1,
            readers: 1,
        }
    }
}

impl Profile {
    fn from_toml(v: toml::Value) -> result::Result<Profile, String> {
        let p: Profile = Default::default();
        let ks = get_property!(v, "key_size", as_integer, p.key_size as i64) as usize;
        let vs = get_property!(v, "value_size", as_integer, p.val_size as i64) as usize;

        let p = Profile {
            name: get_property!(v, "name", as_str, p.name.as_str()).to_string(),
            dir: get_property!(v, "dir", as_str, p.dir.as_str()).to_string(),
            key_size: ks,
            val_size: vs,
            load_batch: get_property!(v, "cas", as_integer, p.load_batch as i64) as usize,
            loads: get_property!(v, "loads", as_integer, p.loads as i64) as usize,
            sets: get_property!(v, "sets", as_integer, p.sets as i64) as usize,
            rems: get_property!(v, "rems", as_integer, p.rems as i64) as usize,
            gets: get_property!(v, "gets", as_integer, p.gets as i64) as usize,
            writers: get_property!(v, "writers", as_integer, p.writers as i64) as usize,
            readers: get_property!(v, "readers", as_integer, p.loads as i64) as usize,
        };

        Ok(p)
    }

    fn reset_writeops(&mut self) {
        self.sets = 0;
        self.rems = 0;
    }

    fn reset_readops(&mut self) {
        self.gets = 0;
    }
}

pub fn perf(opts: Opt) -> result::Result<(), String> {
    let profile: Profile =
        Profile::from_toml(load_profile(&opts)?).expect("invalid profile properties");
    load_and_spawn(opts, profile)
}

fn load_and_spawn(opts: Opt, p: Profile) -> Result<(), String> {
    let (env, db) = init_lmdb(&p);
    initial_load(opts.seed, &p, env, db)?;

    let (env, db) = open_lmdb(&p);
    let mut env = Arc::new(env);

    let mut handles = vec![];
    for j in 0..p.writers {
        let (seed, mut pp, envv) = (opts.seed, p.clone(), Arc::clone(&env));
        pp.reset_readops();
        handles.push(thread::spawn(move || incr_load(j, seed, pp, envv, db)));
    }
    for j in 0..p.readers {
        let (seed, mut pp, envv) = (opts.seed, p.clone(), Arc::clone(&env));
        pp.reset_writeops();
        handles.push(thread::spawn(move || incr_load(j, seed, pp, envv, db)));
    }
    for handle in handles.into_iter() {
        handle.join().unwrap().unwrap()
    }

    let stats = env.stat().unwrap();

    unsafe { Arc::get_mut(&mut env).unwrap().close_db(db) };
    env.sync(true).unwrap();

    print!("rdms-perf: iterating ... ");
    let (elapsed, n) = {
        let start = time::Instant::now();
        let (env, db) = open_lmdb(&p);
        let txn = env.begin_ro_txn().unwrap();
        let iter = txn.open_ro_cursor(db).unwrap().iter();
        let n: usize = iter.map(|_| 1_usize).sum();
        (start.elapsed(), n)
    };
    println!("{} items, took {:?}", n, elapsed);

    println!(
        concat!(
            "rdms-perf: stats ",
            "page_size:{} depth:{} branch_pages:{} leaf_pages:{} overflow_pages:{} ",
            "entries:{}"
        ),
        stats.page_size(),
        stats.depth(),
        stats.branch_pages(),
        stats.leaf_pages(),
        stats.overflow_pages(),
        stats.entries(),
    );

    Ok(())
}

fn initial_load(
    seed: u128,
    p: &Profile,
    mut env: lmdb::Environment,
    db: lmdb::Database, // index
) -> result::Result<(), String> {
    print!("rdms-perf: initial-load ...");

    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let mut txn = env.begin_rw_txn().unwrap();
    let write_flags: lmdb::WriteFlags = Default::default();

    let start = time::Instant::now();
    for _i in 0..p.loads {
        let key = format!("{:0width$}", rng.gen::<u64>(), width = p.key_size);
        let value = format!("{:0width$}", rng.gen::<u64>(), width = p.val_size);
        txn.put(db, &key, &value, write_flags.clone()).unwrap();
    }

    txn.commit().unwrap();

    unsafe { env.close_db(db) };
    env.sync(true).unwrap();

    let stat = {
        let (env, _) = open_lmdb(&p);
        env.stat().unwrap()
    };

    println!(
        "load:{} index.len:{} elapsed:{:?}",
        p.loads,
        stat.entries(),
        start.elapsed()
    );

    Ok(())
}

fn incr_load(
    j: usize,
    seed: u128,
    p: Profile,
    env: Arc<lmdb::Environment>,
    db: lmdb::Database, // index
) -> result::Result<(), String> {
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let write_flags: lmdb::WriteFlags = Default::default();
    let start = time::Instant::now();
    let (mut sets, mut rems, mut gets) = (p.sets, p.rems, p.gets);

    while (sets + rems + gets) > 0 {
        let key = format!("{:0width$}", rng.gen::<u64>(), width = p.key_size);
        match rng.gen::<usize>() % (sets + rems + gets) {
            op if op < sets => {
                let value = format!("{:0width$}", rng.gen::<u64>(), width = p.val_size);
                let mut txn = env.begin_rw_txn().unwrap();
                txn.put(db, &key, &value, write_flags.clone()).unwrap();
                txn.commit().unwrap();
                sets -= 1;
            }
            op if op < (sets + rems) => {
                let mut txn = env.begin_rw_txn().unwrap();
                txn.del(db, &key, None /*data*/).ok();
                txn.commit().unwrap();
                rems -= 1;
            }
            _op => {
                let txn = env.begin_ro_txn().unwrap();
                txn.get(db, &key).ok();
                gets -= 1;
            }
        };
    }

    println!(
        concat!(
            "rdms-perf: incremental-{} for (sets:{} rems:{} gets:{}) ",
            "operations took {:?}",
        ),
        j,
        p.sets,
        p.rems,
        p.gets,
        start.elapsed()
    );

    Ok(())
}

fn init_lmdb(p: &Profile) -> (lmdb::Environment, lmdb::Database) {
    // setup directory
    match std::fs::remove_dir_all(&p.dir) {
        Ok(()) => (),
        Err(ref err) if err.kind() == io::ErrorKind::NotFound => (),
        Err(err) => panic!("{:?}", err),
    }
    let path = std::path::Path::new(&p.dir).join(&p.name);
    std::fs::create_dir_all(&path).unwrap();

    // create the environment
    let mut flags = lmdb::EnvironmentFlags::empty();
    flags.insert(lmdb::EnvironmentFlags::NO_SYNC);
    flags.insert(lmdb::EnvironmentFlags::NO_META_SYNC);
    let env = lmdb::Environment::new()
        .set_flags(flags)
        .set_map_size(10_000_000_000)
        .open(&path)
        .unwrap();

    let db = env.open_db(None).unwrap();

    (env, db)
}

fn open_lmdb(p: &Profile) -> (lmdb::Environment, lmdb::Database) {
    let path = std::path::Path::new(&p.dir).join(&p.name);

    // create the environment
    let mut flags = lmdb::EnvironmentFlags::empty();
    flags.insert(lmdb::EnvironmentFlags::NO_SYNC);
    flags.insert(lmdb::EnvironmentFlags::NO_META_SYNC);
    flags.insert(lmdb::EnvironmentFlags::NO_TLS);
    let env = {
        let mut env = lmdb::Environment::new();
        env.set_flags(flags).set_map_size(10_000_000_000);
        if p.readers > 0 {
            env.set_max_readers(p.readers as u32);
        }
        env.open(&path).unwrap()
    };

    let db = env.open_db(None).unwrap();

    (env, db)
}
