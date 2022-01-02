use serde::Deserialize;

use std::time;

use rdms::{util, wral, Result};

use crate::cmd_perf::Opt;

// Command line options.
#[derive(Clone, Deserialize)]
pub struct Profile {
    name: String,
    ops: usize,
    payload: usize,
    threads: usize,
    journal_limit: usize,
    nosync: bool,
}

impl Default for Profile {
    fn default() -> Profile {
        Profile {
            name: "wral-perf".to_string(),
            ops: 1_000_000,
            payload: 32,
            threads: 8,
            journal_limit: 10_000_000,
            nosync: true,
        }
    }
}

pub fn perf(opts: Opt) -> Result<()> {
    let profile: Profile = util::files::load_toml(&opts.profile)?;
    load_and_spawn(opts, profile)
}

fn load_and_spawn(opts: Opt, p: Profile) -> Result<()> {
    use std::{env, path::PathBuf};

    let wal = {
        let dir: PathBuf = vec![env::temp_dir(), "wral-perf".into()]
            .into_iter()
            .collect();

        let config = wral::Config::new(dir.as_os_str(), &p.name)
            .set_journal_limit(p.journal_limit)
            .set_fsync(!p.nosync);

        println!("{:?}", config);

        wral::Wal::create(config, wral::NoState).unwrap()
    };

    let mut writers = vec![];
    for id in 0..p.threads {
        let (wal, p, seed) = (wal.clone(), p.clone(), opts.seed);
        writers.push(std::thread::spawn(move || writer(id, wal, p, seed)));
    }

    let mut entries: Vec<Vec<wral::Entry>> = vec![];
    for handle in writers {
        entries.push(handle.join().unwrap());
    }
    let mut entries: Vec<wral::Entry> = entries.into_iter().flatten().collect();
    entries.sort_by_key(|a| a.to_seqno());

    let n = entries.len() as u64;
    let sum = entries.iter().map(|e| e.to_seqno()).sum::<u64>();
    assert_eq!(sum, (n * (n + 1)) / 2);

    let mut readers = vec![];
    for id in 0..p.threads {
        let wal = wal.clone();
        let entries = entries.clone();
        readers.push(std::thread::spawn(move || reader(id, wal, entries)));
    }

    for handle in readers {
        handle.join().unwrap();
    }

    wal.purge().unwrap();
    Ok(())
}

fn writer(id: usize, wal: wral::Wal, p: Profile, _seed: u64) -> Vec<wral::Entry> {
    let start = time::Instant::now();

    let mut entries = vec![];
    let op = vec![0; p.payload];
    for _i in 0..p.ops {
        let seqno = wal.add_op(&op).unwrap();
        entries.push(wral::Entry::new(seqno, op.clone()));
    }

    println!(
        "w-{:02} took {:?} to write {} ops",
        id,
        start.elapsed(),
        p.ops
    );
    entries
}

fn reader(id: usize, wal: wral::Wal, entries: Vec<wral::Entry>) {
    let start = time::Instant::now();
    let items: Vec<wral::Entry> = wal.iter().unwrap().map(|x| x.unwrap()).collect();
    assert_eq!(items, entries);

    println!(
        "r-{:02} took {:?} to iter {} ops",
        id,
        start.elapsed(),
        items.len()
    );
}
