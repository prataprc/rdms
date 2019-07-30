use std::sync::mpsc;
use std::thread;

use crate::core::{Diff, Result, Serialize};
use crate::robt_config::Config;
use crate::robt_snap::Snapshot;

pub(crate) struct Robts<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    config: Config,
    levels: Vec<Snapshot<K, V>>,
    todisk: MemToDisk,
    tocompact: DiskCompact,
}

// new instance of multi-level Robt indexes.
impl<K, V> Robts<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    pub(crate) fn new(config: Config) -> Robts<K, V> {
        let todisk = MemToDisk::new(config.clone());
        let tocompact = DiskCompact::new(config.clone());
        Robts {
            todisk,
            tocompact,
            config,
            levels: vec![],
        }
    }
}

// add new levels.
impl<K, V> Robts<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    pub(crate) fn build<I>(iter: I, metadata: Vec<u8>) -> Result<()> {
        // TBD
        Ok(())
    }
}

enum Request {
    Flush,
    Compact,
}

struct MemToDisk {
    config: Config,
    thread: thread::JoinHandle<Result<()>>,
    tx: mpsc::SyncSender<Request>,
}

impl MemToDisk {
    fn new(config: Config) -> MemToDisk {
        let (tx, rx) = mpsc::sync_channel(1);
        let conf = config.clone();
        let thread = thread::spawn(move || thread_mem_to_disk(conf, rx));
        MemToDisk { config, thread, tx }
    }
}

fn thread_mem_to_disk(
    _config: Config,
    _rx: mpsc::Receiver<Request>, // requests
) -> Result<()> {
    // TBD
    Ok(())
}

struct DiskCompact {
    config: Config,
    thread: thread::JoinHandle<Result<()>>,
    tx: mpsc::SyncSender<Request>,
}

impl DiskCompact {
    fn new(config: Config) -> DiskCompact {
        let (tx, rx) = mpsc::sync_channel(1);
        let conf = config.clone();
        let thread = thread::spawn(move || thread_disk_compact(conf, rx));
        DiskCompact { config, thread, tx }
    }
}

fn thread_disk_compact(
    _config: Config,
    _rx: mpsc::Receiver<Request>, // requests
) -> Result<()> {
    // TBD
    Ok(())
}
