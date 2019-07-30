// TODO: bogn mem/disk indexes, change from M,D type parameters
// to trait objects.

use std::sync::{atomic::AtomicPtr, atomic::Ordering, mpsc, Arc};
use std::{mem, thread};

use crate::core::{self, Diff, Result, Serialize};
use crate::error::Error;
use crate::robt_config::Config;
use crate::robt_snap::Snapshot;

struct Levels<K, V>(AtomicPtr<Arc<Vec<Snapshot<K, V>>>>)
where
    K: 'static + Clone + Ord + Serialize,
    V: 'static + Clone + Diff + Serialize,
    <V as Diff>::D: Serialize;

impl<K, V> Levels<K, V>
where
    K: 'static + Clone + Ord + Serialize,
    V: 'static + Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    fn new() -> Levels<K, V> {
        Levels(AtomicPtr::new(Box::leak(Box::new(Arc::new(vec![])))))
    }

    fn get_snapshots(&self) -> Arc<Vec<Snapshot<K, V>>> {
        unsafe { Arc::clone(self.0.load(Ordering::Relaxed).as_ref().unwrap()) }
    }

    fn compare_swap_snapshots(&self, new_snapshots: Vec<Snapshot<K, V>>) {
        let _olds = unsafe { Box::from_raw(self.0.load(Ordering::Relaxed)) };
        let new_snapshots = Box::leak(Box::new(Arc::new(new_snapshots)));
        self.0.store(new_snapshots, Ordering::Relaxed);
    }
}

pub(crate) struct Robts<K, V>
where
    K: 'static + Clone + Ord + Serialize,
    V: 'static + Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    config: Config,
    mem_ratio: f64,
    disk_ratio: f64,
    levels: Levels<K, V>,
    todisk: MemToDisk<K, V>,
    tocompact: DiskCompact<K, V>,
}

// new instance of multi-level Robt indexes.
impl<K, V> Robts<K, V>
where
    K: 'static + Clone + Ord + Serialize,
    V: 'static + Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    const MEM_RATIO: f64 = 0.2;
    const DISK_RATIO: f64 = 0.5;

    pub(crate) fn new(config: Config) -> Robts<K, V> {
        Robts {
            config: config.clone(),
            mem_ratio: Self::MEM_RATIO,
            disk_ratio: Self::DISK_RATIO,
            levels: Levels::new(),
            todisk: MemToDisk::new(config.clone()),
            tocompact: DiskCompact::new(config.clone()),
        }
    }

    pub(crate) fn set_mem_ratio(mut self, ratio: f64) -> Robts<K, V> {
        self.mem_ratio = ratio;
        self
    }

    pub(crate) fn set_disk_ratio(mut self, ratio: f64) -> Robts<K, V> {
        self.disk_ratio = ratio;
        self
    }
}

// add new levels.
impl<K, V> Robts<K, V>
where
    K: 'static + Clone + Ord + Serialize,
    V: 'static + Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    pub(crate) fn flush_to_disk(
        &mut self,
        iter: core::IndexIter<K, V>, // full table scan over mem-index
        metadata: Vec<u8>,
    ) -> Result<()> {
        let _resp = self.todisk.send(Request::MemFlush { iter, metadata })?;
        Ok(())
    }
}

enum Request<K, V>
where
    K: 'static + Clone + Ord + Serialize,
    V: 'static + Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    MemFlush {
        iter: core::IndexIter<K, V>,
        metadata: Vec<u8>,
    },
}

enum Response {
    Ok,
}

struct MemToDisk<K, V>
where
    K: 'static + Clone + Ord + Serialize,
    V: 'static + Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    config: Config,
    thread: thread::JoinHandle<Result<()>>,
    tx: mpsc::SyncSender<(Request<K, V>, mpsc::SyncSender<Response>)>,
}

impl<K, V> MemToDisk<K, V>
where
    K: 'static + Clone + Ord + Serialize,
    V: 'static + Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    fn new(config: Config) -> MemToDisk<K, V> {
        let (tx, rx) = mpsc::sync_channel(1);
        let conf = config.clone();
        let thread = thread::spawn(move || thread_mem_to_disk(conf, rx));
        MemToDisk { config, thread, tx }
    }

    fn send(&mut self, req: Request<K, V>) -> Result<Response> {
        let (tx, rx) = mpsc::sync_channel(0);
        self.tx.send((req, tx))?;
        Ok(rx.recv()?)
    }

    fn close_wait(self) -> Result<()> {
        mem::drop(self.tx);
        match self.thread.join() {
            Ok(res) => res,
            Err(err) => match err.downcast_ref::<String>() {
                Some(msg) => Err(Error::ThreadFail(msg.to_string())),
                None => Err(Error::ThreadFail("unknown error".to_string())),
            },
        }
    }
}

fn thread_mem_to_disk<K, V>(
    _config: Config,
    _rx: mpsc::Receiver<(Request<K, V>, mpsc::SyncSender<Response>)>,
) -> Result<()>
where
    K: 'static + Clone + Ord + Serialize,
    V: 'static + Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    // TBD
    Ok(())
}

struct DiskCompact<K, V>
where
    K: 'static + Clone + Ord + Serialize,
    V: 'static + Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    config: Config,
    thread: thread::JoinHandle<Result<()>>,
    tx: mpsc::SyncSender<(Request<K, V>, mpsc::SyncSender<Response>)>,
}

impl<K, V> DiskCompact<K, V>
where
    K: 'static + Clone + Ord + Serialize,
    V: 'static + Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    fn new(config: Config) -> DiskCompact<K, V> {
        let (tx, rx) = mpsc::sync_channel(1);
        let conf = config.clone();
        let thread = thread::spawn(move || thread_disk_compact(conf, rx));
        DiskCompact { config, thread, tx }
    }

    fn send(&mut self, req: Request<K, V>) -> Result<Response> {
        let (tx, rx) = mpsc::sync_channel(0);
        self.tx.send((req, tx))?;
        Ok(rx.recv()?)
    }

    fn close_wait(self) -> Result<()> {
        mem::drop(self.tx);
        match self.thread.join() {
            Ok(res) => res,
            Err(err) => match err.downcast_ref::<String>() {
                Some(msg) => Err(Error::ThreadFail(msg.to_string())),
                None => Err(Error::ThreadFail("unknown error".to_string())),
            },
        }
    }
}

fn thread_disk_compact<K, V>(
    _config: Config,
    _rx: mpsc::Receiver<(Request<K, V>, mpsc::SyncSender<Response>)>,
) -> Result<()>
where
    K: 'static + Clone + Ord + Serialize,
    V: 'static + Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    // TBD
    Ok(())
}
