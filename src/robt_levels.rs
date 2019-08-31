use std::sync::{atomic::AtomicPtr, atomic::Ordering, mpsc, Arc};
use std::{marker, mem, thread};

use crate::core::{Diff, Footprint, Index, Result, Serialize};
use crate::error::Error;
use crate::robt::Config;
use crate::robt_snap::Snapshot;

struct Levels<K, V>(AtomicPtr<Arc<Vec<Snapshot<K, V>>>>)
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize;

impl<K, V> Levels<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
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

pub(crate) struct Robts<K, V, M>
where
    K: 'static + Sync + Send + Clone + Ord + Serialize + Footprint,
    V: 'static + Sync + Send + Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: 'static + Sync + Send + Index<K, V>,
{
    config: Config,
    mem_ratio: f64,
    disk_ratio: f64,
    levels: Levels<K, V>,
    todisk: MemToDisk<K, V, M>,      // encapsulates a thread
    tocompact: DiskCompact<K, V, M>, // encapsulates a thread
}

// new instance of multi-level Robt indexes.
impl<K, V, M> Robts<K, V, M>
where
    K: 'static + Sync + Send + Clone + Ord + Serialize + Footprint,
    V: 'static + Sync + Send + Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: 'static + Sync + Send + Index<K, V>,
{
    const MEM_RATIO: f64 = 0.2;
    const DISK_RATIO: f64 = 0.5;

    pub(crate) fn new(config: Config) -> Robts<K, V, M> {
        Robts {
            config: config.clone(),
            mem_ratio: Self::MEM_RATIO,
            disk_ratio: Self::DISK_RATIO,
            levels: Levels::new(),
            todisk: MemToDisk::new(config.clone()),
            tocompact: DiskCompact::new(config.clone()),
        }
    }

    pub(crate) fn set_mem_ratio(mut self, ratio: f64) -> Robts<K, V, M> {
        self.mem_ratio = ratio;
        self
    }

    pub(crate) fn set_disk_ratio(mut self, ratio: f64) -> Robts<K, V, M> {
        self.disk_ratio = ratio;
        self
    }
}

// add new levels.
impl<K, V, M> Robts<K, V, M>
where
    K: 'static + Sync + Send + Clone + Ord + Serialize + Footprint,
    V: 'static + Sync + Send + Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: 'static + Sync + Send + Index<K, V>,
{
    pub(crate) fn flush_to_disk(
        &mut self,
        index: Arc<M>, // full table scan over mem-index
        metadata: Vec<u8>,
    ) -> Result<()> {
        let _resp = self.todisk.send(Request::MemFlush {
            index,
            metadata,
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        })?;
        Ok(())
    }
}

enum Request<K, V, M>
where
    K: 'static + Sync + Send + Clone + Ord + Serialize + Footprint,
    V: 'static + Sync + Send + Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: 'static + Sync + Send + Index<K, V>,
{
    MemFlush {
        index: Arc<M>,
        metadata: Vec<u8>,
        phantom_key: marker::PhantomData<K>,
        phantom_val: marker::PhantomData<V>,
    },
}

enum Response {
    Ok,
}

struct MemToDisk<K, V, M>
where
    K: 'static + Sync + Send + Clone + Ord + Serialize + Footprint,
    V: 'static + Sync + Send + Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: 'static + Sync + Send + Index<K, V>,
{
    config: Config,
    thread: thread::JoinHandle<Result<()>>,
    tx: mpsc::SyncSender<(Request<K, V, M>, mpsc::SyncSender<Response>)>,
}

impl<K, V, M> MemToDisk<K, V, M>
where
    K: 'static + Sync + Send + Clone + Ord + Serialize + Footprint,
    V: 'static + Sync + Send + Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: 'static + Sync + Send + Index<K, V>,
{
    fn new(config: Config) -> MemToDisk<K, V, M> {
        let (tx, rx) = mpsc::sync_channel(1);
        let conf = config.clone();
        let thread = thread::spawn(move || thread_mem_to_disk(conf, rx));
        MemToDisk { config, thread, tx }
    }

    fn send(&mut self, req: Request<K, V, M>) -> Result<Response> {
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

fn thread_mem_to_disk<K, V, M>(
    _config: Config,
    _rx: mpsc::Receiver<(Request<K, V, M>, mpsc::SyncSender<Response>)>,
) -> Result<()>
where
    K: 'static + Sync + Send + Clone + Ord + Serialize + Footprint,
    V: 'static + Sync + Send + Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: 'static + Sync + Send + Index<K, V>,
{
    // TBD
    Ok(())
}

struct DiskCompact<K, V, M>
where
    K: 'static + Sync + Send + Clone + Ord + Serialize + Footprint,
    V: 'static + Sync + Send + Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: 'static + Sync + Send + Index<K, V>,
{
    config: Config,
    thread: thread::JoinHandle<Result<()>>,
    tx: mpsc::SyncSender<(Request<K, V, M>, mpsc::SyncSender<Response>)>,
}

impl<K, V, M> DiskCompact<K, V, M>
where
    K: 'static + Sync + Send + Clone + Ord + Serialize + Footprint,
    V: 'static + Sync + Send + Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: 'static + Sync + Send + Index<K, V>,
{
    fn new(config: Config) -> DiskCompact<K, V, M> {
        let (tx, rx) = mpsc::sync_channel(1);
        let conf = config.clone();
        let thread = thread::spawn(move || thread_disk_compact(conf, rx));
        DiskCompact { config, thread, tx }
    }

    fn send(&mut self, req: Request<K, V, M>) -> Result<Response> {
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

fn thread_disk_compact<K, V, M>(
    _config: Config,
    _rx: mpsc::Receiver<(Request<K, V, M>, mpsc::SyncSender<Response>)>,
) -> Result<()>
where
    K: 'static + Sync + Send + Clone + Ord + Serialize + Footprint,
    V: 'static + Sync + Send + Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: 'static + Sync + Send + Index<K, V>,
{
    // TBD
    Ok(())
}
