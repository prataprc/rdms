//! Module `shllrb` implement an ordered set of index using Llrb shards.

use std::{
    borrow::Borrow,
    cmp,
    convert::TryFrom,
    ffi, fmt,
    hash::Hash,
    mem,
    ops::{Bound, RangeBounds},
    result,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc, Arc, Mutex, MutexGuard,
    },
    thread, time,
};

use crate::{
    core::{
        self, CommitIterator, Diff, Entry, Footprint, Index, IndexIter, Reader, Result, Validate,
        WriteIndexFactory, Writer,
    },
    error::Error,
    llrb::{Llrb, LlrbReader, LlrbWriter, Stats as LlrbStats},
    scans::CommitWrapper,
    types::Empty,
};
use log::{debug, error, info, warn};

/// Periodic interval to manage auto-sharding. Refer to auto_shard() for
/// more details.
const SHARD_INTERVAL: time::Duration = time::Duration::from_secs(10);

/// Periodic interval to retry API operation. Happens when a shard is not
/// in Active state.
const RETRY_INTERVAL: time::Duration = time::Duration::from_millis(10);

/// Maximum number of entries in a shard, beyond which a shard shall be split.
const DEFAULT_MAX_ENTRIES: usize = 1_000_000;

// ShardName format.
#[derive(Clone)]
struct ShardName(String);

impl From<(String, usize)> for ShardName {
    fn from((s, shard): (String, usize)) -> ShardName {
        ShardName(format!("{}-shard-{:03}", s, shard))
    }
}

impl TryFrom<ShardName> for (String, usize) {
    type Error = Error;

    fn try_from(name: ShardName) -> Result<(String, usize)> {
        let parts: Vec<&str> = name.0.split('-').collect();
        let err = Error::InvalidFile(format!("not shrobt index"));

        if parts.len() < 3 {
            Err(err)
        } else if parts[parts.len() - 2] != "shard" {
            Err(err)
        } else {
            let shard = parts[parts.len() - 1].parse::<usize>()?;
            let s = parts[..(parts.len() - 2)].join("-");
            Ok((s, shard))
        }
    }
}

impl fmt::Display for ShardName {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for ShardName {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{:?}", self.0)
    }
}

/// ShllrbFactory captures a set of configuration for creating new ShLlrb
/// instances.
///
/// By implementing `WriteIndexFactory` trait this can be
/// used with other, more sophisticated, index implementations.
pub struct ShllrbFactory {
    lsm: bool,
    sticky: bool,
    spin: bool,
    max_shards: usize,
    max_entries: usize,
    interval: time::Duration,
}

/// Create a new factory with initial set of configuration.
///
/// To know more about other configurations supported by the ShllrbFactory
/// refer to its ``set_``, methods.
///
/// * *lsm*, spawn Llrb instances in lsm mode, this will preserve the
///   entire history of all write operations applied on the index-shard.
/// * *max_shards*, maximum number of shards to be allowed within this
///   instance. Auto-sharding will try to balance the shards based
///   on ``max_entries``.
pub fn shllrb_factory(lsm: bool, max_shards: usize) -> ShllrbFactory {
    ShllrbFactory {
        lsm,
        sticky: false,
        spin: true,
        max_shards,
        max_entries: DEFAULT_MAX_ENTRIES,
        interval: SHARD_INTERVAL,
    }
}

/// Configuration methods.
impl ShllrbFactory {
    /// If lsm is _true_, this will preserve the entire history of all write
    /// operations applied on the index-shard. _Default: false_.
    pub fn set_lsm(&mut self, lsm: bool) -> &mut Self {
        self.lsm = lsm;
        self
    }

    /// If spin is _true_, calling thread will spin while waiting for the
    /// latch, otherwise, calling thead will be yielded to OS scheduler.
    /// For more detail refer Llrb::set_spinlatch() method. _Default: false_.
    pub fn set_spinlatch(&mut self, spin: bool) -> &mut Self {
        self.spin = spin;
        self
    }

    /// Create all Llrb instances in sticky mode, refer to Llrb::set_sticky()
    /// for more details. For more detail refer Llrb::set_sticky().
    /// _Default: false_.
    pub fn set_sticky(&mut self, sticky: bool) -> &mut Self {
        self.sticky = sticky;
        self
    }

    /// Set periodic interval for auto-sharding. _Default: 200 seconds_
    pub fn set_interval(&mut self, interval: time::Duration) -> &mut Self {
        self.interval = interval;
        self
    }

    /// Set shard parameters.
    /// * `max_shards`, limit the maximum number of shards. _Default: 2_
    /// * `max_entries` per shard, beyond which shard will be split.
    ///   _Default: 1_000_000_
    pub fn set_shard_config(&mut self, max_shards: usize, max_entries: usize) -> &mut Self {
        self.max_shards = max_shards;
        self.max_entries = max_entries;
        self
    }
}

impl<K, V> WriteIndexFactory<K, V> for ShllrbFactory
where
    K: 'static + Send + Clone + Ord + Footprint,
    V: 'static + Send + Clone + Diff + Footprint,
    <V as Diff>::D: Send,
{
    type I = Box<ShLlrb<K, V>>;

    fn to_type(&self) -> String {
        "shllrb".to_string()
    }

    fn new(&self, name: &str) -> Result<Self::I> {
        let mut index = ShLlrb::new(name);
        index
            .set_lsm(self.lsm)
            .set_spinlatch(self.spin)
            .set_sticky(self.sticky)
            .set_shard_config(self.max_shards, self.max_entries)
            .set_interval(self.interval);
        index.log();
        Ok(index)
    }
}

/// Range partitioned index using [Llrb] shards.
///
/// Refer to `set_*` API for configuring ShLlrb instance.
///
/// [llrb]: https://en.wikipedia.org/wiki/Left-leaning_red-black_tree
pub struct ShLlrb<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    name: String,
    // llrb-options.
    lsm: bool,
    sticky: bool,
    spin: bool,
    // shard-options.
    interval: time::Duration,
    max_shards: usize,
    max_entries: usize,

    snapshot: Snapshot<K, V>,
    auto_shard: Option<thread::JoinHandle<Result<()>>>,
    auto_shard_tx: Option<mpsc::Sender<(String, mpsc::Sender<usize>)>>,
}

struct Snapshot<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    root_seqno: Arc<AtomicU64>,
    shards: Arc<Mutex<Vec<Shard<K, V>>>>,
    rdrefns: Vec<Arc<Mutex<Vec<ShardReader<K, V>>>>>,
    wtrefns: Vec<Arc<Mutex<Vec<ShardWriter<K, V>>>>>,
}

impl<K, V> Drop for ShLlrb<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn drop(&mut self) {
        loop {
            let active = match self.prune_rw() {
                Ok((_, _, active)) => active,
                Err(err) => {
                    error!(
                        target: "shllrb", "{:?}, error locking {:?}",
                        self.name, err
                    );
                    break;
                }
            };

            if active > 0 {
                error!(
                    target: "shllrb",
                    "{:?}, open read/write handles {}", self.name, active
                );
                continue;
            }

            // drop input channel to auto_shard
            mem::drop(self.auto_shard_tx.take());

            // and wait for auto_shard to exit.
            match self.auto_shard.take() {
                Some(auto_shard) => match auto_shard.join() {
                    Ok(Ok(())) => (),
                    Ok(Err(err)) => error!(
                        target: "shllrb", "{} auto_shard: {:?}", self.name, err
                    ),
                    Err(err) => error!(
                        target: "shllrb", "{} auto_shard: {:?}", self.name, err
                    ),
                },
                None => (),
            }

            break;
        }
    }
}

/// Create and configure a range partitioned index.
impl<K, V> Default for ShLlrb<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn default() -> Self {
        let snapshot = Snapshot {
            root_seqno: Arc::new(AtomicU64::new(0)),
            shards: Arc::new(Mutex::new(vec![])),
            rdrefns: vec![],
            wtrefns: vec![],
        };
        ShLlrb {
            name: Default::default(),
            lsm: false,
            sticky: false,
            spin: true,
            interval: SHARD_INTERVAL,
            max_shards: 1,
            max_entries: DEFAULT_MAX_ENTRIES,
            snapshot,
            auto_shard: None,
            auto_shard_tx: Default::default(),
        }
    }
}

/// Create and configure a range partitioned index.
impl<K, V> ShLlrb<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    /// Create a new instance of range-partitioned index using Llrb tree.
    pub fn new<S: AsRef<str>>(name: S) -> Box<ShLlrb<K, V>> {
        let name = name.as_ref().to_string();
        let mut index: ShLlrb<K, V> = Default::default();
        index.name = name.to_string();
        Box::new(index)
    }

    /// Applications can call this to log Information log for application
    pub fn log(&self) {
        info!(
            target: "shllrb",
            "{:?}, new sharded-llrb instance, with config {}",
            self.name, self.to_config_string()
        );
    }

    /// Configure Llrb for LSM, refer to Llrb:new_lsm() for more details.
    pub fn set_lsm(&mut self, lsm: bool) -> &mut Self {
        self.lsm = lsm;
        self
    }

    /// Configure Llrb in sticky mode, refer to Llrb::set_sticky() for
    /// more details.
    pub fn set_sticky(&mut self, sticky: bool) -> &mut Self {
        self.sticky = sticky;
        self
    }

    /// Configure spin-latch behaviour for Llrb, refer to
    /// Llrb::set_spinlatch() for more details.
    pub fn set_spinlatch(&mut self, spin: bool) -> &mut Self {
        self.spin = spin;
        self
    }

    /// Configure shard parameters.
    ///
    /// * _max_shards_, maximum number for shards allowed.
    /// * _max_entries_, maximum number of entries allowed in a single
    ///   shard, beyond which the shard splits into two.
    pub fn set_shard_config(&mut self, max_shards: usize, max_entries: usize) -> &mut Self {
        self.max_shards = max_shards;
        self.max_entries = max_entries;
        self
    }

    fn try_init(&self) -> Result<()> {
        let mut shards = self.as_shards()?;
        if shards.len() == 0 {
            let shard_name: ShardName = (self.name.clone(), 0).into();
            let mut llrb = if self.lsm {
                Llrb::new_lsm(shard_name.to_string())
            } else {
                Llrb::new(shard_name.to_string())
            };
            llrb.set_sticky(self.sticky).set_spinlatch(self.spin);
            shards.push(Shard::new_active(llrb, Bound::Unbounded));
        }

        Ok(())
    }

    fn to_config_string(&self) -> String {
        let ss = vec![
            format!(
                "sharded-llrb = {{ lsm={}, sticky={}, spin={}, interval={} }}",
                self.lsm,
                self.sticky,
                self.spin,
                self.interval.as_secs(),
            ),
            format!(
                "sharded-llrb = {{ max_shards={}, max_entries={} }}",
                self.max_shards, self.max_entries,
            ),
        ];
        ss.join("\n")
    }

    fn as_mut_ptr_snapshot(&self) -> *mut Snapshot<K, V> {
        &self.snapshot as *const Snapshot<K, V> as *mut Snapshot<K, V>
    }

    fn lock_snapshot(&self) -> Result<(MutexGuard<Vec<Shard<K, V>>>, &mut Snapshot<K, V>)> {
        self.try_init()?;

        'outer: loop {
            let snapshot = unsafe { self.as_mut_ptr_snapshot().as_mut().unwrap() };
            let mut shards = self.as_shards()?;
            // make sure that all shards are in Active state.
            for shard in shards.iter_mut() {
                if shard.as_mut_inner().is_none() {
                    mem::drop(shards);
                    thread::sleep(RETRY_INTERVAL);
                    continue 'outer;
                }
            }

            break Ok((shards, snapshot));
        }
    }

    fn to_global_lock(&self) -> Result<GlobalLock<K, V>> {
        use crate::error::Error::ThreadFail;

        self.try_init()?;

        let snapshot = unsafe { self.as_mut_ptr_snapshot().as_mut().unwrap() };
        let shards = self.as_shards()?;

        let mut readers = vec![];
        for rd in snapshot.rdrefns.iter() {
            readers.push(
                rd.lock()
                    .map_err(|err| ThreadFail(format!("shllrb reader lock poisened, {:?}", err)))?,
            )
        }

        let mut writers = vec![];
        for wt in snapshot.wtrefns.iter() {
            writers.push(
                wt.lock()
                    .map_err(|err| ThreadFail(format!("shllrb writer lock poisened, {:?}", err)))?,
            )
        }

        Ok(GlobalLock {
            shards,
            readers,
            writers,
        })
    }

    fn prune_rw(&mut self) -> Result<(usize, usize, usize)> {
        loop {
            let (_shards, snapshot) = self.lock_snapshot()?;

            let mut roffs = vec![];
            for (off, arc_rs) in snapshot.rdrefns.iter().enumerate() {
                if Arc::strong_count(&arc_rs) == 1 {
                    roffs.push(off)
                }
            }
            for off in roffs.iter().rev() {
                snapshot.rdrefns.remove(*off);
            }

            let mut woffs = vec![];
            for (off, arc_ws) in snapshot.wtrefns.iter().enumerate() {
                if Arc::strong_count(&arc_ws) == 1 {
                    woffs.push(off)
                }
            }
            for off in woffs.iter().rev() {
                snapshot.wtrefns.remove(*off);
            }

            let active = self.snapshot.rdrefns.len() + self.snapshot.wtrefns.len();
            break Ok((roffs.len(), woffs.len(), active));
        }
    }
}

impl<K, V> ShLlrb<K, V>
where
    K: 'static + Send + Clone + Ord + Footprint,
    V: 'static + Send + Clone + Diff + Footprint,
    <V as Diff>::D: Send,
{
    /// Configure periodic interval for auto-sharding.
    pub fn set_interval(&mut self, interval: time::Duration) {
        if self.auto_shard_tx.is_some() {
            return;
        }

        self.interval = interval;
        if self.interval.as_secs() > 0 {
            let index = unsafe { Box::from_raw(self as *mut Self as *mut ffi::c_void) };
            let (auto_shard_tx, rx) = mpsc::channel();
            self.auto_shard = Some(thread::spawn(move || auto_shard::<K, V>(index, rx)));
            self.auto_shard_tx = Some(auto_shard_tx);
        }
    }

    /// Try to balance the underlying shards using splits and merges.
    pub fn balance(&mut self) -> Result<usize> {
        let msg = format!("shllrb, auto-sharding");
        let cmd = "balance".to_string();
        let (tx, rx) = mpsc::channel();
        self.auto_shard_tx
            .as_ref()
            .ok_or(Error::UnInitialized(msg))?
            .send((cmd, tx))?;
        Ok(rx.recv()?)
    }

    pub fn do_balance(&mut self) -> Result<usize> {
        let old_count = {
            let (shards, _) = self.lock_snapshot()?; // should be a quick call
            shards.len()
        };

        let mut n = self.try_merging_shards()?;
        n += self.try_spliting_shards()?;

        let new_count = {
            let (shards, _) = self.lock_snapshot()?; // should be a quick call
            shards.len()
        };

        if old_count != new_count {
            info!(
                target: "shllrb",
                "{}, {} old-shards balanced to {} new-shards",
                self.name, old_count, new_count,
            );
        }

        Ok(n)
    }

    // merge happens when.
    // * number of shards have reached max_shards.
    // * there are atleast 2 shards.
    fn try_merging_shards(&mut self) -> Result<usize> {
        // phase-1 mark shards that are going to be affected by the merge.
        let mut merges = {
            let mut gl = self.to_global_lock()?;
            if gl.shards.len() >= self.max_shards && gl.shards.len() > 1 {
                gl.start_merges(MergeOrder::new(&gl.shards).filter().take(
                    (self.max_shards / 5) + 1, // TODO: no magic formula
                ))
            } else {
                vec![]
            }
        };
        // MergeOrder shall order by entries, now re-order by offset.
        merges.sort_by(|x, y| (x.0).0.cmp(&(y.0).0));
        merges.reverse(); // in descending order of offset
        let n_merges = merges.len();
        if n_merges > 0 {
            info!(
                target: "shllrb", "{:?}, {} shards to merge", self.name, n_merges
            );
        }

        // phase-2 spawn threads to commit smaller shards into left/right shard
        let mut threads = vec![];
        for ((c_off, curr), (o_off, other)) in merges.into_iter() {
            // println!("merge at ({}, {})", c_off, o_off);
            threads.push(thread::spawn(move || {
                do_merge((c_off, curr), (o_off, other))
            }));
        }

        // phase-3 gather threads, and update active shards.
        let mut errs: Vec<Error> = vec![];
        for t in threads.into_iter() {
            match t.join() {
                Ok(Ok((c_off, o_off, curr_hk, other))) => {
                    let mut gl = self.to_global_lock()?;
                    gl.insert_active(o_off, vec![other], curr_hk)
                        .unwrap_or_else(|err| {
                            errs.push(err);
                            0
                        });
                    gl.remove_shard(c_off).unwrap_or_else(|err| {
                        errs.push(err);
                        0
                    });
                }
                Ok(Err(err)) => {
                    error!(target: "shllrb", "merge: {:?}", err);
                    errs.push(err);
                }
                Err(err) => {
                    error!(target: "shllrb", "thread: {:?}", err);
                    errs.push(Error::ThreadFail(format!("{:?}", err)));
                }
            }
        }
        // return
        if errs.len() == 0 {
            Ok(n_merges)
        } else {
            Err(Error::MemIndexFail(
                errs.into_iter()
                    .map(|e| format!("{:?}", e))
                    .collect::<Vec<String>>()
                    .join("; "),
            ))
        }
    }

    fn try_spliting_shards(&mut self) -> Result<usize> {
        // phase-1 mark shards that will be affected by the split.
        let mut splits = {
            let mut gl = self.to_global_lock()?;
            if gl.shards.len() < self.max_shards {
                gl.start_splits(SplitOrder::new(&gl.shards).filter().take(
                    self.max_shards - gl.shards.len(), // TODO: no magic formula
                ))
            } else {
                vec![]
            }
        };
        // SortOrder shall order by entries, no re-order by offset.
        splits.sort_by(|x, y| x.0.cmp(&y.0));
        splits.reverse(); // in descending order offset.
        let n_splits = splits.len();
        if n_splits > 0 {
            info!(
                target: "shllrb", "{}, {} shards to split", self.name, n_splits
            );
        }

        // phase-2 spawn threads to split shard into two new shards.
        let (name, mut threads) = (self.to_name(), vec![]);
        for (off, curr) in splits.into_iter() {
            let nm = name.clone();
            threads.push(thread::spawn(move || do_split(nm, off, curr)));
        }

        // phase-3 gather threads, and update active shards.
        let mut errs: Vec<Error> = vec![];
        for t in threads.into_iter() {
            match t.join() {
                Ok(Ok((off, one, two))) => {
                    let mut gl = self.to_global_lock()?;
                    gl.insert_active(off, vec![one, two], None)
                        .unwrap_or_else(|err| {
                            errs.push(err);
                            0
                        });
                }
                Ok(Err(err)) => {
                    error!(target: "shllrb", "split: {:?}", err);
                    errs.push(err);
                }
                Err(err) => {
                    error!(target: "shllrb", "thread: {:?}", err);
                    errs.push(Error::ThreadFail(format!("{:?}", err)));
                }
            }
        }
        // return
        if errs.len() == 0 {
            Ok(n_splits)
        } else {
            Err(Error::MemIndexFail(
                errs.into_iter()
                    .map(|e| format!("{:?}", e))
                    .collect::<Vec<String>>()
                    .join("; "),
            ))
        }
    }
}

/// Maintenance API.
impl<K, V> ShLlrb<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    /// Return whether this index support lsm mode.
    #[inline]
    pub fn is_lsm(&self) -> bool {
        self.lsm
    }

    /// Return whether this index is in sticky mode.
    #[inline]
    pub fn is_sticky(&self) -> bool {
        self.sticky
    }

    /// Return the behaviour of spin-latch behaviour.
    pub fn is_spin(&self) -> bool {
        self.spin
    }

    /// Return number of entries in this index.
    #[inline]
    pub fn len(&self) -> Result<usize> {
        let (shards, _) = self.lock_snapshot()?;

        Ok(shards
            .iter()
            .map(|shard| shard.as_inner().unwrap().index.len())
            .sum())
    }

    /// Identify this index. Applications can choose unique names while
    /// creating Llrb indices.
    #[inline]
    pub fn to_name(&self) -> String {
        self.name.clone()
    }

    /// Gather quick statistics from each shard and return the
    /// consolidated statisics.
    pub fn to_stats(&self) -> Result<LlrbStats> {
        let (shards, _) = self.lock_snapshot()?;
        let mut statss: Vec<LlrbStats> = vec![];
        for shard in shards.iter() {
            statss.push(shard.as_inner().unwrap().index.to_stats()?);
        }

        let stats = statss.remove(0);
        let mut stats = statss.into_iter().fold(stats, |stats, s| stats.merge(s));
        stats.name = self.to_name();
        Ok(stats)
    }

    fn as_shards(&self) -> Result<MutexGuard<Vec<Shard<K, V>>>> {
        use crate::error::Error::ThreadFail;

        self.snapshot
            .shards
            .lock()
            .map_err(|err| ThreadFail(format!("shllrb shard lock poisened, {:?}", err)))
    }
}

impl<K, V> Index<K, V> for Box<ShLlrb<K, V>>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    type W = ShllrbWriter<K, V>;

    type R = ShllrbReader<K, V>;

    type O = Empty;

    #[inline]
    fn to_name(&self) -> Result<String> {
        Ok(self.as_ref().to_name())
    }

    #[inline]
    fn to_root(&self) -> Result<Empty> {
        self.as_ref().to_root()
    }

    #[inline]
    fn to_metadata(&self) -> Result<Vec<u8>> {
        self.as_ref().to_metadata()
    }

    #[inline]
    fn to_seqno(&self) -> Result<u64> {
        self.as_ref().to_seqno()
    }

    #[inline]
    fn set_seqno(&mut self, seqno: u64) -> Result<()> {
        self.as_mut().set_seqno(seqno)
    }

    fn to_reader(&mut self) -> Result<Self::R> {
        self.as_mut().to_reader()
    }

    fn to_writer(&mut self) -> Result<Self::W> {
        self.as_mut().to_writer()
    }

    fn commit<C, F>(&mut self, scanner: core::CommitIter<K, V, C>, metacb: F) -> Result<()>
    where
        C: CommitIterator<K, V>,
        F: Fn(Vec<u8>) -> Vec<u8>,
    {
        self.as_mut().commit(scanner, metacb)
    }

    fn compact<F>(&mut self, cutoff: Bound<u64>, metacb: F) -> Result<usize>
    where
        F: Fn(Vec<Vec<u8>>) -> Vec<u8>,
    {
        self.as_mut().compact(cutoff, metacb)
    }
}

impl<K, V> Index<K, V> for ShLlrb<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    type W = ShllrbWriter<K, V>;

    type R = ShllrbReader<K, V>;

    type O = Empty;

    #[inline]
    fn to_name(&self) -> Result<String> {
        Ok(self.name.clone())
    }

    #[inline]
    fn to_root(&self) -> Result<Empty> {
        Ok(Empty)
    }

    #[inline]
    fn to_metadata(&self) -> Result<Vec<u8>> {
        Ok(vec![])
    }

    #[inline]
    fn to_seqno(&self) -> Result<u64> {
        Ok(self.snapshot.root_seqno.load(Ordering::SeqCst))
    }

    #[inline]
    fn set_seqno(&mut self, seqno: u64) -> Result<()> {
        let (_shards, snapshot) = self.lock_snapshot()?;
        let n = snapshot.rdrefns.len() + snapshot.wtrefns.len();
        if n > 0 {
            panic!(
                "cannot configure sharded_llrb with active readers/writers {}",
                n
            )
        }
        snapshot.root_seqno.store(seqno, Ordering::SeqCst);

        Ok(())
    }

    fn to_reader(&mut self) -> Result<Self::R> {
        let name = self.name.clone();
        let (mut shards, snapshot) = self.lock_snapshot()?;

        let readers = {
            let mut readers = vec![];
            for shard in shards.iter_mut() {
                readers.push(shard.as_mut_inner().unwrap().to_reader()?);
            }
            Arc::new(Mutex::new(readers))
        };
        let id = snapshot.rdrefns.len();
        snapshot.rdrefns.push(Arc::clone(&readers));
        Ok(ShllrbReader::new(name, id, readers))
    }

    fn to_writer(&mut self) -> Result<Self::W> {
        let name = self.name.clone();
        let (mut shards, snapshot) = self.lock_snapshot()?;

        let writers = {
            let mut writers = vec![];
            for shard in shards.iter_mut() {
                writers.push(shard.as_mut_inner().unwrap().to_writer()?);
            }
            Arc::new(Mutex::new(writers))
        };
        let id = snapshot.wtrefns.len();
        snapshot.wtrefns.push(Arc::clone(&writers));
        let seqno = Arc::clone(&snapshot.root_seqno);
        Ok(ShllrbWriter::new(name, id, seqno, writers))
    }

    // holds global lock. no other operations are allowed.
    fn commit<C, F>(&mut self, mut scanner: core::CommitIter<K, V, C>, metacb: F) -> Result<()>
    where
        C: CommitIterator<K, V>,
        F: Fn(Vec<u8>) -> Vec<u8>,
    {
        let mut gl = self.to_global_lock()?;
        let hks = gl
            .shards
            .iter()
            .map(|shard| shard.as_inner().unwrap().high_key.clone());

        let (_, ranges) =
            hks.into_iter()
                .fold((Bound::Unbounded, vec![]), |(low_key, mut ranges), hk| {
                    ranges.push((low_key, hk.clone()));
                    (high_key_to_low_key(&hk), ranges)
                });

        warn!(
            target: "shllrb",
            "{:?}, commit started (blocks all other index meta-ops) ...",
            self.name,
        );

        // println!("num ranges {}", ranges.len());
        let within = scanner.to_within();
        let iters = scanner.range_scans(ranges)?;
        assert_eq!(iters.len(), gl.shards.len());
        for (i, iter) in iters.into_iter().enumerate() {
            let index = &mut gl.shards[i].as_mut_inner().unwrap().index;
            index.commit(
                core::CommitIter::new(CommitWrapper::new(iter), within.clone()),
                |meta| metacb(meta),
            )?;
            let mut seqno = self.snapshot.root_seqno.load(Ordering::SeqCst);
            seqno = cmp::max(seqno, index.to_seqno()?);
            self.snapshot.root_seqno.store(seqno, Ordering::SeqCst);
        }
        Ok(())
    }

    fn compact<F>(&mut self, cutoff: Bound<u64>, metacb: F) -> Result<usize>
    where
        F: Fn(Vec<Vec<u8>>) -> Vec<u8>,
    {
        let (mut shards, _) = self.lock_snapshot()?;
        let mut count = 0;
        for shard in shards.iter_mut() {
            count += shard
                .as_mut_inner()
                .unwrap()
                .index
                .compact(cutoff.clone(), |meta| metacb(meta))?
        }
        info!(target: "shllrb", "{:?}, compacted {} items", self.name, count);
        Ok(count)
    }
}

impl<K, V> Footprint for Box<ShLlrb<K, V>>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn footprint(&self) -> Result<isize> {
        self.as_ref().footprint()
    }
}

impl<K, V> Footprint for ShLlrb<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn footprint(&self) -> Result<isize> {
        let (shards, _) = self.lock_snapshot()?;
        let mut footprint = 0;
        for shard in shards.iter() {
            footprint += shard.as_inner().unwrap().index.footprint()?;
        }
        Ok(footprint)
    }
}

impl<K, V> CommitIterator<K, V> for Box<ShLlrb<K, V>>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn scan<G>(&mut self, within: G) -> Result<IndexIter<K, V>>
    where
        G: Clone + RangeBounds<u64>,
    {
        self.as_mut().scan(within)
    }

    fn scans<G>(&mut self, shards: usize, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
    {
        self.as_mut().scans(shards, within)
    }

    fn range_scans<N, G>(&mut self, ranges: Vec<N>, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        N: Clone + RangeBounds<K>,
        G: Clone + RangeBounds<u64>,
    {
        self.as_mut().range_scans(ranges, within)
    }
}

impl<K, V> CommitIterator<K, V> for ShLlrb<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn scan<G>(&mut self, within: G) -> Result<IndexIter<K, V>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let mut iter = {
            let (shards, _) = self.lock_snapshot()?;
            Box::new(CommitIter::new(vec![], Arc::new(shards)))
        };
        let mut_shards = unsafe {
            let mut_shards = Arc::get_mut(&mut iter.shards).unwrap();
            (mut_shards.as_mut_slice() as *mut [Shard<K, V>])
                .as_mut()
                .unwrap()
        };

        for shard in mut_shards {
            iter.iters
                .push(shard.as_mut_inner().unwrap().index.scan(within.clone())?);
        }
        Ok(iter)
    }

    fn scans<G>(&mut self, _shards: usize, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let (mut shards, _) = self.lock_snapshot()?;
        let mut_shards = unsafe {
            ((&mut shards).as_mut_slice() as *mut [Shard<K, V>])
                .as_mut()
                .unwrap()
        };
        let shards = Arc::new(shards);

        let mut iters = vec![];
        for shard in mut_shards {
            iters.push(Box::new(CommitIter::new(
                vec![shard.as_mut_inner().unwrap().index.scan(within.clone())?],
                Arc::clone(&shards),
            )) as IndexIter<K, V>)
        }
        Ok(iters)
    }

    fn range_scans<N, G>(&mut self, ranges: Vec<N>, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        N: Clone + RangeBounds<K>,
        G: Clone + RangeBounds<u64>,
    {
        let (mut shards, _) = self.lock_snapshot()?;
        let mut mut_shardss = vec![];
        for _ in 0..ranges.len() {
            mut_shardss.push(unsafe {
                ((&mut shards).as_mut_slice() as *mut [Shard<K, V>])
                    .as_mut()
                    .unwrap()
            })
        }
        let shards = Arc::new(shards);

        let mut outer_iters = vec![];
        for (range, mut_shards) in ranges.into_iter().zip(mut_shardss.into_iter()) {
            let mut iter = Box::new(CommitIter::new(vec![], Arc::clone(&shards)));
            for shard in mut_shards.iter_mut() {
                iter.iters.push(
                    shard
                        .as_mut_inner()
                        .unwrap()
                        .index
                        .range_scans(vec![range.clone()], within.clone())?
                        .remove(0),
                );
            }
            outer_iters.push(iter as IndexIter<K, V>);
        }
        Ok(outer_iters)
    }
}

impl<K, V> Validate<LlrbStats> for Box<ShLlrb<K, V>>
where
    K: Clone + Ord + fmt::Debug + Footprint,
    V: Clone + Diff + Footprint,
{
    fn validate(&mut self) -> Result<LlrbStats> {
        self.as_mut().validate()
    }
}

impl<K, V> Validate<LlrbStats> for ShLlrb<K, V>
where
    K: Clone + Ord + fmt::Debug + Footprint,
    V: Clone + Diff + Footprint,
{
    fn validate(&mut self) -> Result<LlrbStats> {
        let (mut shards, _) = self.lock_snapshot()?;
        let mut statss = vec![];
        for shard in shards.iter_mut() {
            statss.push(shard.as_mut_inner().unwrap().index.validate()?)
        }
        let mut within = (Bound::<K>::Unbounded, Bound::<K>::Unbounded);
        for shard in shards.iter_mut() {
            within.0 = high_key_to_low_key(&within.1);
            within.1 = shard.as_inner().unwrap().high_key.clone();
            let index = &mut shard.as_mut_inner().unwrap().index;
            index.first().map(|f| assert!(within.contains(f.as_key())));
            index.last().map(|l| assert!(within.contains(l.as_key())));
        }

        let mut stats = match statss.len() {
            1 => statss.remove(0),
            n if n > 1 => {
                let stats = statss.remove(0);
                statss.into_iter().fold(stats, |stats, s| stats.merge(s))
            }
            _ => unreachable!(),
        };
        stats.name = self.to_name();
        Ok(stats)
    }
}

/// Read handle into [ShLlrb] index.
pub struct ShllrbReader<K, V>
where
    K: Ord + Clone,
    V: Clone + Diff,
{
    name: String,
    id: usize,
    readers: Arc<Mutex<Vec<ShardReader<K, V>>>>,
}

impl<K, V> ShllrbReader<K, V>
where
    K: Ord + Clone,
    V: Clone + Diff,
{
    fn new(
        name: String,
        id: usize,
        readers: Arc<Mutex<Vec<ShardReader<K, V>>>>,
    ) -> ShllrbReader<K, V> {
        let value = ShllrbReader { name, id, readers };
        info!(target: "shllrb", "{:?}, new reader {} ...", value.name, value.id);
        value
    }

    fn find<'a, Q>(key: &Q, rs: &'a mut [ShardReader<K, V>]) -> (usize, &'a mut ShardReader<K, V>)
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match rs.len() {
            0 => unreachable!(),
            1 => (0, &mut rs[0]),
            2 => {
                if ShardReader::less(key, &rs[0]) {
                    (0, &mut rs[0])
                } else {
                    (1, &mut rs[1])
                }
            }
            n => {
                let pivot = n / 2;
                if ShardReader::less(key, &rs[pivot]) {
                    Self::find(key, &mut rs[..pivot + 1])
                } else {
                    let (off, sr) = Self::find(key, &mut rs[pivot + 1..]);
                    (pivot + 1 + off, sr)
                }
            }
        }
    }
}

impl<K, V> Drop for ShllrbReader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn drop(&mut self) {
        info!(target: "shllrb", "{:?}, dropping reader {}", self.name, self.id);
    }
}

impl<K, V> Reader<K, V> for ShllrbReader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn get<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + Hash,
    {
        'outer: loop {
            let mut readers = self.readers.lock().unwrap();
            match Self::find(key, readers.as_mut_slice()) {
                (_, ShardReader::Active { r, .. }) => break r.get(key),
                _ => {
                    mem::drop(readers);
                    thread::sleep(RETRY_INTERVAL);
                    continue 'outer;
                }
            }
        }
    }

    fn iter(&mut self) -> Result<IndexIter<K, V>> {
        'outer: loop {
            let mut iter = Box::new(Iter::new(vec![], self.readers.lock().unwrap()));
            let readers = unsafe {
                (iter.readers.as_mut_slice() as *mut [ShardReader<K, V>])
                    .as_mut()
                    .unwrap()
            };
            for reader in readers {
                match reader {
                    ShardReader::Active { r, .. } => iter.iters.push(r.iter()?),
                    _ => {
                        mem::drop(iter);
                        thread::sleep(RETRY_INTERVAL);
                        continue 'outer;
                    }
                }
            }
            break Ok(iter);
        }
    }

    fn range<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        'outer: loop {
            let mut iter = Box::new(Iter::new(vec![], self.readers.lock().unwrap()));
            let readers = unsafe {
                (iter.readers.as_mut_slice() as *mut [ShardReader<K, V>])
                    .as_mut()
                    .unwrap()
            };

            let start = match range.start_bound() {
                Bound::Excluded(lr) | Bound::Included(lr) => Self::find(lr, readers).0,
                Bound::Unbounded => 0,
            };

            for reader in readers[start..].iter_mut() {
                match reader {
                    ShardReader::Active {
                        ref high_key, r, ..
                    } => {
                        iter.iters.push(r.range(range.clone())?);
                        let ok = match (range.end_bound(), high_key) {
                            (Bound::Unbounded, _) => true,
                            (_, Bound::Unbounded) => false, // last shard.
                            (Bound::Included(hr), Bound::Excluded(hk)) => hr.ge(hk.borrow()),
                            (Bound::Excluded(hr), Bound::Excluded(hk)) => hr.gt(hk.borrow()),
                            _ => unreachable!(),
                        };
                        if !ok {
                            break 'outer Ok(iter);
                        }
                    }
                    _ => {
                        mem::drop(iter);
                        thread::sleep(RETRY_INTERVAL);
                        continue 'outer;
                    }
                };
            }
            break 'outer Ok(iter);
        }
    }

    fn reverse<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        'outer: loop {
            let mut iter = Box::new(Iter::new(vec![], self.readers.lock().unwrap()));
            let readers = unsafe {
                (iter.readers.as_mut_slice() as *mut [ShardReader<K, V>])
                    .as_mut()
                    .unwrap()
            };

            let start = match range.start_bound() {
                Bound::Excluded(lr) | Bound::Included(lr) => Self::find(lr, readers).0,
                Bound::Unbounded => 0,
            };
            for reader in readers[start..].iter_mut() {
                match reader {
                    ShardReader::Active {
                        ref high_key, r, ..
                    } => {
                        let ok = match (range.end_bound(), high_key) {
                            (Bound::Unbounded, _) => true,
                            (_, Bound::Unbounded) => false, // last shard.
                            (Bound::Included(hr), Bound::Excluded(hk))
                            | (Bound::Excluded(hr), Bound::Excluded(hk)) => hr.ge(hk.borrow()),
                            _ => unreachable!(),
                        };
                        iter.iters.push(r.reverse(range.clone())?);
                        if !ok {
                            iter.iters.reverse();
                            break 'outer Ok(iter);
                        }
                    }
                    _ => {
                        mem::drop(iter);
                        thread::sleep(RETRY_INTERVAL);
                        continue 'outer;
                    }
                };
            }
            break 'outer Ok(iter);
        }
    }

    fn get_with_versions<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + Hash,
    {
        self.get(key)
    }

    fn iter_with_versions(&mut self) -> Result<IndexIter<K, V>> {
        self.iter()
    }

    fn range_with_versions<'a, R, Q>(
        &'a mut self, // reader cannot be shared
        r: R,
    ) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        self.range(r)
    }

    fn reverse_with_versions<'a, R, Q>(
        &'a mut self, // reader cannot be shared
        r: R,
    ) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        self.reverse(r)
    }
}

/// Write handle into [ShLlrb] index.
pub struct ShllrbWriter<K, V>
where
    K: Ord + Clone,
    V: Clone + Diff,
{
    name: String,
    id: usize,
    root_seqno: Arc<AtomicU64>,
    writers: Arc<Mutex<Vec<ShardWriter<K, V>>>>,
}

impl<K, V> ShllrbWriter<K, V>
where
    K: Ord + Clone,
    V: Clone + Diff,
{
    fn new(
        name: String,
        id: usize,
        root_seqno: Arc<AtomicU64>,
        writers: Arc<Mutex<Vec<ShardWriter<K, V>>>>,
    ) -> ShllrbWriter<K, V> {
        let value = ShllrbWriter {
            name,
            id,
            root_seqno,
            writers,
        };
        value
    }

    fn find<'a>(rs: &'a mut [ShardWriter<K, V>], key: &K) -> (usize, &'a mut ShardWriter<K, V>) {
        match rs.len() {
            0 => unreachable!(),
            1 => (0, &mut rs[0]),
            2 => {
                if ShardWriter::less(key, &rs[0]) {
                    (0, &mut rs[0])
                } else {
                    (1, &mut rs[1])
                }
            }
            n => {
                let pivot = n / 2;
                if ShardWriter::less(key, &rs[pivot]) {
                    Self::find(&mut rs[..pivot + 1], key)
                } else {
                    let (off, sr) = Self::find(&mut rs[pivot + 1..], key);
                    (pivot + 1 + off, sr)
                }
            }
        }
    }
}

impl<K, V> Drop for ShllrbWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn drop(&mut self) {
        info!(target: "shllrb", "{:?}, dropping writer {}", self.name, self.id);
    }
}

impl<K, V> Writer<K, V> for ShllrbWriter<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn set(&mut self, key: K, value: V) -> Result<Option<Entry<K, V>>> {
        loop {
            let mut writers = self.writers.lock().unwrap();
            match Self::find(writers.as_mut_slice(), &key) {
                (_, ShardWriter::Active { w, .. }) => {
                    let seqno = self.root_seqno.fetch_add(1, Ordering::SeqCst) + 1;
                    break Ok(w.set_index(key, value, Some(seqno))?.1);
                }
                _ => {
                    mem::drop(writers);
                    thread::sleep(RETRY_INTERVAL);
                }
            }
        }
    }

    fn set_cas(&mut self, key: K, value: V, cas: u64) -> Result<Option<Entry<K, V>>> {
        loop {
            let mut writers = self.writers.lock().unwrap();
            match Self::find(writers.as_mut_slice(), &key) {
                (_, ShardWriter::Active { w, .. }) => {
                    let seqno = self.root_seqno.fetch_add(1, Ordering::SeqCst) + 1;
                    break w.set_cas_index(key, value, cas, Some(seqno))?.1;
                }
                _ => {
                    mem::drop(writers);
                    thread::sleep(RETRY_INTERVAL);
                }
            }
        }
    }

    fn delete<Q>(&mut self, key: &Q) -> Result<Option<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        let keyk: K = key.to_owned();
        loop {
            let mut writers = self.writers.lock().unwrap();
            match Self::find(writers.as_mut_slice(), &keyk) {
                (_, ShardWriter::Active { w, .. }) => {
                    let seqno = self.root_seqno.fetch_add(1, Ordering::SeqCst) + 1;
                    break w.delete_index(key, Some(seqno))?.1;
                }
                _ => {
                    mem::drop(writers);
                    thread::sleep(RETRY_INTERVAL);
                }
            }
        }
    }
}

enum Shard<K, V>
where
    K: Ord + Clone,
    V: Clone + Diff,
{
    Active(InnerShard<K, V>),
    Merge(Bound<K>), // highkey
    Split(Bound<K>), // highkey
}

struct InnerShard<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    high_key: Bound<K>,
    index: Box<Llrb<K, V>>,
}

impl<K, V> Shard<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn new_active(index: Box<Llrb<K, V>>, high_key: Bound<K>) -> Shard<K, V> {
        Shard::Active(InnerShard { index, high_key })
    }

    fn new_merge(high_key: Bound<K>) -> Shard<K, V> {
        Shard::Merge(high_key)
    }

    fn new_split(high_key: Bound<K>) -> Shard<K, V> {
        Shard::Split(high_key)
    }
}

impl<K, V> Shard<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn as_inner(&self) -> Option<&InnerShard<K, V>> {
        match self {
            Shard::Active(inner) => Some(inner),
            Shard::Merge(_) | Shard::Split(_) => None,
        }
    }

    fn as_mut_inner(&mut self) -> Option<&mut InnerShard<K, V>> {
        match self {
            Shard::Active(inner) => Some(inner),
            Shard::Merge(_) | Shard::Split(_) => None,
        }
    }
}

impl<K, V> InnerShard<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn to_reader(&mut self) -> Result<ShardReader<K, V>> {
        let r = self.index.to_reader()?;
        Ok(ShardReader::new_active(self.high_key.clone(), r))
    }

    fn to_writer(&mut self) -> Result<ShardWriter<K, V>> {
        let w = self.index.to_writer()?;
        Ok(ShardWriter::new_active(self.high_key.clone(), w))
    }
}

enum ShardReader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    Active {
        high_key: Bound<K>,
        r: LlrbReader<K, V>,
    },
    Merge {
        high_key: Bound<K>,
    },
    Split {
        high_key: Bound<K>,
    },
}

impl<K, V> ShardReader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn new_active(high_key: Bound<K>, r: LlrbReader<K, V>) -> ShardReader<K, V> {
        ShardReader::Active { high_key, r }
    }

    fn new_merge(high_key: Bound<K>) -> ShardReader<K, V> {
        ShardReader::Merge { high_key }
    }

    fn new_split(high_key: Bound<K>) -> ShardReader<K, V> {
        ShardReader::Split { high_key }
    }

    fn less<Q>(key: &Q, s: &ShardReader<K, V>) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let high_key = match s {
            ShardReader::Active { high_key, .. } => high_key,
            ShardReader::Merge { high_key } => high_key,
            ShardReader::Split { high_key } => high_key,
        };
        match high_key {
            Bound::Excluded(high_key) => key.lt(high_key.borrow()),
            Bound::Unbounded => true,
            _ => unreachable!(),
        }
    }
}

enum ShardWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    Active {
        high_key: Bound<K>,
        w: LlrbWriter<K, V>,
    },
    Merge {
        high_key: Bound<K>,
    },
    Split {
        high_key: Bound<K>,
    },
}

impl<K, V> ShardWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn new_active(high_key: Bound<K>, w: LlrbWriter<K, V>) -> ShardWriter<K, V> {
        ShardWriter::Active { high_key, w }
    }

    fn new_merge(high_key: Bound<K>) -> ShardWriter<K, V> {
        ShardWriter::Merge { high_key }
    }

    fn new_split(high_key: Bound<K>) -> ShardWriter<K, V> {
        ShardWriter::Split { high_key }
    }

    fn less(key: &K, s: &ShardWriter<K, V>) -> bool {
        let high_key = match s {
            ShardWriter::Active { high_key, .. } => high_key,
            ShardWriter::Merge { high_key } => high_key,
            ShardWriter::Split { high_key } => high_key,
        };
        match high_key {
            Bound::Excluded(high_key) => key.lt(high_key),
            Bound::Unbounded => true,
            _ => unreachable!(),
        }
    }
}

struct CommitIter<'a, K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    shards: Arc<MutexGuard<'a, Vec<Shard<K, V>>>>,
    iter: Option<IndexIter<'a, K, V>>,
    iters: Vec<IndexIter<'a, K, V>>,
}

impl<'a, K, V> CommitIter<'a, K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    pub fn new(
        iters: Vec<IndexIter<'a, K, V>>,
        shards: Arc<MutexGuard<'a, Vec<Shard<K, V>>>>,
    ) -> CommitIter<'a, K, V> {
        CommitIter {
            shards,
            iter: None,
            iters,
        }
    }
}

impl<'a, K, V> Iterator for CommitIter<'a, K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.iter {
            Some(iter) => match iter.next() {
                Some(item) => Some(item),
                None => {
                    self.iter = None;
                    self.next()
                }
            },
            None if self.iters.len() == 0 => None,
            None => {
                self.iter = Some(self.iters.remove(0));
                self.next()
            }
        }
    }
}

struct Iter<'a, K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    readers: MutexGuard<'a, Vec<ShardReader<K, V>>>, // RAII Lock
    iter: Option<IndexIter<'a, K, V>>,
    iters: Vec<IndexIter<'a, K, V>>,
}

impl<'a, K, V> Iter<'a, K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    pub fn new(
        iters: Vec<IndexIter<'a, K, V>>,
        readers: MutexGuard<'a, Vec<ShardReader<K, V>>>,
    ) -> Iter<'a, K, V> {
        Iter {
            iter: None,
            iters,
            readers,
        }
    }
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.iter {
            Some(iter) => match iter.next() {
                Some(item) => Some(item),
                None => {
                    self.iter = None;
                    self.next()
                }
            },
            None if self.iters.len() == 0 => None,
            None => {
                self.iter = Some(self.iters.remove(0));
                self.iter.as_mut().unwrap().next()
            }
        }
    }
}

struct GlobalLock<'a, K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    shards: MutexGuard<'a, Vec<Shard<K, V>>>,
    readers: Vec<MutexGuard<'a, Vec<ShardReader<K, V>>>>,
    writers: Vec<MutexGuard<'a, Vec<ShardWriter<K, V>>>>,
}

impl<'a, K, V> GlobalLock<'a, K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn right_merge(&mut self, off: usize) -> ((usize, Shard<K, V>), (usize, Shard<K, V>)) {
        if off >= (self.shards.len() - 1) {
            unreachable!()
        }

        let curr = self.shards.remove(off);
        let curr_hk = curr.as_inner().unwrap().high_key.clone();
        self.shards.insert(off, Shard::new_merge(curr_hk.clone()));

        let right = self.shards.remove(off + 1);
        let right_hk = right.as_inner().unwrap().high_key.clone();
        self.shards
            .insert(off + 1, Shard::new_merge(right_hk.clone()));

        for rs in self.readers.iter_mut() {
            rs.remove(off);
            rs.insert(off, ShardReader::new_merge(curr_hk.clone()));
            rs.remove(off + 1);
            rs.insert(off + 1, ShardReader::new_merge(right_hk.clone()));
        }

        for ws in self.writers.iter_mut() {
            ws.remove(off);
            ws.insert(off, ShardWriter::new_merge(curr_hk.clone()));
            ws.remove(off + 1);
            ws.insert(off + 1, ShardWriter::new_merge(right_hk.clone()));
        }

        ((off, curr), (off + 1, right))
    }

    fn left_merge(&mut self, off: usize) -> ((usize, Shard<K, V>), (usize, Shard<K, V>)) {
        if off <= 0 {
            unreachable!()
        }

        let curr = self.shards.remove(off);
        let curr_hk = curr.as_inner().unwrap().high_key.clone();
        self.shards.insert(off, Shard::new_merge(curr_hk.clone()));

        let left = self.shards.remove(off - 1);
        let left_hk = left.as_inner().unwrap().high_key.clone();
        self.shards
            .insert(off - 1, Shard::new_merge(left_hk.clone()));

        for rs in self.readers.iter_mut() {
            rs.remove(off);
            rs.insert(off, ShardReader::new_merge(curr_hk.clone()));
            rs.remove(off - 1);
            rs.insert(off - 1, ShardReader::new_merge(left_hk.clone()));
        }

        for ws in self.writers.iter_mut() {
            ws.remove(off);
            ws.insert(off, ShardWriter::new_merge(curr_hk.clone()));
            ws.remove(off - 1);
            ws.insert(off - 1, ShardWriter::new_merge(left_hk.clone()));
        }

        ((off, curr), (off - 1, left))
    }

    fn start_merges(
        &mut self,
        offsets: Vec<usize>,
    ) -> Vec<((usize, Shard<K, V>), (usize, Shard<K, V>))> {
        if self.shards.len() < 2 {
            unreachable!()
        }

        let mut merges = vec![];
        for off in offsets.into_iter() {
            let (left, curr, right) = match off {
                0 => (
                    None,
                    self.shards[off].as_inner(),
                    self.shards[off + 1].as_inner(),
                ),
                off if off == self.shards.len() - 1 => (
                    self.shards[off - 1].as_inner(),
                    self.shards[off].as_inner(),
                    None,
                ),
                off => (
                    self.shards[off - 1].as_inner(),
                    self.shards[off].as_inner(),
                    self.shards[off + 1].as_inner(),
                ),
            };
            match (left, curr, right) {
                (_, None, _) => continue,
                (None, Some(_), None) => continue,
                (None, Some(_), Some(_)) => merges.push(self.right_merge(off)),
                (Some(_), Some(_), None) => merges.push(self.left_merge(off)),
                (Some(left), Some(_), Some(right)) if left.index.len() < right.index.len() => {
                    merges.push(self.left_merge(off))
                }
                (Some(_), Some(_), Some(_)) => merges.push(self.right_merge(off)),
            };
        }
        merges
    }

    fn split(&mut self, off: usize) -> (usize, Shard<K, V>) {
        let curr = self.shards.remove(off);
        let hk = curr.as_inner().unwrap().high_key.clone();
        self.shards.insert(off, Shard::new_split(hk.clone()));

        for rs in self.readers.iter_mut() {
            rs.remove(off);
            rs.insert(off, ShardReader::new_split(hk.clone()));
        }

        for ws in self.writers.iter_mut() {
            ws.remove(off);
            ws.insert(off, ShardWriter::new_split(hk.clone()));
        }

        (off, curr)
    }

    fn start_splits(&mut self, offsets: Vec<usize>) -> Vec<(usize, Shard<K, V>)> {
        offsets.into_iter().map(|off| self.split(off)).collect()
    }

    fn insert_active(
        &mut self,
        mut off: usize,
        mut new_shards: Vec<Shard<K, V>>,
        curr_hk: Option<Bound<K>>,
    ) -> Result<usize> {
        // validation
        let last_inner = new_shards.last_mut().unwrap().as_mut_inner().unwrap();
        let hk = last_inner.high_key.clone();
        for rs in self.readers.iter_mut() {
            match rs.remove(off) {
                ShardReader::Merge { high_key, .. } | ShardReader::Split { high_key, .. } => {
                    assert!(hk == high_key);
                }
                ShardReader::Active { .. } => unreachable!(),
            }
        }
        for ws in self.writers.iter_mut() {
            match ws.remove(off) {
                ShardWriter::Merge { high_key, .. } | ShardWriter::Split { high_key, .. } => {
                    assert!(hk == high_key);
                }
                ShardWriter::Active { .. } => unreachable!(),
            }
        }
        match self.shards.remove(off) {
            Shard::Merge(high_key) | Shard::Split(high_key) => {
                assert!(hk == high_key);
            }
            Shard::Active(_) => unreachable!(),
        }
        last_inner.high_key = match curr_hk {
            Some(hk) => hk,
            None => hk,
        };
        // ^ validation ok

        for mut shard in new_shards.into_iter() {
            for rs in self.readers.iter_mut() {
                rs.insert(off, shard.as_mut_inner().unwrap().to_reader()?);
            }
            for ws in self.writers.iter_mut() {
                ws.insert(off, shard.as_mut_inner().unwrap().to_writer()?);
            }
            self.shards.insert(off, shard);
            off += 1;
        }
        Ok(off)
    }

    fn remove_shard(&mut self, off: usize) -> Result<usize> {
        for rs in self.readers.iter_mut() {
            match rs.remove(off) {
                ShardReader::Merge { .. } => (),
                ShardReader::Split { .. } => unreachable!(),
                ShardReader::Active { .. } => unreachable!(),
            }
        }
        for ws in self.writers.iter_mut() {
            match ws.remove(off) {
                ShardWriter::Merge { .. } => (),
                ShardWriter::Split { .. } => unreachable!(),
                ShardWriter::Active { .. } => unreachable!(),
            }
        }
        match self.shards.remove(off) {
            Shard::Merge { .. } => (),
            Shard::Split { .. } => unreachable!(),
            Shard::Active { .. } => unreachable!(),
        }
        Ok(off)
    }
}

fn auto_shard<K, V>(
    mut index: Box<ffi::c_void>,
    rx: mpsc::Receiver<(String, mpsc::Sender<usize>)>,
) -> Result<()>
where
    K: 'static + Send + Clone + Ord + Footprint,
    V: 'static + Send + Clone + Diff + Footprint,
    <V as Diff>::D: Send,
{
    let mut elapsed = time::Duration::new(0, 0);
    let index: &mut ShLlrb<K, V> = unsafe {
        let index_ptr: &mut ffi::c_void = index.as_mut();
        let index_ptr = index_ptr as *mut ffi::c_void;
        (index_ptr as *mut ShLlrb<K, V>).as_mut().unwrap()
    };

    let index_name = index.to_name();
    let index_interval = index.interval.as_secs();
    let mut interval = time::Duration::from_secs(1); // TODO: no magic

    info!(
        target: "shllrb",
        "{}, auto-sharding thread started with interval {:?}",
        index_name, interval
    );

    loop {
        let resp_tx = match rx.recv_timeout(interval - elapsed) {
            Ok((cmd, resp_tx)) => {
                if cmd == "balance" {
                    Some(resp_tx)
                } else {
                    unreachable!()
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => None,
            Err(mpsc::RecvTimeoutError::Disconnected) => break Ok(()),
        };

        let (r, w, _) = index.prune_rw()?;
        if r > 0 || w > 0 {
            info!(
                target: "shllrb",
                "{:?}, pruned {} readers {} writers", index_name, r, w
            );
        }

        let start = time::SystemTime::now();
        let n = index.do_balance()?;

        {
            let (shards, _) = index.lock_snapshot()?;
            let isecs = if (n as f64) < ((shards.len() as f64) * 0.05) {
                cmp::min(interval.as_secs() * 2, index_interval)
            } else {
                cmp::max(interval.as_secs() / 2, 1)
            };
            interval = time::Duration::from_secs(isecs);
        }
        elapsed = start.elapsed().unwrap();

        match resp_tx {
            Some(tx) => tx.send(n)?,
            None => (),
        }
    }
}

fn high_key_to_low_key<K>(hk: &Bound<K>) -> Bound<K>
where
    K: Clone,
{
    match hk {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Excluded(hk) => Bound::Included(hk.clone()),
        _ => unreachable!(),
    }
}

fn do_merge<K, V>(
    (c_off, curr): (usize, Shard<K, V>),
    (o_off, mut other): (usize, Shard<K, V>),
) -> Result<(usize, usize, Option<Bound<K>>, Shard<K, V>)>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    let (curr_index, curr_hk) = match curr {
        Shard::Active(InnerShard { index, high_key }) => (index, high_key),
        _ => unreachable!(),
    };
    let curr_name = curr_index.to_name()?;
    let curr_stats = curr_index.to_stats()?;
    let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
    let iter = core::CommitIter::new(curr_index, within);
    warn!(
        target: "shllrb", "{} commiting shard\n{}", curr_name, curr_stats
    );

    let other_index = &mut other.as_mut_inner().unwrap().index;
    match other_index.commit(iter, |meta| meta) {
        Ok(()) if c_off > o_off => {
            info!(
                target: "shllrb", "{} left merge\n{}",
                other_index.to_name()?, other_index.to_stats()?
            );
            Ok((c_off, o_off, Some(curr_hk), other))
        }
        Ok(()) => {
            info!(
                target: "shllrb", "{} right merge\n{}",
                other_index.to_name()?, other_index.to_stats()?
            );
            Ok((c_off, o_off, None, other))
        }
        Err(err) => {
            error!(
                target: "shllrb",
                "{}, error merging {} index: {:?}",
                other_index.to_name()?, curr_name, err
            );
            Err(err)
        }
    }
}

fn do_split<K, V>(
    name: String,
    off: usize,
    curr: Shard<K, V>,
) -> Result<(usize, Shard<K, V>, Shard<K, V>)>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    let n1: ShardName = (name.clone(), off).into();
    let n2: ShardName = (name.clone(), off + 1).into();

    let (curr_index, high_key) = match curr {
        Shard::Active(InnerShard { high_key, index }) => (index, high_key),
        _ => unreachable!(),
    };

    let curr_name = curr_index.to_name()?;
    debug!(
        target: "llrb  ",
        "{} split in progress ...\n{}", curr_name, curr_index.to_stats()?
    );
    match curr_index.split(n1.to_string(), n2.to_string()) {
        Ok((one, two)) => {
            let (s1, s2) = (one.to_stats()?, two.to_stats()?);
            info!(target: "llrb  ", "{} split-shard 1st half\n{}", n1, s1);
            info!(target: "llrb  ", "{} split-shard 2nd half\n{}", n2, s2);

            let one = Shard::new_active(one, Bound::Excluded(two.first().unwrap().to_key()));
            let two = Shard::new_active(two, high_key);

            Ok((off, one, two))
        }
        Err(err) => {
            error!(
                target: "shllrb",
                "{}, error splitting index {:?}", curr_name, err
            );
            Err(err)
        }
    }
}

#[derive(Clone)]
struct MergeOrder(Vec<(usize, usize)>); // (offset, length)

impl MergeOrder {
    fn new<'a, K, V>(shards: &MutexGuard<'a, Vec<Shard<K, V>>>) -> MergeOrder
    where
        K: Ord + Clone,
        V: Clone + Diff,
    {
        let mut mo = MergeOrder(
            shards
                .iter()
                .enumerate()
                .map(|(off, shard)| (off, shard.as_inner().unwrap().index.len()))
                .collect(),
        );
        mo.asc_order(); // order by length
        mo
    }

    fn asc_order(&mut self) {
        self.0.sort_by(|x, y| x.1.cmp(&y.1))
    }

    fn avg_len(&self) -> usize {
        let total: usize = self
            .clone()
            .0
            .into_iter()
            .map(|x| x.1)
            .collect::<Vec<usize>>()
            .into_iter()
            .sum();
        total / self.0.len()
    }

    fn filter(self) -> MergeOrder {
        let avg_len = self.avg_len() / 2; // TODO: no magic formula
        MergeOrder(self.0.into_iter().filter(|x| x.1 < avg_len).collect())
    }

    fn take(self, n: usize) -> Vec<usize> {
        self.0.into_iter().take(n).map(|x| x.0).collect()
    }
}

struct SplitOrder(Vec<(usize, usize)>); // (offset, length)

impl SplitOrder {
    fn new<'a, K, V>(shards: &MutexGuard<'a, Vec<Shard<K, V>>>) -> SplitOrder
    where
        K: Ord + Clone,
        V: Clone + Diff,
    {
        let mut so = SplitOrder(
            shards
                .iter()
                .enumerate()
                .map(|(off, shard)| (off, shard.as_inner().unwrap().index.len()))
                .collect(),
        );
        so.dsc_order();
        so
    }

    fn dsc_order(&mut self) {
        self.0.sort_by(|x, y| x.1.cmp(&y.1));
        self.0.reverse();
    }

    fn avg_len(&self) -> usize {
        let total: usize = self
            .0
            .clone()
            .into_iter()
            .map(|x| x.1)
            .collect::<Vec<usize>>()
            .into_iter()
            .sum();
        total / self.0.len()
    }

    fn filter(self) -> SplitOrder {
        let avg_len = self.avg_len(); // TODO: no magic formula
        SplitOrder(self.0.into_iter().filter(|x| x.1 >= avg_len).collect())
    }

    fn take(self, n: usize) -> Vec<usize> {
        self.0.into_iter().take(n).map(|x| x.0).collect()
    }
}

#[cfg(test)]
#[path = "shllrb_test.rs"]
mod shllrb_test;
