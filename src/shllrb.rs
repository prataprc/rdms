//! Module `shllrb` implement an ordered set of index using Llrb shards.

use std::{
    borrow::Borrow,
    cmp,
    convert::TryFrom,
    fmt,
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
    core::{self, CommitIterator, Diff, Entry, Footprint, Index, IndexIter},
    core::{Cutoff, Reader, Result, Validate, WriteIndexFactory, Writer},
    error::Error,
    llrb::{Llrb, LlrbReader, LlrbWriter, Stats as LlrbStats},
    scans, thread as rt, util,
};
use log::{debug, error, info, warn};

/// Periodic interval to manage auto-sharding. Refer to auto_shard() for
/// more details.
pub const SHARD_INTERVAL: time::Duration = time::Duration::from_secs(10);

/// Periodic interval to retry API operation. Happens when a shard is not
/// in Active state.
pub const RETRY_INTERVAL: time::Duration = time::Duration::from_millis(10);

/// Maximum number of entries in a shard, beyond which a shard shall be split.
pub const DEFAULT_MAX_ENTRIES: usize = 1_000_000;

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
        if parts.len() < 3 {
            err_at!(InvalidInput, msg: format!("not shard name"))
        } else if parts[parts.len() - 2] != "shard" {
            err_at!(InvalidInput, msg: format!("not shard name"))
        } else {
            let shard = {
                let off = parts.len() - 1;
                parse_at!(parts[off], usize)?
            };
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
#[derive(Clone)]
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
    pub fn set_lsm(&mut self, lsm: bool) -> Result<&mut Self> {
        self.lsm = lsm;
        Ok(self)
    }

    /// If spin is _true_, calling thread will spin while waiting for the
    /// latch, otherwise, calling thead will be yielded to OS scheduler.
    /// For more detail refer Llrb::set_spinlatch() method. _Default: false_.
    pub fn set_spinlatch(&mut self, spin: bool) -> Result<&mut Self> {
        self.spin = spin;
        Ok(self)
    }

    /// Create all Llrb instances in sticky mode, refer to Llrb::set_sticky()
    /// for more details. For more detail refer Llrb::set_sticky().
    /// _Default: false_.
    pub fn set_sticky(&mut self, sticky: bool) -> Result<&mut Self> {
        self.sticky = sticky;
        Ok(self)
    }

    /// Set periodic interval for auto-sharding. _Default: 200 seconds_
    pub fn set_interval(&mut self, interval: time::Duration) -> Result<&mut Self> {
        self.interval = interval;
        Ok(self)
    }

    /// Set shard parameters.
    /// * `max_shards`, limit the maximum number of shards. _Default: 2_
    /// * `max_entries` per shard, beyond which shard will be split.
    ///   _Default: 1_000_000_
    pub fn set_shard_config(&mut self, max_shards: usize, max_entries: usize) -> Result<&mut Self> {
        self.max_shards = max_shards;
        self.max_entries = max_entries;
        Ok(self)
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
        let index = ShLlrb::<K, V>::new(name, self.clone().into());
        index.log();
        Ok(index)
    }
}

/// Configuration type for [ShLlrb].
#[derive(Clone, Default)]
pub struct Config {
    // llrb-options.
    lsm: bool,
    sticky: bool,
    spin: bool,
    // shard-options.
    interval: time::Duration,
    max_shards: usize,
    max_entries: usize,
}

impl From<ShllrbFactory> for Config {
    fn from(sf: ShllrbFactory) -> Config {
        Config {
            lsm: sf.lsm,
            sticky: sf.sticky,
            spin: sf.spin,
            interval: sf.interval,
            max_shards: sf.max_shards,
            max_entries: sf.max_entries,
        }
    }
}

impl Config {
    /// Configure Llrb for LSM, refer to Llrb:new_lsm() for more details.
    pub fn set_lsm(&mut self, lsm: bool) -> Result<&mut Self> {
        self.lsm = lsm;
        Ok(self)
    }

    /// Configure Llrb in sticky mode, refer to Llrb::set_sticky() for
    /// more details.
    pub fn set_sticky(&mut self, sticky: bool) -> Result<&mut Self> {
        self.sticky = sticky;
        Ok(self)
    }

    /// Configure spin-latch behaviour for Llrb, refer to
    /// Llrb::set_spinlatch() for more details.
    pub fn set_spinlatch(&mut self, spin: bool) -> Result<&mut Self> {
        self.spin = spin;
        Ok(self)
    }

    /// Configure shard parameters.
    ///
    /// * _max_shards_, maximum number for shards allowed.
    /// * _max_entries_, maximum number of entries allowed in a single
    ///   shard, beyond which the shard splits into two.
    pub fn set_shard_config(&mut self, max_shards: usize, max_entries: usize) -> Result<&mut Self> {
        self.max_shards = max_shards;
        self.max_entries = max_entries;
        Ok(self)
    }

    /// Configure periodic interval for auto-sharding.
    pub fn set_interval(&mut self, interval: time::Duration) -> Result<&mut Self> {
        self.interval = interval;
        Ok(self)
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

    auto_shard: Option<rt::Thread<String, Result<usize>, ()>>,
    snapshot: Arc<Mutex<Snapshot<K, V>>>,
}

struct Snapshot<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    root_seqno: Arc<AtomicU64>,
    metadata: Vec<u8>,
    shards: Vec<Shard<K, V>>,
    rdrefns: Vec<Arc<Mutex<Vec<ShardReader<K, V>>>>>,
    wtrefns: Vec<Arc<Mutex<Vec<ShardWriter<K, V>>>>>,
}

fn do_scan<'a, K, V, G>(
    mut snapshot: MutexGuard<'a, Snapshot<K, V>>,
    within: G,
) -> Result<IndexIter<'a, K, V>>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    G: Clone + RangeBounds<u64>,
{
    let shards = unsafe {
        (snapshot.shards.as_mut_slice() as *mut [Shard<K, V>])
            .as_mut()
            .unwrap()
    };

    let mut iters = vec![];
    for shard in shards.iter_mut() {
        iters.push(shard.as_mut_index().scan(within.clone())?);
    }

    Ok(Box::new(CommitIter::new(iters, Arc::new(snapshot))))
}

fn do_scans<'a, K, V, G>(
    mut snapshot: MutexGuard<'a, Snapshot<K, V>>,
    n_shards: usize,
    within: G,
) -> Result<Vec<IndexIter<'a, K, V>>>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    G: Clone + RangeBounds<u64>,
{
    let shards = unsafe {
        (snapshot.shards.as_mut_slice() as *mut [Shard<K, V>])
            .as_mut()
            .unwrap()
    };
    let snapshot = Arc::new(snapshot);

    let mut iters = vec![];
    for shard in shards.iter_mut() {
        iters.push(Box::new(CommitIter::new(
            vec![shard.as_mut_index().scan(within.clone())?],
            Arc::clone(&snapshot),
        )) as IndexIter<K, V>)
    }

    // If there are not enough shards push empty iterators.
    for _ in iters.len()..n_shards {
        let ss = vec![];
        iters.push(Box::new(ss.into_iter()));
    }

    assert_eq!(iters.len(), n_shards);

    Ok(iters)
}

fn do_range_scans<'a, K, V, N, G>(
    mut snapshot: MutexGuard<'a, Snapshot<K, V>>,
    ranges: Vec<N>,
    within: G,
) -> Result<Vec<IndexIter<'a, K, V>>>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    N: Clone + RangeBounds<K>,
    G: Clone + RangeBounds<u64>,
{
    let mut shardss = vec![];
    for _ in 0..ranges.len() {
        shardss.push(unsafe {
            (snapshot.shards.as_mut_slice() as *mut [Shard<K, V>])
                .as_mut()
                .unwrap()
        })
    }
    let snapshot = Arc::new(snapshot);

    let mut outer_iters = vec![];
    let zip_iter = ranges.into_iter().zip(shardss.into_iter());
    for (range, shards) in zip_iter {
        let mut iters = vec![];
        for shard in shards.iter_mut() {
            iters.push(
                shard
                    .as_mut_index()
                    .range_scans(vec![range.clone()], within.clone())?
                    .remove(0),
            );
        }
        let iter = Box::new(CommitIter::new(iters, Arc::clone(&snapshot)));
        outer_iters.push(iter as IndexIter<K, V>);
    }
    Ok(outer_iters)
}

// A global lock is similar to lock_snapshot(), that along with the guarantee
// that there wont be any concurrent reader or writer threads access the
// shard.
fn to_global_lock<'a, K, V>(
    snapshot: MutexGuard<'a, Snapshot<K, V>>,
) -> Result<GlobalLock<'a, K, V>>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    let mut readers = vec![];
    let rdrefns = unsafe {
        let ss = snapshot.rdrefns.as_slice();
        (ss as *const [Arc<Mutex<Vec<ShardReader<K, V>>>>])
            .as_ref()
            .unwrap()
    };
    for rd in rdrefns.iter() {
        let r = match rd.lock() {
            Ok(r) => Ok(r),
            Err(err) => err_at!(Fatal, msg: format!("poisened lock {}", err)),
        }?;
        readers.push(r);
    }

    let mut writers = vec![];
    let wtrefns = unsafe {
        let ss = snapshot.wtrefns.as_slice();
        (ss as *const [Arc<Mutex<Vec<ShardWriter<K, V>>>>])
            .as_ref()
            .unwrap()
    };
    for wt in wtrefns.iter() {
        let w = match wt.lock() {
            Ok(w) => Ok(w),
            Err(err) => err_at!(Fatal, msg: format!("poisened lock {}", err)),
        }?;
        writers.push(w);
    }

    Ok(GlobalLock {
        snapshot,
        readers,
        writers,
    })
}

impl<K, V> Drop for ShLlrb<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn drop(&mut self) {
        loop {
            let snapshot = match self.as_snapshot() {
                Ok(snapshot) => snapshot,
                Err(err) => {
                    error!(
                        target: "shllrb", "{:?}, lock snapshot:{:?}",
                        self.name, err
                    );
                    break;
                }
            };

            match Self::prune_rw(snapshot) {
                Ok((_, _, active)) if active > 0 => {
                    error!(
                        target: "shllrb",
                        "{:?}, open read/write handles {}", self.name, active
                    );
                    continue;
                }
                Ok((_, _, _)) => break,
                Err(err) => {
                    error!(
                        target: "shllrb", "{:?}, error locking {:?}",
                        self.name, err
                    );
                    break;
                }
            }
        }

        match self.auto_shard.take() {
            Some(auto_shard) => match auto_shard.close_wait() {
                Err(err) => error!(
                    target: "shllrb", "{:?}, auto-shard {:?}", self.name, err
                ),
                Ok(_) => (),
            },
            None => (),
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
        let snapshot = Arc::new(Mutex::new(Snapshot {
            root_seqno: Arc::new(AtomicU64::new(0)),
            metadata: Default::default(),
            shards: Default::default(),
            rdrefns: Default::default(),
            wtrefns: Default::default(),
        }));
        ShLlrb {
            name: Default::default(),
            lsm: false,
            sticky: false,
            spin: true,
            interval: SHARD_INTERVAL,
            max_shards: 1,
            max_entries: DEFAULT_MAX_ENTRIES,

            auto_shard: None,
            snapshot,
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
    pub fn new<S: AsRef<str>>(name: S, config: Config) -> Box<ShLlrb<K, V>>
    where
        K: 'static + Send,
        V: 'static + Send,
        <V as Diff>::D: Send,
    {
        let name = name.as_ref().to_string();

        let shard = {
            let shard_name: ShardName = (name.clone(), 0).into();
            let mut llrb = if config.lsm {
                Llrb::new_lsm(shard_name.to_string())
            } else {
                Llrb::new(shard_name.to_string())
            };
            llrb.set_sticky(config.sticky).ok(); // can't be error
            llrb.set_spinlatch(config.spin).ok(); // can't be error
            Shard::new_active(llrb, Bound::Unbounded)
        };

        let snapshot = Arc::new(Mutex::new(Snapshot {
            root_seqno: Arc::new(AtomicU64::new(0)),
            metadata: Default::default(),
            shards: vec![shard],
            rdrefns: Default::default(),
            wtrefns: Default::default(),
        }));

        let mut index = Box::new(ShLlrb {
            name: name.to_string(),
            lsm: config.lsm,
            sticky: config.sticky,
            spin: config.spin,
            interval: config.interval,
            max_shards: config.max_shards,
            max_entries: config.max_entries,

            auto_shard: None,
            snapshot,
        });

        index.auto_shard = if index.interval.as_secs() > 0 {
            let name = index.name.clone();
            let snapshot = Arc::clone(&index.snapshot);
            Some(rt::Thread::new(move |rx| {
                || auto_shard::<K, V>(name, config, snapshot, rx)
            }))
        } else {
            None
        };

        index
    }
}

/// Maintanence API
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
        let snapshot = self.lock_snapshot()?;

        Ok(snapshot
            .shards
            .iter()
            .map(|shard| shard.as_index().len())
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
        let snapshot = self.lock_snapshot()?;

        let mut statss: Vec<LlrbStats> = vec![];
        for shard in snapshot.shards.iter() {
            statss.push(shard.as_index().to_stats()?);
        }

        let stats = statss.remove(0);
        let mut stats = statss.into_iter().fold(stats, |stats, s| stats.merge(s));
        stats.name = self.to_name();
        Ok(stats)
    }

    /// Applications can call this to log Information log for application
    pub fn log(&self) {
        info!(
            target: "shllrb",
            "{:?}, new sharded-llrb instance, with config {}",
            self.name, self.to_config_string()
        );
    }

    /// Try to balance the underlying shards using splits and merges.
    pub fn balance(&mut self) -> Result<usize> {
        match &self.auto_shard {
            Some(auto_shard) => auto_shard.request("balance".to_string())?,
            None => err_at!(UnInitialized, msg: format!("shllrb.balance()")),
        }
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

    fn as_snapshot(&self) -> Result<MutexGuard<Snapshot<K, V>>> {
        match self.snapshot.lock() {
            Ok(value) => Ok(value),
            Err(err) => err_at!(Fatal, msg: format!("poisened lock {}", err)),
        }
    }

    // Return only if shards are locked and all shards are in active state.
    fn lock_snapshot(&self) -> Result<MutexGuard<Snapshot<K, V>>> {
        'outer: loop {
            let mut snapshot = self.as_snapshot()?;
            // make sure that all shards are in Active state.
            for shard in snapshot.shards.iter_mut() {
                if shard.to_index().is_none() {
                    mem::drop(snapshot);
                    thread::sleep(RETRY_INTERVAL);
                    continue 'outer;
                }
            }

            break Ok(snapshot);
        }
    }

    // reader and writer threads migh exit as part of application's ongoing
    // logic. In such cases, the main instance of ShLlrb should be able clean
    // up itself with dead readers and writers.
    //
    // Return (no-of-readers-pruned, no-of-writers-pruned, no-of-active-refs)
    fn prune_rw(
        mut snapshot: MutexGuard<Snapshot<K, V>>, // with locked snapshot.
    ) -> Result<(usize, usize, usize)> {
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

        let mut active = snapshot.rdrefns.len();
        active += snapshot.wtrefns.len();
        Ok((roffs.len(), woffs.len(), active))
    }
}

impl<K, V> ShLlrb<K, V>
where
    K: 'static + Send + Clone + Ord + Footprint,
    V: 'static + Send + Clone + Diff + Footprint,
    <V as Diff>::D: Send,
{
    fn do_balance(
        name: String,
        snapshot: MutexGuard<Snapshot<K, V>>,
        config: Config,
    ) -> Result<usize> {
        let old_count = snapshot.shards.len();

        let (snapshot, n) = {
            let name = name.clone();
            Self::try_merging_shards(name, snapshot, config.clone())?
        };
        let (snapshot, m) = {
            let name = name.clone();
            Self::try_spliting_shards(name, snapshot, config.clone())?
        };

        let new_count = snapshot.shards.len();

        if old_count != new_count {
            info!(
                target: "shllrb",
                "{}, {} old-shards balanced to {} new-shards",
                name, old_count, new_count,
            );
        }

        Ok(n + m)
    }

    // merge happens when.
    // * number of shards have reached max_shards.
    // * there are atleast 2 shards.
    fn try_merging_shards(
        name: String,
        snapshot: MutexGuard<Snapshot<K, V>>,
        config: Config,
    ) -> Result<(MutexGuard<Snapshot<K, V>>, usize)> {
        // phase-1 mark shards that are going to be affected by the merge.
        let (mut snapshot, mut merges) = {
            let mut gl = to_global_lock(snapshot)?;
            let n_shards = gl.snapshot.shards.len();

            let n = (config.max_shards / 5) + 1; // TODO: no magic formula
            if n_shards >= config.max_shards && n_shards > 1 {
                let mo = MergeOrder::new(&gl.snapshot.shards);
                let merges = gl.mark_merges(mo.filter().take(n))?;
                (gl.snapshot, merges)
            } else {
                return Ok((gl.snapshot, 0));
            }
        };
        // MergeOrder shall order by entries, now re-order by offset.
        merges.sort_by(|x, y| x[0].0.cmp(&y[0].0));
        merges.reverse(); // in descending order of offset
        let n_merges = merges.len();
        if n_merges > 0 {
            info!(
                target: "shllrb", "{:?}, {} shards to merge", name, n_merges
            );
        }

        // phase-2 spawn threads to commit smaller shards into left/right shard
        let mut threads = vec![];
        for item in merges.into_iter() {
            let [(c_off, curr), (o_off, other)] = item;
            // println!("merge at ({}, {})", c_off, o_off);
            threads.push(thread::spawn(move || {
                thread_merge((c_off, curr), (o_off, other))
            }));
        }

        // phase-3 gather threads, and update active shards.
        let mut errs: Vec<Error> = vec![];
        for t in threads.into_iter() {
            match t.join().unwrap() {
                Ok((c_off, o_off, curr_hk, other)) => {
                    let mut gl = to_global_lock(snapshot)?;

                    match gl.insert_active(o_off, vec![other], curr_hk) {
                        Ok(_) => (),
                        Err(err) => errs.push(err),
                    }
                    match gl.remove_shard(c_off) {
                        Err(err) => errs.push(err),
                        Ok(_) => (),
                    }

                    snapshot = gl.snapshot;
                }
                Err(err) => {
                    error!(target: "shllrb", "merge: {:?}", err);
                    errs.push(err);
                }
            }
        }

        // return
        if errs.len() == 0 {
            Ok((snapshot, n_merges))
        } else {
            let msg = errs
                .into_iter()
                .map(|e| format!("merge-err:{:?}", e))
                .collect::<Vec<String>>()
                .join("; ");
            err_at!(Fatal, msg: msg)
        }
    }

    fn try_spliting_shards(
        name: String,
        snapshot: MutexGuard<Snapshot<K, V>>,
        config: Config,
    ) -> Result<(MutexGuard<Snapshot<K, V>>, usize)> {
        // phase-1 mark shards that will be affected by the split.
        let (mut snapshot, mut splits) = {
            let mut gl = to_global_lock(snapshot)?;
            let n_shards = gl.snapshot.shards.len();

            let n = config.max_shards - n_shards; // TODO: no magic formula
            if n_shards < config.max_shards {
                let so = SplitOrder::new(&gl.snapshot.shards, config.max_entries);
                let offsets = so.filter().take(n);
                let mut splits: Vec<(usize, Shard<K, V>)> = vec![];
                for off in offsets.into_iter() {
                    splits.push(gl.mark_split(off)?)
                }
                (gl.snapshot, splits)
            } else {
                (gl.snapshot, vec![])
            }
        };
        // SortOrder shall order by entries, no re-order by offset.
        splits.sort_by(|x, y| x.0.cmp(&y.0));
        splits.reverse(); // in descending order offset.
        let n_splits = splits.len();
        if n_splits > 0 {
            info!(
                target: "shllrb", "{}, {} shards to split", name, n_splits
            );
        }

        // phase-2 spawn threads to split shard into two new shards.
        let mut threads = vec![];
        for (off, curr) in splits.into_iter() {
            let nm = name.clone();
            threads.push(thread::spawn(move || thread_split(nm, off, curr)));
        }

        // phase-3 gather threads, and update active shards.
        let mut errs: Vec<Error> = vec![];
        for t in threads.into_iter() {
            match t.join().unwrap() {
                Ok((off, one, two)) => {
                    let mut gl = to_global_lock(snapshot)?;

                    match gl.insert_active(off, vec![one, two], None) {
                        Ok(_) => (),
                        Err(err) => errs.push(err),
                    }

                    snapshot = gl.snapshot;
                }
                Err(err) => {
                    error!(target: "shllrb", "split: {:?}", err);
                    errs.push(err);
                }
            }
        }

        // return
        if errs.len() == 0 {
            Ok((snapshot, n_splits))
        } else {
            let msg = errs
                .into_iter()
                .map(|e| format!("split-err:{:?}", e))
                .collect::<Vec<String>>()
                .join("; ");
            err_at!(Fatal, msg: msg)
        }
    }
}

impl<K, V> Index<K, V> for Box<ShLlrb<K, V>>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    type W = ShllrbWriter<K, V>;
    type R = ShllrbReader<K, V>;

    #[inline]
    fn to_name(&self) -> Result<String> {
        Ok(self.as_ref().to_name())
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

    fn compact(&mut self, cutoff: Cutoff) -> Result<usize> {
        self.as_mut().compact(cutoff)
    }

    fn close(self) -> Result<()> {
        (*self).close()
    }

    fn purge(self) -> Result<()> {
        (*self).purge()
    }
}

impl<K, V> Index<K, V> for ShLlrb<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    type W = ShllrbWriter<K, V>;
    type R = ShllrbReader<K, V>;

    #[inline]
    fn to_name(&self) -> Result<String> {
        Ok(self.name.clone())
    }

    #[inline]
    fn to_metadata(&self) -> Result<Vec<u8>> {
        Ok(vec![])
    }

    #[inline]
    fn to_seqno(&self) -> Result<u64> {
        let snapshot = self.lock_snapshot()?;

        Ok(snapshot.root_seqno.load(Ordering::SeqCst))
    }

    #[inline]
    fn set_seqno(&mut self, seqno: u64) -> Result<()> {
        let snapshot = self.lock_snapshot()?;

        let n = snapshot.rdrefns.len() + snapshot.wtrefns.len();
        if n == 0 {
            snapshot.root_seqno.store(seqno, Ordering::SeqCst);
            Ok(())
        } else {
            err_at!(APIMisuse, msg: format!("active-handles:{}", n))
        }
    }

    fn to_reader(&mut self) -> Result<Self::R> {
        let (readers, id) = {
            let mut snapshot = self.lock_snapshot()?;

            let readers = {
                let mut readers = vec![];
                for shard in snapshot.shards.iter_mut() {
                    readers.push(shard.to_reader()?);
                }
                Arc::new(Mutex::new(readers))
            };
            let id = snapshot.rdrefns.len();
            snapshot.rdrefns.push(Arc::clone(&readers));
            (readers, id)
        };

        let snapshot = Arc::clone(&self.snapshot);
        Ok(ShllrbReader::new(self.name.clone(), id, snapshot, readers))
    }

    fn to_writer(&mut self) -> Result<Self::W> {
        let mut snapshot = self.lock_snapshot()?;

        let writers = {
            let mut writers = vec![];
            for shard in snapshot.shards.iter_mut() {
                writers.push(shard.to_writer()?);
            }
            Arc::new(Mutex::new(writers))
        };
        let id = snapshot.wtrefns.len();
        let seqno = Arc::clone(&snapshot.root_seqno);
        snapshot.wtrefns.push(Arc::clone(&writers));

        Ok(ShllrbWriter::new(self.name.clone(), id, seqno, writers))
    }

    // NOTE: Error returned by commit are fatal, it leaves the index
    // in in-consistent state.
    // NOTE: Holds global lock. No other operations are allowed.
    fn commit<C, F>(&mut self, mut scanner: core::CommitIter<K, V, C>, metacb: F) -> Result<()>
    where
        C: CommitIterator<K, V>,
        F: Fn(Vec<u8>) -> Vec<u8>,
    {
        let snapshot = self.lock_snapshot()?;
        let mut gl = to_global_lock(snapshot)?;

        let ranges = util::high_keys_to_ranges(
            gl.snapshot
                .shards
                .iter()
                .map(|s| s.to_high_key())
                .collect::<Vec<Bound<K>>>(),
        );

        warn!(
            target: "shllrb",
            "{:?}, commit started (blocks index meta-ops) ...", self.name,
        );

        // println!("num ranges {}", ranges.len());
        let within = scanner.to_within();
        let iters = scanner.range_scans(ranges)?;
        assert_eq!(iters.len(), gl.snapshot.shards.len());
        for (i, iter) in iters.into_iter().enumerate() {
            let mut seqno = gl.snapshot.root_seqno.load(Ordering::SeqCst);

            let index = &mut gl.snapshot.shards[i].as_mut_index();
            let within = within.clone();
            let iter = {
                let iter = scans::CommitWrapper::new(vec![iter]);
                core::CommitIter::new(iter, within)
            };
            index.commit(iter, |_| vec![])?;
            seqno = cmp::max(seqno, index.to_seqno()?);
            gl.snapshot.root_seqno.store(seqno, Ordering::SeqCst);
        }

        gl.snapshot.metadata = metacb(gl.snapshot.metadata.clone());

        Ok(())
    }

    fn compact(&mut self, cutoff: Cutoff) -> Result<usize> {
        let mut snapshot = self.lock_snapshot()?;

        let mut count = 0;
        for shard in snapshot.shards.iter_mut() {
            count += shard.as_mut_index().compact(cutoff.clone())?
        }

        info!(target: "shllrb", "{:?}, compacted {} items", self.name, count);
        Ok(count)
    }

    // to be called only after all other readers and writers exit.
    fn close(mut self) -> Result<()> {
        match self.auto_shard.take() {
            Some(auto_shard) => auto_shard.close_wait()?,
            None => (),
        }

        Ok(())
    }

    // to be called only after all other readers and writers exit.
    fn purge(self) -> Result<()> {
        self.close()
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
        let snapshot = self.lock_snapshot()?;

        let mut footprint = 0;
        for shard in snapshot.shards.iter() {
            footprint += shard.as_index().footprint()?;
        }
        Ok(footprint)
    }
}

impl<K, V> CommitIterator<K, V> for Box<ShLlrb<K, V>>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    #[inline]
    fn scan<G>(&mut self, within: G) -> Result<IndexIter<K, V>>
    where
        G: Clone + RangeBounds<u64>,
    {
        self.as_mut().scan(within)
    }

    #[inline]
    fn scans<G>(&mut self, n_shards: usize, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
    {
        self.as_mut().scans(n_shards, within)
    }

    #[inline]
    fn range_scans<N, G>(&mut self, ranges: Vec<N>, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        N: Clone + RangeBounds<K>,
        G: Clone + RangeBounds<u64>,
    {
        self.as_mut().range_scans(ranges, within)
    }
}

impl<K, V> CommitIterator<K, V> for &mut ShLlrb<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    #[inline]
    fn scan<G>(&mut self, within: G) -> Result<IndexIter<K, V>>
    where
        G: Clone + RangeBounds<u64>,
    {
        (*self).scan(within)
    }

    #[inline]
    fn scans<G>(&mut self, n_shards: usize, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
    {
        (*self).scans(n_shards, within)
    }

    #[inline]
    fn range_scans<N, G>(&mut self, ranges: Vec<N>, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        N: Clone + RangeBounds<K>,
        G: Clone + RangeBounds<u64>,
    {
        (*self).range_scans(ranges, within)
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
        let snapshot = self.lock_snapshot()?; // should be a quick call
        do_scan(snapshot, within)
    }

    fn scans<G>(&mut self, n_shards: usize, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let snapshot = self.lock_snapshot()?;
        do_scans(snapshot, n_shards, within)
    }

    fn range_scans<N, G>(&mut self, ranges: Vec<N>, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        N: Clone + RangeBounds<K>,
        G: Clone + RangeBounds<u64>,
    {
        let snapshot = self.lock_snapshot()?;
        do_range_scans(snapshot, ranges, within)
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
        let mut snapshot = self.lock_snapshot()?;
        let mut statss = vec![];
        for shard in snapshot.shards.iter_mut() {
            statss.push(shard.as_mut_index().validate()?)
        }
        let mut within = (Bound::<K>::Unbounded, Bound::<K>::Unbounded);
        for shard in snapshot.shards.iter_mut() {
            within.0 = util::high_key_to_low_key(&within.1);
            within.1 = shard.to_high_key();
            let index = &mut shard.as_mut_index();
            index.first().map(|f| assert!(within.contains(f.as_key())));
            index.last().map(|l| assert!(within.contains(l.as_key())));
        }

        let mut stats = match statss.len() {
            1 => statss.remove(0),
            n if n > 1 => {
                let stats = statss.remove(0);
                statss.into_iter().fold(stats, |stats, s| stats.merge(s))
            }
            _ => err_at!(Fatal, msg: format!("unreachable"))?,
        };
        stats.name = self.to_name();
        Ok(stats)
    }
}

/// Read handle into [ShLlrb] index.
pub struct ShllrbReader<K, V>
where
    K: Ord + Clone + Footprint,
    V: Clone + Diff + Footprint,
{
    name: String,
    id: usize,
    snapshot: Arc<Mutex<Snapshot<K, V>>>,
    readers: Arc<Mutex<Vec<ShardReader<K, V>>>>,
}

impl<K, V> ShllrbReader<K, V>
where
    K: Ord + Clone + Footprint,
    V: Clone + Diff + Footprint,
{
    fn new(
        name: String,
        id: usize,
        snapshot: Arc<Mutex<Snapshot<K, V>>>,
        readers: Arc<Mutex<Vec<ShardReader<K, V>>>>,
    ) -> ShllrbReader<K, V> {
        let value = ShllrbReader {
            name,
            id,
            snapshot,
            readers,
        };
        info!(target: "shllrb", "{:?}, new reader {} ...", value.name, value.id);
        value
    }

    fn find<'a, Q>(
        key: &Q,
        rs: &'a mut [ShardReader<K, V>], // from shards
    ) -> Result<(usize, &'a mut ShardReader<K, V>)>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match rs.len() {
            0 => err_at!(Fatal, msg: format!("unreachable")),
            1 => Ok((0, &mut rs[0])),
            2 => {
                if ShardReader::less(key, &rs[0]) {
                    Ok((0, &mut rs[0]))
                } else {
                    Ok((1, &mut rs[1]))
                }
            }
            n => {
                let pivot = n / 2;
                if ShardReader::less(key, &rs[pivot]) {
                    Self::find(key, &mut rs[..pivot + 1])
                } else {
                    let (off, sr) = Self::find(key, &mut rs[pivot + 1..])?;
                    Ok((pivot + 1 + off, sr))
                }
            }
        }
    }

    fn as_snapshot(&self) -> Result<MutexGuard<Snapshot<K, V>>> {
        match self.snapshot.lock() {
            Ok(value) => Ok(value),
            Err(err) => err_at!(Fatal, msg: format!("poisened lock {}", err)),
        }
    }

    // Return only if shards are locked and all shards are in active state.
    fn lock_snapshot(&self) -> Result<MutexGuard<Snapshot<K, V>>> {
        'outer: loop {
            let mut snapshot = self.as_snapshot()?;
            // make sure that all shards are in Active state.
            for shard in snapshot.shards.iter_mut() {
                if shard.to_index().is_none() {
                    mem::drop(snapshot);
                    thread::sleep(RETRY_INTERVAL);
                    continue 'outer;
                }
            }

            break Ok(snapshot);
        }
    }

    fn as_readers(&self) -> Result<MutexGuard<Vec<ShardReader<K, V>>>> {
        match self.readers.lock() {
            Ok(value) => Ok(value),
            Err(err) => err_at!(Fatal, msg: format!("poisened lock {}", err)),
        }
    }
}

impl<K, V> Drop for ShllrbReader<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn drop(&mut self) {
        debug!(target: "shllrb", "{:?}, dropping reader {}", self.name, self.id);
    }
}

impl<K, V> Reader<K, V> for ShllrbReader<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn get<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + Hash,
    {
        'outer: loop {
            let mut readers = self.as_readers()?;

            match Self::find(key, readers.as_mut_slice())? {
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
            let mut iter = {
                let readers = self.as_readers()?;
                Box::new(Iter::new(vec![], readers))
            };

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
        use std::ops::Bound::{Excluded, Included, Unbounded};

        'outer: loop {
            let mut iter = {
                let readers = self.as_readers()?;
                Box::new(Iter::new(vec![], readers))
            };

            let readers = unsafe {
                (iter.readers.as_mut_slice() as *mut [ShardReader<K, V>])
                    .as_mut()
                    .unwrap()
            };

            let start = match range.start_bound() {
                Bound::Excluded(lr) => Self::find(lr, readers)?.0,
                Bound::Included(lr) => Self::find(lr, readers)?.0,
                Bound::Unbounded => 0,
            };

            for reader in readers[start..].iter_mut() {
                match reader {
                    ShardReader::Active {
                        ref high_key, r, ..
                    } => {
                        iter.iters.push(r.range(range.clone())?);
                        let ok = match (range.end_bound(), high_key) {
                            (Unbounded, _) => true,
                            (_, Unbounded) => false, // last shard.
                            (Included(hr), Excluded(hk)) => hr.ge(hk.borrow()),
                            (Excluded(hr), Excluded(hk)) => hr.gt(hk.borrow()),
                            _ => err_at!(Fatal, msg: format!("unreachable"))?,
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
        use std::ops::Bound::{Excluded, Included, Unbounded};

        'outer: loop {
            let mut iter = {
                let readers = self.as_readers()?;
                Box::new(Iter::new(vec![], readers))
            };

            let readers = unsafe {
                (iter.readers.as_mut_slice() as *mut [ShardReader<K, V>])
                    .as_mut()
                    .unwrap()
            };

            let start = match range.start_bound() {
                Excluded(lr) => Self::find(lr, readers)?.0,
                Included(lr) => Self::find(lr, readers)?.0,
                Unbounded => 0,
            };

            for reader in readers[start..].iter_mut() {
                match reader {
                    ShardReader::Active {
                        ref high_key, r, ..
                    } => {
                        let ok = match (range.end_bound(), high_key) {
                            (Unbounded, _) => true,
                            (_, Unbounded) => false, // last shard.
                            (Included(hr), Excluded(hk)) => hr.ge(hk.borrow()),
                            (Excluded(hr), Excluded(hk)) => hr.ge(hk.borrow()),
                            _ => err_at!(Fatal, msg: format!("unreachable"))?,
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

impl<K, V> CommitIterator<K, V> for ShllrbReader<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn scan<G>(&mut self, within: G) -> Result<IndexIter<K, V>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let snapshot = self.lock_snapshot()?; // should be a quick call
        do_scan(snapshot, within)
    }

    fn scans<G>(&mut self, n_shards: usize, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let snapshot = self.lock_snapshot()?;
        do_scans(snapshot, n_shards, within)
    }

    fn range_scans<N, G>(&mut self, ranges: Vec<N>, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        N: Clone + RangeBounds<K>,
        G: Clone + RangeBounds<u64>,
    {
        let snapshot = self.lock_snapshot()?;
        do_range_scans(snapshot, ranges, within)
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

    fn find<'a>(
        key: &K,
        rs: &'a mut [ShardWriter<K, V>], // from writers
    ) -> Result<(usize, &'a mut ShardWriter<K, V>)> {
        match rs.len() {
            0 => err_at!(Fatal, msg: format!("unreachable")),
            1 => Ok((0, &mut rs[0])),
            2 => {
                if ShardWriter::less(key, &rs[0]) {
                    Ok((0, &mut rs[0]))
                } else {
                    Ok((1, &mut rs[1]))
                }
            }
            n => {
                let pivot = n / 2;
                if ShardWriter::less(key, &rs[pivot]) {
                    Self::find(key, &mut rs[..pivot + 1])
                } else {
                    let (off, sr) = Self::find(key, &mut rs[pivot + 1..])?;
                    Ok((pivot + 1 + off, sr))
                }
            }
        }
    }

    fn as_writers(&self) -> Result<MutexGuard<Vec<ShardWriter<K, V>>>> {
        match self.writers.lock() {
            Ok(value) => Ok(value),
            Err(err) => err_at!(Fatal, msg: format!("poisened lock {}", err)),
        }
    }
}

impl<K, V> Drop for ShllrbWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn drop(&mut self) {
        debug!(target: "shllrb", "{:?}, dropping writer {}", self.name, self.id);
    }
}

impl<K, V> Writer<K, V> for ShllrbWriter<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn set(&mut self, key: K, value: V) -> Result<Option<Entry<K, V>>> {
        loop {
            let mut writers = self.as_writers()?;
            match Self::find(&key, writers.as_mut_slice())? {
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
            let mut writers = self.as_writers()?;
            match Self::find(&key, writers.as_mut_slice())? {
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
            let mut writers = self.as_writers()?;
            match Self::find(&keyk, writers.as_mut_slice())? {
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
    Active {
        index: Box<Llrb<K, V>>,
        high_key: Bound<K>,
    },
    Merge {
        high_key: Bound<K>,
    },
    Split {
        high_key: Bound<K>,
    },
}

impl<K, V> Shard<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn new_active(index: Box<Llrb<K, V>>, high_key: Bound<K>) -> Shard<K, V> {
        Shard::Active { index, high_key }
    }

    fn to_merge(&self) -> Result<Shard<K, V>> {
        match self {
            Shard::Active { high_key, .. } => Ok(Shard::Merge {
                high_key: high_key.clone(),
            }),
            Shard::Merge { .. } | Shard::Split { .. } => {
                err_at!(Fatal, msg: format!("unreachable"))
            }
        }
    }

    fn to_split(&self) -> Result<Shard<K, V>> {
        match self {
            Shard::Active { high_key, .. } => Ok(Shard::Split {
                high_key: high_key.clone(),
            }),
            Shard::Merge { .. } | Shard::Split { .. } => {
                err_at!(Fatal, msg: format!("unreachable"))
            }
        }
    }

    fn as_index(&self) -> &Llrb<K, V> {
        match self {
            Shard::Active { index, .. } => index.as_ref(),
            Shard::Merge { .. } | Shard::Split { .. } => unreachable!(),
        }
    }

    fn as_mut_index(&mut self) -> &mut Llrb<K, V> {
        match self {
            Shard::Active { index, .. } => index.as_mut(),
            Shard::Merge { .. } | Shard::Split { .. } => unreachable!(),
        }
    }

    fn to_index(&self) -> Option<&Llrb<K, V>> {
        match self {
            Shard::Active { index, .. } => Some(index.as_ref()),
            Shard::Merge { .. } | Shard::Split { .. } => None,
        }
    }

    fn to_high_key(&self) -> Bound<K> {
        match self {
            Shard::Active { high_key, .. } => high_key,
            Shard::Merge { high_key } => high_key,
            Shard::Split { high_key } => high_key,
        }
        .clone()
    }

    fn set_high_key(&mut self, hk: Bound<K>) {
        let high_key = match self {
            Shard::Active { high_key, .. } => high_key,
            Shard::Merge { high_key } => high_key,
            Shard::Split { high_key } => high_key,
        };
        *high_key = hk;
    }

    fn to_reader(&mut self) -> Result<ShardReader<K, V>>
    where
        K: Footprint,
        V: Footprint,
    {
        match self {
            Shard::Active { index, high_key } => {
                let r = index.to_reader()?;
                Ok(ShardReader::new_active(high_key.clone(), r))
            }
            Shard::Merge { .. } | Shard::Split { .. } => {
                err_at!(Fatal, msg: format!("unreachable"))
            }
        }
    }

    fn to_writer(&mut self) -> Result<ShardWriter<K, V>>
    where
        K: Footprint,
        V: Footprint,
    {
        match self {
            Shard::Active { index, high_key } => {
                let w = index.to_writer()?;
                Ok(ShardWriter::new_active(high_key.clone(), w))
            }
            Shard::Merge { .. } | Shard::Split { .. } => {
                err_at!(Fatal, msg: format!("unreachable"))
            }
        }
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

    fn to_merge(&self) -> Result<ShardReader<K, V>> {
        match self {
            ShardReader::Active { high_key, .. } => Ok(ShardReader::Merge {
                high_key: high_key.clone(),
            }),
            ShardReader::Split { .. } => err_at!(Fatal, msg: format!("unreachable")),
            ShardReader::Merge { .. } => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    fn to_split(&self) -> Result<ShardReader<K, V>> {
        match self {
            ShardReader::Active { high_key, .. } => Ok(ShardReader::Split {
                high_key: high_key.clone(),
            }),
            ShardReader::Split { .. } => err_at!(Fatal, msg: format!("unreachable")),
            ShardReader::Merge { .. } => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    fn to_high_key(&self) -> Bound<K> {
        match self {
            ShardReader::Active { high_key, .. } => high_key,
            ShardReader::Merge { high_key } => high_key,
            ShardReader::Split { high_key } => high_key,
        }
        .clone()
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

    fn to_merge(&self) -> Result<ShardWriter<K, V>> {
        match self {
            ShardWriter::Active { high_key, .. } => Ok(ShardWriter::Merge {
                high_key: high_key.clone(),
            }),
            ShardWriter::Split { .. } => err_at!(Fatal, msg: format!("unreachable")),
            ShardWriter::Merge { .. } => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    fn to_split(&self) -> Result<ShardWriter<K, V>> {
        match self {
            ShardWriter::Active { high_key, .. } => Ok(ShardWriter::Split {
                high_key: high_key.clone(),
            }),
            ShardWriter::Split { .. } => err_at!(Fatal, msg: format!("unreachable")),
            ShardWriter::Merge { .. } => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    fn to_high_key(&self) -> Bound<K> {
        match self {
            ShardWriter::Active { high_key, .. } => high_key,
            ShardWriter::Merge { high_key } => high_key,
            ShardWriter::Split { high_key } => high_key,
        }
        .clone()
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
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    _snapshot: Arc<MutexGuard<'a, Snapshot<K, V>>>,
    iter: Option<IndexIter<'a, K, V>>,
    iters: Vec<IndexIter<'a, K, V>>,
}

impl<'a, K, V> CommitIter<'a, K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    pub fn new(
        iters: Vec<IndexIter<'a, K, V>>,
        _snapshot: Arc<MutexGuard<'a, Snapshot<K, V>>>,
    ) -> CommitIter<'a, K, V> {
        CommitIter {
            _snapshot,
            iter: None,
            iters,
        }
    }
}

impl<'a, K, V> Iterator for CommitIter<'a, K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match &mut self.iter {
                Some(iter) => match iter.next() {
                    Some(item) => break Some(item),
                    None => self.iter = None,
                },
                None if self.iters.len() == 0 => break None,
                None => self.iter = Some(self.iters.remove(0)),
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
        loop {
            match &mut self.iter {
                Some(iter) => match iter.next() {
                    Some(item) => break Some(item),
                    None => self.iter = None,
                },
                None if self.iters.len() == 0 => break None,
                None => self.iter = Some(self.iters.remove(0)),
            }
        }
    }
}

struct GlobalLock<'a, K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    snapshot: MutexGuard<'a, Snapshot<K, V>>,
    readers: Vec<MutexGuard<'a, Vec<ShardReader<K, V>>>>,
    writers: Vec<MutexGuard<'a, Vec<ShardWriter<K, V>>>>,
}

impl<'a, K, V> GlobalLock<'a, K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn mark_merges(&mut self, offsets: Vec<usize>) -> Result<Vec<[(usize, Shard<K, V>); 2]>> {
        if self.snapshot.shards.len() < 2 {
            err_at!(Fatal, msg: format!("unreachable"))?
        }

        let mut merges = vec![];
        for off in offsets.into_iter() {
            let (left, curr, right) = match off {
                0 => (
                    None,
                    self.snapshot.shards[off].to_index(),
                    self.snapshot.shards[off + 1].to_index(),
                ),
                off if off == self.snapshot.shards.len() - 1 => (
                    self.snapshot.shards[off - 1].to_index(),
                    self.snapshot.shards[off].to_index(),
                    None,
                ),
                off => (
                    self.snapshot.shards[off - 1].to_index(),
                    self.snapshot.shards[off].to_index(),
                    self.snapshot.shards[off + 1].to_index(),
                ),
            };
            match (left, curr, right) {
                (_, None, _) => continue,
                (None, Some(_), None) => continue,
                (None, Some(_), Some(_)) => merges.push(self.right_merge(off)?),
                (Some(_), Some(_), None) => merges.push(self.left_merge(off)?),
                (Some(l), Some(_), Some(r)) if l.len() < r.len() => {
                    merges.push(self.left_merge(off)?)
                }
                (Some(_), Some(_), Some(_)) => merges.push(self.right_merge(off)?),
            };
        }
        Ok(merges)
    }

    fn right_merge(&mut self, off: usize) -> Result<[(usize, Shard<K, V>); 2]> {
        if off >= (self.snapshot.shards.len() - 1) {
            err_at!(Fatal, msg: format!("unreachable"))?
        }

        let curr = self.snapshot.shards.remove(off);
        self.snapshot.shards.insert(off, curr.to_merge()?);

        let right = self.snapshot.shards.remove(off + 1);
        self.snapshot.shards.insert(off + 1, right.to_merge()?);

        for rs in self.readers.iter_mut() {
            let r = rs.remove(off);
            rs.insert(off, r.to_merge()?);
            let r = rs.remove(off + 1);
            rs.insert(off + 1, r.to_merge()?);

            assert!(rs[off].to_high_key() == curr.to_high_key());
            assert!(rs[off + 1].to_high_key() == right.to_high_key());
        }

        for ws in self.writers.iter_mut() {
            let w = ws.remove(off);
            ws.insert(off, w.to_merge()?);
            let w = ws.remove(off + 1);
            ws.insert(off + 1, w.to_merge()?);

            assert!(ws[off].to_high_key() == curr.to_high_key());
            assert!(ws[off + 1].to_high_key() == right.to_high_key());
        }

        Ok([(off, curr), (off + 1, right)])
    }

    fn left_merge(&mut self, off: usize) -> Result<[(usize, Shard<K, V>); 2]> {
        if off <= 0 {
            err_at!(Fatal, msg: format!("unreachable"))?
        }

        let curr = self.snapshot.shards.remove(off);
        self.snapshot.shards.insert(off, curr.to_merge()?);

        let left = self.snapshot.shards.remove(off - 1);
        self.snapshot.shards.insert(off - 1, left.to_merge()?);

        for rs in self.readers.iter_mut() {
            let r = rs.remove(off);
            rs.insert(off, r.to_merge()?);
            let r = rs.remove(off - 1);
            rs.insert(off - 1, r.to_merge()?);

            assert!(rs[off].to_high_key() == curr.to_high_key());
            assert!(rs[off + 1].to_high_key() == left.to_high_key());
        }

        for ws in self.writers.iter_mut() {
            let w = ws.remove(off);
            ws.insert(off, w.to_merge()?);
            let w = ws.remove(off - 1);
            ws.insert(off - 1, w.to_merge()?);

            assert!(ws[off].to_high_key() == curr.to_high_key());
            assert!(ws[off + 1].to_high_key() == left.to_high_key());
        }

        Ok([(off, curr), (off - 1, left)])
    }

    fn mark_split(&mut self, off: usize) -> Result<(usize, Shard<K, V>)> {
        let curr = self.snapshot.shards.remove(off);
        self.snapshot.shards.insert(off, curr.to_split()?);

        for rs in self.readers.iter_mut() {
            let r = rs.remove(off);
            rs.insert(off, r.to_split()?);
            assert!(rs[off].to_high_key() == curr.to_high_key());
        }

        for ws in self.writers.iter_mut() {
            let w = ws.remove(off);
            ws.insert(off, w.to_split()?);
            assert!(ws[off].to_high_key() == curr.to_high_key());
        }

        Ok((off, curr))
    }

    fn insert_active(
        &mut self,
        mut off: usize,
        mut new_shards: Vec<Shard<K, V>>,
        curr_hk: Option<Bound<K>>,
    ) -> Result<usize> {
        // validation
        let last_shard = new_shards.last_mut().unwrap();
        let hk = last_shard.to_high_key();
        for rs in self.readers.iter_mut() {
            match rs.remove(off) {
                ShardReader::Merge { high_key, .. } => assert!(hk == high_key),
                ShardReader::Split { high_key, .. } => assert!(hk == high_key),
                ShardReader::Active { .. } => err_at!(Fatal, msg: format!("unreachable"))?,
            }
        }
        for ws in self.writers.iter_mut() {
            match ws.remove(off) {
                ShardWriter::Merge { high_key, .. } => assert!(hk == high_key),
                ShardWriter::Split { high_key, .. } => assert!(hk == high_key),
                ShardWriter::Active { .. } => err_at!(Fatal, msg: format!("unreachable"))?,
            }
        }
        match self.snapshot.shards.remove(off) {
            Shard::Merge { high_key } => assert!(hk == high_key),
            Shard::Split { high_key } => assert!(hk == high_key),
            Shard::Active { .. } => err_at!(Fatal, msg: format!("unreachable"))?,
        }
        last_shard.set_high_key(curr_hk.unwrap_or(hk));
        // ^ validation ok

        for mut shard in new_shards.into_iter() {
            for rs in self.readers.iter_mut() {
                rs.insert(off, shard.to_reader()?);
            }
            for ws in self.writers.iter_mut() {
                ws.insert(off, shard.to_writer()?);
            }
            self.snapshot.shards.insert(off, shard);
            off += 1;
        }
        Ok(off)
    }

    fn remove_shard(&mut self, off: usize) -> Result<usize> {
        for rs in self.readers.iter_mut() {
            match rs.remove(off) {
                ShardReader::Merge { .. } => (),
                ShardReader::Split { .. } => err_at!(Fatal, msg: format!("unreachable"))?,
                ShardReader::Active { .. } => err_at!(Fatal, msg: format!("unreachable"))?,
            }
        }
        for ws in self.writers.iter_mut() {
            match ws.remove(off) {
                ShardWriter::Merge { .. } => (),
                ShardWriter::Split { .. } => err_at!(Fatal, msg: format!("unreachable"))?,
                ShardWriter::Active { .. } => err_at!(Fatal, msg: format!("unreachable"))?,
            }
        }
        match self.snapshot.shards.remove(off) {
            Shard::Merge { .. } => (),
            Shard::Split { .. } => err_at!(Fatal, msg: format!("unreachable"))?,
            Shard::Active { .. } => err_at!(Fatal, msg: format!("unreachable"))?,
        }
        Ok(off)
    }
}

fn auto_shard<K, V>(
    index_name: String,
    config: Config,
    snapshot: Arc<Mutex<Snapshot<K, V>>>,
    rx: rt::Rx<String, Result<usize>>,
) -> Result<()>
where
    K: 'static + Send + Clone + Ord + Footprint,
    V: 'static + Send + Clone + Diff + Footprint,
    <V as Diff>::D: Send,
{
    info!(
        target: "shllrb",
        "{}, auto-sharding thread started with interval {:?}",
        index_name, config.interval,
    );

    let mut elapsed = time::Duration::new(0, 0);
    loop {
        let resp_tx = {
            let interval = {
                let interval = ((config.interval * 2) + elapsed) / 2;
                cmp::min(interval, elapsed)
            };
            match rx.recv_timeout(interval) {
                Ok((cmd, resp_tx)) if cmd == "balance" => resp_tx,
                Ok(_) => unreachable!(),
                Err(mpsc::RecvTimeoutError::Timeout) => None,
                Err(mpsc::RecvTimeoutError::Disconnected) => break Ok(()),
            }
        };

        let (r, w, _) = {
            let s = match snapshot.lock() {
                Ok(s) => Ok(s),
                Err(e) => err_at!(Fatal, msg: format!("poisened lock {}", e)),
            }?;
            ShLlrb::<K, V>::prune_rw(s)?
        };
        if r > 0 || w > 0 {
            info!(
                target: "shllrb",
                "{:?}, pruned {} readers {} writers", index_name, r, w
            );
        }

        let start = time::SystemTime::now();
        let res = {
            let name = index_name.clone();
            let s = match snapshot.lock() {
                Ok(s) => Ok(s),
                Err(e) => err_at!(Fatal, msg: format!("poisoned lock {}", e)),
            }?;
            ShLlrb::<K, V>::do_balance(name, s, config.clone())
        };

        elapsed = err_at!(TimeFail, start.elapsed())?;

        match resp_tx {
            Some(tx) => err_at!(IPCFail, tx.send(res))?,
            None => match res {
                Ok(n) => info!(
                    target: "shllrb", "{:?}, balance done: {}", index_name, n
                ),
                Err(err) => info!(
                    target: "dgm   ", "{:?}, balance err, {:?}", index_name, err
                ),
            },
        }
    }
}

fn thread_merge<K, V>(
    (c_off, curr): (usize, Shard<K, V>),
    (o_off, mut other): (usize, Shard<K, V>),
) -> Result<(usize, usize, Option<Bound<K>>, Shard<K, V>)>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    let (curr_index, curr_hk) = match curr {
        Shard::Active { index, high_key } => (index, high_key),
        _ => unreachable!(),
    };
    let curr_name = curr_index.to_name()?;
    let curr_stats = curr_index.to_stats()?;

    let iter = {
        let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
        core::CommitIter::new(curr_index, within)
    };
    warn!(target: "shllrb", "{} commiting shard\n{}", curr_name, curr_stats);

    let other_index = &mut other.as_mut_index();
    let o_name = other_index.to_name();
    let o_stats = other_index.to_stats()?;

    match other_index.commit(iter, |meta| meta) {
        Ok(()) if c_off > o_off => {
            info!(target: "shllrb", "{} left merge\n{}", o_name, o_stats);
            Ok((c_off, o_off, Some(curr_hk), other))
        }
        Ok(()) => {
            info!(target: "shllrb", "{} right merge\n{}", o_name, o_stats);
            Ok((c_off, o_off, None, other))
        }
        Err(err) => {
            error!(
                target: "shllrb",
                "{}, error merging {} index: {:?}",
                o_name, curr_name, err
            );
            Err(err)
        }
    }
}

fn thread_split<K, V>(
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
        Shard::Active { high_key, index } => (index, high_key),
        _ => unreachable!(),
    };

    let c_name = curr_index.to_name()?;
    let c_stats = curr_index.to_stats()?;

    debug!(target: "llrb  ", "{} split in progress ...\n{}", c_name, c_stats);

    match curr_index.split(n1.to_string(), n2.to_string()) {
        Ok((one, two)) => {
            let (s1, s2) = (one.to_stats()?, two.to_stats()?);
            info!(target: "llrb  ", "{} split-shard 1st half\n{}", n1, s1);
            info!(target: "llrb  ", "{} split-shard 2nd half\n{}", n2, s2);

            let one = {
                let high_key = Bound::Excluded(two.first().unwrap().to_key());
                Shard::new_active(one, high_key)
            };
            let two = Shard::new_active(two, high_key);

            Ok((off, one, two))
        }
        Err(err) => {
            error!(target: "shllrb", "{}, splitting index {:?}", c_name, err);
            Err(err)
        }
    }
}

#[derive(Clone)]
struct MergeOrder(Vec<(usize, usize)>); // (offset, entries)

impl MergeOrder {
    fn new<K, V>(shards: &Vec<Shard<K, V>>) -> MergeOrder
    where
        K: Ord + Clone,
        V: Clone + Diff,
    {
        let mut mo = MergeOrder(
            shards
                .iter()
                .enumerate()
                .map(|(off, shard)| (off, shard.as_index().len()))
                .collect(),
        );
        mo.0.sort_by(|x, y| x.1.cmp(&y.1)); // ascending order
        mo
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

struct SplitOrder {
    shards: Vec<(usize, usize)>, // (offset, entries)
    max_entries: usize,
}

impl SplitOrder {
    fn new<K, V>(shards: &Vec<Shard<K, V>>, max_entries: usize) -> SplitOrder
    where
        K: Ord + Clone,
        V: Clone + Diff,
    {
        let shards: Vec<(usize, usize)> = shards
            .iter()
            .enumerate()
            .map(|(off, shard)| (off, shard.as_index().len()))
            .collect();
        let mut so = SplitOrder {
            shards,
            max_entries,
        };
        // dsc_order
        so.shards.sort_by(|x, y| x.1.cmp(&y.1));
        so.shards.reverse();

        so
    }

    fn avg_len(&self) -> usize {
        let total: usize = self
            .shards
            .clone()
            .into_iter()
            .map(|x| x.1)
            .collect::<Vec<usize>>()
            .into_iter()
            .sum();
        total / self.shards.len()
    }

    fn filter(self) -> SplitOrder {
        let avg_len = self.avg_len(); // TODO: no magic formula
        let threshold = self.max_entries / 10; // 10% threshold
        let shards: Vec<(usize, usize)> = self
            .shards
            .into_iter()
            .filter(|x| (x.1 > threshold) && (x.1 >= avg_len))
            .collect();
        SplitOrder {
            shards,
            max_entries: self.max_entries,
        }
    }

    fn take(self, n: usize) -> Vec<usize> {
        self.shards.into_iter().take(n).map(|x| x.0).collect()
    }
}

#[cfg(test)]
#[path = "shllrb_test.rs"]
mod shllrb_test;
