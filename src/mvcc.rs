//! Module ``mvcc`` implement [Multi-Version-Concurrency-Control][mvcc]
//! variant of [Llrb].
//!
//! [Mvcc] type allow concurrent read and write access at API level,
//! while behind the scenes, all write-operations are serialized into
//! single thread, but the key difference is that [Mvcc] index allow
//! concurrent-reads without using locks. To serialize concurrent writes
//! [Mvcc] uses a spin-lock implementation that can be configured to
//! _yield_ or _spin_ while waiting for the lock.
//!
//! **[LSM mode]**: Mvcc index can support log-structured-merge while
//! mutating the tree. In simple terms, this means that nothing shall be
//! over-written in the tree and all the mutations for the same key shall
//! be preserved until they are purged.
//!
//! **Possible ways to configure Mvcc**:
//!
//! *spinlatch*, relevant only in multi-threaded context. Calling
//! _set_spinlatch()_ with _true_ will have the calling thread to spin
//! while waiting to acquire the lock. Calling it with _false_ will have the
//! calling thread to yield to OS scheduler while waiting to acquire the lock.
//!
//! *sticky*, is a shallow variant of lsm, applicable only when
//! `lsm` option is disabled. For more information refer to Mvcc::set_sticky()
//! method.
//!
//! *seqno*, application can set the beginning sequence number before
//! ingesting data into the index.
//!
//! [llrb]: https://en.wikipedia.org/wiki/Left-leaning_red-black_tree
//! [mvcc]: https://en.wikipedia.org/wiki/Multiversion_concurrency_control
//! [LSM mode]: https://en.wikipedia.org/wiki/Log-structured_merge-tree
//!

use log::{debug, error, info, warn};

use std::{
    borrow::Borrow,
    cmp::{self, Ord, Ordering},
    convert::{self, TryFrom, TryInto},
    ffi, fmt,
    fmt::Debug,
    hash::Hash,
    marker, mem,
    ops::{Bound, Deref, DerefMut, RangeBounds},
    result,
    sync::{
        atomic::{AtomicIsize, AtomicPtr, AtomicUsize, Ordering::SeqCst},
        Arc,
    },
    thread,
};

use crate::{
    core::{CommitIter, Result, ScanEntry, ScanIter, Value, WalWriter, WriteIndexFactory},
    core::{CommitIterator, ToJson, Validate, Writer},
    core::{Diff, Entry, Footprint, Index, IndexIter, PiecewiseScan, Reader},
    error::Error,
    llrb::Llrb,
    llrb_node::{LlrbDepth, Node},
    scans,
    spinlock::{self, RWSpinlock},
    types::Empty,
    util,
};

// TODO: Experiment with different atomic::Ordering to improve performance.

const RECLAIM_CAP: usize = 128;

include!("llrb_common.rs");

/// MvccFactory captures a set of configuration for creating new Mvcc
/// instances. By implementing `WriteIndexFactory` trait this can be
/// used with other, more sophisticated, index implementations.
pub struct MvccFactory {
    lsm: bool,
    sticky: bool,
    spin: bool,
}

/// Create a new factory with initial set of configuration. To know
/// more about other configurations supported by the MvccFactory refer
/// to its ``set_``, methods.
///
/// * *lsm*, spawn Mvcc instances in lsm mode, this will preserve the
/// entire history of all write operations applied on the index.
/// * *sticky*, is a shallow variant of lsm, applicable only when
/// `lsm` option is disabled. For more information refer to Mvcc::set_sticky()
/// method.
pub fn mvcc_factory(lsm: bool) -> MvccFactory {
    MvccFactory {
        lsm,
        sticky: false,
        spin: true,
    }
}

impl MvccFactory {
    /// If spin is true, calling thread will spin while waiting for the
    /// latch, otherwise, calling thead will be yielded to OS scheduler.
    pub fn set_spinlatch(&mut self, spin: bool) -> &mut Self {
        self.spin = spin;
        self
    }

    /// Create all Mvcc instances in sticky mode, refer to Mvcc::set_sticky()
    /// for more details.
    pub fn set_sticky(&mut self, sticky: bool) -> &mut Self {
        self.sticky = sticky;
        self
    }

    fn to_config_string(&self) -> String {
        format!(
            "mvcc = {{ lsm = {}, sticky = {}, spin = {} }}",
            self.lsm, self.sticky, self.spin
        )
    }
}

impl<K, V> WriteIndexFactory<K, V> for MvccFactory
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    type I = Box<Mvcc<K, V>>;

    fn to_type(&self) -> String {
        "mvcc".to_string()
    }

    fn new(&self, name: &str) -> Result<Self::I> {
        info!(
            target: "mvccfc",
            "{:?}, new mvcc instance, with config {}", name, self.to_config_string()
        );

        let mut index = if self.lsm {
            Mvcc::new_lsm(name)
        } else {
            let mut index = Mvcc::new(name);
            index.set_sticky(self.sticky);
            index
        };
        index.set_spinlatch(self.spin);
        Ok(index)
    }
}

/// A [Mvcc][mvcc] variant of [LLRB][llrb] index for concurrent readers,
/// serialized writers.
///
/// [mvcc]: https://en.wikipedia.org/wiki/Multiversion_concurrency_control
/// [llrb]: https://en.wikipedia.org/wiki/Left-leaning_red-black_tree
pub struct Mvcc<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    name: String,
    lsm: bool,
    sticky: bool,
    spin: bool,

    snapshot: OuterSnapshot<K, V>,
    latch: RWSpinlock,
    key_footprint: isize,
    tree_footprint: isize,
    n_deleted: usize,
    n_reclaimed: usize,
    readers: Arc<u32>,
    writers: Arc<u32>,
}

impl<K, V> Drop for Mvcc<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn drop(&mut self) {
        loop {
            // validation check 1
            let n = self.multi_rw();
            if n == 0 {
                break;
            }
            error!(
                target: "mvcc  ",
                "{:?}, dropped before read/write handles {}", self.name, n
            );
        }

        // NOTE: Means all references to mvcc are gone and ownership is
        // going out of scope. This also implies that there are only
        // TWO Arc<snapshots>. One is held by self.snapshot and another
        // is held by its `next`.

        // NOTE: self.snapshot's AtomicPtr will fence the drop chain, so
        // we have to get past the atomic fence and drop it here.
        // NOTE: Likewise Snapshot's drop will fence the drop on its
        // `root` field, so we have to get past that and drop it here.
        // TODO: move this logic to OuterSnapshot::Drop
        {
            let mut curr_s: Box<Arc<Snapshot<K,V>>> = // current snapshot
                unsafe { Box::from_raw(self.snapshot.inner.load(SeqCst)) };
            let snapshot = Arc::get_mut(&mut *curr_s).unwrap();
            // println!("drop mvcc {:p} {:p}", self, snapshot);

            let n = match snapshot.root.take() {
                Some(root) => drop_tree(root),
                None => 0,
            };
            self.snapshot
                .n_nodes
                .fetch_sub(n.try_into().unwrap(), SeqCst);
        }

        // validation check 2
        let n = self.snapshot.n_active.load(SeqCst);
        if n != 0 {
            panic!("active snapshots: {}", n);
        }
        // validataion check 2
        let n = self.snapshot.n_nodes.load(SeqCst);
        if n != 0 {
            panic!("leak or double free n_nodes:{}", n);
        }
        debug!(target: "mvcc  ", "{:?}, dropped ...", self.name);
    }
}

impl<K, V> TryFrom<Llrb<K, V>> for Box<Mvcc<K, V>>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    type Error = Error;

    fn try_from(llrb_index: Llrb<K, V>) -> Result<Box<Mvcc<K, V>>> {
        let mut mvcc_index = if llrb_index.is_lsm() {
            Mvcc::new_lsm(llrb_index.to_name())
        } else {
            Mvcc::new(llrb_index.to_name())
        };
        mvcc_index
            .set_sticky(llrb_index.is_sticky())
            .set_spinlatch(llrb_index.is_spin());
        mvcc_index
            .snapshot
            .n_nodes
            .store(llrb_index.len().try_into()?, SeqCst);

        let debris = llrb_index.squash();
        mvcc_index.key_footprint = debris.key_footprint;
        mvcc_index.tree_footprint = debris.tree_footprint;
        mvcc_index.n_deleted = debris.n_deleted;
        mvcc_index.snapshot.shift_snapshot(
            debris.root,
            debris.seqno,
            debris.n_count,
            vec![], /*reclaim*/
        );
        Ok(mvcc_index)
    }
}

/// Construct new instance of Mvcc.
impl<K, V> Mvcc<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    pub fn new<S>(name: S) -> Box<Mvcc<K, V>>
    where
        S: AsRef<str>,
    {
        Box::new(Mvcc {
            name: name.as_ref().to_string(),
            lsm: false,
            sticky: false,
            spin: true,

            snapshot: OuterSnapshot::new(),
            latch: RWSpinlock::new(),
            key_footprint: Default::default(),
            tree_footprint: Default::default(),
            n_deleted: Default::default(),
            n_reclaimed: Default::default(),
            readers: Arc::new(0xC0FFEE),
            writers: Arc::new(0xC0FFEE),
        })
    }

    pub fn new_lsm<S>(name: S) -> Box<Mvcc<K, V>>
    where
        S: AsRef<str>,
    {
        Box::new(Mvcc {
            name: name.as_ref().to_string(),
            lsm: true,
            sticky: false,
            spin: true,

            snapshot: OuterSnapshot::new(),
            latch: RWSpinlock::new(),
            key_footprint: Default::default(),
            tree_footprint: Default::default(),
            n_deleted: Default::default(),
            n_reclaimed: Default::default(),
            readers: Arc::new(0xC0FFEE),
            writers: Arc::new(0xC0FFEE),
        })
    }

    /// Configure behaviour of spin-latch. If `spin` is true, calling
    /// thread shall spin until a latch is acquired or released, if false
    /// calling thread will yield to scheduler. Call this api, before
    /// creating reader and/or writer handles.
    pub fn set_spinlatch(&mut self, spin: bool) -> &mut Self {
        let n = self.multi_rw();
        if n > 0 {
            panic!("cannot configure Mvcc with active readers/writer {}", n);
        }
        self.spin = spin;
        self
    }

    /// Run this instance in sticky mode, which is like a shallow lsm.
    /// In sticky mode, all entries once inserted into the index will
    /// continue to live for the rest of the index life time. In
    /// practical terms this means a delete operations won't remove
    /// the entry from the index, instead the entry shall marked as
    /// deleted and but its value shall be removed.
    pub fn set_sticky(&mut self, sticky: bool) -> &mut Self {
        let n = self.multi_rw();
        if n > 0 {
            panic!("cannot configure Mvcc with active readers/writers {}", n)
        }
        self.sticky = sticky;
        self
    }

    /// Squash this index and return the root and its book-keeping.
    pub(crate) fn squash(mut self) -> SquashDebris<K, V> {
        let n = self.multi_rw();
        if n > 0 {
            panic!("cannot squash Mvcc with active readers/writer {}", n);
        }

        let snapshot =
            Arc::get_mut(unsafe { self.snapshot.inner.load(SeqCst).as_mut().unwrap() }).unwrap();

        self.n_reclaimed = 0;
        self.snapshot.n_nodes.store(0, SeqCst);
        SquashDebris {
            root: snapshot.root.take(),
            seqno: snapshot.seqno,
            n_count: snapshot.n_count,
            n_deleted: self.n_deleted,
            key_footprint: self.key_footprint,
            tree_footprint: self.tree_footprint,
        }
    }

    pub fn clone(&self) -> Box<Mvcc<K, V>> {
        let n = self.multi_rw();
        if n > 0 {
            panic!("cannot clone Mvcc with active readers/writer {}", n);
        }

        let cloned = Box::new(Mvcc {
            name: self.name.clone(),
            lsm: self.lsm,
            sticky: self.sticky,
            spin: self.spin,

            snapshot: OuterSnapshot::new(),
            latch: RWSpinlock::new(),
            key_footprint: self.key_footprint,
            tree_footprint: self.tree_footprint,
            n_deleted: self.n_deleted,
            n_reclaimed: Default::default(),
            readers: Arc::new(0xC0FFEE),
            writers: Arc::new(0xC0FFEE),
        });

        let s: Arc<Snapshot<K, V>> = OuterSnapshot::clone(&self.snapshot);
        let seqno = OuterSnapshot::clone(&self.snapshot).seqno;
        let n_count = self.to_stats().unwrap().entries;
        cloned
            .snapshot
            .n_nodes
            .store(n_count.try_into().unwrap(), SeqCst);
        let root_node = match s.as_root() {
            None => None,
            Some(n) => Some(Box::new(n.clone())),
        };
        cloned
            .snapshot
            .shift_snapshot(root_node, seqno, n_count, vec![]);
        cloned
    }
}

/// Maintanence API.
impl<K, V> Mvcc<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Return whether this index support lsm mode.
    #[inline]
    pub fn is_lsm(&self) -> bool {
        self.lsm
    }

    pub(crate) fn is_spin(&self) -> bool {
        self.spin
    }

    pub(crate) fn is_sticky(&self) -> bool {
        self.sticky
    }

    /// Return number of entries in this instance.
    #[inline]
    pub fn len(&self) -> usize {
        OuterSnapshot::clone(&self.snapshot).n_count
    }

    /// Identify this instance. Applications can choose unique names while
    /// creating Mvcc instances.
    #[inline]
    pub fn to_name(&self) -> String {
        self.name.clone()
    }

    /// Return quickly with basic statisics, only entries() method is valid
    /// with this statisics.
    pub fn to_stats(&self) -> Result<Stats> {
        let _r = self.latch.acquire_read(true /*spin*/);

        let mut stats = Stats::new(&self.name);
        stats.entries = self.len();
        stats.key_footprint = self.key_footprint;
        stats.tree_footprint = self.tree_footprint;
        stats.n_deleted = self.n_deleted;
        stats.n_reclaimed = self.n_reclaimed;
        stats.rw_latch = self.latch.to_stats()?;
        stats.snapshot_latch = self.snapshot.ulatch.to_stats()?;
        Ok(stats)
    }

    fn multi_rw(&self) -> usize {
        Arc::strong_count(&self.readers) + Arc::strong_count(&self.writers) - 2
    }
}

impl<K, V> Mvcc<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn node_new_deleted(&self, key: K, seqno: u64) -> Box<Node<K, V>> {
        self.snapshot.n_nodes.fetch_add(1, SeqCst);
        Node::new_deleted(key, seqno)
    }

    fn node_mvcc_clone(
        &self,
        node: &Node<K, V>, // source node
        reclaim: &mut Vec<Box<Node<K, V>>>,
        copyval: bool,
    ) -> Box<Node<K, V>> {
        self.snapshot.n_nodes.fetch_add(1, SeqCst);
        node.mvcc_clone(reclaim, copyval)
    }

    fn node_from_entry(&self, new_entry: Entry<K, V>) -> Box<Node<K, V>> {
        self.snapshot.n_nodes.fetch_add(1, SeqCst);
        Box::new(From::from(new_entry))
    }

    fn node_mvcc_detach(&self, node: &mut Box<Node<K, V>>) {
        node.mvcc_detach();
        self.snapshot.n_nodes.as_ref().fetch_sub(1, SeqCst);
    }
}

impl<K, V> Index<K, V> for Box<Mvcc<K, V>>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    type W = MvccWriter<K, V>;
    type R = MvccReader<K, V>;
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

    fn commit<C, F>(&mut self, scanner: CommitIter<K, V, C>, metacb: F) -> Result<()>
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

    fn close(self) -> Result<()> {
        (*self).close()
    }

    /// End of index life-cycle. Also clears persisted data (in disk).
    fn purge(self) -> Result<()> {
        (*self).purge()
    }
}

impl<K, V> Index<K, V> for Mvcc<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    type W = MvccWriter<K, V>;
    type R = MvccReader<K, V>;
    type O = Empty;

    fn to_name(&self) -> Result<String> {
        Ok(self.name.clone())
    }

    fn to_root(&self) -> Result<Empty> {
        Ok(Empty)
    }

    fn to_metadata(&self) -> Result<Vec<u8>> {
        Ok(vec![])
    }

    fn to_seqno(&self) -> Result<u64> {
        Ok(OuterSnapshot::clone(&self.snapshot).seqno)
    }

    fn set_seqno(&mut self, seqno: u64) -> Result<()> {
        let n = self.multi_rw();
        if n > 0 {
            panic!("cannot configure Mvcc with active readers/writer {}", n);
        }

        let s = OuterSnapshot::clone(&self.snapshot);
        let root = s.root_duplicate();
        self.snapshot.shift_snapshot(root, seqno, s.n_count, vec![]);

        Ok(())
    }

    /// Lockless concurrent readers are supported
    fn to_reader(&mut self) -> Result<Self::R> {
        let index: Box<ffi::c_void> = unsafe {
            // transmute self as void pointer.
            Box::from_raw(self as *mut Mvcc<K, V> as *mut ffi::c_void)
        };
        let reader = Arc::clone(&self.readers);
        Ok(MvccReader::<K, V>::new(index, reader))
    }

    /// Create a new writer handle. Multiple writers uses spin-lock to
    /// serialize write operation.
    fn to_writer(&mut self) -> Result<Self::W> {
        let index: Box<ffi::c_void> = unsafe {
            // transmute self as void pointer.
            Box::from_raw(self as *mut Mvcc<K, V> as *mut ffi::c_void)
        };
        let writer = Arc::clone(&self.writers);
        Ok(MvccWriter::<K, V>::new(index, writer))
    }

    fn commit<C, F>(&mut self, mut scanner: CommitIter<K, V, C>, _metacb: F) -> Result<()>
    where
        C: CommitIterator<K, V>,
        F: Fn(Vec<u8>) -> Vec<u8>,
    {
        warn!(target: "mvcc  ", "{:?}, ignores all metadata", self.name);

        let full_table_iter = scanner.scan()?;
        let count = {
            let _latch = self.latch.acquire_write(self.spin);

            let mut count = 0;
            for entry in full_table_iter {
                self.set_index_entry(entry?)?;
                count += 1;
            }
            count
        };

        info!(target: "mvcc  ", "{:?}, committed {} items", self.name, count);
        Ok(())
    }

    fn compact<F>(&mut self, cutoff: Bound<u64>, _metacb: F) -> Result<usize>
    where
        F: Fn(Vec<Vec<u8>>) -> Vec<u8>,
    {
        // before proceeding with compaction, verify the cutoff argument for
        // unusual values.
        match cutoff {
            Bound::Unbounded => {
                warn!(target: "mvcc  ", "compact with unbounded cutoff");
            }
            Bound::Included(seqno) if seqno >= self.to_seqno()? => {
                warn!(target: "mvcc  ", "compact cutsoff the entire index {}", seqno);
            }
            Bound::Excluded(seqno) if seqno > self.to_seqno()? => {
                warn!(target: "mvcc  ", "compact cutsoff the entire index {}", seqno);
            }
            _ => (),
        }

        let (mut low, mut count) = (Bound::Unbounded, 0);
        const LIMIT: usize = 1_000; // TODO: no magic number
        let count = loop {
            let (seen, limit) = {
                let _latch = self.latch.acquire_write(self.spin);

                let snapshot: &Arc<Snapshot<K, V>> = self.snapshot.as_ref();
                let root = snapshot.root_duplicate();
                let mut cc = CompactCtxt {
                    cutoff,
                    dels: vec![],
                    tree_footprint: self.tree_footprint,
                    reclaim: vec![],
                };
                let (root, seen, limit) = self.compact_loop(root, low, &mut cc, LIMIT)?;
                self.n_reclaimed += cc.reclaim.len();
                self.tree_footprint = cc.tree_footprint;
                self.snapshot
                    .shift_snapshot(root, snapshot.seqno, snapshot.n_count, cc.reclaim);

                for key in cc.dels.into_iter() {
                    self.delete_index_entry(key)?;
                }
                (seen, limit)
            };

            match (seen, limit) {
                (_, limit) if limit > 0 => break count + (LIMIT - limit),
                (Some(key), _) => low = Bound::Excluded(key),
                _ => unreachable!(),
            }
            count += LIMIT;
        };
        info!(target: "mvcc  ", "{:?}, compacted {} items", self.name, count);
        Ok(count)
    }

    fn close(self) -> Result<()> {
        Ok(())
    }

    fn purge(self) -> Result<()> {
        self.close()
    }
}

impl<K, V> Footprint for Box<Mvcc<K, V>>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn footprint(&self) -> Result<isize> {
        self.as_ref().footprint()
    }
}

impl<K, V> Footprint for Mvcc<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn footprint(&self) -> Result<isize> {
        let _r = self.latch.acquire_read(true /*spin*/);

        Ok(self.tree_footprint)
    }
}

impl<K, V> Mvcc<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    pub fn set_index(
        &mut self,
        key: K,
        value: V,
        seqno: Option<u64>,
    ) -> Result<(u64, Option<Entry<K, V>>)> {
        let _w = self.latch.acquire_write(self.spin);

        let entry = {
            let snapshot: &Arc<Snapshot<K, V>> = self.snapshot.as_ref();
            let seqno = match seqno {
                Some(seqno) => seqno,
                None => snapshot.seqno + 1,
            };
            Entry::new(key, Value::new_upsert_value(value, seqno))
        };
        let (seqno, old_entry) = self.set_index_entry(entry)?;
        if let Some(old_entry) = &old_entry {
            if old_entry.is_deleted() && (!self.lsm && !self.sticky) {
                panic!("impossible case");
            }
        }
        Ok((seqno, old_entry))
    }

    pub fn set_cas_index(
        &mut self,
        key: K,
        value: V,
        cas: u64,
        seqno: Option<u64>, // seqno for this mutation
    ) -> Result<(u64, Result<Option<Entry<K, V>>>)> {
        let _w = self.latch.acquire_write(self.spin);

        let snapshot: &Arc<Snapshot<K, V>> = self.snapshot.as_ref();

        let seqno = match seqno {
            Some(seqno) => seqno,
            None => snapshot.seqno + 1,
        };
        let lsm = self.lsm;
        let key_footprint = util::key_footprint(&key)?;

        let new_entry = Entry::new(key, Value::new_upsert_value(value, seqno));

        let mut n_count = snapshot.n_count;
        let root = snapshot.root_duplicate();
        let mut rclm: Vec<Box<Node<K, V>>> = Vec::with_capacity(RECLAIM_CAP);
        let s = match self.upsert_cas(root, new_entry, cas, lsm, &mut rclm)? {
            UpsertCasResult {
                node: Some(mut root),
                new_node,
                old_entry,
                err: None,
                size,
            } => {
                match &old_entry {
                    None => {
                        self.key_footprint += key_footprint;
                        n_count += 1;
                    }
                    Some(oe) if oe.is_deleted() && (self.lsm || self.sticky) => {
                        self.n_deleted -= 1;
                    }
                    _ => (),
                }
                self.tree_footprint += size;

                root.set_black();
                (seqno, root, new_node, Ok(old_entry))
            }
            UpsertCasResult {
                node: Some(mut root),
                new_node,
                err: Some(err),
                ..
            } => {
                root.set_black();
                (seqno, root, new_node, Err(err))
            }
            _ => panic!("set_cas: impossible case, call programmer"),
        };
        let (seqno, root, optn, entry) = s;

        if let Some(mut n) = optn {
            n.dirty = false;
            Box::leak(n);
        }

        // TODO: can we optimize this for no-op cases (err cases) ?
        self.n_reclaimed += rclm.len();
        self.snapshot.shift_snapshot(
            // new snapshot
            Some(root),
            seqno,
            n_count,
            rclm,
        );
        Ok((seqno, entry))
    }

    pub fn delete_index<Q>(
        &mut self,
        key: &Q,
        seqno: Option<u64>, // seqno for this mutation
    ) -> Result<(u64, Result<Option<Entry<K, V>>>)>
    where
        // TODO: From<Q> and Clone will fail if V=String and Q=str
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        let _w = self.latch.acquire_write(self.spin);

        let snapshot: &Arc<Snapshot<K, V>> = self.snapshot.as_ref();
        let seqno = match seqno {
            Some(seqno) => seqno,
            None => snapshot.seqno + 1,
        };
        let key_footprint = util::key_footprint(&key.to_owned())?;

        let mut n_count = snapshot.n_count;
        let root = snapshot.root_duplicate();
        let mut rclm: Vec<Box<Node<K, V>>> = Vec::with_capacity(RECLAIM_CAP);
        let (seqno, root, old_entry) = if self.lsm || self.sticky {
            let res = if self.lsm {
                self.delete_lsm(root, key, seqno, &mut rclm)?
            } else {
                self.delete_sticky(root, key, seqno, &mut rclm)?
            };

            let s = match res {
                DeleteResult {
                    node: Some(mut root),
                    new_node,
                    old_entry,
                    size,
                } => {
                    root.set_black();
                    (Some(root), new_node, old_entry, size)
                }
                DeleteResult {
                    node: None,
                    new_node,
                    old_entry,
                    size,
                } => (None, new_node, old_entry, size),
            };
            let (root, new_node, old_entry, size) = s;

            self.tree_footprint += size;
            // println!("delete {:?}", entry.as_ref().map(|e| e.is_deleted()));
            match &old_entry {
                None => {
                    self.key_footprint += key_footprint;
                    n_count += 1;
                    self.n_deleted += 1;
                }
                Some(entry) if !entry.is_deleted() => self.n_deleted += 1,
                _ => (),
            }
            if let Some(mut n) = new_node {
                n.dirty = false;
                Box::leak(n);
            }
            (seqno, root, old_entry)
        } else {
            // in non-lsm mode remove the entry from the tree.
            let res = match self.do_delete(root, key, &mut rclm)? {
                res @ DeleteResult { node: None, .. } => res,
                mut res => {
                    res.node.as_mut().map(|node| node.set_black());
                    res
                }
            };
            let seqno = if res.old_entry.is_some() {
                self.key_footprint -= key_footprint;
                self.tree_footprint += res.size;
                n_count -= 1;
                seqno
            } else {
                seqno
            };
            (seqno, res.node, res.old_entry)
        };

        self.n_reclaimed += rclm.len();
        self.snapshot.shift_snapshot(root, seqno, n_count, rclm);
        Ok((seqno, Ok(old_entry)))
    }
}

/// Create/Update/Delete operations on Mvcc instance.
impl<K, V> Writer<K, V> for Mvcc<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    /// Set {key, value} pair into index. If key is already
    /// present, update the value and return the previous entry, else
    /// create a new entry.
    ///
    /// *LSM mode*: Add a new version for the key, perserving the old value.
    fn set(&mut self, key: K, value: V) -> Result<Option<Entry<K, V>>> {
        let (_seqno, old_entry) = self.set_index(key, value, None)?;
        Ok(old_entry)
    }

    /// Similar to set, but succeeds only when CAS matches with entry's
    /// last `seqno`. In other words, since seqno is unique to each mutation,
    /// we use `seqno` of the mutation as the CAS value. Use CAS == 0 to
    /// enforce a create operation.
    ///
    /// *LSM mode*: Add a new version for the key, perserving the old value.
    fn set_cas(&mut self, key: K, value: V, cas: u64) -> Result<Option<Entry<K, V>>> {
        let (_seqno, entry) = self.set_cas_index(key, value, cas, None)?;
        entry
    }

    /// Delete the given key. Note that back-to-back delete for the same
    /// key shall collapse into a single delete, first delete is ingested
    /// while the rest are ignored.
    ///
    /// *LSM mode*: Mark the entry as deleted along with seqno at which it
    /// deleted
    ///
    /// NOTE: K should be borrowable as &Q and Q must be convertable to
    /// owned K. This is require in lsm mode, where owned K must be
    /// inserted into the tree.
    fn delete<Q>(&mut self, key: &Q) -> Result<Option<Entry<K, V>>>
    where
        // TODO: From<Q> and Clone will fail if V=String and Q=str
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        let (_seqno, entry) = self.delete_index(key, None)?;
        entry
    }
}

struct UpsertResult<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    node: Option<Box<Node<K, V>>>,
    new_node: Option<Box<Node<K, V>>>,
    old_entry: Option<Entry<K, V>>,
    size: isize, // differencen in footprint
}

struct UpsertCasResult<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    node: Option<Box<Node<K, V>>>,
    new_node: Option<Box<Node<K, V>>>,
    old_entry: Option<Entry<K, V>>,
    size: isize, // difference in footprint
    err: Option<Error>,
}

struct DeleteResult<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    node: Option<Box<Node<K, V>>>,
    new_node: Option<Box<Node<K, V>>>,
    old_entry: Option<Entry<K, V>>,
    size: isize, // difference in footprint
}

/// Create/Update/Delete operations on Mvcc instance.
impl<K, V> Mvcc<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn upsert(
        &self,
        node: Option<Box<Node<K, V>>>,
        new_entry: Entry<K, V>,
        lsm: bool,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> Result<UpsertResult<K, V>> {
        if node.is_none() {
            let node: Box<Node<K, V>> = self.node_from_entry(new_entry);
            let n = node.duplicate();
            let size: isize = node.footprint()?.try_into()?;
            return Ok(UpsertResult {
                node: Some(node),
                new_node: Some(n),
                old_entry: None,
                size,
            });
        }

        let node = node.unwrap();
        let r = match node.as_key().cmp(new_entry.as_key()) {
            Ordering::Greater => {
                let mut new_node = self.node_mvcc_clone(&node, reclaim, false);
                let left = new_node.left.take();
                let mut r = self.upsert(left, new_entry, lsm, reclaim)?;
                new_node.left = r.node;
                r.node = Some(self.walkuprot_23(new_node, reclaim));
                r
            }
            Ordering::Less => {
                let mut new_node = self.node_mvcc_clone(&node, reclaim, false);
                let right = new_node.right.take();
                let mut r = self.upsert(right, new_entry, lsm, reclaim)?;
                new_node.right = r.node;
                r.node = Some(self.walkuprot_23(new_node, reclaim));
                r
            }
            Ordering::Equal if lsm => {
                let mut new_node = self.node_mvcc_clone(&node, reclaim, true);
                let size = new_node.footprint()?;
                let (old_entry, entry) = {
                    let entry = new_node.entry.clone();
                    (Some(entry.clone()), entry)
                };
                new_node.entry = entry.xmerge(new_entry)?;
                let size = new_node.footprint()? - size;

                new_node.dirty = true;
                let n = new_node.duplicate();
                UpsertResult {
                    node: Some(self.walkuprot_23(new_node, reclaim)),
                    new_node: Some(n),
                    old_entry: old_entry,
                    size,
                }
            }
            Ordering::Equal => {
                let mut new_node = self.node_mvcc_clone(&node, reclaim, true);
                let entry = new_node.entry.clone();
                let size = new_node.prepend_version(new_entry, lsm)?;

                new_node.dirty = true;
                let n = new_node.duplicate();
                UpsertResult {
                    node: Some(self.walkuprot_23(new_node, reclaim)),
                    new_node: Some(n),
                    old_entry: Some(entry),
                    size,
                }
            }
        };

        Box::leak(node);
        Ok(r)
    }

    fn upsert_cas(
        &self,
        node: Option<Box<Node<K, V>>>,
        nentry: Entry<K, V>,
        cas: u64,
        lsm: bool,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> Result<UpsertCasResult<K, V>> {
        if node.is_none() && cas > 0 {
            return Ok(UpsertCasResult {
                node: None,
                new_node: None,
                old_entry: None,
                size: 0,
                err: Some(Error::InvalidCAS(0)),
            });
        } else if node.is_none() {
            let node: Box<Node<K, V>> = self.node_from_entry(nentry);
            let n = node.duplicate();
            let size: isize = node.footprint()?.try_into()?;
            return Ok(UpsertCasResult {
                node: Some(node),
                new_node: Some(n),
                old_entry: None,
                size,
                err: None,
            });
        }

        let node = node.unwrap();
        let cmp = node.as_key().cmp(nentry.as_key());
        let r = if cmp == Ordering::Greater {
            let mut newnd = self.node_mvcc_clone(&node, reclaim, false);
            let left = newnd.left.take();
            let mut r = self.upsert_cas(left, nentry, cas, lsm, reclaim)?;
            newnd.left = r.node;
            r.node = Some(self.walkuprot_23(newnd, reclaim));
            r
        } else if cmp == Ordering::Less {
            let mut newnd = self.node_mvcc_clone(&node, reclaim, false);
            let right = newnd.right.take();
            let mut r = self.upsert_cas(right, nentry, cas, lsm, reclaim)?;
            newnd.right = r.node;
            r.node = Some(self.walkuprot_23(newnd, reclaim));
            r
        } else if node.is_deleted() && cas != 0 && cas != node.to_seqno() {
            let newnd = self.node_mvcc_clone(&node, reclaim, true);
            UpsertCasResult {
                node: Some(newnd),
                new_node: None,
                old_entry: None,
                size: 0,
                err: Some(Error::InvalidCAS(node.to_seqno())),
            }
        } else if !node.is_deleted() && cas != node.to_seqno() {
            let newnd = self.node_mvcc_clone(&node, reclaim, true);
            UpsertCasResult {
                node: Some(newnd),
                new_node: None,
                old_entry: None,
                size: 0,
                err: Some(Error::InvalidCAS(node.to_seqno())),
            }
        } else {
            let mut newnd = self.node_mvcc_clone(&node, reclaim, true);
            let entry = Some(node.entry.clone());
            let size = newnd.prepend_version(nentry, lsm)?;
            newnd.dirty = true;
            let n = newnd.duplicate();
            UpsertCasResult {
                node: Some(self.walkuprot_23(newnd, reclaim)),
                new_node: Some(n),
                old_entry: entry,
                size,
                err: None,
            }
        };

        Box::leak(node);
        Ok(r)
    }

    fn delete_lsm<Q>(
        &self,
        node: Option<Box<Node<K, V>>>,
        key: &Q,
        seqno: u64,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> Result<DeleteResult<K, V>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        if node.is_none() {
            let mut node = self.node_new_deleted(key.to_owned(), seqno);
            node.dirty = false;
            let n = node.duplicate();
            let size: isize = node.footprint()?.try_into()?;
            return Ok(DeleteResult {
                node: Some(node),
                new_node: Some(n),
                old_entry: None,
                size,
            });
        }

        let node = node.unwrap();
        let (new_node, n, entry, size) = match node.as_key().borrow().cmp(&key) {
            Ordering::Greater => {
                let mut new_node = self.node_mvcc_clone(&node, reclaim, false);
                let left = new_node.left.take();
                let r = self.delete_lsm(left, key, seqno, reclaim)?;
                new_node.left = r.node;
                (new_node, r.new_node, r.old_entry, r.size)
            }
            Ordering::Less => {
                let mut new_node = self.node_mvcc_clone(&node, reclaim, false);
                let right = new_node.right.take();
                let r = self.delete_lsm(right, key, seqno, reclaim)?;
                new_node.right = r.node;
                (new_node, r.new_node, r.old_entry, r.size)
            }
            Ordering::Equal => {
                let mut new_node = self.node_mvcc_clone(&node, reclaim, true);
                let old_entry = node.entry.clone();
                let size = new_node.delete(seqno)?;
                new_node.dirty = true;
                let n = new_node.duplicate();
                (new_node, Some(n), Some(old_entry), size)
            }
        };

        Box::leak(node);
        Ok(DeleteResult {
            node: Some(self.walkuprot_23(new_node, reclaim)),
            new_node: n,
            old_entry: entry,
            size,
        })
    }

    fn delete_sticky<Q>(
        &self,
        node: Option<Box<Node<K, V>>>,
        key: &Q,
        seqno: u64,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> Result<DeleteResult<K, V>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        if node.is_none() {
            let mut node = self.node_new_deleted(key.to_owned(), seqno);
            node.dirty = false;
            let n = node.duplicate();
            let size: isize = node.footprint()?.try_into()?;
            return Ok(DeleteResult {
                node: Some(node),
                new_node: Some(n),
                old_entry: None,
                size,
            });
        }

        let node = node.unwrap();
        let (new_node, n, entry, size) = match node.as_key().borrow().cmp(&key) {
            Ordering::Greater => {
                let mut new_node = self.node_mvcc_clone(&node, reclaim, false);
                let left = new_node.left.take();
                let r = self.delete_sticky(left, key, seqno, reclaim)?;
                new_node.left = r.node;
                (new_node, r.new_node, r.old_entry, r.size)
            }
            Ordering::Less => {
                let mut new_node = self.node_mvcc_clone(&node, reclaim, false);
                let right = new_node.right.take();
                let r = self.delete_sticky(right, key, seqno, reclaim)?;
                new_node.right = r.node;
                (new_node, r.new_node, r.old_entry, r.size)
            }
            Ordering::Equal => {
                let mut size = node.footprint()?;
                let cutoff = Bound::Included(node.to_seqno());
                let mut new_node = self.node_mvcc_clone(&node, reclaim, true);
                let old_entry = node.entry.clone();
                new_node.delete(seqno)?;
                new_node.dirty = true;
                new_node.entry = new_node.entry.clone().purge(cutoff).unwrap();
                size = new_node.footprint()? - size;
                let n = new_node.duplicate();
                (new_node, Some(n), Some(old_entry), size)
            }
        };

        Box::leak(node);
        Ok(DeleteResult {
            node: Some(self.walkuprot_23(new_node, reclaim)),
            new_node: n,
            old_entry: entry,
            size,
        })
    }

    // this is the non-lsm path.
    fn do_delete<Q>(
        &self,
        node: Option<Box<Node<K, V>>>,
        key: &Q,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> Result<DeleteResult<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        if node.is_none() {
            return Ok(DeleteResult {
                node: None,
                new_node: None,
                old_entry: None,
                size: 0,
            });
        }

        let node = node.unwrap();
        let mut newnd = self.node_mvcc_clone(&node, reclaim, true);
        Box::leak(node);

        if newnd.as_key().borrow().gt(key) {
            if newnd.left.is_none() {
                // key not present, nothing to delete
                Ok(DeleteResult {
                    node: Some(newnd),
                    new_node: None,
                    old_entry: None,
                    size: 0,
                })
            } else {
                let ok = !is_red(newnd.as_left_deref());
                if ok && !is_red(newnd.left.as_ref().unwrap().as_left_deref()) {
                    newnd = self.move_red_left(newnd, reclaim);
                }
                let mut r = self.do_delete(newnd.left.take(), key, reclaim)?;
                newnd.left = r.node;
                r.node = Some(self.fixup(newnd, reclaim));
                Ok(r)
            }
        } else {
            if is_red(newnd.as_left_deref()) {
                newnd = self.rotate_right(newnd, reclaim);
            }

            // if key equals node and no right children
            if !newnd.as_key().borrow().lt(key) && newnd.right.is_none() {
                self.node_mvcc_detach(&mut newnd);
                let size: isize = newnd.footprint()?.try_into()?;
                return Ok(DeleteResult {
                    node: None,
                    new_node: None,
                    old_entry: Some(newnd.entry.clone()),
                    size: -size,
                });
            }

            let ok = newnd.right.is_some() && !is_red(newnd.as_right_deref());
            if ok && !is_red(newnd.right.as_ref().unwrap().as_left_deref()) {
                newnd = self.move_red_right(newnd, reclaim);
            }

            // if key equal node and there is a right children
            if !newnd.as_key().borrow().lt(key) {
                // node == key
                let right = newnd.right.take();
                let (right, mut res_node) = self.delete_min(right, reclaim)?;
                newnd.right = right;
                if res_node.is_none() {
                    panic!("do_delete(): fatal logic, call the programmer");
                }
                let mut newnode = res_node.take().unwrap();
                newnode.left = newnd.left.take();
                newnode.right = newnd.right.take();
                newnode.black = newnd.black;
                let entry = newnd.entry.clone();
                let size: isize = newnd.footprint()?.try_into()?;
                Ok(DeleteResult {
                    node: Some(self.fixup(newnode, reclaim)),
                    new_node: None,
                    old_entry: Some(entry),
                    size: -size,
                })
            } else {
                let mut r = self.do_delete(newnd.right.take(), key, reclaim)?;
                newnd.right = r.node;
                r.node = Some(self.fixup(newnd, reclaim));
                Ok(r)
            }
        }
    }

    // return [node, old_node]
    fn delete_min(
        &self,
        node: Option<Box<Node<K, V>>>,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> Result<(Option<Box<Node<K, V>>>, Option<Box<Node<K, V>>>)> {
        if node.is_none() {
            return Ok((None, None));
        }

        let node = node.unwrap();
        let mut new_node = self.node_mvcc_clone(&node, reclaim, true);
        Box::leak(node);

        if new_node.left.is_none() {
            self.node_mvcc_detach(&mut new_node);
            Ok((None, Some(new_node)))
        } else {
            let left = new_node.as_left_deref();
            if !is_red(left) && !is_red(left.unwrap().as_left_deref()) {
                new_node = self.move_red_left(new_node, reclaim);
            }
            let left = new_node.left.take();
            let (left, old_node) = self.delete_min(left, reclaim)?;
            new_node.left = left;
            Ok((Some(self.fixup(new_node, reclaim)), old_node))
        }
    }
}

impl<K, V> Mvcc<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn set_index_entry(&self, entry: Entry<K, V>) -> Result<(u64, Option<Entry<K, V>>)> {
        let mself = unsafe {
            // caller hold a write latch.
            (self as *const Self as *mut Self).as_mut().unwrap()
        };

        let snapshot: &Arc<Snapshot<K, V>> = mself.snapshot.as_ref();
        let key_footprint = util::key_footprint(entry.as_key())?;
        let (seqno, deleted) = (entry.to_seqno(), entry.is_deleted());

        let mut n_count = snapshot.n_count;
        let root = snapshot.root_duplicate();
        let mut rclm: Vec<Box<Node<K, V>>> = Vec::with_capacity(RECLAIM_CAP);
        match mself.upsert(root, entry, mself.lsm, &mut rclm)? {
            UpsertResult {
                node: Some(mut root),
                new_node: Some(mut n),
                old_entry,
                size,
            } => {
                // println!("set_index_entry, result {}", size);
                match &old_entry {
                    None => {
                        n_count += 1;
                        mself.key_footprint += key_footprint;
                        if deleted {
                            mself.n_deleted += 1;
                        }
                    }
                    Some(oe) => match (oe.is_deleted(), deleted) {
                        (true, false) => mself.n_deleted -= 1,
                        (false, true) => mself.n_deleted += 1,
                        _ => (),
                    },
                }
                mself.n_reclaimed += rclm.len();
                mself.tree_footprint += size;

                root.set_black();
                n.dirty = false;
                Box::leak(n);
                let seqno = cmp::max(snapshot.seqno, seqno);
                mself
                    .snapshot
                    .shift_snapshot(Some(root), seqno, n_count, rclm);
                Ok((seqno, old_entry))
            }
            _ => panic!("set: impossible case, call programmer"),
        }
    }
}

struct CompactCtxt<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    cutoff: Bound<u64>,
    dels: Vec<K>,
    tree_footprint: isize,
    reclaim: Vec<Box<Node<K, V>>>,
}

impl<K, V> Mvcc<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn compact_loop(
        &self,
        node: Option<Box<Node<K, V>>>,
        low: Bound<K>,
        cc: &mut CompactCtxt<K, V>,
        limit: usize,
    ) -> Result<(Option<Box<Node<K, V>>>, Option<K>, usize)> {
        use std::ops::Bound::{Excluded, Unbounded};

        let mself = unsafe {
            // caller hold a write latch.
            (self as *const Self as *mut Self).as_mut().unwrap()
        };

        match (node, low) {
            (None, _) => Ok((None, None, limit)),
            // find the starting point
            (Some(node), Unbounded) => {
                let mut newn = mself.node_mvcc_clone(&node, &mut cc.reclaim, true);
                Box::leak(node);
                match mself.compact_loop(newn.left.take(), Unbounded, cc, limit)? {
                    (left, seen, limit) if limit == 0 => {
                        newn.left = left;
                        Ok((Some(newn), seen, limit))
                    }
                    (left, _, limit) => {
                        newn.left = left;
                        Self::compact_entry(&mut newn, cc)?;
                        let (right, low) = (newn.right.take(), Unbounded);
                        let (right, seen, limit) =
                            match mself.compact_loop(right, low, cc, limit - 1)? {
                                (right, None, limit) => (right, Some(newn.to_key()), limit),
                                res => res,
                            };
                        newn.right = right;
                        Ok((Some(newn), seen, limit))
                    }
                }
            }
            (Some(node), Excluded(key)) => {
                let mut newn = mself.node_mvcc_clone(&node, &mut cc.reclaim, true);
                Box::leak(node);
                match key.cmp(newn.as_key()) {
                    Ordering::Less => {
                        let (left, low) = (newn.left.take(), Excluded(key));
                        match mself.compact_loop(left, low, cc, limit)? {
                            (left, seen, limit) if limit == 0 => {
                                newn.left = left;
                                Ok((Some(newn), seen, limit))
                            }
                            (left, _, limit) => {
                                newn.left = left;
                                Self::compact_entry(&mut newn, cc)?;
                                let (right, low) = (newn.right.take(), Unbounded);
                                let (right, seen, limit) =
                                    match mself.compact_loop(right, low, cc, limit - 1)? {
                                        (right, None, limit) => (right, Some(newn.to_key()), limit),
                                        res => res,
                                    };
                                newn.right = right;
                                Ok((Some(newn), seen, limit))
                            }
                        }
                    }
                    _ => {
                        let right = newn.right.take();
                        let (right, seen, limit) =
                            mself.compact_loop(right, Excluded(key), cc, limit)?;
                        newn.right = right;
                        Ok((Some(newn), seen, limit))
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    fn compact_entry(node: &mut Node<K, V>, cc: &mut CompactCtxt<K, V>) -> Result<()> {
        let (size, key) = (node.entry.footprint()?, node.entry.to_key());
        cc.tree_footprint += match node.entry.clone().purge(cc.cutoff) {
            None => {
                cc.dels.push(key);
                0
            }
            Some(entry) => {
                node.entry = entry;
                node.entry.footprint()? - size
            }
        };

        Ok(())
    }

    fn delete_index_entry(&self, key: K) -> Result<()> {
        let mself = unsafe {
            // caller hold a write latch.
            (self as *const Self as *mut Self).as_mut().unwrap()
        };

        // in non-lsm mode remove the entry from the tree.
        let snapshot: &Arc<Snapshot<K, V>> = mself.snapshot.as_ref();
        let mut n_count = snapshot.n_count;
        let root = snapshot.root_duplicate();

        let mut rclm: Vec<Box<Node<K, V>>> = Vec::with_capacity(RECLAIM_CAP);
        let res = match mself.do_delete(root, key.borrow(), &mut rclm)? {
            res @ DeleteResult { node: None, .. } => res,
            mut res => {
                res.node.as_mut().map(|node| node.set_black());
                res
            }
        };
        match res.old_entry {
            Some(old_entry) => {
                mself.key_footprint -= util::key_footprint(&key)?;
                mself.tree_footprint += res.size;
                n_count -= 1;
                if old_entry.is_deleted() {
                    mself.n_deleted -= 1;
                }
            }
            None => unreachable!(),
        }
        mself.n_reclaimed += rclm.len();
        mself
            .snapshot
            .shift_snapshot(res.node, snapshot.seqno, n_count, rclm);

        Ok(())
    }
}

/// Read operations on Mvcc instance.
impl<K, V> Reader<K, V> for Mvcc<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Get the latest version for key.
    fn get<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + Hash,
    {
        let snapshot: Arc<Snapshot<K, V>> = OuterSnapshot::clone(&self.snapshot);
        let res = get(snapshot.as_root(), key);
        res
    }

    fn iter(&mut self) -> Result<IndexIter<K, V>> {
        let mut iter = Box::new(Iter {
            _latch: Default::default(),
            _arc: OuterSnapshot::clone(&self.snapshot),
            paths: Default::default(),
        });
        let root = iter
            ._arc
            .as_ref()
            .root_duplicate()
            .map(|n| Box::leak(n) as &Node<K, V>);
        iter.paths = Some(build_iter(IFlag::Left, root, vec![]));
        Ok(iter)
    }

    fn range<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let mut r = Box::new(Range {
            _latch: Default::default(),
            _arc: OuterSnapshot::clone(&self.snapshot),
            range,
            paths: Default::default(),
            high: marker::PhantomData,
        });
        let root = r
            ._arc
            .as_ref()
            .root_duplicate()
            .map(|n| Box::leak(n) as &Node<K, V>);
        r.paths = match r.range.start_bound() {
            Bound::Unbounded => Some(build_iter(IFlag::Left, root, vec![])),
            Bound::Included(low) => Some(find_start(root, low, true, vec![])),
            Bound::Excluded(low) => Some(find_start(root, low, false, vec![])),
        };
        Ok(r)
    }

    fn reverse<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let mut r = Box::new(Reverse {
            _latch: Default::default(),
            _arc: OuterSnapshot::clone(&self.snapshot),
            range,
            paths: Default::default(),
            low: marker::PhantomData,
        });
        let root = r
            ._arc
            .as_ref()
            .root_duplicate()
            .map(|n| Box::leak(n) as &Node<K, V>);
        r.paths = match r.range.end_bound() {
            Bound::Unbounded => Some(build_iter(IFlag::Right, root, vec![])),
            Bound::Included(high) => Some(find_end(root, high, true, vec![])),
            Bound::Excluded(high) => Some(find_end(root, high, false, vec![])),
        };
        Ok(r)
    }

    /// Short circuited to get().
    fn get_with_versions<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + Hash,
    {
        self.get(key)
    }

    /// Short circuited to iter().
    fn iter_with_versions(&mut self) -> Result<IndexIter<K, V>> {
        self.iter()
    }

    /// Short circuited to range().
    fn range_with_versions<'a, R, Q>(
        &'a mut self, // reader cannot be shared
        range: R,
    ) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        self.range(range)
    }

    /// Short circuited to reverse()
    fn reverse_with_versions<'a, R, Q>(
        &'a mut self, // reader cannot be shared
        range: R,
    ) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        self.reverse(range)
    }
}

impl<K, V> CommitIterator<K, V> for Box<Mvcc<K, V>>
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

impl<K, V> CommitIterator<K, V> for Mvcc<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn scan<G>(&mut self, within: G) -> Result<IndexIter<K, V>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let mut ss = scans::SkipScan::new(self.to_reader()?);
        ss.set_seqno_range(within);
        Ok(Box::new(ss))
    }

    fn scans<G>(&mut self, shards: usize, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
    {
        match shards {
            0 => return Ok(vec![]),
            1 => return Ok(vec![self.scan(within)?]),
            _ => (),
        }

        let keys = {
            let snapshot: Arc<Snapshot<K, V>> = OuterSnapshot::clone(&self.snapshot);
            let mut keys = vec![];
            do_shards(snapshot.as_root(), shards - 1, &mut keys);
            keys
        };
        let keys: Vec<K> = keys.into_iter().filter_map(convert::identity).collect();

        let mut lkey = Bound::Unbounded;
        let mut scans: Vec<IndexIter<K, V>> = vec![];
        for hkey in keys {
            let mut ss = scans::SkipScan::new(self.to_reader()?);
            ss.set_key_range((lkey, Bound::Excluded(hkey.clone())))
                .set_seqno_range(within.clone());
            lkey = Bound::Included(hkey);
            scans.push(Box::new(ss));
        }
        let mut ss = scans::SkipScan::new(self.to_reader()?);
        ss.set_key_range((lkey, Bound::Unbounded))
            .set_seqno_range(within.clone());
        scans.push(Box::new(ss));
        Ok(scans)
    }

    fn range_scans<N, G>(&mut self, ranges: Vec<N>, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        N: RangeBounds<K>,
        G: Clone + RangeBounds<u64>,
    {
        let mut scans: Vec<IndexIter<K, V>> = vec![];
        for range in ranges {
            let mut ss = scans::SkipScan::new(self.to_reader()?);
            ss.set_key_range(range).set_seqno_range(within.clone());
            scans.push(Box::new(ss));
        }
        Ok(scans)
    }
}

impl<K, V> PiecewiseScan<K, V> for Mvcc<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Return an iterator over entries that meet following properties
    /// * Only entries greater than range.start_bound().
    /// * Only entries whose modified seqno is within seqno-range.
    fn pw_scan<G>(&mut self, from: Bound<K>, within: G) -> Result<ScanIter<K, V>>
    where
        G: Clone + RangeBounds<u64>,
    {
        // validate arguments.
        let (start, end) = util::to_start_end(within);
        // similar to range pre-processing
        let mut iter = Box::new(IterPWScan {
            _latch: Default::default(),
            _arc: OuterSnapshot::clone(&self.snapshot),
            start,
            end,
            paths: Default::default(),
        });
        let root = iter
            ._arc
            .as_ref()
            .root_duplicate()
            .map(|n| Box::leak(n) as &Node<K, V>);
        iter.paths = match from {
            Bound::Unbounded => Some(build_iter(IFlag::Left, root, vec![])),
            Bound::Included(low) => {
                let paths = Some(find_start(root, low.borrow(), true, vec![]));
                paths
            }
            Bound::Excluded(low) => {
                let paths = Some(find_start(root, low.borrow(), false, vec![]));
                paths
            }
        };
        Ok(iter)
    }
}

impl<K, V> Validate<Stats> for Box<Mvcc<K, V>>
where
    K: Clone + Ord + Debug,
    V: Clone + Diff,
{
    fn validate(&mut self) -> Result<Stats> {
        self.as_mut().validate()
    }
}

/// Deep walk validate of Mvcc index. Note that in addition to normal
/// contraints to type parameter `K`, K-type shall also implement
/// `Debug` trait.
impl<K, V> Validate<Stats> for Mvcc<K, V>
where
    K: Clone + Ord + Debug,
    V: Clone + Diff,
{
    /// Validate LLRB tree with following rules:
    ///
    /// * Root node is always black in color.
    /// * Make sure that the maximum depth do not exceed 100.
    ///
    /// Additionally return full statistics on the tree. Refer to [`Stats`]
    /// for more information.
    fn validate(&mut self) -> Result<Stats> {
        let arc_mvcc = OuterSnapshot::clone(&self.snapshot);

        let root = arc_mvcc.as_root();
        let (red, depth) = (is_red(root), 0);
        let mut depths: LlrbDepth = Default::default();

        if red {
            let msg = format!("Mvcc Root node must be black: {}", self.name);
            return Err(Error::ValidationFail(msg));
        }

        let ss = (0, 0); // (blacks, n_deleted);
        let ss = validate_tree(root, red, ss, depth, &mut depths)?;
        if ss.1 != self.n_deleted {
            let msg = format!("Mvcc n_deleted {} != {}", ss.1, self.n_deleted);
            return Err(Error::ValidationFail(msg));
        }

        if depths.to_max() > MAX_TREE_DEPTH {
            let msg = format!("Mvcc tree exceeds max_depth {}", depths.to_max());
            return Err(Error::ValidationFail(msg));
        }

        let mut stats = Stats::new(&self.name);
        stats.entries = self.len();
        stats.key_footprint = self.key_footprint;
        stats.tree_footprint = self.tree_footprint;
        stats.n_deleted = self.n_deleted;
        stats.n_reclaimed = self.n_reclaimed;
        stats.node_size = mem::size_of::<Node<K, V>>();
        stats.rw_latch = self.latch.to_stats()?;
        stats.snapshot_latch = self.snapshot.ulatch.to_stats()?;
        stats.blacks = Some(ss.0);
        stats.depths = Some(depths);
        Ok(stats)
    }
}

impl<K, V> Mvcc<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    ////--------- rotation routines for 2-3 algorithm ----------------

    fn walkuprot_23(
        &self,
        mut node: Box<Node<K, V>>,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> Box<Node<K, V>> {
        let (left, right) = (node.as_left_deref(), node.as_right_deref());
        if is_red(right) && !is_red(left) {
            node = self.rotate_left(node, reclaim);
        }
        let left = node.as_left_deref();
        if is_red(left) && is_red(left.unwrap().as_left_deref()) {
            node = self.rotate_right(node, reclaim);
        }
        let (left, right) = (node.as_left_deref(), node.as_right_deref());
        if is_red(left) && is_red(right) {
            self.flip(node.deref_mut(), reclaim)
        }
        node
    }

    //              (i)                       (i)
    //               |                         |
    //              node                     right
    //              /  \                      / \
    //             /    (r)                 (r)  \
    //            /       \                 /     \
    //          left     right           node     r-r
    //                    / \            /  \
    //                 r-l  r-r       left  r-l
    //
    fn rotate_left(
        &self,
        mut node: Box<Node<K, V>>,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> Box<Node<K, V>> {
        let old_right = node.right.take().unwrap();
        if is_black(Some(old_right.as_ref())) {
            panic!("rotateleft(): rotating a black link ? call the programmer");
        }

        let mut right = if old_right.dirty {
            old_right
        } else {
            self.node_mvcc_clone(Box::leak(old_right), reclaim, true)
        };

        node.right = right.left.take();
        right.black = node.black;
        node.set_red();
        right.left = Some(node);

        right
    }

    //              (i)                       (i)
    //               |                         |
    //              node                      left
    //              /  \                      / \
    //            (r)   \                   (r)  \
    //           /       \                 /      \
    //         left     right            l-l      node
    //         / \                                / \
    //      l-l  l-r                            l-r  right
    //
    fn rotate_right(
        &self,
        mut node: Box<Node<K, V>>,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> Box<Node<K, V>> {
        let old_left = node.left.take().unwrap();
        if is_black(Some(old_left.as_ref())) {
            panic!("rotateright(): rotating a black link ? call the programmer")
        }

        let mut left = if old_left.dirty {
            old_left
        } else {
            self.node_mvcc_clone(Box::leak(old_left), reclaim, true)
        };

        node.left = left.right.take();
        left.black = node.black;
        node.set_red();
        left.right = Some(node);

        left
    }

    //        (x)                   (!x)
    //         |                     |
    //        node                  node
    //        / \                   / \
    //      (y) (z)              (!y) (!z)
    //     /      \              /      \
    //   left    right         left    right
    //
    fn flip(&self, node: &mut Node<K, V>, reclaim: &mut Vec<Box<Node<K, V>>>) {
        let old_left = node.left.take().unwrap();
        let old_right = node.right.take().unwrap();

        let mut left = if old_left.dirty {
            old_left
        } else {
            self.node_mvcc_clone(Box::leak(old_left), reclaim, true)
        };
        let mut right = if old_right.dirty {
            old_right
        } else {
            self.node_mvcc_clone(Box::leak(old_right), reclaim, true)
        };

        left.toggle_link();
        right.toggle_link();
        node.toggle_link();

        node.left = Some(left);
        node.right = Some(right);
    }

    fn fixup(
        &self,
        mut node: Box<Node<K, V>>,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> Box<Node<K, V>> {
        if is_red(node.as_right_deref()) {
            node = self.rotate_left(node, reclaim)
        }
        let left = node.as_left_deref();
        if is_red(left) && is_red(left.unwrap().as_left_deref()) {
            node = self.rotate_right(node, reclaim)
        }
        if is_red(node.as_left_deref()) && is_red(node.as_right_deref()) {
            self.flip(node.deref_mut(), reclaim);
        }
        node
    }

    fn move_red_left(
        &self,
        mut node: Box<Node<K, V>>,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> Box<Node<K, V>> {
        self.flip(node.deref_mut(), reclaim);
        if is_red(node.right.as_ref().unwrap().as_left_deref()) {
            let right = node.right.take().unwrap();
            node.right = Some(self.rotate_right(right, reclaim));
            node = self.rotate_left(node, reclaim);
            self.flip(node.deref_mut(), reclaim);
        }
        node
    }

    fn move_red_right(
        &self,
        mut node: Box<Node<K, V>>,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> Box<Node<K, V>> {
        self.flip(node.deref_mut(), reclaim);
        if is_red(node.left.as_ref().unwrap().as_left_deref()) {
            node = self.rotate_right(node, reclaim);
            self.flip(node.deref_mut(), reclaim);
        }
        node
    }
}

struct OuterSnapshot<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    ulatch: RWSpinlock,
    inner: AtomicPtr<Arc<Snapshot<K, V>>>,
    n_nodes: Arc<AtomicIsize>,
    n_active: Arc<AtomicUsize>,
}

impl<K, V> AsRef<Arc<Snapshot<K, V>>> for OuterSnapshot<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn as_ref(&self) -> &Arc<Snapshot<K, V>> {
        unsafe { self.inner.load(SeqCst).as_ref().unwrap() }
    }
}

impl<K, V> OuterSnapshot<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    // create the first snapshot and a placeholder `next` snapshot for Mvcc.
    fn new() -> OuterSnapshot<K, V> {
        let n_active = Arc::new(AtomicUsize::new(1));
        let n = Arc::clone(&n_active);
        let n_nodes = Arc::new(AtomicIsize::new(0));

        let curr_snapshot: Box<Snapshot<K, V>> = // current snapshot
            Snapshot::new(None, Arc::clone(&n_nodes), n);

        let arc: Box<Arc<Snapshot<K, V>>> = Box::new(Arc::new(*curr_snapshot));
        OuterSnapshot {
            ulatch: RWSpinlock::new(),
            inner: AtomicPtr::new(Box::leak(arc)),
            n_nodes,
            n_active,
        }
    }

    // similar to Arc::clone for AtomicPtr<Arc<Snapshot<K,V>>>
    fn clone(this: &OuterSnapshot<K, V>) -> Arc<Snapshot<K, V>> {
        let _r = this.ulatch.acquire_read(true /*spin*/);
        let inner_snap: &Arc<Snapshot<K,V>> =  // from heap
            unsafe { this.inner.load(SeqCst).as_ref().unwrap() };
        Arc::clone(inner_snap)
    }

    fn shift_snapshot(
        &self,
        root: Option<Box<Node<K, V>>>,
        seqno: u64,
        n_count: usize,
        reclaim: Vec<Box<Node<K, V>>>,
    ) {
        // :/ sometimes when a reader holds a snapshot for a long time
        // it can lead to very long chain of snapshot due to incoming
        // mutations. And when the "long-reader" releases the snapshot
        // the entire chain of snapshots could be dropped by recusively,
        // leading to stackoverflow :\.

        let curr_s_1: Box<Arc<Snapshot<K,V>>> = // current snapshot, drop later
            unsafe { Box::from_raw(self.inner.load(SeqCst)) };

        loop {
            let curr_m: &mut Snapshot<K, V> = unsafe {
                let ptr = curr_s_1.as_ref().as_ref() as *const Snapshot<K, V>;
                (ptr as *mut Snapshot<K, V>).as_mut().unwrap()
            };
            curr_m.next = match curr_m.next.take() {
                None => None,
                Some(next) => Self::try_free_snapshot(next),
            };

            if self.n_active.load(SeqCst) < 1000 {
                // TODO: no magic number
                break;
            } else {
                thread::yield_now();
            }
        }

        let curr_s_2: Option<Arc<Snapshot<K,V>>> = // another copy
            Some(Arc::clone(curr_s_1.as_ref()));
        let m = Arc::clone(&self.n_active);
        let mut next_s: Box<Snapshot<K, V>> = // new snapshot
            Snapshot::new(curr_s_2, Arc::clone(&self.n_nodes), m);

        // populate the next snapshot.
        next_s.root = root;
        next_s.reclaim = Some(reclaim);
        next_s.seqno = seqno;
        next_s.n_count = n_count;

        let next_s: Box<Arc<Snapshot<K, V>>> = Box::new(Arc::new(*next_s));

        // let x = Arc::strong_count(curr_s_1.as_ref());
        // let y = Arc::strong_count(next_s.as_ref());
        //println!(
        //    "shiftsnap {:p} {:p} {} {} ",
        //    curr_r,
        //    next_s.as_ref().as_ref(),
        //    x,
        //    y
        //);
        self.n_active.fetch_add(1, SeqCst);

        let _w = self.ulatch.acquire_write(true /*spin*/);
        self.inner.store(Box::leak(next_s), SeqCst);
    }

    fn try_free_snapshot(
        mut snap: Arc<Snapshot<K, V>>, // current.next.take().unwrap()
    ) -> Option<Arc<Snapshot<K, V>>> {
        match Arc::get_mut(&mut snap) {
            None => Some(snap),
            Some(snap_m) => match snap_m.next.take() {
                None => None,
                Some(next) => match Self::try_free_snapshot(next) {
                    None => None,
                    Some(next) => {
                        snap_m.next = Some(next);
                        Some(snap)
                    }
                },
            },
        }
    }
}

pub(crate) struct Snapshot<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    root: Option<Box<Node<K, V>>>,
    reclaim: Option<Vec<Box<Node<K, V>>>>,
    seqno: u64,     // starts from 0 and incr for every mutation.
    n_count: usize, // number of entries in the tree.
    n_nodes: Arc<AtomicIsize>,
    n_active: Arc<AtomicUsize>,
    next: Option<Arc<Snapshot<K, V>>>,
}

impl<K, V> Snapshot<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    // shall be called twice while creating the Mvcc index and once
    // for every new snapshot that gets created and shifted into the chain.
    fn new(
        next: Option<Arc<Snapshot<K, V>>>,
        n_nodes: Arc<AtomicIsize>,
        n_active: Arc<AtomicUsize>,
    ) -> Box<Snapshot<K, V>> {
        // println!("new mvcc-root {:p}", snapshot);
        Box::new(Snapshot {
            root: Default::default(),
            reclaim: Default::default(),
            seqno: Default::default(),
            n_count: Default::default(),
            next,
            n_nodes,
            n_active,
        })
    }

    fn root_duplicate(&self) -> Option<Box<Node<K, V>>> {
        match &self.root {
            None => None,
            Some(node) => {
                let node = node.deref() as *const Node<K, V> as *mut Node<K, V>;
                Some(unsafe { Box::from_raw(node) })
            }
        }
    }

    fn as_root(&self) -> Option<&Node<K, V>> {
        self.root.as_ref().map(Deref::deref)
    }
}

impl<K, V> Drop for Snapshot<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn drop(&mut self) {
        // NOTE: `root` will be leaked, so that the tree is intact.

        self.root.take().map(Box::leak); // Leak root

        // NOTE: `reclaim` nodes will be dropped.
        // Note that child nodes won't be dropped.
        match self.reclaim.take() {
            Some(reclaim) => {
                self.n_nodes
                    .as_ref()
                    .fetch_sub(reclaim.len().try_into().unwrap(), SeqCst);
            }
            _ => (),
        };
        let _n = self.n_active.fetch_sub(1, SeqCst);

        // IMPORTANT NOTE: if free is slower than allow, and whether there
        // is heavy background mutation, we might end up with stackoverflow
        // due to recursive drops of snapshot chain, convert that recursion
        // into a loop.
        let mut child = self.next.take();
        while let Some(snap) = child.take() {
            match Arc::try_unwrap(snap) {
                Ok(mut snap) => {
                    // we are the only reference to this snapshot, and drop
                    // is going to be called at exit, convert the recursive
                    // drop to loop.
                    child = snap.next.take();
                    mem::drop(snap)
                }
                Err(_snap) => break, // decrement the reference
            }
        }

        //if n > 10 {
        //    println!("active snapshots {}", _n);
        //}
        //println!("drop snapshot {:p}", self);
    }
}

impl<K, V> Default for Snapshot<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn default() -> Snapshot<K, V> {
        Snapshot {
            root: Default::default(),
            reclaim: Default::default(),
            seqno: Default::default(),
            n_count: Default::default(),
            next: Default::default(),
            n_nodes: Default::default(),
            n_active: Default::default(),
        }
    }
}

/// Read handle into [Mvcc] index.
pub struct MvccReader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    _refn: Arc<u32>,
    id: usize,
    index: Option<Box<ffi::c_void>>, // Box<Mvcc<K, V>>
    phantom_key: marker::PhantomData<K>,
    phantom_val: marker::PhantomData<V>,
}

impl<K, V> MvccReader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn new(index: Box<ffi::c_void>, _refn: Arc<u32>) -> MvccReader<K, V> {
        let id = Arc::strong_count(&_refn);
        let mut r = MvccReader {
            _refn,
            id,
            index: Some(index),
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        };

        let index: &mut Mvcc<K, V> = r.as_mut();
        debug!(target: "mvcc  ", "{:?}, new reader {}", index.name, id);
        r
    }
}

impl<K, V> Drop for MvccReader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn drop(&mut self) {
        let id = self.id;
        let index: &mut Mvcc<K, V> = self.as_mut();
        debug!(target: "mvcc  ", "{:?}, dropping reader {}", index.name, id);

        // leak this index, it is only a reference
        Box::leak(self.index.take().unwrap());
    }
}

impl<K, V> AsMut<Mvcc<K, V>> for MvccReader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn as_mut(&mut self) -> &mut Mvcc<K, V> {
        unsafe {
            // transmute void pointer to mutable reference into index.
            let index_ptr = self.index.as_mut().unwrap().as_mut();
            let index_ptr = index_ptr as *mut ffi::c_void;
            (index_ptr as *mut Mvcc<K, V>).as_mut().unwrap()
        }
    }
}

impl<K, V> Reader<K, V> for MvccReader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Get ``key`` from index.
    fn get<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + Hash,
    {
        let index: &mut Mvcc<K, V> = self.as_mut();
        index.get(key)
    }

    /// Iterate over all entries in this index.
    fn iter(&mut self) -> Result<IndexIter<K, V>> {
        let index: &mut Mvcc<K, V> = self.as_mut();
        index.iter()
    }

    /// Iterate from lower bound to upper bound.
    fn range<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let index: &mut Mvcc<K, V> = self.as_mut();
        index.range(range)
    }

    /// Iterate from upper bound to lower bound.
    fn reverse<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let index: &mut Mvcc<K, V> = self.as_mut();
        index.reverse(range)
    }

    /// Short circuited to get().
    fn get_with_versions<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + Hash,
    {
        self.get(key)
    }

    /// Short circuited to iter().
    fn iter_with_versions(&mut self) -> Result<IndexIter<K, V>> {
        self.iter()
    }

    /// Short circuited to range().
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

    /// Short circuited to reverse()
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

impl<K, V> PiecewiseScan<K, V> for MvccReader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Return an iterator over entries that meet following properties
    /// * Only entries greater than range.start_bound().
    /// * Only entries whose modified seqno is within seqno-range.
    fn pw_scan<G>(&mut self, from: Bound<K>, within: G) -> Result<ScanIter<K, V>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let index: &mut Mvcc<K, V> = self.as_mut();
        index.pw_scan(from, within)
    }
}

/// Write handle into [Mvcc] index.
pub struct MvccWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    _refn: Arc<u32>,
    id: usize,
    index: Option<Box<ffi::c_void>>,
    phantom_key: marker::PhantomData<K>,
    phantom_val: marker::PhantomData<V>,
}

impl<K, V> MvccWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn new(index: Box<ffi::c_void>, _refn: Arc<u32>) -> MvccWriter<K, V> {
        let id = Arc::strong_count(&_refn);
        let mut w = MvccWriter {
            _refn,
            id,
            index: Some(index),
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        };

        let index: &mut Mvcc<K, V> = w.as_mut();
        debug!(target: "mvcc  ", "{:?}, new writer {}", index.name, id);
        w
    }
}

impl<K, V> Drop for MvccWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn drop(&mut self) {
        let id = self.id;
        let index: &mut Mvcc<K, V> = self.as_mut();
        debug!(target: "mvcc  ", "{:?}, dropping writer {}", index.name, id);

        // leak this index, it is only a reference
        Box::leak(self.index.take().unwrap());
    }
}

impl<K, V> AsMut<Mvcc<K, V>> for MvccWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn as_mut(&mut self) -> &mut Mvcc<K, V> {
        unsafe {
            // transmute void pointer to mutable reference into index.
            let index_ptr = self.index.as_mut().unwrap().as_mut();
            let index_ptr = index_ptr as *mut ffi::c_void;
            (index_ptr as *mut Mvcc<K, V>).as_mut().unwrap()
        }
    }
}

impl<K, V> MvccWriter<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    /// Refer Llrb::set_index() for more details.
    pub fn set_index(
        &mut self,
        key: K,
        value: V,
        seqno: Option<u64>,
    ) -> Result<(u64, Option<Entry<K, V>>)> {
        let index: &mut Mvcc<K, V> = self.as_mut();
        index.set_index(key, value, seqno)
    }

    /// Refer Llrb::set_index() for more details.
    pub fn set_cas_index(
        &mut self,
        key: K,
        value: V,
        cas: u64,
        seqno: Option<u64>,
    ) -> Result<(u64, Result<Option<Entry<K, V>>>)> {
        let index: &mut Mvcc<K, V> = self.as_mut();
        index.set_cas_index(key, value, cas, seqno)
    }

    /// Refer Llrb::set_index() for more details.
    pub fn delete_index<Q>(
        &mut self,
        key: &Q,
        seqno: Option<u64>,
    ) -> Result<(u64, Result<Option<Entry<K, V>>>)>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        let index: &mut Mvcc<K, V> = self.as_mut();
        index.delete_index(key, seqno)
    }
}

impl<K, V> Writer<K, V> for MvccWriter<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    /// Set {key, value} pair into index. If key is already
    /// present, update the value and return the previous entry, else
    /// create a new entry.
    ///
    /// *LSM mode*: Add a new version for the key, perserving the old value.
    fn set(&mut self, key: K, value: V) -> Result<Option<Entry<K, V>>> {
        let index: &mut Mvcc<K, V> = self.as_mut();
        let (_seqno, old_entry) = index.set_index(key, value, None)?;
        Ok(old_entry)
    }

    /// Similar to set, but succeeds only when CAS matches with entry's
    /// last `seqno`. In other words, since seqno is unique to each mutation,
    /// we use `seqno` of the mutation as the CAS value. Use CAS == 0 to
    /// enforce a create operation.
    ///
    /// *LSM mode*: Add a new version for the key, perserving the old value.
    fn set_cas(&mut self, key: K, value: V, cas: u64) -> Result<Option<Entry<K, V>>> {
        let index: &mut Mvcc<K, V> = self.as_mut();
        let (_seqno, entry) = index.set_cas_index(key, value, cas, None)?;
        entry
    }

    /// Delete the given key. Note that back-to-back delete for the same
    /// key shall collapse into a single delete, first delete is ingested
    /// while the rest are ignored.
    ///
    /// *LSM mode*: Mark the entry as deleted along with seqno at which it
    /// deleted
    ///
    /// NOTE: K should be borrowable as &Q and Q must be convertable to
    /// owned K. This is require in lsm mode, where owned K must be
    /// inserted into the tree.
    fn delete<Q>(&mut self, key: &Q) -> Result<Option<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        let index: &mut Mvcc<K, V> = self.as_mut();
        let (_seqno, entry) = index.delete_index(key, None)?;
        entry
    }
}

impl<K, V> WalWriter<K, V> for MvccWriter<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn set_index(
        &mut self,
        key: K,
        value: V,
        seqno: u64, // seqno for this mutation
    ) -> Result<Option<Entry<K, V>>> {
        let index: &mut Mvcc<K, V> = self.as_mut();
        let (_seqno, old_entry) = index.set_index(key, value, Some(seqno))?;
        Ok(old_entry)
    }

    fn set_cas_index(
        &mut self,
        key: K,
        value: V,
        cas: u64,
        seqno: u64,
    ) -> Result<Option<Entry<K, V>>> {
        let index: &mut Mvcc<K, V> = self.as_mut();
        let (_seqno, res) = index.set_cas_index(key, value, cas, Some(seqno))?;
        res
    }

    fn delete_index<Q>(
        &mut self,
        key: &Q,
        seqno: u64, // seqno for this delete
    ) -> Result<Option<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        let index: &mut Mvcc<K, V> = self.as_mut();
        let (_seqno, res) = index.delete_index(key, Some(seqno))?;
        res
    }
}

/// Statistics for [`Mvcc`] tree.
pub struct Stats {
    pub name: String,
    pub entries: usize,
    pub n_deleted: usize,
    pub n_reclaimed: usize,
    pub node_size: usize,
    pub key_footprint: isize,
    pub tree_footprint: isize,
    pub rw_latch: spinlock::Stats,
    pub snapshot_latch: spinlock::Stats,
    pub blacks: Option<usize>,
    pub depths: Option<LlrbDepth>,
}

impl Stats {
    pub(crate) fn new(name: &str) -> Stats {
        Stats {
            name: name.to_string(),
            entries: Default::default(),
            n_deleted: Default::default(),
            n_reclaimed: Default::default(),
            node_size: Default::default(),
            key_footprint: Default::default(),
            tree_footprint: Default::default(),
            rw_latch: Default::default(),
            snapshot_latch: Default::default(),
            blacks: None,
            depths: None,
        }
    }
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        let none = "none".to_string();
        let b = self.blacks.as_ref().map_or(none.clone(), |x| x.to_string());
        let d = self.depths.as_ref().map_or(none.clone(), |x| x.to_string());
        write!(f, "mvcc.name = {}\n", self.name)?;
        write!(
            f,
            "mvcc = {{ entries={}, n_deleted={} node_size={}, blacks={} }}\n",
            self.entries, self.n_deleted, self.node_size, b,
        )?;
        write!(
            f,
            "mvcc = {{ n_reclaimed={}, key_footprint={}, tree_footprint={} }}\n",
            self.n_reclaimed, self.key_footprint, self.tree_footprint,
        )?;
        write!(f, "mvcc.rw_latch = {}\n", self.rw_latch)?;
        write!(f, "mvcc.snap_latch = {}\n", self.snapshot_latch)?;
        write!(f, "mvcc.depths = {}\n", d)
    }
}

impl ToJson for Stats {
    fn to_json(&self) -> String {
        let null = "null".to_string();
        let rw_l = self.rw_latch.to_json();
        let snap_l = self.snapshot_latch.to_json();
        format!(
            concat!(
                r#"{{ ""mvcc": {{ "name": {}, "entries": {:X}, "#,
                r#""n_deleted": {}, "n_reclaimed": {}, "#,
                r#""key_footprint": {}, "tree_footprint": {}, "#,
                r#""node_size": {}, "rw_latch": {}, "#,
                r#""snap_latch": {}, "blacks": {}, "depths": {} }} }}"#,
            ),
            self.name,
            self.entries,
            self.n_deleted,
            self.n_reclaimed,
            self.key_footprint,
            self.tree_footprint,
            self.node_size,
            rw_l,
            snap_l,
            self.blacks
                .as_ref()
                .map_or(null.clone(), |x| format!("{}", x)),
            self.depths.as_ref().map_or(null.clone(), |x| x.to_json()),
        )
    }
}

// drop_tree variant for mvcc.
// by default dropping a node does not drop its children.
fn drop_tree<K, V>(mut node: Box<Node<K, V>>) -> usize
where
    K: Ord + Clone,
    V: Clone + Diff,
{
    // println!("drop_tree - node {:p}", node);

    // left child shall be dropped after drop_tree() returns.
    let mut n = match node.left.take() {
        Some(left) => drop_tree(left),
        None => 0,
    };
    // right child shall be dropped after drop_tree() returns.
    n += match node.right.take() {
        Some(right) => drop_tree(right),
        None => 0,
    };
    n + 1
}

#[allow(dead_code)]
fn print_reclaim<K, V>(prefix: &str, reclaim: &Vec<Box<Node<K, V>>>)
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    print!("{}reclaim ", prefix);
    reclaim.iter().for_each(|item| print!("{:p} ", *item));
    println!("");
}

#[cfg(test)]
#[path = "mvcc_test.rs"]
mod mvcc_test;
