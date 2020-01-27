//! Module ``llrb`` implement an in-memory index type, using
//! _Left Leaning Red Black_ tree.
//!
//! [Llrb] type allow concurrent read and write access at API level,
//! while behind the scenes, all CRUD access are serialized into single
//! threaded operation. To serialize concurrent access [Llrb] uses
//! a spin-lock implementation that can be configured to _yield_ or
//! _spin_ while waiting for the lock.
//!
//! **[LSM mode]**: Llrb index can support log-structured-merge while
//! mutating the tree. In simple terms, this means that nothing shall be
//! over-written in the tree and all the mutations for the same key shall
//! be preserved until they are purged.
//!
//! **Possible ways to configure Llrb**:
//!
//! *spinlatch*, relevant only in multi-threaded context. Calling
//! _set_spinlatch()_ with _true_ will have the calling thread to spin
//! while waiting to acquire the lock. Calling it with _false_ will have the
//! calling thread to yield to OS scheduler while waiting to acquire the lock.
//!
//! *sticky*, is a shallow variant of lsm, applicable only when
//! `lsm` option is disabled. For more information refer to Llrb::set_sticky()
//! method.
//!
//! *seqno*, application can set the beginning sequence number before
//! ingesting data into the index.
//!
//! [llrb]: https://en.wikipedia.org/wiki/Left-leaning_red-black_tree
//! [LSM mode]: https://en.wikipedia.org/wiki/Log-structured_merge-tree
//!

use log::{debug, error, info, warn};

use std::{
    borrow::Borrow,
    cmp::{self, Ord, Ordering},
    convert::{self, TryInto},
    ffi, fmt,
    hash::Hash,
    marker, mem,
    ops::{Bound, Deref, DerefMut, RangeBounds},
    result,
    sync::Arc,
};

use crate::{
    core::{CommitIter, Result, ScanEntry, ScanIter, Value, WalWriter, WriteIndexFactory},
    core::{CommitIterator, ToJson, Validate, Writer},
    core::{Diff, Entry, Footprint, Index, IndexIter, PiecewiseScan, Reader},
    error::Error,
    mvcc::{Mvcc, Snapshot},
    scans,
    spinlock::{self, RWSpinlock},
    types::Empty,
    util,
};
// re-export
pub use crate::llrb_node::Node;

include!("llrb_common.rs");

pub use crate::llrb_node::LlrbDepth;

/// LlrbFactory captures a set of configuration for creating new Llrb
/// instances. By implementing `WriteIndexFactory` trait this can be
/// used with other, more sophisticated, index implementations.
pub struct LlrbFactory {
    lsm: bool,
    sticky: bool,
    spin: bool,
}

/// Create a new factory with initial set of configuration. To know
/// more about other configurations supported by the LlrbFactory refer
/// to its ``set_``, methods.
///
/// * *lsm*, spawn Llrb instances in lsm mode, this will preserve the
///   entire history of all write operations applied on the index.
pub fn llrb_factory(lsm: bool) -> LlrbFactory {
    LlrbFactory {
        lsm,
        sticky: false,
        spin: true,
    }
}

/// Configuration methods.
impl LlrbFactory {
    /// If lsm is _true_, this will preserve the entire history of all write
    /// operations applied on the index. _Default: false_.
    pub fn set_lsm(&mut self, lsm: bool) -> &mut Self {
        self.lsm = lsm;
        self
    }

    /// If spin is _true_, calling thread will spin while waiting for the
    /// latch, otherwise, calling thead will be yielded to OS scheduler.
    /// _Default: false_.
    pub fn set_spinlatch(&mut self, spin: bool) -> &mut Self {
        self.spin = spin;
        self
    }

    /// Create all Llrb instances in sticky mode, refer to Llrb::set_sticky()
    /// for more details.
    /// _Default: false_.
    pub fn set_sticky(&mut self, spin: bool) -> &mut Self {
        self.spin = spin;
        self
    }

    fn to_config_string(&self) -> String {
        format!(
            "llrb = {{ lsm = {}, sticky = {}, spin = {} }}",
            self.lsm, self.sticky, self.spin
        )
    }
}

impl<K, V> WriteIndexFactory<K, V> for LlrbFactory
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    type I = Box<Llrb<K, V>>;

    fn to_type(&self) -> String {
        "llrb".to_string()
    }

    fn new(&self, name: &str) -> Result<Self::I> {
        info!(
            target: "llrbfc",
            "{:?}, new llrb instance, with config {}", name, self.to_config_string()
        );

        let mut index = if self.lsm {
            Llrb::new_lsm(name)
        } else {
            Llrb::new(name)
        };
        index.set_sticky(self.sticky).set_spinlatch(self.spin);
        Ok(index)
    }
}

/// Single threaded, in-memory index using [left-leaning-red-black][llrb] tree.
///
/// [llrb]: https://en.wikipedia.org/wiki/Left-leaning_red-black_tree
pub struct Llrb<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    name: String,
    lsm: bool,
    sticky: bool,
    spin: bool,

    root: Option<Box<Node<K, V>>>,
    seqno: u64,
    n_count: usize,   // number entries index.
    n_deleted: usize, // number of entries marked deleted.
    latch: RWSpinlock,
    key_footprint: isize,
    tree_footprint: isize,
    readers: Arc<u32>,
    writers: Arc<u32>,
}

impl<K, V> Drop for Llrb<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn drop(&mut self) {
        loop {
            let n = self.multi_rw();
            if n == 0 {
                break;
            }
            error!(
                target: "llrb  ",
                "{:?}, dropped before read/write handles {}", self.name, n
            );
        }

        debug!(target: "llrb  ", "{:?}, dropped ...", self.name);
        self.root.take().map(drop_tree);
    }
}

// by default dropping a node does not drop its children.
fn drop_tree<K, V>(mut node: Box<Node<K, V>>)
where
    K: Ord + Clone,
    V: Clone + Diff,
{
    // println!("drop_tree - node {:p}", node);

    // left child shall be dropped after drop_tree() returns.
    node.left.take().map(|left| drop_tree(left));
    // right child shall be dropped after drop_tree() returns.
    node.right.take().map(|right| drop_tree(right));
}

impl<K, V> From<Mvcc<K, V>> for Box<Llrb<K, V>>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn from(mvcc_index: Mvcc<K, V>) -> Box<Llrb<K, V>> {
        let mut index = if mvcc_index.is_lsm() {
            Llrb::new_lsm(mvcc_index.to_name())
        } else {
            Llrb::new(mvcc_index.to_name())
        };
        index
            .set_sticky(mvcc_index.is_sticky())
            .set_spinlatch(mvcc_index.is_spin());

        let debris = mvcc_index.squash();
        index.root = debris.root;
        index.seqno = debris.seqno;
        index.n_count = debris.n_count;
        index.n_deleted = debris.n_deleted;
        index.key_footprint = debris.key_footprint;
        index.tree_footprint = debris.tree_footprint;

        index
    }
}

/// Different ways to construct a new Llrb index.
impl<K, V> Llrb<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Create an empty Llrb index, identified by `name`.
    /// Applications can choose unique names.
    pub fn new<S: AsRef<str>>(name: S) -> Box<Llrb<K, V>> {
        Box::new(Llrb {
            name: name.as_ref().to_string(),
            lsm: false,
            sticky: false,
            spin: true,

            root: None,
            seqno: Default::default(),
            n_count: Default::default(),
            n_deleted: Default::default(),
            latch: RWSpinlock::new(),
            key_footprint: Default::default(),
            tree_footprint: Default::default(),
            readers: Arc::new(0xC0FFEE),
            writers: Arc::new(0xC0FFEE),
        })
    }

    /// Create a new Llrb index in lsm mode. In lsm mode, mutations
    /// are added as log for each key, instead of over-writing previous
    /// mutation. Note that, in case of back-to-back delete, first delete
    /// shall be applied and subsequent deletes shall be ignored.
    pub fn new_lsm<S>(name: S) -> Box<Llrb<K, V>>
    where
        S: AsRef<str>,
    {
        Box::new(Llrb {
            name: name.as_ref().to_string(),
            lsm: true,
            sticky: false,
            spin: true,

            root: None,
            seqno: Default::default(),
            n_count: Default::default(),
            n_deleted: Default::default(),
            latch: RWSpinlock::new(),
            key_footprint: Default::default(),
            tree_footprint: Default::default(),
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
            panic!("cannot configure Llrb with active readers/writers {}", n)
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
            panic!("cannot configure Llrb with active readers/writers {}", n)
        }
        self.sticky = sticky;
        self
    }

    /// Squash this index and return the root and its book-keeping.
    pub(crate) fn squash(mut self) -> SquashDebris<K, V> {
        let n = self.multi_rw();
        if n > 0 {
            panic!("cannot squash Llrb with active readers/writer {}", n);
        }
        SquashDebris {
            root: self.root.take(),
            seqno: self.seqno,
            n_count: self.n_count,
            n_deleted: self.n_deleted,
            key_footprint: self.key_footprint,
            tree_footprint: self.tree_footprint,
        }
    }

    pub fn clone(&self) -> Box<Llrb<K, V>> {
        Box::new(Llrb {
            name: self.name.clone(),
            lsm: self.lsm,
            sticky: self.sticky,
            spin: self.spin,

            root: self.root.clone(),
            seqno: self.seqno,
            n_count: self.n_count,
            n_deleted: self.n_deleted,
            latch: RWSpinlock::new(),
            key_footprint: self.key_footprint,
            tree_footprint: self.tree_footprint,
            readers: Arc::new(0xC0FFEE),
            writers: Arc::new(0xC0FFEE),
        })
    }
}

impl<K, V> Llrb<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    pub fn split(
        mut self,
        name1: String,
        name2: String,
    ) -> Result<(Box<Llrb<K, V>>, Box<Llrb<K, V>>)> {
        let n = self.multi_rw();
        if n > 0 {
            panic!("cannot call llrb.split() with active readers/writers");
        }

        let (mut one, mut two) = if self.lsm {
            (Llrb::new_lsm(&name1), Llrb::new_lsm(&name2))
        } else {
            (Llrb::new(&name1), Llrb::new(&name2))
        };

        match &mut self.root {
            None => (),
            Some(root) => {
                one.root = Self::do_split(root.left.take(), &mut one)?;
                one.root.as_mut().map(|n| n.set_black());
                two.root = Self::do_split(root.right.take(), &mut two)?;
                two.root.as_mut().map(|n| n.set_black());
                (&*two).set_index_entry(root.entry.clone())?;
            }
        }
        one.seqno = self.seqno;
        two.seqno = self.seqno;

        // validation
        assert_eq!(cmp::max(one.seqno, two.seqno), self.seqno);
        let n_count = one.n_count + two.n_count;
        assert_eq!(n_count, self.n_count);
        let n_deleted = one.n_deleted + two.n_deleted;
        assert_eq!(n_deleted, self.n_deleted);
        let key_footprint = one.key_footprint + two.key_footprint;
        assert_eq!(key_footprint, self.key_footprint);
        let tree_footprint = one.tree_footprint + two.tree_footprint;
        assert_eq!(tree_footprint, self.tree_footprint);

        Ok((one, two))
    }

    fn do_split(
        node: Option<Box<Node<K, V>>>,
        index: &mut Llrb<K, V>,
    ) -> Result<Option<Box<Node<K, V>>>> {
        match node {
            None => Ok(None),
            Some(mut node) => {
                index.seqno = cmp::max(index.seqno, node.to_seqno());
                index.n_count += 1;
                if node.is_deleted() {
                    index.n_deleted += 1;
                }
                index.key_footprint += util::key_footprint(node.as_key())?;
                index.tree_footprint += node.footprint()?;

                node.left = Self::do_split(node.left.take(), index)?;
                node.right = Self::do_split(node.right.take(), index)?;
                Ok(Some(node))
            }
        }
    }
}

/// Maintanence API.
impl<K, V> Llrb<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
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

    pub fn is_spin(&self) -> bool {
        self.spin
    }

    /// Return number of entries in this index.
    #[inline]
    pub fn len(&self) -> usize {
        let _latch = self.latch.acquire_read(self.spin);
        self.n_count
    }

    /// Identify this index. Applications can choose unique names while
    /// creating Llrb indices.
    #[inline]
    pub fn to_name(&self) -> String {
        self.name.clone()
    }

    /// Return quickly with basic statisics, only entries() method is valid
    /// with this statisics.
    pub fn to_stats(&self) -> Result<Stats> {
        let mut stats = Stats::new(&self.name);
        stats.entries = self.len();
        stats.n_deleted = self.n_deleted;
        stats.node_size = mem::size_of::<Node<K, V>>();
        stats.key_footprint = self.key_footprint;
        stats.tree_footprint = self.tree_footprint;
        stats.rw_latch = self.latch.to_stats()?;
        Ok(stats)
    }

    /// Return the first entry in the index. Return None if index is empty.
    pub fn first(&self) -> Option<Entry<K, V>> {
        let _latch = self.latch.acquire_read(self.spin);
        let node = self.root.as_ref().map(Deref::deref);
        node.map(|mut node| loop {
            node = match node.as_left_deref() {
                Some(nref) => nref,
                None => break node.entry.clone(),
            };
        })
    }

    /// Return the last entry in the index. Return None if index is empty.
    pub fn last(&self) -> Option<Entry<K, V>> {
        let _latch = self.latch.acquire_read(self.spin);
        let node = self.root.as_ref().map(Deref::deref);
        node.map(|mut node| loop {
            node = match node.as_right_deref() {
                Some(nref) => nref,
                None => break node.entry.clone(),
            };
        })
    }

    fn multi_rw(&self) -> usize {
        Arc::strong_count(&self.readers) + Arc::strong_count(&self.writers) - 2
    }
}

impl<K, V> Index<K, V> for Box<Llrb<K, V>>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    type W = LlrbWriter<K, V>;
    type R = LlrbReader<K, V>;
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

    /// Create a new reader handle, for multi-threading.
    /// Llrb uses spin-lock to coordinate between readers and writers.
    fn to_reader(&mut self) -> Result<Self::R> {
        self.as_mut().to_reader()
    }

    /// Create a new writer handle, for multi-threading.
    /// Llrb uses spin-lock to coordinate between readers and writers.
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

impl<K, V> Index<K, V> for Llrb<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    type W = LlrbWriter<K, V>;
    type R = LlrbReader<K, V>;
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
        let _latch = self.latch.acquire_read(self.spin);
        Ok(self.seqno)
    }

    fn set_seqno(&mut self, seqno: u64) -> Result<()> {
        let n = self.multi_rw();
        if n > 0 {
            panic!("cannot configure Llrb with active readers/writers {}", n)
        }
        self.seqno = seqno;
        Ok(())
    }

    /// Create a new reader handle, for multi-threading.
    /// Llrb uses spin-lock to coordinate between readers and writers.
    fn to_reader(&mut self) -> Result<Self::R> {
        let index = unsafe {
            // transmute self as void pointer.
            Box::from_raw(self as *mut Llrb<K, V> as *mut ffi::c_void)
        };
        let reader = Arc::clone(&self.readers);
        Ok(LlrbReader::<K, V>::new(index, reader))
    }

    /// Create a new writer handle, for multi-threading.
    /// Llrb uses spin-lock to coordinate between readers and writers.
    fn to_writer(&mut self) -> Result<Self::W> {
        let index = unsafe {
            // transmute self as void pointer.
            Box::from_raw(self as *mut Llrb<K, V> as *mut ffi::c_void)
        };
        let writer = Arc::clone(&self.writers);
        Ok(LlrbWriter::<K, V>::new(index, writer))
    }

    fn commit<C, F>(&mut self, mut scanner: CommitIter<K, V, C>, _metacb: F) -> Result<()>
    where
        C: CommitIterator<K, V>,
        F: Fn(Vec<u8>) -> Vec<u8>,
    {
        warn!(
            target: "llrb  ",
            "{:?}, commit started (blocks all other index operations) ...",
            self.name
        );

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

        info!(target: "llrb  ", "{:?}, committed {} items", self.name, count);
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
                warn!(target: "llrb  ", "compact with unbounded cutoff");
            }
            Bound::Included(seqno) if seqno >= self.to_seqno()? => {
                warn!(target: "llrb  ", "compact cutsoff the entire index {}", seqno);
            }
            Bound::Excluded(seqno) if seqno > self.to_seqno()? => {
                warn!(target: "llrb  ", "compact cutsoff the entire index {}", seqno);
            }
            _ => (),
        }

        let (mut count, mut low) = (0_usize, Bound::Unbounded);
        const LIMIT: usize = 1_000; // TODO: no magic number
        let count = loop {
            let _latch = self.latch.acquire_write(self.spin);

            let root = self.root.as_mut().map(DerefMut::deref_mut);
            let mut cc = CompactCtxt {
                cutoff,
                dels: vec![],
                tree_footprint: &mut self.tree_footprint,
            };
            let (seen, limit) = Llrb::<K, V>::compact_loop(root, low, &mut cc, LIMIT)?;
            for key in cc.dels {
                self.delete_index_entry(key)?;
            }
            match (seen, limit) {
                (_, limit) if limit > 0 => break count + (LIMIT - limit),
                (Some(key), _) => low = Bound::Excluded(key),
                _ => unreachable!(),
            }
            count += LIMIT;
        };
        info!(target: "llrb  ", "{:?}, compacted {} items", self.name, count);
        Ok(count)
    }

    fn close(self) -> Result<()> {
        Ok(())
    }

    fn purge(self) -> Result<()> {
        self.close()
    }
}

impl<K, V> Footprint for Box<Llrb<K, V>>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn footprint(&self) -> Result<isize> {
        self.as_ref().footprint()
    }
}

impl<K, V> Footprint for Llrb<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn footprint(&self) -> Result<isize> {
        let _latch = self.latch.acquire_read(self.spin);
        Ok(self.tree_footprint)
    }
}

struct UpsertResult<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    node: Option<Box<Node<K, V>>>,
    old_entry: Option<Entry<K, V>>,
    size: isize, // differencen in footprint
}

struct UpsertCasResult<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    node: Option<Box<Node<K, V>>>,
    old_entry: Option<Entry<K, V>>,
    size: isize, // difference in footprint
    err: Option<Error>,
}

struct DeleteResult<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    node: Option<Box<Node<K, V>>>,
    old_entry: Option<Entry<K, V>>,
    size: isize, // difference in footprint
}

/// Create/Update/Delete operations on Llrb index.
impl<K, V> Llrb<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    /// Set {key, value} in index. Return older entry if present.
    /// Return the seqno (index) for this mutation and older entry
    /// if present. If operation was invalid or NOOP, returned seqno
    /// shall be ZERO.
    ///
    /// *LSM mode*: Add a new version for the key, perserving the old value.
    pub fn set_index(
        &mut self,
        key: K,
        value: V,
        seqno: Option<u64>,
    ) -> Result<(u64, Option<Entry<K, V>>)> {
        let _latch = self.latch.acquire_write(self.spin);
        let entry = {
            let seqno = match seqno {
                Some(seqno) => seqno,
                None => self.seqno + 1,
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

    /// Similar to set, but succeeds only when CAS matches with entry's
    /// Set {key, value} in index if an older entry exists with the
    /// same ``cas`` value. To create a fresh entry, pass ``cas`` as ZERO.
    /// Return the seqno (index) for this mutation and older entry
    /// if present. If operation was invalid or NOOP, returned seqno shall
    /// be ZERO.
    ///
    /// *LSM mode*: Add a new version for the key, perserving the old value.
    pub fn set_cas_index(
        &mut self,
        key: K,
        value: V,
        cas: u64,
        seqno: Option<u64>,
    ) -> Result<(u64, Result<Option<Entry<K, V>>>)> {
        let _latch = self.latch.acquire_write(self.spin);
        let seqno = match seqno {
            Some(seqno) => seqno,
            None => self.seqno + 1,
        };

        let key_footprint = util::key_footprint(&key)?;
        let new_entry = {
            let value = Value::new_upsert_value(value, seqno);
            Entry::new(key, value)
        };
        self.seqno = seqno;
        match Llrb::upsert_cas(self.root.take(), new_entry, cas, self.lsm)? {
            UpsertCasResult {
                node: root,
                err: Some(err),
                ..
            } => {
                self.root = root;
                Ok((self.seqno, Err(err)))
            }
            UpsertCasResult {
                node: Some(mut root),
                old_entry,
                size,
                err: None,
            } => {
                match &old_entry {
                    None => {
                        self.n_count += 1;
                        self.key_footprint += key_footprint;
                    }
                    Some(oe) if oe.is_deleted() && (self.lsm || self.sticky) => {
                        self.n_deleted -= 1;
                    }
                    _ => (),
                }
                self.tree_footprint += size;

                root.set_black();
                self.root = Some(root);
                Ok((self.seqno, Ok(old_entry)))
            }
            _ => panic!("set_cas: impossible case, call programmer"),
        }
    }

    /// Delete key from index. Return the seqno (index) for this mutation
    /// and entry if present. If operation was invalid or NOOP, returned
    /// seqno shall be ZERO.
    pub fn delete_index<Q>(
        &mut self,
        key: &Q,
        seqno: Option<u64>, // seqno for this delete
    ) -> Result<(u64, Result<Option<Entry<K, V>>>)>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        let _latch = self.latch.acquire_write(self.spin);
        let seqno = match seqno {
            Some(seqno) => seqno,
            None => self.seqno + 1,
        };

        let key_footprint = util::key_footprint(&key.to_owned())?;

        if self.lsm || self.sticky {
            let res = if self.lsm {
                Llrb::delete_lsm(self.root.take(), key, seqno)?
            } else {
                let res = Llrb::delete_sticky(self.root.take(), key, seqno)?;
                if cfg!(debug_assertions) {
                    match &res.old_entry {
                        Some(oe) => assert_eq!(oe.as_deltas().len(), 0),
                        _ => (),
                    }
                }
                res
            };
            self.root = res.node;
            self.root.as_mut().map(|r| r.set_black());
            self.seqno = seqno;
            self.tree_footprint += res.size;

            return match res.old_entry {
                None => {
                    self.key_footprint += key_footprint;
                    self.n_count += 1;
                    self.n_deleted += 1;
                    Ok((seqno, Ok(None)))
                }
                Some(entry) => {
                    if !entry.is_deleted() {
                        self.n_deleted += 1;
                    }
                    Ok((seqno, Ok(Some(entry))))
                }
            };
        } else {
            // in non-lsm mode remove the entry from the tree.
            let res = match Llrb::do_delete(self.root.take(), key)? {
                res @ DeleteResult { node: None, .. } => res,
                mut res => {
                    res.node.as_mut().map(|node| node.set_black());
                    res
                }
            };
            self.root = res.node;
            self.seqno = seqno;
            if res.old_entry.is_some() {
                self.key_footprint -= key_footprint;
                self.tree_footprint += res.size;

                self.n_count -= 1;
                Ok((seqno, Ok(res.old_entry)))
            } else {
                Ok((seqno, Ok(res.old_entry)))
            }
        }
    }
}

/// Create/Update/Delete operations on Llrb index.
impl<K, V> Writer<K, V> for Llrb<K, V>
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
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        let (_seqno, entry) = self.delete_index(key, None)?;
        entry
    }
}

/// Create/Update/Delete operations on Llrb index.
impl<K, V> Llrb<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn upsert(
        node: Option<Box<Node<K, V>>>,
        nentry: Entry<K, V>,
        lsm: bool, // preserve old entries
    ) -> Result<UpsertResult<K, V>> {
        match node {
            None => {
                let mut node: Box<Node<K, V>> = Box::new(From::from(nentry));
                node.dirty = false;
                let size: isize = node.footprint()?.try_into()?;
                return Ok(UpsertResult {
                    node: Some(node),
                    old_entry: None,
                    size,
                });
            }
            Some(mut node) => {
                node = Llrb::walkdown_rot23(node);
                match node.as_key().cmp(nentry.as_key()) {
                    Ordering::Greater => {
                        let mut r = Llrb::upsert(node.left.take(), nentry, lsm)?;
                        node.left = r.node;
                        r.node = Some(Llrb::walkuprot_23(node));
                        Ok(r)
                    }
                    Ordering::Less => {
                        let mut r = Llrb::upsert(node.right.take(), nentry, lsm)?;
                        node.right = r.node;
                        r.node = Some(Llrb::walkuprot_23(node));
                        Ok(r)
                    }
                    Ordering::Equal if lsm => {
                        let size = node.footprint()?;
                        let (old_entry, entry) = {
                            let entry = node.entry.clone();
                            (Some(entry.clone()), entry)
                        };
                        node.entry = entry.xmerge(nentry)?;
                        let size = node.footprint()? - size;
                        Ok(UpsertResult {
                            node: Some(Llrb::walkuprot_23(node)),
                            old_entry,
                            size,
                        })
                    }
                    Ordering::Equal => {
                        let old_entry = Some(node.entry.clone());
                        let size = node.prepend_version(nentry, lsm)?;
                        Ok(UpsertResult {
                            node: Some(Llrb::walkuprot_23(node)),
                            old_entry,
                            size,
                        })
                    }
                }
            }
        }
    }

    fn upsert_cas(
        node: Option<Box<Node<K, V>>>,
        nentry: Entry<K, V>,
        cas: u64,
        lsm: bool,
    ) -> Result<UpsertCasResult<K, V>> {
        let mut node = match node {
            None if cas > 0 => {
                return Ok(UpsertCasResult {
                    node: None,
                    old_entry: None,
                    size: 0,
                    err: Some(Error::InvalidCAS(0)),
                });
            }
            None => {
                let mut node: Box<Node<K, V>> = Box::new(From::from(nentry));
                node.dirty = false;
                let size: isize = node.footprint()?.try_into()?;
                return Ok(UpsertCasResult {
                    node: Some(node),
                    old_entry: None,
                    size,
                    err: None,
                });
            }
            Some(node) => node,
        };

        node = Llrb::walkdown_rot23(node);
        match node.as_key().cmp(nentry.as_key()) {
            Ordering::Greater => {
                let mut r = Llrb::upsert_cas(node.left.take(), nentry, cas, lsm)?;
                node.left = r.node;
                r.node = Some(Llrb::walkuprot_23(node));
                Ok(r)
            }
            Ordering::Less => {
                let mut r = Llrb::upsert_cas(node.right.take(), nentry, cas, lsm)?;
                node.right = r.node;
                r.node = Some(Llrb::walkuprot_23(node));
                Ok(r)
            }
            Ordering::Equal => {
                let seqno = node.to_seqno();
                let p = node.is_deleted() && cas != 0 && cas != seqno;
                let p = p || (!node.is_deleted() && cas != seqno);
                if p {
                    Ok(UpsertCasResult {
                        node: Some(Llrb::walkuprot_23(node)),
                        old_entry: None,
                        size: 0,
                        err: Some(Error::InvalidCAS(seqno)),
                    })
                } else {
                    let old_entry = Some(node.entry.clone());
                    let size = node.prepend_version(nentry, lsm)?;
                    Ok(UpsertCasResult {
                        node: Some(Llrb::walkuprot_23(node)),
                        old_entry,
                        size,
                        err: None,
                    })
                }
            }
        }
    }

    fn delete_lsm<Q>(
        node: Option<Box<Node<K, V>>>,
        key: &Q,
        seqno: u64, // seqno for this mutation
    ) -> Result<DeleteResult<K, V>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        match node {
            None => {
                // insert and mark as delete
                let mut node = Node::new_deleted(key.to_owned(), seqno);
                node.dirty = false;
                let size: isize = node.footprint()?.try_into()?;
                Ok(DeleteResult {
                    node: Some(node),
                    old_entry: None,
                    size,
                })
            }
            Some(mut node) => {
                node = Llrb::walkdown_rot23(node);
                match node.as_key().borrow().cmp(&key) {
                    Ordering::Greater => {
                        let mut r = Llrb::delete_lsm(node.left.take(), key, seqno)?;
                        node.left = r.node;
                        r.node = Some(Llrb::walkuprot_23(node));
                        Ok(r)
                    }
                    Ordering::Less => {
                        let mut r = Llrb::delete_lsm(node.right.take(), key, seqno)?;
                        node.right = r.node;
                        r.node = Some(Llrb::walkuprot_23(node));
                        Ok(r)
                    }
                    Ordering::Equal => {
                        let entry = node.entry.clone();
                        let size = node.delete(seqno)?;
                        Ok(DeleteResult {
                            node: Some(Llrb::walkuprot_23(node)),
                            old_entry: Some(entry),
                            size,
                        })
                    }
                }
            }
        }
    }

    fn delete_sticky<Q>(
        node: Option<Box<Node<K, V>>>,
        key: &Q,
        seqno: u64, // seqno for this mutation
    ) -> Result<DeleteResult<K, V>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        match node {
            None => {
                // insert and mark as delete
                let mut node = Node::new_deleted(key.to_owned(), seqno);
                node.dirty = false;
                let size: isize = node.footprint()?.try_into()?;
                Ok(DeleteResult {
                    node: Some(node),
                    old_entry: None,
                    size,
                })
            }
            Some(mut node) => {
                node = Llrb::walkdown_rot23(node);
                match node.as_key().borrow().cmp(&key) {
                    Ordering::Greater => {
                        let mut r = Llrb::delete_sticky(node.left.take(), key, seqno)?;
                        node.left = r.node;
                        r.node = Some(Llrb::walkuprot_23(node));
                        Ok(r)
                    }
                    Ordering::Less => {
                        let mut r = Llrb::delete_sticky(node.right.take(), key, seqno)?;
                        node.right = r.node;
                        r.node = Some(Llrb::walkuprot_23(node));
                        Ok(r)
                    }
                    Ordering::Equal => {
                        let mut size = node.footprint()?;
                        let entry = node.entry.clone();
                        node.delete(seqno)?;
                        node.entry = node
                            .entry
                            .clone()
                            .purge(Bound::Included(entry.to_seqno()))
                            .unwrap();
                        size = node.footprint()? - size; // TODO
                        Ok(DeleteResult {
                            node: Some(Llrb::walkuprot_23(node)),
                            old_entry: Some(entry),
                            size,
                        })
                    }
                }
            }
        }
    }

    // this is the non-lsm path.
    fn do_delete<Q>(node: Option<Box<Node<K, V>>>, key: &Q) -> Result<DeleteResult<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut node = match node {
            None => {
                return Ok(DeleteResult {
                    node: None,
                    old_entry: None,
                    size: 0,
                })
            }
            Some(node) => node,
        };

        if node.as_key().borrow().gt(key) {
            if node.left.is_none() {
                Ok(DeleteResult {
                    node: Some(node),
                    old_entry: None,
                    size: 0,
                })
            } else {
                let ok = !is_red(node.as_left_deref());
                if ok && !is_red(node.left.as_ref().unwrap().as_left_deref()) {
                    node = Llrb::move_red_left(node);
                }
                let mut r = Llrb::do_delete(node.left.take(), key)?;
                node.left = r.node;
                r.node = Some(Llrb::fixup(node));
                Ok(r)
            }
        } else {
            if is_red(node.as_left_deref()) {
                node = Llrb::rotate_right(node);
            }

            if !node.as_key().borrow().lt(key) && node.right.is_none() {
                return Ok(DeleteResult {
                    node: None,
                    old_entry: Some(node.entry.clone()),
                    size: -node.footprint()?,
                });
            }

            let ok = node.right.is_some() && !is_red(node.as_right_deref());
            if ok && !is_red(node.right.as_ref().unwrap().as_left_deref()) {
                node = Llrb::move_red_right(node);
            }

            if !node.as_key().borrow().lt(key) {
                // node == key
                let (right, mut res_node) = Llrb::delete_min(node.right.take());
                node.right = right;
                if res_node.is_none() {
                    panic!("do_delete(): fatal logic, call the programmer");
                }
                let subdel = res_node.take().unwrap();
                let mut newnode = Box::new(subdel.clone_detach());
                newnode.left = node.left.take();
                newnode.right = node.right.take();
                newnode.black = node.black;
                newnode.dirty = false;
                let size: isize = node.footprint()?.try_into()?;
                Ok(DeleteResult {
                    node: Some(Llrb::fixup(newnode)),
                    old_entry: Some(node.entry.clone()),
                    size: -size,
                })
            } else {
                let mut r = Llrb::do_delete(node.right.take(), key)?;
                node.right = r.node;
                r.node = Some(Llrb::fixup(node));
                Ok(r)
            }
        }
    }

    // return [node, old_node]
    fn delete_min(
        node: Option<Box<Node<K, V>>>, // root node
    ) -> (Option<Box<Node<K, V>>>, Option<Node<K, V>>) {
        match node {
            None => (None, None),
            Some(node) if node.left.is_none() => (None, Some(*node)),
            Some(mut node) => {
                let left = node.as_left_deref();
                if !is_red(left) && !is_red(left.unwrap().as_left_deref()) {
                    node = Llrb::move_red_left(node);
                }
                let (left, old_node) = Llrb::delete_min(node.left.take());
                node.left = left;
                (Some(Llrb::fixup(node)), old_node)
            }
        }
    }
}

impl<K, V> Llrb<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn set_index_entry(&self, entry: Entry<K, V>) -> Result<(u64, Option<Entry<K, V>>)> {
        let mself = unsafe {
            // caller hold a write latch.
            (self as *const Self as *mut Self).as_mut().unwrap()
        };

        let key_footprint = util::key_footprint(entry.as_key())?;
        let (seqno, deleted) = (entry.to_seqno(), entry.is_deleted());
        match Llrb::upsert(mself.root.take(), entry, mself.lsm)? {
            UpsertResult {
                node: Some(mut root),
                old_entry,
                size,
            } => {
                // println!("set_index_entry, result {}", size);
                match &old_entry {
                    None => {
                        mself.n_count += 1;
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
                mself.tree_footprint += size;

                root.set_black();
                mself.root = Some(root);
                mself.seqno = cmp::max(mself.seqno, seqno);
                Ok((mself.seqno, old_entry))
            }
            _ => panic!("set: impossible case, call programmer"),
        }
    }
}

struct CompactCtxt<'a, K>
where
    K: Clone + Ord + Footprint,
{
    cutoff: Bound<u64>,
    dels: Vec<K>,
    tree_footprint: &'a mut isize,
}

impl<K, V> Llrb<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn compact_loop(
        node: Option<&mut Node<K, V>>,
        low: Bound<K>,
        cc: &mut CompactCtxt<K>,
        limit: usize,
    ) -> Result<(Option<K>, usize)> {
        use std::ops::Bound::{Excluded, Unbounded};

        match (node, low) {
            (None, _) => Ok((None, limit)),
            // find the starting point
            (Some(node), Unbounded) => {
                match Self::compact_loop(node.as_left_deref_mut(), Unbounded, cc, limit)? {
                    (seen, limit) if limit == 0 => Ok((seen, limit)),
                    (_, limit) => {
                        Self::compact_entry(node, cc)?;
                        let right = node.as_right_deref_mut();
                        match Self::compact_loop(right, Unbounded, cc, limit - 1)? {
                            (None, limit) => Ok((Some(node.to_key()), limit)),
                            (seen, limit) => Ok((seen, limit)),
                        }
                    }
                }
            }
            (Some(node), Excluded(key)) => match key.cmp(node.as_key()) {
                Ordering::Less => {
                    match Self::compact_loop(node.as_left_deref_mut(), Excluded(key), cc, limit)? {
                        (seen, limit) if limit == 0 => Ok((seen, limit)),
                        (_, limit) => {
                            Self::compact_entry(node, cc)?;
                            let rnode = node.as_right_deref_mut();
                            match Self::compact_loop(rnode, Unbounded, cc, limit - 1)? {
                                (None, limit) => Ok((Some(node.to_key()), limit)),
                                (seen, limit) => Ok((seen, limit)),
                            }
                        }
                    }
                }
                _ => Self::compact_loop(node.as_right_deref_mut(), Excluded(key), cc, limit),
            },
            _ => unreachable!(),
        }
    }

    fn compact_entry(node: &mut Node<K, V>, cc: &mut CompactCtxt<K>) -> Result<()> {
        let size = node.entry.footprint()?;
        *cc.tree_footprint += match node.entry.clone().purge(cc.cutoff) {
            None => {
                cc.dels.push(node.entry.to_key());
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
        let res = match Llrb::do_delete(mself.root.take(), key.borrow())? {
            res @ DeleteResult { node: None, .. } => res,
            mut res => {
                res.node.as_mut().map(|node| node.set_black());
                res
            }
        };
        mself.root = res.node;
        match res.old_entry {
            Some(old_entry) => {
                mself.key_footprint -= util::key_footprint(&key)?;
                mself.tree_footprint += res.size;
                mself.n_count -= 1;
                if old_entry.is_deleted() {
                    mself.n_deleted -= 1;
                }
            }
            None => unreachable!(),
        }

        Ok(())
    }
}

/// Read operations on Llrb index.
impl<K, V> Reader<K, V> for Llrb<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Get the entry for `key`.
    fn get<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + Hash,
    {
        let _latch = self.latch.acquire_read(self.spin);
        get(self.root.as_ref().map(Deref::deref), key)
    }

    /// Return an iterator over all entries in this index.
    fn iter(&mut self) -> Result<IndexIter<K, V>> {
        let _latch = Some(self.latch.acquire_read(self.spin));

        let node = self.root.as_ref().map(Deref::deref);
        Ok(Box::new(Iter {
            _latch,
            _arc: Default::default(),
            paths: Some(build_iter(IFlag::Left, node, vec![])),
        }))
    }

    /// Range over all entries from low to high.
    fn range<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let _latch = Some(self.latch.acquire_read(self.spin));

        let root = self.root.as_ref().map(Deref::deref);
        let paths = match range.start_bound() {
            Bound::Unbounded => Some(build_iter(IFlag::Left, root, vec![])),
            Bound::Included(low) => Some(find_start(root, low, true, vec![])),
            Bound::Excluded(low) => Some(find_start(root, low, false, vec![])),
        };
        Ok(Box::new(Range {
            _latch,
            _arc: Default::default(),
            range,
            paths,
            high: marker::PhantomData,
        }))
    }

    /// Reverse range over all entries from high to low.
    fn reverse<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let _latch = Some(self.latch.acquire_read(self.spin));

        let root = self.root.as_ref().map(Deref::deref);
        let paths = match range.end_bound() {
            Bound::Unbounded => Some(build_iter(IFlag::Right, root, vec![])),
            Bound::Included(high) => Some(find_end(root, high, true, vec![])),
            Bound::Excluded(high) => Some(find_end(root, high, false, vec![])),
        };
        let low = marker::PhantomData;
        Ok(Box::new(Reverse {
            _latch,
            _arc: Default::default(),
            range,
            paths,
            low,
        }))
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
        rng: R,
    ) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        self.range(rng)
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

impl<K, V> CommitIterator<K, V> for Box<Llrb<K, V>>
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

impl<K, V> CommitIterator<K, V> for Llrb<K, V>
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
            let _latch = Some(self.latch.acquire_read(self.spin));
            let (root, mut keys) = (self.root.as_ref().map(Deref::deref), vec![]);
            do_shards(root, shards - 1, &mut keys);
            keys
        };
        let keys: Vec<K> = keys.into_iter().filter_map(convert::identity).collect();

        let mut scans: Vec<IndexIter<K, V>> = vec![];
        let mut lkey = Bound::Unbounded;
        for hkey in keys {
            let range = (lkey.clone(), Bound::Excluded(hkey.clone()));
            if self.range(range.clone())?.next().is_some() {
                let mut ss = scans::SkipScan::new(self.to_reader()?);
                ss.set_key_range(range).set_seqno_range(within.clone());
                lkey = Bound::Included(hkey);
                scans.push(Box::new(ss));
            }
        }

        let range = (lkey, Bound::Unbounded);
        if self.range(range.clone())?.next().is_some() {
            let mut ss = scans::SkipScan::new(self.to_reader()?);
            ss.set_key_range(range).set_seqno_range(within);
            scans.push(Box::new(ss));
        }

        Ok(scans)
    }

    fn range_scans<N, G>(&mut self, ranges: Vec<N>, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        N: Clone + RangeBounds<K>,
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

impl<K, V> PiecewiseScan<K, V> for Llrb<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Return an iterator over entries that meet following properties
    /// * Only entries greater than from bound,
    /// * Only entries whose modified seqno is within seqno-range.
    fn pw_scan<G>(&mut self, from: Bound<K>, within: G) -> Result<ScanIter<K, V>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let _latch = Some(self.latch.acquire_read(self.spin));

        // similar to range pre-processing
        let root = self.root.as_ref().map(Deref::deref);
        let paths = match from {
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
        let (start, end) = util::to_start_end(within);
        Ok(Box::new(IterPWScan {
            _latch,
            _arc: Default::default(),
            start,
            end,
            paths,
        }))
    }
}

impl<K, V> Validate<Stats> for Box<Llrb<K, V>>
where
    K: Clone + Ord + fmt::Debug,
    V: Clone + Diff,
{
    fn validate(&mut self) -> Result<Stats> {
        self.as_mut().validate()
    }
}

/// Deep walk validate of Llrb index.
impl<K, V> Validate<Stats> for Llrb<K, V>
where
    K: Clone + Ord + fmt::Debug,
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
        let _latch = self.latch.acquire_read(self.spin);

        let root = self.root.as_ref().map(Deref::deref);
        let (red, depth) = (is_red(root), 0);
        let mut depths: LlrbDepth = Default::default();

        if red {
            let msg = format!("Llrb Root node must be black: {}", self.name);
            return Err(Error::ValidationFail(msg));
        }

        let ss = (0, 0);
        let ss = validate_tree(root, red, ss, depth, &mut depths)?;
        if ss.1 != self.n_deleted {
            let msg = format!("Llrb n_deleted {} != {}", ss.1, self.n_deleted);
            return Err(Error::ValidationFail(msg));
        }

        if depths.to_max() > MAX_TREE_DEPTH {
            let msg = format!("Llrb tree exceeds max_depth {}", depths.to_max());
            return Err(Error::ValidationFail(msg));
        }

        let mut stats = Stats::new(&self.name);
        stats.entries = self.len();
        stats.n_deleted = self.n_deleted;
        stats.node_size = mem::size_of::<Node<K, V>>();
        stats.key_footprint = self.key_footprint;
        stats.tree_footprint = self.tree_footprint;
        stats.rw_latch = self.latch.to_stats()?;
        stats.blacks = Some(ss.0);
        stats.depths = Some(depths);
        Ok(stats)
    }
}

impl<K, V> Llrb<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    //--------- rotation routines for 2-3 algorithm ----------------

    fn walkdown_rot23(node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        node
    }

    fn walkuprot_23(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        if is_red(node.as_right_deref()) && !is_red(node.as_left_deref()) {
            node = Llrb::rotate_left(node);
        }
        let left = node.as_left_deref();
        if is_red(left) && is_red(left.unwrap().as_left_deref()) {
            node = Llrb::rotate_right(node);
        }
        if is_red(node.as_left_deref()) && is_red(node.as_right_deref()) {
            Llrb::flip(node.deref_mut())
        }
        node
    }

    //              (i)                       (i)
    //               |                         |
    //              node                       x
    //              /  \                      / \
    //             /    (r)                 (r)  \
    //            /       \                 /     \
    //          left       x             node      xr
    //                    / \            /  \
    //                  xl   xr       left   xl
    //
    fn rotate_left(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        if is_black(node.as_right_deref()) {
            panic!("rotateleft(): rotating a black link ? call the programmer");
        }
        let mut x = node.right.take().unwrap();
        node.right = x.left.take();
        x.black = node.black;
        node.set_red();
        x.left = Some(node);
        x
    }

    //              (i)                       (i)
    //               |                         |
    //              node                       x
    //              /  \                      / \
    //            (r)   \                   (r)  \
    //           /       \                 /      \
    //          x       right             xl      node
    //         / \                                / \
    //       xl   xr                             xr  right
    //
    fn rotate_right(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        if is_black(node.as_left_deref()) {
            panic!("rotateright(): rotating a black link ? call the programmer")
        }
        let mut x = node.left.take().unwrap();
        node.left = x.right.take();
        x.black = node.black;
        node.set_red();
        x.right = Some(node);
        x
    }

    //        (x)                   (!x)
    //         |                     |
    //        node                  node
    //        / \                   / \
    //      (y) (z)              (!y) (!z)
    //     /      \              /      \
    //   left    right         left    right
    //
    fn flip(node: &mut Node<K, V>) {
        node.left.as_mut().unwrap().toggle_link();
        node.right.as_mut().unwrap().toggle_link();
        node.toggle_link();
    }

    fn fixup(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        node = if is_red(node.as_right_deref()) {
            Llrb::rotate_left(node)
        } else {
            node
        };
        node = {
            let left = node.as_left_deref();
            if is_red(left) && is_red(left.unwrap().as_left_deref()) {
                Llrb::rotate_right(node)
            } else {
                node
            }
        };
        if is_red(node.as_left_deref()) && is_red(node.as_right_deref()) {
            Llrb::flip(node.deref_mut());
        }
        node
    }

    fn move_red_left(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        Llrb::flip(node.deref_mut());
        if is_red(node.right.as_ref().unwrap().as_left_deref()) {
            node.right = Some(Llrb::rotate_right(node.right.take().unwrap()));
            node = Llrb::rotate_left(node);
            Llrb::flip(node.deref_mut());
        }
        node
    }

    fn move_red_right(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        Llrb::flip(node.deref_mut());
        if is_red(node.left.as_ref().unwrap().as_left_deref()) {
            node = Llrb::rotate_right(node);
            Llrb::flip(node.deref_mut());
        }
        node
    }
}

/// Read handle into [Llrb] index.
pub struct LlrbReader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    _refn: Arc<u32>,
    id: usize,
    index: Option<Box<ffi::c_void>>, // Box<Llrb<K, V>>
    phantom_key: marker::PhantomData<K>,
    phantom_val: marker::PhantomData<V>,
}

impl<K, V> LlrbReader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn new(index: Box<ffi::c_void>, _refn: Arc<u32>) -> LlrbReader<K, V> {
        let id = Arc::strong_count(&_refn);
        let mut r = LlrbReader {
            _refn,
            id,
            index: Some(index),
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        };

        let index: &mut Llrb<K, V> = r.as_mut();
        debug!( target: "llrb  ", "{:?}, creating a new reader {} ...", index.name, id);

        r
    }
}

impl<K, V> Drop for LlrbReader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn drop(&mut self) {
        let id = self.id;
        let index: &mut Llrb<K, V> = self.as_mut();
        debug!(target: "llrb  ", "{:?}, dropping reader {}", index.name, id);

        // leak this index, it is only a reference
        Box::leak(self.index.take().unwrap());
    }
}

impl<K, V> AsMut<Llrb<K, V>> for LlrbReader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn as_mut(&mut self) -> &mut Llrb<K, V> {
        unsafe {
            // transmute void pointer to mutable reference into index.
            let index_ptr = self.index.as_mut().unwrap().as_mut();
            let index_ptr = index_ptr as *mut ffi::c_void;
            (index_ptr as *mut Llrb<K, V>).as_mut().unwrap()
        }
    }
}

impl<K, V> Reader<K, V> for LlrbReader<K, V>
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
        let index: &mut Llrb<K, V> = self.as_mut();
        index.get(key)
    }

    /// Iterate over all entries in this index.
    fn iter(&mut self) -> Result<IndexIter<K, V>> {
        let index: &mut Llrb<K, V> = self.as_mut();
        index.iter()
    }

    /// Iterate from lower bound to upper bound.
    fn range<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let index: &mut Llrb<K, V> = self.as_mut();
        index.range(range)
    }

    /// Iterate from upper bound to lower bound.
    fn reverse<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let index: &mut Llrb<K, V> = self.as_mut();
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

impl<K, V> PiecewiseScan<K, V> for LlrbReader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Return an iterator over entries that meet following properties
    /// * Only entries greater than from bound,
    /// * Only entries whose modified seqno is within seqno-range.
    fn pw_scan<G>(&mut self, from: Bound<K>, within: G) -> Result<ScanIter<K, V>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let index: &mut Llrb<K, V> = self.as_mut();
        index.pw_scan(from, within)
    }
}

/// Write handle into [Llrb] index.
pub struct LlrbWriter<K, V>
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

impl<K, V> LlrbWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn new(index: Box<ffi::c_void>, _refn: Arc<u32>) -> LlrbWriter<K, V> {
        let id = Arc::strong_count(&_refn);
        let mut w = LlrbWriter {
            _refn,
            id,
            index: Some(index),
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        };

        let index: &mut Llrb<K, V> = w.as_mut();
        debug!(target: "llrb  ", "{:?}, creating a new writer {}", index.name, id);

        w
    }
}

impl<K, V> Drop for LlrbWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn drop(&mut self) {
        let id = self.id;
        let index: &mut Llrb<K, V> = self.as_mut();
        debug!(target: "llrb  ", "{:?}, dropping writer {}", index.name, id);

        // leak this index, it is only a reference
        Box::leak(self.index.take().unwrap());
    }
}

impl<K, V> AsMut<Llrb<K, V>> for LlrbWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn as_mut(&mut self) -> &mut Llrb<K, V> {
        unsafe {
            // transmute void pointer to mutable reference into index.
            let index_ptr = self.index.as_mut().unwrap().as_mut();
            let index_ptr = index_ptr as *mut ffi::c_void;
            (index_ptr as *mut Llrb<K, V>).as_mut().unwrap()
        }
    }
}

impl<K, V> LlrbWriter<K, V>
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
        let index: &mut Llrb<K, V> = self.as_mut();
        index.set_index(key, value, seqno)
    }

    /// Refer Llrb::set_cas_index() for more details.
    pub fn set_cas_index(
        &mut self,
        key: K,
        value: V,
        cas: u64,
        seqno: Option<u64>,
    ) -> Result<(u64, Result<Option<Entry<K, V>>>)> {
        let index: &mut Llrb<K, V> = self.as_mut();
        index.set_cas_index(key, value, cas, seqno)
    }

    /// Refer Llrb::delete_index() for more details.
    pub fn delete_index<Q>(
        &mut self,
        key: &Q,
        seqno: Option<u64>,
    ) -> Result<(u64, Result<Option<Entry<K, V>>>)>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        let index: &mut Llrb<K, V> = self.as_mut();
        index.delete_index(key, seqno)
    }
}

impl<K, V> Writer<K, V> for LlrbWriter<K, V>
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
        let index: &mut Llrb<K, V> = self.as_mut();
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
        let index: &mut Llrb<K, V> = self.as_mut();
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
        let index: &mut Llrb<K, V> = self.as_mut();
        let (_seqno, entry) = index.delete_index(key, None)?;
        entry
    }
}

/// Create/Update/Delete operations on Llrb index.
impl<K, V> WalWriter<K, V> for LlrbWriter<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn set_index(&mut self, key: K, value: V, seqno: u64) -> Result<Option<Entry<K, V>>> {
        let index: &mut Llrb<K, V> = self.as_mut();
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
        let index: &mut Llrb<K, V> = self.as_mut();
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
        let index: &mut Llrb<K, V> = self.as_mut();
        let (_seqno, res) = index.delete_index(key, Some(seqno))?;
        res
    }
}

/// Statistics for [`Llrb`] tree.
pub struct Stats {
    pub name: String,
    pub entries: usize,
    pub n_deleted: usize,
    pub node_size: usize,
    pub key_footprint: isize,
    pub tree_footprint: isize,
    pub rw_latch: spinlock::Stats,
    pub blacks: Option<usize>,
    pub depths: Option<LlrbDepth>,
}

impl Stats {
    pub(crate) fn new(name: &str) -> Stats {
        Stats {
            name: name.to_string(),
            entries: Default::default(),
            n_deleted: Default::default(),
            node_size: Default::default(),
            key_footprint: Default::default(),
            tree_footprint: Default::default(),
            rw_latch: Default::default(),
            blacks: Default::default(),
            depths: Default::default(),
        }
    }

    pub fn merge(self, other: Self) -> Self {
        let rw_latch = spinlock::Stats {
            value: 0xC0FFEE,
            read_locks: self.rw_latch.read_locks + other.rw_latch.read_locks,
            write_locks: self.rw_latch.write_locks + other.rw_latch.write_locks,
            conflicts: self.rw_latch.conflicts + other.rw_latch.conflicts,
        };
        let blacks = match (self.blacks, other.blacks) {
            (Some(b1), Some(b2)) => Some((b1 + b2) / 2),
            (Some(b1), None) => Some(b1),
            (None, Some(b2)) => Some(b2),
            (None, None) => None,
        };
        let depths = match (self.depths, other.depths) {
            (Some(d1), Some(d2)) => Some(d1.merge(d2)),
            (Some(d1), None) => Some(d1),
            (None, Some(d2)) => Some(d2),
            (None, None) => None,
        };
        Stats {
            name: Default::default(),
            entries: self.entries + other.entries,
            n_deleted: self.n_deleted + other.n_deleted,
            node_size: self.node_size,
            key_footprint: self.key_footprint + other.key_footprint,
            tree_footprint: self.tree_footprint + other.tree_footprint,
            rw_latch,
            blacks,
            depths,
        }
    }
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        let none = "none".to_string();
        let b = self.blacks.as_ref().map_or(none.clone(), |x| x.to_string());
        let d = self.depths.as_ref().map_or(none.clone(), |x| x.to_string());
        write!(f, "llrb.name = {}\n", self.name)?;
        write!(
            f,
            "llrb = {{ entries={}, n_deleted={}, node_size={}, blacks={} }}\n",
            self.entries, self.n_deleted, self.node_size, b,
        )?;
        write!(
            f,
            "llrb = {{ key_footprint={}, tree_footprint={} }}\n",
            self.key_footprint, self.tree_footprint,
        )?;
        write!(f, "llrb.rw_latch = {}\n", self.rw_latch)?;
        write!(f, "llrb.depths = {}", d)
    }
}

impl ToJson for Stats {
    fn to_json(&self) -> String {
        let null = "null".to_string();
        let l_stats = self.rw_latch.to_json();
        format!(
            concat!(
                r#"{{ ""llrb": {{ "name": {}, "entries": {:X}, "n_deleted": {}",
                r#""key_footprint": {}, "tree_footprint": {}, "#,
                r#""node_size": {}, "#,
                r#""rw_latch": {}, "blacks": {}, "depths": {} }} }}"#,
            ),
            self.name,
            self.entries,
            self.n_deleted,
            self.key_footprint,
            self.tree_footprint,
            self.node_size,
            l_stats,
            self.blacks
                .as_ref()
                .map_or(null.clone(), |x| format!("{}", x)),
            self.depths.as_ref().map_or(null.clone(), |x| x.to_json()),
        )
    }
}

#[cfg(test)]
#[path = "llrb_test.rs"]
mod llrb_test;
