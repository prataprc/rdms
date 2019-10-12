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
//! *seqno*, application can set the beginning sequence number before
//! ingesting data into the index.
//!
//! [llrb]: https://en.wikipedia.org/wiki/Left-leaning_red-black_tree
//! [mvcc]: https://en.wikipedia.org/wiki/Multiversion_concurrency_control
//! [LSM mode]: https://en.wikipedia.org/wiki/Log-structured_merge-tree
use std::{
    borrow::Borrow,
    cmp::{Ord, Ordering},
    convert::TryInto,
    fmt::Debug,
    marker, mem,
    ops::{Bound, Deref, DerefMut, RangeBounds},
    sync::{
        atomic::{AtomicIsize, AtomicPtr, AtomicUsize, Ordering::SeqCst},
        mpsc, Arc,
    },
    thread,
};

use crate::core::{Diff, Entry, Footprint, Result, ScanEntry, Value};
use crate::core::{EphemeralIndex, FullScan, IndexIter, ScanIter};
use crate::core::{Reader, WalWriter, Writer};
use crate::error::Error;
use crate::llrb::Llrb;
use crate::llrb_node::{LlrbDepth, Node, Stats};
use crate::spinlock::{self, RWSpinlock};

const RECLAIM_CAP: usize = 128;

include!("llrb_common.rs");

// TODO: Experiment with different atomic::Ordering to improve performance.

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
    spin: bool,

    snapshot: OuterSnapshot<K, V>,
    latch: RWSpinlock,
    key_footprint: AtomicIsize,
    tree_footprint: AtomicIsize,
    readers: Arc<u32>,
    writers: Arc<u32>,
}

impl<K, V> Drop for Mvcc<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn drop(&mut self) {
        // validation check 1
        let n = self.multi_rw();
        if n > Self::CONCUR_REF_COUNT {
            panic!("Mvcc dropped before read/write handles {}", n);
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
            spin: true,

            snapshot: OuterSnapshot::new(),
            latch: RWSpinlock::new(),
            key_footprint: AtomicIsize::new(0),
            tree_footprint: AtomicIsize::new(0),
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
            spin: true,

            snapshot: OuterSnapshot::new(),
            latch: RWSpinlock::new(),
            key_footprint: AtomicIsize::new(0),
            tree_footprint: AtomicIsize::new(0),
            readers: Arc::new(0xC0FFEE),
            writers: Arc::new(0xC0FFEE),
        })
    }

    pub fn from_llrb(llrb_index: Llrb<K, V>) -> Box<Mvcc<K, V>> {
        let mut mvcc_index = if llrb_index.is_lsm() {
            Mvcc::new_lsm(llrb_index.to_name())
        } else {
            Mvcc::new(llrb_index.to_name())
        };
        mvcc_index.set_spinlatch(llrb_index.to_spin());
        mvcc_index
            .snapshot
            .n_nodes
            .store(llrb_index.len() as isize, SeqCst);

        let debris = llrb_index.squash();
        mvcc_index.key_footprint.store(debris.key_footprint, SeqCst);
        mvcc_index
            .tree_footprint
            .store(debris.tree_footprint, SeqCst);
        mvcc_index.snapshot.shift_snapshot(
            debris.root,
            debris.seqno,
            debris.n_count,
            vec![], /*reclaim*/
        );
        mvcc_index
    }

    /// Configure behaviour of spin-latch. If `spin` is true, calling
    /// thread shall spin until a latch is acquired or released, if false
    /// calling thread will yield to scheduler.
    pub fn set_spinlatch(&mut self, spin: bool) {
        let n = self.multi_rw();
        if n > Self::CONCUR_REF_COUNT {
            panic!("cannot configure Mvcc with active readers/writer {}", n);
        }

        self.spin = spin;
    }

    pub fn clone(&self) -> Box<Mvcc<K, V>> {
        let n = self.multi_rw();
        if n > Self::CONCUR_REF_COUNT {
            panic!("cannot clone Mvcc with active readers/writer {}", n);
        }

        let cloned = Box::new(Mvcc {
            name: self.name.clone(),
            lsm: self.lsm,
            spin: self.spin,

            snapshot: OuterSnapshot::new(),
            latch: RWSpinlock::new(),
            key_footprint: AtomicIsize::new(self.key_footprint.load(SeqCst)),
            tree_footprint: AtomicIsize::new(self.tree_footprint.load(SeqCst)),
            readers: Arc::new(0xC0FFEE),
            writers: Arc::new(0xC0FFEE),
        });

        let s: Arc<Snapshot<K, V>> = OuterSnapshot::clone(&self.snapshot);
        let root_node = match s.as_root() {
            None => None,
            Some(n) => Some(Box::new(n.clone())),
        };
        cloned
            .snapshot
            .shift_snapshot(root_node, s.seqno, s.n_count, vec![]);
        cloned
    }

    fn shallow_clone(&self) -> Box<Mvcc<K, V>> {
        let n = self.multi_rw();
        if n > Self::CONCUR_REF_COUNT {
            panic!("cannot shallow-clone with active readers/writer {}", n);
        }

        Box::new(Mvcc {
            name: self.name.clone(),
            lsm: self.lsm,
            spin: self.spin,

            snapshot: OuterSnapshot::new(),
            latch: RWSpinlock::new(),
            key_footprint: AtomicIsize::new(self.key_footprint.load(SeqCst)),
            tree_footprint: AtomicIsize::new(self.tree_footprint.load(SeqCst)),
            readers: Arc::new(0xC0FFEE),
            writers: Arc::new(0xC0FFEE),
        })
    }
}

/// Maintanence API.
impl<K, V> Mvcc<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    const CONCUR_REF_COUNT: usize = 2;

    /// Return whether this index support lsm mode.
    #[inline]
    pub fn is_lsm(&self) -> bool {
        self.lsm
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

    /// Return current seqno.
    #[inline]
    pub fn to_seqno(&self) -> u64 {
        OuterSnapshot::clone(&self.snapshot).seqno
    }

    /// Return quickly with basic statisics, only entries() method is valid
    /// with this statisics.
    pub fn to_stats(&self) -> Stats {
        let n = self.latch.to_conflicts() + self.snapshot.ulatch.to_conflicts();
        Stats::new_partial(self.len(), mem::size_of::<Node<K, V>>(), n)
    }

    fn multi_rw(&self) -> usize {
        Arc::strong_count(&self.readers) + Arc::strong_count(&self.writers)
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

impl<K, V> EphemeralIndex<K, V> for Mvcc<K, V>
where
    K: Send + Sync + Clone + Ord + Footprint,
    V: Send + Sync + Clone + Diff + Footprint,
{
    type W = MvccWriter<K, V>;
    type R = MvccReader<K, V>;

    /// Make a new empty index of this type, with same configuration.
    fn new(&self) -> Result<Box<Self>> {
        Ok(self.shallow_clone())
    }

    /// Application can set the start sequence number for this index.
    fn set_seqno(&mut self, seqno: u64) {
        let n = self.multi_rw();
        if n > Self::CONCUR_REF_COUNT {
            panic!("cannot configure Mvcc with active readers/writer {}", n);
        }

        let snapshot = OuterSnapshot::clone(&self.snapshot);
        let root = snapshot.root_duplicate();
        self.snapshot
            .shift_snapshot(root, seqno, snapshot.n_count, vec![]);
    }

    /// Lockless concurrent readers are supported
    fn to_reader(&mut self) -> Result<Self::R> {
        let index: Box<std::ffi::c_void> = unsafe {
            // transmute self as void pointer.
            Box::from_raw(self as *mut Mvcc<K, V> as *mut std::ffi::c_void)
        };
        Ok(MvccReader::<K, V>::new(index))
    }

    /// Create a new writer handle. Multiple writers uses spin-lock to
    /// serialize write operation.
    fn to_writer(&mut self) -> Result<Self::W> {
        let index: Box<std::ffi::c_void> = unsafe {
            // transmute self as void pointer.
            Box::from_raw(self as *mut Mvcc<K, V> as *mut std::ffi::c_void)
        };
        Ok(MvccWriter::<K, V>::new(index))
    }
}

impl<K, V> Footprint for Mvcc<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn footprint(&self) -> isize {
        self.tree_footprint.load(SeqCst)
    }
}

impl<K, V> Mvcc<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn set_index(
        &mut self,
        key: K,
        value: V,
        seqno: Option<u64>, // seqno for this mutation
    ) -> (Option<u64>, Result<Option<Entry<K, V>>>) {
        let _w = self.latch.acquire_write(self.spin);
        let snapshot: &Arc<Snapshot<K, V>> = self.snapshot.as_ref();

        let seqno = match seqno {
            Some(seqno) => seqno,
            None => snapshot.seqno + 1,
        };
        let key_footprint = key.footprint();
        let new_entry = Entry::new(key, Value::new_upsert_value(value, seqno));

        let mut n_count = snapshot.n_count;
        let root = snapshot.root_duplicate();
        let mut reclm: Vec<Box<Node<K, V>>> = Vec::with_capacity(RECLAIM_CAP);
        match self.upsert(root, new_entry, self.lsm, &mut reclm) {
            UpsertResult {
                node: Some(mut root),
                new_node: Some(mut n),
                old_entry,
                size,
            } => {
                root.set_black();
                if old_entry.is_none() {
                    n_count += 1;
                    self.key_footprint.fetch_add(key_footprint, SeqCst);
                }
                self.tree_footprint.fetch_add(size, SeqCst);
                n.dirty = false;
                Box::leak(n);
                self.snapshot
                    .shift_snapshot(Some(root), seqno, n_count, reclm);
                (Some(seqno), Ok(old_entry))
            }
            _ => unreachable!(),
        }
    }

    fn set_cas_index(
        &mut self,
        key: K,
        value: V,
        cas: u64,
        seqno: Option<u64>, // seqno for this mutation
    ) -> (Option<u64>, Result<Option<Entry<K, V>>>) {
        let _w = self.latch.acquire_write(self.spin);
        let snapshot: &Arc<Snapshot<K, V>> = self.snapshot.as_ref();

        let seqno = match seqno {
            Some(seqno) => seqno,
            None => snapshot.seqno + 1,
        };
        let lsm = self.lsm;
        let key_footprint = key.footprint();

        let new_entry = Entry::new(key, Value::new_upsert_value(value, seqno));

        let mut n_count = snapshot.n_count;
        let root = snapshot.root_duplicate();
        let mut rclm: Vec<Box<Node<K, V>>> = Vec::with_capacity(RECLAIM_CAP);
        let s = match self.upsert_cas(root, new_entry, cas, lsm, &mut rclm) {
            UpsertCasResult {
                node: Some(mut root),
                new_node,
                old_entry,
                err: None,
                size,
            } => {
                root.set_black();
                if old_entry.is_none() {
                    self.key_footprint.fetch_add(key_footprint, SeqCst);
                    n_count += 1
                }
                self.tree_footprint.fetch_add(size, SeqCst);
                (seqno, root, new_node, Ok(old_entry))
            }
            UpsertCasResult {
                node: Some(mut root),
                new_node,
                err: Some(err),
                ..
            } => {
                root.set_black();
                (snapshot.seqno, root, new_node, Err(err))
            }
            _ => panic!("set_cas: impossible case, call programmer"),
        };
        let (seqno, root, optn, entry) = s;

        if let Some(mut n) = optn {
            n.dirty = false;
            Box::leak(n);
        }

        // TODO: can we optimize this for no-op cases (err cases) ?
        self.snapshot
            .shift_snapshot(Some(root), seqno, n_count, rclm);

        (Some(seqno), entry)
    }

    fn delete_index<Q>(
        &mut self,
        key: &Q,
        seqno: Option<u64>, // seqno for this mutation
    ) -> (Option<u64>, Result<Option<Entry<K, V>>>)
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
        let key_footprint = key.to_owned().footprint();

        let mut n_count = snapshot.n_count;
        let root = snapshot.root_duplicate();
        let mut reclm: Vec<Box<Node<K, V>>> = Vec::with_capacity(RECLAIM_CAP);
        let (seqno, root, old_entry) = if self.lsm {
            let s = match self.delete_lsm(root, key, seqno, &mut reclm) {
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

            self.tree_footprint.fetch_add(size, SeqCst);
            // println!("delete {:?}", entry.as_ref().map(|e| e.is_deleted()));
            match &old_entry {
                None => {
                    self.key_footprint.fetch_add(key_footprint, SeqCst);
                    n_count += 1;
                }
                _ => (),
            }
            if let Some(mut n) = new_node {
                n.dirty = false;
                Box::leak(n);
            }
            (seqno, root, old_entry)
        } else {
            // in non-lsm mode remove the entry from the tree.
            let res = match self.do_delete(root, key, &mut reclm) {
                res @ DeleteResult { node: None, .. } => res,
                mut res => {
                    res.node.as_mut().map(|node| node.set_black());
                    res
                }
            };
            let seqno = if res.old_entry.is_some() {
                self.key_footprint.fetch_add(key_footprint, SeqCst);
                self.tree_footprint.fetch_add(res.size, SeqCst);
                n_count -= 1;
                seqno
            } else {
                snapshot.seqno
            };
            (seqno, res.node, res.old_entry)
        };

        self.snapshot.shift_snapshot(root, seqno, n_count, reclm);
        (Some(seqno), Ok(old_entry))
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
        let (_seqno, entry) = self.set_index(key, value, None);
        entry
    }

    /// Similar to set, but succeeds only when CAS matches with entry's
    /// last `seqno`. In other words, since seqno is unique to each mutation,
    /// we use `seqno` of the mutation as the CAS value. Use CAS == 0 to
    /// enforce a create operation.
    ///
    /// *LSM mode*: Add a new version for the key, perserving the old value.
    fn set_cas(&mut self, key: K, value: V, cas: u64) -> Result<Option<Entry<K, V>>> {
        let (_seqno, entry) = self.set_cas_index(key, value, cas, None);
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
        let (_seqno, entry) = self.delete_index(key, None);
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
    ) -> UpsertResult<K, V> {
        if node.is_none() {
            let node: Box<Node<K, V>> = self.node_from_entry(new_entry);
            let n = node.duplicate();
            let size: isize = node.footprint().try_into().unwrap();
            return UpsertResult {
                node: Some(node),
                new_node: Some(n),
                old_entry: None,
                size,
            };
        }

        let node = node.unwrap();
        let cmp = node.as_key().cmp(new_entry.as_key());
        let r = if cmp == Ordering::Greater {
            let mut new_node = self.node_mvcc_clone(&node, reclaim, false);
            let left = new_node.left.take();
            let mut r = self.upsert(left, new_entry, lsm, reclaim);
            new_node.left = r.node;
            r.node = Some(self.walkuprot_23(new_node, reclaim));
            r
        } else if cmp == Ordering::Less {
            let mut new_node = self.node_mvcc_clone(&node, reclaim, false);
            let right = new_node.right.take();
            let mut r = self.upsert(right, new_entry, lsm, reclaim);
            new_node.right = r.node;
            r.node = Some(self.walkuprot_23(new_node, reclaim));
            r
        } else {
            let mut new_node = self.node_mvcc_clone(&node, reclaim, true);
            let entry = node.entry.clone();
            let size = new_node.prepend_version(new_entry, lsm);
            new_node.dirty = true;
            let n = new_node.duplicate();
            UpsertResult {
                node: Some(self.walkuprot_23(new_node, reclaim)),
                new_node: Some(n),
                old_entry: Some(entry),
                size,
            }
        };

        Box::leak(node);
        r
    }

    fn upsert_cas(
        &self,
        node: Option<Box<Node<K, V>>>,
        nentry: Entry<K, V>,
        cas: u64,
        lsm: bool,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> UpsertCasResult<K, V> {
        if node.is_none() && cas > 0 {
            return UpsertCasResult {
                node: None,
                new_node: None,
                old_entry: None,
                size: 0,
                err: Some(Error::InvalidCAS),
            };
        } else if node.is_none() {
            let node: Box<Node<K, V>> = self.node_from_entry(nentry);
            let n = node.duplicate();
            let size: isize = node.footprint().try_into().unwrap();
            return UpsertCasResult {
                node: Some(node),
                new_node: Some(n),
                old_entry: None,
                size,
                err: None,
            };
        }

        let node = node.unwrap();
        let cmp = node.as_key().cmp(nentry.as_key());
        let r = if cmp == Ordering::Greater {
            let mut newnd = self.node_mvcc_clone(&node, reclaim, false);
            let left = newnd.left.take();
            let mut r = self.upsert_cas(left, nentry, cas, lsm, reclaim);
            newnd.left = r.node;
            r.node = Some(self.walkuprot_23(newnd, reclaim));
            r
        } else if cmp == Ordering::Less {
            let mut newnd = self.node_mvcc_clone(&node, reclaim, false);
            let right = newnd.right.take();
            let mut r = self.upsert_cas(right, nentry, cas, lsm, reclaim);
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
                err: Some(Error::InvalidCAS),
            }
        } else if !node.is_deleted() && cas != node.to_seqno() {
            let newnd = self.node_mvcc_clone(&node, reclaim, true);
            UpsertCasResult {
                node: Some(newnd),
                new_node: None,
                old_entry: None,
                size: 0,
                err: Some(Error::InvalidCAS),
            }
        } else {
            let mut newnd = self.node_mvcc_clone(&node, reclaim, true);
            let entry = Some(node.entry.clone());
            let size = newnd.prepend_version(nentry, lsm);
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
        r
    }

    fn delete_lsm<Q>(
        &self,
        node: Option<Box<Node<K, V>>>,
        key: &Q,
        seqno: u64,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> DeleteResult<K, V>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        if node.is_none() {
            let mut node = self.node_new_deleted(key.to_owned(), seqno);
            node.dirty = false;
            let n = node.duplicate();
            let size: isize = node.footprint().try_into().unwrap();
            return DeleteResult {
                node: Some(node),
                new_node: Some(n),
                old_entry: None,
                size,
            };
        }

        let node = node.unwrap();
        let (new_node, n, entry, size) = match node.as_key().borrow().cmp(&key) {
            Ordering::Greater => {
                let mut new_node = self.node_mvcc_clone(&node, reclaim, false);
                let left = new_node.left.take();
                let r = self.delete_lsm(left, key, seqno, reclaim);
                new_node.left = r.node;
                (new_node, r.new_node, r.old_entry, r.size)
            }
            Ordering::Less => {
                let mut new_node = self.node_mvcc_clone(&node, reclaim, false);
                let right = new_node.right.take();
                let r = self.delete_lsm(right, key, seqno, reclaim);
                new_node.right = r.node;
                (new_node, r.new_node, r.old_entry, r.size)
            }
            Ordering::Equal => {
                let mut new_node = self.node_mvcc_clone(&node, reclaim, true);
                let old_entry = node.entry.clone();
                let size = new_node.delete(seqno);
                new_node.dirty = true;
                let n = new_node.duplicate();
                (new_node, Some(n), Some(old_entry), size)
            }
        };

        Box::leak(node);
        DeleteResult {
            node: Some(self.walkuprot_23(new_node, reclaim)),
            new_node: n,
            old_entry: entry,
            size,
        }
    }

    // this is the non-lsm path.
    fn do_delete<Q>(
        &self,
        node: Option<Box<Node<K, V>>>,
        key: &Q,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> DeleteResult<K, V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        if node.is_none() {
            return DeleteResult {
                node: None,
                new_node: None,
                old_entry: None,
                size: 0,
            };
        }

        let node = node.unwrap();
        let mut newnd = self.node_mvcc_clone(&node, reclaim, true);
        Box::leak(node);

        if newnd.as_key().borrow().gt(key) {
            if newnd.left.is_none() {
                // key not present, nothing to delete
                DeleteResult {
                    node: Some(newnd),
                    new_node: None,
                    old_entry: None,
                    size: 0,
                }
            } else {
                let ok = !is_red(newnd.as_left_deref());
                if ok && !is_red(newnd.left.as_ref().unwrap().as_left_deref()) {
                    newnd = self.move_red_left(newnd, reclaim);
                }
                let mut r = self.do_delete(newnd.left.take(), key, reclaim);
                newnd.left = r.node;
                r.node = Some(self.fixup(newnd, reclaim));
                r
            }
        } else {
            if is_red(newnd.as_left_deref()) {
                newnd = self.rotate_right(newnd, reclaim);
            }

            // if key equals node and no right children
            if !newnd.as_key().borrow().lt(key) && newnd.right.is_none() {
                self.node_mvcc_detach(&mut newnd);
                let size: isize = newnd.footprint().try_into().unwrap();
                return DeleteResult {
                    node: None,
                    new_node: None,
                    old_entry: Some(newnd.entry.clone()),
                    size,
                };
            }

            let ok = newnd.right.is_some() && !is_red(newnd.as_right_deref());
            if ok && !is_red(newnd.right.as_ref().unwrap().as_left_deref()) {
                newnd = self.move_red_right(newnd, reclaim);
            }

            // if key equal node and there is a right children
            if !newnd.as_key().borrow().lt(key) {
                // node == key
                let right = newnd.right.take();
                let (right, mut res_node) = self.delete_min(right, reclaim);
                newnd.right = right;
                if res_node.is_none() {
                    panic!("do_delete(): fatal logic, call the programmer");
                }
                let mut newnode = res_node.take().unwrap();
                newnode.left = newnd.left.take();
                newnode.right = newnd.right.take();
                newnode.black = newnd.black;
                let entry = newnd.entry.clone();
                let size: isize = newnd.footprint().try_into().unwrap();
                DeleteResult {
                    node: Some(self.fixup(newnode, reclaim)),
                    new_node: None,
                    old_entry: Some(entry),
                    size,
                }
            } else {
                let mut r = self.do_delete(newnd.right.take(), key, reclaim);
                newnd.right = r.node;
                r.node = Some(self.fixup(newnd, reclaim));
                r
            }
        }
    }

    // return [node, old_node]
    fn delete_min(
        &self,
        node: Option<Box<Node<K, V>>>,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> (Option<Box<Node<K, V>>>, Option<Box<Node<K, V>>>) {
        if node.is_none() {
            return (None, None);
        }

        let node = node.unwrap();
        let mut new_node = self.node_mvcc_clone(&node, reclaim, true);
        Box::leak(node);

        if new_node.left.is_none() {
            self.node_mvcc_detach(&mut new_node);
            (None, Some(new_node))
        } else {
            let left = new_node.as_left_deref();
            if !is_red(left) && !is_red(left.unwrap().as_left_deref()) {
                new_node = self.move_red_left(new_node, reclaim);
            }
            let left = new_node.left.take();
            let (left, old_node) = self.delete_min(left, reclaim);
            new_node.left = left;
            (Some(self.fixup(new_node, reclaim)), old_node)
        }
    }
}

/// Read operations on Mvcc instance.
impl<K, V> Reader<K, V> for Mvcc<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Get the latest version for key.
    fn get<Q>(&self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let snapshot: Arc<Snapshot<K, V>> = OuterSnapshot::clone(&self.snapshot);
        let res = get(snapshot.as_root(), key);
        res
    }

    fn iter(&self) -> Result<IndexIter<K, V>> {
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

    fn range<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
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

    fn reverse<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
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
    fn get_with_versions<Q>(&self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.get(key)
    }

    /// Short circuited to iter().
    fn iter_with_versions(&self) -> Result<IndexIter<K, V>> {
        self.iter()
    }

    /// Short circuited to range().
    fn range_with_versions<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        self.range(range)
    }

    /// Short circuited to reverse()
    fn reverse_with_versions<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        self.reverse(range)
    }
}

impl<K, V> FullScan<K, V> for Mvcc<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff + From<<V as Diff>::D>,
{
    /// Return an iterator over entries that meet following properties
    /// * Only entries greater than range.start_bound().
    /// * Only entries whose modified seqno is within seqno-range.
    fn full_scan<G>(&self, from: Bound<K>, within: G) -> Result<ScanIter<K, V>>
    where
        G: Clone + RangeBounds<u64>,
    {
        // validate arguments.
        let start = match within.start_bound() {
            Bound::Included(x) => Bound::Included(*x),
            Bound::Excluded(x) => Bound::Excluded(*x),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end = match within.end_bound() {
            Bound::Included(x) => Bound::Included(*x),
            Bound::Excluded(x) => Bound::Excluded(*x),
            Bound::Unbounded => Bound::Unbounded,
        };
        // similar to range pre-processing
        let mut iter = Box::new(IterFullScan {
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

/// Deep walk validate of Mvcc index. Note that in addition to normal
/// contraints to type parameter `K`, K-type shall also implement
/// `Debug` trait.
impl<K, V> Mvcc<K, V>
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
    pub fn validate(&self) -> Result<Stats> {
        let arc_mvcc = OuterSnapshot::clone(&self.snapshot);

        let root = arc_mvcc.as_root();
        let (red, blacks, depth) = (is_red(root), 0, 0);
        let mut depths: LlrbDepth = Default::default();

        if red {
            panic!("LLRB violation: Root node is alway black: {}", self.name);
        }

        let blacks = validate_tree(root, red, blacks, depth, &mut depths)?;

        if depths.to_max() > 100 {
            // TODO: avoid magic numbers
            panic!("LLRB depth has exceeded limit: {}", depths.to_max());
        }

        let n = self.latch.to_conflicts() + self.snapshot.ulatch.to_conflicts();
        Ok(Stats::new_full(
            arc_mvcc.n_count,
            std::mem::size_of::<Node<K, V>>(),
            n,
            blacks,
            depths,
        ))
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
        let n_active = Arc::new(AtomicUsize::new(2));
        let m = Arc::clone(&n_active);
        let n = Arc::clone(&n_active);
        let n_nodes = Arc::new(AtomicIsize::new(0));

        let next_snapshot: Option<Arc<Snapshot<K,V>>> = // dummy next snapshot
            Some(Arc::new(*Snapshot::new(None, Arc::clone(&n_nodes), m)));
        let curr_snapshot: Box<Snapshot<K, V>> = // current snapshot
            Snapshot::new(next_snapshot, Arc::clone(&n_nodes), n);

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
        loop {
            if self.n_active.load(SeqCst) < 1000 {
                // TODO: no magic number
                break;
            } else {
                thread::yield_now();
            }
        }

        let _w = self.ulatch.acquire_write(true /*spin*/);
        let m = Arc::clone(&self.n_active);

        // * curr_s points to next_s, and currently the only reference to next_s.
        // * curr_s gets dropped, but there can be readers holding a reference.
        // * before curr_s gets dropped next_s is cloned, leaked, stored.

        let curr_s: Box<Arc<Snapshot<K,V>>> = // current snapshot
            unsafe { Box::from_raw(self.inner.load(SeqCst)) };
        let curr_r: &Snapshot<K, V> = curr_s.as_ref().as_ref();

        let curr_m: &mut Snapshot<K, V> = unsafe {
            (curr_r as *const Snapshot<K,V> // safe extract
            as *mut Snapshot<K,V>)
                .as_mut()
                .unwrap()
        };
        // next snapshot mutable reference.
        let next_m = Arc::get_mut(curr_m.next.as_mut().unwrap()).unwrap();
        // populate the next snapshot.
        next_m.root = root;
        next_m.reclaim = Some(reclaim);
        next_m.seqno = seqno;
        next_m.n_count = n_count;
        let n_nodes = Arc::clone(&self.n_nodes);
        next_m.next = Some(Arc::new(*Snapshot::new(None, n_nodes, m)));

        let next_s: Box<Arc<Snapshot<K, V>>> = Box::new(Arc::clone(
            // clone a Arc reference of next snapshot and make it current
            curr_r.next.as_ref().unwrap(),
        ));

        // let x = Arc::strong_count(curr_s.as_ref());
        // let y = Arc::strong_count(next_s.as_ref());
        //println!(
        //    "shiftsnap {:p} {:p} {} {} ",
        //    curr_r,
        //    next_s.as_ref().as_ref(),
        //    x,
        //    y
        //);
        self.n_active.fetch_add(1, SeqCst);

        self.inner.store(Box::leak(next_s), SeqCst);
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

/// Read handle into [Mvcc] index, that implements both [Send] and [Sync].
pub struct MvccReader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    index: Option<Box<std::ffi::c_void>>, // Box<Mvcc<K, V>>
    phantom_key: marker::PhantomData<K>,
    phantom_val: marker::PhantomData<V>,
}

impl<K, V> Drop for MvccReader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn drop(&mut self) {
        // leak this index, it is only a reference
        Box::leak(self.index.take().unwrap());
    }
}

impl<K, V> AsRef<Mvcc<K, V>> for MvccReader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn as_ref(&self) -> &Mvcc<K, V> {
        unsafe {
            // transmute void pointer to mutable reference into index.
            let index_ptr = self.index.as_ref().unwrap().as_ref();
            let index_ptr = index_ptr as *const std::ffi::c_void;
            (index_ptr as *const Mvcc<K, V>).as_ref().unwrap()
        }
    }
}

impl<K, V> MvccReader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn new(index: Box<std::ffi::c_void>) -> MvccReader<K, V> {
        MvccReader {
            index: Some(index),
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        }
    }
}

impl<K, V> Reader<K, V> for MvccReader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Get ``key`` from index.
    fn get<Q>(&self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let index: &Mvcc<K, V> = self.as_ref();
        index.get(key)
    }

    /// Iterate over all entries in this index.
    fn iter(&self) -> Result<IndexIter<K, V>> {
        let index: &Mvcc<K, V> = self.as_ref();
        index.iter()
    }

    /// Iterate from lower bound to upper bound.
    fn range<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let index: &Mvcc<K, V> = self.as_ref();
        index.range(range)
    }

    /// Iterate from upper bound to lower bound.
    fn reverse<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let index: &Mvcc<K, V> = self.as_ref();
        index.reverse(range)
    }

    /// Short circuited to get().
    fn get_with_versions<Q>(&self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.get(key)
    }

    /// Short circuited to iter().
    fn iter_with_versions(&self) -> Result<IndexIter<K, V>> {
        self.iter()
    }

    /// Short circuited to range().
    fn range_with_versions<'a, R, Q>(&'a self, r: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        self.range(r)
    }

    /// Short circuited to reverse()
    fn reverse_with_versions<'a, R, Q>(&'a self, r: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        self.reverse(r)
    }
}

/// Write handle into [Mvcc] index, that implements both [Send] and [Sync].
pub struct MvccWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    index: Option<Box<std::ffi::c_void>>,
    phantom_key: marker::PhantomData<K>,
    phantom_val: marker::PhantomData<V>,
}

impl<K, V> Drop for MvccWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn drop(&mut self) {
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
            let index_ptr = index_ptr as *mut std::ffi::c_void;
            (index_ptr as *mut Mvcc<K, V>).as_mut().unwrap()
        }
    }
}

impl<K, V> MvccWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn new(index: Box<std::ffi::c_void>) -> MvccWriter<K, V> {
        MvccWriter {
            index: Some(index),
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        }
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
        let (_seqno, entry) = index.set_index(key, value, None);
        entry
    }

    /// Similar to set, but succeeds only when CAS matches with entry's
    /// last `seqno`. In other words, since seqno is unique to each mutation,
    /// we use `seqno` of the mutation as the CAS value. Use CAS == 0 to
    /// enforce a create operation.
    ///
    /// *LSM mode*: Add a new version for the key, perserving the old value.
    fn set_cas(&mut self, key: K, value: V, cas: u64) -> Result<Option<Entry<K, V>>> {
        let index: &mut Mvcc<K, V> = self.as_mut();
        let (_seqno, entry) = index.set_cas_index(key, value, cas, None);
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
        let (_seqno, entry) = index.delete_index(key, None);
        entry
    }
}

impl<K, V> WalWriter<K, V> for MvccWriter<K, V>
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
    fn set_index(
        &mut self,
        key: K,
        value: V,
        seqno: u64, // seqno for this mutation
    ) -> (Option<u64>, Result<Option<Entry<K, V>>>) {
        let index: &mut Mvcc<K, V> = self.as_mut();
        index.set_index(key, value, Some(seqno))
    }

    /// Similar to set, but succeeds only when CAS matches with entry's
    /// Set {key, value} in index if an older entry exists with the
    /// same ``cas`` value. To create a fresh entry, pass ``cas`` as ZERO.
    /// Return the seqno (index) for this mutation and older entry
    /// if present. If operation was invalid or NOOP, returned seqno shall
    /// be ZERO.
    ///
    /// *LSM mode*: Add a new version for the key, perserving the old value.
    fn set_cas_index(
        &mut self,
        key: K,
        value: V,
        cas: u64,
        seqno: u64,
    ) -> (Option<u64>, Result<Option<Entry<K, V>>>) {
        let index: &mut Mvcc<K, V> = self.as_mut();
        index.set_cas_index(key, value, cas, Some(seqno))
    }

    /// Delete key from index. Return the seqno (index) for this mutation
    /// and entry if present. If operation was invalid or NOOP, returned
    /// seqno shall be ZERO.
    fn delete_index<Q>(
        &mut self,
        key: &Q,
        seqno: u64, // seqno for this delete
    ) -> (Option<u64>, Result<Option<Entry<K, V>>>)
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        let index: &mut Mvcc<K, V> = self.as_mut();
        index.delete_index(key, Some(seqno))
    }
}

// TODO: by freeing nodes in a separate thread we can save some cycles
// on the write thread. But right now we don't have a mallocer that
// can perform well with concurrent threads.
#[allow(dead_code)]
fn gc<K, V>(rx: mpsc::Receiver<usize>, n_nodes: Arc<AtomicIsize>) -> Result<()>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    for n in rx {
        // println!("gc node");
        n_nodes.as_ref().fetch_sub(n.try_into().unwrap(), SeqCst);
    }
    Ok(())
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
