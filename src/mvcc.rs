//! [LLRB][llrb] index for concurrent readers, single writer using
//! [Multi-Version-Concurrency-Control][mvcc].
//!
//! [mvcc]: https://en.wikipedia.org/wiki/Multiversion_concurrency_control
//! [llrb]: https://en.wikipedia.org/wiki/Left-leaning_red-black_tree
use std::{
    borrow::Borrow,
    cmp::{Ord, Ordering},
    convert::TryInto,
    fmt::Debug,
    marker, mem,
    ops::{Bound, Deref, DerefMut, RangeBounds},
    sync::{
        atomic::{AtomicIsize, AtomicPtr, Ordering::Relaxed},
        Arc,
    },
};

use crate::core::{Diff, Entry, Footprint, Result, Value};
use crate::core::{FullScan, Index, IndexIter, Reader, Writer};
use crate::error::Error;
use crate::llrb::Llrb;
use crate::llrb_node::{LlrbDepth, Node, Stats};
use crate::spinlock;

const RECLAIM_CAP: usize = 128;

include!("llrb_common.rs");

/// [LLRB][llrb] index for concurrent readers, single writer using
/// [Multi-Version-Concurrency-Control][mvcc].
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
    snapshot: Snapshot<K, V>,
    key_footprint: AtomicIsize,
    tree_footprint: AtomicIsize,
    w: Option<MvccWriter<K, V>>,
}

impl<K, V> Drop for Mvcc<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn drop(&mut self) {
        // NOTE: Means all references to mvcc are gone and ownership is
        // going out of scope. This also implies that there are only
        // TWO Arc<> snapshots. One is held by self.snapshot and another
        // is held by `next`.

        // NOTE: Snapshot's AtomicPtr will fence the drop chain, so we have
        // to get past the atomic fence and drop it here.

        // NOTE: Likewise MvccRoot will fence the drop on its `root` field, so we
        // have to get past that and drop it here.

        let snapshot_ptr = self.snapshot.value.load(Relaxed);
        // snapshot shall be dropped, along with it MvccRoot.
        let mut snapshot = unsafe { Box::from_raw(snapshot_ptr) };
        let mvcc_root = Arc::get_mut(&mut *snapshot).unwrap();

        //println!("drop mvcc {:p} {:p}", self, mvcc_root);
        mvcc_root.root.take().map(|root| drop_tree(root));
    }
}

/// Construct new instance of Mvcc.
impl<K, V> Mvcc<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    pub fn new<S>(name: S) -> Box<Mvcc<K, V>>
    where
        S: AsRef<str>,
    {
        let mut index = Box::new(Mvcc {
            name: name.as_ref().to_string(),
            lsm: false,
            snapshot: Snapshot::new(),
            key_footprint: AtomicIsize::new(0),
            tree_footprint: AtomicIsize::new(0),
            w: Default::default(),
        });
        let idx = index.as_mut() as *mut Mvcc<K, V>;
        index.w = Some(MvccWriter::new(idx));
        index
    }

    pub fn new_lsm<S>(name: S) -> Box<Mvcc<K, V>>
    where
        S: AsRef<str>,
    {
        let mut index = Box::new(Mvcc {
            name: name.as_ref().to_string(),
            lsm: true,
            snapshot: Snapshot::new(),
            key_footprint: AtomicIsize::new(0),
            tree_footprint: AtomicIsize::new(0),
            w: unsafe { mem::zeroed() },
        });
        let idx = index.as_mut() as *mut Mvcc<K, V>;
        index.w = Some(MvccWriter::new(idx));
        index
    }

    fn shallow_clone(&self) -> Box<Mvcc<K, V>> {
        let mut index = Box::new(Mvcc {
            name: self.name.clone(),
            lsm: self.lsm,
            snapshot: Snapshot::new(),
            key_footprint: AtomicIsize::new(self.key_footprint.load(Relaxed)),
            tree_footprint: AtomicIsize::new(self.tree_footprint.load(Relaxed)),
            w: unsafe { mem::zeroed() },
        });
        let idx = index.as_mut() as *mut Mvcc<K, V>;
        index.w = Some(MvccWriter::new(idx));
        index
    }

    fn clone(&self) -> Box<Mvcc<K, V>> {
        let mut cloned = Box::new(Mvcc {
            name: self.name.clone(),
            lsm: self.lsm,
            snapshot: Snapshot::new(),
            key_footprint: AtomicIsize::new(self.key_footprint.load(Relaxed)),
            tree_footprint: AtomicIsize::new(self.tree_footprint.load(Relaxed)),
            w: Default::default(),
        });
        let idx = cloned.as_mut() as *mut Mvcc<K, V>;
        cloned.w = Some(MvccWriter::new(idx));

        let s: Arc<MvccRoot<K, V>> = Snapshot::clone(&self.snapshot);
        let root_node = match s.as_root() {
            None => None,
            Some(n) => Some(Box::new(n.clone())),
        };
        cloned
            .snapshot
            .shift_snapshot(root_node, s.seqno, s.n_count, vec![]);
        cloned
    }

    fn from_llrb(llrb_index: Llrb<K, V>) -> Box<Mvcc<K, V>> {
        let mvcc_index = if llrb_index.is_lsm() {
            Mvcc::new_lsm(llrb_index.to_name())
        } else {
            Mvcc::new(llrb_index.to_name())
        };

        let debris = llrb_index.squash();
        mvcc_index
            .key_footprint
            .store(debris.key_footprint, Relaxed);
        mvcc_index
            .tree_footprint
            .store(debris.tree_footprint, Relaxed);

        mvcc_index.snapshot.shift_snapshot(
            debris.root,
            debris.seqno,
            debris.n_count,
            vec![], /*reclaim*/
        );
        mvcc_index
    }
}

/// Maintanence API.
impl<K, V> Mvcc<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Return number of entries in this instance.
    #[inline]
    pub fn len(&self) -> usize {
        Snapshot::clone(&self.snapshot).n_count
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
        Snapshot::clone(&self.snapshot).seqno
    }

    /// Return quickly with basic statisics, only entries() method is valid
    /// with this statisics.
    pub fn stats(&self) -> Stats {
        Stats::new_partial(self.len(), mem::size_of::<Node<K, V>>())
    }
}

impl<K, V> Index<K, V> for Mvcc<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    type W = MvccWriter<K, V>;

    /// Make a new empty index of this type, with same configuration.
    fn make_new(&self) -> Result<Box<Self>> {
        Ok(self.shallow_clone())
    }

    /// Create a new writer handle. Only one writer handle can be
    /// active at any time, creating more than one writer handle
    /// will panic. Concurrent readers are allowed without using any
    /// underlying locks/latches.
    fn to_writer(&mut self) -> Self::W {
        match self.w.take() {
            Some(w) => w,
            None => panic!("writer not initialized"),
        }
    }
}

impl<K, V> Footprint for Mvcc<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn footprint(&self) -> isize {
        self.tree_footprint.load(Relaxed)
    }
}

/// Create/Update/Delete operations on Mvcc instance.
impl<K, V> Mvcc<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    pub fn set(&mut self, key: K, value: V) -> Result<Option<Entry<K, V>>> {
        let seqno = self.to_seqno();
        match &mut self.w {
            Some(w) => {
                let (_seqno, entry) = w.set_index(key, value, seqno + 1);
                entry
            }
            None => panic!("already given a writer_handle for this index"),
        }
    }

    pub fn set_cas(&mut self, key: K, value: V, cas: u64) -> Result<Option<Entry<K, V>>> {
        let seqno = self.to_seqno();
        match &mut self.w {
            Some(w) => {
                let (_, entry) = w.set_cas_index(key, value, cas, seqno + 1);
                entry
            }
            None => panic!("already given a writer_handle for this index"),
        }
    }

    pub fn delete<Q>(&mut self, key: &Q) -> Result<Option<Entry<K, V>>>
    where
        // TODO: From<Q> and Clone will fail if V=String and Q=str
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        let seqno = self.to_seqno();
        match &mut self.w {
            Some(w) => {
                let (_seqno, entry) = w.delete_index(key, seqno + 1);
                entry
            }
            None => panic!("already given a writer_handle for this index"),
        }
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
        node: Option<Box<Node<K, V>>>,
        new_entry: Entry<K, V>,
        lsm: bool,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> UpsertResult<K, V> {
        if node.is_none() {
            let node: Box<Node<K, V>> = Box::new(From::from(new_entry));
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
        let mut new_node = node.mvcc_clone(reclaim);

        let cmp = new_node.as_key().cmp(new_entry.as_key());
        let r = if cmp == Ordering::Greater {
            let left = new_node.left.take();
            let mut r = Mvcc::upsert(left, new_entry, lsm, reclaim);
            new_node.left = r.node;
            r.node = Some(Mvcc::walkuprot_23(new_node, reclaim));
            r
        } else if cmp == Ordering::Less {
            let right = new_node.right.take();
            let mut r = Mvcc::upsert(right, new_entry, lsm, reclaim);
            new_node.right = r.node;
            r.node = Some(Mvcc::walkuprot_23(new_node, reclaim));
            r
        } else {
            let entry = node.entry.clone();
            let size = new_node.prepend_version(new_entry, lsm);
            new_node.dirty = true;
            let n = new_node.duplicate();
            UpsertResult {
                node: Some(Mvcc::walkuprot_23(new_node, reclaim)),
                new_node: Some(n),
                old_entry: Some(entry),
                size,
            }
        };

        Box::leak(node);
        r
    }

    fn upsert_cas(
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
            let node: Box<Node<K, V>> = Box::new(From::from(nentry));
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
        let mut newnd = node.mvcc_clone(reclaim);

        let cmp = newnd.as_key().cmp(nentry.as_key());
        let r = if cmp == Ordering::Greater {
            let left = newnd.left.take();
            let mut r = Mvcc::upsert_cas(left, nentry, cas, lsm, reclaim);
            newnd.left = r.node;
            r.node = Some(Mvcc::walkuprot_23(newnd, reclaim));
            r
        } else if cmp == Ordering::Less {
            let right = newnd.right.take();
            let mut r = Mvcc::upsert_cas(right, nentry, cas, lsm, reclaim);
            newnd.right = r.node;
            r.node = Some(Mvcc::walkuprot_23(newnd, reclaim));
            r
        } else if newnd.is_deleted() && cas != 0 && cas != newnd.to_seqno() {
            UpsertCasResult {
                node: Some(newnd),
                new_node: None,
                old_entry: None,
                size: 0,
                err: Some(Error::InvalidCAS),
            }
        } else if !newnd.is_deleted() && cas != newnd.to_seqno() {
            UpsertCasResult {
                node: Some(newnd),
                new_node: None,
                old_entry: None,
                size: 0,
                err: Some(Error::InvalidCAS),
            }
        } else {
            let entry = Some(node.entry.clone());
            let size = newnd.prepend_version(nentry, lsm);
            newnd.dirty = true;
            let n = newnd.duplicate();
            UpsertCasResult {
                node: Some(Mvcc::walkuprot_23(newnd, reclaim)),
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
            let mut node = Node::new_deleted(key.to_owned(), seqno);
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
        let mut new_node = node.mvcc_clone(reclaim);

        let (n, entry, size) = match new_node.as_key().borrow().cmp(&key) {
            Ordering::Greater => {
                let left = new_node.left.take();
                let r = Mvcc::delete_lsm(left, key, seqno, reclaim);
                new_node.left = r.node;
                (r.new_node, r.old_entry, r.size)
            }
            Ordering::Less => {
                let right = new_node.right.take();
                let r = Mvcc::delete_lsm(right, key, seqno, reclaim);
                new_node.right = r.node;
                (r.new_node, r.old_entry, r.size)
            }
            Ordering::Equal => {
                let old_entry = node.entry.clone();
                let size = if !node.is_deleted() {
                    new_node.delete(seqno)
                } else {
                    0
                };
                new_node.dirty = true;
                let n = new_node.duplicate();
                (Some(n), Some(old_entry), size)
            }
        };

        Box::leak(node);
        DeleteResult {
            node: Some(Mvcc::walkuprot_23(new_node, reclaim)),
            new_node: n,
            old_entry: entry,
            size,
        }
    }

    // this is the non-lsm path.
    fn do_delete<Q>(
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
        let mut newnd = node.mvcc_clone(reclaim);
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
                    newnd = Mvcc::move_red_left(newnd, reclaim);
                }
                let mut r = Mvcc::do_delete(newnd.left.take(), key, reclaim);
                newnd.left = r.node;
                r.node = Some(Mvcc::fixup(newnd, reclaim));
                r
            }
        } else {
            if is_red(newnd.as_left_deref()) {
                newnd = Mvcc::rotate_right(newnd, reclaim);
            }

            // if key equals node and no right children
            if !newnd.as_key().borrow().lt(key) && newnd.right.is_none() {
                newnd.mvcc_detach();
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
                newnd = Mvcc::move_red_right(newnd, reclaim);
            }

            // if key equal node and there is a right children
            if !newnd.as_key().borrow().lt(key) {
                // node == key
                let right = newnd.right.take();
                let (right, mut res_node) = Mvcc::delete_min(right, reclaim);
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
                    node: Some(Mvcc::fixup(newnode, reclaim)),
                    new_node: None,
                    old_entry: Some(entry),
                    size,
                }
            } else {
                let mut r = Mvcc::do_delete(newnd.right.take(), key, reclaim);
                newnd.right = r.node;
                r.node = Some(Mvcc::fixup(newnd, reclaim));
                r
            }
        }
    }

    // return [node, old_node]
    fn delete_min(
        node: Option<Box<Node<K, V>>>,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> (Option<Box<Node<K, V>>>, Option<Box<Node<K, V>>>) {
        if node.is_none() {
            return (None, None);
        }

        let node = node.unwrap();
        let mut new_node = node.mvcc_clone(reclaim);
        Box::leak(node);

        if new_node.left.is_none() {
            new_node.mvcc_detach();
            (None, Some(new_node))
        } else {
            let left = new_node.as_left_deref();
            if !is_red(left) && !is_red(left.unwrap().as_left_deref()) {
                new_node = Mvcc::move_red_left(new_node, reclaim);
            }
            let left = new_node.left.take();
            let (left, old_node) = Mvcc::delete_min(left, reclaim);
            new_node.left = left;
            (Some(Mvcc::fixup(new_node, reclaim)), old_node)
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
        get(Snapshot::clone(&self.snapshot).as_root(), key)
    }

    fn iter(&self) -> Result<IndexIter<K, V>> {
        let mut iter = Box::new(Iter {
            _latch: Default::default(),
            _arc: Snapshot::clone(&self.snapshot),
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
            _arc: Snapshot::clone(&self.snapshot),
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
            _arc: Snapshot::clone(&self.snapshot),
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
    fn full_scan<G>(&self, from: Bound<K>, within: G) -> Result<IndexIter<K, V>>
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
            _arc: Snapshot::clone(&self.snapshot),
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
    /// * From root to any leaf, no consecutive reds allowed in its path.
    /// * Number of blacks should be same on under left child and right child.
    /// * Make sure that keys are in sorted order.
    ///
    /// Additionally return full statistics on the tree. Refer to [`Stats`]
    /// for more information.
    pub fn validate(&self) -> Result<Stats> {
        let arc_mvcc = Snapshot::clone(&self.snapshot);
        let root = arc_mvcc.as_root();
        let (red, blacks, depth) = (is_red(root), 0, 0);
        let mut depths: LlrbDepth = Default::default();
        let blacks = validate_tree(root, red, blacks, depth, &mut depths)?;

        Ok(Stats::new_full(
            arc_mvcc.n_count,
            std::mem::size_of::<Node<K, V>>(),
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
        mut node: Box<Node<K, V>>,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> Box<Node<K, V>> {
        if is_red(node.as_right_deref()) && !is_red(node.as_left_deref()) {
            node = Mvcc::rotate_left(node, reclaim);
        }
        let left = node.as_left_deref();
        if is_red(left) && is_red(left.unwrap().as_left_deref()) {
            node = Mvcc::rotate_right(node, reclaim);
        }
        if is_red(node.as_left_deref()) && is_red(node.as_right_deref()) {
            Mvcc::flip(node.deref_mut(), reclaim)
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
    fn rotate_left(
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
            Box::leak(old_right).mvcc_clone(reclaim)
        };

        node.right = right.left.take();
        right.black = node.black;
        node.set_red();
        right.left = Some(node);

        right
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
    fn rotate_right(
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
            Box::leak(old_left).mvcc_clone(reclaim)
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
    fn flip(node: &mut Node<K, V>, reclaim: &mut Vec<Box<Node<K, V>>>) {
        let old_left = node.left.take().unwrap();
        let old_right = node.right.take().unwrap();

        let mut left = if old_left.dirty {
            old_left
        } else {
            Box::leak(old_left).mvcc_clone(reclaim)
        };
        let mut right = if old_right.dirty {
            old_right
        } else {
            Box::leak(old_right).mvcc_clone(reclaim)
        };

        left.toggle_link();
        right.toggle_link();
        node.toggle_link();

        node.left = Some(left);
        node.right = Some(right);
    }

    fn fixup(
        mut node: Box<Node<K, V>>,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> Box<Node<K, V>> {
        if is_red(node.as_right_deref()) {
            node = Mvcc::rotate_left(node, reclaim)
        }
        let left = node.as_left_deref();
        if is_red(left) && is_red(left.unwrap().as_left_deref()) {
            node = Mvcc::rotate_right(node, reclaim)
        }
        if is_red(node.as_left_deref()) && is_red(node.as_right_deref()) {
            Mvcc::flip(node.deref_mut(), reclaim);
        }
        node
    }

    fn move_red_left(
        mut node: Box<Node<K, V>>,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> Box<Node<K, V>> {
        Mvcc::flip(node.deref_mut(), reclaim);
        if is_red(node.right.as_ref().unwrap().as_left_deref()) {
            let right = node.right.take().unwrap();
            node.right = Some(Mvcc::rotate_right(right, reclaim));
            node = Mvcc::rotate_left(node, reclaim);
            Mvcc::flip(node.deref_mut(), reclaim);
        }
        node
    }

    fn move_red_right(
        mut node: Box<Node<K, V>>,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> Box<Node<K, V>> {
        Mvcc::flip(node.deref_mut(), reclaim);
        if is_red(node.left.as_ref().unwrap().as_left_deref()) {
            node = Mvcc::rotate_right(node, reclaim);
            Mvcc::flip(node.deref_mut(), reclaim);
        }
        node
    }
}

#[derive(Default)]
struct Snapshot<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    value: AtomicPtr<Arc<MvccRoot<K, V>>>,
}

impl<K, V> Snapshot<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    // create the first snapshot and a placeholder `next` snapshot for Mvcc.
    fn new() -> Snapshot<K, V> {
        let mvcc_root = MvccRoot::new(Some(Arc::new(MvccRoot::new(None))));
        let arc = Box::new(Arc::new(mvcc_root));
        //println!("new snapshot {:p} {}", arc, Arc::strong_count(&arc));
        Snapshot {
            value: AtomicPtr::new(Box::leak(arc)),
        }
    }

    // similar to Arc::clone for AtomicPtr<Arc<MvccRoot<K,V>>>
    fn clone(this: &Snapshot<K, V>) -> Arc<MvccRoot<K, V>> {
        Arc::clone(unsafe { this.value.load(Relaxed).as_ref().unwrap() })
    }

    fn shift_snapshot(
        &self,
        root: Option<Box<Node<K, V>>>,
        seqno: u64,
        n_count: usize,
        reclaim: Vec<Box<Node<K, V>>>,
    ) {
        // * curr_s points to next_s, and currently the only reference to next_s.
        // * curr_s gets dropped, but there can be readers holding a reference.
        // * when curr_s gets dropped it reference to next_s is decremented.
        // * before curr_s gets dropped next_s is cloned, leaked, stored.

        let curr_s = unsafe { Box::from_raw(self.value.load(Relaxed)) };
        let next_s = Box::new(Arc::clone(curr_s.next.as_ref().unwrap()));
        let mvcc_root = unsafe {
            (&**next_s as *const MvccRoot<K, V> as *mut MvccRoot<K, V>)
                .as_mut()
                .unwrap()
        };

        mvcc_root.root = root;
        mvcc_root.seqno = seqno;
        mvcc_root.n_count = n_count;
        mvcc_root.next = Some(Arc::new(MvccRoot::new(None)));
        mvcc_root.reclaim = reclaim;

        self.value.store(Box::leak(next_s), Relaxed);
    }
}

pub(crate) struct MvccRoot<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    root: Option<Box<Node<K, V>>>,
    reclaim: Vec<Box<Node<K, V>>>,
    seqno: u64,     // starts from 0 and incr for every mutation.
    n_count: usize, // number of entries in the tree.
    next: Option<Arc<MvccRoot<K, V>>>,
}

impl<K, V> MvccRoot<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    // shall be called twice while creating the Mvcc index and once
    // for every new snapshot that gets created and shifted into the chain.
    fn new(next: Option<Arc<MvccRoot<K, V>>>) -> MvccRoot<K, V> {
        //println!("new mvcc-root {:p}", mvcc_root);
        let mut mvcc_root: MvccRoot<K, V> = Default::default();
        mvcc_root.next = next;
        mvcc_root
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

impl<K, V> Drop for MvccRoot<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn drop(&mut self) {
        // NOTE: `root` will be leaked, so that the tree is intact.

        // NOTE: `reclaim` nodes will be dropped, but due the Drop
        // implementation of Node, child nodes won't be dropped.

        // NOTE: `next` snapshot will be dropped and its reference
        // count decremented, whether it is freed is based on the last
        // active reference at that moment.

        self.root.take().map(Box::leak); // Leak root
    }
}

impl<K, V> Default for MvccRoot<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn default() -> MvccRoot<K, V> {
        MvccRoot {
            root: Default::default(),
            reclaim: Default::default(),
            seqno: Default::default(),
            n_count: Default::default(),
            next: Default::default(),
        }
    }
}

/// MvccWriter handle for [`Mvcc`] index.
///
/// Note that only one writer handle can be active at any given
/// time to write into Mvcc index.
///
/// [Mvcc]: crate::mvcc::Mvcc
pub struct MvccWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    index: Option<*mut Mvcc<K, V>>,
}

impl<K, V> Drop for MvccWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn drop(&mut self) {
        // NOTE: forget the writer, which is a self-reference.
    }
}

impl<K, V> MvccWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn new(index: *mut Mvcc<K, V>) -> MvccWriter<K, V> {
        MvccWriter { index: Some(index) }
    }
}

impl<K, V> MvccWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn get_index(&mut self) -> &mut Mvcc<K, V> {
        match &mut self.index {
            Some(index) => unsafe { index.as_mut().unwrap() },
            None => unreachable!(),
        }
    }
}

impl<K, V> Writer<K, V> for MvccWriter<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn set_index(
        &mut self,
        key: K,
        value: V,
        seqno: u64, // seqno for this mutation
    ) -> (Option<u64>, Result<Option<Entry<K, V>>>) {
        let index = self.get_index();
        let lsm = index.lsm;
        let snapshot = Snapshot::clone(&index.snapshot);
        let key_footprint = key.footprint();

        let mut n_count = snapshot.n_count;
        let value = Box::new(Value::new_upsert_value(value, seqno));
        let new_entry = Entry::new(key, value);

        let root = snapshot.root_duplicate();
        let mut reclm: Vec<Box<Node<K, V>>> = Vec::with_capacity(RECLAIM_CAP);
        match Mvcc::upsert(root, new_entry, lsm, &mut reclm) {
            UpsertResult {
                node: Some(mut root),
                new_node: Some(mut n),
                old_entry,
                size,
            } => {
                root.set_black();
                if old_entry.is_none() {
                    n_count += 1;
                    index.key_footprint.fetch_add(key_footprint, Relaxed);
                }
                index.tree_footprint.fetch_add(size, Relaxed);
                n.dirty = false;
                Box::leak(n);
                index
                    .snapshot
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
        mut seqno: u64, // seqno for this mutation
    ) -> (Option<u64>, Result<Option<Entry<K, V>>>) {
        let index = self.get_index();
        let lsm = index.lsm;
        let snapshot = Snapshot::clone(&index.snapshot);
        let key_footprint = key.footprint();

        let mut n_count = snapshot.n_count;
        let value = Box::new(Value::new_upsert_value(value, seqno));
        let new_entry = Entry::new(key, value);
        let root = snapshot.root_duplicate();
        let mut rclm: Vec<Box<Node<K, V>>> = Vec::with_capacity(RECLAIM_CAP);
        let s = match Mvcc::upsert_cas(root, new_entry, cas, lsm, &mut rclm) {
            UpsertCasResult {
                node: Some(mut root),
                new_node,
                err: Some(err),
                ..
            } => {
                seqno = index.to_seqno();
                root.set_black();
                (root, new_node, Err(err))
            }
            UpsertCasResult {
                node: Some(mut root),
                new_node,
                old_entry,
                err: None,
                size,
            } => {
                root.set_black();
                if old_entry.is_none() {
                    index.key_footprint.fetch_add(key_footprint, Relaxed);
                    n_count += 1
                }
                index.tree_footprint.fetch_add(size, Relaxed);
                (root, new_node, Ok(old_entry))
            }
            _ => panic!("set_cas: impossible case, call programmer"),
        };
        let (root, optn, entry) = s;

        // TODO: can we optimize this for no-op cases (err cases) ?
        index
            .snapshot
            .shift_snapshot(Some(root), seqno, n_count, rclm);

        if let Some(mut n) = optn {
            n.dirty = false;
            Box::leak(n);
        }
        (Some(seqno), entry)
    }

    fn delete_index<Q>(
        &mut self,
        key: &Q,
        mut seqno: u64, // seqno for this mutation
    ) -> (Option<u64>, Result<Option<Entry<K, V>>>)
    where
        // TODO: From<Q> and Clone will fail if V=String and Q=str
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        let index = self.get_index();
        let snapshot = Snapshot::clone(&index.snapshot);

        let key_footprint = key.to_owned().footprint();

        let mut n_count = snapshot.n_count;
        let root = snapshot.root_duplicate();
        let mut reclm: Vec<Box<Node<K, V>>> = Vec::with_capacity(RECLAIM_CAP);
        let (root, old_entry) = if index.lsm {
            let s = match Mvcc::delete_lsm(root, key, seqno, &mut reclm) {
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

            //println!("delete {:?}", entry.as_ref().map(|e| e.is_deleted()));
            match &old_entry {
                None => {
                    index.key_footprint.fetch_add(key_footprint, Relaxed);
                    index.tree_footprint.fetch_add(size, Relaxed);

                    n_count += 1;
                }
                Some(e) if e.is_deleted() => {
                    seqno = index.to_seqno();
                }
                _ /* not-deleted */ => {
                    index.tree_footprint.fetch_add(size, Relaxed);
                }
            }

            if let Some(mut n) = new_node {
                n.dirty = false;
                Box::leak(n);
            }
            (root, old_entry)
        } else {
            // in non-lsm mode remove the entry from the tree.
            let res = match Mvcc::do_delete(root, key, &mut reclm) {
                res @ DeleteResult { node: None, .. } => res,
                mut res => {
                    res.node.as_mut().map(|node| node.set_black());
                    res
                }
            };
            if res.old_entry.is_some() {
                index.key_footprint.fetch_add(key_footprint, Relaxed);
                index.tree_footprint.fetch_add(res.size, Relaxed);

                n_count -= 1;
            } else {
                seqno = index.to_seqno();
            }
            (res.node, res.old_entry)
        };

        index.snapshot.shift_snapshot(root, seqno, n_count, reclm);
        (Some(seqno), Ok(old_entry))
    }
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
