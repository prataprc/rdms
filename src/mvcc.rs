//! [LLRB][llrb] index for concurrent readers, single writer using
//! [Multi-Version-Concurrency-Control][mvcc].
//!
//! [mvcc]: https://en.wikipedia.org/wiki/Multiversion_concurrency_control
//! [llrb]: https://en.wikipedia.org/wiki/Left-leaning_red-black_tree
use std::{
    borrow::Borrow,
    cmp::{Ord, Ordering},
    fmt::Debug,
    marker, mem,
    ops::{Bound, Deref, DerefMut, RangeBounds},
    sync::{
        atomic::{AtomicPtr, AtomicU8, Ordering::Relaxed},
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
    writers: AtomicU8,
}

impl<K, V> Clone for Mvcc<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn clone(&self) -> Mvcc<K, V> {
        let cloned = Mvcc {
            name: self.name.clone(),
            lsm: self.lsm,
            snapshot: Snapshot::new(),
            writers: Default::default(),
        };

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
}

impl<K, V> Drop for Mvcc<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn drop(&mut self) {
        // NOTE: Means all references to mvcc are gone and ownership is going out
        // of scope. This also implies that there are only TWO Arc<> snapshots.
        // One is held by self.snapshot and another is held by `next`.

        // NOTE: AtomicPtr will fence the drop chain, so we have to get past the
        // atomic fence and drop it here.

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

impl<K, V> From<Llrb<K, V>> for Mvcc<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn from(mut llrb_index: Llrb<K, V>) -> Mvcc<K, V> {
        let mvcc_index = if llrb_index.is_lsm() {
            Mvcc::new_lsm(llrb_index.to_name())
        } else {
            Mvcc::new(llrb_index.to_name())
        };

        let debris = llrb_index.squash();
        // TODO
        //mvcc_index.key_footprint = debris.key_footprint;
        //mvcc_index.val_footprint = debris.val_footprint;

        mvcc_index.snapshot.shift_snapshot(
            debris.root,
            debris.seqno,
            debris.n_count,
            vec![], /*reclaim*/
        );
        mvcc_index
    }
}

/// Construct new instance of Mvcc.
impl<K, V> Mvcc<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    pub fn new<S>(name: S) -> Mvcc<K, V>
    where
        S: AsRef<str>,
    {
        Mvcc {
            name: name.as_ref().to_string(),
            lsm: false,
            snapshot: Snapshot::new(),
            writers: Default::default(),
        }
    }

    pub fn new_lsm<S>(name: S) -> Mvcc<K, V>
    where
        S: AsRef<str>,
    {
        Mvcc {
            name: name.as_ref().to_string(),
            lsm: true,
            snapshot: Snapshot::new(),
            writers: Default::default(),
        }
    }

    fn shallow_clone(&self) -> Mvcc<K, V> {
        Mvcc {
            name: self.name.clone(),
            lsm: self.lsm,
            snapshot: Snapshot::new(),
            writers: AtomicU8::new(0),
        }
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
    fn make_new(&self) -> Result<Self> {
        Ok(self.shallow_clone())
    }

    /// Create a new writer handle. Only one writer handle can be
    /// active at any time, creating more than one writer handle
    /// will panic. Concurrent readers are allowed without using any
    /// underlying locks/latches.
    fn to_writer(index: Arc<Mvcc<K, V>>) -> Result<Self::W> {
        if index.writers.compare_and_swap(0, 1, Relaxed) == 0 {
            Ok(MvccWriter { index })
        } else {
            panic!("there cannot be more than one writers!")
        }
    }
}

impl<K, V> Footprint for Mvcc<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn footprint(&self) -> usize {
        // TBD
        0
    }
}

/// Create/Update/Delete operations on Mvcc instance.
impl<K, V> Mvcc<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    pub fn set(&mut self, key: K, value: V) -> Result<Option<Entry<K, V>>> {
        let index = unsafe { Arc::from_raw(self as *const Mvcc<K, V>) };
        let mut w = Mvcc::to_writer(index)?;

        let seqno = self.to_seqno();
        let (_seqno, entry) = w.set_index(key, value, seqno + 1);
        entry
    }

    pub fn set_cas(&mut self, key: K, value: V, cas: u64) -> Result<Option<Entry<K, V>>> {
        let index = unsafe { Arc::from_raw(self as *const Mvcc<K, V>) };
        let mut w = Mvcc::to_writer(index)?;

        let seqno = self.to_seqno();
        let (_seqno, entry) = w.set_cas_index(key, value, cas, seqno + 1);
        entry
    }

    pub fn delete<Q>(&mut self, key: &Q) -> Result<Option<Entry<K, V>>>
    where
        // TODO: From<Q> and Clone will fail if V=String and Q=str
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        let index = unsafe { Arc::from_raw(self as *const Mvcc<K, V>) };
        let mut w = Mvcc::to_writer(index)?;

        let seqno = self.to_seqno();
        let (_seqno, entry) = w.delete_index(key, seqno + 1);
        entry
    }
}

/// Create/Update/Delete operations on Mvcc instance.
impl<K, V> Mvcc<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn upsert(
        node: Option<Box<Node<K, V>>>,
        new_entry: Entry<K, V>,
        lsm: bool,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> (
        Option<Box<Node<K, V>>>,
        Option<Box<Node<K, V>>>,
        Option<Entry<K, V>>,
    ) {
        if node.is_none() {
            let node: Box<Node<K, V>> = Box::new(From::from(new_entry));
            let n = node.duplicate();
            return (Some(node), Some(n), None);
        }

        let node = node.unwrap();
        let mut new_node = node.mvcc_clone(reclaim);

        let cmp = new_node.as_key().cmp(new_entry.as_key());
        let (new_node, n, entry) = if cmp == Ordering::Greater {
            let left = new_node.left.take();
            let (l, n, en) = Mvcc::upsert(left, new_entry, lsm, reclaim);
            new_node.left = l;
            (Some(Mvcc::walkuprot_23(new_node, reclaim)), n, en)
        } else if cmp == Ordering::Less {
            let right = new_node.right.take();
            let (r, n, en) = Mvcc::upsert(right, new_entry, lsm, reclaim);
            new_node.right = r;
            (Some(Mvcc::walkuprot_23(new_node, reclaim)), n, en)
        } else {
            let entry = node.entry.clone();
            new_node.prepend_version(new_entry, lsm);
            new_node.dirty = true;
            let n = new_node.duplicate();
            (
                Some(Mvcc::walkuprot_23(new_node, reclaim)),
                Some(n),
                Some(entry),
            )
        };

        Box::leak(node);
        (new_node, n, entry)
    }

    fn upsert_cas(
        node: Option<Box<Node<K, V>>>,
        nentry: Entry<K, V>,
        cas: u64,
        lsm: bool,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> (
        Option<Box<Node<K, V>>>, // mvcc-path
        Option<Box<Node<K, V>>>, // new_node
        Option<Entry<K, V>>,
        Option<Error>,
    ) {
        if node.is_none() && cas > 0 {
            return (None, None, None, Some(Error::InvalidCAS));
        } else if node.is_none() {
            let node: Box<Node<K, V>> = Box::new(From::from(nentry));
            let n = node.duplicate();
            return (Some(node), Some(n), None, None);
        }

        let node = node.unwrap();
        let mut newnd = node.mvcc_clone(reclaim);

        let cmp = newnd.as_key().cmp(nentry.as_key());
        let (newnd, n, entry, err) = if cmp == Ordering::Greater {
            let left = newnd.left.take();
            let s = Mvcc::upsert_cas(left, nentry, cas, lsm, reclaim);
            let (left, n, entry, e) = s;
            newnd.left = left;
            (Some(Mvcc::walkuprot_23(newnd, reclaim)), n, entry, e)
        } else if cmp == Ordering::Less {
            let right = newnd.right.take();
            let s = Mvcc::upsert_cas(right, nentry, cas, lsm, reclaim);
            let (rh, n, entry, e) = s;
            newnd.right = rh;
            (Some(Mvcc::walkuprot_23(newnd, reclaim)), n, entry, e)
        } else if newnd.is_deleted() && cas != 0 && cas != newnd.to_seqno() {
            (Some(newnd), None, None, Some(Error::InvalidCAS))
        } else if !newnd.is_deleted() && cas != newnd.to_seqno() {
            (Some(newnd), None, None, Some(Error::InvalidCAS))
        } else {
            let entry = Some(node.entry.clone());
            newnd.prepend_version(nentry, lsm);
            newnd.dirty = true;
            let n = newnd.duplicate();
            (
                Some(Mvcc::walkuprot_23(newnd, reclaim)),
                Some(n),
                entry,
                None,
            )
        };

        Box::leak(node);
        (newnd, n, entry, err)
    }

    fn delete_lsm<Q>(
        node: Option<Box<Node<K, V>>>,
        key: &Q,
        seqno: u64,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> (
        Option<Box<Node<K, V>>>,
        Option<Box<Node<K, V>>>,
        Option<Entry<K, V>>,
    )
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        if node.is_none() {
            let mut node = Node::new_deleted(key.to_owned(), seqno);
            node.dirty = false;
            let n = node.duplicate();
            return (Some(node), Some(n), None);
        }

        let node = node.unwrap();
        let mut new_node = node.mvcc_clone(reclaim);

        let (n, entry) = match new_node.as_key().borrow().cmp(&key) {
            Ordering::Greater => {
                let left = new_node.left.take();
                let s = Mvcc::delete_lsm(left, key, seqno, reclaim);
                let (left, n, entry) = s;
                new_node.left = left;
                (n, entry)
            }
            Ordering::Less => {
                let right = new_node.right.take();
                let s = Mvcc::delete_lsm(right, key, seqno, reclaim);
                let (right, n, entry) = s;
                new_node.right = right;
                (n, entry)
            }
            Ordering::Equal => {
                let entry = node.entry.clone();
                if !node.is_deleted() {
                    new_node.delete(seqno);
                }
                new_node.dirty = true;
                let n = new_node.duplicate();
                (Some(n), Some(entry))
            }
        };

        Box::leak(node);
        (Some(Mvcc::walkuprot_23(new_node, reclaim)), n, entry)
    }

    // this is the non-lsm path.
    fn do_delete<Q>(
        node: Option<Box<Node<K, V>>>,
        key: &Q,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> (Option<Box<Node<K, V>>>, Option<Entry<K, V>>)
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        if node.is_none() {
            return (None, None);
        }

        let node = node.unwrap();
        let mut newnd = node.mvcc_clone(reclaim);
        Box::leak(node);

        if newnd.as_key().borrow().gt(key) {
            if newnd.left.is_none() {
                // key not present, nothing to delete
                (Some(newnd), None)
            } else {
                let ok = !is_red(newnd.as_left_deref());
                if ok && !is_red(newnd.left.as_ref().unwrap().as_left_deref()) {
                    newnd = Mvcc::move_red_left(newnd, reclaim);
                }
                let left = newnd.left.take();
                let (left, entry) = Mvcc::do_delete(left, key, reclaim);
                newnd.left = left;
                (Some(Mvcc::fixup(newnd, reclaim)), entry)
            }
        } else {
            if is_red(newnd.as_left_deref()) {
                newnd = Mvcc::rotate_right(newnd, reclaim);
            }

            // if key equals node and no right children
            if !newnd.as_key().borrow().lt(key) && newnd.right.is_none() {
                newnd.mvcc_detach();
                return (None, Some(newnd.entry.clone()));
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
                (Some(Mvcc::fixup(newnode, reclaim)), Some(entry))
            } else {
                let right = newnd.right.take();
                let (right, entry) = Mvcc::do_delete(right, key, reclaim);
                newnd.right = right;
                (Some(Mvcc::fixup(newnd, reclaim)), entry)
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
    K: Clone + Ord,
    V: Clone + Diff,
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
    index: Arc<Mvcc<K, V>>,
}

impl<K, V> MvccWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn get_index(&self) -> &mut Mvcc<K, V> {
        let index = self.index.as_ref();
        unsafe {
            let index = index as *const Mvcc<K, V> as *mut Mvcc<K, V>;
            index.as_mut().unwrap()
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

        let mut n_count = snapshot.n_count;
        let value = Box::new(Value::new_upsert_value(value, seqno));
        let new_entry = Entry::new(key, value);

        let root = snapshot.root_duplicate();
        let mut reclm: Vec<Box<Node<K, V>>> = Vec::with_capacity(RECLAIM_CAP);
        match Mvcc::upsert(root, new_entry, lsm, &mut reclm) {
            (Some(mut root), Some(mut n), entry) => {
                root.set_black();
                if entry.is_none() {
                    n_count += 1;
                }
                n.dirty = false;
                Box::leak(n);
                index
                    .snapshot
                    .shift_snapshot(Some(root), seqno, n_count, reclm);
                (Some(seqno), Ok(entry))
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

        let mut n_count = snapshot.n_count;
        let value = Box::new(Value::new_upsert_value(value, seqno));
        let new_entry = Entry::new(key, value);
        let root = snapshot.root_duplicate();
        let mut rclm: Vec<Box<Node<K, V>>> = Vec::with_capacity(RECLAIM_CAP);
        let s = match Mvcc::upsert_cas(root, new_entry, cas, lsm, &mut rclm) {
            (Some(mut root), optn, _, Some(err)) => {
                seqno = index.to_seqno();
                root.set_black();
                (root, optn, Err(err))
            }
            (Some(mut root), optn, entry, None) => {
                root.set_black();
                if entry.is_none() {
                    n_count += 1
                }
                (root, optn, Ok(entry))
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

        let mut n_count = snapshot.n_count;
        let root = snapshot.root_duplicate();
        let mut reclm: Vec<Box<Node<K, V>>> = Vec::with_capacity(RECLAIM_CAP);
        let (root, entry) = if index.lsm {
            let s = match Mvcc::delete_lsm(root, key, seqno, &mut reclm) {
                (Some(mut root), optn, entry) => {
                    root.set_black();
                    (Some(root), optn, entry)
                }
                (None, optn, entry) => (None, optn, entry),
            };
            let (root, optn, entry) = s;

            //println!("delete {:?}", entry.as_ref().map(|e| e.is_deleted()));
            match &entry {
                None => {
                    n_count += 1;
                }
                Some(e) if e.is_deleted() => {
                    seqno = index.to_seqno();
                }
                _ => (),
            }

            if let Some(mut n) = optn {
                n.dirty = false;
                Box::leak(n);
            }
            (root, entry)
        } else {
            // in non-lsm mode remove the entry from the tree.
            let (root, entry) = match Mvcc::do_delete(root, key, &mut reclm) {
                (None, entry) => (None, entry),
                (Some(mut root), entry) => {
                    root.set_black();
                    (Some(root), entry)
                }
            };
            if entry.is_some() {
                n_count -= 1;
            } else {
                seqno = index.to_seqno();
            }
            (root, entry)
        };

        index.snapshot.shift_snapshot(root, seqno, n_count, reclm);
        (Some(seqno), Ok(entry))
    }
}

impl<K, V> Drop for MvccWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn drop(&mut self) {
        use std::sync::atomic::Ordering;

        if self.index.writers.compare_and_swap(1, 0, Ordering::Relaxed) != 1 {
            unreachable!()
        }
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
