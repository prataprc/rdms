//! Single threaded, in-memory index using [left-leaning-red-black][llrb] tree.
//!
//! [llrb]: https://en.wikipedia.org/wiki/Left-leaning_red-black_tree
use std::borrow::Borrow;
use std::cmp::{Ord, Ordering};
use std::convert::TryInto;
use std::fmt::Debug;
use std::ops::{Bound, Deref, DerefMut, RangeBounds};
use std::sync::Arc;
use std::{marker, mem};

use crate::core::{Diff, Entry, Footprint, Result, Value};
use crate::core::{FullScan, Index, IndexIter, Reader, Writer};
use crate::error::Error;
use crate::llrb_node::{LlrbDepth, Node, Stats};
use crate::mvcc::MvccRoot;
use crate::spinlock::{self, RWSpinlock};

// TODO: Rename LlrbWriter to Writer;

include!("llrb_common.rs");

/// Single threaded, in-memory index using [left-leaning-red-black][llrb] tree.
///
/// **[LSM mode]**: Llrb index can support log-structured-merge while
/// mutating the tree. In simple terms, this means that nothing shall be
/// over-written in the tree and all the mutations for the same key shall
/// be preserved until they are purged. Though, there is one exception to
/// it, back-to-back deletes will collapse into a no-op and only the
/// first delete shall be ingested.
///
/// [llrb]: https://en.wikipedia.org/wiki/Left-leaning_red-black_tree
/// [LSM mode]: https://en.wikipedia.org/wiki/Log-structured_merge-tree
pub struct Llrb<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    name: String,
    lsm: bool,
    spin: bool,

    root: Option<Box<Node<K, V>>>,
    seqno: u64,
    n_count: usize,
    latch: RWSpinlock,
    key_footprint: isize,
    tree_footprint: isize,
    w: Option<LlrbWriter<K, V>>,
}

impl<K, V> Drop for Llrb<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn drop(&mut self) {
        self.root.take().map(drop_tree);
    }
}

pub(crate) struct SquashDebris<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    pub(crate) root: Option<Box<Node<K, V>>>,
    pub(crate) seqno: u64,
    pub(crate) n_count: usize,
    pub(crate) key_footprint: isize,
    pub(crate) tree_footprint: isize,
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
        let mut index = Box::new(Llrb {
            name: name.as_ref().to_string(),
            lsm: false,
            spin: true,
            root: None,
            seqno: Default::default(),
            n_count: Default::default(),
            latch: RWSpinlock::new(),
            key_footprint: Default::default(),
            tree_footprint: Default::default(),
            w: Default::default(),
        });
        let idx = index.as_mut() as *mut Llrb<K, V>;
        index.w = Some(LlrbWriter::new(idx));
        index
    }

    /// Create a new Llrb index in lsm mode. In lsm mode, mutations
    /// are added as log for each key, instead of over-writing previous
    /// mutation. Note that, in case of back-to-back delete, first delete
    /// shall be applied and subsequent deletes shall be ignored.
    pub fn new_lsm<S>(name: S) -> Box<Llrb<K, V>>
    where
        S: AsRef<str>,
    {
        let mut index = Box::new(Llrb {
            name: name.as_ref().to_string(),
            lsm: true,
            spin: true,
            root: None,
            seqno: Default::default(),
            n_count: Default::default(),
            latch: RWSpinlock::new(),
            key_footprint: Default::default(),
            tree_footprint: Default::default(),
            w: Default::default(),
        });
        let idx = index.as_mut() as *mut Llrb<K, V>;
        index.w = Some(LlrbWriter::new(idx));
        index
    }

    pub fn set_spinlatch(&mut self, spin: bool) -> &mut Llrb<K, V> {
        self.spin = spin;
        self
    }

    /// Squash this index and return the root and its book-keeping.
    /// IMPORTANT: after calling this method, value must be dropped.
    pub(crate) fn squash(mut self) -> SquashDebris<K, V> {
        SquashDebris {
            root: self.root.take(),
            seqno: self.seqno,
            n_count: self.n_count,
            key_footprint: self.key_footprint,
            tree_footprint: self.tree_footprint,
        }
    }

    fn shallow_clone(&self) -> Box<Llrb<K, V>> {
        let mut index = Box::new(Llrb {
            name: self.name.clone(),
            lsm: self.lsm,
            spin: self.spin,
            root: None, // this the shallow part
            seqno: Default::default(),
            n_count: Default::default(),
            latch: RWSpinlock::new(),
            key_footprint: Default::default(),
            tree_footprint: Default::default(),
            w: Default::default(),
        });
        let idx = index.as_mut() as *mut Llrb<K, V>;
        index.w = Some(LlrbWriter::new(idx));
        index
    }

    pub fn clone(&self) -> Box<Llrb<K, V>> {
        let mut index = Box::new(Llrb {
            name: self.name.clone(),
            lsm: self.lsm,
            spin: self.spin,
            root: self.root.clone(),
            seqno: self.seqno,
            n_count: self.n_count,
            latch: RWSpinlock::new(),
            key_footprint: self.key_footprint,
            tree_footprint: self.tree_footprint,
            w: Default::default(),
        });
        let idx = index.as_mut() as *mut Llrb<K, V>;
        index.w = Some(LlrbWriter::new(idx));
        index
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
    pub(crate) fn is_lsm(&self) -> bool {
        self.lsm
    }

    /// Return number of entries in this index.
    #[inline]
    pub fn len(&self) -> usize {
        self.n_count
    }

    /// Identify this index. Applications can choose unique names while
    /// creating Llrb indices.
    #[inline]
    pub fn to_name(&self) -> String {
        self.name.clone()
    }

    /// Return current seqno.
    pub fn to_seqno(&self) -> u64 {
        self.seqno
    }

    /// Return quickly with basic statisics, only entries() method is valid
    /// with this statisics.
    pub fn stats(&self) -> Stats {
        Stats::new_partial(self.len(), mem::size_of::<Node<K, V>>())
    }
}

impl<K, V> Index<K, V> for Box<Llrb<K, V>>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    type W = LlrbWriter<K, V>;

    /// Make a new empty index of this type, with same configuration.
    fn make_new(&self) -> Result<Self> {
        Ok(self.shallow_clone())
    }

    /// Create a new writer handle. Only one writer handle can be active at
    /// any time, creating more than one writer handle will panic.
    /// Concurrent readers are allowed but the data-structure is protected
    /// via a spin-lock, that can optionally lock.
    fn to_writer(&mut self) -> Self::W {
        match self.w.take() {
            Some(w) => w,
            None => panic!("writer not initialized"),
        }
    }
}

impl<K, V> Footprint for Box<Llrb<K, V>>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn footprint(&self) -> isize {
        self.tree_footprint
    }
}

/// Create/Update/Delete operations on Llrb index.
impl<K, V> Llrb<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    /// Set {key, value} pair into index. If key is already
    /// present, update the value and return the previous entry, else
    /// create a new entry.
    ///
    /// *LSM mode*: Add a new version for the key, perserving the old value.
    pub fn set(&mut self, key: K, value: V) -> Result<Option<Entry<K, V>>> {
        match &mut self.w {
            Some(w) => {
                let (_seqno, entry) = w.set_index(key, value, self.seqno + 1);
                entry
            }
            None => panic!("already given a writer_handle for this index"),
        }
    }

    /// Similar to set, but succeeds only when CAS matches with entry's
    /// last `seqno`. In other words, since seqno is unique to each mutation,
    /// we use `seqno` of the mutation as the CAS value. Use CAS == 0 to
    /// enforce a create operation.
    ///
    /// *LSM mode*: Add a new version for the key, perserving the old value.
    pub fn set_cas(&mut self, key: K, value: V, cas: u64) -> Result<Option<Entry<K, V>>> {
        match &mut self.w {
            Some(w) => {
                let seqno = self.seqno + 1;
                let (_seqno, entry) = w.set_cas_index(key, value, cas, seqno);
                entry
            }
            None => panic!("already given a writer_handle for this index"),
        }
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
    pub fn delete<Q>(&mut self, key: &Q) -> Result<Option<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        match &mut self.w {
            Some(w) => {
                let (_seqno, entry) = w.delete_index(key, self.seqno + 1);
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
    old_entry: Option<Entry<K, V>>,
    size: isize, // differencen in footprint
}

struct UpsertCasResult<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    node: Option<Box<Node<K, V>>>,
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
    old_entry: Option<Entry<K, V>>,
    size: isize, // difference in footprint
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
    ) -> UpsertResult<K, V> {
        match node {
            None => {
                let mut node: Box<Node<K, V>> = Box::new(From::from(nentry));
                node.dirty = false;
                let size: isize = node.footprint().try_into().unwrap();
                return UpsertResult {
                    node: Some(node),
                    old_entry: None,
                    size,
                };
            }
            Some(mut node) => {
                node = Llrb::walkdown_rot23(node);
                match node.as_key().cmp(nentry.as_key()) {
                    Ordering::Greater => {
                        let mut r = Llrb::upsert(node.left.take(), nentry, lsm);
                        node.left = r.node;
                        r.node = Some(Llrb::walkuprot_23(node));
                        r
                    }
                    Ordering::Less => {
                        let mut r = Llrb::upsert(node.right.take(), nentry, lsm);
                        node.right = r.node;
                        r.node = Some(Llrb::walkuprot_23(node));
                        r
                    }
                    Ordering::Equal => {
                        let old_entry = Some(node.entry.clone());
                        let size = node.prepend_version(nentry, lsm);
                        UpsertResult {
                            node: Some(Llrb::walkuprot_23(node)),
                            old_entry,
                            size,
                        }
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
    ) -> UpsertCasResult<K, V> {
        let mut node = match node {
            None if cas > 0 => {
                return UpsertCasResult {
                    node: None,
                    old_entry: None,
                    size: 0,
                    err: Some(Error::InvalidCAS),
                };
            }
            None => {
                let mut node: Box<Node<K, V>> = Box::new(From::from(nentry));
                node.dirty = false;
                let size: isize = node.footprint().try_into().unwrap();
                return UpsertCasResult {
                    node: Some(node),
                    old_entry: None,
                    size,
                    err: None,
                };
            }
            Some(node) => node,
        };

        node = Llrb::walkdown_rot23(node);
        match node.as_key().cmp(nentry.as_key()) {
            Ordering::Greater => {
                let left = node.left.take();
                let mut r = Llrb::upsert_cas(left, nentry, cas, lsm);
                node.left = r.node;
                r.node = Some(Llrb::walkuprot_23(node));
                r
            }
            Ordering::Less => {
                let right = node.right.take();
                let mut r = Llrb::upsert_cas(right, nentry, cas, lsm);
                node.right = r.node;
                r.node = Some(Llrb::walkuprot_23(node));
                r
            }
            Ordering::Equal => {
                let p = node.is_deleted() && cas != 0 && cas != node.to_seqno();
                let p = p || (!node.is_deleted() && cas != node.to_seqno());
                if p {
                    UpsertCasResult {
                        node: Some(Llrb::walkuprot_23(node)),
                        old_entry: None,
                        size: 0,
                        err: Some(Error::InvalidCAS),
                    }
                } else {
                    let old_entry = Some(node.entry.clone());
                    let size = node.prepend_version(nentry, lsm);
                    UpsertCasResult {
                        node: Some(Llrb::walkuprot_23(node)),
                        old_entry,
                        size,
                        err: None,
                    }
                }
            }
        }
    }

    fn delete_lsm<Q>(
        node: Option<Box<Node<K, V>>>,
        key: &Q,
        seqno: u64, // seqno for this mutation
    ) -> DeleteResult<K, V>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        match node {
            None => {
                // insert and mark as delete
                let mut node = Node::new_deleted(key.to_owned(), seqno);
                node.dirty = false;
                let size: isize = node.footprint().try_into().unwrap();
                DeleteResult {
                    node: Some(node),
                    old_entry: None,
                    size,
                }
            }
            Some(mut node) => {
                node = Llrb::walkdown_rot23(node);
                match node.as_key().borrow().cmp(&key) {
                    Ordering::Greater => {
                        let left = node.left.take();
                        let mut r = Llrb::delete_lsm(left, key, seqno);
                        node.left = r.node;
                        r.node = Some(Llrb::walkuprot_23(node));
                        r
                    }
                    Ordering::Less => {
                        let right = node.right.take();
                        let mut r = Llrb::delete_lsm(right, key, seqno);
                        node.right = r.node;
                        r.node = Some(Llrb::walkuprot_23(node));
                        r
                    }
                    Ordering::Equal => {
                        let entry = node.entry.clone();
                        let size = if !node.is_deleted() {
                            node.delete(seqno)
                        } else {
                            0
                        };
                        DeleteResult {
                            node: Some(Llrb::walkuprot_23(node)),
                            old_entry: Some(entry),
                            size,
                        }
                    }
                }
            }
        }
    }

    // this is the non-lsm path.
    fn do_delete<Q>(node: Option<Box<Node<K, V>>>, key: &Q) -> DeleteResult<K, V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut node = match node {
            None => {
                return DeleteResult {
                    node: None,
                    old_entry: None,
                    size: 0,
                }
            }
            Some(node) => node,
        };

        if node.as_key().borrow().gt(key) {
            if node.left.is_none() {
                DeleteResult {
                    node: Some(node),
                    old_entry: None,
                    size: 0,
                }
            } else {
                let ok = !is_red(node.as_left_deref());
                if ok && !is_red(node.left.as_ref().unwrap().as_left_deref()) {
                    node = Llrb::move_red_left(node);
                }
                let mut r = Llrb::do_delete(node.left.take(), key);
                node.left = r.node;
                r.node = Some(Llrb::fixup(node));
                r
            }
        } else {
            if is_red(node.as_left_deref()) {
                node = Llrb::rotate_right(node);
            }

            if !node.as_key().borrow().lt(key) && node.right.is_none() {
                return DeleteResult {
                    node: None,
                    old_entry: Some(node.entry.clone()),
                    size: node.footprint(),
                };
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
                let size: isize = node.footprint().try_into().unwrap();
                DeleteResult {
                    node: Some(Llrb::fixup(newnode)),
                    old_entry: Some(node.entry.clone()),
                    size,
                }
            } else {
                let mut r = Llrb::do_delete(node.right.take(), key);
                node.right = r.node;
                r.node = Some(Llrb::fixup(node));
                r
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

/// Read operations on Llrb index.
impl<K, V> Reader<K, V> for Llrb<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Get the entry for `key`.
    fn get<Q>(&self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let _latch = self.latch.acquire_read(self.spin);
        get(self.root.as_ref().map(Deref::deref), key)
    }

    /// Return an iterator over all entries in this index.
    fn iter(&self) -> Result<IndexIter<K, V>> {
        let node = self.root.as_ref().map(Deref::deref);
        Ok(Box::new(Iter {
            _latch: Some(self.latch.acquire_read(self.spin)),
            _arc: Default::default(),
            paths: Some(build_iter(IFlag::Left, node, vec![])),
        }))
    }

    /// Range over all entries from low to high.
    fn range<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let root = self.root.as_ref().map(Deref::deref);
        let paths = match range.start_bound() {
            Bound::Unbounded => Some(build_iter(IFlag::Left, root, vec![])),
            Bound::Included(low) => Some(find_start(root, low, true, vec![])),
            Bound::Excluded(low) => Some(find_start(root, low, false, vec![])),
        };
        Ok(Box::new(Range {
            _latch: Some(self.latch.acquire_read(self.spin)),
            _arc: Default::default(),
            range,
            paths,
            high: marker::PhantomData,
        }))
    }

    /// Reverse range over all entries from high to low.
    fn reverse<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let root = self.root.as_ref().map(Deref::deref);
        let paths = match range.end_bound() {
            Bound::Unbounded => Some(build_iter(IFlag::Right, root, vec![])),
            Bound::Included(high) => Some(find_end(root, high, true, vec![])),
            Bound::Excluded(high) => Some(find_end(root, high, false, vec![])),
        };
        let low = marker::PhantomData;
        Ok(Box::new(Reverse {
            _latch: Some(self.latch.acquire_read(self.spin)),
            _arc: Default::default(),
            range,
            paths,
            low,
        }))
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
    fn range_with_versions<'a, R, Q>(&'a self, rng: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        self.range(rng)
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

impl<K, V> FullScan<K, V> for Box<Llrb<K, V>>
where
    K: Clone + Ord,
    V: Clone + Diff + From<<V as Diff>::D>,
{
    /// Return an iterator over entries that meet following properties
    /// * Only entries greater than from bound,
    /// * Only entries whose modified seqno is within seqno-range.
    fn full_scan<G>(&self, from: Bound<K>, prd: G) -> Result<IndexIter<K, V>>
    where
        G: Clone + RangeBounds<u64>,
    {
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
        let start = match prd.start_bound() {
            Bound::Included(x) => Bound::Included(*x),
            Bound::Excluded(x) => Bound::Excluded(*x),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end = match prd.end_bound() {
            Bound::Included(x) => Bound::Included(*x),
            Bound::Excluded(x) => Bound::Excluded(*x),
            Bound::Unbounded => Bound::Unbounded,
        };
        Ok(Box::new(IterFullScan {
            _latch: Some(self.latch.acquire_read(self.spin)),
            _arc: Default::default(),
            start,
            end,
            paths,
        }))
    }
}

/// Deep walk validate of Llrb index.
impl<K, V> Llrb<K, V>
where
    K: Clone + Ord + Debug,
    V: Clone + Diff,
{
    /// Validate LLRB tree with following rules:
    ///
    /// * From root to any leaf, no consecutive reds allowed in its path.
    /// * Number of blacks should be same under left child and right child.
    /// * Make sure that keys are in sort order.
    ///
    /// Additionally return full statistics on the tree. Refer to [`Stats`]
    /// for more information.
    pub fn validate(&self) -> Result<Stats> {
        let _latch = self.latch.acquire_read(self.spin);

        let root = self.root.as_ref().map(Deref::deref);
        let (red, blacks, depth) = (is_red(root), 0, 0);
        let mut depths: LlrbDepth = Default::default();
        let blacks = validate_tree(root, red, blacks, depth, &mut depths)?;

        Ok(Stats::new_full(
            self.n_count,
            std::mem::size_of::<Node<K, V>>(),
            blacks,
            depths,
        ))
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

/// Writer handle for Llrb.
pub struct LlrbWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    index: Option<*mut Llrb<K, V>>,
}

impl<K, V> Drop for LlrbWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn drop(&mut self) {
        // NOTE: forget the writer, which is a self-reference.
    }
}

impl<K, V> LlrbWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn new(index: *mut Llrb<K, V>) -> LlrbWriter<K, V> {
        LlrbWriter { index: Some(index) }
    }
}

impl<K, V> LlrbWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn get_index(&mut self) -> &mut Llrb<K, V> {
        match &mut self.index {
            Some(index) => unsafe { index.as_mut().unwrap() },
            None => unreachable!(),
        }
    }
}

/// Create/Update/Delete operations on Llrb index.
impl<K, V> Writer<K, V> for LlrbWriter<K, V>
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
        let index = self.get_index();
        let _latch = index.latch.acquire_write(index.spin);

        let key_footprint = key.footprint();
        let new_entry = {
            let value = Box::new(Value::new_upsert_value(value, seqno));
            Entry::new(key, value)
        };
        // !("set_index, root {}", index.root.is_some());
        match Llrb::upsert(index.root.take(), new_entry, index.lsm) {
            UpsertResult {
                node: Some(mut root),
                old_entry,
                size,
            } => {
                // println!("set_index, result {}", size);
                root.set_black();
                index.root = Some(root);
                if old_entry.is_none() {
                    index.n_count += 1;
                    index.key_footprint += key_footprint;
                }
                index.seqno = seqno;
                index.tree_footprint += size;
                (Some(seqno), Ok(old_entry))
            }
            _ => panic!("set: impossible case, call programmer"),
        }
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
        let index = self.get_index();
        let _latch = index.latch.acquire_write(index.spin);

        let key_footprint = key.footprint();
        let new_entry = {
            let value = Box::new(Value::new_upsert_value(value, seqno));
            Entry::new(key, value)
        };
        match Llrb::upsert_cas(index.root.take(), new_entry, cas, index.lsm) {
            UpsertCasResult {
                node: root,
                err: Some(err),
                ..
            } => {
                index.root = root;
                (None, Err(err))
            }
            UpsertCasResult {
                node: Some(mut root),
                old_entry,
                size,
                err: None,
            } => {
                root.set_black();
                index.root = Some(root);
                if old_entry.is_none() {
                    index.n_count += 1;
                    index.key_footprint += key_footprint;
                }
                index.seqno = seqno;
                index.tree_footprint += size;
                (Some(seqno), Ok(old_entry))
            }
            _ => panic!("set_cas: impossible case, call programmer"),
        }
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
        let index = self.get_index();
        let _latch = index.latch.acquire_write(index.spin);

        let key_footprint = key.to_owned().footprint();

        if index.lsm {
            let res = Llrb::delete_lsm(index.root.take(), key, seqno);
            index.root = res.node;
            index.root.as_mut().map(|r| r.set_black());

            return match res.old_entry {
                None => {
                    index.key_footprint += key_footprint;
                    index.tree_footprint += res.size;

                    index.n_count += 1;
                    index.seqno = seqno;
                    (Some(seqno), Ok(None))
                }
                Some(entry) if !entry.is_deleted() => {
                    index.tree_footprint += res.size;

                    index.seqno = seqno;
                    (Some(seqno), Ok(Some(entry)))
                }
                entry => (None, Ok(entry)),
            };
        } else {
            // in non-lsm mode remove the entry from the tree.
            let res = match Llrb::do_delete(index.root.take(), key) {
                res @ DeleteResult { node: None, .. } => res,
                mut res => {
                    res.node.as_mut().map(|node| node.set_black());
                    res
                }
            };
            index.root = res.node;
            if res.old_entry.is_some() {
                index.key_footprint -= key_footprint;
                index.tree_footprint += res.size;

                index.n_count -= 1;
                index.seqno = seqno;
                (Some(seqno), Ok(res.old_entry))
            } else {
                (None, Ok(res.old_entry))
            }
        }
    }
}

#[cfg(test)]
#[path = "llrb_test.rs"]
mod llrb_test;
