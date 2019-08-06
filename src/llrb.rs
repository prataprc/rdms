//! Single threaded, in-memory index using [left-leaning-red-black][llrb] tree.
//!
//! [llrb]: https://en.wikipedia.org/wiki/Left-leaning_red-black_tree
use std::borrow::Borrow;
use std::cmp::{Ord, Ordering};
use std::fmt::Debug;
use std::ops::{Bound, Deref, DerefMut, RangeBounds};
use std::sync::Arc;
use std::{cmp, marker, mem};

use crate::core::{Diff, Entry, Result, Value};
use crate::core::{FullScan, Index, IndexIter, Reader, Writer};
use crate::error::Error;
use crate::llrb_node::{LlrbDepth, Node, Stats};
use crate::mvcc::MvccRoot;

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
    root: Option<Box<Node<K, V>>>,
    seqno: u64,
    n_count: usize,
}

impl<K, V> Clone for Llrb<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn clone(&self) -> Llrb<K, V> {
        Llrb {
            name: self.name.clone(),
            lsm: self.lsm,
            root: self.root.clone(),
            seqno: self.seqno,
            n_count: self.n_count,
        }
    }
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

/// Different ways to construct a new Llrb index.
impl<K, V> Llrb<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Create an empty Llrb index, identified by `name`.
    /// Applications can choose unique names.
    pub fn new<S: AsRef<str>>(name: S) -> Llrb<K, V> {
        Llrb {
            name: name.as_ref().to_string(),
            lsm: false,
            root: None,
            seqno: 0,
            n_count: 0,
        }
    }

    /// Create a new Llrb index in lsm mode. In lsm mode, mutations
    /// are added as log for each key, instead of over-writing previous
    /// mutation. Note that, in case of back-to-back delete, first delete
    /// shall be applied and subsequent deletes shall be ignored.
    pub fn new_lsm<S>(name: S) -> Llrb<K, V>
    where
        S: AsRef<str>,
    {
        Llrb {
            name: name.as_ref().to_string(),
            lsm: true,
            root: None,
            seqno: 0,
            n_count: 0,
        }
    }

    /// Squash this index and return the root and its book-keeping.
    /// IMPORTANT: after calling this method, value must be dropped.
    pub(crate) fn squash(&mut self) -> (Option<Box<Node<K, V>>>, u64, usize) {
        (self.root.take(), self.seqno, self.n_count)
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

    /// Set current seqno. Use this API iff you are totaly sure
    /// about what you are doing.
    #[inline]
    #[allow(dead_code)] // TODO: remove this once bogn is weaved-up.
    pub(crate) fn set_seqno(&mut self, seqno: u64) {
        self.seqno = seqno
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
    #[inline]
    pub fn to_seqno(&self) -> u64 {
        self.seqno
    }

    /// Return quickly with basic statisics, only entries() method is valid
    /// with this statisics.
    pub fn stats(&self) -> Stats {
        Stats::new_partial(self.len(), mem::size_of::<Node<K, V>>())
    }
}

impl<K, V> Index<K, V> for Llrb<K, V>
where
    K: Clone + Ord,
    V: Clone + From<<V as Diff>::D> + Diff,
{
    // Make a new empty index of this type, with same configuration.
    fn make_new(&self) -> Self {
        if self.lsm {
            Llrb::new_lsm(&self.name)
        } else {
            Llrb::new(&self.name)
        }
    }
}

/// Create/Update/Delete operations on Llrb index.
impl<K, V> Llrb<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Set {key, value} pair into index. If key is already
    /// present, update the value and return the previous entry, else
    /// create a new entry.
    ///
    /// *LSM mode*: Add a new version for the key, perserving the old value.
    pub fn set(&mut self, key: K, value: V) -> Result<Option<Entry<K, V>>> {
        let res = self.set_index(key, value, self.seqno + 1);
        self.seqno += 1;
        res
    }

    /// Similar to set, but succeeds only when CAS matches with entry's
    /// last `seqno`. In other words, since seqno is unique to each mutation,
    /// we use `seqno` of the mutation as the CAS value. Use CAS == 0 to
    /// enforce a create operation.
    ///
    /// *LSM mode*: Add a new version for the key, perserving the old value.
    pub fn set_cas(&mut self, key: K, value: V, cas: u64) -> Result<Option<Entry<K, V>>> {
        let (seqno, res) = self.set_cas_index(key, value, cas, self.seqno + 1);
        self.seqno = cmp::max(seqno, self.seqno);
        res
    }

    /// Delete the given key. Note that back-to-back delete for the same
    /// key shall collapse into a single delete, first delete is ingested
    /// while the rest are ignored.
    ///
    /// *LSM mode*: Mark the entry as deleted along with seqno at which it
    /// deleted
    ///
    /// NOTE: K should be borrowable as &Q and Q must be converted to owned K.
    /// This is require in lsm mode, where owned K must be inserted into the
    /// tree.
    pub fn delete<Q>(&mut self, key: &Q) -> Result<Option<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        let (seqno, res) = self.delete_index(key, self.seqno + 1);
        self.seqno = cmp::max(seqno, self.seqno);
        res
    }
}

/// Create/Update/Delete operations on Llrb index.
impl<K, V> Writer<K, V> for Llrb<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn set_index(&mut self, key: K, value: V, seqno: u64) -> Result<Option<Entry<K, V>>> {
        let value = Box::new(Value::new_upsert_value(value, seqno));
        let new_entry = Entry::new(key, value);
        match Llrb::upsert(self.root.take(), new_entry, self.lsm) {
            (Some(mut root), entry) => {
                root.set_black();
                self.root = Some(root);
                if entry.is_none() {
                    self.n_count += 1;
                }
                Ok(entry)
            }
            _ => panic!("set: impossible case, call programmer"),
        }
    }

    /// Similar to set, but succeeds only when CAS matches with entry's
    /// last `seqno`. In other words, since seqno is unique to each mutation,
    /// we use `seqno` of the mutation as the CAS value. Use CAS == 0 to
    /// enforce a create operation.
    ///
    /// *LSM mode*: Add a new version for the key, perserving the old value.
    fn set_cas_index(
        &mut self,
        key: K,
        value: V,
        cas: u64,
        seqno: u64,
    ) -> (u64, Result<Option<Entry<K, V>>>) {
        let value = Box::new(Value::new_upsert_value(value, seqno));
        let new_entry = Entry::new(key, value);
        match Llrb::upsert_cas(self.root.take(), new_entry, cas, self.lsm) {
            (root, _, Some(err)) => {
                self.root = root;
                (0, Err(err))
            }
            (Some(mut root), entry, None) => {
                root.set_black();
                self.root = Some(root);
                if entry.is_none() {
                    self.n_count += 1;
                }
                (seqno, Ok(entry))
            }
            _ => panic!("set_cas: impossible case, call programmer"),
        }
    }

    fn delete_index<Q>(
        &mut self,
        key: &Q,
        seqno: u64, // seqno for this delete
    ) -> (u64, Result<Option<Entry<K, V>>>)
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        if self.lsm {
            let (root, entry) = Llrb::delete_lsm(self.root.take(), key, seqno);
            self.root = root;
            self.root.as_mut().map(|r| r.set_black());

            return match entry {
                None => {
                    self.n_count += 1;
                    (seqno, Ok(None))
                }
                Some(entry) if !entry.is_deleted() => (seqno, Ok(Some(entry))),
                entry => (0, Ok(entry)),
            };
        } else {
            // in non-lsm mode remove the entry from the tree.
            let (root, entry) = match Llrb::do_delete(self.root.take(), key) {
                (None, entry) => (None, entry),
                (Some(mut root), entry) => {
                    root.set_black();
                    (Some(root), entry)
                }
            };
            self.root = root;
            if entry.is_some() {
                self.n_count -= 1;
                (seqno, Ok(entry))
            } else {
                (0, Ok(entry))
            }
        }
    }
}

/// Create/Update/Delete operations on Llrb index.
impl<K, V> Llrb<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn upsert(
        node: Option<Box<Node<K, V>>>,
        nentry: Entry<K, V>,
        lsm: bool,
    ) -> (Option<Box<Node<K, V>>>, Option<Entry<K, V>>) {
        match node {
            None => {
                let mut node: Box<Node<K, V>> = Box::new(From::from(nentry));
                node.dirty = false;
                return (Some(node), None);
            }
            Some(mut node) => {
                node = Llrb::walkdown_rot23(node);
                match node.as_key().cmp(nentry.as_key()) {
                    Ordering::Greater => {
                        let res = Llrb::upsert(node.left.take(), nentry, lsm);
                        let (left, entry) = res;
                        node.left = left;
                        (Some(Llrb::walkuprot_23(node)), entry)
                    }
                    Ordering::Less => {
                        let res = Llrb::upsert(node.right.take(), nentry, lsm);
                        let (right, entry) = res;
                        node.right = right;
                        (Some(Llrb::walkuprot_23(node)), entry)
                    }
                    Ordering::Equal => {
                        let entry = node.entry.clone();
                        node.prepend_version(nentry, lsm);
                        (Some(Llrb::walkuprot_23(node)), Some(entry))
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
    ) -> (Option<Box<Node<K, V>>>, Option<Entry<K, V>>, Option<Error>) {
        let mut node = match node {
            None if cas > 0 => {
                return (None, None, Some(Error::InvalidCAS));
            }
            None => {
                let mut node: Box<Node<K, V>> = Box::new(From::from(nentry));
                node.dirty = false;
                return (Some(node), None, None);
            }
            Some(node) => node,
        };

        node = Llrb::walkdown_rot23(node);
        let (entry, err) = match node.as_key().cmp(nentry.as_key()) {
            Ordering::Greater => {
                let left = node.left.take();
                let (l, en, e) = Llrb::upsert_cas(left, nentry, cas, lsm);
                node.left = l;
                (en, e)
            }
            Ordering::Less => {
                let rt = node.right.take();
                let (r, en, e) = Llrb::upsert_cas(rt, nentry, cas, lsm);
                node.right = r;
                (en, e)
            }
            Ordering::Equal => {
                if node.is_deleted() && cas != 0 && cas != node.to_seqno() {
                    (None, Some(Error::InvalidCAS))
                } else if !node.is_deleted() && cas != node.to_seqno() {
                    (None, Some(Error::InvalidCAS))
                } else {
                    let entry = node.entry.clone();
                    node.prepend_version(nentry, lsm);
                    (Some(entry), None)
                }
            }
        };
        node = Llrb::walkuprot_23(node);
        return (Some(node), entry, err);
    }

    fn delete_lsm<Q>(
        node: Option<Box<Node<K, V>>>,
        key: &Q,
        seqno: u64,
    ) -> (Option<Box<Node<K, V>>>, Option<Entry<K, V>>)
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        match node {
            None => {
                // insert and mark as delete
                let mut node = Node::new_deleted(key.to_owned(), seqno);
                node.dirty = false;
                (Some(node), None)
            }
            Some(mut node) => {
                node = Llrb::walkdown_rot23(node);
                match node.as_key().borrow().cmp(&key) {
                    Ordering::Greater => {
                        let left = node.left.take();
                        let (left, entry) = Llrb::delete_lsm(left, key, seqno);
                        node.left = left;
                        (Some(Llrb::walkuprot_23(node)), entry)
                    }
                    Ordering::Less => {
                        let right = node.right.take();
                        let (right, entry) = Llrb::delete_lsm(right, key, seqno);
                        node.right = right;
                        (Some(Llrb::walkuprot_23(node)), entry)
                    }
                    Ordering::Equal => {
                        let entry = node.entry.clone();
                        if !node.is_deleted() {
                            node.delete(seqno);
                        }
                        (Some(Llrb::walkuprot_23(node)), Some(entry))
                    }
                }
            }
        }
    }

    // this is the non-lsm path.
    fn do_delete<Q>(
        node: Option<Box<Node<K, V>>>,
        key: &Q,
    ) -> (Option<Box<Node<K, V>>>, Option<Entry<K, V>>)
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut node = match node {
            None => return (None, None),
            Some(node) => node,
        };

        if node.as_key().borrow().gt(key) {
            if node.left.is_none() {
                (Some(node), None)
            } else {
                let ok = !is_red(node.as_left_deref());
                if ok && !is_red(node.left.as_ref().unwrap().as_left_deref()) {
                    node = Llrb::move_red_left(node);
                }
                let (left, entry) = Llrb::do_delete(node.left.take(), key);
                node.left = left;
                (Some(Llrb::fixup(node)), entry)
            }
        } else {
            if is_red(node.as_left_deref()) {
                node = Llrb::rotate_right(node);
            }

            if !node.as_key().borrow().lt(key) && node.right.is_none() {
                return (None, Some(node.entry.clone()));
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
                (Some(Llrb::fixup(newnode)), Some(node.entry.clone()))
            } else {
                let (right, entry) = Llrb::do_delete(node.right.take(), key);
                node.right = right;
                (Some(Llrb::fixup(node)), entry)
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
    V: Clone + Diff + From<<V as Diff>::D>,
{
    /// Get the entry for `key`.
    fn get<Q>(&self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        get(self.root.as_ref().map(Deref::deref), key)
    }

    /// Return an iterator over all entries in this index.
    fn iter(&self) -> Result<IndexIter<K, V>> {
        let node = self.root.as_ref().map(Deref::deref);
        Ok(Box::new(Iter {
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

impl<K, V> FullScan<K, V> for Llrb<K, V>
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
    K: Clone + Ord,
    V: Clone + Diff,
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
