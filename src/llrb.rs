use std::borrow::Borrow;
use std::cmp::{Ord, Ordering};
use std::ops::{Bound, Deref, DerefMut};

use crate::error::BognError;
use crate::llrb_common::{self, drop_tree, is_black, is_red, Iter, Range, Stats};
use crate::llrb_depth::Depth;
use crate::llrb_node::Node;
use crate::traits::{AsEntry, AsKey};

// TODO: optimize comparison
// TODO: Remove AtomicPtr and test/benchmark.
// TODO: Remove RwLock and use AtomicPtr and latch mechanism, test/benchmark.
// TODO: Remove Mutex and check write performance.

/// Llrb manage a single instance of in-memory index using
/// [left-leaning-red-black][llrb] tree.
///
/// **[LSM mode]**: Llrb instance can support what is called as
/// log-structured-merge while mutating the tree. In simple terms, this
/// means that nothing shall be over-written in the tree and all the
/// mutations for the same key shall be preserved until they are undone or
/// purged. Although there is one exception to it, back-to-back deletes
/// will collapse into a no-op and only the first delete shall be ingested.
///
/// [llrb]: https://en.wikipedia.org/wiki/Left-leaning_red-black_tree
/// [LSM mode]: https://en.wikipedia.org/wiki/Log-structured_merge-tree
pub struct Llrb<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    pub(crate) name: String,
    pub(crate) lsm: bool,
    pub(crate) root: Option<Box<Node<K, V>>>,
    pub(crate) seqno: u64,     // starts from 0 and incr for every mutation.
    pub(crate) n_count: usize, // number of entries in the tree.
}

impl<K, V> Drop for Llrb<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    fn drop(&mut self) {
        self.root.take().map(|node| drop_tree(node));
    }
}

impl<K, V> Clone for Llrb<K, V>
where
    K: AsKey,
    V: Default + Clone,
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

/// Different ways to construct a new Llrb instance.
impl<K, V> Llrb<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    /// Create an empty instance of Llrb, identified by `name`.
    /// Applications can choose unique names. When `lsm` is true, mutations
    /// are added as log for each key, instead of over-writing previous
    /// mutation.
    pub fn new<S>(name: S, lsm: bool) -> Llrb<K, V>
    where
        S: AsRef<str>,
    {
        Llrb {
            name: name.as_ref().to_string(),
            lsm,
            root: None,
            seqno: 0,
            n_count: 0,
        }
    }

    /// Create a new instance of Llrb tree and load it with entries from
    /// `iter`. Note that iterator shall return items that implement
    /// [AsEntry].
    pub fn load_from<E, S>(
        name: S,
        iter: impl Iterator<Item = E>,
        lsm: bool,
    ) -> Result<Llrb<K, V>, BognError>
    where
        S: AsRef<str>,
        E: AsEntry<K, V>,
        <E as AsEntry<K, V>>::Value: Default + Clone,
    {
        let mut llrb = Llrb::new(name.as_ref().to_string(), lsm);
        for entry in iter {
            llrb.seqno = std::cmp::max(llrb.seqno, entry.seqno());
            let mut node = Llrb::load_entry(llrb.root.take(), entry)?;
            node.set_black();
            llrb.root = Some(node);
            llrb.n_count += 1;
        }
        Ok(llrb)
    }

    fn load_entry<E>(node: Option<Box<Node<K, V>>>, entry: E) -> Result<Box<Node<K, V>>, BognError>
    where
        E: AsEntry<K, V>,
        <E as AsEntry<K, V>>::Value: Default + Clone,
    {
        if node.is_none() {
            return Ok(Node::from_entry(entry));
        }

        let (mut node, key) = (node.unwrap(), entry.key_ref());
        node = Llrb::walkdown_rot23(node);
        match node.key.cmp(key) {
            Ordering::Greater => {
                node.left = Some(Llrb::load_entry(node.left.take(), entry)?);
                Ok(Llrb::walkuprot_23(node))
            }
            Ordering::Less => {
                node.right = Some(Llrb::load_entry(node.right.take(), entry)?);
                Ok(Llrb::walkuprot_23(node))
            }
            Ordering::Equal => {
                let err = format!("load_entry: {:?}", key);
                Err(BognError::DuplicateKey(err))
            }
        }
    }
}

/// Maintanence API.
impl<K, V> Llrb<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    /// Identify this instance. Applications can choose unique names while
    /// creating Llrb instances.
    pub fn id(&self) -> String {
        self.name.clone()
    }

    /// Return number of entries in this instance.
    pub fn len(&self) -> usize {
        self.n_count
    }

    /// Set current seqno.
    pub fn set_seqno(&mut self, seqno: u64) {
        self.seqno = seqno
    }

    /// Return current seqno.
    pub fn get_seqno(&self) -> u64 {
        self.seqno
    }
}

/// CRUD operations on Llrb instance.
impl<K, V> Llrb<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    /// Get the latest version for key.
    pub fn get<Q>(&self, key: &Q) -> Option<impl AsEntry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let root = self.root.as_ref().map(|item| item.deref());
        llrb_common::get(root, key)
    }

    /// Return an iterator over all entries in this instance.
    pub fn iter(&self) -> Iter<K, V> {
        Iter {
            arc: Default::default(),
            root: self.root.as_ref().map(|item| item.deref()),
            node_iter: vec![].into_iter(),
            after_key: Bound::Unbounded,
            limit: 100,
            fin: false,
        }
    }

    /// Range over all entries from low to high.
    pub fn range(&self, low: Bound<K>, high: Bound<K>) -> Range<K, V> {
        Range {
            arc: Default::default(),
            root: self.root.as_ref().map(|item| item.deref()),
            node_iter: vec![].into_iter(),
            low,
            high,
            limit: 100, // TODO: no magic number.
            fin: false,
        }
    }

    /// Set operation for non-mvcc instance. If key is already
    /// present, return the previous entry. In LSM mode, this will
    /// add a new version for the key.
    ///
    /// If an entry already exist for the, return the old-entry will all its
    /// versions.
    pub fn set(&mut self, key: K, value: V) -> Option<impl AsEntry<K, V>> {
        let seqno = self.seqno + 1;
        let root = self.root.take();

        match Llrb::upsert(root, key, value, seqno, self.lsm) {
            (Some(mut root), old_node) => {
                root.set_black();
                self.root = Some(root);
                if old_node.is_none() {
                    self.n_count += 1;
                }
                self.seqno = seqno;
                old_node
            }
            (None, _old_node) => unreachable!(),
        }
    }

    /// Set a new entry into a non-mvcc instance, only if entry's seqno matches
    /// the supplied CAS. Use CAS == 0 to enforce a create operation. If key is
    /// already present, return the previous entry. In LSM mode, this will add
    /// a new version for the key.
    pub fn set_cas(
        &mut self,
        key: K,
        value: V,
        cas: u64,
    ) -> Result<Option<impl AsEntry<K, V>>, BognError> {
        let seqno = self.seqno + 1;
        let root = self.root.take();

        match Llrb::upsert_cas(root, key, value, cas, seqno, self.lsm) {
            (root, _, Some(err)) => {
                self.root = root;
                Err(err)
            }
            (Some(mut root), old_node, None) => {
                root.set_black();
                self.seqno = seqno;
                self.root = Some(root);
                if old_node.is_none() {
                    self.n_count += 1;
                }
                Ok(old_node)
            }
            _ => panic!("set_cas: impossible case, call programmer"),
        }
    }

    /// Delete the given key from non-mvcc intance, in LSM mode it simply marks
    /// the version as deleted. Note that back-to-back delete for the same
    /// key shall collapse into a single delete.
    pub fn delete<Q>(&mut self, key: &Q) -> Option<impl AsEntry<K, V>>
    where
        // TODO: From<Q> and Clone will fail if V=String and Q=str
        K: Borrow<Q> + From<Q>,
        Q: Clone + Ord + ?Sized,
    {
        let seqno = self.seqno + 1;

        if self.lsm {
            let root = self.root.take();
            let (root, old_node) = Llrb::delete_lsm(root, key, seqno);
            let mut root = root.unwrap();
            root.set_black();
            self.root = Some(root);

            if old_node.is_none() {
                self.n_count += 1;
                self.seqno = seqno;
            } else if !old_node.as_ref().unwrap().is_deleted() {
                self.seqno = seqno;
            }
            return old_node;
        }

        // in non-lsm mode remove the entry from the tree.
        let root = self.root.take();
        let (root, old_node) = match Llrb::do_delete(root, key) {
            (None, old_node) => (None, old_node),
            (Some(mut root), old_node) => {
                root.set_black();
                (Some(root), old_node)
            }
        };
        self.root = root;
        if old_node.is_some() {
            self.n_count -= 1;
            self.seqno = seqno;
        }
        old_node
    }

    /// Validate LLRB tree with following rules:
    ///
    /// * From root to any leaf, no consecutive reds allowed in its path.
    /// * Number of blacks should be same on under left child and right child.
    /// * Make sure that keys are in sorted order.
    ///
    /// Additionally return full statistics on the tree. Refer to [`Stats`]
    /// for more information.
    pub fn validate(&self) -> Result<Stats, BognError> {
        let node_size = std::mem::size_of::<Node<K, V>>();
        let mut stats = Stats::new(self.n_count, node_size);
        stats.set_depths(Depth::new());

        let root = self.root.as_ref().map(std::ops::Deref::deref);
        let (red, nb, d) = (is_red(root), 0, 0);
        let blacks = llrb_common::validate_tree(root, red, nb, d, &mut stats)?;
        stats.set_blacks(blacks);
        Ok(stats)
    }
}

impl<K, V> Llrb<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    fn upsert(
        node: Option<Box<Node<K, V>>>,
        key: K,
        value: V,
        seqno: u64,
        lsm: bool,
    ) -> (Option<Box<Node<K, V>>>, Option<Node<K, V>>) {
        if node.is_none() {
            let black = false;
            let mut node = Node::new(key, value, seqno, black);
            node.dirty = false;
            return (Some(node), None);
        }

        let mut node = node.unwrap();
        node = Llrb::walkdown_rot23(node);

        match node.key.cmp(&key) {
            Ordering::Greater => {
                let (l, o) = Llrb::upsert(node.left.take(), key, value, seqno, lsm);
                node.left = l;
                (Some(Llrb::walkuprot_23(node)), o)
            }
            Ordering::Less => {
                let (r, o) = Llrb::upsert(node.right.take(), key, value, seqno, lsm);
                node.right = r;
                (Some(Llrb::walkuprot_23(node)), o)
            }
            Ordering::Equal => {
                let old_node = node.clone_detach();
                node.prepend_version(value, seqno, lsm);
                (Some(Llrb::walkuprot_23(node)), Some(old_node))
            }
        }
    }

    fn upsert_cas(
        node: Option<Box<Node<K, V>>>,
        key: K,
        val: V,
        cas: u64,
        seqno: u64,
        lsm: bool,
    ) -> (
        Option<Box<Node<K, V>>>,
        Option<Node<K, V>>,
        Option<BognError>,
    ) {
        if node.is_none() && cas > 0 {
            return (None, None, Some(BognError::InvalidCAS));
        } else if node.is_none() {
            let black = false;
            let mut node = Node::new(key, val, seqno, black);
            node.dirty = false;
            return (Some(node), None, None);
        }

        let mut node = node.unwrap();
        node = Llrb::walkdown_rot23(node);
        let (old_node, err) = match node.key.cmp(&key) {
            Ordering::Greater => {
                let (k, v, left) = (key, val, node.left.take());
                let (l, o, e) = Llrb::upsert_cas(left, k, v, cas, seqno, lsm);
                node.left = l;
                (o, e)
            }
            Ordering::Less => {
                let (k, v, right) = (key, val, node.right.take());
                let (r, o, e) = Llrb::upsert_cas(right, k, v, cas, seqno, lsm);
                node.right = r;
                (o, e)
            }
            Ordering::Equal => {
                if node.is_deleted() && cas != 0 && cas != node.seqno() {
                    (None, Some(BognError::InvalidCAS))
                } else if !node.is_deleted() && cas != node.seqno() {
                    (None, Some(BognError::InvalidCAS))
                } else {
                    let old_node = node.clone_detach();
                    node.prepend_version(val, seqno, lsm);
                    (Some(old_node), None)
                }
            }
        };

        node = Llrb::walkuprot_23(node);
        return (Some(node), old_node, err);
    }

    fn delete_lsm<Q>(
        node: Option<Box<Node<K, V>>>,
        key: &Q,
        seqno: u64,
    ) -> (Option<Box<Node<K, V>>>, Option<Node<K, V>>)
    where
        K: Borrow<Q> + From<Q>,
        Q: Clone + Ord + ?Sized,
    {
        if node.is_none() {
            // insert and mark as delete
            let (key, black) = (key.clone().into(), false);
            let mut node = Node::new(key, Default::default(), seqno, black);
            node.dirty = false;
            node.delete(seqno, true /*lsm*/);
            return (Some(node), None);
        }

        let mut node = node.unwrap();
        node = Llrb::walkdown_rot23(node);

        let (node, old_node) = match node.key.borrow().cmp(&key) {
            Ordering::Greater => {
                let (l, o) = Llrb::delete_lsm(node.left.take(), key, seqno);
                node.left = l;
                (node, o)
            }
            Ordering::Less => {
                let (r, o) = Llrb::delete_lsm(node.right.take(), key, seqno);
                node.right = r;
                (node, o)
            }
            Ordering::Equal => {
                let old_node = node.clone_detach();
                if node.is_deleted() {
                    (node, Some(old_node)) // noop
                } else {
                    node.delete(seqno, true /*lsm*/);
                    (node, Some(old_node))
                }
            }
        };

        (Some(Llrb::walkuprot_23(node)), old_node)
    }

    // this is the non-lsm path.
    fn do_delete<Q>(
        node: Option<Box<Node<K, V>>>,
        key: &Q,
    ) -> (Option<Box<Node<K, V>>>, Option<Node<K, V>>)
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut node = match node {
            None => return (None, None),
            Some(node) => node,
        };

        if node.key.borrow().gt(key) {
            if node.left.is_none() {
                (Some(node), None)
            } else {
                let ok = !is_red(node.left_deref());
                if ok && !is_red(node.left.as_ref().unwrap().left_deref()) {
                    node = Llrb::move_red_left(node);
                }
                let (left, old_node) = Llrb::do_delete(node.left.take(), key);
                node.left = left;
                (Some(Llrb::fixup(node)), old_node)
            }
        } else {
            if is_red(node.left_deref()) {
                node = Llrb::rotate_right(node);
            }

            if !node.key.borrow().lt(key) && node.right.is_none() {
                return (None, Some(*node));
            }

            let ok = node.right.is_some() && !is_red(node.right_deref());
            if ok && !is_red(node.right.as_ref().unwrap().left_deref()) {
                node = Llrb::move_red_right(node);
            }

            if !node.key.borrow().lt(key) {
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
                (Some(Llrb::fixup(newnode)), Some(*node))
            } else {
                let (right, old_node) = Llrb::do_delete(node.right.take(), key);
                node.right = right;
                (Some(Llrb::fixup(node)), old_node)
            }
        }
    }

    // return [node, old_node]
    fn delete_min(
        node: Option<Box<Node<K, V>>> // root node
    ) -> (Option<Box<Node<K, V>>>, Option<Node<K, V>>) {
        if node.is_none() {
            return (None, None);
        }
        let mut node = node.unwrap();
        if node.left.is_none() {
            return (None, Some(*node));
        }
        let left = node.left_deref();
        if !is_red(left) && !is_red(left.unwrap().left_deref()) {
            node = Llrb::move_red_left(node);
        }
        let (left, old_node) = Llrb::delete_min(node.left.take());
        node.left = left;
        (Some(Llrb::fixup(node)), old_node)
    }

    //--------- rotation routines for 2-3 algorithm ----------------

    fn walkdown_rot23(node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        node
    }

    fn walkuprot_23(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        if is_red(node.right_deref()) && !is_red(node.left_deref()) {
            node = Llrb::rotate_left(node);
        }
        let left = node.left_deref();
        if is_red(left) && is_red(left.unwrap().left_deref()) {
            node = Llrb::rotate_right(node);
        }
        if is_red(node.left_deref()) && is_red(node.right_deref()) {
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
        if is_black(node.right_deref()) {
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
        if is_black(node.left_deref()) {
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
        node = if is_red(node.right_deref()) {
            Llrb::rotate_left(node)
        } else {
            node
        };
        node = {
            let left = node.left_deref();
            if is_red(left) && is_red(left.unwrap().left_deref()) {
                Llrb::rotate_right(node)
            } else {
                node
            }
        };
        if is_red(node.left_deref()) && is_red(node.right_deref()) {
            Llrb::flip(node.deref_mut());
        }
        node
    }

    fn move_red_left(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        Llrb::flip(node.deref_mut());
        if is_red(node.right.as_ref().unwrap().left_deref()) {
            node.right = Some(Llrb::rotate_right(node.right.take().unwrap()));
            node = Llrb::rotate_left(node);
            Llrb::flip(node.deref_mut());
        }
        node
    }

    fn move_red_right(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        Llrb::flip(node.deref_mut());
        if is_red(node.left.as_ref().unwrap().left_deref()) {
            node = Llrb::rotate_right(node);
            Llrb::flip(node.deref_mut());
        }
        node
    }
}
