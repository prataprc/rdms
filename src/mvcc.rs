use std::borrow::Borrow;
use std::cmp::{Ord, Ordering};
use std::ops::{Bound, Deref, DerefMut};
use std::sync::{
    atomic::{AtomicPtr, AtomicU64, Ordering::Relaxed},
    Arc,
};

use crate::error::BognError;
use crate::llrb::Llrb;
use crate::llrb_common::{self, drop_tree, is_black, is_red, Iter, Range, Stats};
use crate::llrb_node::Node;
use crate::traits::AsEntry;

// TODO: Remove AtomicPtr and test/benchmark.
// TODO: Remove RwLock and use AtomicPtr and latch mechanism, test/benchmark.
// TODO: Remove Mutex and check write performance.

const RECLAIM_CAP: usize = 128;

pub struct Mvcc<K, V>
where
    K: Default + Clone + Ord,
    V: Default + Clone,
{
    pub(crate) name: String,
    pub(crate) lsm: bool,
    pub(crate) snapshot: Snapshot<K, V>,
    pub(crate) writers: AtomicU64,
}

impl<K, V> Clone for Mvcc<K, V>
where
    K: Default + Clone + Ord,
    V: Default + Clone,
{
    fn clone(&self) -> Mvcc<K, V> {
        let mvcc = Mvcc {
            name: self.name.clone(),
            lsm: self.lsm,
            snapshot: Snapshot::new(),
            writers: AtomicU64::new(0),
        };

        let arc: Arc<MvccRoot<K, V>> = self.snapshot.clone();
        let root = Box::new(arc.as_ref().as_ref().unwrap().clone());

        let (seqno, n_count) = (arc.seqno, arc.n_count);
        mvcc.snapshot
            .shift_snapshot(Some(root), seqno, n_count, vec![]);
        mvcc
    }
}

impl<K, V> Drop for Mvcc<K, V>
where
    K: Default + Clone + Ord,
    V: Default + Clone,
{
    fn drop(&mut self) {
        // NOTE: Means all references to mvcc are gone and ownership is going out
        // of scope. This also implies that there are only TWO Arc<> snapshots.
        // One is held by self.snapshot and another is held by `next`.

        // NOTE: AtomicPtr will fence the drop chain, so we have to get past the
        // atomic fence and drop it here.

        // NOTE: Likewise MvccRoot will fence the drop on its `root` field, so we
        // have to get past that and drop it here.

        // drop arc.
        let mut boxed_arc = unsafe { Box::from_raw(self.snapshot.value.load(Relaxed)) };
        let mvcc_root = Arc::get_mut(boxed_arc.deref_mut()).unwrap();

        //println!("drop mvcc {:p} {:p}", self, mvcc_root);

        // TODO: drop root, make it functional
        match mvcc_root.root.take() {
            // drop root
            Some(root) => drop_tree(root),
            None => (),
        };
    }
}

impl<K, V> From<Llrb<K, V>> for Mvcc<K, V>
where
    K: Default + Clone + Ord,
    V: Default + Clone,
{
    fn from(mut llrb: Llrb<K, V>) -> Mvcc<K, V> {
        let mvcc = Mvcc::new(llrb.name.clone(), llrb.lsm);
        mvcc.snapshot.shift_snapshot(
            llrb.root.take(),
            llrb.seqno,
            llrb.n_count,
            vec![], /*reclaim*/
        );
        mvcc
    }
}

impl<K, V> Mvcc<K, V>
where
    K: Default + Clone + Ord,
    V: Default + Clone,
{
    pub fn new<S>(name: S, lsm: bool) -> Mvcc<K, V>
    where
        S: AsRef<str>,
    {
        Mvcc {
            name: name.as_ref().to_string(),
            lsm,
            snapshot: Snapshot::new(),
            writers: AtomicU64::new(0),
        }
    }
}

/// Maintanence API.
impl<K, V> Mvcc<K, V>
where
    K: Default + Clone + Ord,
    V: Default + Clone,
{
    /// Identify this instance. Applications can choose unique names while
    /// creating Mvcc instances.
    pub fn id(&self) -> String {
        self.name.clone()
    }

    /// Return number of entries in this instance.
    pub fn len(&self) -> usize {
        self.snapshot.clone().n_count
    }

    /// Set current seqno.
    pub fn set_seqno(&mut self, seqno: u64) {
        if self.writers.compare_and_swap(0, 1, Relaxed) != 0 {
            panic!("Mvcc cannot have concurrent writers");
        }

        let arc: Arc<MvccRoot<K, V>> = self.snapshot.clone();
        let root = arc.deref().as_duplicate();

        self.snapshot
            .shift_snapshot(root, seqno, arc.n_count, vec![]);

        if self.writers.compare_and_swap(1, 0, Relaxed) != 1 {
            unreachable!();
        }
    }

    /// Return current seqno.
    pub fn get_seqno(&self) -> u64 {
        self.snapshot.clone().seqno
    }
}

impl<K, V> Mvcc<K, V>
where
    K: Default + Clone + Ord,
    V: Default + Clone,
{
    /// Get the latest version for key.
    pub fn get<Q>(&self, key: &Q) -> Option<impl AsEntry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let arc = self.snapshot.clone();
        let mvcc_root: &MvccRoot<K, V> = arc.as_ref();
        llrb_common::get(mvcc_root.as_ref(), key)
    }

    pub fn iter(&self) -> Iter<K, V> {
        let arc = self.snapshot.clone();
        Iter {
            arc,
            root: None,
            node_iter: vec![].into_iter(),
            after_key: Some(Bound::Unbounded),
            limit: llrb_common::ITER_LIMIT,
        }
    }

    pub fn range(&self, low: Bound<K>, high: Bound<K>) -> Range<K, V> {
        let arc = self.snapshot.clone();
        Range {
            arc,
            root: None,
            node_iter: vec![].into_iter(),
            low: Some(low),
            high,
            limit: llrb_common::ITER_LIMIT,
        }
    }

    pub fn set(&self, key: K, value: V) -> Option<impl AsEntry<K, V>> {
        if self.writers.compare_and_swap(0, 1, Relaxed) != 0 {
            panic!("Mvcc cannot have concurrent writers");
        }

        let lsm = self.lsm;
        let arc = self.snapshot.clone();

        let seqno = arc.seqno + 1;
        let mut n_count = arc.n_count;
        let root = arc.as_duplicate();
        let mut reclaim: Vec<Box<Node<K, V>>> = Vec::with_capacity(RECLAIM_CAP);

        let res = match Mvcc::upsert(root, key, value, seqno, lsm, &mut reclaim) {
            (Some(mut root), Some(mut n), old_node) => {
                root.set_black();
                if old_node.is_none() {
                    n_count += 1;
                }
                n.dirty = false;
                Box::leak(n);
                self.snapshot
                    .shift_snapshot(Some(root), seqno, n_count, reclaim);
                old_node
            }
            _ => unreachable!(),
        };

        if self.writers.compare_and_swap(1, 0, Relaxed) != 1 {
            unreachable!();
        }
        res
    }

    pub fn set_cas(
        &self,
        key: K,
        value: V,
        cas: u64,
    ) -> Result<Option<impl AsEntry<K, V>>, BognError<K>> {
        if self.writers.compare_and_swap(0, 1, Relaxed) != 0 {
            panic!("Mvcc cannot have concurrent writers");
        }

        let lsm = self.lsm;
        let arc = self.snapshot.clone();

        let seqno = arc.seqno + 1;
        let mut n_count = arc.n_count;

        let root = arc.as_duplicate();
        let mut reclaim: Vec<Box<Node<K, V>>> = Vec::with_capacity(RECLAIM_CAP);

        let (root, optn, ret) =
            match Mvcc::upsert_cas(root, key, value, cas, seqno, lsm, &mut reclaim) {
                (Some(mut root), optn, _, Some(err)) => {
                    root.set_black();
                    (root, optn, Err(err))
                }
                (Some(mut root), optn, old_node, None) => {
                    root.set_black();
                    if old_node.is_none() {
                        n_count += 1
                    }
                    (root, optn, Ok(old_node))
                }
                _ => panic!("set_cas: impossible case, call programmer"),
            };

        self.snapshot
            .shift_snapshot(Some(root), seqno, n_count, reclaim);

        if let Some(mut n) = optn {
            n.dirty = false;
            Box::leak(n);
        }
        if self.writers.compare_and_swap(1, 0, Relaxed) != 1 {
            unreachable!();
        }
        ret
    }

    pub fn delete<Q>(&self, key: &Q) -> Option<impl AsEntry<K, V>>
    where
        // TODO: From<Q> and Clone will fail if V=String and Q=str
        K: Borrow<Q> + From<Q>,
        Q: Clone + Ord + ?Sized,
    {
        if self.writers.compare_and_swap(0, 1, Relaxed) != 0 {
            panic!("Mvcc cannot have concurrent writers");
        }

        let arc = self.snapshot.clone();

        let mut seqno = arc.seqno + 1;
        let mut n_count = arc.n_count;
        let root = arc.as_duplicate();
        let mut reclaim: Vec<Box<Node<K, V>>> = Vec::with_capacity(RECLAIM_CAP);

        let (root, old_node) = if self.lsm {
            let (root, optn, old_node) = match Mvcc::delete_lsm(root, key, seqno, &mut reclaim) {
                (Some(mut root), optn, old_node) => {
                    root.set_black();
                    (Some(root), optn, old_node)
                }
                (None, optn, old_node) => (None, optn, old_node),
            };
            match old_node.as_ref() {
                None => n_count += 1,
                Some(old_node) if old_node.is_deleted() => seqno -= 1,
                _ => (),
            }
            if let Some(mut n) = optn {
                n.dirty = false;
                Box::leak(n);
            }
            (root, old_node)
        } else {
            // in non-lsm mode remove the entry from the tree.
            let (root, old_node) = match Mvcc::do_delete(root, key, &mut reclaim) {
                (None, old_node) => (None, old_node),
                (Some(mut root), old_node) => {
                    root.set_black();
                    (Some(root), old_node)
                }
            };
            if old_node.is_some() {
                n_count -= 1;
            } else {
                seqno -= 1;
            }
            (root, old_node.map(|item| *item))
        };

        self.snapshot.shift_snapshot(root, seqno, n_count, reclaim);
        if self.writers.compare_and_swap(1, 0, Relaxed) != 1 {
            unreachable!();
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
    pub fn validate(&self) -> Result<Stats, BognError<K>> {
        let arc = self.snapshot.clone();

        let n_count = arc.n_count;
        let node_size = std::mem::size_of::<Node<K, V>>();
        let mut stats = Stats::new(n_count, node_size);
        stats.set_depths(Default::default());

        let root = arc.as_ref().as_ref();
        let (red, nb, d) = (is_red(root), 0, 0);
        let blacks = llrb_common::validate_tree(root, red, nb, d, &mut stats)?;
        stats.set_blacks(blacks);
        Ok(stats)
    }
}

impl<K, V> Mvcc<K, V>
where
    K: Default + Clone + Ord,
    V: Default + Clone,
{
    fn upsert(
        node: Option<Box<Node<K, V>>>,
        key: K,
        value: V,
        seqno: u64,
        lsm: bool,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> (
        Option<Box<Node<K, V>>>,
        Option<Box<Node<K, V>>>,
        Option<Node<K, V>>,
    ) {
        if node.is_none() {
            let node = Node::new(key, value, seqno, false /*black*/);
            let n = node.duplicate();
            return (Some(node), Some(n), None);
        }

        let node = node.unwrap();
        let mut new_node = node.mvcc_clone(reclaim);
        //node = Mvcc::walkdown_rot23(node);

        let cmp = new_node.key.cmp(&key);
        let (new_node, n, old_node) = if cmp == Ordering::Greater {
            let left = new_node.left.take();
            let (l, n, o) = Mvcc::upsert(left, key, value, seqno, lsm, reclaim);
            new_node.left = l;
            (Some(Mvcc::walkuprot_23(new_node, reclaim)), n, o)
        } else if cmp == Ordering::Less {
            let right = new_node.right.take();
            let (r, n, o) = Mvcc::upsert(right, key, value, seqno, lsm, reclaim);
            new_node.right = r;
            (Some(Mvcc::walkuprot_23(new_node, reclaim)), n, o)
        } else {
            let old_node = node.clone_detach();
            new_node.prepend_version(value, seqno, lsm);
            new_node.dirty = true;
            let n = new_node.duplicate();
            (
                Some(Mvcc::walkuprot_23(new_node, reclaim)),
                Some(n),
                Some(old_node),
            )
        };

        Box::leak(node);
        (new_node, n, old_node)
    }

    fn upsert_cas(
        node: Option<Box<Node<K, V>>>,
        key: K,
        val: V,
        cas: u64,
        seqno: u64,
        lsm: bool,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> (
        Option<Box<Node<K, V>>>, // mvcc-path
        Option<Box<Node<K, V>>>, // new_node
        Option<Node<K, V>>,
        Option<BognError<K>>,
    ) {
        if node.is_none() && cas > 0 {
            return (None, None, None, Some(BognError::InvalidCAS));
        } else if node.is_none() {
            let node = Node::new(key, val, seqno, false /*black*/);
            let n = node.duplicate();
            return (Some(node), Some(n), None, None);
        }

        let node = node.unwrap();
        let mut new_node = node.mvcc_clone(reclaim);
        // node = Mvcc::walkdown_rot23(node);

        let cmp = new_node.key.cmp(&key);
        let (new_node, n, old_node, err) = if cmp == Ordering::Greater {
            let (k, v, left) = (key, val, new_node.left.take());
            let (l, n, o, e) = Mvcc::upsert_cas(left, k, v, cas, seqno, lsm, reclaim);
            new_node.left = l;
            (Some(Mvcc::walkuprot_23(new_node, reclaim)), n, o, e)
        } else if cmp == Ordering::Less {
            let (k, v, right) = (key, val, new_node.right.take());
            let (rh, n, o, e) = Mvcc::upsert_cas(right, k, v, cas, seqno, lsm, reclaim);
            new_node.right = rh;
            (Some(Mvcc::walkuprot_23(new_node, reclaim)), n, o, e)
        } else if new_node.is_deleted() && cas != 0 && cas != new_node.seqno() {
            // TODO: should we have the cas != new_node.seqno() predicate ??
            (Some(new_node), None, None, Some(BognError::InvalidCAS))
        } else if !new_node.is_deleted() && cas != new_node.seqno() {
            (Some(new_node), None, None, Some(BognError::InvalidCAS))
        } else {
            let old_node = Some(node.clone_detach());
            new_node.prepend_version(val, seqno, lsm);
            new_node.dirty = true;
            let n = new_node.duplicate();
            (
                Some(Mvcc::walkuprot_23(new_node, reclaim)),
                Some(n),
                old_node,
                None,
            )
        };

        Box::leak(node);
        (new_node, n, old_node, err)
    }

    fn delete_lsm<Q>(
        node: Option<Box<Node<K, V>>>,
        key: &Q,
        seqno: u64,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> (
        Option<Box<Node<K, V>>>,
        Option<Box<Node<K, V>>>,
        Option<Node<K, V>>,
    )
    where
        K: Borrow<Q> + From<Q>,
        Q: Clone + Ord + ?Sized,
    {
        if node.is_none() {
            let (key, black) = (key.clone().into(), false);
            let mut node = Node::new(key, Default::default(), seqno, black);
            node.delete(seqno, true /*lsm*/);
            let n = node.duplicate();
            return (Some(node), Some(n), None);
        }

        let node = node.unwrap();
        let mut new_node = node.mvcc_clone(reclaim);
        //let mut node = Mvcc::walkdown_rot23(node.unwrap());

        let (n, old_node) = match new_node.key.borrow().cmp(&key) {
            Ordering::Greater => {
                let left = new_node.left.take();
                let (left, n, old_node) = Mvcc::delete_lsm(left, key, seqno, reclaim);
                new_node.left = left;
                (n, old_node)
            }
            Ordering::Less => {
                let right = new_node.right.take();
                let (right, n, old_node) = Mvcc::delete_lsm(right, key, seqno, reclaim);
                new_node.right = right;
                (n, old_node)
            }
            Ordering::Equal => {
                new_node.delete(seqno, true /*lsm*/);
                new_node.dirty = true;
                let n = new_node.duplicate();
                (Some(n), Some(node.clone_detach()))
            }
        };

        Box::leak(node);
        (Some(Mvcc::walkuprot_23(new_node, reclaim)), n, old_node)
    }

    // this is the non-lsm path.
    fn do_delete<Q>(
        node: Option<Box<Node<K, V>>>,
        key: &Q,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> (Option<Box<Node<K, V>>>, Option<Box<Node<K, V>>>)
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        if node.is_none() {
            return (None, None);
        }

        let node = node.unwrap();
        let mut new_node = node.mvcc_clone(reclaim);
        Box::leak(node);

        if new_node.key.borrow().gt(key) {
            if new_node.left.is_none() {
                // key not present, nothing to delete
                (Some(new_node), None)
            } else {
                let ok = !is_red(new_node.left_deref());
                if ok && !is_red(new_node.left.as_ref().unwrap().left_deref()) {
                    new_node = Mvcc::move_red_left(new_node, reclaim);
                }
                let left = new_node.left.take();
                let (left, old_node) = Mvcc::do_delete(left, key, reclaim);
                new_node.left = left;
                (Some(Mvcc::fixup(new_node, reclaim)), old_node)
            }
        } else {
            if is_red(new_node.left_deref()) {
                new_node = Mvcc::rotate_right(new_node, reclaim);
            }

            // if key equals node and no right children
            if !new_node.key.borrow().lt(key) && new_node.right.is_none() {
                new_node.mvcc_detach();
                return (None, Some(new_node));
            }

            let ok = new_node.right.is_some() && !is_red(new_node.right_deref());
            if ok && !is_red(new_node.right.as_ref().unwrap().left_deref()) {
                new_node = Mvcc::move_red_right(new_node, reclaim);
            }

            // if key equal node and there is a right children
            if !new_node.key.borrow().lt(key) {
                // node == key
                let right = new_node.right.take();
                let (right, mut res_node) = Mvcc::delete_min(right, reclaim);
                new_node.right = right;
                if res_node.is_none() {
                    panic!("do_delete(): fatal logic, call the programmer");
                }
                let mut newnode = res_node.take().unwrap();
                newnode.left = new_node.left.take();
                newnode.right = new_node.right.take();
                newnode.black = new_node.black;
                (Some(Mvcc::fixup(newnode, reclaim)), Some(new_node))
            } else {
                let right = new_node.right.take();
                let (right, old_node) = Mvcc::do_delete(right, key, reclaim);
                new_node.right = right;
                (Some(Mvcc::fixup(new_node, reclaim)), old_node)
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
            let left = new_node.left_deref();
            if !is_red(left) && !is_red(left.unwrap().left_deref()) {
                new_node = Mvcc::move_red_left(new_node, reclaim);
            }
            let left = new_node.left.take();
            let (left, old_node) = Mvcc::delete_min(left, reclaim);
            new_node.left = left;
            (Some(Mvcc::fixup(new_node, reclaim)), old_node)
        }
    }

    ////--------- rotation routines for 2-3 algorithm ----------------

    //fn walkdown_rot23(node: Box<Node<K, V>>) -> Box<Node<K, V>> {
    //    node
    //}

    fn walkuprot_23(
        mut node: Box<Node<K, V>>,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> Box<Node<K, V>> {
        if is_red(node.right_deref()) && !is_red(node.left_deref()) {
            node = Mvcc::rotate_left(node, reclaim);
        }
        let left = node.left_deref();
        if is_red(left) && is_red(left.unwrap().left_deref()) {
            node = Mvcc::rotate_right(node, reclaim);
        }
        if is_red(node.left_deref()) && is_red(node.right_deref()) {
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
        if is_red(node.right_deref()) {
            node = Mvcc::rotate_left(node, reclaim)
        }
        let left = node.left_deref();
        if is_red(left) && is_red(left.unwrap().left_deref()) {
            node = Mvcc::rotate_right(node, reclaim)
        }
        if is_red(node.left_deref()) && is_red(node.right_deref()) {
            Mvcc::flip(node.deref_mut(), reclaim);
        }
        node
    }

    fn move_red_left(
        mut node: Box<Node<K, V>>,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> Box<Node<K, V>> {
        Mvcc::flip(node.deref_mut(), reclaim);
        if is_red(node.right.as_ref().unwrap().left_deref()) {
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
        if is_red(node.left.as_ref().unwrap().left_deref()) {
            node = Mvcc::rotate_right(node, reclaim);
            Mvcc::flip(node.deref_mut(), reclaim);
        }
        node
    }
}

#[derive(Default)]
pub(crate) struct Snapshot<K, V>
where
    K: Default + Clone + Ord,
    V: Default + Clone,
{
    pub(crate) value: AtomicPtr<Arc<MvccRoot<K, V>>>,
}

impl<K, V> Snapshot<K, V>
where
    K: Default + Clone + Ord,
    V: Default + Clone,
{
    fn new() -> Snapshot<K, V> {
        let next = Some(Arc::new(MvccRoot::new(None)));
        let mvcc_root: MvccRoot<K, V> = MvccRoot::new(next);
        let arc = Box::new(Arc::new(mvcc_root));
        //println!("new snapshot {:p} {}", arc, Arc::strong_count(&arc));
        Snapshot {
            value: AtomicPtr::new(Box::leak(arc)),
        }
    }

    fn shift_snapshot(
        &self,
        root: Option<Box<Node<K, V>>>,
        seqno: u64,
        n_count: usize,
        reclaim: Vec<Box<Node<K, V>>>,
    ) {
        let arc = unsafe {
            Box::from_raw(self.value.load(Relaxed)) // gets arc-dropped
        };

        let next_arc = Box::new(Arc::clone(arc.as_ref().next.as_ref().unwrap()));
        let mvcc_root = unsafe {
            (next_arc.deref().deref() as *const MvccRoot<K, V> as *mut MvccRoot<K, V>)
                .as_mut()
                .unwrap()
        };

        mvcc_root.root = root;
        mvcc_root.seqno = seqno;
        mvcc_root.n_count = n_count;
        mvcc_root.next = Some(Arc::new(MvccRoot::new(None)));
        //println!(
        //    "shift snapshot {:p} {} {} {:p}",
        //    next_arc,
        //    Arc::strong_count(&arc),
        //    Arc::strong_count(&next_arc),
        //    mvcc_root.next.as_ref().unwrap().deref(),
        //);
        //print_reclaim("    ", &reclaim);
        mvcc_root.reclaim = reclaim;

        self.value.store(Box::leak(next_arc), Relaxed);
    }

    fn clone(&self) -> Arc<MvccRoot<K, V>> {
        Arc::clone(unsafe { self.value.load(Relaxed).as_ref().unwrap() })
    }

    #[allow(dead_code)]
    fn as_ref(&self) -> &MvccRoot<K, V> {
        unsafe { self.value.load(Relaxed).as_ref().unwrap() }
    }

    #[allow(dead_code)]
    fn as_mut(&self) -> &mut MvccRoot<K, V> {
        unsafe {
            (self.value.load(Relaxed).as_mut().unwrap().deref().deref() as *const MvccRoot<K, V>
                as *mut MvccRoot<K, V>)
                .as_mut()
                .unwrap()
        }
    }
}

#[derive(Default)]
pub(crate) struct MvccRoot<K, V>
where
    K: Default + Clone + Ord,
    V: Default + Clone,
{
    pub(crate) root: Option<Box<Node<K, V>>>,
    pub(crate) reclaim: Vec<Box<Node<K, V>>>,
    pub(crate) seqno: u64,     // starts from 0 and incr for every mutation.
    pub(crate) n_count: usize, // number of entries in the tree.
    pub(crate) next: Option<Arc<MvccRoot<K, V>>>,
}

impl<K, V> MvccRoot<K, V>
where
    K: Default + Clone + Ord,
    V: Default + Clone,
{
    fn new(next: Option<Arc<MvccRoot<K, V>>>) -> MvccRoot<K, V> {
        //println!("new mvcc-root {:p}", mvcc_root);
        let mut mvcc_root: MvccRoot<K, V> = Default::default();
        mvcc_root.next = next;
        mvcc_root
    }

    pub(crate) fn as_ref(&self) -> Option<&Node<K, V>> {
        self.root.as_ref().map(|item| item.deref())
    }

    #[allow(dead_code)]
    pub(crate) fn as_mut(&self) -> Option<&mut Node<K, V>> {
        unsafe {
            (&self.root as *const Option<Box<Node<K, V>>> as *mut Option<Box<Node<K, V>>>)
                .as_mut()
                .unwrap()
                .as_mut()
                .map(|item| item.deref_mut())
        }
    }

    pub(crate) fn as_duplicate(&self) -> Option<Box<Node<K, V>>> {
        let x = &self.root as *const Option<Box<Node<K, V>>> as *mut Option<Box<Node<K, V>>>;
        let y = unsafe { x.as_mut().unwrap() };
        if y.is_none() {
            return None;
        }
        Some(unsafe { Box::from_raw(y.as_mut().unwrap().deref_mut()) })
    }
}

impl<K, V> Drop for MvccRoot<K, V>
where
    K: Default + Clone + Ord,
    V: Default + Clone,
{
    fn drop(&mut self) {
        // NOTE: `root` will be leaked, so that the tree is intact.

        // NOTE: `reclaim` nodes will be dropped, but due the Drop
        // drop implementation of Node, child nodes won't be dropped.

        // NOTE: `next` snapshot will be dropped and its reference
        // count decremented, whether it is freed is based on the active
        // reference at this instant.

        self.root.take().map(|root| Box::leak(root)); // Leak root

        // NOTE debug
        //let next_arc = match self.next.as_ref() {
        //    None => return (),
        //    Some(boxed_next_arc) => boxed_next_arc.deref(),
        //};
        //let next_mvcc_root = unsafe {
        //    (next_arc.deref().deref().deref() as *const MvccRoot<K, V> as *mut MvccRoot<K, V>)
        //        .as_mut()
        //        .unwrap()
        //};
        //println!(
        //    "drop mvcc-root {:p} -> {:p} {}",
        //    self,
        //    next_mvcc_root,
        //    Arc::strong_count(&next_arc)
        //);
        //print_reclaim("    ", &mut self.reclaim);
    }
}

#[allow(dead_code)]
pub(crate) fn print_reclaim<K, V>(prefix: &str, reclaim: &Vec<Box<Node<K, V>>>)
where
    K: Default + Clone + Ord,
    V: Default + Clone,
{
    print!("{}reclaim ", prefix);
    reclaim.iter().for_each(|item| print!("{:p} ", *item));
    println!("");
}
