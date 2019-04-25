use std::borrow::Borrow;
use std::cmp::{Ord, Ordering};
use std::fmt::Debug;
use std::ops::{Bound, Deref, DerefMut};
use std::sync::{
    atomic::{AtomicPtr, Ordering::Relaxed},
    Arc,
};

use crate::core::{self, Diff, Serialize};
use crate::error::BognError;
use crate::llrb::Llrb;
use crate::llrb_node::Node;
use crate::llrb_util::Stats;
use crate::sync_writer::SyncWriter;

const RECLAIM_CAP: usize = 128;

include!("llrb_common.rs");

pub struct Mvcc<K, V>
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
{
    name: String,
    lsm: bool,
    snapshot: Snapshot<K, V>,
    fencer: SyncWriter,
}

impl<K, V> Clone for Mvcc<K, V>
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
{
    fn clone(&self) -> Mvcc<K, V> {
        let mvcc = Mvcc {
            name: self.name.clone(),
            lsm: self.lsm,
            snapshot: Snapshot::new(),
            fencer: SyncWriter::new(),
        };

        let arc_mvcc: Arc<MvccRoot<K, V>> = Snapshot::clone(&self.snapshot);
        let root = match arc_mvcc.root_ref() {
            None => None,
            Some(n) => Some(Box::new(n.clone())),
        };
        mvcc.snapshot
            .shift_snapshot(root, arc_mvcc.seqno, arc_mvcc.n_count, vec![]);
        mvcc
    }
}

impl<K, V> Drop for Mvcc<K, V>
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
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
        let root_arc = self.snapshot.value.load(Relaxed);
        let mut boxed_arc = unsafe { Box::from_raw(root_arc) };
        let mvcc_root = Arc::get_mut(boxed_arc.deref_mut()).unwrap();

        //println!("drop mvcc {:p} {:p}", self, mvcc_root);

        mvcc_root.root.take().map(|root| drop_tree(root));
    }
}

impl<K, V> From<Llrb<K, V>> for Mvcc<K, V>
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
{
    fn from(mut llrb: Llrb<K, V>) -> Mvcc<K, V> {
        let mvcc = Mvcc::new(llrb.id(), llrb.is_lsm());
        let (root, seqno, n_count) = llrb.squash();
        mvcc.snapshot
            .shift_snapshot(root, seqno, n_count, vec![] /*reclaim*/);
        mvcc
    }
}

impl<K, V> Mvcc<K, V>
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
{
    pub fn new<S>(name: S, lsm: bool) -> Mvcc<K, V>
    where
        S: AsRef<str>,
    {
        Mvcc {
            name: name.as_ref().to_string(),
            lsm,
            snapshot: Snapshot::new(),
            fencer: SyncWriter::new(),
        }
    }
}

/// Maintanence API.
impl<K, V> Mvcc<K, V>
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
{
    /// Identify this instance. Applications can choose unique names while
    /// creating Mvcc instances.
    pub fn id(&self) -> String {
        self.name.clone()
    }

    /// Return number of entries in this instance.
    pub fn len(&self) -> usize {
        Snapshot::clone(&self.snapshot).n_count
    }

    /// Set current seqno.
    pub fn set_seqno(&mut self, seqno: u64) {
        let _lock = self.fencer.lock();

        let mvcc_arc: Arc<MvccRoot<K, V>> = Snapshot::clone(&self.snapshot);
        let (root, n_count) = (mvcc_arc.root_duplicate(), mvcc_arc.n_count);

        self.snapshot.shift_snapshot(root, seqno, n_count, vec![]);
    }

    /// Return current seqno.
    pub fn get_seqno(&self) -> u64 {
        Snapshot::clone(&self.snapshot).seqno
    }

    pub fn mvccroot_ref(&self) -> &MvccRoot<K, V> {
        unsafe { self.snapshot.value.load(Relaxed).as_ref().unwrap() }
    }
}

impl<K, V> Mvcc<K, V>
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
{
    /// Get the latest version for key.
    pub fn get<Q>(&self, key: &Q) -> Option<core::Entry<K, V>>
    where
        K: Borrow<Q> + Debug,
        Q: Ord + ?Sized,
    {
        let arc_mvcc = Snapshot::clone(&self.snapshot);
        get(arc_mvcc.root_ref(), key)
    }

    pub fn iter(&self) -> Iter<K, V> {
        Iter {
            arc: Snapshot::clone(&self.snapshot),
            root: None,
            iter: vec![].into_iter(),
            after_key: Some(Bound::Unbounded),
            limit: ITER_LIMIT,
        }
    }

    pub fn range(&self, low: Bound<K>, high: Bound<K>) -> Range<K, V> {
        Range {
            arc: Snapshot::clone(&self.snapshot),
            root: None,
            iter: vec![].into_iter(),
            low: Some(low),
            high,
            limit: ITER_LIMIT,
        }
    }

    pub fn set(&self, key: K, value: V) -> Option<core::Entry<K, V>> {
        let _lock = self.fencer.lock();

        let lsm = self.lsm;
        let arc_mvcc = Snapshot::clone(&self.snapshot);

        let (seqno, mut n_count) = (arc_mvcc.seqno + 1, arc_mvcc.n_count);
        let root = arc_mvcc.root_duplicate();
        let mut reclm: Vec<Box<Node<K, V>>> = Vec::with_capacity(RECLAIM_CAP);

        match Mvcc::upsert(root, key, value, seqno, lsm, &mut reclm) {
            (Some(mut root), Some(mut n), entry) => {
                root.set_black();
                if entry.is_none() {
                    n_count += 1;
                }
                n.dirty = false;
                Box::leak(n);
                self.snapshot
                    .shift_snapshot(Some(root), seqno, n_count, reclm);
                entry
            }
            _ => unreachable!(),
        }
    }

    pub fn set_cas(
        &self,
        key: K,
        value: V,
        cas: u64,
    ) -> Result<Option<core::Entry<K, V>>, BognError> {
        let _lock = self.fencer.lock();

        let lsm = self.lsm;
        let arc_mvcc = Snapshot::clone(&self.snapshot);
        let (seqno, mut n_count) = (arc_mvcc.seqno + 1, arc_mvcc.n_count);
        let rt = arc_mvcc.root_duplicate();
        let mut rclm: Vec<Box<Node<K, V>>> = Vec::with_capacity(RECLAIM_CAP);

        let s = match Mvcc::upsert_cas(rt, key, value, cas, seqno, lsm, &mut rclm) {
            (Some(mut root), optn, _, Some(err)) => {
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

        self.snapshot
            .shift_snapshot(Some(root), seqno, n_count, rclm);

        if let Some(mut n) = optn {
            n.dirty = false;
            Box::leak(n);
        }
        entry
    }

    pub fn delete<Q>(&self, key: &Q) -> Option<core::Entry<K, V>>
    where
        // TODO: From<Q> and Clone will fail if V=String and Q=str
        K: Borrow<Q> + From<Q> + Debug,
        Q: Clone + Ord + ?Sized,
    {
        let _lock = self.fencer.lock();

        let arc_mvcc = Snapshot::clone(&self.snapshot);
        let (seqno, mut n_count) = (arc_mvcc.seqno + 1, arc_mvcc.n_count);
        let root = arc_mvcc.root_duplicate();
        let mut reclm: Vec<Box<Node<K, V>>> = Vec::with_capacity(RECLAIM_CAP);

        let (root, entry) = if self.lsm {
            let s = match Mvcc::delete_lsm(root, key, seqno, &mut reclm) {
                (Some(mut root), optn, entry) => {
                    root.set_black();
                    (Some(root), optn, entry)
                }
                (None, optn, entry) => (None, optn, entry),
            };
            let (root, optn, entry) = s;

            if entry.is_none() {
                n_count += 1
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
            }
            (root, entry)
        };

        self.snapshot.shift_snapshot(root, seqno, n_count, reclm);
        entry
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
        let arc_mvcc = Snapshot::clone(&self.snapshot);

        let n_count = arc_mvcc.n_count;
        let node_size = std::mem::size_of::<Node<K, V>>();
        let mut stats = Stats::new(n_count, node_size);
        stats.set_depths(Default::default());

        let root = arc_mvcc.root_ref();
        let (red, nb, d) = (is_red(root), 0, 0);
        let blacks = validate_tree(root, red, nb, d, &mut stats)?;
        stats.set_blacks(blacks);
        Ok(stats)
    }
}

impl<K, V> Mvcc<K, V>
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
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
        Option<core::Entry<K, V>>,
    ) {
        if node.is_none() {
            let node = Node::new(key, value, seqno, false /*black*/);
            let n = node.duplicate();
            return (Some(node), Some(n), None);
        }

        let node = node.unwrap();
        let mut new_node = node.mvcc_clone(reclaim);
        //node = Mvcc::walkdown_rot23(node);

        let cmp = new_node.key_ref().cmp(&key);
        let (new_node, n, entry) = if cmp == Ordering::Greater {
            let left = new_node.left.take();
            let (l, n, en) = Mvcc::upsert(left, key, value, seqno, lsm, reclaim);
            new_node.left = l;
            (Some(Mvcc::walkuprot_23(new_node, reclaim)), n, en)
        } else if cmp == Ordering::Less {
            let right = new_node.right.take();
            let (r, n, en) = Mvcc::upsert(right, key, value, seqno, lsm, reclaim);
            new_node.right = r;
            (Some(Mvcc::walkuprot_23(new_node, reclaim)), n, en)
        } else {
            let entry = node.entry.clone();
            new_node.prepend_version(value, seqno, lsm);
            new_node.dirty = true;
            let n = new_node.duplicate();
            (
                Some(Mvcc::walkuprot_23(new_node, reclaim)),
                Some(n),
                Some(entry
            )
        };

        Box::leak(node);
        (new_node, n, entry)
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
        Option<core::Entry<K, V>>,
        Option<BognError>,
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

        let cmp = new_node.key_ref().cmp(&key);
        let (new_node, n, entry, err) = if cmp == Ordering::Greater {
            let left = new_node.left.take();
            let s = Mvcc::upsert_cas(left, key, val, cas, seqno, lsm, reclaim);
            let (left, n, entry, e) = s;
            new_node.left = left;
            (Some(Mvcc::walkuprot_23(new_node, reclaim)), n, entry, e)
        } else if cmp == Ordering::Less {
            let right = new_node.right.take();
            let s = Mvcc::upsert_cas(right, key, val, cas, seqno, lsm, reclaim);
            let (rh, n, entry, e) = s;
            new_node.right = rh;
            (Some(Mvcc::walkuprot_23(new_node, reclaim)), n, entry, e)
        } else if new_node.is_deleted() && cas != 0 && cas != new_node.seqno() {
            // TODO: should we have the cas != new_node.seqno() predicate ??
            (Some(new_node), None, None, Some(BognError::InvalidCAS))
        } else if !new_node.is_deleted() && cas != new_node.seqno() {
            (Some(new_node), None, None, Some(BognError::InvalidCAS))
        } else {
            let entry = Some(node.entry.clone());
            new_node.prepend_version(val, seqno, lsm);
            new_node.dirty = true;
            let n = new_node.duplicate();
            (
                Some(Mvcc::walkuprot_23(new_node, reclaim)),
                Some(n),
                entry,
                None,
            )
        };

        Box::leak(node);
        (new_node, n, entry, err)
    }

    fn delete_lsm<Q>(
        node: Option<Box<Node<K, V>>>,
        key: &Q,
        seqno: u64,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> (
        Option<Box<Node<K, V>>>,
        Option<Box<Node<K, V>>>,
        Option<core::Entry<K, V>>,
    )
    where
        K: Borrow<Q> + From<Q> + Debug,
        Q: Clone + Ord + ?Sized,
    {
        if node.is_none() {
            let (key, black) = (key.clone().into(), false);
            let mut node = Node::new(key, Default::default(), seqno, black);
            node.delete(seqno);
            let n = node.duplicate();
            return (Some(node), Some(n), None);
        }

        let node = node.unwrap();
        let mut new_node = node.mvcc_clone(reclaim);
        //let mut node = Mvcc::walkdown_rot23(node.unwrap());

        let (n, entry) = match new_node.key_ref().borrow().cmp(&key) {
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
                new_node.delete(seqno);
                new_node.dirty = true;
                let n = new_node.duplicate();
                (Some(n), Some(node.entry.clone()))
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
    ) -> (Option<Box<Node<K, V>>>, Option<core::Entry<K, V>>)
    where
        K: Borrow<Q> + Debug,
        Q: Ord + ?Sized,
    {
        if node.is_none() {
            return (None, None);
        }

        let node = node.unwrap();
        let mut new_node = node.mvcc_clone(reclaim);
        Box::leak(node);

        if new_node.key_ref().borrow().gt(key) {
            if new_node.left.is_none() {
                // key not present, nothing to delete
                (Some(new_node), None)
            } else {
                let ok = !is_red(new_node.left_deref());
                if ok && !is_red(new_node.left.as_ref().unwrap().left_deref()) {
                    new_node = Mvcc::move_red_left(new_node, reclaim);
                }
                let left = new_node.left.take();
                let (left, entry) = Mvcc::do_delete(left, key, reclaim);
                new_node.left = left;
                (Some(Mvcc::fixup(new_node, reclaim)), entry)
            }
        } else {
            if is_red(new_node.left_deref()) {
                new_node = Mvcc::rotate_right(new_node, reclaim);
            }

            // if key equals node and no right children
            if !new_node.key_ref().borrow().lt(key) && new_node.right.is_none() {
                new_node.mvcc_detach();
                return (None, Some(new_node.entry.clone()));
            }

            let ok = new_node.right.is_some() && !is_red(new_node.right_deref());
            if ok && !is_red(new_node.right.as_ref().unwrap().left_deref()) {
                new_node = Mvcc::move_red_right(new_node, reclaim);
            }

            // if key equal node and there is a right children
            if !new_node.key_ref().borrow().lt(key) {
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
                let entry = new_node.entry.clone();
                (Some(Mvcc::fixup(newnode, reclaim)), Some(entry))
            } else {
                let right = new_node.right.take();
                let (right, entry) = Mvcc::do_delete(right, key, reclaim);
                new_node.right = right;
                (Some(Mvcc::fixup(new_node, reclaim)), entry)
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
struct Snapshot<K, V>
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
{
    value: AtomicPtr<Arc<MvccRoot<K, V>>>,
}

impl<K, V> Snapshot<K, V>
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
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
        // gets arc-dropped
        let arc = unsafe { Box::from_raw(self.value.load(Relaxed)) };
        let next_arc = Box::new(Arc::clone(arc.next.as_ref().unwrap()));
        let mvcc_root = unsafe {
            (&**next_arc as *const MvccRoot<K, V> as *mut MvccRoot<K, V>)
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
}

pub struct MvccRoot<K, V>
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
{
    root: Option<Box<Node<K, V>>>,
    reclaim: Vec<Box<Node<K, V>>>,
    seqno: u64,     // starts from 0 and incr for every mutation.
    n_count: usize, // number of entries in the tree.
    next: Option<Arc<MvccRoot<K, V>>>,
}

impl<K, V> MvccRoot<K, V>
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
{
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

    pub fn root_ref(&self) -> Option<&Node<K, V>> {
        self.root.as_ref().map(Deref::deref)
    }
}

impl<K, V> Drop for MvccRoot<K, V>
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
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
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
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

#[allow(dead_code)]
fn print_reclaim<K, V>(prefix: &str, reclaim: &Vec<Box<Node<K, V>>>)
where
    K: Clone + Ord,
    V: Default + Clone + Diff + Serialize,
{
    print!("{}reclaim ", prefix);
    reclaim.iter().for_each(|item| print!("{:p} ", *item));
    println!("");
}
