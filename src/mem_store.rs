use std::cmp::{Ordering, Ord};
use std::borrow::Borrow;
use std::ops::Bound;

use crate::traits::{AsKey, AsValue, AsNode};
use crate::error::BognError;

// TODO: search for red, black and dirty logic and double-check.
// TODO: llrb_depth_histogram, as feature, to measure the depth of LLRB tree.
// TODO: Memstore: should we implement Drop as part of cleanup.
// TODO: Memstore: Clone trait ?
// TODO: Memstore: Implement `pub undo`.
// TODO: Memstore: Implement `pub purge`.


/// Memstore to manage a single instance of in-memory sorted index using
/// left-leaning-red-black tree.
///
/// lsm mode: Memstore instances support what is called as log-structured-merge
/// while mutating the tree. In simple terms, this means that nothing shall be
/// over-written in the tree and all the mutations for the same key shall be 
/// preserved until they are purged. Although there is one exception to it,
/// where back-to-back deletes will collapse.
///
/// IMPORTANT: This tree is not thread safe.
pub struct Memstore<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    name: String,
    lsm: bool,
    root: Option<Box<Node<K, V>>>,
    seqno: u64,   // seqno so far, starts from 0 and incr for every mutation.
    n_count: u64, // number of entries in the tree.
}

impl<K, V> Memstore<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    /// Create an empty instance of Memstore, identified by `name`.
    /// Applications can use unique names.
    pub fn new(name: String, lsm: bool) -> Memstore<K, V> {
        let store = Memstore {
            name,
            lsm,
            seqno: 0,
            root: None,
            n_count: 0,
        };
        store
    }

    /// Clone the under-lying tree into a new Memstore instance.
    pub fn clone_tree(&self) -> Memstore<K,V> {
        let new_store = Memstore{
            name: self.name.clone(),
            lsm: self.lsm,
            seqno: self.seqno,
            n_count: self.n_count,
            root: self.root.clone(),
        };
        new_store
    }

    /// Create a new instance of Memstore tree and load it with entries from
    /// `iter`. Note that iterator shall return items which can be converted
    /// to Memstore node.
    pub fn load_from<N>(name: String, iter: impl Iterator<Item=N>, lsm: bool)
        -> Memstore<K,V>
    where
        N: Into<Node<K,V>> + AsNode<K,V>
    {
        let mut store = Memstore::new(name, lsm);
        for n in iter {
            let root = store.root.take();
            let mut root = store.load_node(root, n.key(), Some(n));
            root.as_mut().unwrap().set_black();
            store.root = root;
        }
        store
    }

    fn load_node<N>(
        &mut self,
        node: Option<Box<Node<K,V>>>,
        key: K,
        n: Option<N>) -> Option<Box<Node<K,V>>>
    where
        N: Into<Node<K,V>> + AsNode<K,V>
    {
        if node.is_none() {
            let node: Node<K,V> = n.unwrap().into();
            self.seqno = node.seqno();
            self.n_count += if node.is_deleted() { 0 } else { 1 };
            Some(Box::new(node))

        } else {
            let mut node = node.unwrap();
            node = Memstore::walkdown_rot23(node);
            if node.key.gt(&key) {
                node.left = self.load_node(node.left, key, n);
                Some(Memstore::walkuprot_23(node))

            } else if node.key.lt(&key) {
                node.right = self.load_node(node.right, key, n);
                Some(Memstore::walkuprot_23(node))

            } else {
                panic!("load_node: duplicate keys not allowed");
            }
        }
    }

    /// Identify this instance. Applications can use unique names while
    /// creating Memstore instances.
    pub fn id(&self) -> String {
        self.name.clone()
    }

    /// Set current seqno.
    pub fn set_seqno(&mut self, seqno: u64) {
        self.seqno = seqno;
    }

    /// Return current seqno.
    pub fn get_seqno(&self) -> u64 {
        self.seqno
    }

    /// Get the latest version for key.
    pub fn get<Q>(&self, key: &Q) -> Option<impl AsNode<K,V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut node = &self.root;
        while node.is_some() {
            let nref = node.as_ref().unwrap();
            node = match nref.key.borrow().cmp(key) {
                Ordering::Less => &nref.right,
                Ordering::Equal => return Some(nref.clone_detach()),
                Ordering::Greater => &nref.left,
            };
        }
        None
    }

    /// Get all the versions for key.
    pub fn get_versions<Q>(&self, key: &Q) -> Option<impl AsNode<K,V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut node = &self.root;
        while node.is_some() {
            let nref = node.as_ref().unwrap();
            node = match nref.key.borrow().cmp(key) {
                Ordering::Less => &nref.right,
                Ordering::Equal => return Some(*nref.clone()),
                Ordering::Greater => &nref.left,
            };
        }
        None
    }

    /// Return an iterator over all entries in this instance.
    pub fn iter(&self) -> Iter<K,V> {
        let mut acc: Vec<Node<K,V>> = vec![];
        let root = &self.root;
        scan(root, &Bound::Unbounded, 100, &mut acc); // TODO: no magic number
        if acc.len() == 0 {
            let after_key = Bound::Unbounded;
            let node_iter = acc.into_iter().rev();
            return Iter{root, empty: true, node_iter, after_key}
        }
        let after_key = Bound::Excluded(acc.last().unwrap().key());
        let node_iter = acc.into_iter().rev();
        return Iter{root, empty: false, node_iter, after_key}
    }

    /// Set a new entry into this instance. If key is already present, return
    /// the previous entry. In LSM mode, this will add a new version for the
    /// key.
    pub fn set(&mut self, key: K, value: V) -> Option<impl AsNode<K,V>> {
        let seqno = self.seqno + 1;

        let root = self.root.take();
        let mut res = Memstore::upsert(root, key, value, seqno, self.lsm);
        let mut root = res[0].take().unwrap();
        root.set_black();

        self.root = Some(root);
        self.seqno = seqno;
        match res[1].take() {
            Some(old_node) => Some(*old_node),
            None => { self.n_count += 1; None },
        }
    }

    fn upsert(
        node: Option<Box<Node<K,V>>>,
        key: K,
        value: V,
        seqno: u64,
        lsm: bool) -> [Option<Box<Node<K,V>>>; 2]
    {
        if node.is_none() {
            let (access, black) = (0, false);
            [Some(Box::new(Node::new(key, value, seqno, access, black))), None]

        } else {
            let mut node = node.unwrap();
            node = Memstore::walkdown_rot23(node);
            if node.key.gt(&key) {
                let mut res = Memstore::upsert(node.left, key, value, seqno, lsm);
                node.left = res[0].take();
                node = Memstore::walkuprot_23(node);
                [Some(node), res[1].take()]

            } else if node.key.lt(&key) {
                let mut res = Memstore::upsert(node.right, key, value, seqno, lsm);
                node.right = res[0].take();
                node = Memstore::walkuprot_23(node);
                [Some(node), res[1].take()]

            } else {
                let old_node = node.clone_detach();
                node.prepend_value(value, seqno, 0, /*access*/ lsm);
                node = Memstore::walkuprot_23(node);
                [Some(node), Some(Box::new(old_node))]
            }
        }
    }

    /// Set a new entry into this instance, only if entry's seqno matches the
    /// supplied CAS. Use CAS == 0 to enforce a create operation. If key is
    /// already present, return the previous entry. In LSM mode, this will add
    /// a new version for the key.
    pub fn set_cas(&mut self, key: K, value: V, cas: u64)
        -> Result<Option<impl AsNode<K,V>>, BognError>
    {
        let seqno = self.seqno + 1;
        let lsm = self.lsm;

        let root = self.root.take();
        let mut res = Memstore::upsert_cas(root, key, value, cas, seqno, lsm)?;
        let mut root = res[0].take().unwrap();
        root.set_black();

        self.root = Some(root);
        self.seqno = seqno;
        match res[1].take() {
            Some(old_node) => Ok(Some(*old_node)),
            None => { self.n_count += 1; Ok(None) },
        }
    }

    fn upsert_cas(
        node: Option<Box<Node<K,V>>>,
        key: K,
        value: V,
        cas: u64,
        seqno: u64,
        lsm: bool,
        ) -> Result<[Option<Box<Node<K,V>>>; 2], BognError>
    {
        if node.is_none() && cas > 0 {
            Err(BognError::InvalidCAS)

        } else if node.is_none() {
            let (access, black) = (0, false);
            let node = Box::new(Node::new(key, value, seqno, access, black));
            Ok([Some(node), None])

        } else {
            let mut node = node.unwrap();
            node = Memstore::walkdown_rot23(node);
            if node.key.gt(&key) {
                let n = node.left;
                let mut res = Memstore::upsert_cas(
                    n, key, value, cas, seqno, lsm)?;
                node.left = res[0].take();
                node = Memstore::walkuprot_23(node);
                Ok([Some(node), res[1].take()])

            } else if node.key.lt(&key) {
                let n = node.right;
                let mut res = Memstore::upsert_cas(
                    n, key, value, cas, seqno, lsm)?;
                node.right = res[0].take();
                node = Memstore::walkuprot_23(node);
                Ok([Some(node), res[1].take()])

            } else if node.is_deleted() && cas != 0 && cas != node.seqno() {
                Err(BognError::InvalidCAS)

            } else if !node.is_deleted() && cas != node.seqno() {
                Err(BognError::InvalidCAS)

            } else {
                let old_node = node.clone_detach();
                node.prepend_value(value, seqno, 0, /*access*/ lsm);
                node = Memstore::walkuprot_23(node);
                Ok([Some(node), Some(Box::new(old_node))])
            }
        }
    }

    /// Delete the given key from this intance, in LSM mode it simply marks
    /// the version as deleted. Note that back-to-back delete for the same
    /// key shall collapse into a single delete.
    pub fn delete<Q>(&mut self, key: &Q) -> Option<impl AsNode<K,V>>
    where
        K: Borrow<Q> + From<Q>,
        Q: Clone + Ord + ?Sized,
    {
        let seqno = self.seqno + 1;

        let deleted_node = if self.lsm {
            match self.delete_lsm(key, seqno) {
                res @ Some(_) => res,
                None => {
                    let mut root = Memstore::delete_insert(
                        self.root.take(), key, seqno, self.lsm).unwrap();
                    root.set_black();
                    self.root = Some(root);
                    self.n_count += 1;
                    None
                }
            }

        } else {
            let mut res = Memstore::do_delete(self.root.take(), key);
            self.root = res[0].take();
            if self.root.is_some() {
                self.root.as_mut().unwrap().set_black();
            }
            Some(*res[1].take().unwrap())
        };

        self.seqno = seqno;
        deleted_node
    }

    fn delete_lsm<Q>(&mut self, key: &Q, del_seqno: u64) -> Option<Node<K,V>>
    where
        K: Borrow<Q> + From<Q>,
        Q: Clone + Ord + ?Sized,
    {
        let mut node = &mut self.root;
        while node.is_some() {
            let nref = node.as_mut().unwrap();
            node = match nref.key.borrow().cmp(key) {
                Ordering::Less => &mut nref.right,
                Ordering::Equal => {
                    nref.delete(del_seqno, true /*true*/);
                    return Some(nref.clone_detach());
                },
                Ordering::Greater => &mut nref.left,
            };
        }
        None
    }

    fn delete_insert<Q>(
        node: Option<Box<Node<K,V>>>,
        key: &Q,
        seqno: u64,
        lsm: bool) -> Option<Box<Node<K,V>>>
    where
        K: Borrow<Q> + From<Q>,
        Q: Clone + Ord + ?Sized,
    {
        if node.is_none() {
            let (access, black) = (0, false);
            let key = key.clone().into();
            let value = Default::default();
            let mut node = Node::new(key, value, seqno, access, black);
            node.delete(seqno, lsm);
            Some(Box::new(node))

        } else {
            let mut node = node.unwrap();
            node = Memstore::walkdown_rot23(node);
            if node.key.borrow().gt(&key) {
                node.left = Memstore::delete_insert(node.left, key, seqno, lsm);
                Some(Memstore::walkuprot_23(node))

            } else if node.key.borrow().lt(&key) {
                node.right = Memstore::delete_insert(node.right, key, seqno, lsm);
                Some(Memstore::walkuprot_23(node))

            } else {
                panic!("delete_insert(): key already exist")
            }
        }
    }

    fn do_delete<Q>(node: Option<Box<Node<K,V>>>, key: &Q)
        -> [Option<Box<Node<K,V>>>; 2]
    where
        K: Borrow<Q> + From<Q>,
        Q: Clone + Ord + ?Sized,
    {
        if node.is_none() {
            return [None, None];
        }
        let mut node = node.unwrap();
        // TODO: optimize comparision let cmp = node.key.borrow().cmp(key).
        if node.key.borrow().gt(key) {
            if node.left.is_none() {
                return [Some(node), None];
            }
            if !is_red(&node.left) && !is_red(&node.left.as_ref().unwrap().left) {
                node = Memstore::move_red_left(node);
            }
            let mut res = Memstore::do_delete(node.left, key);
            node.left = res[0].take();
            [Some(Memstore::fixup(node)), res[1].take()]

        } else {
            if is_red(&node.left) {
                node = Memstore::rotate_right(node);
            }

            if !node.key.borrow().lt(key) && node.right.is_none() {
                return [None, Some(node)];
            }
            let ok = node.right.is_some() && !is_red(&node.right);
            if ok && !is_red(&node.right.as_ref().unwrap().left) {
                node = Memstore::move_red_right(node);
            }

            if !node.key.borrow().lt(key) { // node == key
                let mut res = Memstore::delete_min(node.right);
                node.right = res[0].take();
                if res[1].is_none() {
                    panic!("do_delete(): fatal logic, call the programmer");
                }
                let mut newnode = node.clone();
                newnode.left = node.left.take();
                node.right = node.right;
                newnode.black = node.black;
                let subdel = res[1].take();
                newnode.valn = subdel.unwrap().valn;
                [Some(Memstore::fixup(newnode)), Some(node)]
            } else {
                let mut res = Memstore::do_delete(node.right, key);
                node.right = res[0].take();
                [Some(Memstore::fixup(node)), res[1].take()]
            }
        }
    }

    fn delete_min(node: Option<Box<Node<K,V>>>) -> [Option<Box<Node<K,V>>>; 2] {
        if node.is_none() {
            return [None, None]
        }
        let mut node = node.unwrap();
        if node.left.is_none() {
            return [None, Some(node)]
        }
        if !is_red(&node.left) && !is_red(&node.left.as_ref().unwrap().left) {
            node = Memstore::move_red_left(node);
        }
        let mut res = Memstore::delete_min(node.left);
        node.left = res[0].take();
        [Some(Memstore::fixup(node)), res[1].take()]
    }

    /// validate llrb rules:
    /// a. No consecutive reds should be found in the tree.
    /// b. number of blacks should be same on both sides.
    pub fn validate(&self) {
        if self.root.is_none() {
            return
        }

        let (fromred, nblacks) = (is_red(&self.root), 0);
        Memstore::validate_tree(&self.root, fromred, nblacks);
    }

    fn validate_tree(
        node: &Option<Box<Node<K,V>>>,
        fromred: bool,
        mut nblacks: u64) -> u64
    {
        if node.is_none() {
            return nblacks
        }

        let red = is_red(node);
        if fromred && red {
            panic!("llrb_store: consecutive red spotted");
        }
        if !red {
            nblacks += 1;
        }
        let node = &node.as_ref().unwrap();
        let left = node.left.as_ref().unwrap();
        let right = node.right.as_ref().unwrap();
        let lblacks = Memstore::validate_tree(&node.left, red, nblacks);
        let rblacks = Memstore::validate_tree(&node.right, red, nblacks);
        if lblacks != rblacks {
            panic!(
                "llrb_store: unbalanced blacks left: {} and right: {}",
                lblacks, rblacks
            );
        }
        if node.left.is_some() {
            if left.key.ge(&node.key) {
                panic!("left key {:?} >= parent {:?}", left.key, node.key);
            }
        }
        if node.right.is_some() {
            if right.key.le(&node.key) {
                panic!("right key {:?} <= parent {:?}", right.key, node.key);
            }
        }
        lblacks
    }

    //--------- rotation routines for 2-3 algorithm ----------------

    fn walkdown_rot23(node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        node
    }

    fn walkuprot_23(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        if is_red(&node.right) && is_black(&node.left) {
            node = Memstore::rotate_left(node);
        }
        if is_red(&node.left) && is_red(&node.left.as_ref().unwrap().left) {
            node = Memstore::rotate_right(node);
        }
        if is_red(&node.left) && is_red(&node.right) {
            node = Memstore::flip(node)
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
        if is_black(&node.right) {
            panic!("rotateleft(): rotating a black link ? call the programmer");
        }
        let mut x = node.right.unwrap();
        node.right = x.left;
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
        if is_black(&node.left) {
            panic!("rotateright(): rotating a black link ? call the programmer")
        }
        let mut x = node.left.unwrap();
        node.left = x.right;
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
    // REQUIRE: Left and Right children must be present
    fn flip(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        node.left.as_mut().unwrap().toggle_link();
        node.right.as_mut().unwrap().toggle_link();
        node.toggle_link();
        node
    }

    fn fixup(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        if is_red(&node.right) {
            node = Memstore::rotate_left(node);
        }
        if is_red(&node.left) && is_red(&node.left.as_ref().unwrap().left) {
            node = Memstore::rotate_right(node);
        }
        if is_red(&node.left) && is_red(&node.right) {
            node = Memstore::flip(node);
        }
        node
    }

    // REQUIRE: Left and Right children must be present
    fn move_red_left(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        node = Memstore::flip(node);
        if is_red(&node.right.as_ref().unwrap().left) {
            node.right = Some(Memstore::rotate_right(node.right.take().unwrap()));
            node = Memstore::rotate_left(node);
            node = Memstore::flip(node);
        }
        node
    }

    // REQUIRE: Left and Right children must be present
    fn move_red_right(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        node = Memstore::flip(node);
        if is_red(&node.left.as_ref().unwrap().left) {
            node = Memstore::rotate_right(node);
            node = Memstore::flip(node);
        }
        node
    }
}

fn is_red<K, V>(node: &Option<Box<Node<K, V>>>) -> bool
where
    K: AsKey,
    V: Default + Clone,
{
    if node.is_none() {
        false
    } else {
        !is_black(node)
    }
}

fn is_black<K, V>(node: &Option<Box<Node<K, V>>>) -> bool
where
    K: AsKey,
    V: Default + Clone,
{
    if node.is_none() {
        true
    } else {
        node.as_ref().unwrap().is_black()
    }
}

//----------------------------------------------------------------------------

/// A single entry in Memstore can have mutiple version of values, ValueNode
/// represent each version.
#[derive(Clone)]
pub struct ValueNode<V>
where
    V: Default + Clone,
{
    data: V,                         // actual value
    seqno: u64,                      // when this version mutated
    deleted: Option<u64>,            // for lsm, deleted > 0
    prev: Option<Box<ValueNode<V>>>, // point to previous version
}

// Various operations on ValueNode, all are immutable operations.
impl<V> ValueNode<V>
where
    V: Default + Clone,
{
    fn new(
        data: V,
        seqno: u64,
        deleted: Option<u64>,
        prev: Option<Box<ValueNode<V>>>) -> ValueNode<V>
    {
        let mut vn: ValueNode<V> = Default::default();
        vn.data = data;
        vn.seqno = seqno;
        vn.deleted = deleted;
        vn.prev = prev;
        vn
    }

    fn clone_detach(&self) -> ValueNode<V> {
        ValueNode {
            data: self.data.clone(),
            seqno: self.seqno,
            deleted: self.deleted,
            prev: None
        }
    }

    #[inline]
    fn is_deleted(&self) -> bool {
        self.deleted.is_some()
    }

    fn delete(&mut self, seqno: u64) {
        // back-to-back deletes shall collapse
        self.deleted = Some(seqno);
    }

    fn undo(&mut self) -> bool {
        if self.deleted.is_some() {
            // collapsed deletes can be undone only once
            self.deleted = None;
            true
        } else if self.prev.is_none() {
            false
        } else {
            let source = self.prev.take().unwrap();
            self.clone_from(&source);
            true
        }
    }

    fn value_nodes(&self, acc: &mut Vec<ValueNode<V>>) {
        acc.push(self.clone());
        if self.prev.is_some() {
            self.prev.as_ref().unwrap().value_nodes(acc)
        }
    }
}

impl<V> Default for ValueNode<V>
where
    V: Default + Clone,
{
    fn default() -> ValueNode<V> {
        ValueNode {
            data: Default::default(),
            seqno: 0,
            deleted: None,
            prev: None,
        }
    }
}

impl<V> AsValue<V> for ValueNode<V>
where
    V: Default + Clone,
{
    fn value(&self) -> V {
        self.data.clone()
    }

    fn seqno(&self) -> u64 {
        if self.deleted.is_some() {
            self.deleted.unwrap()
        } else {
            self.seqno
        }
    }

    fn is_deleted(&self) -> bool {
        self.deleted.is_some()
    }
}

/// Node corresponds to a single entry in Memstore instance.
#[derive(Clone)]
pub struct Node<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    key: K,
    valn: ValueNode<V>,
    access: u64,                    // most recent access for this key
    black: bool,                    // store: black or red
    left: Option<Box<Node<K, V>>>,  // store: left child
    right: Option<Box<Node<K, V>>>, // store: right child
}

// Primary operations on a single node.
impl<K, V> Node<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    // CREATE operation
    fn new(key: K, value: V, seqno: u64, access: u64, black: bool) -> Node<K, V> {
        let mut node: Node<K, V> = Default::default();
        node.key = key;
        node.valn = ValueNode::new(value, seqno, None, None);
        node.access = access;
        node.black = black;
        node
    }

    fn clone_detach(&self) -> Node<K,V> {
        Node {
            key: self.key.clone(),
            valn: self.valn.clone_detach(),
            access: self.access,
            black: false,
            left: None,
            right: None,
        }
    }

    // prepend operation, equivalent to SET / INSERT / UPDATE
    fn prepend_value(&mut self, value: V, seqno: u64, access: u64, lsm: bool) {
        let prev = if lsm {
            Some(Box::new(self.valn.clone()))
        } else {
            None
        };
        self.valn = ValueNode::new(value, seqno, None, prev);
        self.access = access;
    }

    // DELETE operation
    fn delete(&mut self, seqno: u64, _lsm: bool) {
        self.valn.delete(seqno)
    }

    // UNDO operation
    fn undo(&mut self, lsm: bool) -> bool {
        if lsm {
            self.valn.undo()
        } else {
            false
        }
    }

    #[inline]
    fn set_red(&mut self) {
        self.black = false
    }

    #[inline]
    fn set_black(&mut self) {
        self.black = true
    }

    #[inline]
    fn toggle_link(&mut self) {
        self.black = !self.black
    }

    #[inline]
    fn is_black(&self) -> bool {
        self.black
    }
}

impl<K, V> Default for Node<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    fn default() -> Node<K, V> {
        Node {
            key: Default::default(),
            valn: Default::default(),
            access: 0,
            black: false,
            left: None,
            right: None,
        }
    }
}

impl<K,V> AsNode<K,V> for Node<K,V>
where
    K: AsKey,
    V: Default + Clone,
{
    type Value=ValueNode<V>;

    fn key(&self) -> K {
        self.key.clone()
    }

    fn value(&self) -> Self::Value {
        self.valn.clone()
    }

    fn versions(&self) -> Vec<Self::Value> {
        let mut acc: Vec<Self::Value> = vec![];
        self.valn.value_nodes(&mut acc);
        acc
    }

    fn seqno(&self) -> u64 {
        self.valn.seqno()
    }

    fn access(&self) -> u64 {
        self.access
    }

    fn is_deleted(&self) -> bool {
        self.valn.is_deleted()
    }
}

pub struct Iter<'a, K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    empty: bool,
    root: &'a Option<Box<Node<K, V>>>,
    node_iter: std::iter::Rev<std::vec::IntoIter<Node<K,V>>>,
    after_key: Bound<K>,
}

impl<'a,K,V> Iterator for Iter<'a,K,V>
where
    K: AsKey,
    V: Default + Clone,
{
    type Item=Node<K,V>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.empty {
            return None
        }
        match self.node_iter.next() {
            Some(item) => Some(item),
            None => {
                let mut acc: Vec<Node<K,V>> = vec![];
                scan(self.root, &self.after_key, 100, &mut acc);
                if acc.len() == 0 {
                    self.empty = true;
                    None
                } else {
                    self.after_key = Bound::Excluded(acc.last().unwrap().key());
                    self.node_iter = acc.into_iter().rev();
                    self.node_iter.next()
                }
            }
        }
    }
}

fn scan<K,V>(
    node: &Option<Box<Node<K,V>>>,
    key: &Bound<K>,
    limit: usize,
    acc: &mut Vec<Node<K,V>>) -> bool
where
    K: AsKey,
    V: Default + Clone,
{
    if node.is_none() {
        return true
    }
    let node = node.as_ref().unwrap();
    match key {
        Bound::Included(ky) => {
            if node.key.borrow().le(&ky) {
                return scan(&node.right, key, limit, acc)
            }
        },
        Bound::Excluded(ky) => {
            if node.key.borrow().le(&ky) {
                return scan(&node.right, key, limit, acc)
            }
        },
        _ => (),
    }
    if !scan(&node.left, key, limit, acc) {
        return false
    }
    acc.push(node.clone_detach());
    if acc.len() >= limit {
        return false
    }
    return scan(&node.right, key, limit, acc)
}
