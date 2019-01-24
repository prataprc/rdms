use std::cmp::{Ordering, Ord};
use std::borrow::Borrow;
use std::ops::Bound;

use crate::traits::{AsKey, AsValue, AsEntry};
use crate::error::BognError;

// TODO: Fuzzy testing
// TODO: Performance testing
// TODO: Sizing.
// TODO: Implement and document primitive types, std-types that can be used
// as key (K) / value (V) for Llrb.
// TODO: optimize comparison
// TODO: llrb_depth_histogram, as feature, to measure the depth of LLRB tree.


/// Llrb manage a single instance of in-memory sorted index using
/// [left-leaning-red-black][llrb] tree.
///
/// **[LSM mode]**: Llrb instance can support what is called as
/// log-structured-merge while mutating the tree. In simple terms, this
/// means that nothing shall be over-written in the tree and all the
/// mutations for the same key shall be preserved until they are undone or
/// purged. Although there is one exception to it, back-to-back deletes
/// will collapse into a no-op, only the first delete shall be ingested.
///
/// IMPORTANT: This tree is not thread safe.
///
/// [llrb]: https://en.wikipedia.org/wiki/Left-leaning_red-black_tree
/// [LSM mode]: https://en.wikipedia.org/wiki/Log-structured_merge-tree
pub struct Llrb<K, V>
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
        S: AsRef<str>
    {
        let store = Llrb {
            name: name.as_ref().to_string(),
            lsm,
            seqno: 0,
            root: None,
            n_count: 0,
        };
        store
    }

    /// Create a new instance of Llrb tree and load it with entries from
    /// `iter`. Note that iterator shall return items that implement [`AsEntry`].
    pub fn load_from<E>(
        name: String,
        iter: impl Iterator<Item=E>,
        lsm: bool
    ) -> Result<Llrb<K,V>, BognError>
    where
        E: AsEntry<K,V>,
        <E as AsEntry<K, V>>::Value: Default + Clone,
    {
        let mut store = Llrb::new(name, lsm);
        for entry in iter {
            let root = store.root.take();
            match store.load_entry(root, entry.key(), entry)? {
                Some(mut root) => {
                    root.set_black();
                    store.root = Some(root);
                },
                None => ()
            }
        }
        Ok(store)
    }

    fn load_entry<E>(
        &mut self,
        node: Option<Box<Node<K,V>>>,
        key: K,
        entry: E
    ) -> Result<Option<Box<Node<K,V>>>, BognError>
    where
        E: AsEntry<K,V>,
        <E as AsEntry<K, V>>::Value: Default + Clone,
    {
        if node.is_none() {
            let node: Node<K,V> = Node::from_entry(entry);
            self.seqno = node.seqno();
            self.n_count += if node.is_deleted() { 0 } else { 1 };
            Ok(Some(Box::new(node)))

        } else {
            let mut node = node.unwrap();
            node = Llrb::walkdown_rot23(node);
            if node.key.gt(&key) {
                node.left = self.load_entry(node.left, key, entry)?;
                Ok(Some(Llrb::walkuprot_23(node)))

            } else if node.key.lt(&key) {
                node.right = self.load_entry(node.right, key, entry)?;
                Ok(Some(Llrb::walkuprot_23(node)))

            } else {
                Err(BognError::DuplicateKey(format!("load_entry: {:?}", key)))
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

    /// Set current seqno.
    pub fn set_seqno(&mut self, seqno: u64) {
        self.seqno = seqno;
    }

    /// Return current seqno.
    pub fn get_seqno(&self) -> u64 {
        self.seqno
    }

    /// Return number of entries in this instance.
    pub fn count(&self) -> u64 {
        self.n_count
    }
}

/// CRUD operations on Llrb instance.
impl<K, V> Llrb<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    /// Get the latest version for key.
    pub fn get<Q>(&self, key: &Q) -> Option<impl AsEntry<K,V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut node = &self.root;
        while node.is_some() {
            let nref = node.as_ref().unwrap();
            node = match nref.key.borrow().cmp(key) {
                Ordering::Less => &nref.right,
                Ordering::Greater => &nref.left,
                Ordering::Equal => return Some(nref.clone_detach()),
            };
        }
        None
    }

    /// Return an iterator over all entries in this instance.
    pub fn iter(&self) -> Iter<K,V> {
        Iter::new(&self.root)
    }

    /// Range over all entries from low to high.
    pub fn range(&self, low: Bound<K>, high: Bound<K>) -> Range<K,V> {
        Range::new(&self.root, low, high)
    }

    /// Set a new entry into this instance. If key is already present, return
    /// the previous entry. In LSM mode, this will add a new version for the
    /// key.
    ///
    /// If an entry already exist for the, return the old-entry will all its
    /// versions.
    pub fn set(&mut self, key: K, value: V) -> Option<impl AsEntry<K,V>> {
        let seqno = self.seqno + 1;
        let root = self.root.take();

        let old_node = match self.upsert(root, key, value, seqno) {
            [Some(mut root), old_node] => {
                root.set_black();
                self.root = Some(root);
                old_node
            },
            [None, old_node] => old_node,
        };

        self.seqno = seqno;
        old_node.map(|old_node| *old_node)
    }

    fn upsert(
        &mut self,
        node: Option<Box<Node<K,V>>>,
        key: K,
        value: V,
        seqno: u64
    ) -> [Option<Box<Node<K,V>>>; 2]
    {
        if node.is_none() {
            let black = false;
            self.n_count += 1;
            return [Some(Box::new(Node::new(key, value, seqno, black))), None]
        }

        let mut node = node.unwrap();
        node = Llrb::walkdown_rot23(node);

        if node.key.gt(&key) {
            let mut res = self.upsert(node.left, key, value, seqno);
            node.left = res[0].take();
            node = Llrb::walkuprot_23(node);
            [Some(node), res[1].take()]

        } else if node.key.lt(&key) {
            let mut res = self.upsert(node.right, key, value, seqno);
            node.right = res[0].take();
            node = Llrb::walkuprot_23(node);
            [Some(node), res[1].take()]

        } else {
            let old_node = node.clone_detach();
            node.prepend_version(value, seqno, self.lsm);
            node = Llrb::walkuprot_23(node);
            [Some(node), Some(Box::new(old_node))]
        }
    }

    /// Set a new entry into this instance, only if entry's seqno matches the
    /// supplied CAS. Use CAS == 0 to enforce a create operation. If key is
    /// already present, return the previous entry. In LSM mode, this will add
    /// a new version for the key.
    pub fn set_cas(&mut self, key: K, value: V, cas: u64)
        -> Result<Option<impl AsEntry<K,V>>, BognError>
    {
        let seqno = self.seqno + 1;
        let root = self.root.take();

        match self.upsert_cas(root, key, value, cas, seqno) {
            ([root, _], Some(err)) => {
                self.root = root;
                Err(err)
            },
            ([Some(mut root), old_node], None) => {
                root.set_black();
                self.seqno = seqno;

                self.root = Some(root);
                Ok(old_node.map(|old_node| *old_node))
            },
            _ => panic!("set_cas: impossible case, call programmer"),
        }
    }

    fn upsert_cas(
        &mut self,
        node: Option<Box<Node<K,V>>>,
        key: K,
        value: V,
        cas: u64,
        seqno: u64
    ) -> ([Option<Box<Node<K,V>>>; 2], Option<BognError>)
    {
        if node.is_none() && cas > 0 {
            return ([None, None], Some(BognError::InvalidCAS))

        } else if node.is_none() {
            let black = false;
            self.n_count += 1;
            let node = Box::new(Node::new(key, value, seqno, black));
            return ([Some(node), None], None)

        }

        let mut node = node.unwrap();
        node = Llrb::walkdown_rot23(node);

        let (old_node, err) = if node.key.gt(&key) {
            let mut res = self.upsert_cas(node.left, key, value, cas, seqno);
            node.left = res.0[0].take();
            (res.0[1].take(), res.1)

        } else if node.key.lt(&key) {
            let mut res = self.upsert_cas(node.right, key, value, cas, seqno);
            node.right = res.0[0].take();
            (res.0[1].take(), res.1)

        } else if node.is_deleted() && cas != 0 && cas != node.seqno() {
            (None, Some(BognError::InvalidCAS))

        } else if !node.is_deleted() && cas != node.seqno() {
            (None, Some(BognError::InvalidCAS))

        } else {
            let old_node = node.clone_detach();
            node.prepend_version(value, seqno, self.lsm);
            (Some(Box::new(old_node)), None)
        };

        node = Llrb::walkuprot_23(node);
        return ([Some(node), old_node], err)
    }

    /// Delete the given key from this intance, in LSM mode it simply marks
    /// the version as deleted. Note that back-to-back delete for the same
    /// key shall collapse into a single delete.
    pub fn delete<Q>(&mut self, key: &Q) -> Option<impl AsEntry<K,V>>
    where
        K: Borrow<Q> + From<Q>,
        Q: Clone + Ord + ?Sized,
    {
        let seqno = self.seqno + 1;

        if self.lsm {
            let old_node = match self.delete_lsm(key, seqno) {
                // mark the node as deleted, and return the entry.
                Some(old_node) => Some(*old_node),
                // entry is not present, then insert a new
                // entry and mark the entry as deleted.
                None => {
                    let root = self.root.take();
                    let mut root = self.delete_insert(root, key, seqno).unwrap();
                    root.set_black();
                    self.root = Some(root);
                    self.n_count += 1;
                    None
                }
            };
            if old_node.is_some() {
                self.seqno = seqno;
            }
            return old_node
        }

        // in non-lsm mode remove the entry from the tree.
        let root = self.root.take();
        let (root, old_node) = match self.do_delete(root, key) {
            [None, old_node] => (None, old_node),
            [Some(mut root), old_node] => {
                root.set_black();
                (Some(root), old_node)
            },
        };
        self.root = root;
        if old_node.is_some() {
            self.n_count -= 1;
            self.seqno = seqno
        }
        old_node.map(|item| *item)
    }

    fn delete_lsm<Q>(&mut self, key: &Q, del_seqno: u64)
        -> Option<Box<Node<K,V>>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut node = &mut self.root;
        while node.is_some() {
            let nref = node.as_mut().unwrap();
            node = match nref.key.borrow().cmp(key) {
                Ordering::Less => &mut nref.right,
                Ordering::Equal => {
                    return if !nref.is_deleted() {
                        let old_node = nref.clone_detach();
                        nref.delete(del_seqno, true /*lsm*/);
                        Some(Box::new(old_node))
                    } else {
                        None
                    }
                },
                Ordering::Greater => &mut nref.left,
            };
        }
        None
    }

    fn delete_insert<Q>(
        &mut self,
        node: Option<Box<Node<K,V>>>,
        key: &Q,
        seqno: u64
    ) -> Option<Box<Node<K,V>>>
    where
        K: Borrow<Q> + From<Q>,
        Q: Clone + Ord + ?Sized,
    {
        if node.is_none() {
            let (key, black) = (key.clone().into(), false);
            let mut node = Node::new(key, Default::default(), seqno, black);
            node.delete(seqno, self.lsm);
            return Some(Box::new(node))

        }

        let mut node = node.unwrap();
        node = Llrb::walkdown_rot23(node);

        if node.key.borrow().gt(&key) {
            node.left = self.delete_insert(node.left, key, seqno);

        } else if node.key.borrow().lt(&key) {
            node.right = self.delete_insert(node.right, key, seqno);

        } else {
            panic!("delete_insert(): key already exist, call programmer")
        }

        Some(Llrb::walkuprot_23(node))
    }

    // this is the non-lsm path.
    fn do_delete<Q>(&mut self, node: Option<Box<Node<K,V>>>, key: &Q)
        -> [Option<Box<Node<K,V>>>; 2]
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        if node.is_none() {
            return [None, None];
        }

        let mut node = node.unwrap();

        if node.key.borrow().gt(key) {
            if node.left.is_none() {
                [Some(node), None]

            } else {
                let ok = !is_red(&node.left);
                if ok && !is_red(&node.left.as_ref().unwrap().left) {
                    node = Llrb::move_red_left(node);
                }
                let mut res = self.do_delete(node.left, key);
                node.left = res[0].take();
                [Some(Llrb::fixup(node)), res[1].take()]
            }

        } else {
            if is_red(&node.left) {
                node = Llrb::rotate_right(node);
            }

            if !node.key.borrow().lt(key) && node.right.is_none() {
                return [None, Some(node)];
            }

            let ok = node.right.is_some() && !is_red(&node.right);
            if ok && !is_red(&node.right.as_ref().unwrap().left) {
                node = Llrb::move_red_right(node);
            }

            if !node.key.borrow().lt(key) { // node == key
                let mut res = Llrb::delete_min(node.right);
                node.right = res[0].take();
                if res[1].is_none() {
                    panic!("do_delete(): fatal logic, call the programmer");
                }
                let subdel = res[1].take().unwrap();
                let mut newnode = Box::new(subdel.clone_detach());
                newnode.left = node.left.take();
                newnode.right = node.right.take();
                newnode.black = node.black;
                node.valn.prev = None; // just take one version before.
                [Some(Llrb::fixup(newnode)), Some(node)]

            } else {
                let mut res = self.do_delete(node.right, key);
                node.right = res[0].take();
                [Some(Llrb::fixup(node)), res[1].take()]
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
            node = Llrb::move_red_left(node);
        }
        let mut res = Llrb::delete_min(node.left);
        node.left = res[0].take();
        [Some(Llrb::fixup(node)), res[1].take()]
    }

    /// validate llrb rules:
    /// a. No consecutive reds should be found in the tree.
    /// b. number of blacks should be same on both sides.
    pub fn validate(&self) -> Result<(), BognError> {
        if self.root.is_some() {
            let (fromred, nblacks) = (is_red(&self.root), 0);
            Llrb::validate_tree(&self.root, fromred, nblacks)?;
        }
        Ok(())
    }

    fn validate_tree(
        node: &Option<Box<Node<K,V>>>,
        fromred: bool,
        mut nblacks: u64
    ) -> Result<u64, BognError>
    {
        if node.is_none() {
            return Ok(nblacks)
        }

        let red = is_red(node);
        if fromred && red {
            return Err(BognError::ConsecutiveReds);
        }
        if !red {
            nblacks += 1;
        }
        let node = &node.as_ref().unwrap();
        let lblacks = Llrb::validate_tree(&node.left, red, nblacks)?;
        let rblacks = Llrb::validate_tree(&node.right, red, nblacks)?;
        if lblacks != rblacks {
            let err = format!(
                "llrb_store: unbalanced blacks left: {} and right: {}",
                lblacks, rblacks,
            );
            return Err(BognError::UnbalancedBlacks(err));
        }
        if node.left.is_some() {
            let left = node.left.as_ref().unwrap();
            if left.key.ge(&node.key) {
                let err = format!(
                    "left key {:?} >= parent {:?}", left.key, node.key
                );
                return Err(BognError::SortError(err));
            }
        }
        if node.right.is_some() {
            let right = node.right.as_ref().unwrap();
            if right.key.le(&node.key) {
                let err = format!(
                    "right {:?} <= parent {:?}", right.key, node.key
                );
                return Err(BognError::SortError(err));
            }
        }
        Ok(lblacks)
    }

    //--------- rotation routines for 2-3 algorithm ----------------

    fn walkdown_rot23(node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        node
    }

    fn walkuprot_23(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        if is_red(&node.right) && !is_red(&node.left) {
            node = Llrb::rotate_left(node);
        }
        if is_red(&node.left) && is_red(&node.left.as_ref().unwrap().left) {
            node = Llrb::rotate_right(node);
        }
        if is_red(&node.left) && is_red(&node.right) {
            node = Llrb::flip(node)
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
            node = Llrb::rotate_left(node);
        }
        if is_red(&node.left) && is_red(&node.left.as_ref().unwrap().left) {
            node = Llrb::rotate_right(node);
        }
        if is_red(&node.left) && is_red(&node.right) {
            node = Llrb::flip(node);
        }
        node
    }

    // REQUIRE: Left and Right children must be present
    fn move_red_left(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        node = Llrb::flip(node);
        if is_red(&node.right.as_ref().unwrap().left) {
            node.right = Some(Llrb::rotate_right(node.right.take().unwrap()));
            node = Llrb::rotate_left(node);
            node = Llrb::flip(node);
        }
        node
    }

    // REQUIRE: Left and Right children must be present
    fn move_red_right(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        node = Llrb::flip(node);
        if is_red(&node.left.as_ref().unwrap().left) {
            node = Llrb::rotate_right(node);
            node = Llrb::flip(node);
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

impl<K,V> Clone for Llrb<K,V>
where
    K: AsKey,
    V: Default + Clone,
{
    fn clone(&self) -> Llrb<K,V> {
        let new_store = Llrb{
            name: self.name.clone(),
            lsm: self.lsm,
            seqno: self.seqno,
            n_count: self.n_count,
            root: self.root.clone(),
        };
        new_store
    }
}

//----------------------------------------------------------------------------

pub struct Iter<'a, K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    root: Option<&'a Box<Node<K, V>>>,
    node_iter: std::vec::IntoIter<Node<K,V>>,
    after_key: Bound<K>,
    limit: usize,
}

impl<'a,K,V> Iter<'a,K,V>
where
    K: AsKey,
    V: Default + Clone,
{
    fn new(root: &'a Option<Box<Node<K,V>>>) -> Iter<'a,K,V> {
        let mut iter = Iter{
            root: None,
            node_iter: vec![].into_iter(),
            after_key: Bound::Unbounded,
            limit: 100, // TODO: no magic number.
        };
        if root.is_some() {
            iter.root = Some(root.as_ref().unwrap())
        }
        iter
    }

    fn scan_iter(
        &mut self,
        node: Option<&Box<Node<K,V>>>,
        acc: &mut Vec<Node<K,V>>
    ) -> bool
    {
        if node.is_none() {
            return true
        }

        let node = node.unwrap();
        //println!("scan_iter {:?} {:?}", node.key, self.after_key);
        let (left, right) = (node.left.as_ref(), node.right.as_ref());
        match &self.after_key {
            Bound::Included(akey) | Bound::Excluded(akey) => {
                if node.key.borrow().le(akey) {
                    return self.scan_iter(right, acc);
                }
            },
            Bound::Unbounded => (),
        }

        //println!("left {:?} {:?}", node.key, self.after_key);
        if !self.scan_iter(left, acc) {
            return false
        }

        acc.push(node.clone_detach());
        //println!("push {:?} {}", self.after_key, acc.len());
        if acc.len() >= self.limit {
            return false
        }

        return self.scan_iter(right, acc)
    }
}

impl<'a,K,V> Iterator for Iter<'a,K,V>
where
    K: AsKey,
    V: Default + Clone,
{
    type Item=Node<K,V>;

    fn next(&mut self) -> Option<Self::Item> {
        //println!("yyy");
        if self.root.is_none() {
            return None
        }

        let node = self.node_iter.next();
        if node.is_some() {
            return node
        }

        let mut acc: Vec<Node<K,V>> = Vec::with_capacity(self.limit);
        self.scan_iter(self.root, &mut acc);

        if acc.len() == 0 {
            self.root = None;
            None
        } else {
            //println!("iter-next {}", acc.len());
            self.after_key = Bound::Excluded(acc.last().unwrap().key());
            self.node_iter = acc.into_iter();
            let node = self.node_iter.next();
            if node.is_none() {
                self.root = None
            }
            node
        }
    }
}

pub struct Range<'a, K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    root: Option<&'a Box<Node<K, V>>>,
    node_iter: std::vec::IntoIter<Node<K,V>>,
    low: Bound<K>,
    high: Bound<K>,
    limit: usize,
}


impl<'a,K,V> Range<'a,K,V>
where
    K: AsKey,
    V: Default + Clone,
{
    fn new(
        root: &'a Option<Box<Node<K,V>>>,
        low: Bound<K>,
        high: Bound<K>
    ) -> Range<'a,K,V>
    {
        let mut range = Range{
            root: None,
            node_iter: vec![].into_iter(),
            low,
            high,
            limit: 100, // TODO: no magic number.
        };
        if root.is_some() {
            range.root = Some(root.as_ref().unwrap())
        }
        range
    }

    pub fn rev(self) -> Reverse<'a,K,V> {
        Reverse::new(self.root, self.low, self.high)
    }

    fn range_iter(
        &mut self,
        node: Option<&Box<Node<K,V>>>,
        acc: &mut Vec<Node<K,V>>
    ) -> bool
    {
        if node.is_none() {
            return true
        }

        let node = node.unwrap();
        //println!("range_iter {:?} {:?}", node.key, self.low);
        let (left, right) = (node.left.as_ref(), node.right.as_ref());
        match &self.low {
            Bound::Included(qow) if node.key.lt(qow) => {
                return self.range_iter(right, acc);
            },
            Bound::Excluded(qow) if node.key.le(qow) => {
                return self.range_iter(right, acc);
            },
            _ => (),
        }

        //println!("left {:?} {:?}", node.key, self.low);
        if !self.range_iter(left, acc) {
            return false
        }

        acc.push(node.clone_detach());
        //println!("push {:?} {}", self.low, acc.len());
        if acc.len() >= self.limit {
            return false
        }

        return self.range_iter(right, acc)
    }
}

impl<'a,K,V> Iterator for Range<'a,K,V>
where
    K: AsKey,
    V: Default + Clone,
{
    type Item=Node<K,V>;

    fn next(&mut self) -> Option<Self::Item> {
        //println!("yyy");
        if self.root.is_none() {
            return None
        }

        let node = self.node_iter.next();
        let node = if node.is_none() {
            let mut acc: Vec<Node<K,V>> = Vec::with_capacity(self.limit);
            self.range_iter(self.root, &mut acc);
            if acc.len() > 0 {
                //println!("iter-next {}", acc.len());
                self.low = Bound::Excluded(acc.last().unwrap().key());
                self.node_iter = acc.into_iter();
                self.node_iter.next()
            } else {
                None
            }
        } else {
            node
        };

        if node.is_none() {
            self.root = None;
            return None
        }

        // handle upper limit
        let node = node.unwrap();
        //println!("llrb next {:?}", node.key);
        match &self.high {
            Bound::Unbounded => Some(node),
            Bound::Included(qigh) if node.key.le(qigh) => Some(node),
            Bound::Excluded(qigh) if node.key.lt(qigh) => Some(node),
            _ => {
                self.root = None;
                None
            }
        }
    }
}

pub struct Reverse<'a, K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    root: Option<&'a Box<Node<K, V>>>,
    node_iter: std::vec::IntoIter<Node<K,V>>,
    high: Bound<K>,
    low: Bound<K>,
    limit: usize,
}


impl<'a,K,V> Reverse<'a,K,V>
where
    K: AsKey,
    V: Default + Clone,
{
    fn new(
        root: Option<&'a Box<Node<K,V>>>,
        low: Bound<K>,
        high: Bound<K>
    ) -> Reverse<'a,K,V>
    {
        let mut reverse = Reverse{
            root: None,
            node_iter: vec![].into_iter(),
            low,
            high,
            limit: 100, // TODO: no magic number.
        };
        if root.is_some() {
            reverse.root = Some(root.as_ref().unwrap())
        }
        reverse
    }

    fn reverse_iter(
        &mut self,
        node: Option<&Box<Node<K,V>>>,
        acc: &mut Vec<Node<K,V>>
    ) -> bool
    {
        if node.is_none() {
            return true
        }

        let node = node.unwrap();
        //println!("reverse_iter {:?} {:?}", node.key, self.high);
        let (left, right) = (node.left.as_ref(), node.right.as_ref());
        match &self.high {
            Bound::Included(qigh) if node.key.gt(qigh) => {
                return self.reverse_iter(left, acc);
            },
            Bound::Excluded(qigh) if node.key.ge(qigh) => {
                return self.reverse_iter(left, acc);
            },
            _ => (),
        }

        //println!("left {:?} {:?}", node.key, self.high);
        if !self.reverse_iter(right, acc) {
            return false
        }

        acc.push(node.clone_detach());
        //println!("push {:?} {}", self.high, acc.len());
        if acc.len() >= self.limit {
            return false
        }

        return self.reverse_iter(left, acc)
    }
}

impl<'a,K,V> Iterator for Reverse<'a,K,V>
where
    K: AsKey,
    V: Default + Clone,
{
    type Item=Node<K,V>;

    fn next(&mut self) -> Option<Self::Item> {
        //println!("yyy");
        if self.root.is_none() {
            return None
        }

        let node = self.node_iter.next();
        let node = if node.is_none() {
            let mut acc: Vec<Node<K,V>> = Vec::with_capacity(self.limit);
            self.reverse_iter(self.root, &mut acc);
            if acc.len() > 0 {
                //println!("iter-next {}", acc.len());
                self.high = Bound::Excluded(acc.last().unwrap().key());
                self.node_iter = acc.into_iter();
                self.node_iter.next()
            } else {
                None
            }
        } else {
            node
        };

        if node.is_none() {
            self.root = None;
            return None
        }

        // handle lower limit
        let node = node.unwrap();
        //println!("llrb next {:?}", node.key);
        match &self.low {
            Bound::Unbounded => Some(node),
            Bound::Included(qow) if node.key.ge(qow) => Some(node),
            Bound::Excluded(qow) if node.key.gt(qow) => Some(node),
            _ => {
                //println!("llrb reverse over {:?}", &self.low);
                self.root = None;
                None
            }
        }
    }
}


//----------------------------------------------------------------------

/// A single entry in Llrb can have mutiple version of values, ValueNode
/// represent each version.
#[derive(Clone)]
pub struct ValueNode<V>
where
    V: Default + Clone,
{
    data: V,                         // actual value
    seqno: u64,                      // when this version mutated
    deleted: Option<u64>,            // for lsm, deleted can be > 0
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
        ValueNode{ data, seqno, deleted, prev }
    }

    // clone this version alone, detach it from previous versions.
    fn clone_detach(&self) -> ValueNode<V> {
        ValueNode {
            data: self.data.clone(),
            seqno: self.seqno,
            deleted: self.deleted,
            prev: None
        }
    }

    // detach individual versions and collect them in a vector.
    fn value_nodes(&self, acc: &mut Vec<ValueNode<V>>) {
        acc.push(self.clone_detach());
        if self.prev.is_some() {
            self.prev.as_ref().unwrap().value_nodes(acc)
        }
    }

    // mark this version as deleted, along with its seqno.
    fn delete(&mut self, seqno: u64) {
        // back-to-back deletes shall collapse
        self.deleted = Some(seqno);
    }

    #[allow(dead_code)]
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
        match self.deleted {
            Some(seqno) => seqno,
            None => self.seqno,
        }
    }

    fn is_deleted(&self) -> bool {
        self.deleted.is_some()
    }
}

/// Node corresponds to a single entry in Llrb instance.
#[derive(Clone)]
pub struct Node<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    key: K,
    valn: ValueNode<V>,
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
    fn new(key: K, value: V, seqno: u64, black: bool) -> Node<K, V> {
        let valn = ValueNode::new(value, seqno, None, None);
        Node{ key, valn, black, left: None, right: None }
    }

    fn from_entry<E>(entry: E) -> Node<K,V>
    where
        E: AsEntry<K,V>,
        <E as AsEntry<K, V>>::Value: Default + Clone,
    {
        let asvalue = entry.value();
        let valn = ValueNode::new(asvalue.value(), asvalue.seqno(), None, None);
        Node{ key: entry.key(), valn, black: false, left: None, right: None }
    }

    // clone and detach this node from the tree.
    fn clone_detach(&self) -> Node<K,V> {
        Node {
            key: self.key.clone(),
            valn: self.valn.clone(),
            black: false,
            left: None,
            right: None,
        }
    }

    // prepend operation, equivalent to SET / INSERT / UPDATE
    fn prepend_version(&mut self, value: V, seqno: u64, lsm: bool) {
        let prev = if lsm {
            Some(Box::new(self.valn.clone()))
        } else {
            None
        };
        self.valn = ValueNode::new(value, seqno, None, prev);
    }

    // DELETE operation
    fn delete(&mut self, seqno: u64, _lsm: bool) {
        self.valn.delete(seqno)
    }

    // UNDO operation
    #[allow(dead_code)]
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
            black: false,
            left: None,
            right: None,
        }
    }
}

impl<K,V> AsEntry<K,V> for Node<K,V>
where
    K: AsKey,
    V: Default + Clone,
{
    type Value=ValueNode<V>;

    fn key(&self) -> K {
        self.key.clone()
    }

    fn value(&self) -> Self::Value {
        self.valn.clone_detach()
    }

    fn versions(&self) -> Vec<Self::Value> {
        let mut acc: Vec<Self::Value> = vec![];
        self.valn.value_nodes(&mut acc);
        acc
    }

    fn seqno(&self) -> u64 {
        self.valn.seqno()
    }

    fn is_deleted(&self) -> bool {
        self.valn.is_deleted()
    }
}
