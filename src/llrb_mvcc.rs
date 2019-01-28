use std::borrow::Borrow;
use std::cmp::Ordering;
use std::marker::PhantomData;
use std::ops::DerefMut;

use crate::error::BognError;
use crate::llrb::{is_black, is_red, Node};
use crate::traits::{AsEntry, AsKey};

pub struct Mvcc<K, V> {
    key: PhantomData<K>,
    value: PhantomData<V>,
}

impl<K, V> Mvcc<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    //pub fn set(
    //    llrb: &mut Llrb<K, V>, /* main index */
    //    key: K,
    //    value: V,
    //) -> Option<impl AsEntry<K, V>> {
    //    let seqno = llrb.seqno + 1;
    //    let root = llrb.root.take();

    //    let old_node = match Single::upsert(root, key, value, seqno, llrb.lsm) {
    //        (Some(mut root), old_node) => {
    //            root.set_black();
    //            llrb.root = Some(root);
    //            old_node
    //        }
    //        (None, old_node) => old_node,
    //    };

    //    llrb.seqno = seqno;
    //    if old_node.is_none() {
    //        llrb.n_count += 1;
    //    }
    //    old_node
    //}

    pub fn upsert(
        node: Option<&mut Node<K, V>>,
        key: K,
        value: V,
        seqno: u64,
        lsm: bool,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> (Option<Box<Node<K, V>>>, Option<Node<K, V>>) {
        if node.is_none() {
            let black = false;
            return (Some(Box::new(Node::new(key, value, seqno, black))), None);
        }

        let node = node.unwrap();
        let mut new_node = node.mvcc_clone(reclaim);
        //node = Mvcc::walkdown_rot23(node);

        let cmp = new_node.key.cmp(&key);
        if cmp == Ordering::Greater {
            let left = new_node.left_deref_mut();
            let (l, o) = Mvcc::upsert(left, key, value, seqno, lsm, reclaim);
            new_node.left = l;
            (Some(Mvcc::walkuprot_23(new_node, reclaim)), o)
        } else if cmp == Ordering::Less {
            let right = new_node.right_deref_mut();
            let (r, o) = Mvcc::upsert(right, key, value, seqno, lsm, reclaim);
            new_node.right = r;
            (Some(Mvcc::walkuprot_23(new_node, reclaim)), o)
        } else {
            let old_node = node.clone_detach();
            new_node.prepend_version(value, seqno, lsm);
            (Some(Mvcc::walkuprot_23(new_node, reclaim)), Some(old_node))
        }
    }

    pub fn upsert_cas(
        node: Option<&mut Node<K, V>>,
        key: K,
        val: V,
        cas: u64,
        seqno: u64,
        lsm: bool,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> (
        Option<Box<Node<K, V>>>,
        Option<Node<K, V>>,
        Option<BognError>,
    ) {
        if node.is_none() && cas > 0 {
            return (None, None, Some(BognError::InvalidCAS));
        } else if node.is_none() {
            let black = false;
            let node = Box::new(Node::new(key, val, seqno, black));
            return (Some(node), None, None);
        }

        let node = node.unwrap();
        let mut new_node = node.mvcc_clone(reclaim);
        // node = Mvcc::walkdown_rot23(node);

        let (r, cmp) = (reclaim, new_node.key.cmp(&key));
        let (old_node, err) = if cmp == Ordering::Greater {
            let (k, v, left) = (key, val, new_node.left_deref_mut());
            let (l, o, e) = Mvcc::upsert_cas(left, k, v, cas, seqno, lsm, r);
            new_node.left = l;
            (o, e)
        } else if cmp == Ordering::Less {
            let (k, v, right) = (key, val, new_node.right_deref_mut());
            let (rh, o, e) = Mvcc::upsert_cas(right, k, v, cas, seqno, lsm, r);
            new_node.right = rh;
            (o, e)
        } else if new_node.is_deleted() && cas != 0 && cas != new_node.seqno() {
            (None, Some(BognError::InvalidCAS))
        } else if !new_node.is_deleted() && cas != new_node.seqno() {
            (None, Some(BognError::InvalidCAS))
        } else {
            let old_node = node.clone_detach();
            new_node.prepend_version(val, seqno, lsm);
            (Some(old_node), None)
        };

        return (Some(Mvcc::walkuprot_23(new_node, r)), old_node, err);
    }

    pub fn delete_insert<Q>(
        node: Option<&mut Node<K, V>>,
        key: &Q,
        seqno: u64,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> (Option<Box<Node<K, V>>>, Option<Node<K, V>>)
    where
        K: Borrow<Q> + From<Q>,
        Q: Clone + Ord + ?Sized,
    {
        if node.is_none() {
            let (key, black) = (key.clone().into(), false);
            let mut node = Node::new(key, Default::default(), seqno, black);
            node.delete(seqno, true /*lsm*/);
            return (Some(Box::new(node)), None);
        }

        let node = node.unwrap();
        let mut new_node = node.mvcc_clone(reclaim);
        //let mut node = Mvcc::walkdown_rot23(node.unwrap());

        let cmp = new_node.key.borrow().cmp(&key);
        let old_node = if cmp == Ordering::Greater {
            let left = new_node.left_deref_mut();
            let (l, old_node) = Mvcc::delete_insert(left, key, seqno, reclaim);
            new_node.left = l;
            old_node
        } else if cmp == Ordering::Less {
            let right = new_node.right_deref_mut();
            let (r, old_node) = Mvcc::delete_insert(right, key, seqno, reclaim);
            new_node.right = r;
            old_node
        } else {
            new_node.delete(seqno, true /*lsm*/);
            Some(node.clone_detach())
        };

        (Some(Mvcc::walkuprot_23(new_node, reclaim)), old_node)
    }

    // this is the non-lsm path.
    pub fn do_delete<Q>(
        node: Option<&mut Node<K, V>>,
        key: &Q,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> (Option<Box<Node<K, V>>>, Option<Box<Node<K, V>>>)
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut node = match node {
            None => return (None, None),
            Some(node) => node,
        };

        let mut new_node = node.mvcc_clone(reclaim);

        if new_node.key.borrow().gt(key) {
            if new_node.left.is_none() {
                // key not present, nothing to delete
                (Some(new_node), None)
            } else {
                let ok = !is_red(new_node.left_deref());
                if ok && !is_red(new_node.left.as_ref().unwrap().left_deref()) {
                    new_node = Mvcc::move_red_left(new_node, reclaim);
                }
                let left = new_node.left_deref_mut();
                let res = Mvcc::do_delete(left, key, reclaim);
                new_node.left = res.0;
                (Some(Mvcc::fixup(new_node, reclaim)), res.1)
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
                let right = new_node.right_deref_mut();
                let mut res = Mvcc::delete_min(right, reclaim);
                new_node.right = res.0;
                if res.1.is_none() {
                    panic!("do_delete(): fatal logic, call the programmer");
                }
                let mut newnode = res.1.take().unwrap();
                newnode.left = new_node.left.take();
                newnode.right = new_node.right.take();
                newnode.black = new_node.black;
                (Some(Mvcc::fixup(newnode, reclaim)), Some(new_node))
            } else {
                let right = new_node.right_deref_mut();
                let res = Mvcc::do_delete(right, key, reclaim);
                new_node.right = res.0;
                (Some(Mvcc::fixup(new_node, reclaim)), res.1)
            }
        }
    }

    // return [node, old_node]
    pub fn delete_min(
        node: Option<&mut Node<K, V>>,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> (Option<Box<Node<K, V>>>, Option<Box<Node<K, V>>>) {
        if node.is_none() {
            return (None, None);
        }

        let mut new_node = node.unwrap().mvcc_clone(reclaim);

        if new_node.left.is_none() {
            new_node.mvcc_detach();
            return (None, Some(new_node));
        }
        let left = new_node.left_deref();
        if !is_red(left) && !is_red(left.unwrap().left_deref()) {
            new_node = Mvcc::move_red_left(new_node, reclaim);
        }
        let left = new_node.left_deref_mut();
        let (left, old_node) = Mvcc::delete_min(left, reclaim);
        new_node.left = left;
        (Some(Mvcc::fixup(new_node, reclaim)), old_node)
    }

    ////--------- rotation routines for 2-3 algorithm ----------------

    pub fn walkdown_rot23(node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        node
    }

    pub fn walkuprot_23(
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
        let mut right = node.right_deref_mut().unwrap().mvcc_clone(reclaim);
        if is_black(Some(right.as_ref())) {
            panic!("rotateleft(): rotating a black link ? call the programmer");
        }
        node.right = right.left;
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
        let mut left = node.left_deref_mut().unwrap().mvcc_clone(reclaim);
        if is_black(Some(left.as_ref())) {
            panic!("rotateright(): rotating a black link ? call the programmer")
        }
        node.left = left.right;
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
        let mut left = node.left_deref_mut().unwrap().mvcc_clone(reclaim);
        let mut right = node.right_deref_mut().unwrap().mvcc_clone(reclaim);

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
        node = if is_red(node.right_deref()) {
            Mvcc::rotate_left(node, reclaim)
        } else {
            node
        };
        node = {
            let left = node.left_deref();
            if is_red(left) && is_red(left.unwrap().left_deref()) {
                Mvcc::rotate_right(node, reclaim)
            } else {
                node
            }
        };
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