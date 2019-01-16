use std::cmp::{Ordering, PartialEq, PartialOrd};

use crate::traits::{KeyTrait, NodeTrait, Serialize, ValueTrait};

/// Llrb to manage a single instance of in-memory sorted index using
/// left-leaning-red-black tree.
///
/// IMPORTANT: This tree is not thread safe.
struct Llrb<K, V>
where
    K: KeyTrait,
    V: ValueTrait,
{
    name: String,
    root: Option<Node<K, V>>,
    seqno: u64, // seqno so far, starts from 0 and incr for every mutation
                // TODO: llrb_depth_histogram, as a feature, to measure the depth of LLRB tree.
}

impl<K, V> Llrb<K, V>
where
    K: KeyTrait,
    V: ValueTrait,
{
    // create a new instance of Llrb
    fn new(name: String, seqno: u64) -> Llrb<K, V> {
        let llrb = Llrb {
            name,
            seqno,
            root: None,
        };
        // TODO: llrb.inittxns()
        llrb
    }

    //    fn load_from<N,K,V>(name: String, iter: Iterator<Item=N>)
    //    where
    //        N: NodeTrait<K,V>
    //    {
    //        let mut llrb = Llrb::new(name, 0);
    //        for node in iter {
    //            llrb.seqno = node.get_seqno();
    //            if node.is_deleted() {
    //                llrb.delete(node.get_key(), None, true /*lsm*/);
    //            }
    //        }
    //    }

    //--------- rotation routines for 2-3 algorithm ----------------

    fn walkdown_rot23(node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        node
    }

    fn walkuprot_23(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        if is_red(&node.right) && is_black(&node.left) {
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
        let mut x = node.right;
        if is_black(&x) {
            panic!("rotateleft(): rotating a black link ? call the programmer");
        }
        let mut x = x.unwrap();
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
        let mut x = node.left;
        if is_black(&x) {
            panic!("rotateright(): rotating a black link ? call the programmer")
        }
        let mut x = x.unwrap();
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
    K: KeyTrait,
    V: ValueTrait,
{
    if node.is_none() {
        false
    } else {
        !is_black(node)
    }
}

fn is_black<K, V>(node: &Option<Box<Node<K, V>>>) -> bool
where
    K: KeyTrait,
    V: ValueTrait,
{
    if node.is_none() {
        true
    } else {
        node.as_ref().unwrap().is_black()
    }
}

//----------------------------------------------------------------------------

pub(crate) enum ValueResult<V> {
    Valid(V),
    Deleted(V),
}

#[derive(Clone)]
pub(crate) struct ValueNode<V>
where
    V: Default + Clone + Serialize,
{
    data: V,                         // actual value
    seqno: u64,                      // when this version mutated
    prev: Option<Box<ValueNode<V>>>, // point to previous version
    deleted: bool,                   // for lsm, mark this version deleted
}

// Various operations on ValueNode, all are immutable operations.
impl<V> ValueNode<V>
where
    V: Default + Clone + Serialize,
{
    fn new(v: V, seqno: u64, prev: Option<Box<ValueNode<V>>>) -> ValueNode<V> {
        let mut vn: ValueNode<V> = Default::default();
        vn.data = v;
        vn.seqno = seqno;
        vn.prev = match prev {
            Some(prev) => Some(prev),
            None => None,
        };
        vn
    }

    fn delete(&mut self) {
        self.deleted = true;
    }

    fn undo(&mut self) -> bool {
        if self.deleted {
            self.deleted = false;
            true
        } else {
            match &self.prev {
                Some(prev) => {
                    *self = *(prev.clone());
                    true
                }
                None => false,
            }
        }
    }

    #[inline]
    fn is_deleted(&self) -> bool {
        self.deleted
    }

    #[inline]
    fn get_value(&self) -> ValueResult<V> {
        if self.is_deleted() {
            ValueResult::Deleted(self.data.clone())
        } else {
            ValueResult::Valid(self.data.clone())
        }
    }

    #[inline]
    fn get_seqno(&self) -> u64 {
        self.seqno
    }

    fn get_values(&self, acc: &mut Vec<ValueResult<V>>) {
        acc.push(self.get_value());
        if let Some(v) = self.prev.clone() {
            v.get_values(acc)
        }
    }

    fn value_nodes(&self, acc: &mut Vec<ValueNode<V>>) {
        acc.push(self.clone());
        if let Some(v) = self.prev.clone() {
            v.value_nodes(acc)
        }
    }
}

impl<V> Default for ValueNode<V>
where
    V: Default + Clone + Serialize,
{
    fn default() -> ValueNode<V> {
        ValueNode {
            data: Default::default(),
            seqno: 0,
            prev: None,
            deleted: false,
        }
    }
}

#[derive(Clone)]
pub(crate) struct Node<K, V>
where
    K: KeyTrait,
    V: ValueTrait,
{
    key: K,
    valn: ValueNode<V>,
    seqno: u64,                     // most recent mutation on this key
    access: u64,                    // most recent access for this key
    black: bool,                    // llrb: black or red
    left: Option<Box<Node<K, V>>>,  // llrb: left child
    right: Option<Box<Node<K, V>>>, // llrb: right child
}

// Primary operations on a single node.
impl<K, V> Node<K, V>
where
    K: KeyTrait,
    V: ValueTrait,
{
    // CREATE operation
    fn new(key: K, value: V, seqno: u64, access: u64) -> Node<K, V> {
        let mut node: Node<K, V> = Default::default();
        node.key = key;
        node.valn = ValueNode::new(value, seqno, None);
        node.seqno = seqno;
        node.access = access;
        node
    }

    // prepend operation, equivalent to SET / INSERT / UPDATE
    fn prepend_value(&mut self, value: V, seqno: u64, access: u64, lsm: bool) {
        let prev = if lsm {
            Some(Box::new(self.valn.clone()))
        } else {
            None
        };
        self.valn = ValueNode::new(value, seqno, prev);
        self.seqno = seqno;
        self.access = self.access;
    }

    // DELETE operation
    fn delete(&mut self, _lsm: bool) {
        self.valn.delete()
    }

    // UNDO operation
    fn undo(&mut self, lsm: bool) -> bool {
        if lsm {
            self.valn.undo()
        } else {
            false
        }
    }

    // GET operation
    fn get_value(&self) -> ValueResult<V> {
        self.valn.get_value()
    }

    // GETLOG operation
    fn get_values(&self, acc: &mut Vec<ValueResult<V>>) {
        self.valn.get_values(acc)
    }

    // GETLOG operation
    fn value_nodes(&self, acc: &mut Vec<ValueNode<V>>) {
        self.valn.value_nodes(acc)
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

    #[inline]
    fn is_deleted(&self) -> bool {
        self.valn.is_deleted()
    }
}

impl<K, V> Default for Node<K, V>
where
    K: KeyTrait,
    V: ValueTrait,
{
    fn default() -> Node<K, V> {
        Node {
            key: Default::default(),
            valn: Default::default(),
            seqno: 0,
            access: 0,
            black: false,
            left: None,
            right: None,
        }
    }
}

impl<K, V> PartialEq for Node<K, V>
where
    K: KeyTrait,
    V: ValueTrait,
{
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<K, V> PartialOrd for Node<K, V>
where
    K: KeyTrait,
    V: ValueTrait,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.key.partial_cmp(&other.key)
    }
}
