use std::ops::{Deref, DerefMut};

use crate::traits::{AsEntry, AsKey, AsValue};

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
        prev: Option<Box<ValueNode<V>>>,
    ) -> ValueNode<V> {
        ValueNode {
            data,
            seqno,
            deleted,
            prev,
        }
    }

    // clone this version alone, detach it from previous versions.
    fn clone_detach(&self) -> ValueNode<V> {
        ValueNode {
            data: self.data.clone(),
            seqno: self.seqno,
            deleted: self.deleted,
            prev: None,
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
    pub(crate) key: K,
    pub(crate) valn: ValueNode<V>,
    pub(crate) black: bool,                    // store: black or red
    pub(crate) left: Option<Box<Node<K, V>>>,  // store: left child
    pub(crate) right: Option<Box<Node<K, V>>>, // store: right child
}

// Primary operations on a single node.
impl<K, V> Node<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    // CREATE operation
    pub(crate) fn new(key: K, value: V, seqno: u64, black: bool) -> Node<K, V> {
        let valn = ValueNode::new(value, seqno, None, None);
        Node {
            key,
            valn,
            black,
            left: None,
            right: None,
        }
    }

    pub(crate) fn from_entry<E>(entry: E) -> Node<K, V>
    where
        E: AsEntry<K, V>,
        <E as AsEntry<K, V>>::Value: Default + Clone,
    {
        let asvalue = entry.value();
        let valn = ValueNode::new(asvalue.value(), asvalue.seqno(), None, None);
        Node {
            key: entry.key(),
            valn,
            black: false,
            left: None,
            right: None,
        }
    }

    // clone and detach this node from the tree.
    pub(crate) fn clone_detach(&self) -> Node<K, V> {
        Node {
            key: self.key.clone(),
            valn: self.valn.clone(),
            black: self.black,
            left: None,
            right: None,
        }
    }

    pub(crate) fn mvcc_detach(&mut self) {
        match self.left.take() {
            Some(box_node) => {
                Box::leak(box_node);
            }
            None => (),
        };
        match self.right.take() {
            Some(box_node) => {
                Box::leak(box_node);
            }
            None => (),
        };
    }

    // unsafe clone for MVCC COW
    pub(crate) fn mvcc_clone(
        &mut self,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> Box<Node<K, V>> {
        let mut new_node = Node {
            key: self.key.clone(),
            valn: self.valn.clone(),
            black: self.black,
            left: None,
            right: None,
        };
        if self.left.is_some() {
            let ref_node = self.left.as_mut().unwrap().deref_mut();
            new_node.left = unsafe { Some(Box::from_raw(ref_node)) };
        }
        if self.right.is_some() {
            let ref_node = self.right.as_mut().unwrap().deref_mut();
            new_node.right = unsafe { Some(Box::from_raw(ref_node)) };
        }

        reclaim.push(unsafe { Box::from_raw(self) });

        Box::new(new_node)
    }

    pub(crate) fn left_deref(&self) -> Option<&Node<K, V>> {
        self.left.as_ref().map(|item| item.deref())
    }

    pub(crate) fn right_deref(&self) -> Option<&Node<K, V>> {
        self.right.as_ref().map(|item| item.deref())
    }

    pub(crate) fn left_deref_mut(&mut self) -> Option<&mut Node<K, V>> {
        self.left.as_mut().map(|item| item.deref_mut())
    }

    pub(crate) fn right_deref_mut(&mut self) -> Option<&mut Node<K, V>> {
        self.right.as_mut().map(|item| item.deref_mut())
    }

    // prepend operation, equivalent to SET / INSERT / UPDATE
    pub(crate) fn prepend_version(&mut self, value: V, seqno: u64, lsm: bool) {
        let prev = if lsm {
            Some(Box::new(self.valn.clone()))
        } else {
            None
        };
        self.valn = ValueNode::new(value, seqno, None, prev);
    }

    // DELETE operation
    pub(crate) fn delete(&mut self, seqno: u64, _lsm: bool) {
        self.valn.delete(seqno)
    }

    // UNDO operation
    #[allow(dead_code)]
    pub(crate) fn undo(&mut self, lsm: bool) -> bool {
        if lsm {
            self.valn.undo()
        } else {
            false
        }
    }

    #[inline]
    pub(crate) fn set_red(&mut self) {
        self.black = false
    }

    #[inline]
    pub(crate) fn set_black(&mut self) {
        self.black = true
    }

    #[inline]
    pub(crate) fn toggle_link(&mut self) {
        self.black = !self.black
    }

    #[inline]
    pub(crate) fn is_black(&self) -> bool {
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

impl<K, V> AsEntry<K, V> for Node<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    type Value = ValueNode<V>;

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
