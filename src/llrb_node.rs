use std::ops::Deref;

use crate::core::{AsDelta, AsEntry, Diff, Serialize};
use crate::error::BognError;
use crate::vlog;

/// A single entry in Llrb can have mutiple version of values, DeltaNode
/// represent the difference between this value and next value.
#[derive(Clone, Default)]
pub struct DeltaNode<V>
where
    V: Default + Clone + Diff + Serialize,
{
    delta: <V as Diff>::D, // actual value
    seqno: u64,            // when this version mutated
    deleted: Option<u64>,  // for lsm, deleted can be > 0
}

// Various operations on DeltaNode, all are immutable operations.
impl<V> DeltaNode<V>
where
    V: Default + Clone + Diff + Serialize,
{
    fn new(delta: <V as Diff>::D, seqno: u64, del: Option<u64>) -> DeltaNode<V> {
        DeltaNode {
            delta,
            seqno,
            deleted: del,
        }
    }
}

impl<V> AsDelta<V> for DeltaNode<V>
where
    V: Default + Clone + Diff + Serialize,
{
    #[inline]
    fn delta(&self) -> vlog::Delta<V> {
        vlog::Delta::new_delta(self.delta.clone())
    }

    #[inline]
    fn seqno(&self) -> u64 {
        self.deleted.unwrap_or(self.seqno)
    }

    #[inline]
    fn is_deleted(&self) -> bool {
        self.deleted.is_some()
    }
}

/// Node corresponds to a single entry in Llrb instance.
#[derive(Clone)]
pub struct Node<K, V>
where
    K: Clone + Ord,
    V: Default + Clone + Diff + Serialize,
{
    pub(crate) key: K,
    pub(crate) value: V,
    pub(crate) seqno: u64,
    pub(crate) deleted: Option<u64>,
    pub(crate) deltas: Vec<DeltaNode<V>>,
    pub(crate) black: bool,                    // store: black or red
    pub(crate) dirty: bool,                    // new node in mvcc path
    pub(crate) left: Option<Box<Node<K, V>>>,  // store: left child
    pub(crate) right: Option<Box<Node<K, V>>>, // store: right child
}

// Primary operations on a single node.
impl<K, V> Node<K, V>
where
    K: Clone + Ord,
    V: Default + Clone + Diff + Serialize,
{
    // CREATE operation
    pub(crate) fn new(k: K, v: V, seqno: u64, black: bool) -> Box<Node<K, V>> {
        let node = Box::new(Node {
            key: k,
            value: v,
            seqno,
            deleted: None,
            deltas: vec![],
            black,
            dirty: true,
            left: None,
            right: None,
        });
        //println!("new node {:p}", node);
        node
    }

    pub(crate) fn from_entry<E>(entry: E) -> Result<Box<Node<K, V>>, BognError>
    where
        E: AsEntry<K, V>,
        <E as AsEntry<K, V>>::Delta: Clone,
    {
        let black = false;
        let (key, value) = (entry.key(), entry.value().value()?);
        let mut node = Node::new(key, value, entry.seqno(), black);
        if entry.is_deleted() {
            node.deleted = Some(entry.seqno())
        }
        for e_delta in entry.deltas().into_iter() {
            let (delta, seqno) = (e_delta.delta().delta()?, e_delta.seqno());
            let del = if e_delta.is_deleted() {
                Some(seqno)
            } else {
                None
            };
            node.deltas.push(DeltaNode::new(delta, seqno, del));
        }
        Ok(node)
    }

    // unsafe clone for MVCC CoW
    pub(crate) fn mvcc_clone(
        &self,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> Box<Node<K, V>> {
        let new_node = Box::new(Node {
            key: self.key.clone(),
            value: self.value.clone(),
            seqno: self.seqno,
            deleted: self.deleted,
            deltas: self.deltas.clone(),
            black: self.black,
            dirty: self.dirty,
            left: self.left.as_ref().map(|n| n.duplicate()),
            right: self.right.as_ref().map(|n| n.duplicate()),
        });
        //println!("new node (mvcc) {:p} {:p}", self, new_node);
        reclaim.push(self.duplicate());
        new_node
    }

    #[inline]
    pub(crate) fn left_deref(&self) -> Option<&Node<K, V>> {
        self.left.as_ref().map(Deref::deref)
    }

    #[inline]
    pub(crate) fn right_deref(&self) -> Option<&Node<K, V>> {
        self.right.as_ref().map(Deref::deref)
    }

    // prepend operation, equivalent to SET / INSERT / UPDATE
    pub(crate) fn prepend_version(&mut self, value: V, seqno: u64, lsm: bool) {
        if lsm {
            let d = self.value.diff(&value);
            let delta = DeltaNode::new(d, self.seqno, self.deleted);
            self.deltas.push(delta);
            self.value = value;
            self.seqno = seqno;
            self.deleted = None;
        } else {
            self.value = value;
            self.seqno = seqno;
        }
    }

    // DELETE operation, back to back delete shall collapse
    #[inline]
    pub(crate) fn delete(&mut self, seqno: u64) {
        if self.deleted.is_none() {
            self.deleted = Some(seqno)
        }
    }

    #[inline]
    pub(crate) fn duplicate(&self) -> Box<Node<K, V>> {
        unsafe { Box::from_raw(self as *const Node<K, V> as *mut Node<K, V>) }
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

impl<K, V> Node<K, V>
where
    K: Clone + Ord,
    V: Default + Clone + Diff + Serialize,
{
    // leak nodes children.
    pub(crate) fn mvcc_detach(&mut self) {
        self.left.take().map(Box::leak);
        self.right.take().map(Box::leak);
    }

    // clone and detach this node from the tree.
    pub(crate) fn clone_detach(&self) -> Node<K, V> {
        Node {
            key: self.key.clone(),
            value: self.value.clone(),
            seqno: self.seqno,
            deleted: self.deleted,
            deltas: self.deltas.clone(),
            black: self.black,
            dirty: true,
            left: None,
            right: None,
        }
    }
}

impl<K, V> AsEntry<K, V> for Node<K, V>
where
    K: Clone + Ord,
    V: Default + Clone + Diff + Serialize,
{
    type Delta = DeltaNode<V>;

    #[inline]
    fn key(&self) -> K {
        self.key.clone()
    }

    #[inline]
    fn key_ref(&self) -> &K {
        &self.key
    }

    #[inline]
    fn value(&self) -> vlog::Value<V> {
        vlog::Value::new_value(self.value.clone())
    }

    #[inline]
    fn seqno(&self) -> u64 {
        self.deleted.unwrap_or(self.seqno)
    }

    #[inline]
    fn is_deleted(&self) -> bool {
        self.deleted.is_some()
    }

    #[inline]
    fn deltas(&self) -> Vec<Self::Delta> {
        self.deltas.clone()
    }
}

/// Fence recursive drops
impl<K, V> Drop for Node<K, V>
where
    K: Clone + Ord,
    V: Default + Clone + Diff + Serialize,
{
    fn drop(&mut self) {
        self.left.take().map(Box::leak);
        self.right.take().map(Box::leak);
    }
}
