use std::ops::Deref;

use crate::core::{self, Diff, Serialize};

/// Node corresponds to a single entry in Llrb instance.
#[derive(Clone)]
pub struct Node<K, V>
where
    K: Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
{
    pub(crate) entry: core::Entry<K, V>,
    pub(crate) black: bool,                    // store: black or red
    pub(crate) dirty: bool,                    // new node in mvcc path
    pub(crate) left: Option<Box<Node<K, V>>>,  // store: left child
    pub(crate) right: Option<Box<Node<K, V>>>, // store: right child
}

// Primary operations on a single node.
impl<K, V> Node<K, V>
where
    K: Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
{
    // CREATE operation
    pub(crate) fn new(k: K, v: V, seqno: u64, black: bool) -> Box<Node<K, V>> {
        let node = Box::new(Node {
            entry: core::Entry::new_native(k, v, seqno),
            black,
            dirty: true,
            left: None,
            right: None,
        });
        //println!("new node {:p}", node);
        node
    }

    // unsafe clone for MVCC CoW
    pub(crate) fn mvcc_clone(
        &self,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> Box<Node<K, V>> {
        let new_node = Box::new(Node {
            entry: self.entry.clone(),
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
        self.entry.prepend_version(value, seqno, lsm)
    }

    // DELETE operation, back to back delete shall collapse
    #[inline]
    pub(crate) fn delete(&mut self, seqno: u64) {
        self.entry.delete(seqno)
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

    pub(crate) fn key_ref(&self) -> &K {
        &self.entry.key_ref()
    }

    pub(crate) fn seqno(&self) -> u64 {
        self.entry.seqno()
    }

    pub(crate) fn is_deleted(&self) -> bool {
        self.entry.is_deleted()
    }
}

impl<K, V> Node<K, V>
where
    K: Clone + Ord + Serialize,
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
            entry: self.entry.clone(),
            black: self.black,
            dirty: true,
            left: None,
            right: None,
        }
    }
}

/// Fence recursive drops
impl<K, V> Drop for Node<K, V>
where
    K: Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
{
    fn drop(&mut self) {
        self.left.take().map(Box::leak);
        self.right.take().map(Box::leak);
    }
}

impl<K, V> From<core::Entry<K, V>> for Node<K, V>
where
    K: Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
{
    fn from(entry: core::Entry<K, V>) -> Node<K, V> {
        Node {
            entry,
            black: false,
            dirty: true,
            left: None,
            right: None,
        }
    }
}
