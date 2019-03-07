use std::ops::Deref;

use crate::traits::{AsEntry, AsVersion};

/// A single entry in Llrb can have mutiple version of values, Version
/// represent each version.
#[derive(Clone, Default)]
pub struct Version<V>
where
    V: Default + Clone,
{
    data: V,                       // actual value
    seqno: u64,                    // when this version mutated
    deleted: Option<u64>,          // for lsm, deleted can be > 0
    prev: Option<Box<Version<V>>>, // point to previous version
}

// Various operations on Version, all are immutable operations.
impl<V> Version<V>
where
    V: Default + Clone,
{
    fn new(data: V, seqno: u64, deleted: Option<u64>, prev: Option<Box<Version<V>>>) -> Version<V> {
        Version {
            data,
            seqno,
            deleted,
            prev,
        }
    }

    // clone this version alone, detach it from previous versions.
    fn clone_detach(&self) -> Version<V> {
        Version {
            data: self.data.clone(),
            seqno: self.seqno,
            deleted: self.deleted,
            prev: None,
        }
    }

    // detach individual versions, from latest to oldest, and collect
    // them in a vector.
    fn value_nodes(&self, acc: &mut Vec<Version<V>>) {
        acc.push(self.clone_detach());
        self.prev.as_ref().map(|v| v.value_nodes(acc));
    }

    // mark this version as deleted, along with its seqno.
    fn delete(&mut self, seqno: u64) {
        self.deleted = Some(seqno); // back-to-back deletes shall collapse
    }

    #[allow(dead_code)]
    fn undo(&mut self) -> bool {
        if self.deleted.is_some() {
            self.deleted = None; // collapsed deletes can be undone only once
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

impl<V> AsVersion<V> for Version<V>
where
    V: Default + Clone,
{
    #[inline]
    fn value(&self) -> V {
        self.data.clone()
    }

    #[inline]
    fn seqno(&self) -> u64 {
        self.deleted.map_or(self.seqno, |seqno| seqno)
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
    K: Default + Clone + Ord,
    V: Default + Clone,
{
    pub(crate) key: K,
    pub(crate) valn: Version<V>,
    pub(crate) black: bool,                    // store: black or red
    pub(crate) dirty: bool,                    // new node in mvcc path
    pub(crate) left: Option<Box<Node<K, V>>>,  // store: left child
    pub(crate) right: Option<Box<Node<K, V>>>, // store: right child
}

// Primary operations on a single node.
impl<K, V> Node<K, V>
where
    K: Default + Clone + Ord,
    V: Default + Clone,
{
    // CREATE operation
    pub(crate) fn new(key: K, value: V, seqno: u64, black: bool) -> Box<Node<K, V>> {
        let node = Box::new(Node {
            key,
            valn: Version::new(value, seqno, None, None),
            black,
            dirty: true,
            left: None,
            right: None,
        });
        //println!("new node {:p}", node);
        node
    }

    pub(crate) fn from_entry<E>(entry: E) -> Box<Node<K, V>>
    where
        E: AsEntry<K, V>,
        <E as AsEntry<K, V>>::Version: Default + Clone,
    {
        let black = false;
        Node::new(entry.key(), entry.value(), entry.seqno(), black)
    }

    // clone and detach this node from the tree.
    pub(crate) fn clone_detach(&self) -> Node<K, V> {
        Node {
            key: self.key.clone(),
            valn: self.valn.clone(),
            black: self.black,
            dirty: true,
            left: None,
            right: None,
        }
    }

    pub(crate) fn mvcc_detach(&mut self) {
        self.left.take().map(|box_node| Box::leak(box_node));
        self.right.take().map(|box_node| Box::leak(box_node));
    }

    // unsafe clone for MVCC COW
    pub(crate) fn mvcc_clone(
        &self,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> Box<Node<K, V>> {
        let new_node = Box::new(Node {
            key: self.key.clone(),
            valn: self.valn.clone(),
            black: self.black,
            dirty: self.dirty,
            left: self.left_deref().map(|n| n.duplicate()),
            right: self.right_deref().map(|n| n.duplicate()),
        });
        //println!("new node (mvcc) {:p} {:p}", self, new_node);
        reclaim.push(self.duplicate());
        new_node
    }

    #[inline]
    pub(crate) fn left_deref(&self) -> Option<&Node<K, V>> {
        self.left.as_ref().map(|item| item.deref())
    }

    #[inline]
    pub(crate) fn right_deref(&self) -> Option<&Node<K, V>> {
        self.right.as_ref().map(|item| item.deref())
    }

    // prepend operation, equivalent to SET / INSERT / UPDATE
    pub(crate) fn prepend_version(&mut self, value: V, seqno: u64, lsm: bool) {
        let prev = if lsm {
            Some(Box::new(self.valn.clone()))
        } else {
            None
        };
        self.valn = Version::new(value, seqno, None, prev);
    }

    // DELETE operation
    #[inline]
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

impl<K, V> Default for Node<K, V>
where
    K: Default + Clone + Ord,
    V: Default + Clone,
{
    fn default() -> Node<K, V> {
        Node {
            key: Default::default(),
            valn: Default::default(),
            black: false,
            dirty: true,
            left: None,
            right: None,
        }
    }
}

impl<K, V> AsEntry<K, V> for Node<K, V>
where
    K: Default + Clone + Ord,
    V: Default + Clone,
{
    type Version = Version<V>;

    #[inline]
    fn key(&self) -> K {
        self.key.clone()
    }

    #[inline]
    fn key_ref(&self) -> &K {
        &self.key
    }

    #[inline]
    fn latest_version(&self) -> &Self::Version {
        &self.valn
    }

    #[inline]
    fn value(&self) -> V {
        self.valn.value()
    }

    #[inline]
    fn versions(&self) -> Vec<Self::Version> {
        let mut acc: Vec<Self::Version> = vec![];
        self.valn.value_nodes(&mut acc);
        acc
    }

    #[inline]
    fn seqno(&self) -> u64 {
        self.valn.seqno()
    }

    #[inline]
    fn is_deleted(&self) -> bool {
        self.valn.is_deleted()
    }
}

impl<K, V> Drop for Node<K, V>
where
    K: Default + Clone + Ord,
    V: Default + Clone,
{
    fn drop(&mut self) {
        self.left.take().map(|left| Box::leak(left));
        self.right.take().map(|right| Box::leak(right));
    }
}
