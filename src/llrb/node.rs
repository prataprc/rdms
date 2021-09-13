use std::sync::Arc;

use crate::db::{Diff, Entry};

// Node corresponds to a single entry in Llrb instance.
#[derive(Clone)]
pub struct Node<K, V>
where
    V: Diff,
{
    pub entry: Arc<Entry<K, V>>,
    pub black: bool,                    // store: black or red
    pub left: Option<Arc<Node<K, V>>>,  // store: left child
    pub right: Option<Arc<Node<K, V>>>, // store: right child
}

impl<K, V> Node<K, V>
where
    V: Diff,
{
    pub fn set(&mut self, value: V, seqno: u64)
    where
        K: Clone,
        V: Clone,
    {
        let mut entry = self.entry.as_ref().clone();
        entry.value.set(value, seqno);
        self.entry = Arc::new(entry);
    }

    pub fn insert(&mut self, value: V, seqno: u64)
    where
        K: Clone,
        V: Clone,
    {
        let mut entry = self.entry.as_ref().clone();
        entry.insert(value, seqno);
        self.entry = Arc::new(entry);
    }

    pub fn commit(&mut self, other: Entry<K, V>)
    where
        K: PartialEq + Clone,
        V: Clone,
    {
        self.entry = Arc::new(self.entry.as_ref().commit(&other));
    }

    pub fn delete(&mut self, seqno: u64)
    where
        K: Clone,
        V: Clone,
        <V as Diff>::Delta: From<V>,
    {
        let mut entry = self.entry.as_ref().clone();
        entry.delete(seqno);
        self.entry = Arc::new(entry);
    }

    #[inline]
    pub fn set_red(&mut self) {
        self.black = false
    }

    #[inline]
    pub fn set_black(&mut self) {
        self.black = true
    }

    #[inline]
    pub fn toggle_link(&mut self) {
        self.black = !self.black
    }
}

impl<K, V> Node<K, V>
where
    V: Diff,
{
    #[inline]
    pub fn as_left_ref(&self) -> Option<&Node<K, V>> {
        self.left.as_deref()
    }

    #[inline]
    pub fn as_right_ref(&self) -> Option<&Node<K, V>> {
        self.right.as_deref()
    }

    #[inline]
    pub fn is_black(&self) -> bool {
        self.black
    }

    pub fn as_key(&self) -> &K {
        self.entry.as_key()
    }

    pub fn to_seqno(&self) -> u64 {
        self.entry.to_seqno()
    }

    pub fn is_deleted(&self) -> bool {
        self.entry.is_deleted()
    }
}

impl<K, V> From<Entry<K, V>> for Node<K, V>
where
    V: Diff,
{
    fn from(entry: Entry<K, V>) -> Node<K, V> {
        Node {
            entry: Arc::new(entry),
            black: false,
            left: None,
            right: None,
        }
    }
}

#[cfg(test)]
#[path = "node_test.rs"]
mod node_test;
