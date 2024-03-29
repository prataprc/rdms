use std::sync::Arc;

use crate::{
    dbs::{self, Footprint},
    Error, Result,
};

// Node corresponds to a single entry in Llrb instance.
#[derive(Clone)]
pub struct Node<K, V>
where
    V: dbs::Diff,
{
    pub entry: Arc<dbs::Entry<K, V>>,
    pub black: bool,                    // store: black or red
    pub left: Option<Arc<Node<K, V>>>,  // store: left child
    pub right: Option<Arc<Node<K, V>>>, // store: right child
}

impl<K, V> Footprint for Node<K, V>
where
    K: Footprint,
    V: dbs::Diff + Footprint,
    <V as dbs::Diff>::Delta: Footprint,
{
    fn footprint(&self) -> Result<isize> {
        use std::{convert::TryFrom, mem::size_of};

        let size = size_of::<Node<K, V>>();
        let overhead = err_at!(FailConvert, isize::try_from(size))?;
        Ok(overhead + self.entry.footprint()?)
    }
}

impl<K, V> Node<K, V>
where
    V: dbs::Diff,
{
    pub fn set(&mut self, value: V, seqno: u64)
    where
        K: Clone,
    {
        let mut entry = self.entry.as_ref().clone();
        entry.value = dbs::Value::new_upsert(value, seqno);
        entry.deltas = Vec::default();
        self.entry = Arc::new(entry);
    }

    pub fn insert(&mut self, value: V, seqno: u64)
    where
        K: Clone,
    {
        self.entry = Arc::new(self.entry.as_ref().insert(value, seqno));
    }

    pub fn delete(&mut self, seqno: u64)
    where
        K: Clone,
    {
        self.entry = Arc::new(self.entry.as_ref().delete(seqno));
    }

    pub fn commit(&mut self, other: dbs::Entry<K, V>) -> Result<()>
    where
        K: PartialEq + Clone,
    {
        self.entry = Arc::new(self.entry.as_ref().commit(&other)?);
        Ok(())
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
    V: dbs::Diff,
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

    #[inline]
    pub fn as_key(&self) -> &K {
        self.entry.as_key()
    }

    #[inline]
    pub fn to_seqno(&self) -> u64 {
        self.entry.to_seqno()
    }

    #[inline]
    pub fn is_deleted(&self) -> bool {
        self.entry.is_deleted()
    }
}

impl<K, V> From<dbs::Entry<K, V>> for Node<K, V>
where
    V: dbs::Diff,
{
    fn from(entry: dbs::Entry<K, V>) -> Node<K, V> {
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
