//! Module `panic` handles unimplemented features.

use std::{borrow::Borrow, ops::RangeBounds};

use crate::{
    core::{CommitIterator, Diff, Entry, IndexIter, Reader, Result, Writer},
    error::Error,
};

/// Placeholder type, to handle unimplemented features.
pub struct Panic(String);

impl Panic {
    pub fn new(name: &str) -> Panic {
        Panic(name.to_string())
    }
}

// Write methods
impl<K, V> Writer<K, V> for Panic
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn set(&mut self, _key: K, _value: V) -> Result<Option<Entry<K, V>>> {
        err_at!(NotImplemented, msg:self.0)
    }

    fn set_cas(&mut self, _: K, _: V, _: u64) -> Result<Option<Entry<K, V>>> {
        err_at!(NotImplemented, msg:self.0)
    }

    fn delete<Q>(&mut self, _key: &Q) -> Result<Option<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        err_at!(NotImplemented, msg:self.0)
    }
}

impl<K, V> Reader<K, V> for Panic
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn get<Q>(&mut self, _: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        err_at!(NotImplemented, msg:self.0)
    }

    fn iter(&mut self) -> Result<IndexIter<K, V>> {
        err_at!(NotImplemented, msg:self.0)
    }

    fn range<'a, R, Q>(&'a mut self, _: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        err_at!(NotImplemented, msg:self.0)
    }

    fn reverse<'a, R, Q>(&'a mut self, _: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        err_at!(NotImplemented, msg:self.0)
    }

    fn get_with_versions<Q>(&mut self, _: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        err_at!(NotImplemented, msg:self.0)
    }

    fn iter_with_versions(&mut self) -> Result<IndexIter<K, V>> {
        err_at!(NotImplemented, msg:self.0)
    }

    fn range_with_versions<'a, R, Q>(&'a mut self, _: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        err_at!(NotImplemented, msg:self.0)
    }

    fn reverse_with_versions<'a, R, Q>(&'a mut self, _: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        err_at!(NotImplemented, msg:self.0)
    }
}

impl<K, V> CommitIterator<K, V> for Panic
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn scan<G>(&mut self, _within: G) -> Result<IndexIter<K, V>>
    where
        G: Clone + RangeBounds<u64>,
    {
        err_at!(NotImplemented, msg:self.0)
    }

    fn scans<G>(&mut self, _n_shards: usize, _within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
    {
        err_at!(NotImplemented, msg:self.0)
    }

    fn range_scans<N, G>(&mut self, _ranges: Vec<N>, _within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
        N: Clone + RangeBounds<K>,
    {
        err_at!(NotImplemented, msg:self.0)
    }
}
