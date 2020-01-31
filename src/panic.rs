//! Module `panic` handles unimplemented features.

use std::{borrow::Borrow, ops::RangeBounds};

use crate::core::{Diff, Entry, IndexIter, Reader, Result, Writer};

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
        panic!("set operation not supported by {} !!", self.0);
    }

    fn set_cas(&mut self, _: K, _: V, _: u64) -> Result<Option<Entry<K, V>>> {
        panic!("set operation not supported by {} !!", self.0);
    }

    fn delete<Q>(&mut self, _key: &Q) -> Result<Option<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        panic!("set operation not supported by {} !!", self.0);
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
        panic!("get operation not supported by {} !!", self.0);
    }

    /// Iterate over all entries in this index. Returned entry may not
    /// have all its previous versions, if it is costly to fetch from disk.
    fn iter(&mut self) -> Result<IndexIter<K, V>> {
        panic!("iter operation not supported by {} !!", self.0);
    }

    /// Iterate from lower bound to upper bound. Returned entry may not
    /// have all its previous versions, if it is costly to fetch from disk.
    fn range<'a, R, Q>(&'a mut self, _: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        panic!("range operation not supported by {} !!", self.0);
    }

    /// Iterate from upper bound to lower bound. Returned entry may not
    /// have all its previous versions, if it is costly to fetch from disk.
    fn reverse<'a, R, Q>(&'a mut self, _: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        panic!("reverse operation not supported by {} !!", self.0);
    }

    /// Get ``key`` from index. Returned entry shall have all its
    /// previous versions, can be a costly call.
    fn get_with_versions<Q>(&mut self, _: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        panic!("get_with_versions operation not supported by {} !!", self.0);
    }

    /// Iterate over all entries in this index. Returned entry shall
    /// have all its previous versions, can be a costly call.
    fn iter_with_versions(&mut self) -> Result<IndexIter<K, V>> {
        panic!(
            "iter_with_versions operation not supported by {} !!",
            self.0
        );
    }

    /// Iterate from lower bound to upper bound. Returned entry shall
    /// have all its previous versions, can be a costly call.
    fn range_with_versions<'a, R, Q>(&'a mut self, _: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        panic!(
            "range_with_versions operation not supported by {} !!",
            self.0
        );
    }

    /// Iterate from upper bound to lower bound. Returned entry shall
    /// have all its previous versions, can be a costly call.
    fn reverse_with_versions<'a, R, Q>(&'a mut self, _: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        panic!(
            "reverse_with_versions operation not supported by {} !!",
            self.0
        );
    }
}
