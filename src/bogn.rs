use std::borrow::Borrow;
use std::marker;
use std::ops::RangeBounds;

use crate::core::{Diff, Entry, Footprint, Result};
use crate::core::{Index, IndexIter, Reader, Writer};

/// Index keys and corresponding values. Check module documentation for
/// the full set of features.
pub struct Bogn<K, V, M, D>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    M: Index<K, V>,
    D: Index<K, V>,
{
    name: String,
    mem: M,
    disk: D,
    seqno: u64,
    _key: marker::PhantomData<K>,
    _value: marker::PhantomData<V>,
}

impl<K, V, M, D> Bogn<K, V, M, D>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    M: Index<K, V>,
    D: Index<K, V>,
{
    pub fn new<S>(name: S, mem: M, disk: D) -> Result<Bogn<K, V, M, D>>
    where
        S: AsRef<str>,
    {
        Ok(Bogn {
            name: name.as_ref().to_string(),
            mem,
            disk,
            seqno: 0,
            _key: marker::PhantomData,
            _value: marker::PhantomData,
        })
    }
}

impl<K, V, M, D> Bogn<K, V, M, D>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    M: Index<K, V> + Reader<K, V> + Writer<K, V>,
{
    pub fn to_name(&self) -> String {
        self.name.clone()
    }

    pub fn to_seqno(&self) -> u64 {
        self.seqno
    }

    //pub fn stats(&self) -> Stats {
    //    // TBD
    //}
}

impl<K, V, M> Bogn<K, V, M>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    M: Index<K, V> + Reader<K, V>,
{
    /// Get ``key`` from index.
    pub fn get<Q>(&self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.mem.get(key)
    }

    /// Iterate over all entries in this index.
    pub fn iter(&self) -> Result<IndexIter<K, V>> {
        self.mem.iter()
    }

    /// Iterate from lower bound to upper bound.
    pub fn range<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        self.mem.range(range)
    }

    /// Iterate from upper bound to lower bound.
    pub fn reverse<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        self.mem.reverse(range)
    }
}

impl<K, V, M> Bogn<K, V, M>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    M: Index<K, V> + Writer<K, V>,
{
    /// Set {key, value} in index. Return older entry if present.
    pub fn set(&mut self, key: K, value: V) -> Result<Option<Entry<K, V>>> {
        self.mem.set(key, value)
    }

    /// Set {key, value} in index if an older entry exists with the
    /// same ``cas`` value. To create a fresh entry, pass ``cas`` as ZERO.
    /// Return the older entry if present.
    pub fn set_cas(&mut self, key: K, value: V, cas: u64) -> Result<Option<Entry<K, V>>> {
        self.mem.set_cas(key, value, cas)
    }

    /// Delete key from DB. Return the entry if it is already present.
    pub fn delete<Q>(&mut self, key: &Q) -> Result<Option<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        self.mem.delete(key)
    }
}

//impl<K,V> Bogn<K,V>
//where
//    K: Clone + Ord,
//    V: Clone + Diff,
//{
//    validate
//}
