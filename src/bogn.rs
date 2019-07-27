use std::borrow::Borrow;
use std::ops::RangeBounds;
use std::{cmp, marker};

use crate::core::{Diff, Entry, Index, IndexIter, Result};

pub struct Bogn<K, V, M>
where
    K: Clone + Ord,
    V: Clone + Diff,
    M: Index<K, V>,
{
    name: String,
    mem: M,
    seqno: u64,
    _key: marker::PhantomData<K>,
    _value: marker::PhantomData<V>,
}

impl<K, V, M> Bogn<K, V, M>
where
    K: Clone + Ord,
    V: Clone + Diff,
    M: Index<K, V>,
{
    /// Create bogn index in ``mem-only`` mode. Memory only indexes are
    /// ephimeral indexes. They don't persist data on disk to give
    /// durability gaurantees.
    pub fn mem_only<S>(name: S, mem: M) -> Result<Bogn<K, V, M>>
    where
        S: AsRef<str>,
    {
        Ok(Bogn {
            name: name.as_ref().to_string(),
            mem,
            seqno: 0,
            _key: marker::PhantomData,
            _value: marker::PhantomData,
        })
    }
}

impl<K, V, M> Bogn<K, V, M>
where
    K: Clone + Ord,
    V: Clone + Diff,
    M: Index<K, V>,
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
    K: Clone + Ord,
    V: Clone + Diff,
    M: Index<K, V>,
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
    pub fn range<R, Q>(&self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q>,
        Q: Ord + ?Sized,
    {
        self.mem.range(range)
    }

    /// Iterate from upper bound to lower bound.
    pub fn reverse<R, Q>(&self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q>,
        Q: Ord + ?Sized,
    {
        self.mem.reverse(range)
    }
}

impl<K, V, M> Bogn<K, V, M>
where
    K: Clone + Ord,
    V: Clone + Diff,
    M: Index<K, V>,
{
    /// Set {key, value} in index. Return older entry if present.
    pub fn set(&mut self, key: K, value: V) -> Result<Option<Entry<K, V>>> {
        let res = self.mem.set_index(key, value, self.seqno + 1);
        self.seqno += 1;
        res
    }

    /// Set {key, value} in index if an older entry exists with the
    /// same ``cas`` value. To create a fresh entry, pass ``cas`` as ZERO.
    /// Return the older entry if present.
    pub fn set_cas(&mut self, key: K, value: V, cas: u64) -> Result<Option<Entry<K, V>>> {
        let seqno = self.seqno + 1;
        let (seqno, res) = self.mem.set_cas_index(key, value, cas, seqno);
        self.seqno = cmp::max(seqno, self.seqno);
        res
    }

    /// Delete key from DB. Return the entry if it is already present.
    pub fn delete<Q>(&mut self, key: &Q) -> Result<Option<Entry<K, V>>> {
        let (seqno, res) = self.mem.delete_index(key, self.seqno + 1);
        self.seqno = cmp::max(seqno, self.seqno);
        res
    }
}

//impl<K,V> Bogn<K,V>
//where
//    K: Clone + Ord,
//    V: Clone + Diff,
//{
//    validate
//}
