use std::borrow::Borrow;
use std::marker;
use std::ops::RangeBounds;

use crate::core::{Diff, Entry, Index, IndexIter, Result};
use crate::error::Error;

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
    pub fn get<Q>(&self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.mem.get(key)
    }

    pub fn iter(&self) -> Result<IndexIter<K, V>> {
        self.mem.iter()
    }

    pub fn range<R, Q>(&self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q>,
        Q: Ord + ?Sized,
    {
        self.mem.range(range)
    }

    pub fn reverse<R, Q>(&self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q>,
        Q: Ord + ?Sized,
    {
        self.mem.reverse(range)
    }
}

//impl<K,V> Bogn<K,V>
//where
//    K: Clone + Ord,
//    V: Clone + Diff,
//{
//    validate
//}
