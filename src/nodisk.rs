use std::borrow::Borrow;
use std::ops::RangeBounds;

use crate::core::{Diff, Footprint, Index, IndexIter, Reader, Writer};
use crate::core::{Entry, Result};

/// NoDisk type denotes empty Disk type. Applications can use this
/// type while instantiating bogn index in mem-only mode.
pub struct NoDisk;

impl Footprint for NoDisk {
    fn footprint(&self) -> isize {
        0
    }
}

impl<K, V> Index<K, V> for NoDisk
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    type W = NoDisk;
    type R = NoDisk;

    fn make_new(&self) -> Result<Box<Self>> {
        panic!("index type is just a place holder");
    }

    fn to_reader(&mut self) -> Result<Self::R> {
        panic!("index type is just a place holder");
    }

    fn to_writer(&mut self) -> Result<Self::W> {
        panic!("index type is just a place holder");
    }
}

impl<K, V> Reader<K, V> for NoDisk
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn get<Q>(&self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        panic!("index type is just a place holder");
    }

    fn iter(&self) -> Result<IndexIter<K, V>> {
        panic!("index type is just a place holder");
    }

    fn range<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        panic!("index type is just a place holder");
    }

    fn reverse<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        panic!("index type is just a place holder");
    }

    fn get_with_versions<Q>(&self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        panic!("index type is just a place holder");
    }

    fn iter_with_versions(&self) -> Result<IndexIter<K, V>> {
        panic!("index type is just a place holder");
    }

    fn range_with_versions<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        panic!("index type is just a place holder");
    }

    fn reverse_with_versions<'a, R, Q>(&'a self, rng: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        panic!("index type is just a place holder");
    }
}

impl<K, V> Writer<K, V> for NoDisk
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn set(&mut self, k: K, v: V) -> Result<Option<Entry<K, V>>> {
        panic!("index type is just a place holder");
    }

    fn set_cas(&mut self, k: K, v: V, cas: u64) -> Result<Option<Entry<K, V>>> {
        panic!("index type is just a place holder");
    }

    fn delete<Q>(&mut self, key: &Q) -> Result<Option<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        panic!("index type is just a place holder");
    }
}
