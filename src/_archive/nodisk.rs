//! Module `nodisk` define a dummy disk index.

use std::{borrow::Borrow, ffi, hash::Hash, marker, ops::RangeBounds};

use crate::{
    core::{CommitIter, CommitIterator, Cutoff, Result, Serialize},
    core::{Diff, DiskIndexFactory, Entry, Footprint, Index, IndexIter, Reader},
    error::Error,
    panic::Panic,
};

/// Factory type, to construct NoDisk indexes.
pub struct NoDiskFactory;

/// Return [NoDiskFactory].
pub fn nodisk_factory() -> NoDiskFactory {
    NoDiskFactory
}

impl<K, V> DiskIndexFactory<K, V> for NoDiskFactory
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
{
    type I = NoDisk<K, V>;

    #[inline]
    fn new(&self, _dir: &ffi::OsStr, _name: &str) -> Result<NoDisk<K, V>> {
        Ok(NoDisk::new())
    }

    #[inline]
    fn open(&self, _dir: &ffi::OsStr, _name: &str) -> Result<NoDisk<K, V>> {
        Ok(NoDisk::new())
    }

    #[inline]
    fn to_type(&self) -> String {
        "nodisk".to_string()
    }
}

/// Index type, for empty Disk type. Can be used with mem-only storage.
///
/// Applications can use this type while instantiating `rdms-index` in
/// mem-only mode.
#[derive(Clone)]
pub struct NoDisk<K, V> {
    phantom_key: marker::PhantomData<K>,
    phantom_val: marker::PhantomData<V>,
}

impl<K, V> NoDisk<K, V> {
    #[inline]
    fn new() -> NoDisk<K, V> {
        NoDisk {
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        }
    }
}

impl<K, V> Footprint for NoDisk<K, V> {
    #[inline]
    fn footprint(&self) -> Result<isize> {
        Ok(0)
    }
}

impl<K, V> Index<K, V> for NoDisk<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    type R = Panic;
    type W = Panic;

    #[inline]
    fn to_name(&self) -> Result<String> {
        Ok("no-disk mama !!".to_string())
    }

    #[inline]
    fn to_metadata(&self) -> Result<Vec<u8>> {
        Ok(vec![])
    }

    #[inline]
    fn to_seqno(&self) -> Result<u64> {
        Ok(0)
    }

    #[inline]
    fn set_seqno(&mut self, _seqno: u64) -> Result<()> {
        // noop
        Ok(())
    }

    #[inline]
    fn to_reader(&mut self) -> Result<Self::R> {
        Ok(Panic::new("nodisk"))
    }

    #[inline]
    fn to_writer(&mut self) -> Result<Self::W> {
        Ok(Panic::new("nodisk"))
    }

    #[inline]
    fn commit<C, F>(&mut self, _: CommitIter<K, V, C>, _metadb: F) -> Result<()>
    where
        C: CommitIterator<K, V>,
        F: Fn(Vec<u8>) -> Vec<u8>,
    {
        Ok(())
    }

    #[inline]
    fn compact(&mut self, _: Cutoff) -> Result<usize> {
        Ok(0)
    }

    #[inline]
    fn close(self) -> Result<()> {
        Ok(())
    }

    #[inline]
    fn purge(self) -> Result<()> {
        Ok(())
    }
}

impl<K, V> Reader<K, V> for NoDisk<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn get<Q>(&mut self, _key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + Hash,
    {
        Err(Error::NotFound)
    }

    fn iter(&mut self) -> Result<IndexIter<K, V>> {
        let empty: Vec<Result<Entry<K, V>>> = vec![];
        Ok(Box::new(empty.into_iter()))
    }

    fn range<'a, R, Q>(&'a mut self, _range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let empty: Vec<Result<Entry<K, V>>> = vec![];
        Ok(Box::new(empty.into_iter()))
    }

    fn reverse<'a, R, Q>(&'a mut self, _range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let empty: Vec<Result<Entry<K, V>>> = vec![];
        Ok(Box::new(empty.into_iter()))
    }

    fn get_with_versions<Q>(&mut self, _key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + Hash,
    {
        Err(Error::NotFound)
    }

    fn iter_with_versions(&mut self) -> Result<IndexIter<K, V>> {
        let empty: Vec<Result<Entry<K, V>>> = vec![];
        Ok(Box::new(empty.into_iter()))
    }

    fn range_with_versions<'a, R, Q>(&mut self, _r: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let empty: Vec<Result<Entry<K, V>>> = vec![];
        Ok(Box::new(empty.into_iter()))
    }

    fn reverse_with_versions<'a, R, Q>(&mut self, _: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let empty: Vec<Result<Entry<K, V>>> = vec![];
        Ok(Box::new(empty.into_iter()))
    }
}

impl<K, V> CommitIterator<K, V> for NoDisk<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn scan<G>(&mut self, _within: G) -> Result<IndexIter<K, V>>
    where
        G: Clone + RangeBounds<u64>,
    {
        err_at!(NotImplemented, msg:"<NoDisk as CommitIterator>.scan()".to_string())
    }

    fn scans<G>(&mut self, _n_shards: usize, _within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
    {
        err_at!(NotImplemented, msg:"<NoDisk as CommitIterator>.scans()".to_string())
    }

    fn range_scans<N, G>(
        &mut self,
        _ranges: Vec<N>,
        _within: G,
    ) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
        N: Clone + RangeBounds<K>,
    {
        err_at!(NotImplemented, msg:"<NoDisk as CommitIterator>.range_scans()".to_string())
    }
}
