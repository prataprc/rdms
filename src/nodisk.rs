use std::{borrow::Borrow, ffi, marker, ops::RangeBounds};

use crate::core::{Diff, DurableIndex, Footprint, IndexIter, Reader};
use crate::core::{DiskIndexFactory, Serialize};
use crate::core::{Entry, Result};
use crate::error::Error;
use crate::types::Empty;

pub struct NoDiskFactory;

pub fn nodisk_factory() -> NoDiskFactory {
    NoDiskFactory
}

impl<K, V> DiskIndexFactory<K, V> for NoDiskFactory
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    type I = NoDisk<K, V>;

    fn new(&self, _dir: &ffi::OsStr, _name: &str) -> NoDisk<K, V> {
        NoDisk::new()
    }

    fn open(&self, _dir: &ffi::OsStr, _file_name: &str) -> Result<NoDisk<K, V>> {
        Ok(NoDisk::new())
    }
}

/// NoDisk type denotes empty Disk type.
///
/// Applications can use this type while instantiating `rdms-index` in
/// mem-only mode.
pub struct NoDisk<K, V> {
    phantom_key: marker::PhantomData<K>,
    phantom_val: marker::PhantomData<V>,
}

impl<K, V> NoDisk<K, V> {
    fn new() -> NoDisk<K, V> {
        NoDisk {
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        }
    }
}

impl<K, V> Footprint for NoDisk<K, V> {
    fn footprint(&self) -> Result<isize> {
        Ok(0)
    }
}

impl<K, V> DurableIndex<K, V> for NoDisk<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    type R = NoDisk<K, V>;

    type C = Empty;

    fn commit(&mut self, _iter: IndexIter<K, V>, _meta: Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn prepare_compact(&self) -> Self::C {
        Empty
    }

    fn compact(
        &mut self,
        _iter: IndexIter<K, V>, // skip
        _meta: Vec<u8>,
        _prepare: Self::C,
    ) -> Result<()> {
        Ok(())
    }

    fn to_reader(&mut self) -> Result<Self::R> {
        Ok(NoDisk::new())
    }
}

impl<K, V> Reader<K, V> for NoDisk<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn get<Q>(&self, _key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        Err(Error::KeyNotFound)
    }

    fn iter(&self) -> Result<IndexIter<K, V>> {
        Ok(Box::new(NoDiskIter {
            _phantom_key: &self.phantom_key,
            _phantom_val: &self.phantom_val,
        }))
    }

    fn range<'a, R, Q>(&'a self, _range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        Ok(Box::new(NoDiskIter {
            _phantom_key: &self.phantom_key,
            _phantom_val: &self.phantom_val,
        }))
    }

    fn reverse<'a, R, Q>(&'a self, _range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        Ok(Box::new(NoDiskIter {
            _phantom_key: &self.phantom_key,
            _phantom_val: &self.phantom_val,
        }))
    }

    fn get_with_versions<Q>(&self, _key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        Err(Error::KeyNotFound)
    }

    fn iter_with_versions(&self) -> Result<IndexIter<K, V>> {
        Ok(Box::new(NoDiskIter {
            _phantom_key: &self.phantom_key,
            _phantom_val: &self.phantom_val,
        }))
    }

    fn range_with_versions<'a, R, Q>(&self, _range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        Ok(Box::new(NoDiskIter {
            _phantom_key: &self.phantom_key,
            _phantom_val: &self.phantom_val,
        }))
    }

    fn reverse_with_versions<'a, R, Q>(&self, _rng: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        Ok(Box::new(NoDiskIter {
            _phantom_key: &self.phantom_key,
            _phantom_val: &self.phantom_val,
        }))
    }
}

struct NoDiskIter<'a, K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    _phantom_key: &'a marker::PhantomData<K>,
    _phantom_val: &'a marker::PhantomData<V>,
}

impl<'a, K, V> Iterator for NoDiskIter<'a, K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
