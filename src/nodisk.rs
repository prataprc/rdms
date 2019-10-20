use std::{borrow::Borrow, ffi, fs, marker, ops::RangeBounds};

use crate::{
    core::{
        Diff, DiskIndexFactory, DurableIndex, Entry, Footprint, IndexIter, Reader, Result,
        Serialize,
    },
    error::Error,
    types::{Empty, EmptyIter},
};

pub struct NoDiskFactory;

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

    fn new(&self, _dir: &ffi::OsStr, _name: &str) -> NoDisk<K, V> {
        NoDisk::new()
    }

    fn open(&self, _: &ffi::OsStr, _: fs::DirEntry) -> Result<NoDisk<K, V>> {
        Ok(NoDisk::new())
    }

    fn to_type(&self) -> String {
        "nodisk".to_string()
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

    fn to_name(&self) -> String {
        "no-disk mama !!".to_string()
    }

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
    fn get<Q>(&mut self, _key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        Err(Error::KeyNotFound)
    }

    fn iter(&mut self) -> Result<IndexIter<K, V>> {
        Ok(Box::new(EmptyIter {
            _phantom_key: &self.phantom_key,
            _phantom_val: &self.phantom_val,
        }))
    }

    fn range<'a, R, Q>(&'a mut self, _range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        Ok(Box::new(EmptyIter {
            _phantom_key: &self.phantom_key,
            _phantom_val: &self.phantom_val,
        }))
    }

    fn reverse<'a, R, Q>(&'a mut self, _range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        Ok(Box::new(EmptyIter {
            _phantom_key: &self.phantom_key,
            _phantom_val: &self.phantom_val,
        }))
    }

    fn get_with_versions<Q>(&mut self, _key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        Err(Error::KeyNotFound)
    }

    fn iter_with_versions(&mut self) -> Result<IndexIter<K, V>> {
        Ok(Box::new(EmptyIter {
            _phantom_key: &self.phantom_key,
            _phantom_val: &self.phantom_val,
        }))
    }

    fn range_with_versions<'a, R, Q>(&mut self, _r: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        Ok(Box::new(EmptyIter {
            _phantom_key: &self.phantom_key,
            _phantom_val: &self.phantom_val,
        }))
    }

    fn reverse_with_versions<'a, R, Q>(&mut self, _: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        Ok(Box::new(EmptyIter {
            _phantom_key: &self.phantom_key,
            _phantom_val: &self.phantom_val,
        }))
    }
}
