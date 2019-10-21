use std::{borrow::Borrow, ffi, fs, marker, ops::RangeBounds};

use crate::{
    core::{Diff, DiskIndexFactory, Entry, Footprint, Index, IndexIter, Reader},
    core::{Result, Serialize, Writer},
    error::Error,
    types::EmptyIter,
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

    fn new(&self, _dir: &ffi::OsStr, _name: &str) -> Result<NoDisk<K, V>> {
        Ok(NoDisk::new())
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

impl<K, V> Index<K, V> for NoDisk<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    type R = NoDisk<K, V>;
    type W = NoDisk<K, V>;

    fn to_name(&self) -> String {
        "no-disk mama !!".to_string()
    }

    fn to_metadata(&mut self) -> Result<Vec<u8>> {
        Ok(vec![])
    }

    fn to_seqno(&mut self) -> u64 {
        0
    }

    fn set_seqno(&mut self, _seqno: u64) {
        // noop
    }

    fn to_reader(&mut self) -> Result<Self::R> {
        Ok(NoDisk::new())
    }

    fn to_writer(&mut self) -> Result<Self::W> {
        Ok(NoDisk::new())
    }

    fn commit(&mut self, _: IndexIter<K, V>, _: Vec<u8>) -> Result<Self> {
        Ok(NoDisk::new())
    }

    fn compact(&mut self, _: IndexIter<K, V>, _: Vec<u8>) -> Result<Self> {
        Ok(NoDisk::new())
    }
}

impl<K, V> Writer<K, V> for NoDisk<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn set(&mut self, _: K, _: V) -> Result<Option<Entry<K, V>>> {
        panic!("not supported")
    }

    fn set_cas(&mut self, _: K, _: V, _cas: u64) -> Result<Option<Entry<K, V>>> {
        panic!("not supported")
    }

    fn delete<Q>(&mut self, _key: &Q) -> Result<Option<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        panic!("not supported")
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
