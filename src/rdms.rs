use std::{borrow::Borrow, fmt::Debug, marker, ops::RangeBounds};

use crate::{
    core::{Diff, Entry, Footprint, Index, IndexIter, Reader, Result, Writer},
    lsm,
};

pub struct Config {
    /// Set commit interval, in seconds, for auto-commit.
    /// If initialized to ZERO, then auto-commit is disabled and
    /// applications are expected to manually call the commit() method.
    pub interval_commit: usize,

    /// Set compact interval, in seconds, for auto-compact.
    /// If initialized to ZERO, then auto-compact is disabled and
    /// applications are expected to manually call the compact() method.
    pub interval_compact: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            interval_commit: Self::INTERVAL_COMMIT,
            interval_compact: Self::INTERVAL_COMPACT,
        }
    }
}

impl Config {
    const INTERVAL_COMMIT: usize = 0;
    const INTERVAL_COMPACT: usize = 0;

    pub fn set_auto_commit(&mut self, interval: usize) -> &mut Config {
        self.interval_commit = interval;
        self
    }

    pub fn set_auto_compact(&mut self, interval: usize) -> &mut Config {
        self.interval_compact = interval;
        self
    }
}

/// Index keys and corresponding values. Check module documentation for
/// the full set of features.
pub struct Rdms<K, V, M, D>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    M: Index<K, V>,
    D: Index<K, V>,
{
    name: String,
    config: Config,

    mem: M,
    disk: D,
    seqno: u64,
    _key: marker::PhantomData<K>,
    _value: marker::PhantomData<V>,
}

impl<K, V, M, D> Rdms<K, V, M, D>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    M: Index<K, V>,
    D: Index<K, V>,
{
    pub fn new<S>(
        name: S,
        mem: M,  // memory instance that has Index::make_new() trait.
        disk: D, // disk instance that has Index::make_new() trait.
        config: Config,
    ) -> Result<Rdms<K, V, M, D>>
    where
        S: AsRef<str>,
    {
        Ok(Rdms {
            name: name.as_ref().to_string(),
            config,

            mem,
            disk,
            seqno: 0,
            _key: marker::PhantomData,
            _value: marker::PhantomData,
        })
    }
}

impl<K, V, M, D> Rdms<K, V, M, D>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    M: Index<K, V>,
    D: Index<K, V>,
{
    pub fn to_name(&self) -> String {
        self.name.clone()
    }

    pub fn to_seqno(&self) -> u64 {
        self.seqno
    }

    // TODO: implement to_stats()
}

impl<K, V, M, D> Rdms<K, V, M, D>
where
    K: 'static + Clone + Ord + Footprint,
    V: 'static + Clone + Diff + Footprint,
    M: Index<K, V> + Reader<K, V>,
    D: Index<K, V> + Reader<K, V>,
{
    /// Get ``key`` from index.
    pub fn get<Q>(&self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let versions = false;
        let y = lsm::y_get(
            lsm::getter(&self.mem, versions),
            lsm::getter(&self.disk, versions),
        );
        y(key)
    }

    /// Iterate over all entries in this index.
    pub fn iter(&self) -> Result<IndexIter<K, V>> {
        let no_reverse = false;
        Ok(lsm::y_iter(self.mem.iter()?, self.disk.iter()?, no_reverse))
    }

    /// Iterate from lower bound to upper bound.
    pub fn range<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        Ok(lsm::y_iter(
            self.mem.range(range.clone())?,
            self.disk.range(range)?,
            false, /*reverse*/
        ))
    }

    /// Iterate from upper bound to lower bound.
    pub fn reverse<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        Ok(lsm::y_iter(
            self.mem.reverse(range.clone())?,
            self.disk.reverse(range)?,
            true, /*reverse*/
        ))
    }
}

impl<K, V, M, D> Rdms<K, V, M, D>
where
    K: 'static + Clone + Ord + Footprint,
    V: 'static + Clone + Diff + From<<V as Diff>::D> + Footprint,
    M: Index<K, V> + Reader<K, V>,
    D: Index<K, V> + Reader<K, V>,
{
    pub fn get_with_versions<Q>(&self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let versions = true;
        let y = lsm::y_get_versions(
            lsm::getter(&self.mem, versions),
            lsm::getter(&self.disk, versions),
        );
        y(key)
    }

    pub fn iter_with_versions(&self) -> Result<IndexIter<K, V>> {
        Ok(lsm::y_iter_versions(
            self.mem.iter()?,
            self.disk.iter()?,
            false, /*reverse*/
        ))
    }

    pub fn range_with_versions<'a, R, Q>(
        &'a self,
        range: R, // forward range from lower bound to upper bound
    ) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        Ok(lsm::y_iter_versions(
            self.mem.range(range.clone())?,
            self.disk.range(range)?,
            false, /*reverse*/
        ))
    }

    pub fn reverse_with_versions<'a, R, Q>(
        &'a self,
        range: R, // reverse range from upper bound to lower bound
    ) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        Ok(lsm::y_iter_versions(
            self.mem.reverse(range.clone())?,
            self.disk.reverse(range)?,
            true, /*reverse*/
        ))
    }
}

impl<K, V, M, D> Rdms<K, V, M, D>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    M: Index<K, V> + Writer<K, V>,
    D: Index<K, V>,
{
    /// Set {key, value} in index. Return older entry if present.
    pub fn set(&mut self, key: K, value: V) -> Result<Option<Entry<K, V>>> {
        self.mem.set(key, value)
    }

    /// Set {key, value} in index if an older entry exists with the
    /// same ``cas`` value. To create a fresh entry, pass ``cas`` as ZERO.
    /// Return the older entry if present.
    pub fn set_cas(
        &mut self,
        key: K,
        value: V,
        cas: u64, // previous seqno for key
    ) -> Result<Option<Entry<K, V>>> {
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

impl<K, V, M, D> Rdms<K, V, M, D>
where
    K: Clone + Ord + Debug + Footprint,
    V: Clone + Diff + Footprint,
    M: Index<K, V>,
    D: Index<K, V>,
{
    pub fn validate(&self) -> Result<()> {
        // return Stats
        // TBD
        Ok(())
    }
}
