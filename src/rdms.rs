use std::marker;

use crate::core::{Diff, Footprint, Index, IndexIter, Result};

/// Default commit interval, in seconds, for auto-commit.
pub const COMMIT_INTERVAL: usize = 30 * 60; // 30 minutes

/// Default compact interval, in seconds, for auto-compact.
/// If initialized to ZERO, then auto-compact is disabled and
/// applications are expected to manually call the compact() method.
pub const COMPACT_INTERVAL: usize = 120 * 60; // 2 hours

/// Index keys and corresponding values. Check module documentation for
/// the full set of features.
pub struct Rdms<K, V, I>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    I: Index<K, V>,
{
    name: String,
    commit_interval: usize,
    compact_interval: usize,

    index: I,
    _key: marker::PhantomData<K>,
    _value: marker::PhantomData<V>,
}

impl<K, V, I> Rdms<K, V, I>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    I: Index<K, V>,
{
    pub fn new<S>(name: S, index: I) -> Result<Rdms<K, V, I>>
    where
        S: AsRef<str>,
    {
        Ok(Rdms {
            name: name.as_ref().to_string(),
            commit_interval: Default::default(),
            compact_interval: Default::default(),

            index,
            _key: marker::PhantomData,
            _value: marker::PhantomData,
        })
    }

    /// Set commit interval, in seconds, for auto-commit.
    /// If initialized to ZERO, then auto-commit is disabled and
    /// applications are expected to manually call the commit() method.
    pub fn set_auto_commit(&mut self, interval: usize) {
        self.commit_interval = interval;
    }

    /// Set compact interval, in seconds, for auto-compact.
    /// If initialized to ZERO, then auto-compact is disabled and
    /// applications are expected to manually call the compact() method.
    pub fn set_auto_compact(&mut self, interval: usize) {
        self.compact_interval = interval;
    }
}

impl<K, V, I> Rdms<K, V, I>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    I: Index<K, V>,
{
    pub fn to_name(&self) -> String {
        self.name.to_string()
    }

    pub fn to_metadata(&mut self) -> Result<Vec<u8>> {
        self.index.to_metadata()
    }

    pub fn to_seqno(&mut self) -> u64 {
        self.index.to_seqno()
    }

    pub fn set_seqno(&mut self, seqno: u64) {
        self.index.set_seqno(seqno)
    }

    pub fn to_reader(&mut self) -> Result<<I as Index<K, V>>::R> {
        self.index.to_reader()
    }

    pub fn to_writer(&mut self) -> Result<<I as Index<K, V>>::W> {
        self.index.to_writer()
    }

    pub fn commit(&mut self, iter: IndexIter<K, V>, meta: Vec<u8>) -> Result<()> {
        self.index.commit(iter, meta)
    }

    pub fn compact(&mut self) -> Result<()> {
        self.index.compact()
    }
}
