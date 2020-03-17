//! Module `rdms` implement a full-featured storage index.
//!
//! [Rdms] can be composed using underlying components and mechanisms defined
//! in [core] module.

use std::{
    fmt, marker,
    sync::{self, Arc, MutexGuard},
};

#[allow(unused_imports)]
use crate::core;
use crate::{
    core::{CommitIter, CommitIterator, Diff, Footprint, Index},
    core::{Cutoff, Result, Validate},
    error::Error,
};

/// Index type, composable index type. Check module documentation for
/// full set of features.
pub struct Rdms<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Index<K, V>,
{
    name: String,

    index: Option<Arc<sync::Mutex<I>>>,

    _key: marker::PhantomData<K>,
    _value: marker::PhantomData<V>,
}

impl<K, V, I> Rdms<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Index<K, V>,
{
    /// Create a new `Rdms` instance, identified by `name` using an underlying
    /// `index`.
    pub fn new<S>(name: S, index: I) -> Result<Box<Rdms<K, V, I>>>
    where
        S: AsRef<str>,
    {
        let value = Box::new(Rdms {
            name: name.as_ref().to_string(),

            index: Some(Arc::new(sync::Mutex::new(index))),

            _key: marker::PhantomData,
            _value: marker::PhantomData,
        });
        Ok(value)
    }

    /// Close this `Rdms` instance. The life-cycle of the index finishes when
    /// close is called. Note that, only memory resources will be cleared by
    /// this call. To cleanup persisted data (in disk) use the `purge()`.
    pub fn close(mut self) -> Result<()> {
        self.do_close()?;
        match Arc::try_unwrap(self.index.take().unwrap()) {
            Ok(index) => {
                index.into_inner().unwrap().close()?;
                Ok(())
            }
            Err(_) => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    /// Purge this index along with disk data.
    pub fn purge(mut self) -> Result<()> {
        self.do_close()?;
        match Arc::try_unwrap(self.index.take().unwrap()) {
            Ok(index) => {
                index.into_inner().unwrap().close()?;
                Ok(())
            }
            Err(_) => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    fn do_close(&mut self) -> Result<()> {
        Ok(()) // TODO cleanup this function
    }
}

impl<K, V, I> Rdms<K, V, I>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    I: Index<K, V>,
{
    fn as_index(&self) -> Result<MutexGuard<I>> {
        match self.index.as_ref().unwrap().lock() {
            Ok(value) => Ok(value),
            Err(err) => err_at!(Fatal, msg: format!("poisened lock {}", err)),
        }
    }
}

impl<K, V, I> Rdms<K, V, I>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    I: Index<K, V>,
{
    pub fn to_name(&self) -> Result<String> {
        Ok(self.name.to_string())
    }

    pub fn to_metadata(&self) -> Result<Vec<u8>> {
        let index = self.as_index()?;
        index.to_metadata()
    }

    pub fn to_seqno(&self) -> Result<u64> {
        let index = self.as_index()?;
        index.to_seqno()
    }

    pub fn set_seqno(&mut self, seqno: u64) -> Result<()> {
        let mut index = self.as_index()?;
        index.set_seqno(seqno)
    }

    pub fn to_reader(&mut self) -> Result<<I as Index<K, V>>::R> {
        let mut index = self.as_index()?;
        index.to_reader()
    }

    pub fn to_writer(&mut self) -> Result<<I as Index<K, V>>::W> {
        let mut index = self.as_index()?;
        index.to_writer()
    }

    pub fn commit<C, F>(&mut self, scanner: CommitIter<K, V, C>, metacb: F) -> Result<()>
    where
        C: CommitIterator<K, V>,
        F: Fn(Vec<u8>) -> Vec<u8>,
    {
        let mut index = self.as_index()?;
        index.commit(scanner, metacb)
    }

    pub fn compact(&mut self, cutoff: Cutoff) -> Result<usize> {
        let mut index = self.as_index()?;
        index.compact(cutoff)
    }
}

impl<K, V, T, I> Validate<T> for Box<Rdms<K, V, I>>
where
    K: Clone + Ord + Footprint + fmt::Debug,
    V: Clone + Diff + Footprint,
    I: Index<K, V> + Validate<T>,
    T: fmt::Display,
{
    fn validate(&mut self) -> Result<T> {
        let mut index = self.as_index()?;
        index.validate()
    }
}
