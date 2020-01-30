use std::{
    ffi, fmt, result,
    sync::{atomic::AtomicU64, Arc},
    vec,
};

use crate::{
    core::{Result, Serialize},
    dlog_entry::DEntry,
    dlog_journal::Shard,
};

#[allow(unused_imports)]
use crate::rdms::Rdms;

/// Dlog entry logging for [`Rdms`] index.
pub struct Dlog<S, T>
where
    S: Clone + Default + Serialize + DlogState<T>,
    T: Clone + Default + Serialize,
{
    pub(crate) dir: ffi::OsString,
    pub(crate) name: String,

    pub(crate) index: Arc<AtomicU64>, // seqno
    pub(crate) shards: Vec<Shard<S, T>>,
}

impl<S, T> fmt::Debug for Dlog<S, T>
where
    S: Clone + Default + Serialize + DlogState<T>,
    T: Clone + Default + Serialize,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "Dlog<{:?},{}>", self.dir, self.name)
    }
}

impl<S, T> Dlog<S, T>
where
    S: Clone + Default + Serialize + DlogState<T>,
    T: Clone + Default + Serialize,
{
    /// Create a new [`Dlog`] instance under directory ``dir``, using specified
    /// number of shards ``nshards`` and ``name`` must be unique if more than
    /// only [`Dlog`] instances are going to be created under the same ``dir``.
    pub fn create(
        dir: ffi::OsString,
        name: String,
        nshards: usize,
        journal_limit: usize,
        nosync: bool,
    ) -> Result<Dlog<S, T>> {
        let dlog_index = Arc::new(AtomicU64::new(1));

        // purge existing shard/journals for name.
        let mut shards = vec![];
        for shard_id in 0..nshards {
            let index = Arc::clone(&dlog_index);
            let (d, n, l) = (dir.clone(), name.clone(), journal_limit);
            shards.push(Shard::<S, T>::create(d, n, shard_id, index, l, nosync)?);
        }

        // create this Dlog. later shards/journals can be added.
        Ok(Dlog {
            dir,
            name,

            index: dlog_index,
            shards,
        })
    }

    /// Load an existing [`Dlog`] instance identified by ``name`` under
    /// directory ``dir``.
    pub fn load(
        dir: ffi::OsString,
        name: String,
        nshards: usize,
        journal_limit: usize,
        nosync: bool,
    ) -> Result<Dlog<S, T>> {
        let dlog_index = Arc::new(AtomicU64::new(1));

        let mut shards = vec![];
        for shard_id in 0..nshards {
            let index = Arc::clone(&dlog_index);
            let (d, n, l) = (dir.clone(), name.clone(), journal_limit);
            shards.push(Shard::<S, T>::load(d, n, shard_id, index, l, nosync)?);
        }

        Ok(Dlog {
            dir,
            name,

            index: dlog_index,
            shards,
        })
    }
}

pub(crate) enum OpRequest<T>
where
    T: Default + Serialize,
{
    Op { op: T },
    PurgeTill { before: u64 },
}

impl<T> OpRequest<T>
where
    T: Default + Serialize,
{
    pub(crate) fn new_op(op: T) -> OpRequest<T> {
        OpRequest::Op { op }
    }

    pub(crate) fn new_purge_till(before: u64) -> OpRequest<T> {
        OpRequest::PurgeTill { before }
    }
}

#[derive(PartialEq)]
pub(crate) enum OpResponse {
    Index(u64),
    Purged(u64),
}

impl OpResponse {
    pub(crate) fn new_index(index: u64) -> OpResponse {
        OpResponse::Index(index)
    }

    pub(crate) fn new_purged(index: u64) -> OpResponse {
        OpResponse::Purged(index)
    }
}

pub trait DlogState<T>
where
    T: Serialize,
{
    type Key: Default + Serialize;
    type Val: Default + Serialize;

    fn on_add_entry(&mut self, entry: &DEntry<T>) -> ();

    fn to_type(&self) -> String;
}

//#[cfg(test)]
//#[path = "dlog_test.rs"]
//mod dlog_test;
