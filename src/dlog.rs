//! Module `dlog` implement entry logging for [Rdms] index.
//!
//! Takes care of batching write operations, serializing, appending
//! them to disk, and finally commiting the appended batch(es). A
//! single `Dlog` can be managed using ``n`` number of shards, where
//! each shard manages the log using a set of journal-files.
//!
//! **Shards**:
//!
//! Every shard is managed in a separate thread and each shard serializes
//! the log-operations, batches them if possible, flushes them and return
//! a index-sequence-no for each operation back to the caller. Basic idea
//! behind shard is to match with I/O concurrency available in modern SSDs.
//!
//! **Journals**:
//!
//! A shard of `Dlog` is organized into ascending list of journal files,
//! where each journal file do not exceed the configured size-limit.
//! Journal files are append only and flushed in batches when ever
//! possible. Journal files are purged once `Dlog` is notified about
//! durability guarantee uptill an index-sequence-no.
//!
//! A Typical Dlog operation-cycles fall under one of the following catogaries:
//!
//! * Initial Dlog cycle, when new Dlog is created on disk.
//! * Reload Dlog cycle, when opening an existing Dlog on disk.
//! * Replay Dlog cycle, when entries Dlog needs to be replayed on DB.
//! * Purge Dlog cycle, when an existing Dlog needs to totally purged.
//!
//! **Initial Dlog cycle**:
//!
//! ```compile_fail
//!                                        +--------------+
//!     Dlog::create() -> spawn_writer() -> | purge_till() |
//!                                        |    close()   |
//!                                        +--------------+
//! ```
//!
//! **Reload Dlog cycle**:
//!
//! ```compile_fail
//!                                      +--------------+
//!     Dlog::load() -> spawn_writer() -> | purge_till() |
//!                                      |    close()   |
//!                                      +--------------+
//! ```
//!
//! **Replay Dlog cycle**:
//!
//! ```compile_fail
//!     Dlog::load() -> replay() -> close()
//! ```
//!
//! **Purge cycle**:
//!
//! ```compile_fail
//!     +---------------+
//!     | Dlog::create() |
//!     |     or        | ---> Dlog::purge()
//!     | Dlog::load()   |
//!     +---------------+
//! ```
//!

use std::{
    ffi,
    sync::{atomic::AtomicU64, Arc},
    vec,
};

use crate::{
    core::{Result, Serialize},
    dlog_entry::DEntry,
    dlog_journal::Shard,
};

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
    ) -> Result<Dlog<S, T>> {
        let dlog_index = Arc::new(AtomicU64::new(1));

        // purge existing shard/journals for name.
        let mut shards = vec![];
        for shard_id in 0..nshards {
            let index = Arc::clone(&dlog_index);
            let (d, n, l) = (dir.clone(), name.clone(), journal_limit);
            shards.push(Shard::<S, T>::create(d, n, shard_id, index, l)?);
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
    ) -> Result<Dlog<S, T>> {
        let dlog_index = Arc::new(AtomicU64::new(1));

        let mut shards = vec![];
        for shard_id in 0..nshards {
            let index = Arc::clone(&dlog_index);
            let (d, n, l) = (dir.clone(), name.clone(), journal_limit);
            shards.push(Shard::<S, T>::load(d, n, shard_id, index, l)?);
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
    T: Default + Serialize,
{
    type Key: Default + Serialize;
    type Val: Default + Serialize;

    fn on_add_entry(&mut self, entry: &DEntry<T>) -> ();

    fn to_type(&self) -> String;
}

#[cfg(test)]
#[path = "dlog_test.rs"]
mod dlog_test;
