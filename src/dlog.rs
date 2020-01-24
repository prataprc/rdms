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
    borrow::Borrow,
    cmp,
    collections::HashMap,
    convert::TryInto,
    ffi, fs,
    io::{self, Read, Seek, Write},
    mem, path, result,
    sync::{atomic::AtomicU64, mpsc, Arc},
    thread, vec,
};

use crate::{
    core::{Diff, Replay, Result, Serialize},
    dlog_entry,
    {error::Error, util},
};

pub(crate) trait DlogState<T>
where
    T: Default + Serialize,
{
    type Key: Default + Serialize;
    type Val: Default + Serialize;

    fn on_add_entry(&mut self, entry: &dlog_entry::Entry<T>) -> ();

    fn to_type(&self) -> String;
}

// default limit for each journal file size.
const JOURNAL_LIMIT: usize = 1 * 1024 * 1024 * 1024;

/// Dlog entry logging for [`Rdms`] index.
///
/// [Rdms]: crate::Rdms
pub struct Dlog<S, T>
where
    S: Default + Serialize + DlogState,
    T: Default + Serialize,
{
    pub(crate) dir: ffi::OsString,
    pub(crate) name: String,
    pub(crate) journal_limit: usize,

    pub(crate) index: Arc<AtomicU64>, // seqno
    pub(crate) shards: Vec<Shard<S, T>>,
}

impl<S, T> Dlog<S, T>
where
    S: Default + Serialize + DlogState,
    T: Default + Serialize,
{
    /// Create a new [`Dlog`] instance under directory ``dir``, using specified
    /// number of shards ``nshards`` and ``name`` must be unique if more than
    /// only [`Dlog`] instances are going to be created under the same ``dir``.
    pub fn create(dir: ffi::OsString, name: String, nshards: usize, journal_limit: usize) -> Result<Dlog<S, T>> {
        let dlog_index = Arc::new(AtomicU64::new(1)),

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
            journal_limit,

            index: dlog_index,
            shards,
        })
    }

    /// Load an existing [`Dlog`] instance identified by ``name`` under
    /// directory ``dir``.
    pub fn load(dir: ffi::OsString, name: String, nshards: usize, journal_limit: usize) -> Result<Dlog<S, T>> {
        let dlog_index = Arc::new(AtomicU64::new(1)),

        let mut shards = vec![];
        for shard_id in 0..nshards {
            let index = Arc::clone(&dlog_index);
            let (d, n, l) = (dir.clone(), name.clone(), journal_limit);
            shards.push(Shard::<S, T>::load(d, n, shard_id, index, l)?);
        }

        Ok(Dlog {
            dir,
            name,
            journal_limit,

            index: Arc::new(Box::new(AtomicU64::new(index + 1))),
            shards,
        })
    }
}

impl<K, V> Dlog<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    /// When DB suffer a crash and looses latest set of mutations, [`Wal`]
    /// can be used to fetch the latest set of mutations and replay them on
    /// DB. Return total number of operations replayed on DB.
    pub fn replay<W: Replay<K, V>>(mut self, db: &mut W) -> Result<usize> {
        // validate
        if self.is_active() {
            let msg = format!("cannot replay with active shards");
            return Err(Error::InvalidWAL(msg));
        }
        if self.journals.len() == 0 {
            return Ok(0);
        }
        // sort-merge journals from different shards.
        let journal = self.journals.remove(0);
        let mut iter = ReplayIter::new_journal(journal.into_iter()?);
        for journal in self.journals.drain(..) {
            let y = ReplayIter::new_journal(journal.into_iter()?);
            iter = ReplayIter::new_merge(iter, y);
        }
        let mut ops = 0;
        for entry in iter {
            let entry = entry?;
            let index = entry.to_index();
            match entry.into_op() {
                Op::Set { key, value } => {
                    db.set_index(key, value, index)?;
                }
                Op::SetCAS { key, value, cas } => {
                    db.set_cas_index(key, value, cas, index)?;
                }
                Op::Delete { key } => {
                    db.delete_index(key, index)?;
                }
            }
            ops += 1;
        }
        Ok(ops)
    }
}

enum ReplayIter<K, V>
where
    K: Serialize,
    V: Serialize,
{
    JournalIter {
        iter: BatchIter<K, V>,
    },
    MergeIter {
        x: Box<ReplayIter<K, V>>,
        y: Box<ReplayIter<K, V>>,
        x_entry: Option<Result<Entry<K, V>>>,
        y_entry: Option<Result<Entry<K, V>>>,
    },
}

impl<K, V> ReplayIter<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn new_journal(iter: BatchIter<K, V>) -> ReplayIter<K, V> {
        ReplayIter::JournalIter { iter }
    }

    fn new_merge(
        mut x: ReplayIter<K, V>, // journal iterator
        mut y: ReplayIter<K, V>, // journal iterator
    ) -> ReplayIter<K, V> {
        let x_entry = x.next();
        let y_entry = y.next();
        ReplayIter::MergeIter {
            x: Box::new(x),
            y: Box::new(y),
            x_entry,
            y_entry,
        }
    }
}

impl<K, V> Iterator for ReplayIter<K, V>
where
    K: Serialize,
    V: Serialize,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ReplayIter::JournalIter { iter } => iter.next(),
            ReplayIter::MergeIter {
                x,
                y,
                x_entry,
                y_entry,
            } => match (x_entry.take(), y_entry.take()) {
                (Some(Ok(xe)), Some(Ok(ye))) => {
                    let c = xe.to_index().cmp(&ye.to_index());
                    match c {
                        cmp::Ordering::Less => {
                            *x_entry = x.next();
                            *y_entry = Some(Ok(ye));
                            Some(Ok(xe))
                        }
                        cmp::Ordering::Greater => {
                            *y_entry = y.next();
                            *x_entry = Some(Ok(xe));
                            Some(Ok(ye))
                        }
                        cmp::Ordering::Equal => unreachable!(),
                    }
                }
                (Some(Ok(xe)), None) => {
                    *x_entry = x.next();
                    Some(Ok(xe))
                }
                (None, Some(Ok(ye))) => {
                    *y_entry = y.next();
                    Some(Ok(ye))
                }
                (_, Some(Err(err))) => Some(Err(err)),
                (Some(Err(err)), _) => Some(Err(err)),
                _ => None,
            },
        }
    }
}

#[cfg(test)]
#[path = "dlog_test.rs"]
mod dlog_test;
