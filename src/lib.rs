//! Package **Rdms** provide a collection of algorithms for indexing
//! data either _in memory_ or _in disk_ or _both_. Rdms indexes are
//! optimized for document databases and bigdata.
//!
//! Features:
//!
//! * Provide CRUD API, Create, Read, Update, Delete.
//! * Parameterized over a key-type (**K**) and a value-type (**V**).
//! * Parameterized over Memory-index and Disk-index.
//! * Memory index suitable for data ingestion and caching frequently
//!   accessed key.
//! * Concurrent reads, with single concurrent write.
//! * Concurrent writes (_Work in progress_).
//! * Version control, centralised.
//! * Version control, distributed (_Work in progress_).
//! * Log Structured Merge for multi-level indexing.
//!
//! **Key**, each data shall be indexed using an associated key. A key
//! and its corresponding data, also called its value, is called as an
//! indexed entry.
//!
//! **CRUD**, means [basic set of operations][CRUD] expected for storing
//! data. All storage operation shall involve, application supplied, key.
//!
//! **Seqno**, each index instance shall carry a 64-bit sequence-number as
//! the count of mutations ingested by the index. Every time a mutating
//! method is called the sequence-number will be incremented and
//! corresponding entry, if successful, shall be tagged with that
//! sequence-number.
//!
//! **Log-Structured-Merge [LSM]**, is a common technique used in managing
//! heterogenous data-structures that are transparent to the index. In
//! case of Rdms, in-memory structures are different from on-disk
//! structures, and LSM technique is used to maintain consistency between
//! them.
//!
//! **CAS**, a.k.a compare-and-set, can be specified by applications
//! that need consistency gaurantees for a single index-entry. In API
//! context CAS is same as _sequence-number_.
//!
//! **Piece-wise full-table scanning**, in many cases long running scans
//! are bad for indexes using locks and/or multi-version-concurrency-control.
//! And there _will_ be situations where a full table scan is required,
//! while allowing background read/write operations. Piece-wise scanning
//! can help in those situations, provided the index is configured for LSM.
//!
//! **Compaction**
//!
//! Compaction is the process of de-duplicating/removing entries from an
//! index instance. In `rdms` there are three types of compaction.
//!
//! _deduplication_
//!
//! This is typically applicable only for disk indexes. Disk index types,
//! that use append only design, leave behind old entries as garbage blocks
//! when they are modified. This involves a periodic clean up of garbage
//! entries/blocks to reduce disk foot-print.
//!
//! _mono-compaction_
//!
//! This is applicable for index instances that do not need system-level
//! LSM. In such cases, the oldest-level's snapshot can compact away older
//! versions of each entry and purge entries that are marked deleted.
//!
//! _lsm-compaction_
//!
//! Rdms, unlike other lsm-based-storage, can have the entire index
//! as LSM for system level database designs. To be more precise, in lsm
//! mode, even the root level that holds the entire dataset can retain
//! older versions. With this feature it is possible to design secondary
//! indexes, network distribution and other features like `backup` and
//! `archival` ensuring consistency. This also means the index footprint
//! will indefinitely accumulate older versions. With limited disk space,
//! it is upto the application logic to issue `lsm-compaction` when
//! it is safe to purge entries/versions that are older than certain seqno.
//!
//! _tombstone-compaction_
//!
//! Tombstone compaction is similar to `lsm-compaction` which one main
//! difference. When application logic issue `tombstone-compaction` only
//! deleted entries that are older than specified seqno will be purged.
//!
//! [LSM]: https://en.wikipedia.org/wiki/Log-structured_merge-tree
//! [CRUD]: https://en.wikipedia.org/wiki/Create,_read,_update_and_delete
//!

#![feature(drain_filter)]

#[macro_use]
pub mod error;

// core modules
pub mod core;
mod entry;
pub mod panic;
pub mod spinlock;
pub mod sync;
mod sync_writer;
pub mod thread;
pub mod types;
#[macro_use]
mod util;
mod vlog;

// support modules
pub mod dlog;
mod dlog_entry;
mod dlog_journal;
pub mod lsm;
pub mod scans;

// write ahead logging.
pub mod wal;

// raft
pub mod raft_log;

// mem index
pub mod llrb;
mod llrb_node;
pub mod mvcc;
pub mod shllrb;
// disk index
pub mod dgm;
pub mod nodisk;
pub mod robt;
mod robt_entry;
mod robt_index;
pub mod shrobt;
// pub mod backup; TODO

// bloom filters.
pub mod croaring;
pub mod nobitmap;

pub mod rdms;
pub use crate::rdms::Rdms;
