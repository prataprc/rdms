//! Bogn provide a collection of algorithms for indexing data either
//! in memory or in disk or in both. Bogn indexes are optimized for
//! document databases and bigdata.
//!
//! Each index will carry a sequence-number as the count of mutations
//! ingested by the index. For every successful mutation, the
//! sequence-number will be incremented and corresponding entry
//! shall be tagged with that sequence-number.
//!
//! Log-Structured-Merge, [LSM], is a common technique used in managing
//! heterogenous data-structures that are transparent to the index. In
//! case of Bogn, in-memory structures are different from on-disk
//! structures, and LSM technique is used to maintain consistency between
//! them.
//!
//! CAS, similar to compare-and-set, can be specified by applications
//! that need consistency gaurantees for a single index-entry. In API
//! context CAS == sequence-number.
//!
//! [LSM]: https://en.wikipedia.org/wiki/Log-structured_merge-tree

// TODO: Document work. Mvcc does not allow concurrent write access.
// and doing so will panic.

#![feature(rc_into_raw_non_null)]
#![feature(bind_by_move_pattern_guards)]
#![feature(drain_filter)]

extern crate jsondata;
extern crate llrb_index;

mod bogn;
mod core;
mod error;
mod spinlock;
mod sync_writer;
mod util;
mod vlog;

pub mod scans;

pub use crate::bogn::Bogn;
pub use crate::core::{Diff, Entry, Replay, Result, Serialize, VersionIter};
pub use crate::core::{Index, IndexIter, Reader, Writer};
pub use crate::error::Error;
pub use crate::spinlock::RWSpinlock;

mod robt_build;
mod robt_config;
mod robt_entry;
mod robt_indx;
mod robt_levels;
mod robt_snap;
mod robt_stats;
/// Read Only BTree for disk based indexes.
///
/// ROBT instances shall have an index file and an optional value-log-file,
/// refer to [Config] for more information.
///
/// [Config]: crate::robt_config::Config
pub mod robt {
    pub use crate::robt_build::Builder;
    pub use crate::robt_config::Config;
    pub use crate::robt_snap::Snapshot;
}

mod type_bytes;
mod type_empty;
mod type_i32;
mod type_i64;
pub use crate::type_empty::Empty; // TODO: proper nomenclature.

pub mod llrb;
mod llrb_node;
mod lsm;
pub mod mvcc;
pub mod wal;

#[cfg(test)]
mod core_test;
#[cfg(test)]
mod llrb_test;
#[cfg(test)]
mod mvcc_test;
#[cfg(test)]
mod spinlock_test;
#[cfg(test)]
mod sync_writer_test;
#[cfg(test)]
mod type_bytes_test;
#[cfg(test)]
mod type_empty_test;
#[cfg(test)]
mod type_i32_test;
#[cfg(test)]
mod type_i64_test;
#[cfg(test)]
mod util_test;
#[cfg(test)]
mod vlog_test;
