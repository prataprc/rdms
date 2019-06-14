//! Bogn provide a collection of algorithms for indexing data either
//! in memory or in disk or in both. Bogn indexes are optimized for
//! document databases and bigdata. This means:
//!
//! Each index will carry a sequence-number as the count of mutations
//! ingested by the index. For every successful mutation, the
//! sequence-number will be incremented and the entry, on which the
//! mutation was applied, shall be tagged with that sequence-number.
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
#![feature(copy_within)]
#![feature(bind_by_move_pattern_guards)]

extern crate jsondata;

//mod bubt_build;
//mod bubt_config;
//mod bubt_indx;
//mod bubt_snap;
//mod bubt_stats;
mod bubt_entry;
mod core;
mod error;
mod llrb;
mod llrb_node;
mod mvcc;
mod sync_writer;
mod type_bytes;
mod type_empty;
mod type_i32;
mod type_i64;
mod util;
mod vlog;

//pub use crate::bubt_build::Builder;
//pub use crate::bubt_config::Config;
//pub use crate::bubt_snap::Snapshot;
pub use crate::core::{Diff, Serialize};
pub use crate::error::Error;
pub use crate::llrb::Llrb;
pub use crate::llrb_node::LlrbStats;
pub use crate::mvcc::Mvcc;
pub use crate::type_empty::Empty; // TODO: proper nomenclature.

#[cfg(test)]
mod core_test;
#[cfg(test)]
mod llrb_test;
#[cfg(test)]
mod mvcc_test;
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
