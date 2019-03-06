//! Bogn provide a collection of algorithms for indexing data either
//! in memory or in disk or in both. Bogn indexes are defined for
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

#![feature(rc_into_raw_non_null)]

mod empty;
mod error;
mod llrb;
mod llrb_common;
mod llrb_depth;
mod llrb_node;
mod mvcc;
mod sync_writer;
mod traits;

pub use crate::empty::Empty;
pub use crate::error::BognError;
pub use crate::llrb::Llrb;
pub use crate::llrb_common::Stats;
pub use crate::mvcc::Mvcc;
pub use crate::traits::{AsEntry, AsValue};

#[cfg(test)]
mod llrb_test;
#[cfg(test)]
mod mvcc_test;
