//! Bogn provide a collection of algorithms for indexing data either
//! in memory or in disk or in both. Bogn indexes are defined for
//! document databases and Bigdata. This means:
//!
//! Each index will carry a sequence-number, starting from ZERO, as
//! a count of mutations ingested by the index. For every successful
//! mutation, the sequence-number will be incremented and the entry,
//! on which the mutation was applied, will have that sequence-number
//! attached.
//!
//! [`Log-Structured-Merge`] is
//!
/// [lsm]: https://en.wikipedia.org/wiki/Log-structured_merge-tree
mod empty;
mod error;
mod llrb;
mod traits;

pub use crate::empty::Empty;
pub use crate::error::BognError;
pub use crate::llrb::Llrb;
pub use crate::traits::{AsEntry, AsKey, AsValue};

#[cfg(test)]
mod llrb_test;
