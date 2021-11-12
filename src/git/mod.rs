//! Module implement db like interface into git repository.

/// Default git directory under working git repository.
pub const GIT_DIR: &'static str = ".git";

mod config;
mod index;

pub use config::{Config, Permissions};
pub use index::{Index, IterLevel, Range, Reverse, Txn};
