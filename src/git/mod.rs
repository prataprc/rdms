//! Module implement db like interface into git repository.
//!
//! GIT is a candidate for DBA store. In other words, several of GIT ideas are
//! incorporated into the DBA design. But this wrapper around the `libgit2`
//! is minimalistic. While the types and traits defined under [dba] module maps
//! to this wrapper, it is a read-only mapping. That is we use `libgit2` for all
//! heavy lifting, most noteably, write operations into git-store and convert
//! the native `libgit2` types to [dba] types for efficiency and ergonomics.

use crate::dba;

/// Default git directory under working git repository.
pub const GIT_DIR: &str = ".git";

mod config;
mod index;
mod trie;

pub use config::{Config, InitConfig, OpenConfig, Permissions};
pub use index::{Index, IterLevel, Txn};
use trie::{Node, Op, Trie};

/// Type abstracts write-access into git storage.
pub enum WriteOp<K, V>
where
    K: dba::AsKey,
    V: AsRef<[u8]>,
{
    /// insert leaf component
    Ins { key: K, value: V },
    /// Remove leaf component
    Rem { key: K },
}
