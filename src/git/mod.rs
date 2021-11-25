//! Module implement db like interface into git repository.

use crate::dba;

/// Default git directory under working git repository.
pub const GIT_DIR: &str = ".git";

mod config;
mod index;
mod trie;

pub use config::{Config, InitConfig, OpenConfig, Permissions};
pub use index::{Index, IterLevel, Txn};
use trie::{Node, Op, Trie};

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
