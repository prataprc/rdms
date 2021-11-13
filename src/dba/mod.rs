//! Traits and Types, related to core-database. The kind of database that follows
//! asynchronous distribution of data, uses content addressing.

use crate::Result;

mod entry;
mod types;

pub use entry::{Entry, Object, Type, User};

pub enum Hash {
    Sha1 { hash: [u8; 20] },
}

pub trait AsKey {
    fn to_key_path(&self) -> Result<Vec<String>>;
}

pub trait AsValue {
    fn to_sha1(&self) -> Result<[u8; 20]>;
}
