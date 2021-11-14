//! Traits and Types, related to core-database. The kind of database that follows
//! asynchronous distribution of data, uses content addressing.

use crate::Result;

mod entry;
mod types;

pub use entry::{Edge, Entry, Object, Oid, Type, User};

pub trait AsKey {
    fn to_key_path(&self) -> Result<Vec<String>>;
}
