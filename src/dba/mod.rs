//! Traits and Types, related to core-database. The kind of database that follows
//! asynchronous distribution of data, uses content addressing.

use crate::Result;

mod entry;
mod types;

pub use entry::{Entry, Object, Type, User};

pub enum Oid {
    Sha1 { hash: [u8; 20] },
}

impl Oid {
    fn from_sha1(bytes: &[u8]) -> Oid {
        let mut hash = [0; 20];
        hash[..].copy_from_slice(bytes);
        Oid::Sha1 { hash }
    }
}

pub trait AsKey {
    fn to_key_path(&self) -> Result<Vec<String>>;
}
