//! Traits and Types, related to core-database for asynchronous distribution of data.
//!
//! _**Content addressing**_
//!
//! Asynchronous datastores are content addressed, where the contents are called objects
//! and their address is computed using a hash-digest. Hash-digest can also be
//! cryptographically strong. For example, below we compute address for JSON object
//! using SHA1:
//!
//! ```ignore
//! content: "{ "planet": "earth", "size": 6371 }"
//! address: eefb77629ed77802247c30e9462ff8886e9cbcf6
//! ```
//!
//! _**Object**_
//!
//! The design concept of object is such that they can be wired together using
//! parent-child relationship to represent a tree. Refer to [Object] enumeration type.
//!
//! _**Object-id aka `Oid`**_
//!
//! Refer to [Oid] enumeration type. It is typically a hash digest value generated on
//! the object's content.
//!
//!

use crate::Result;

mod entry;
mod git;
mod types;

pub use entry::{Edge, Entry, Object, Oid, Type, User};

/// AsKey trait can be implemented by any type, that can then be used as key to
/// access `dba` datastores.
pub trait AsKey {
    /// Convert type into list of strings. Semantically, each element in the list
    /// can be treated as a children to the previous element and the first element
    /// is a child of a ROOT. This can also be viewed as file-system path.
    fn to_key_path(&self) -> Result<Vec<String>>;
}

pub fn to_content_key(s: &str, levels: usize) -> Option<String> {
    match s.len() {
        0 => None,
        _ => {
            let mut ss: Vec<String> =
                s.chars().take(levels).map(|ch| ch.to_string()).collect();
            ss.push(s.to_string());
            Some(ss.join("/"))
        }
    }
}
