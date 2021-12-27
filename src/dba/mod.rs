//! Traits and Types, related to core-database for asynchronous distribution of data.

use crate::Result;

mod entry;
mod git;
mod types;

pub use entry::{Edge, Entry, Object, Oid, Type, User};

pub trait AsKey {
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
