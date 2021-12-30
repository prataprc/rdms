//! Module implement zim web-archive parser.

mod workers;
mod zim;

pub use zim::{Cluster, Compression, Entry, Header, Namespace, Zimf};
