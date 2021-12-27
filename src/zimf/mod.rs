//! Module implement zim web-archive parser.

mod workers;
mod zimf;

pub use zimf::{Cluster, Compression, Entry, Header, Namespace, Zimf};
