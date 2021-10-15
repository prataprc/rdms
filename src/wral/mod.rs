//! Module implement Write-Ahead-Logging.
//!
//! Write-Ahead-Logging is implemented by [Wal] type, to get started create
//! first a configuration [Config] value. Subsequently, a fresh Wal instance can be
//! created or existing Wal from disk can be loaded, using the configuration.
//! Wal optionally takes a type parameter `S` for state, that can be used by
//! application to persist storage state along with each batch.
//! By default, `NoState` is used.
//!
//! Concurrent writers
//! ------------------
//!
//! [Wal] writes are batch-processed, where batching is automatically dictated
//! by storage (disk, ssd) latency. Latency can get higher when `fsync` is
//! enabled for every batch flush. With fsync enabled it is hard to reduce
//! the latency, and to get better throughput applications can do concurrent
//! writes. This is possible because [Wal] type can be cloned with underlying
//! structure safely shared among all the clones. For example,
//!
//! ```ignore
//! let wal = wral::Wal::create(config, wral::NoState).unwrap();
//! let mut writers = vec![];
//! for id in 0..n_threads {
//!     let wal = wal.clone();
//!     writers.push(std::thread::spawn(move || writer(id, wal)));
//! }
//! ```
//!
//! Application employing concurrent [Wal] must keep in mind that `seqno`
//! generated for consecutive ops may not be monotonically increasing within
//! the same thread, and must make sure to serialize operations across the
//! writers through other means.
//!
//! Concurrent readers
//! ------------------
//!
//! It is possible for a [Wal] value and its clones to concurrently read the
//! log journal (typically iterating over its entries). Remember that read
//! operations shall block concurrent writes and vice-versa. But concurrent
//! reads shall be allowed.

use std::ffi;

mod batch;
mod entry;
mod files;
mod journal;
mod journals;
mod state;
mod wral;

pub use crate::wral::entry::Entry;
pub use crate::wral::state::{NoState, State};
pub use crate::wral::wral::Wal;

/// Default journal file limit is set at 1GB.
pub const JOURNAL_LIMIT: usize = 1024 * 1024 * 1024;
/// Default channel buffer for flush thread.
pub const SYNC_BUFFER: usize = 1024;

/// Configuration for [Wal] type.
#[derive(Debug, Clone)]
pub struct Config {
    /// Uniquely name Wal instances.
    pub name: String,
    /// Directory in which wral journals are stored.
    pub dir: ffi::OsString,
    /// Define file-size limit for a single journal file, beyond which journal files
    /// are rotated.
    pub journal_limit: usize,
    /// Enable fsync for every flush.
    pub fsync: bool,
}

impl<'a> arbitrary::Arbitrary<'a> for Config {
    fn arbitrary(u: &mut arbitrary::Unstructured) -> arbitrary::Result<Self> {
        use std::env;

        let name: String = u.arbitrary()?;
        let dir = env::temp_dir().into_os_string();

        let journal_limit = *u.choose(&[100, 1000, 10_000, 1_000_000])?;
        let fsync: bool = u.arbitrary()?;

        let config = Config {
            name,
            dir,
            journal_limit,
            fsync,
        };
        Ok(config)
    }
}

impl Config {
    pub fn new(dir: &ffi::OsStr, name: &str) -> Config {
        Config {
            name: name.to_string(),
            dir: dir.to_os_string(),
            journal_limit: JOURNAL_LIMIT,
            fsync: true,
        }
    }

    pub fn set_journal_limit(&mut self, journal_limit: usize) -> &mut Self {
        self.journal_limit = journal_limit;
        self
    }

    pub fn set_fsync(&mut self, fsync: bool) -> &mut Self {
        self.fsync = fsync;
        self
    }
}
