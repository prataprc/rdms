use arbitrary::Arbitrary;
use cbordata::Cborize;

use std::{
    cmp,
    fmt::{self, Display},
    result,
};

#[allow(unused_imports)]
use crate::wral::Wal;

/// Single Op-entry in Write-ahead-log.
///
/// The actual operation is serialized and opaque to [Wal] instance. Applications
/// can iterate over the [Wal] instance for each entry, that is, an Entry value
/// is typically read-only for applications.
#[derive(Debug, Clone, Eq, Default, Cborize, Arbitrary)]
pub struct Entry {
    pub seqno: u64,  // Seqno for this entry, Monotonically increasing number.
    pub op: Vec<u8>, // Write operation, in serialized format, opaque to logging.
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.seqno.eq(&other.seqno)
    }
}

impl Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "entry<seqno:{}>", self.seqno)
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.seqno.cmp(&other.seqno)
    }
}

impl Entry {
    const ID: u32 = 0x0;

    #[inline]
    pub fn new(seqno: u64, op: Vec<u8>) -> Entry {
        Entry { seqno, op }
    }

    /// Return the entry's seqno.
    #[inline]
    pub fn to_seqno(&self) -> u64 {
        self.seqno
    }

    /// Unwrap entry's seqno and serialized operation.
    #[inline]
    pub fn unwrap(self) -> (u64, Vec<u8>) {
        (self.seqno, self.op)
    }
}

#[cfg(test)]
#[path = "entry_test.rs"]
mod entry_test;
