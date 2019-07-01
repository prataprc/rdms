use std::io;

use crate::jsondata;

// TODO: check unused error variants and double check error arguments.
// TODO: Generic but meaningful error messages.
// TODO: Document error variants.

/// Error enumerates over all possible errors that this package
/// shall return.
#[derive(Debug)]
pub enum Error {
    /// Can be returned by set_cas() API when:
    /// * In non-lsm mode, requested entry is missing but specified
    ///   CAS is not ZERO. Note that this combination is an alias for
    ///   create-only operation.
    /// * In lsm mode, requested entry is marked as deleted, and
    ///   specifed CAS is neither ZERO, nor matching with entry's
    ///   last modified sequence-number.
    /// * Requested entry's last modified sequence-number does not
    ///   match with specified CAS.
    InvalidCAS,
    /// Fatal case, breaking one of the two LLRB rules.
    ConsecutiveReds,
    /// Fatal case, breaking one of the two LLRB rules. The String
    /// component of this variant can be used for debugging. The
    /// first parameter in the tuple gives the number of blacks
    /// found on the left child, the second parameter gives for right
    /// child.
    UnbalancedBlacks(usize, usize),
    /// Fatal case, index entries are not in sort-order. The two
    /// keys are the mismatching items.
    SortError(String, String),
    /// Duplicated keys are not allowed in the index. Each and every
    /// Key must be unique.
    DuplicateKey(String),
    /// Llrb and Mvcc index uses dirty node marker for newly
    /// created nodes in its mutation path.
    DirtyNode,
    /// Supplied key is not found in the index.
    KeyNotFound,
    /// Error converting one type to another type.
    FailConversion(String),
    /// Expected a native value. TODO: hide this ?
    NotNativeValue,
    /// Expected a native delta. TODO: hide this ?
    NotNativeDelta,
    /// Key size, after serializing, exceeds limit.
    KeySizeExceeded(usize),
    /// Value size, after serializing, exceeds limit.
    ValueSizeExceeded(usize),
    /// Value-diff size, after serializing, exceeds limit.
    DiffSizeExceeded(usize),
    /// De-serialization failed.
    DecodeFail(String),
    /// Unable to read expected bytes from file.
    PartialRead(String, usize, usize),
    InvalidFile(String),
    IoError(io::Error),
    PartialWrite(usize, usize),
    ValueDecode(Vec<u8>),
    JsonError(jsondata::Error),
    InvalidSnapshot(String),
    Utf8Error(std::str::Utf8Error),
    /// Invalid batch in WAL, write-ahead-log.
    InvalidBatch(String),
    // Local error, means, given key is less than the entire data set.
    __LessThan,
    // z-block of btree has overflowed.
    __ZBlockOverflow(usize),
    // m-block of btree has overflowed.
    __MBlockOverflow(usize),
    // iteration exhausted in m-block entries.
    __MBlockExhausted(usize),
    // iteration exhausted in z-block entries.
    __ZBlockExhausted(usize),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IoError(err)
    }
}

impl From<jsondata::Error> for Error {
    fn from(err: jsondata::Error) -> Error {
        Error::JsonError(err)
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Error {
        Error::Utf8Error(err)
    }
}

impl PartialEq for Error {
    fn eq(&self, other: &Error) -> bool {
        use Error::InvalidFile;
        use Error::{ConsecutiveReds, DirtyNode, InvalidCAS};

        match (self, other) {
            (InvalidCAS, InvalidCAS) => true,
            (ConsecutiveReds, ConsecutiveReds) => true,
            (DirtyNode, DirtyNode) => true,
            (InvalidFile(s1), InvalidFile(s2)) => s1 == s2,
            _ => false,
        }
    }
}
