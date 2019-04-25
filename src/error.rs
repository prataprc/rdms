use std::{ffi, io};

// TODO: check unused error variants and double check error arguments.

/// BognError enumerates over all possible errors that this package
/// shall return.
pub enum BognError {
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
    /// MVCC algorithm uses dirty node marker for newly created nodes
    /// in its mutation path.
    DirtyNode,
    InvalidFile(ffi::OsString),
    IoError(io::Error),
    PartialRead(usize, usize),
    PartialWrite(usize, usize),
    ValueDecode(Vec<u8>),
}

impl From<io::Error> for BognError {
    fn from(err: io::Error) -> BognError {
        BognError::IoError(err)
    }
}

impl PartialEq for BognError {
    fn eq(&self, other: &BognError) -> bool {
        use BognError::{ConsecutiveReds, InvalidCAS, UnbalancedBlacks};
        use BognError::{DirtyNode, DuplicateKey, InvalidFile, SortError};
        use BognError::{IoError, PartialRead, PartialWrite, ValueDecode};

        match (self, other) {
            (InvalidCAS, InvalidCAS) => true,
            (ConsecutiveReds, ConsecutiveReds) => true,
            (DirtyNode, DirtyNode) => true,
            _ => false,
        }
    }
}
