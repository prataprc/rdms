use std::io;

use crate::jsondata;

// TODO: check unused error variants and double check error arguments.

/// BognError enumerates over all possible errors that this package
/// shall return.
#[derive(Debug)]
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
    KeyNotFound,
    InvalidFile(String),
    IoError(io::Error),
    PartialRead(usize, usize),
    PartialWrite(usize, usize),
    ValueDecode(Vec<u8>),
    ZBlockOverflow(usize),
    JsonError(jsondata::Error),
    InvalidSnapshot(String),
    Utf8Error(std::str::Utf8Error),
    ZBlockExhausted,
    MBlockExhausted,
}

impl From<io::Error> for BognError {
    fn from(err: io::Error) -> BognError {
        BognError::IoError(err)
    }
}

impl From<jsondata::Error> for BognError {
    fn from(err: jsondata::Error) -> BognError {
        BognError::JsonError(err)
    }
}

impl From<std::str::Utf8Error> for BognError {
    fn from(err: std::str::Utf8Error) -> BognError {
        BognError::Utf8Error(err)
    }
}

impl PartialEq for BognError {
    fn eq(&self, other: &BognError) -> bool {
        use BognError::{ConsecutiveReds, DirtyNode, InvalidCAS};

        match (self, other) {
            (InvalidCAS, InvalidCAS) => true,
            (ConsecutiveReds, ConsecutiveReds) => true,
            (DirtyNode, DirtyNode) => true,
            _ => false,
        }
    }
}

//impl PartialEq for BognError {
//    fn eq(&self, other: &BognError) -> bool {
//        use BognError::{ConsecutiveReds, DirtyNode, InvalidCAS};
//
//        match (self, other) {
//            (InvalidCAS, InvalidCAS) => true,
//            (ConsecutiveReds, ConsecutiveReds) => true,
//            (DirtyNode, DirtyNode) => true,
//            _ => false,
//        }
//    }
//}
