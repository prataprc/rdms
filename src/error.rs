/// BognError enumerates over all possible errors that this package
/// shall return.
#[derive(PartialEq)]
pub enum BognError<K> {
    /// Applicable to set_cas() API. This error is returned when:
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
    SortError(K, K),
    /// Duplicated keys are not allowed in the index. Each and every
    /// Key must be unique.
    DuplicateKey(K),
    /// MVCC algorithm uses dirty node marker for newly created nodes
    /// in its mutation path.
    DirtyNode,
}
