use std::fmt::Debug;

/// AsKey is an aggregate trait for key types.
pub trait AsKey: Default + Clone + Ord + Debug {}

/// AsValue act both as aggregate trait and define behaviour for
/// each version of index-entry.
///
/// Note that in [LSM][lsm] mode, all mutations that happen over an
/// entry will be managed as a log list. In such cases, each mutation
/// shall create a new version for the entry.
///
/// [lsm]: https://en.wikipedia.org/wiki/Log-structured_merge-tree
pub trait AsValue<V>
where
    V: Default + Clone,
{
    /// Return value for this version.
    fn value(&self) -> V;
    /// Return seqno at which the mutation happened.
    fn seqno(&self) -> u64;
    /// Return whether this mutation involves deleting the key.
    fn is_deleted(&self) -> bool;
}

/// AsEntry define behaviour for index-entry contructed over
/// Key-Value <K,V> types.
pub trait AsEntry<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    type Value: AsValue<V>;

    /// Return a copy of entry's key. In bogn-index an entry is
    /// identified by a unique-key.
    fn key(&self) -> K;

    /// Return a copy of entry's latest value.
    fn value(&self) -> Self::Value;

    /// Return a copy of entry's versions.
    ///
    /// In [lsm][lsm] mode, mutations on the same key shall be preserved
    /// as a log list, where each mutation is called as the key's version.
    ///
    /// In non-lsm mode, entries shall have only one version, because all
    /// new muations on the same key will over-write its previous mutation.
    ///
    /// [lsm]: https://en.wikipedia.org/wiki/Log-structured_merge-tree
    fn versions(&self) -> Vec<Self::Value>;

    /// Return last modified seqno.
    fn seqno(&self) -> u64;

    /// Return whether this mutation involves deleting the key.
    fn is_deleted(&self) -> bool;
}

impl AsKey for i32 {}
impl AsKey for i64 {}
impl AsKey for u64 {}
