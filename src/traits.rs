use std::fmt::Debug;

/// AsKey is an aggregate trait for key types.
pub trait AsKey: Default + Clone + Ord + Debug {}

/// AsValue act both as aggregate trait and define behaviour for
/// each version of an index-entry.
///
/// Note that in [LSM] mode, all mutations that happen over an
/// entry will be managed as a log list. In such cases, each mutation
/// shall create a new version for the entry.
///
/// [LSM]: https://en.wikipedia.org/wiki/Log-structured_merge-tree
pub trait AsValue<V>
where
    V: Default + Clone,
{
    /// Return a copy of the value for this version.
    fn value(&self) -> V;
    /// Return sequence-number at which the mutation happened.
    fn seqno(&self) -> u64;
    /// Valid only in LSM mode. Return whether this version is marked as
    /// deleted.
    fn is_deleted(&self) -> bool;
}

/// AsEntry define behaviour for a single index-entry parametrised over
/// Key-Value <K,V> types.
pub trait AsEntry<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    type Value: AsValue<V>;

    /// Return a copy of entry's key. In bogn-index each entry is
    /// identified by unique-key.
    fn key(&self) -> K;

    /// Return a copy of entry's latest value.
    fn value(&self) -> Self::Value;

    /// Return a copy of entry's versions.
    ///
    /// In [LSM] mode, mutations on the same key shall be preserved
    /// as a log list. And versions() shall return a [`Vec`] of all mutations
    /// for this node, latest first and oldest last.
    ///
    /// In non-lsm mode, entries shall have only one version, because
    /// newer muations on the same key will over-write its previous mutation.
    /// And versions() shall return a [`Vec`] with arity one.
    ///
    /// [lsm]: https://en.wikipedia.org/wiki/Log-structured_merge-tree
    fn versions(&self) -> Vec<Self::Value>;

    /// Return the sequence-number of most recent mutation for this entry.
    fn seqno(&self) -> u64;

    /// Valid only in LSM mode. Return whether this entry is marked as deleted.
    fn is_deleted(&self) -> bool;
}

impl AsKey for i64 {}
impl AsKey for i32 {}
impl AsKey for u64 {}
