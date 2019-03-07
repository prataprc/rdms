/// AsVersion act both as aggregate trait and define behaviour for
/// each version of an index-entry.
///
/// Note that in [LSM] mode, all mutations that happen over an
/// entry will be managed as a log list. In such cases, each mutation
/// shall create a new version for the entry.
///
/// [LSM]: https://en.wikipedia.org/wiki/Log-structured_merge-tree
pub trait AsVersion<V>
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
    K: Default + Clone + Ord,
    V: Default + Clone,
{
    type Version: AsVersion<V>;

    /// Return a copy of entry's key. In bogn-index each entry is
    /// identified by unique-key.
    fn key(&self) -> K;

    /// Return a reference to entry's key.
    fn key_ref(&self) -> &K;

    /// Return reference to entry's latest value. Use [AsVersion] methods
    /// to get value fields.
    fn latest_version(&self) -> &Self::Version;

    /// Return a copy of the latest value.
    fn value(&self) -> V;

    /// Return the sequence-number of most recent mutation for this entry.
    fn seqno(&self) -> u64;

    /// Valid only in LSM mode. Return whether this entry is marked as deleted.
    fn is_deleted(&self) -> bool;

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
    fn versions(&self) -> Vec<Self::Version>;
}
