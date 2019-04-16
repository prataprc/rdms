/// Diffable values.
///
/// O = previous value
/// N = next value
/// D = difference between O and N
///
/// Then,
///
/// D = N - O (diff operation)
/// O = N - D (merge operation)
pub trait Diff {
    type D: Default + Clone;

    /// Return the delta between two version of value.
    /// D = N - O
    fn diff(&self, other: &Self) -> Self::D;

    /// Merge delta with this value to create another value.
    /// O = N - D
    fn merge(&self, other: &Self::D) -> Self;
}

/// AsDelta define behaviour for each version of an index-entry.
///
/// Note that in [LSM] mode, all mutations that happen over an
/// entry will be managed as a log list. In such cases, each mutation
/// shall create a new version for the entry.
///
/// [LSM]: https://en.wikipedia.org/wiki/Log-structured_merge-tree
pub trait AsDelta<V>
where
    V: Default + Clone + Diff,
{
    /// Return a copy of difference.
    fn delta(&self) -> <V as Diff>::D;

    /// Return a reference to difference.
    fn delta_ref(&self) -> &<V as Diff>::D;

    /// Return a mutable reference to difference.
    fn delta_mut(&mut self) -> &mut <V as Diff>::D;

    /// Return sequence-number at which the mutation happened.
    fn seqno(&self) -> u64;

    /// Return whether this version is marked as deleted. Valid
    /// only in LSM mode.
    fn is_deleted(&self) -> bool;
}

/// AsEntry define behaviour for a single index-entry parametrised
/// over Key-Value <K,V> types.
pub trait AsEntry<K, V>
where
    K: Default + Clone + Ord,
    V: Default + Clone + Diff,
{
    type Delta: Clone + AsDelta<V>;

    /// Return a copy of entry's key. In bogn-index each entry is
    /// identified by unique-key.
    fn key(&self) -> K;

    /// Return a reference to entry's key.
    fn key_ref(&self) -> &K;

    /// Return a copy of the latest value.
    fn value(&self) -> V;

    /// Return a reference to entry's latest value.
    fn value_ref(&self) -> &V;

    /// Return the sequence-number of most recent mutation for this entry.
    fn seqno(&self) -> u64;

    /// Valid only in LSM mode. Return whether this entry is marked as
    /// deleted.
    fn is_deleted(&self) -> bool;

    /// Return previous versions as delta of current version. The current
    /// version (A), the previous version (B), and the difference between
    /// the two (D) share the following relation ship.
    ///
    /// Op(A) = B | where Op is operation on A.
    /// A - D = B
    /// A = B + D
    /// By successively applying the delta on the latest version we get
    /// the previous version.
    fn deltas(&self) -> Vec<Self::Delta>;
}

pub trait Serialize: Sized {
    fn encode(&self, buf: Vec<u8>) -> Vec<u8>;

    fn decode(buf: &[u8]) -> Result<Self, String>;
}
