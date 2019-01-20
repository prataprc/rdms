use std::fmt::Debug;

/// AsKey is an aggregate trait.
pub trait AsKey: Default + Clone + Ord + Debug {}

/// AsValue acts both as aggregate trait and provides necessary
/// methods to handle multiple versions for the same key.
pub trait AsValue<V> where V: Default + Clone {
    /// Value of a mutation version for given key
    fn value(&self) -> V;
    /// Return seqno at which the mutation happened.
    fn seqno(&self) -> u64;
    /// Return whether this mutation involves deleting the key.
    fn is_deleted(&self) -> bool;
}

pub trait AsNode<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    type Value: AsValue<V>;

    /// Key for this node.
    fn key(&self) -> K;
    /// Return latest value for this node.
    fn value(&self) -> Self::Value;
    /// Return all versions for this key.
    fn versions(&self) -> Vec<Self::Value>;
    /// Return last modified seqno.
    fn seqno(&self) -> u64;
    /// Return last mutation timestamp.
    fn access(&self) -> u64;
    /// Return whether this mutation involves deleting the key.
    fn is_deleted(&self) -> bool;
}
