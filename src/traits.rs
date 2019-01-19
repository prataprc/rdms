pub trait AsKey: Default + Clone + Ord + Serialize {}

pub trait AsValue<V> where V: Default + Clone + Serialize {
    fn value(&self) -> V;
    fn seqno(&self) -> u64;
    fn is_deleted(&self) -> bool;
}

pub trait AsNode<K, V>
where
    K: AsKey,
    V: Default + Clone + Serialize,
{
    type Value: AsValue<V>;

    fn key(&self) -> K;
    fn value(&self) -> Self::Value;
    fn versions(&self) -> Vec<Self::Value>;
    fn seqno(&self) -> u64;
    fn access(&self) -> u64;
    fn is_deleted(&self) -> bool;
}

pub trait Serialize {
    fn encode(&self, buffer: &mut Vec<u8>);
    fn decode(&mut self, buffer: Vec<u8>);
}
