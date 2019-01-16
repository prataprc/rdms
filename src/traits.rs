pub(crate) trait KeyTrait: Default + Clone + PartialEq + PartialOrd + Serialize {}

pub(crate) trait ValueTrait: Default + Clone + Serialize {}

pub(crate) trait NodeTrait<K, V>
where
    K: KeyTrait,
    V: ValueTrait,
{
    fn get_key() -> K;
    fn get_value() -> V;
    fn get_seqno() -> u64;
    fn is_delete() -> bool;
}

pub trait Serialize {
    fn encode(&self, buffer: &mut Vec<u8>);
    fn decode(&mut self, buffer: Vec<u8>);
}
