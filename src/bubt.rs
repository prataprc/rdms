use crate::core::{AsDelta, AsEntry, Diff, Serialize};
use crate::vlog;

pub struct Entry<K, V, T>
where
    K: Clone + Ord,
    V: Default + Clone + Diff + Serialize,
    T: Clone + AsDelta<V>,
{
    pub(crate) key: K,
    pub(crate) value_log: vlog::Value<V>,
    pub(crate) seqno: u64,
    pub(crate) is_deleted: bool,
    pub(crate) deltas: Vec<T>,
}

impl<K, V, T> Entry<K, V, T>
where
    K: Clone + Ord,
    V: Default + Clone + Diff + Serialize,
    T: Clone + AsDelta<V>,
{
    pub(crate) fn new<E>(e: E) -> Entry<K, V, T>
    where
        E: AsEntry<K, V>,
    {
        Entry {
            key: e.key(),
            value_log: e.value(),
            seqno: e.seqno(),
            is_deleted: e.is_deleted(),
            deltas: e.deltas(),
        }
    }

    fn encode(&self) -> (Vec<u8>, Vec<u8>) {
        (vec![], vec![])
    }
}
