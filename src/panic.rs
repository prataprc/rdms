use std::borrow::Borrow;

use crate::core::{Diff, Entry, Footprint, Result, Serialize, Writer};

pub struct Panic;

// Write methods
impl<K, V> Writer<K, V> for Panic
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
{
    fn set(&mut self, _key: K, _value: V) -> Result<Option<Entry<K, V>>> {
        panic!("set operation not allowed !!");
    }

    fn set_cas(&mut self, _: K, _: V, _: u64) -> Result<Option<Entry<K, V>>> {
        panic!("set operation not allowed !!");
    }

    fn delete<Q>(&mut self, _key: &Q) -> Result<Option<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        panic!("set operation not allowed !!");
    }
}
