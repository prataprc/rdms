use crate::error::BognError;
use crate::vlog;

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
    type D: Default + Clone + Serialize;

    /// Return the delta between two version of value.
    /// D = N - O
    fn diff(&self, other: &Self) -> Self::D;

    /// Merge delta with this value to create another value.
    /// O = N - D
    fn merge(&self, other: &Self::D) -> Self;
}

pub trait Serialize: Sized {
    fn encode(&self, buf: &mut Vec<u8>);

    fn decode(&mut self, buf: &[u8]) -> Result<(), BognError>;
}

#[derive(Clone)]
pub struct Delta<V>
where
    V: Default + Clone + Diff + Serialize,
{
    delta: vlog::Delta<V>, // actual value
    seqno: u64,            // when this version mutated
    deleted: Option<u64>,  // for lsm, deleted can be > 0
}

impl<V> Delta<V>
where
    V: Default + Clone + Diff + Serialize,
{
    fn new(delta: <V as Diff>::D, seqno: u64, deleted: Option<u64>) -> Delta<V> {
        Delta {
            delta: vlog::Delta::new_native(delta),
            seqno,
            deleted,
        }
    }

    fn delta(&self) -> vlog::Delta<V> {
        self.delta.clone()
    }

    fn seqno(&self) -> u64 {
        self.deleted.unwrap_or(self.seqno)
    }

    fn is_deleted(&self) -> bool {
        self.deleted.is_some()
    }
}

#[derive(Clone)]
pub struct Entry<K, V>
where
    K: Clone + Ord,
    V: Default + Clone + Diff + Serialize,
{
    key: K,
    value: vlog::Value<V>,
    seqno: u64,
    deleted: Option<u64>,
    deltas: Vec<Delta<V>>,
}

impl<K, V> Entry<K, V>
where
    K: Clone + Ord,
    V: Default + Clone + Diff + Serialize,
{
    pub(crate) fn new(key: K, value: V, seqno: u64) -> Entry<K, V> {
        Entry {
            key,
            value: vlog::Value::new_native(value),
            seqno,
            deleted: None,
            deltas: vec![],
        }
    }

    pub(crate) fn prepend_version(&mut self, value: V, seqno: u64, lsm: bool) {
        if lsm {
            match &self.value {
                vlog::Value::Native { value: old_value } => {
                    let d = value.diff(old_value);
                    let delta = Delta::new(d, self.seqno, self.deleted);
                    self.deltas.push(delta);
                    self.value = vlog::Value::new_native(value);
                    self.seqno = seqno;
                    self.deleted = None;
                }
                vlog::Value::Reference { fpos: _, length: _ } => {
                    self.value = vlog::Value::new_native(value);
                    self.seqno = seqno;
                    self.deleted = None;
                }
            }
        } else {
            self.value = vlog::Value::new_native(value);
            self.seqno = seqno;
            self.deleted = None;
        }
    }

    pub(crate) fn delete(&mut self, seqno: u64) {
        if self.deleted.is_none() {
            self.deleted = Some(seqno)
        }
    }

    pub fn key(&self) -> K {
        self.key.clone()
    }

    pub fn key_ref(&self) -> &K {
        &self.key
    }

    pub fn value(&self) -> V {
        match &self.value {
            vlog::Value::Native { value } => value.clone(),
            vlog::Value::Reference { fpos: _, length: _ } => {
                let msg = "impossible situation";
                panic!(msg)
            }
        }
    }

    pub fn seqno(&self) -> u64 {
        self.deleted.unwrap_or(self.seqno)
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted.is_some()
    }

    pub fn deltas(&self) -> Vec<Delta<V>> {
        self.deltas.clone()
    }
}
