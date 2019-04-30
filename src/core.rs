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
    fn diff(&self, old: &Self) -> Self::D;

    /// Merge delta with this value to create another value.
    /// O = N - D
    fn merge(&self, delta: &Self::D) -> Self;
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

    pub(crate) fn vlog_delta_ref(&self) -> &vlog::Delta<V> {
        &self.delta
    }

    pub fn delta(&self) -> <V as Diff>::D {
        match &self.delta {
            vlog::Delta::Native { delta } => delta.clone(),
            vlog::Delta::Reference { fpos: _, length: _ } => {
                panic!("impossible situation, call the programmer")
            }
            vlog::Delta::Backup { .. } => panic!("impossible situation"),
        }
    }

    pub fn seqno(&self) -> u64 {
        self.deleted.unwrap_or(self.seqno)
    }

    pub fn born_seqno(&self) -> u64 {
        self.seqno
    }

    pub fn dead_seqno(&self) -> Option<u64> {
        self.deleted
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted.is_some()
    }
}

#[derive(Clone)]
pub struct Entry<K, V>
where
    K: Ord + Clone + Serialize,
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
    K: Ord + Clone + Serialize,
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
                    self.deltas.insert(0, delta);
                    self.value = vlog::Value::new_native(value);
                    self.seqno = seqno;
                    self.deleted = None;
                }
                vlog::Value::Backup { /* file, fpos, length */ .. } => {
                    // TODO: Figure out a way to use {file, fpos, length} to
                    // get the entry details from disk. Note that disk index
                    // can have different formats based on configuration.
                    // Take that into account.
                    panic!("TBD")
                }
                vlog::Value::Reference { .. } => panic!("impossible situation"),
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

    pub(crate) fn vlog_value_ref(&self) -> &vlog::Value<V> {
        &self.value
    }

    pub(crate) fn purge(&mut self, before: u64) -> bool {
        if self.seqno < before {
            // purge everything
            true
        } else {
            for i in 0..self.deltas.len() {
                if self.deltas[i].seqno < before {
                    self.deltas.truncate(i); // purge everything after `i`
                    break;
                }
            }
            false
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
            vlog::Value::Reference { .. } => panic!("impossible situation"),
            vlog::Value::Backup { .. } => panic!("impossible situation"),
        }
    }

    pub fn seqno(&self) -> u64 {
        self.deleted.unwrap_or(self.seqno)
    }

    pub fn born_seqno(&self) -> u64 {
        self.seqno
    }

    pub fn dead_seqno(&self) -> Option<u64> {
        self.deleted
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted.is_some()
    }

    pub fn deltas(&self) -> Vec<Delta<V>> {
        self.deltas.clone()
    }

    pub fn deltas_ref(&self) -> &Vec<Delta<V>> {
        &self.deltas
    }
}
