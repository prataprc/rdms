// TODO: Try to remove Default trait constraint from K and V parameters.

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
/// O = N - D (merge operation, to get old value)
pub trait Diff {
    type D: Default + Clone + Serialize;

    /// Return the delta between two version of value.
    /// D = N - O
    fn diff(&self, old: &Self) -> Self::D;

    /// Merge delta with this value to create another value.
    /// O = N - D
    fn merge(&self, delta: &Self::D) -> Self;
}

/// Serialize types and values to binary sequence of bytes.
pub trait Serialize: Sized {
    /// Convert this value into binary equivalent.
    fn encode(&self, buf: &mut Vec<u8>);

    /// Reverse process of encode, given the binary equivalent, `buf`,
    /// of a value, construct self.
    fn decode(&mut self, buf: &[u8]) -> Result<()>;
}

/// Delta maintains the older version of value, with necessary fields for
/// log-structured-merge.
#[derive(Clone)]
pub struct Delta<V>
where
    V: Default + Clone + Diff + Serialize,
{
    delta: vlog::Delta<V>, // actual value
    seqno: u64,            // when this version mutated
    deleted: Option<u64>,  // for lsm, deleted can be > 0
}

// Delta construction methods.
impl<V> Delta<V>
where
    V: Default + Clone + Diff + Serialize,
{
    pub(crate) fn new(
        delta: vlog::Delta<V>, // construct with any variant
        seqno: u64,
        deleted: Option<u64>,
    ) -> Delta<V> {
        Delta {
            delta,
            seqno,
            deleted,
        }
    }

    /// Use facing values of Delta must constructed using this API.
    fn new_delta(
        delta: <V as Diff>::D, // construct with native value.
        seqno: u64,
        deleted: Option<u64>,
    ) -> Delta<V> {
        Delta {
            delta: vlog::Delta::new_native(delta),
            seqno,
            deleted,
        }
    }
}

/// Read methods.
impl<V> Delta<V>
where
    V: Default + Clone + Diff + Serialize,
{
    pub(crate) fn vlog_delta_ref(&self) -> &vlog::Delta<V> {
        &self.delta
    }

    /// Return the underlying `difference` value for this delta.
    pub fn diff(&self) -> <V as Diff>::D {
        match &self.delta {
            vlog::Delta::Native { delta } => delta.clone(),
            vlog::Delta::Reference { .. } | vlog::Delta::Backup { .. } => {
                panic!("impossible situation, call the programmer!")
            }
        }
    }

    /// Return the seqno at which this delta was modified,
    /// which includes Create and Delete operations.
    /// To differentiate between Create and Delete operations
    /// use born_seqno() and dead_seqno() methods respectively.
    pub fn seqno(&self) -> u64 {
        self.deleted.unwrap_or(self.seqno)
    }

    /// Return the seqno at which this delta was created.
    pub fn born_seqno(&self) -> u64 {
        self.seqno
    }

    /// Return the seqno at which this delta was deleted.
    pub fn dead_seqno(&self) -> Option<u64> {
        self.deleted
    }

    /// Return whether this delta was deleted.
    pub fn is_deleted(&self) -> bool {
        self.deleted.is_some()
    }
}

/// Entry, the covering structure for a {Key, value} pair
/// indexed by bogn. It is a user facing structure and also
/// used in stitching together different components of Bogn.
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

// NOTE: user-facing entry values must be constructed with
// native value and native deltas.
// Entry construction methods.
impl<K, V> Entry<K, V>
where
    K: Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
{
    pub(crate) fn new(
        key: K,
        value: vlog::Value<V>,
        seqno: u64,
        deleted: Option<u64>,
        deltas: Vec<Delta<V>>,
    ) -> Entry<K, V> {
        Entry {
            key,
            value,
            seqno,
            deleted: deleted,
            deltas: deltas,
        }
    }

    pub(crate) fn new_entry(key: K, value: V, seqno: u64) -> Entry<K, V> {
        Entry {
            key,
            value: vlog::Value::new_native(value),
            seqno,
            deleted: None,
            deltas: vec![],
        }
    }
}

// write/update methods.
impl<K, V> Entry<K, V>
where
    K: Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
{
    // Prepend a new version, also the lates version, for this entry.
    // In non-lsm mode this is equivalent to over-writing previous value.
    pub(crate) fn prepend_version(&mut self, value: V, seqno: u64, lsm: bool) {
        if lsm {
            self.prepend_version_lsm(value, seqno)
        } else {
            self.prepend_version_nolsm(value, seqno)
        }
    }

    fn prepend_version_lsm(&mut self, value: V, seqno: u64) {
        match &self.value {
            vlog::Value::Native { value: old_value } => {
                let d = value.diff(old_value);
                let delta = Delta::new_delta(d, self.seqno, self.deleted);
                self.deltas.insert(0, delta);
                self.value = vlog::Value::new_native(value);
                self.seqno = seqno;
                self.deleted = None;
            }
            vlog::Value::Backup { .. } => {
                // TODO: Figure out a way to use {file, fpos, length} to
                // get the entry details from disk. Note that disk index
                // can have different formats based on configuration.
                // Take that into account.
                panic!("TBD")
            }
            vlog::Value::Reference { .. } => panic!("impossible situation"),
        }
    }

    fn prepend_version_nolsm(&mut self, value: V, seqno: u64) {
        self.value = vlog::Value::new_native(value);
        self.seqno = seqno;
        self.deleted = None;
    }

    // if entry is already deleted, this call becomes a no-op.
    pub(crate) fn delete(&mut self, seqno: u64) {
        if self.deleted.is_none() {
            self.deleted = Some(seqno)
        }
    }

    pub(crate) fn purge(&mut self, before: u64) -> bool {
        if self.seqno < before {
            // purge everything
            true
        } else {
            for i in 0..self.deltas.len() {
                if self.deltas[i].seqno < before {
                    self.deltas.truncate(i); // purge everything from i..len
                    break;
                }
            }
            false
        }
    }
}

// read methods.
impl<K, V> Entry<K, V>
where
    K: Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
{
    #[inline]
    pub(crate) fn vlog_value_ref(&self) -> &vlog::Value<V> {
        &self.value
    }

    #[inline]
    pub(crate) fn deltas_ref(&self) -> &[Delta<V>] {
        &self.deltas
    }

    /// Return ownership of key.
    #[inline]
    pub fn key(self) -> K {
        self.key
    }

    /// Return a reference to key.
    #[inline]
    pub fn key_ref(&self) -> &K {
        &self.key
    }

    /// Return value.
    pub fn value(&self) -> V {
        match &self.value {
            vlog::Value::Native { value } => value.clone(),
            vlog::Value::Reference { .. } | vlog::Value::Backup { .. } => {
                panic!("impossible situation, call the programmer")
            }
        }
    }

    /// Return the latest seqno that created/updated/deleted this entry.
    #[inline]
    pub fn seqno(&self) -> u64 {
        self.deleted.unwrap_or(self.seqno)
    }

    /// Return the seqno that created or updated the latest value for this
    /// entry.
    #[inline]
    pub fn born_seqno(&self) -> u64 {
        self.seqno
    }

    /// Return the seqno that deleted the latest value for this entry.
    #[inline]
    pub fn dead_seqno(&self) -> Option<u64> {
        self.deleted
    }

    /// Return whether the entry is deleted.
    #[inline]
    pub fn is_deleted(&self) -> bool {
        self.deleted.is_some()
    }

    /// Return the previous versions of this entry as Deltas.
    #[inline]
    pub fn deltas(&self) -> Vec<Delta<V>> {
        self.deltas.clone()
    }
}

pub type Result<T> = std::result::Result<T, BognError>;
