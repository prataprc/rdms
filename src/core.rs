use crate::error::Error;
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
pub trait Diff: Sized {
    type D: Clone + From<Self> + Into<Self>;

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
    fn decode(&mut self, buf: &[u8]) -> Result<(), Error>;
}

/// Delta maintains the older version of value, with necessary fields for
/// log-structured-merge.
#[derive(Clone)]
pub enum Delta<V>
where
    V: Clone + Diff,
{
    U { delta: vlog::Delta<V>, seqno: u64 },
    D { deleted: u64 },
}

// Delta construction methods.
impl<V> Delta<V>
where
    V: Clone + Diff,
{
    pub(crate) fn new_upsert(delta: vlog::Delta<V>, seqno: u64) -> Delta<V> {
        Delta::U { delta, seqno }
    }

    pub(crate) fn new_delete(deleted: u64) -> Delta<V> {
        Delta::D { deleted }
    }

    #[allow(dead_code)] // TODO: remove this once bogn is weaved-up.
    pub(crate) fn into_upserted(self) -> Option<(vlog::Delta<V>, u64)> {
        match self {
            Delta::U { delta, seqno } => Some((delta, seqno)),
            Delta::D { .. } => None,
        }
    }

    #[allow(dead_code)] // TODO: remove this once bogn is weaved-up.
    pub(crate) fn into_deleted(self) -> Option<u64> {
        match self {
            Delta::D { deleted } => Some(deleted),
            Delta::U { .. } => None,
        }
    }
}

/// Read methods.
impl<V> Delta<V>
where
    V: Clone + Diff,
{
    /// Return the underlying `difference` value for this delta.
    pub fn into_diff(self) -> Option<<V as Diff>::D> {
        match self {
            Delta::D { .. } => None,
            Delta::U { delta, .. } => delta.into_native(),
        }
    }

    /// Return the seqno at which this delta was modified,
    /// which includes Create and Delete operations.
    /// To differentiate between Create and Delete operations
    /// use born_seqno() and dead_seqno() methods respectively.
    pub fn to_seqno(&self) -> u64 {
        match self {
            Delta::U { seqno, .. } => *seqno,
            Delta::D { deleted } => *deleted,
        }
    }

    /// Return the seqno and the state of modification. `true` means
    /// this version was a create/update, and `false` means
    /// this version was deleted.
    pub fn to_seqno_state(&self) -> (bool, u64) {
        match self {
            Delta::U { seqno, .. } => (true, *seqno),
            Delta::D { deleted } => (false, *deleted),
        }
    }
}

#[derive(Clone)]
pub(crate) enum Value<V>
where
    V: Clone + Diff,
{
    U { value: vlog::Value<V>, seqno: u64 },
    D { deleted: u64 },
}

impl<V> Value<V>
where
    V: Clone + Diff,
{
    pub(crate) fn new_upsert(value: vlog::Value<V>, seqno: u64) -> Value<V> {
        Value::U { value, seqno }
    }

    pub(crate) fn new_delete(deleted: u64) -> Value<V> {
        Value::D { deleted }
    }

    pub(crate) fn to_native_value(&self) -> Option<V> {
        match &self {
            Value::D { .. } => None,
            Value::U {
                value: vlog::Value::Native { value, .. },
                ..
            } => Some(value.clone()),
            Value::U {
                value: vlog::Value::Reference { .. },
                ..
            } => panic!("impossible situation, call the programmer"),
            Value::U {
                value: vlog::Value::Backup { .. },
                ..
            } => panic!("impossible situation, call the programmer"),
        }
    }

    pub(crate) fn is_deleted(&self) -> bool {
        match self {
            Value::U { .. } => false,
            Value::D { .. } => true,
        }
    }
}

/// Entry, the covering structure for a {Key, value} pair
/// indexed by bogn. It is a user facing structure and also
/// used in stitching together different components of Bogn.
#[derive(Clone)]
pub struct Entry<K, V>
where
    K: Ord + Clone,
    V: Clone + Diff,
{
    key: K,
    value: Value<V>,
    deltas: Vec<Delta<V>>,
}

// NOTE: user-facing entry values must be constructed with
// native value and native deltas.
// Entry construction methods.
impl<K, V> Entry<K, V>
where
    K: Ord + Clone,
    V: Clone + Diff,
{
    pub(crate) fn new(key: K, value: Value<V>) -> Entry<K, V> {
        Entry {
            key,
            value,
            deltas: vec![],
        }
    }

    #[allow(dead_code)] // TODO: remove this once bogn is weaved-up.
    pub(crate) fn set_deltas(&mut self, deltas: Vec<Delta<V>>) {
        self.deltas = deltas;
    }
}

// write/update methods.
impl<K, V> Entry<K, V>
where
    K: Ord + Clone,
    V: Clone + Diff,
{
    // Prepend a new version, also the lates version, for this entry.
    // In non-lsm mode this is equivalent to over-writing previous value.
    pub(crate) fn prepend_version(&mut self, new_entry: Self, lsm: bool) {
        if lsm {
            self.prepend_version_lsm(new_entry)
        } else {
            self.prepend_version_nolsm(new_entry)
        }
    }

    fn prepend_version_nolsm(&mut self, new_entry: Self) {
        self.value = new_entry.value.clone();
    }

    fn prepend_version_lsm(&mut self, new_entry: Self) {
        match &self.value {
            Value::D { deleted } => {
                self.deltas.insert(0, Delta::new_delete(*deleted));
            }
            Value::U {
                value: vlog::Value::Native { value },
                seqno,
            } => {
                let d = new_entry.to_native_value().unwrap().diff(value);
                let delta = vlog::Delta::new_native(d);
                self.deltas.insert(0, Delta::new_upsert(delta, *seqno));
            }
            Value::U {
                value: vlog::Value::Backup { .. },
                ..
            } => {
                // TODO: Figure out a way to use {file, fpos, length} to
                // get the entry details from disk. Note that disk index
                // can have different formats based on configuration.
                // Take that into account.
                panic!("TBD")
            }
            Value::U {
                value: vlog::Value::Reference { .. },
                ..
            } => panic!("impossible situation"),
        }
        self.prepend_version_nolsm(new_entry)
    }

    // only lsm, if entry is already deleted this call becomes a no-op.
    pub(crate) fn delete(&mut self, seqno: u64) {
        match &self.value {
            Value::D { .. } => (),
            Value::U {
                value: vlog::Value::Native { value },
                seqno,
            } => {
                let d: <V as Diff>::D = From::from(value.clone());
                let delta = vlog::Delta::new_native(d);
                self.deltas.insert(0, Delta::new_upsert(delta, *seqno));
            }
            Value::U {
                value: vlog::Value::Backup { .. },
                ..
            } => {
                // TODO: Figure out a way to use {file, fpos, length} to
                // get the entry details from disk. Note that disk index
                // can have different formats based on configuration.
                // Take that into account.
                panic!("TBD");
            }
            Value::U {
                value: vlog::Value::Reference { .. },
                ..
            } => {
                panic!("impossible situation");
            }
        }
        self.value = Value::D { deleted: seqno };
    }

    #[allow(dead_code)] // TODO: remove this once bogn is weaved-up.
    pub(crate) fn purge(&mut self, before: u64) -> bool {
        for i in 0..self.deltas.len() {
            if self.deltas[i].to_seqno() < before {
                self.deltas.truncate(i); // purge everything from i..len
                break;
            }
        }
        if self.to_seqno() < before {
            true
        } else {
            false
        }
    }
}

// read methods.
impl<K, V> Entry<K, V>
where
    K: Ord + Clone,
    V: Clone + Diff,
{
    #[inline]
    #[allow(dead_code)] // TODO: remove this once bogn is weaved-up.
    pub(crate) fn as_deltas(&self) -> &[Delta<V>] {
        &self.deltas
    }

    /// Return ownership of key.
    #[inline]
    pub fn to_key(&self) -> K {
        self.key.clone()
    }

    /// Return a reference to key.
    #[inline]
    pub fn as_key(&self) -> &K {
        &self.key
    }

    /// Return value.
    pub fn to_native_value(&self) -> Option<V> {
        self.value.to_native_value()
    }

    /// Return the latest seqno that created/updated/deleted this entry.
    #[inline]
    pub fn to_seqno(&self) -> u64 {
        match &self.value {
            Value::U { seqno, .. } => *seqno,
            Value::D { deleted, .. } => *deleted,
        }
    }

    /// Return the seqno and the state of modification. `true` means
    /// latest value was a create/update, and `false` means latest value
    /// was deleted.
    #[inline]
    pub fn to_seqno_state(&self) -> (bool, u64) {
        match &self.value {
            Value::U { seqno, .. } => (true, *seqno),
            Value::D { deleted, .. } => (false, *deleted),
        }
    }

    /// Return whether this entry is in deleted state, applicable onle
    /// in lsm mode.
    pub fn is_deleted(&self) -> bool {
        self.value.is_deleted()
    }

    /// Return the previous versions of this entry as Deltas.
    #[inline]
    pub fn to_deltas(&self) -> Vec<Delta<V>> {
        self.deltas.clone()
    }

    /// Return an iterator of previous versions.
    pub fn versions(&self) -> VersionIter<K, V> {
        let entry = Some(self.clone());
        let curval = self.to_native_value();
        VersionIter {
            key: self.key.clone(),
            entry,
            curval,
            deltas: self.to_deltas().into_iter(),
        }
    }
}

pub struct VersionIter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    key: K,
    entry: Option<Entry<K, V>>,
    curval: Option<V>,
    deltas: std::vec::IntoIter<Delta<V>>,
}

impl<K, V> Iterator for VersionIter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff + From<<V as Diff>::D>,
{
    type Item = Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.entry.take() {
            Some(entry) => {
                self.curval = entry.to_native_value();
                Some(entry)
            }
            None => match (self.deltas.next(), self.curval.take()) {
                (None, _) => None,
                (Some(Delta::D { deleted }), _) => {
                    // this entry is deleted.
                    let key = self.key.clone();
                    Some(Entry::new(key, Value::new_delete(deleted)))
                }
                (Some(Delta::U { delta, seqno }), None) => {
                    // previous entry was a delete.
                    let nv: V = From::from(delta.into_native().unwrap());
                    let key = self.key.clone();
                    self.curval = Some(nv.clone());
                    Some(Entry::new(
                        key,
                        Value::new_upsert(vlog::Value::new_native(nv), seqno),
                    ))
                }
                (Some(Delta::U { delta, seqno }), Some(curval)) => {
                    // this and previous entry are create/update.
                    let nv = curval.merge(&delta.into_native().unwrap());
                    self.curval = Some(nv.clone());
                    let key = self.key.clone();
                    Some(Entry::new(
                        key,
                        Value::new_upsert(vlog::Value::new_native(nv), seqno),
                    ))
                }
            },
        }
    }
}
