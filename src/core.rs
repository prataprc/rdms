use std::borrow::Borrow;
use std::ops::{Bound, RangeBounds};

use crate::error::Error;
use crate::vlog;

/// Result returned by bogn functions and methods.
pub type Result<T> = std::result::Result<T, Error>;

/// Index entry iterator.
pub type IndexIter<K, V> = Box<dyn Iterator<Item = Entry<K, V>> + Send>;

/// Index operations.
pub trait Index<K, V>: Reader<K, V> + Writer<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    // Make a new empty index of this type, with same configuration.
    fn make_new(&self) -> Self;
}

/// Index read operation.
pub trait Reader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Get ``key`` from index.
    fn get<Q>(&self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized;

    /// Iterate over all entries in this index.
    fn iter(&self) -> Result<IndexIter<K, V>>;

    /// Return an iterator over entries that meet following properties
    /// * Only entries greater than range.start_bound().
    /// * Only entries whose modified seqno is within seqno-range.
    fn iter_within<R, Q>(&self, range: R, before: u64) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q>,
        Q: Ord + ?Sized;

    /// Iterate from lower bound to upper bound.
    fn range<R, Q>(&self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q>,
        Q: Ord + ?Sized;

    /// Iterate from upper bound to lower bound.
    fn reverse<R, Q>(&self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q>,
        Q: Ord + ?Sized;
}

/// Index write operations.
pub trait Writer<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Set {key, value} in index. Return older entry if present.
    fn set_index(
        &mut self,
        key: K,
        value: V,
        index: u64, // seqno for this mutation
    ) -> Result<Option<Entry<K, V>>>;

    /// Set {key, value} in index if an older entry exists with the
    /// same ``cas`` value. To create a fresh entry, pass ``cas`` as ZERO.
    /// Return the seqno (index) for this mutation and older entry
    /// if present. If operation was invalid or NOOP, returned seqno shall
    /// be ZERO.
    fn set_cas_index(
        &mut self,
        key: K,
        value: V,
        cas: u64,
        index: u64,
    ) -> (u64, Result<Option<Entry<K, V>>>);

    /// Delete key from DB. Return the seqno (index) for this mutation
    /// and entry if present. If operation was invalid or NOOP, returned
    /// seqno shall be ZERO.
    fn delete_index<Q>(
        &mut self,
        key: &Q,
        index: u64, // seqno for this mutation
    ) -> (u64, Result<Option<Entry<K, V>>>);
}

/// Replay WAL (Write-Ahead-Log) entries on index.
pub trait Replay<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn set(
        &mut self,
        key: K,
        value: V,
        index: u64, // replay seqno
    ) -> Result<Entry<K, V>>;

    fn set_cas(
        &mut self,
        key: K,
        value: V,
        cas: u64,
        index: u64, // replay seqno
    ) -> Result<Entry<K, V>>;

    fn delete<Q>(&mut self, key: &Q, index: u64) -> Result<Entry<K, V>>;
}

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
    /// Convert this value into binary equivalent. Encoded bytes shall
    /// appended to the input-buffer `buf`. Return bytes encoded.
    fn encode(&self, buf: &mut Vec<u8>) -> usize;

    /// Reverse process of encode, given the binary equivalent, `buf`,
    /// of a value, construct self.
    fn decode(&mut self, buf: &[u8]) -> Result<usize>;
}

/// Delta maintains the older version of value, with necessary fields for
/// log-structured-merge.
#[derive(Clone)]
pub(crate) enum InnerDelta<V>
where
    V: Clone + Diff,
{
    U { delta: vlog::Delta<V>, seqno: u64 },
    D { seqno: u64 },
}

#[derive(Clone)]
pub(crate) struct Delta<V>
where
    V: Clone + Diff,
{
    data: InnerDelta<V>,
}

// Delta construction methods.
impl<V> Delta<V>
where
    V: Clone + Diff,
{
    pub(crate) fn new_upsert(delta: vlog::Delta<V>, seqno: u64) -> Delta<V> {
        Delta {
            data: InnerDelta::U { delta, seqno },
        }
    }

    pub(crate) fn new_delete(seqno: u64) -> Delta<V> {
        Delta {
            data: InnerDelta::D { seqno },
        }
    }

    #[allow(dead_code)] // TODO: remove this once bogn is weaved-up.
    pub(crate) fn into_upserted(self) -> Option<(vlog::Delta<V>, u64)> {
        match self.data {
            InnerDelta::U { delta, seqno } => Some((delta, seqno)),
            InnerDelta::D { .. } => None,
        }
    }

    #[allow(dead_code)] // TODO: remove this once bogn is weaved-up.
    pub(crate) fn into_deleted(self) -> Option<u64> {
        match self.data {
            InnerDelta::D { seqno } => Some(seqno),
            InnerDelta::U { .. } => None,
        }
    }
}

impl<V> AsRef<InnerDelta<V>> for Delta<V>
where
    V: Clone + Diff,
{
    fn as_ref(&self) -> &InnerDelta<V> {
        &self.data
    }
}

/// Read methods.
impl<V> Delta<V>
where
    V: Clone + Diff,
{
    /// Return the underlying `difference` value for this delta.
    #[allow(dead_code)] // TODO: remove if not required.
    pub(crate) fn into_diff(self) -> Option<<V as Diff>::D> {
        match self.data {
            InnerDelta::D { .. } => None,
            InnerDelta::U { delta, .. } => delta.into_native_delta(),
        }
    }

    /// Return the seqno at which this delta was modified,
    /// which includes Create and Delete operations.
    /// To differentiate between Create and Delete operations
    /// use born_seqno() and dead_seqno() methods respectively.
    #[allow(dead_code)] // TODO: remove if not required.
    pub(crate) fn to_seqno(&self) -> u64 {
        match &self.data {
            InnerDelta::U { seqno, .. } => *seqno,
            InnerDelta::D { seqno } => *seqno,
        }
    }

    /// Return the seqno and the state of modification. `true` means
    /// this version was a create/update, and `false` means
    /// this version was deleted.
    #[allow(dead_code)] // TODO: remove if not required.
    pub(crate) fn to_seqno_state(&self) -> (bool, u64) {
        match &self.data {
            InnerDelta::U { seqno, .. } => (true, *seqno),
            InnerDelta::D { seqno } => (false, *seqno),
        }
    }
}

#[derive(Clone)]
pub(crate) enum Value<V>
where
    V: Clone + Diff,
{
    U { value: vlog::Value<V>, seqno: u64 },
    D { seqno: u64 },
}

impl<V> Value<V>
where
    V: Clone + Diff,
{
    pub(crate) fn new_upsert(value: vlog::Value<V>, seqno: u64) -> Value<V> {
        Value::U { value, seqno }
    }

    pub(crate) fn new_upsert_value(value: V, seqno: u64) -> Value<V> {
        let value = vlog::Value::new_native(value);
        Value::U { value, seqno }
    }

    pub(crate) fn new_delete(seqno: u64) -> Value<V> {
        Value::D { seqno }
    }

    pub(crate) fn to_native_value(&self) -> Option<V> {
        match &self {
            Value::D { .. } => None,
            Value::U { value, .. } => value.to_native_value(),
        }
    }

    pub(crate) fn is_deleted(&self) -> bool {
        match self {
            Value::U { .. } => false,
            Value::D { .. } => true,
        }
    }
}

/// Entry is the covering structure for a {Key, value} pair
/// indexed by bogn.
///
/// It is a user facing structure, also used in stitching together
/// different components of Bogn.
#[derive(Clone)]
pub struct Entry<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    key: K,
    value: Box<Value<V>>,
    deltas: Vec<Delta<V>>,
}

// NOTE: user-facing entry values must be constructed with
// native value and native deltas.
// Entry construction methods.
impl<K, V> Entry<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    pub const KEY_SIZE_LIMIT: usize = 1024 * 1024 * 1024; // 1GB
    pub const DIFF_SIZE_LIMIT: usize = 1024 * 1024 * 1024 * 1024; // 1TB
    pub const VALUE_SIZE_LIMIT: usize = 1024 * 1024 * 1024 * 1024; // 1TB

    pub(crate) fn new(key: K, value: Box<Value<V>>) -> Entry<K, V> {
        Entry {
            key,
            value,
            deltas: vec![],
        }
    }

    pub(crate) fn set_deltas(&mut self, deltas: Vec<Delta<V>>) {
        self.deltas = deltas;
    }
}

// write/update methods.
impl<K, V> Entry<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    // Prepend a new version, also the latest version, for this entry.
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
        match self.value.as_ref() {
            Value::D { seqno } => {
                self.deltas.insert(0, Delta::new_delete(*seqno));
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
        match self.value.as_ref() {
            Value::D { .. } => (), // NOOP
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
        *self.value = Value::D { seqno };
    }

    // purge all versions whose seqno is ``before``.
    pub(crate) fn purge_todo(&mut self, before: Bound<u64>) -> bool {
        for i in 0..self.deltas.len() {
            let seqno = self.deltas[i].to_seqno();
            let ok = match before {
                Bound::Included(before) if seqno <= before => true,
                Bound::Excluded(before) if seqno < before => true,
                _ => false,
            };
            if ok {
                self.deltas.truncate(i); // purge everything from i..len
                break;
            }
        }
        let seqno = self.to_seqno();
        match before {
            Bound::Included(before) if seqno <= before => true,
            Bound::Excluded(before) if seqno < before => true,
            _ => false,
        }
    }
}

impl<K, V> Entry<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff + From<<V as Diff>::D>,
{
    // pick all versions whose seqno is within the range of [from, to], with
    // inclusive bounds.
    pub(crate) fn pick_within<R>(&self, range: R) -> Option<Entry<K, V>>
    where
        R: RangeBounds<u64>,
    {
        let e = self.to_seqno();
        let s = self.deltas.last().map_or(e, |d| d.to_seqno());
        let ok1 = match range.end_bound() {
            Bound::Included(re) if s > *re => false,
            Bound::Excluded(re) if s >= *re => false,
            _ => true,
        };
        let ok2 = match range.start_bound() {
            Bound::Included(rs) if e < *rs => false,
            Bound::Excluded(rs) if e <= *rs => false,
            _ => true,
        };
        if ok1 || ok2 {
            let entry = self.clone();
            let deltas = entry.deltas;
            let entry = Entry::new(entry.key, entry.value);
            let mut entry = entry.skip_till(range.end_bound(), deltas).unwrap();
            entry.purge_todo(match range.start_bound() {
                Bound::Included(x) => Bound::Included(*x),
                Bound::Excluded(x) => Bound::Excluded(*x),
                Bound::Unbounded => Bound::Unbounded,
            });
            Some(entry)
        } else {
            None
        }
    }

    fn skip_till(
        mut self,
        end: Bound<&u64>, // skip till end
        deltas: Vec<Delta<V>>,
    ) -> Option<Entry<K, V>> {
        let mut iter = deltas.into_iter();
        while let Some(delta) = iter.next() {
            let seqno = self.to_seqno();
            let ok = match end {
                Bound::Included(re) if seqno <= *re => true,
                Bound::Excluded(re) if seqno < *re => true,
                _ => false,
            };
            if ok {
                self.deltas = iter.collect();
                return Some(self);
            }
            match (delta.data, self.to_native_value()) {
                (InnerDelta::D { seqno }, Some(_)) => {
                    self.value = Box::new(Value::new_delete(seqno));
                }
                (InnerDelta::U { delta, seqno }, None) => {
                    let nv: V = From::from(delta.into_native_delta().unwrap());
                    let value = vlog::Value::new_native(nv);
                    self.value = Box::new(Value::new_upsert(value, seqno));
                }
                (InnerDelta::U { delta, seqno }, Some(nv)) => {
                    let nv = nv.merge(&delta.into_native_delta().unwrap());
                    let value = vlog::Value::new_native(nv);
                    self.value = Box::new(Value::new_upsert(value, seqno))
                }
                (InnerDelta::D { .. }, None) => {
                    panic!("consecutive versions can't be a delete");
                }
            }
        }
        None
    }
}

// read methods.
impl<K, V> Entry<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    #[inline]
    pub(crate) fn as_deltas(&self) -> &Vec<Delta<V>> {
        &self.deltas
    }

    pub(crate) fn to_delta_count(&self) -> usize {
        self.deltas.len()
    }

    pub(crate) fn as_value(&self) -> &Value<V> {
        &self.value
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
        match self.value.as_ref() {
            Value::U { seqno, .. } => *seqno,
            Value::D { seqno, .. } => *seqno,
        }
    }

    /// Return the seqno and the state of modification. `true` means
    /// latest value was a create/update, and `false` means latest value
    /// was deleted.
    #[inline]
    pub fn to_seqno_state(&self) -> (bool, u64) {
        match &self.value.as_ref() {
            Value::U { seqno, .. } => (true, *seqno),
            Value::D { seqno, .. } => (false, *seqno),
        }
    }

    /// Return whether this entry is in deleted state, applicable onle
    /// in lsm mode.
    pub fn is_deleted(&self) -> bool {
        self.value.is_deleted()
    }

    /// Return the previous versions of this entry as Deltas.
    #[inline]
    pub(crate) fn to_deltas(&self) -> Vec<Delta<V>> {
        self.deltas.clone()
    }

    /// Return an iterator of previous versions.
    pub fn versions(&self) -> VersionIter<K, V> {
        VersionIter {
            key: self.key.clone(),
            entry: Some(Entry {
                key: self.key.clone(),
                value: self.value.clone(),
                deltas: Default::default(),
            }),
            curval: None,
            deltas: self.to_deltas().into_iter(),
        }
    }
}

/// Iterate from latest to oldest available version for this entry.
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
        if let Some(entry) = self.entry.take() {
            self.curval = entry.to_native_value();
            return Some(entry);
        }
        match (self.deltas.next().map(|x| x.data), self.curval.take()) {
            (None, _) => None,
            (Some(InnerDelta::D { .. }), None) => {
                panic!("consecutive versions can't be a delete");
            }
            (Some(InnerDelta::D { seqno }), _) => {
                // this entry is deleted.
                let key = self.key.clone();
                Some(Entry::new(key, Box::new(Value::new_delete(seqno))))
            }
            (Some(InnerDelta::U { delta, seqno }), None) => {
                // previous entry was a delete.
                let nv: V = From::from(delta.into_native_delta().unwrap());
                let key = self.key.clone();
                self.curval = Some(nv.clone());
                let v = Value::new_upsert(vlog::Value::new_native(nv), seqno);
                Some(Entry::new(key, Box::new(v)))
            }
            (Some(InnerDelta::U { delta, seqno }), Some(curval)) => {
                // this and previous entry are create/update.
                let nv = curval.merge(&delta.into_native_delta().unwrap());
                self.curval = Some(nv.clone());
                let key = self.key.clone();
                let v = Value::new_upsert(vlog::Value::new_native(nv), seqno);
                Some(Entry::new(key, Box::new(v)))
            }
        }
    }
}
