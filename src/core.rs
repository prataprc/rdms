use std::borrow::Borrow;
use std::convert::TryInto;
use std::ops::{Bound, RangeBounds};
use std::{
    fs,
    mem::{self, ManuallyDrop},
    sync::atomic::AtomicBool,
    sync::atomic::Ordering::SeqCst,
};

use crate::{error::Error, vlog};

/// Result returned by rdms functions and methods.
pub type Result<T> = std::result::Result<T, Error>;

/// Type alias to trait-objects iterating over an index.
pub type IndexIter<'a, K, V> = Box<dyn Iterator<Item = Result<Entry<K, V>>> + 'a>;

/// Type alias to trait-objects iterating, piece-wise, over [`Index`].
pub(crate) type ScanIter<'a, K, V> = Box<dyn Iterator<Item = Result<ScanEntry<K, V>>> + 'a>;

/// Trait for diffable values.
///
/// All values indexed in [Rdms] must support this trait, since [Rdms]
/// can manage successive modifications to the same entry.
///
/// If,
/// ```notest
/// P = old value; C = new value; D = difference between P and C
/// ```
///
/// Then,
/// ```notest
/// D = C - P (diff operation)
/// P = C - D (merge operation, to get old value)
/// ```
///
/// [Rdms]: crate::Rdms
///
pub trait Diff: Sized {
    type D: Clone + From<Self> + Into<Self> + Footprint;

    /// Return the delta between two consecutive versions of a value.
    /// ``Delta = New - Old``.
    fn diff(&self, old: &Self) -> Self::D;

    /// Merge delta with newer version to return older version of the value.
    /// ``Old = New - Delta``.
    fn merge(&self, delta: &Self::D) -> Self;
}

/// To be implemented by index-types, key-types and value-types.
///
/// This trait is required to compute the memory or disk foot-print
/// for index-types, key-types and value-types.
///
/// **Note: This can be an approximate measure.**
///
pub trait Footprint {
    fn footprint(&self) -> Result<isize>;
}

/// Index write operations.
pub trait WalWriter<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    /// Set {key, value} in index. Return older entry if present.
    /// Return the seqno (index) for this mutation and older entry
    /// if present. If operation was invalid or NOOP, returned seqno
    /// shall be ZERO.
    ///
    /// *LSM mode*: Add a new version for the key, perserving the old value.
    fn set_index(
        &mut self,
        key: K,
        value: V,
        index: u64, // seqno for this mutation
    ) -> (Option<u64>, Result<Option<Entry<K, V>>>);

    /// Set {key, value} in index if an older entry exists with the
    /// same ``cas`` value. To create a fresh entry, pass ``cas`` as ZERO.
    /// Return the seqno (index) for this mutation and older entry
    /// if present. If operation was invalid or NOOP, returned seqno shall
    /// be ZERO.
    ///
    /// *LSM mode*: Add a new version for the key, perserving the old value.
    fn set_cas_index(
        &mut self,
        key: K,
        value: V,
        cas: u64,
        index: u64,
    ) -> (Option<u64>, Result<Option<Entry<K, V>>>);

    /// Delete key from index. Return the seqno (index) for this mutation
    /// and entry if present. If operation was invalid or NOOP, returned
    /// seqno shall be ZERO.
    ///
    /// *LSM mode*: Mark the entry as deleted along with seqno at which it
    /// deleted
    ///
    /// NOTE: K should be borrowable as &Q and Q must be convertable to
    /// owned K. This is require in lsm mode, where owned K must be
    /// inserted into the tree.
    fn delete_index<Q>(
        &mut self,
        key: &Q,
        index: u64, // seqno for this mutation
    ) -> (Option<u64>, Result<Option<Entry<K, V>>>)
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized;
}

/// Replay WAL (Write-Ahead-Log) entries on index.
pub trait Replay<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Replay set operation on index.
    fn set_index(
        &mut self,
        key: K,
        value: V,
        index: u64, // replay seqno
    ) -> Result<Entry<K, V>>;

    /// Replay set-cas operation on index.
    fn set_cas_index(
        &mut self,
        key: K,
        value: V,
        cas: u64,
        index: u64, // replay seqno
    ) -> Result<Entry<K, V>>;

    /// Replay delete operation on index.
    fn delete_index(&mut self, key: K, index: u64) -> Result<Entry<K, V>>;
}

/// Factory trait to create new index snapshot with pre-defined configuration.
pub trait IndexFactory<K, V> {
    type I;

    /// new index instance with predefined configuration.
    fn new<S: AsRef<str>>(&self, name: S) -> Self::I;
}

/// EphemeralIndex trait implemented by in-memory index.
///
/// To ingest key, value pairs, support read, but does not persist
/// data on disk.
pub trait EphemeralIndex<K, V>: Sized + Footprint
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    /// A writer associated type, that can ingest key-value pairs.
    type W: Writer<K, V>;

    /// A reader assciated type, that are thread safe.
    type R: Reader<K, V>;

    /// Application can set the start sequence number for this index.
    fn set_seqno(&mut self, seqno: u64);

    /// Create a new read handle, for multi-threading. Note that not all
    /// indexes allow concurrent readers. Refer to index API for more details.
    fn to_reader(&mut self) -> Result<Self::R>;

    /// Create a new write handle, for multi-threading. Note that not all
    /// indexes allow concurrent writers. Refer to index API for more details.
    fn to_writer(&mut self) -> Result<Self::W>;
}

/// DurableIndex trait implemented by disk index.
///
/// To commit data onto disk, support read operations and other lsm-methods.
pub trait DurableIndex<K, V>: Sized
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// A reader assciated type, that are thread safe.
    type R: Reader<K, V>;

    /// Flush to disk all new entries that are not yet persisted
    /// on to disk. Return number of entries commited to disk.
    fn commit(&mut self, iter: IndexIter<K, V>) -> Result<usize>;

    /// Compact disk snapshots if there are any.
    fn compact(&mut self) -> Result<()>;

    /// Create a new read handle, for multi-threading. Note that not all
    /// indexes allow concurrent readers. Refer to index API for more details.
    fn to_reader(&mut self) -> Result<Self::R>;
}

/// Index read operations.
pub trait Reader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Get ``key`` from index. Returned entry may not have all its
    /// previous versions, if it is costly to fetch from disk.
    fn get<Q>(&self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized;

    /// Iterate over all entries in this index. Returned entry may not
    /// have all its previous versions, if it is costly to fetch from disk.
    fn iter(&self) -> Result<IndexIter<K, V>>;

    /// Iterate from lower bound to upper bound. Returned entry may not
    /// have all its previous versions, if it is costly to fetch from disk.
    fn range<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized;

    /// Iterate from upper bound to lower bound. Returned entry may not
    /// have all its previous versions, if it is costly to fetch from disk.
    fn reverse<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized;

    /// Get ``key`` from index. Returned entry shall have all its
    /// previous versions, can be a costly call.
    fn get_with_versions<Q>(&self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized;

    /// Iterate over all entries in this index. Returned entry shall
    /// have all its previous versions, can be a costly call.
    fn iter_with_versions(&self) -> Result<IndexIter<K, V>>;

    /// Iterate from lower bound to upper bound. Returned entry shall
    /// have all its previous versions, can be a costly call.
    fn range_with_versions<'a, R, Q>(&'a self, r: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized;

    /// Iterate from upper bound to lower bound. Returned entry shall
    /// have all its previous versions, can be a costly call.
    fn reverse_with_versions<'a, R, Q>(&'a self, r: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized;
}

/// Index write operations.
pub trait Writer<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    /// Set {key, value} in index. Return older entry if present.
    /// Return the older entry if present. If operation was invalid or
    /// NOOP, returned seqno shall be ZERO.
    ///
    /// *LSM mode*: Add a new version for the key, perserving the old value.
    fn set(&mut self, k: K, v: V) -> Result<Option<Entry<K, V>>>;

    /// Set {key, value} in index if an older entry exists with the
    /// same ``cas`` value. To create a fresh entry, pass ``cas`` as ZERO.
    /// Return the older entry if present. If operation was invalid or
    /// NOOP, returned seqno shall be ZERO.
    ///
    /// *LSM mode*: Add a new version for the key, perserving the old value.
    fn set_cas(&mut self, k: K, v: V, cas: u64) -> Result<Option<Entry<K, V>>>;

    /// Delete key from index. Return the mutation and entry if present.
    /// If operation was invalid or NOOP, returned seqno shall be ZERO.
    ///
    /// *LSM mode*: Mark the entry as deleted along with seqno at which it
    /// deleted
    ///
    /// NOTE: K should be borrowable as &Q and Q must be convertable to
    /// owned K. This is require in lsm mode, where owned K must be
    /// inserted into the tree.
    fn delete<Q>(&mut self, key: &Q) -> Result<Option<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized;
}

/// Serialize values to binary sequence of bytes.
pub trait Serialize: Sized {
    /// Convert this value into binary equivalent. Encoded bytes shall
    /// be appended to the input-buffer `buf`. Return bytes encoded.
    fn encode(&self, buf: &mut Vec<u8>) -> usize;

    /// Reverse process of encode, given the binary equivalent `buf`,
    /// construct ``self``.
    fn decode(&mut self, buf: &[u8]) -> Result<usize>;
}

/// Index full table scan.
pub(crate) trait FullScan<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff + From<<V as Diff>::D>,
{
    /// Return an iterator over entries that meet following properties
    /// * Only entries greater than range.start_bound().
    /// * Only entries whose modified seqno is within seqno-range.
    ///
    /// This method is typically valid only for memory-only indexes. Also,
    /// returned entry may not have all its previous versions, if it is
    /// costly to fetch from disk.
    fn full_scan<G>(&self, from: Bound<K>, within: G) -> Result<ScanIter<K, V>>
    where
        G: Clone + RangeBounds<u64>;
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
}

impl<V> Footprint for Delta<V>
where
    V: Clone + Diff,
{
    fn footprint(&self) -> Result<isize> {
        let mut footprint: isize = mem::size_of::<Delta<V>>().try_into().unwrap();
        footprint += match &self.data {
            InnerDelta::U { delta, .. } => delta.diff_footprint()?,
            InnerDelta::D { .. } => 0,
        };
        Ok(footprint)
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
    pub(crate) fn to_diff(&self) -> Option<<V as Diff>::D> {
        match &self.data {
            InnerDelta::D { .. } => None,
            InnerDelta::U { delta, .. } => delta.to_native_delta(),
        }
    }

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

    #[allow(dead_code)] // TODO: remove this once rdms is weaved-up.
    pub(crate) fn into_upserted(self) -> Option<(vlog::Delta<V>, u64)> {
        match self.data {
            InnerDelta::U { delta, seqno } => Some((delta, seqno)),
            InnerDelta::D { .. } => None,
        }
    }

    #[allow(dead_code)] // TODO: remove this once rdms is weaved-up.
    pub(crate) fn into_deleted(self) -> Option<u64> {
        match self.data {
            InnerDelta::D { seqno } => Some(seqno),
            InnerDelta::U { .. } => None,
        }
    }

    pub(crate) fn is_reference(&self) -> bool {
        match self.data {
            InnerDelta::U {
                delta: vlog::Delta::Reference { .. },
                ..
            } => true,
            _ => false,
        }
    }

    #[cfg(test)]
    pub(crate) fn is_deleted(&self) -> bool {
        match self.data {
            InnerDelta::D { .. } => true,
            InnerDelta::U { .. } => false,
        }
    }
}

pub(crate) enum Value<V>
where
    V: Clone + Diff,
{
    U {
        value: ManuallyDrop<Box<vlog::Value<V>>>,
        is_reclaim: AtomicBool,
        seqno: u64,
    },
    D {
        seqno: u64,
    },
}

impl<V> Clone for Value<V>
where
    V: Clone + Diff,
{
    fn clone(&self) -> Value<V> {
        match self {
            Value::U {
                value,
                is_reclaim,
                seqno,
            } => Value::U {
                value: value.clone(),
                is_reclaim: AtomicBool::new(is_reclaim.load(SeqCst)),
                seqno: *seqno,
            },
            Value::D { seqno } => Value::D { seqno: *seqno },
        }
    }
}

impl<V> Drop for Value<V>
where
    V: Clone + Diff,
{
    fn drop(&mut self) {
        // if is_reclaim is false, then it is a mvcc-clone. so don't touch
        // the value.
        match self {
            Value::U {
                value, is_reclaim, ..
            } => {
                if is_reclaim.load(SeqCst) {
                    unsafe { ManuallyDrop::drop(value) };
                }
            }
            _ => (),
        }
    }
}

impl<V> Value<V>
where
    V: Clone + Diff,
{
    pub(crate) fn new_upsert(v: Box<vlog::Value<V>>, seqno: u64) -> Value<V> {
        Value::U {
            value: ManuallyDrop::new(v),
            is_reclaim: AtomicBool::new(true),
            seqno,
        }
    }

    pub(crate) fn new_upsert_value(value: V, seqno: u64) -> Value<V> {
        let value = Box::new(vlog::Value::new_native(value));
        Value::U {
            value: ManuallyDrop::new(value),
            is_reclaim: AtomicBool::new(true),
            seqno,
        }
    }

    pub(crate) fn new_delete(seqno: u64) -> Value<V> {
        Value::D { seqno }
    }

    pub(crate) fn mvcc_clone(&self, copyval: bool) -> Value<V> {
        match self {
            Value::U {
                value,
                seqno,
                is_reclaim,
            } if !copyval => {
                is_reclaim.store(false, SeqCst);
                let v = value.as_ref() as *const vlog::Value<V>;
                let value = unsafe { Box::from_raw(v as *mut vlog::Value<V>) };
                Value::U {
                    value: ManuallyDrop::new(value),
                    is_reclaim: AtomicBool::new(true),
                    seqno: *seqno,
                }
            }
            val => val.clone(),
        }
    }

    pub(crate) fn to_native_value(&self) -> Option<V> {
        match &self {
            Value::U { value, .. } => value.to_native_value(),
            Value::D { .. } => None,
        }
    }

    pub(crate) fn to_seqno(&self) -> u64 {
        match self {
            Value::U { seqno, .. } => *seqno,
            Value::D { seqno } => *seqno,
        }
    }

    pub(crate) fn is_deleted(&self) -> bool {
        match self {
            Value::U { .. } => false,
            Value::D { .. } => true,
        }
    }

    pub(crate) fn is_reference(&self) -> bool {
        match self {
            Value::U { value, .. } => value.is_reference(),
            _ => false,
        }
    }
}

impl<V> Footprint for Value<V>
where
    V: Clone + Diff + Footprint,
{
    fn footprint(&self) -> Result<isize> {
        let mut fp: isize = mem::size_of::<Value<V>>().try_into().unwrap();
        fp += match self {
            Value::U { value, .. } => value.value_footprint()?,
            Value::D { .. } => 0,
        };
        Ok(fp)
    }
}

/// Entry is the covering structure for a {Key, value} pair
/// indexed by rdms data structures.
///
/// It is a user facing structure, also used in stitching together
/// different components of `Rdms`.
#[derive(Clone)]
pub struct Entry<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    key: K,
    value: Value<V>,
    deltas: Vec<Delta<V>>,
}

// Entry construction methods.
impl<K, V> Entry<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    pub const KEY_SIZE_LIMIT: usize = 1024 * 1024 * 1024; // 1GB
    pub const DIFF_SIZE_LIMIT: usize = 1024 * 1024 * 1024 * 1024; // 1TB
    pub const VALUE_SIZE_LIMIT: usize = 1024 * 1024 * 1024 * 1024; // 1TB

    pub(crate) fn new(key: K, value: Value<V>) -> Entry<K, V> {
        Entry {
            key,
            value,
            deltas: vec![],
        }
    }

    pub(crate) fn mvcc_clone(&self, copyval: bool) -> Entry<K, V> {
        Entry {
            key: self.key.clone(),
            value: self.value.mvcc_clone(copyval),
            deltas: self.deltas.clone(),
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
    V: Clone + Diff + Footprint,
{
    // Corresponds to CREATE and UPDATE operations also the latest version,
    // for this entry. In non-lsm mode this is equivalent to over-writing
    // previous value.
    //
    // `nentry` is new_entry to be CREATE/UPDATE into index.
    //
    // TODO: may be we can just pass the Value, instead of `nentry` ?
    pub(crate) fn prepend_version(&mut self, nentry: Self, lsm: bool) -> Result<isize> {
        if lsm {
            self.prepend_version_lsm(nentry)
        } else {
            self.prepend_version_nolsm(nentry)
        }
    }

    // `nentry` is new_entry to be CREATE/UPDATE into index.
    fn prepend_version_nolsm(&mut self, nentry: Self) -> Result<isize> {
        let size = self.value.footprint()?;
        self.value = nentry.value.clone();
        Ok((self.value.footprint()? - size).try_into().unwrap())
    }

    // `nentry` is new_entry to be CREATE/UPDATE into index.
    fn prepend_version_lsm(&mut self, nentry: Self) -> Result<isize> {
        let delta = match &self.value {
            Value::D { seqno } => Delta::new_delete(*seqno),
            Value::U { value, seqno, .. } if !value.is_reference() => {
                // compute delta
                match &nentry.value {
                    Value::D { .. } => {
                        let value = value.to_native_value().unwrap();
                        let diff: <V as Diff>::D = From::from(value);
                        let dlt = vlog::Delta::new_native(diff);
                        Delta::new_upsert(dlt, *seqno)
                    }
                    Value::U { value: nvalue, .. } => {
                        let value = value.to_native_value().unwrap();
                        let dff = nvalue.to_native_value().unwrap().diff(&value);
                        let dlt = vlog::Delta::new_native(dff);
                        Delta::new_upsert(dlt, *seqno)
                    }
                }
            }
            Value::U { .. } => unreachable!(),
        };

        let size = {
            let size = nentry.value.footprint()? + delta.footprint()?;
            size - self.value.footprint()?
        };

        self.deltas.insert(0, delta);
        self.prepend_version_nolsm(nentry);
        Ok(size.try_into().unwrap())
    }

    // DELETE operation, only in lsm-mode.
    pub(crate) fn delete(&mut self, seqno: u64) -> Result<isize> {
        let delta_size = match &self.value {
            Value::D { seqno } => {
                self.deltas.insert(0, Delta::new_delete(*seqno));
                0
            }
            Value::U { value, seqno, .. } if !value.is_reference() => {
                let delta = {
                    let value = value.to_native_value().unwrap();
                    let d: <V as Diff>::D = From::from(value);
                    vlog::Delta::new_native(d)
                };
                let size = delta.diff_footprint()?;
                self.deltas.insert(0, Delta::new_upsert(delta, *seqno));
                size
            }
            Value::U { .. } => unreachable!(),
        };
        let size = self.value.footprint()?;
        self.value = Value::new_delete(seqno);
        Ok((size + delta_size - self.value.footprint()?)
            .try_into()
            .unwrap())
    }
}

impl<K, V> Entry<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    // purge all versions whose seqno <= or < ``cutoff``.
    pub(crate) fn purge(mut self, cutoff: Bound<u64>) -> Option<Entry<K, V>> {
        let n = self.to_seqno();
        // If all versions of this entry are before cutoff, then purge entry
        match cutoff {
            Bound::Included(cutoff) if n <= cutoff => return None,
            Bound::Excluded(cutoff) if n < cutoff => return None,
            Bound::Unbounded => return None,
            _ => (),
        }
        // Otherwise, purge only those versions that are before cutoff
        self.deltas = self
            .deltas
            .drain(..)
            .take_while(|d| {
                let seqno = d.to_seqno();
                match cutoff {
                    Bound::Included(cutoff) if seqno > cutoff => true,
                    Bound::Excluded(cutoff) if seqno >= cutoff => true,
                    _ => false,
                }
            })
            .collect();
        Some(self)
    }
}

impl<K, V> Entry<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff + From<<V as Diff>::D>,
{
    // Pick all versions whose seqno is within the specified range.
    // Note that, by rdms-design only memory-indexes ingesting new
    // mutations are subjected to this filter function.
    pub(crate) fn filter_within(
        &self,
        start: Bound<u64>, // filter from
        end: Bound<u64>,   // filter till
    ) -> Option<Entry<K, V>> {
        // skip versions newer than requested range.
        let entry = self.skip_till(start.clone(), end)?;
        // purge versions older than request range.
        match start {
            Bound::Included(x) => entry.purge(Bound::Excluded(x)),
            Bound::Excluded(x) => entry.purge(Bound::Included(x)),
            Bound::Unbounded => Some(entry),
        }
    }

    fn skip_till(&self, ob: Bound<u64>, nb: Bound<u64>) -> Option<Entry<K, V>> {
        // skip entire entry if it is before the specified range.
        let n = self.to_seqno();
        match ob {
            Bound::Included(o_seqno) if n < o_seqno => return None,
            Bound::Excluded(o_seqno) if n <= o_seqno => return None,
            _ => (),
        }
        // skip the entire entry if it is after the specified range.
        let o = self.deltas.last().map_or(n, |d| d.to_seqno());
        match nb {
            Bound::Included(nb) if o > nb => return None,
            Bound::Excluded(nb) if o >= nb => return None,
            Bound::Included(nb) if n <= nb => return Some(self.clone()),
            Bound::Excluded(nb) if n < nb => return Some(self.clone()),
            Bound::Unbounded => return Some(self.clone()),
            _ => (),
        };

        // println!("skip_till {} {} {:?}", o, n, nb);
        // partial skip.
        let mut entry = self.clone();
        let mut iter = entry.deltas.drain(..);
        while let Some(delta) = iter.next() {
            let value = entry.value.to_native_value();
            let (value, _) = next_value(value, delta.data);
            entry.value = value;
            let seqno = entry.value.to_seqno();
            let done = match nb {
                Bound::Included(n_seqno) if seqno <= n_seqno => true,
                Bound::Excluded(n_seqno) if seqno < n_seqno => true,
                _ => false,
            };
            // println!("skip_till loop {} {:?} {} ", seqno, nb, done);
            if done {
                // collect the remaining deltas and return
                entry.deltas = iter.collect();
                // println!("skip_till fin {}", entry.deltas.len());
                return Some(entry);
            }
        }
        unreachable!()
    }

    /// Return an iterator for all existing versions for this entry.
    pub fn versions(&self) -> VersionIter<K, V> {
        VersionIter {
            key: self.key.clone(),
            entry: Some(Entry {
                key: self.key.clone(),
                value: self.value.clone(),
                deltas: Default::default(),
            }),
            curval: None,
            deltas: Some(self.to_deltas().into_iter()),
        }
    }
}

impl<K, V> Entry<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff + From<<V as Diff>::D> + Footprint,
{
    // Merge two version chain for same entry. This can happen between
    // two entries from memory-index and disk-index, or disk-index and
    // disk-index. In either case it is expected that all versions of
    // one entry shall either be greater than all versions of the other entry.
    pub(crate) fn flush_merge(self, entry: Entry<K, V>) -> Entry<K, V> {
        // `a` is newer than `b`, and all versions in a and b are mutually
        // exclusive in seqno ordering.
        let (a, mut b) = if self.to_seqno() > entry.to_seqno() {
            (self, entry)
        } else if entry.to_seqno() > self.to_seqno() {
            (entry, self)
        } else {
            unreachable!()
        };

        // TODO remove this validation logic once rdms is fully stable.
        a.validate_flush_merge(&b);
        for ne in a.versions().collect::<Vec<Entry<K, V>>>().into_iter().rev() {
            // println!("flush_merge {} {}", ne.to_seqno(), ne.is_deleted());
            b.prepend_version(ne, true /* lsm */);
        }
        b
    }

    // `self` is newer than `entr`
    fn validate_flush_merge(&self, entr: &Entry<K, V>) {
        // validate ordering
        let mut seqnos = vec![self.to_seqno()];
        self.deltas.iter().for_each(|d| seqnos.push(d.to_seqno()));
        seqnos.push(entr.to_seqno());
        entr.deltas.iter().for_each(|d| seqnos.push(d.to_seqno()));
        let mut fail = seqnos[0..seqnos.len() - 1]
            .into_iter()
            .zip(seqnos[1..].into_iter())
            .any(|(a, b)| a <= b);
        // println!("validate_flush_merge1 {} {:?}", fail, seqnos);
        // validate self contains all native value and deltas.
        fail = fail || self.value.is_reference();
        fail = fail || self.deltas.iter().any(|d| d.is_reference());

        if fail {
            unreachable!()
        }
    }
}

impl<K, V> Entry<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    pub(crate) fn fetch_value(&mut self, fd: &mut fs::File) -> Result<()> {
        match &self.value {
            Value::U { value, seqno, .. } => match value.to_reference() {
                Some((fpos, len, _seqno)) => {
                    let value = Box::new(vlog::fetch_value(fpos, len, fd)?);
                    self.value = Value::new_upsert(value, *seqno);
                    Ok(())
                }
                _ => Ok(()),
            },
            _ => Ok(()),
        }
    }

    pub(crate) fn fetch_deltas(&mut self, fd: &mut fs::File) -> Result<()> {
        for delta in self.deltas.iter_mut() {
            match delta.data {
                InnerDelta::U {
                    delta: vlog::Delta::Reference { fpos, length, .. },
                    seqno,
                } => {
                    let d = vlog::fetch_delta(fpos, length, fd)?;
                    *delta = Delta::new_upsert(d, seqno);
                }
                _ => (),
            }
        }
        Ok(())
    }
}

// read methods.
impl<K, V> Entry<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Return a reference to key.
    #[inline]
    pub fn as_key(&self) -> &K {
        &self.key
    }

    /// Return owned key vlalue.
    #[inline]
    pub fn to_key(&self) -> K {
        self.key.clone()
    }

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

    /// Return the previous versions of this entry as Deltas.
    #[inline]
    pub(crate) fn to_deltas(&self) -> Vec<Delta<V>> {
        self.deltas.clone()
    }

    /// Return value. If entry is marked as deleted, return None.
    pub fn to_native_value(&self) -> Option<V> {
        self.value.to_native_value()
    }

    /// Return the latest seqno that created/updated/deleted this entry.
    #[inline]
    pub fn to_seqno(&self) -> u64 {
        match self.value {
            Value::U { seqno, .. } => seqno,
            Value::D { seqno, .. } => seqno,
        }
    }

    /// Return the seqno and the state of modification. _`true`_ means
    /// latest value was a create/update, and _`false`_ means latest value
    /// was deleted.
    #[inline]
    pub fn to_seqno_state(&self) -> (bool, u64) {
        match self.value {
            Value::U { seqno, .. } => (true, seqno),
            Value::D { seqno, .. } => (false, seqno),
        }
    }

    /// Return whether this entry is in deleted state, applicable onle
    /// in lsm mode.
    pub fn is_deleted(&self) -> bool {
        self.value.is_deleted()
    }
}

impl<K, V> Footprint for Entry<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    /// Return the previous versions of this entry as Deltas.
    fn footprint(&self) -> Result<isize> {
        let mut fp: isize = mem::size_of::<Entry<K, V>>().try_into().unwrap();
        fp += self.value.footprint()?;
        for delta in self.deltas.iter() {
            fp += delta.footprint()?;
        }
        Ok(fp)
    }
}

/// Iterate from latest to oldest available version for this entry.
pub struct VersionIter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff + From<<V as Diff>::D>,
{
    key: K,
    entry: Option<Entry<K, V>>,
    curval: Option<V>,
    deltas: Option<std::vec::IntoIter<Delta<V>>>,
}

impl<K, V> Iterator for VersionIter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff + From<<V as Diff>::D>,
{
    type Item = Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        // first iteration
        if let Some(entry) = self.entry.take() {
            if entry.value.is_reference() {
                self.deltas.take();
                return None;
            } else {
                self.curval = entry.to_native_value();
                return Some(entry);
            }
        }
        // remaining iterations
        let delta = {
            match &mut self.deltas {
                Some(deltas) => match deltas.next() {
                    None => {
                        return None;
                    }
                    Some(delta) if delta.is_reference() => {
                        self.deltas.take();
                        return None;
                    }
                    Some(delta) => delta,
                },
                None => return None,
            }
        };
        let (value, curval) = next_value(self.curval.take(), delta.data);
        self.curval = curval;
        Some(Entry::new(self.key.clone(), value))
    }
}

fn next_value<V>(value: Option<V>, delta: InnerDelta<V>) -> (Value<V>, Option<V>)
where
    V: Clone + Diff + From<<V as Diff>::D>,
{
    match (value, delta) {
        (None, InnerDelta::D { seqno }) => {
            // consequitive delete
            (Value::new_delete(seqno), None)
        }
        (Some(_), InnerDelta::D { seqno }) => {
            // this entry is deleted.
            (Value::new_delete(seqno), None)
        }
        (None, InnerDelta::U { delta, seqno }) => {
            // previous entry was a delete.
            let nv: V = From::from(delta.into_native_delta().unwrap());
            let v = Box::new(vlog::Value::new_native(nv.clone()));
            let value = Value::new_upsert(v, seqno);
            (value, Some(nv))
        }
        (Some(curval), InnerDelta::U { delta, seqno }) => {
            // this and previous entry are create/update.
            let nv = curval.merge(&delta.into_native_delta().unwrap());
            let v = Box::new(vlog::Value::new_native(nv.clone()));
            let value = Value::new_upsert(v, seqno);
            (value, Some(nv))
        }
    }
}

// Wrapper type for entries iterated by piece-wise full-table scanner.
pub enum ScanEntry<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    // Entry found, continue with iteration.
    Found(Entry<K, V>),
    // Refill.
    Retry(K),
}

#[cfg(test)]
#[path = "core_test.rs"]
mod core_test;
