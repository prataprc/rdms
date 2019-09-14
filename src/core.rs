use std::borrow::Borrow;
use std::convert::TryInto;
use std::ops::{Bound, RangeBounds};
use std::{fs, mem};

use crate::error::Error;
use crate::vlog;

/// Result returned by bogn functions and methods.
pub type Result<T> = std::result::Result<T, Error>;

/// Type alias to trait-objects iterating over [`Index`] [`Entry`].
pub type IndexIter<'a, K, V> = Box<dyn Iterator<Item = Result<Entry<K, V>>> + 'a>;

/// Trait for diffable values.
///
/// All values indexed in [Bogn] must support this trait, since [Bogn]
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
/// [Bogn]: crate::Bogn
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
    fn footprint(&self) -> isize;
}

/// Index trait implemented by [Bogn]'s underlying data-structures that
/// can ingest key, value pairs.
///
/// [Bogn]: crate::Bogn
///
pub trait Index<K, V>: Sized + Footprint
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    /// A writer type, that can ingest key-value pairs, associated with
    /// this index.
    type W: Writer<K, V>;

    /// Make a new empty index of this type, with same configuration.
    fn make_new(&self) -> Result<Box<Self>>;

    /// Create a new writer handle. Note that, not all indexes allow
    /// concurrent writers, and not all indexes support concurrent
    /// read/write.
    fn to_writer(&mut self) -> Self::W;
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
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized;

    /// Iterate from upper bound to lower bound. Returned entry may not
    /// have all its previous versions, if it is costly to fetch from disk.
    fn reverse<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
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
    fn range_with_versions<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized;

    /// Iterate from upper bound to lower bound. Returned entry shall
    /// have all its previous versions, can be a costly call.
    fn reverse_with_versions<'a, R, Q>(&'a self, rng: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized;
}

/// Index full table scan.
pub trait FullScan<K, V>
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
    fn full_scan<G>(&self, from: Bound<K>, within: G) -> Result<IndexIter<K, V>>
    where
        G: Clone + RangeBounds<u64>;
}

/// Index write operations.
pub trait Writer<K, V>
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

/// Serialize values to binary sequence of bytes.
pub trait Serialize: Sized {
    /// Convert this value into binary equivalent. Encoded bytes shall
    /// be appended to the input-buffer `buf`. Return bytes encoded.
    fn encode(&self, buf: &mut Vec<u8>) -> usize;

    /// Reverse process of encode, given the binary equivalent `buf`,
    /// construct ``self``.
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

    pub(crate) fn footprint(&self) -> isize {
        let mut footprint: isize = mem::size_of::<Delta<V>>().try_into().unwrap();
        footprint += match &self.data {
            InnerDelta::U { delta, .. } => delta.diff_footprint(),
            InnerDelta::D { .. } => 0,
        };
        footprint
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
            Value::U {
                value: vlog::Value::Reference { .. },
                ..
            } => true,
            _ => false,
        }
    }
}

impl<V> Value<V>
where
    V: Clone + Diff + Footprint,
{
    pub(crate) fn footprint(&self) -> isize {
        let mut footprint: isize = mem::size_of::<Value<V>>().try_into().unwrap();
        footprint += match self {
            Value::U { value, .. } => value.value_footprint(),
            Value::D { .. } => 0,
        };
        footprint
    }
}

/// Entry is the covering structure for a {Key, value} pair
/// indexed by bogn data structures.
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
    V: Clone + Diff + Footprint,
{
    // Prepend a new version, also the latest version, for this entry.
    // In non-lsm mode this is equivalent to over-writing previous value.
    pub(crate) fn prepend_version(&mut self, nentry: Self, lsm: bool) -> isize {
        if lsm {
            self.prepend_version_lsm(nentry)
        } else {
            self.prepend_version_nolsm(nentry)
        }
    }

    fn prepend_version_nolsm(&mut self, nentry: Self) -> isize {
        let size = self.value.footprint();
        self.value = nentry.value.clone();
        (self.value.footprint() - size).try_into().unwrap()
    }

    fn prepend_version_lsm(&mut self, nentry: Self) -> isize {
        let delta = match self.value.as_ref() {
            Value::D { seqno } => Delta::new_delete(*seqno),
            Value::U {
                value: vlog::Value::Native { value },
                seqno,
            } => {
                let d = nentry.to_native_value().unwrap().diff(value);
                let nd = vlog::Delta::new_native(d);
                Delta::new_upsert(nd, *seqno)
            }
            Value::U {
                value: vlog::Value::Reference { .. },
                ..
            } => unreachable!(),
        };

        let size = {
            let size = nentry.value.footprint() + delta.footprint();
            size - self.value.footprint()
        };

        self.deltas.insert(0, delta);
        self.prepend_version_nolsm(nentry);
        size.try_into().unwrap()
    }

    // only lsm, if entry is already deleted this call becomes a no-op.
    pub(crate) fn delete(&mut self, seqno: u64) -> isize {
        let delta_size = match self.value.as_ref() {
            Value::D { .. } => 0, // NOOP
            Value::U {
                value: vlog::Value::Native { value },
                seqno,
            } => {
                let delta = {
                    let d: <V as Diff>::D = From::from(value.clone());
                    vlog::Delta::new_native(d)
                };
                let size = delta.diff_footprint();
                self.deltas.insert(0, Delta::new_upsert(delta, *seqno));
                size
            }
            Value::U {
                value: vlog::Value::Reference { .. },
                ..
            } => unreachable!(),
        };
        let size = self.value.footprint();
        *self.value = Value::new_delete(seqno);
        (size + delta_size - self.value.footprint())
            .try_into()
            .unwrap()
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
            .into_iter()
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
    // Note that, by bogn-design only memory-indexes ingesting new
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
        let mut iter = entry.deltas.into_iter();
        while let Some(delta) = iter.next() {
            let value = entry.value.to_native_value();
            let (value, _) = next_value(value, delta.data);
            entry.value = Box::new(value);
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

        // TODO remove this validation logic once bogn is fully stable.
        a.validate_flush_merge(&b);
        for ne in a.versions().collect::<Vec<Entry<K, V>>>().into_iter().rev() {
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
        match self.value.as_ref() {
            Value::U {
                value: vlog::Value::Reference { fpos, length, .. },
                seqno,
            } => {
                let value = vlog::fetch_value(*fpos, *length, fd)?;
                self.value = Box::new(Value::new_upsert(value, *seqno));
                Ok(())
            }
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
}

impl<K, V> Entry<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    /// Return the previous versions of this entry as Deltas.
    pub fn footprint(&self) -> isize {
        let mut fp: isize = mem::size_of::<Entry<K, V>>().try_into().unwrap();
        fp += self.value.footprint();
        for delta in self.deltas.iter() {
            fp += delta.footprint();
        }
        fp
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
        Some(Entry::new(self.key.clone(), Box::new(value)))
    }
}

fn next_value<V>(value: Option<V>, delta: InnerDelta<V>) -> (Value<V>, Option<V>)
where
    V: Clone + Diff + From<<V as Diff>::D>,
{
    match (value, delta) {
        (None, InnerDelta::D { .. }) => {
            panic!("consecutive versions can't be a delete");
        }
        (Some(_), InnerDelta::D { seqno }) => {
            // this entry is deleted.
            (Value::new_delete(seqno), None)
        }
        (None, InnerDelta::U { delta, seqno }) => {
            // previous entry was a delete.
            let nv: V = From::from(delta.into_native_delta().unwrap());
            let v = vlog::Value::new_native(nv.clone());
            let value = Value::new_upsert(v, seqno);
            (value, Some(nv))
        }
        (Some(curval), InnerDelta::U { delta, seqno }) => {
            // this and previous entry are create/update.
            let nv = curval.merge(&delta.into_native_delta().unwrap());
            let v = vlog::Value::new_native(nv.clone());
            let value = Value::new_upsert(v, seqno);
            (value, Some(nv))
        }
    }
}

#[cfg(test)]
#[path = "core_test.rs"]
mod core_test;
