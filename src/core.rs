//! Module `core` define and implement core types and traits for [rdms].
//!
//! List of types implementing CommitIterator
//! =========================================
//!
//! * [CommitWrapper][scans::CommitWrapper], a wrapper type to convert any
//!   iterator into a [CommitIterator].
//! * [std::vec::IntoIter], iterator from std-lib for a vector of entries.
//! * [Llrb], memory index using left-leaning-red-black tree.
//! * [Mvcc], memory index using multi-version-concurrency-control for LLRB.
//! * [Robt], disk index using full-packed, immutable btree.
//!

use std::{
    borrow::Borrow,
    ffi, fmt,
    hash::Hash,
    marker,
    ops::{Bound, RangeBounds},
    result,
};

pub use crate::entry::Entry;
pub(crate) use crate::entry::{Delta, InnerDelta, Value};

use crate::{error::Error, util};
#[allow(unused_imports)]
use crate::{
    llrb::Llrb,
    mvcc::Mvcc,
    rdms::{self, Rdms},
    robt::Robt,
    scans,
    wal::Wal,
};

/// Type alias for all results returned by [rdms] methods.
pub type Result<T> = result::Result<T, Error>;

/// Type alias to trait-objects iterating over an index.
pub type IndexIter<'a, K, V> = Box<dyn Iterator<Item = Result<Entry<K, V>>> + 'a>;

/// Type alias to trait-objects iterating, piece-wise, over [Index].
pub type ScanIter<'a, K, V> = Box<dyn Iterator<Item = Result<ScanEntry<K, V>>> + 'a>;

/// A convenience trait to group thread-safe trait conditions.
pub trait ThreadSafe: 'static + Send {}

// TODO: should cutoff have a force variant to force compaction ?
/// Cutoff enumerated parameter to [compact][Index::compact] method. Refer
/// to [rdms] library documentation for more information on compaction.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Cutoff {
    /// Mono compaction is for non-lsm compaction.
    Mono,
    /// Tombstone-compaction, refer to [rdms] for more detail.
    Tombstone(Bound<u64>),
    /// Lsm-compaction, refer to [rdms] for more detail.
    Lsm(Bound<u64>),
}

impl Cutoff {
    pub fn new_mono() -> Cutoff {
        Cutoff::Mono
    }

    pub fn new_tombstone(b: Bound<u64>) -> Cutoff {
        Cutoff::Tombstone(b)
    }

    pub fn new_tombstone_empty() -> Cutoff {
        Cutoff::Lsm(Bound::Excluded(std::u64::MIN))
    }

    pub fn new_lsm(b: Bound<u64>) -> Cutoff {
        Cutoff::Lsm(b)
    }

    pub fn new_lsm_empty() -> Cutoff {
        Cutoff::Lsm(Bound::Excluded(std::u64::MIN))
    }
    pub fn to_bound(&self) -> Bound<u64> {
        match self {
            Cutoff::Mono => Bound::Excluded(std::u64::MIN),
            Cutoff::Lsm(b) => b.clone(),
            Cutoff::Tombstone(b) => b.clone(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Cutoff::Mono => false,
            Cutoff::Lsm(Bound::Excluded(n)) => *n == std::u64::MIN,
            Cutoff::Tombstone(Bound::Excluded(n)) => *n == std::u64::MIN,
            _ => false,
        }
    }
}

/// Trait for diffable values.
///
/// Version control is a unique feature built into [rdms]. And this is possible
/// by values implementing this trait. Note that this version control follows
/// centralised behaviour, as apposed to distributed behaviour, for which we
/// need three-way-merge trait. Now more on how it works:
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
pub trait Diff: Sized + From<<Self as Diff>::D> {
    type D: Clone + From<Self> + Into<Self> + Footprint;

    /// Return the delta between two consecutive versions of a value.
    /// `Delta = New - Old`.
    fn diff(&self, old: &Self) -> Self::D;

    /// Merge delta with newer version to return older version of the value.
    /// `Old = New - Delta`.
    fn merge(&self, delta: &Self::D) -> Self;
}

/// Trait to be implemented by index-types, key-types and, value-types.
///
/// This trait is required to compute the memory or disk foot-print
/// for index-types, key-types and value-types.
///
/// **Note: This can be an approximate measure.**
///
pub trait Footprint {
    /// Return the approximate size of the underlying type, when
    /// stored in memory or serialized on disk.
    ///
    /// NOTE: `isize` is used instead of `usize` because of delta computation.
    fn footprint(&self) -> Result<isize>;
}

/// Trait define methods to integrate index with [Wal] (Write-Ahead-Log).
///
/// All the methods defined by this trait will be dispatched when
/// reloading an index from on-disk Write-Ahead-Log.
pub trait Replay<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Replay set operation from wal-file onto index.
    fn set_index(&mut self, key: K, value: V, index: u64) -> Result<()>;

    /// Replay set-cas operation from wal-file onto index.
    fn set_cas_index(&mut self, key: K, value: V, cas: u64, index: u64) -> Result<()>;

    /// Replay delete operation from wal-file onto index.
    fn delete_index(&mut self, key: K, index: u64) -> Result<()>;
}

/// Trait define methods to integrate index with Wal (Write-Ahead-Log).
///
/// After writing into the [Wal], write operation shall be applied on
/// the [Index] [write-handle][Index::W].
pub trait WalWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Set {key, value} in index. Return older entry if present.
    ///
    /// *LSM mode*: Add a new version for the key, perserving the old value.
    fn set_index(&mut self, key: K, value: V, index: u64) -> Result<Option<Entry<K, V>>>;

    /// Set {key, value} in index if an older entry exists with the
    /// same `cas` value. To create a fresh entry, pass `cas` as ZERO.
    /// Return older entry if present.
    ///
    /// *LSM mode*: Add a new version for the key, perserving the old value.
    fn set_cas_index(
        &mut self,
        key: K,
        value: V,
        cas: u64,
        index: u64,
    ) -> Result<Option<Entry<K, V>>>;

    /// Delete key from index. Return old entry if present.
    ///
    /// *LSM mode*: Mark the entry as deleted along with seqno at which it
    /// deleted
    ///
    /// NOTE: K should be borrowable as &Q and Q must be convertable to
    /// owned K. This is require in lsm mode, where owned K must be
    /// inserted into the tree.
    fn delete_index<Q>(&mut self, key: &Q, index: u64) -> Result<Option<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized;
}

/// Trait to create new memory based index instances using pre-defined set of
/// configuration.
pub trait WriteIndexFactory<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    type I: Index<K, V> + Footprint;

    /// Create a new index instance with predefined configuration,
    /// Typically this index will be used to index new set of entries.
    fn new(&self, name: &str) -> Result<Self::I>;

    /// Index type identification purpose.
    fn to_type(&self) -> String;
}

/// Trait to create new disk based index instances using pre-defined set
/// of configuration.
pub trait DiskIndexFactory<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    type I: Clone + Index<K, V> + CommitIterator<K, V> + Footprint;

    /// Create a new index instance with predefined configuration.
    /// Typically this index will be used to commit newer snapshots
    /// onto disk.
    fn new(&self, dir: &ffi::OsStr, name: &str) -> Result<Self::I>;

    /// Open an existing index instance with predefined configuration.
    fn open(&self, dir: &ffi::OsStr, name: &str) -> Result<Self::I>;

    /// Index type for identification purpose.
    fn to_type(&self) -> String;
}

/// Trait to commit a batch of pre-sorted entries into target index.
///
/// Main purpose of this trait is to give target index, into which
/// the source iterator must be commited, an ability to generate the
/// actual iterator(s) the way it suits itself. In other words, target
/// index might call any of the method to generate the required iterator(s).
///
/// On the other hand, it may not be possible for the target index to
/// know the `within` sequence-no range to filter out entries and its
/// versions, for which we use [CommitIter]
pub trait CommitIterator<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Return a handle for full table iteration. Caller can hold this handle
    /// for a long time, hence implementors should make sure to handle
    /// unwanted side-effects.
    fn scan<G>(&mut self, within: G) -> Result<IndexIter<K, V>>
    where
        G: Clone + RangeBounds<u64>;

    /// Return a list of equally balanced handles to iterate on
    /// range-partitioned entries.
    fn scans<G>(&mut self, n_shards: usize, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>;

    /// Same as [scans][CommitIterator::scans] but range partition is
    /// decided by the `ranges` argument. And unlike the `shards` argument,
    /// `ranges` argument is treated with precision, number of iterators
    /// returned shall exactly match _range.len()_.
    fn range_scans<N, G>(&mut self, ranges: Vec<N>, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
        N: Clone + RangeBounds<K>;
}

/// Trait implemented by all types of rdms-indexes.
///
/// Note that not all index types shall implement all the methods
/// defined by this trait.
///
pub trait Index<K, V>: Sized
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Writer handle into this index to ingest, concurrently, key-value pairs.
    type W: Writer<K, V>;

    /// Reader handle into this index to concurrently access with other readers
    /// and writers.
    type R: Reader<K, V> + CommitIterator<K, V>;

    /// Return the name of the index.
    fn to_name(&self) -> Result<String>;

    /// Return application metadata, that was previously commited into index.
    fn to_metadata(&self) -> Result<Vec<u8>>;

    /// Return the current seqno tracked by this index.
    fn to_seqno(&self) -> Result<u64>;

    /// Application can set the start sequence number for this index.
    fn set_seqno(&mut self, seqno: u64) -> Result<()>;

    /// Create a new read handle, for multi-threading. Note that not all
    /// indexes allow concurrent readers. Refer to index API for more details.
    fn to_reader(&mut self) -> Result<Self::R>;

    /// Create a new write handle, for multi-threading. Note that not all
    /// indexes allow concurrent writers. Refer to index API for more details.
    fn to_writer(&mut self) -> Result<Self::W>;

    /// Commit entries from iterator into the index. Though it takes mutable
    /// reference, there can be concurrent compact() call. It is upto the
    /// implementing type to synchronize the concurrent commit() and compact()
    /// calls.
    fn commit<C, F>(&mut self, scanner: CommitIter<K, V, C>, mf: F) -> Result<()>
    where
        C: CommitIterator<K, V>,
        F: Fn(Vec<u8>) -> Vec<u8>;

    /// Compact index to reduce index-footprint. Though it takes mutable
    /// reference, there can be concurrent commit() call. It is upto the
    /// implementing type to synchronize the concurrent commit() and
    /// compact() calls. All entries whose mutation versions are below the
    /// `cutoff` bound can be purged permenantly.
    ///
    /// Return number of items in index.
    fn compact(&mut self, cutoff: Cutoff) -> Result<usize>;

    /// End of index life-cycle. Persisted data (in disk) shall not be
    /// cleared. Refer [purge][Index::purge] for that.
    fn close(self) -> Result<()>;

    /// End of index life-cycle. Also clears persisted data (in disk).
    fn purge(self) -> Result<()>;
}

/// Trait to self-validate index's internal state.
pub trait Validate<T: fmt::Display> {
    /// Call this to make sure all is well. Note that this can be
    /// a costly call. Returned value can be serialized into string
    /// format and logged, printed, etc..
    fn validate(&mut self) -> Result<T>;
}

/// Trait to manage keys in a bitmapped Bloom-filter.
pub trait Bloom: Sized {
    /// Create an empty bit-map.
    fn create() -> Self;

    /// Return the number of items in the bitmap.
    fn len(&self) -> Result<usize>;

    /// Add key into the index.
    fn add_key<Q: ?Sized + Hash>(&mut self, element: &Q);

    /// Add key into the index.
    fn add_digest32(&mut self, digest: u32);

    /// Check whether key in persent, there can be false positives but
    /// no false negatives.
    fn contains<Q: ?Sized + Hash>(&self, element: &Q) -> bool;

    /// Serialize the bit-map to binary array.
    fn to_vec(&self) -> Vec<u8>;

    /// Deserialize the binary array to bit-map.
    fn from_vec(buf: &[u8]) -> Result<Self>;

    /// Merge two bitmaps.
    fn or(&self, other: &Self) -> Result<Self>;
}

/// Trait define read operations for rdms-index.
pub trait Reader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Get `key` from index. Returned entry may not have all its
    /// previous versions, if it is costly to fetch from disk.
    fn get<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + Hash;

    /// Iterate over all entries in this index. Returned entry may not
    /// have all its previous versions, if it is costly to fetch from disk.
    fn iter(&mut self) -> Result<IndexIter<K, V>>;

    /// Iterate from lower bound to upper bound. Returned entry may not
    /// have all its previous versions, if it is costly to fetch from disk.
    fn range<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized;

    /// Iterate from upper bound to lower bound. Returned entry may not
    /// have all its previous versions, if it is costly to fetch from disk.
    fn reverse<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized;

    /// Get `key` from index. Returned entry shall have all its
    /// previous versions, can be a costly call.
    fn get_with_versions<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + Hash;

    /// Iterate over all entries in this index. Returned entry shall
    /// have all its previous versions, can be a costly call.
    fn iter_with_versions(&mut self) -> Result<IndexIter<K, V>>;

    /// Iterate from lower bound to upper bound. Returned entry shall
    /// have all its previous versions, can be a costly call.
    fn range_with_versions<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized;

    /// Iterate from upper bound to lower bound. Returned entry shall
    /// have all its previous versions, can be a costly call.
    fn reverse_with_versions<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized;
}

/// Trait define write operations for rdms-index.
pub trait Writer<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Set {key, value} in index. Return older entry if present.
    /// If operation was invalid or NOOP, returned seqno shall be ZERO.
    ///
    /// *LSM mode*: Add a new version for the key, perserving the old value.
    fn set(&mut self, k: K, v: V) -> Result<Option<Entry<K, V>>>;

    /// Set {key, value} in index if an older entry exists with the
    /// same `cas` value. To create a fresh entry, pass `cas` as ZERO.
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

/// Trait to serialize key and value types.
pub trait Serialize: Sized {
    /// Convert this value into binary equivalent. Encoded bytes shall
    /// be appended to the input-buffer `buf`. Return bytes encoded.
    fn encode(&self, buf: &mut Vec<u8>) -> Result<usize>;

    /// Reverse process of encode, given the binary equivalent `buf`,
    /// construct `self`.
    fn decode(&mut self, buf: &[u8]) -> Result<usize>;
}

/// Trait typically implemented by mem-only indexes, to construct a stable
/// full-table scan.
///
/// Indexes implementing this trait is expected to return an iterator over
/// all its entries. Some of the necessary conditions are:
///
/// * Iteration should be stable even if there is background mutation.
/// * Iteration should not block background mutation, it might block for a
///   short while though.
pub trait PiecewiseScan<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Return an iterator over entries that meet following properties
    /// * Only entries greater than range.start_bound().
    /// * Only entries whose modified seqno is within seqno-range.
    ///
    /// This method is typically implemented by memory-only indexes. Also,
    /// returned entry may not have all its previous versions, if it is
    /// costly to fetch from disk.
    fn pw_scan<G>(&mut self, from: Bound<K>, within: G) -> Result<ScanIter<K, V>>
    where
        G: Clone + RangeBounds<u64>;
}

/// Trait to serialize an implementing type to JSON encoded string.
///
/// Typically used for web-interfaces.
pub trait ToJson {
    /// Call this method to get the JSON encoded string.
    fn to_json(&self) -> String;
}

/// Covering type for entries iterated by piece-wise full-table scanner.
///
/// This covering type is necessary because of the way [PiecewiseScan]
/// implementation works. Refer to the documentation of the trait for
/// additional detail. To meet the trait's expectation, the implementing
/// index should have the ability to differentiate between end-of-iteration
/// and end-of-iteration to release the read-lock, if any.
pub enum ScanEntry<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Entry found, continue with iteration.
    Found(Entry<K, V>),
    /// Refill denotes end-of-iteration to release the read-lock.
    Retry(K),
}

/// Container type for types implementing [CommitIterator] trait.
///
/// Refer to the trait for more details. Instead of using [CommitIterator]
/// type directly, we are using [CommitIter] indirection for [Index::commit]
/// operation to handle situations where it is not possible/efficient to
/// construct a filtered-iterator, but the target index is known to pick any
/// of the [CommitIterator] method to construct the actual iterators.
pub struct CommitIter<K, V, C>
where
    K: Clone + Ord,
    V: Clone + Diff,
    C: CommitIterator<K, V>,
{
    scanner: C,
    start: Bound<u64>,
    end: Bound<u64>,

    _phantom_key: marker::PhantomData<K>,
    _phantom_val: marker::PhantomData<V>,
}

impl<K, V, C> CommitIter<K, V, C>
where
    K: Clone + Ord,
    V: Clone + Diff,
    C: CommitIterator<K, V>,
{
    /// Construct a new commitable iterator from scanner, `within` shall
    /// be passed to scanner that allows target index to generate the actual
    /// iterator allowing for both efficiency and flexibility.
    pub fn new<G>(scanner: C, within: G) -> CommitIter<K, V, C>
    where
        G: RangeBounds<u64>,
    {
        let (start, end) = util::to_start_end(within);
        CommitIter {
            scanner,
            start,
            end,
            _phantom_key: marker::PhantomData,
            _phantom_val: marker::PhantomData,
        }
    }

    /// Return the `within` argument supplied while constructing this iterator.
    pub fn to_within(&self) -> (Bound<u64>, Bound<u64>) {
        (self.start.clone(), self.end.clone())
    }

    /// Calls underlying scanner's [scan][CommitIterator::scan] method
    /// along with `within` to generate the actual commitable iterator.
    pub fn scan(&mut self) -> Result<IndexIter<K, V>> {
        let within = (self.start.clone(), self.end.clone());
        self.scanner.scan(within)
    }

    /// Same as scan, except that it calls scanner's
    /// [scans][CommitIterator::scans] method.
    pub fn scans(&mut self, n_shards: usize) -> Result<Vec<IndexIter<K, V>>> {
        let within = (self.start.clone(), self.end.clone());
        self.scanner.scans(n_shards, within)
    }

    /// Same as scan, except that it calls scanner's
    /// [range_scans][CommitIterator::range_scans] method.
    pub fn range_scans<N>(&mut self, rs: Vec<N>) -> Result<Vec<IndexIter<K, V>>>
    where
        N: Clone + RangeBounds<K>,
    {
        let within = (self.start.clone(), self.end.clone());
        self.scanner.range_scans(rs, within)
    }
}

impl<K, V> CommitIter<K, V, std::vec::IntoIter<Result<Entry<K, V>>>>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Construct an empty iterable.
    pub fn new_empty() -> CommitIter<K, V, std::vec::IntoIter<Result<Entry<K, V>>>> {
        CommitIter {
            scanner: vec![].into_iter(),
            start: Bound::<u64>::Unbounded,
            end: Bound::<u64>::Unbounded,
            _phantom_key: marker::PhantomData,
            _phantom_val: marker::PhantomData,
        }
    }
}

#[cfg(test)]
#[path = "core_test.rs"]
mod core_test;
