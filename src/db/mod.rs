//! Traits and Types, related to core-database, and required by rest of this package.

use std::{borrow::Borrow, fmt, hash::Hash, result};

use crate::Result;

// trait-defs: Diff, BuildIndex, Lookup, Footprint, Replay, WalWriter, Bloom
// type-defs : Entry, Value, Delta, Cutoff, NoDiff

mod compact;
mod delta;
mod diff;
mod entry;
mod types;
mod value;

pub use compact::Cutoff;
pub(crate) use delta::Delta;
pub use diff::{Diff, NoDiff};
pub use entry::Entry;
pub(crate) use value::Value;

/// Trait to bulk-add entries into an index.
pub trait BuildIndex<K, V, B> {
    type Error;

    /// Build an index form iterator. Optionally a bitmap can be specified to
    /// implement a bloom filter. If bitmap filter is not required, pass bitmap
    /// as `NoBitmap`. `seqno` can be supplied to set the snapshot's seqno, if
    /// supplied as None, snapshot will take is latest-seqno as the high seqno
    /// found in the iterated entries.
    fn build_index<I>(
        &mut self,
        iter: I,
        bitmap: B,
        seqno: Option<u64>,
    ) -> result::Result<(), Self::Error>
    where
        V: Diff,
        I: Iterator<Item = Entry<K, V, <V as Diff>::Delta>>;
}

/// Trait to access key,value index.
pub trait Lookup<K, V> {
    fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: PartialEq;

    fn set(&mut self, key: K, value: V) -> Option<V>
    where
        K: PartialEq;

    fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: PartialEq;
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

/// Trait define methods to integrate index with Wal (Write-Ahead-Log).
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

// TODO: check whether WalWriter and Replay trait can be consolidated.
/// Trait define methods to integrate index with Wal (Write-Ahead-Log).
///
/// After writing into the `Wal`, write operation shall be applied on
/// the `Index`.
pub trait WalWriter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Set {key, value} in index. Return older entry if present.
    ///
    /// *LSM mode*: Add a new version for the key, perserving the old value.
    fn set_index(
        &mut self,
        key: K,
        value: V,
        index: u64,
    ) -> Result<Option<Entry<K, V, <V as Diff>::Delta>>>;

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
    ) -> Result<Option<Entry<K, V, <V as Diff>::Delta>>>;

    /// Delete key from index. Return old entry if present.
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
        index: u64,
    ) -> Result<Option<Entry<K, V, <V as Diff>::Delta>>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized;
}

/// Trait to build and manage keys in a bit-mapped Bloom-filter.
pub trait Bloom: Sized + Default {
    type Err: fmt::Display;

    /// Add key into the index.
    fn add_key<Q: ?Sized + Hash>(&mut self, key: &Q);

    /// Add key into the index.
    fn add_digest32(&mut self, digest: u32);

    /// Build keys, added so far via `add_key` and `add_digest32` into the
    /// bitmap index. Useful for types that support batch building and
    /// immutable bitmap index.
    fn build(&mut self) -> Result<()>;

    /// Check whether key in present, there can be false positives but
    /// no false negatives.
    fn contains<Q: ?Sized + Hash>(&self, element: &Q) -> bool;

    /// Serialize the bit-map to binary array.
    fn to_bytes(&self) -> result::Result<Vec<u8>, Self::Err>;

    /// Deserialize the binary array to bit-map.
    fn from_bytes(buf: &[u8]) -> result::Result<(Self, usize), Self::Err>;

    /// Merge two bitmaps.
    fn or(&self, other: &Self) -> result::Result<Self, Self::Err>;
}

/// Trait to serialize an implementing type to JSON encoded string.
pub trait ToJson {
    /// Call this method to get the JSON encoded string.
    fn to_json(&self) -> String;
}

// TODO: check whether this can be removed in future.
// Trait to create new memory based index instances using pre-defined set of
// configuration. This is needed for multi-level index.
//pub trait WriteIndexFactory<K, V>
//where
//    K: Clone + Ord,
//    V: Clone + Diff,
//{
//    type I: Index<K, V> + Footprint;
//
//    /// Create a new index instance with predefined configuration,
//    /// Typically this index will be used to index new set of entries.
//    fn new(&self, name: &str) -> Result<Self::I>;
//
//    /// Index type identification purpose.
//    fn to_type(&self) -> String;
//}

// TODO: check whether this can be removed in future.
// Trait to create new disk based index instances using pre-defined set
// of configuration. This is needed for multi-level index.
//pub trait DiskIndexFactory<K, V>
//where
//    K: Clone + Ord,
//    V: Clone + Diff,
//{
//    type I: Clone + Index<K, V> + CommitIterator<K, V> + Footprint;
//
//    /// Create a new index instance with predefined configuration.
//    /// Typically this index will be used to commit newer snapshots
//    /// onto disk.
//    fn new(&self, dir: &ffi::OsStr, name: &str) -> Result<Self::I>;
//
//    /// Open an existing index instance with predefined configuration.
//    fn open(&self, dir: &ffi::OsStr, name: &str) -> Result<Self::I>;
//
//    /// Index type for identification purpose.
//    fn to_type(&self) -> String;
//}
