//! Traits and Types required by rest of the rdms-modules.

use std::{fmt, hash::Hash, ops::Bound, result};

use crate::Result;

mod delta;
mod entry;
mod nodiff;
mod value;

// pub use entry::{Delta, Entry};
pub use entry::Entry;
pub use nodiff::NoDiff;
pub use value::Value;

//TODO
// mod db;

/// Trait for diff-able values.
///
/// Version control is a necessary feature for non-destructive writes.
/// Using this trait it is possible to generate concise older versions as
/// deltas. Note that this version control follows centralized behavior, as
/// apposed to distributed behavior, for which we need three-way-merge.
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
pub trait Diff: Sized + From<<Self as Diff>::Delta> {
    type Delta: Clone + From<Self>;

    /// Return the delta between two consecutive versions of a value.
    /// `Delta = New - Old`.
    fn diff(&self, old: &Self) -> Self::Delta;

    /// Merge delta with newer version to return older version of the value.
    /// `Old = New - Delta`.
    fn merge(&self, delta: &Self::Delta) -> Self;
}

/// Trait to bulk-add entries into an index.
pub trait BuildIndex<K, V, D, B> {
    type Err;

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
    ) -> result::Result<(), Self::Err>
    where
        I: Iterator<Item = Entry<K, V, D>>;
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

//pub trait Lookup<K, V> {
//    fn get<Q>(&self, key: &Q) -> Option<V>
//    where
//        K: Borrow<Q>,
//        Q: PartialEq;
//
//    fn set(&mut self, key: K, value: V) -> Option<V>
//    where
//        K: PartialEq;
//
//    fn remove<Q>(&mut self, key: &Q) -> Option<V>
//    where
//        K: Borrow<Q>,
//        Q: PartialEq;
//}

macro_rules! impl_diff_basic_types {
    ($($type:ident,)*) => (
        $(
            impl Diff for $type {
                type Delta = $type;

                fn diff(&self, old: &$type) -> Self::Delta {
                    *old
                }

                fn merge(&self, delta: &Self::Delta) -> Self {
                    *delta
                }
            }
        )*
    );
}

impl_diff_basic_types![
    bool, char, f32, f64, i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize,
];

/// Cutoff is enumerated type to describe compaction behaviour.
///
/// All versions of an entry older than Cutoff is skipped while compaction. If all
/// versions of an entry is older than Cutoff then whole entry can be skiipped.
///
/// Different behavior for compaction is captured below:
///
/// _deduplication_
///
/// This is basically applicable for snapshots that don't have to preserve
/// any of the older versions and compact away entries marked as deleted.
///
/// _lsm-compaction_
///
/// Discard all versions of an entry older than the specified seqno.
///
/// This is applicable for database index that store their index as multi-level
/// snapshots, similar to [leveldb][leveldb]. Most of the lsm-based-storage will
/// have their root snapshot as the oldest and only source of truth, but this
/// is not possible for distributed index that ends up with multiple truths
/// across different nodes. To facilitate such designs, in lsm mode, even the
/// root level at any given node, can retain older versions upto a specified
/// `seqno`, which is computed through eventual consistency.
///
/// _tombstone-compaction_
///
/// When application logic issue `tombstone-compaction` only entries marked as
/// deleted and whose deleted seqno is older than specified seqno shall be
/// compacted away.
///
/// [leveldb]: https://en.wikipedia.org/wiki/LevelDB
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Cutoff {
    /// Deduplicating behavior.
    Mono,
    /// Lsm compaction behaviour.
    Lsm(Bound<u64>),
    /// Tombstone compaction behaviour.
    Tombstone(Bound<u64>),
}
