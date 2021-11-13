use arbitrary::{Arbitrary, Unstructured};

use std::ops::Bound;

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
/// any of the older versions, and also, compact away entries marked as deleted.
///
/// _lsm-compaction_
///
/// Discard all versions of value/entry older than the specified seqno.
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
/// _seqno ZERO_
///
/// If `seqno` is specified as ZERO for cutoff, then compaction operation is treated
/// as no-op.
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

impl<'a> Arbitrary<'a> for Cutoff {
    fn arbitrary(u: &mut Unstructured) -> arbitrary::Result<Self> {
        let variant = u.arbitrary::<u8>()? % 3;
        let bound = u.arbitrary::<u8>()? % 3;
        let seqno = {
            let ss: Vec<u64> = (0..16).map(|x| 1 << (x * 2)).collect();
            ss[u.arbitrary::<usize>()? % ss.len()]
        };

        let bound = match bound {
            0 => Bound::Unbounded,
            1 => Bound::Included(seqno),
            2 => Bound::Excluded(seqno),
            _ => unreachable!(),
        };
        match variant {
            0 => Ok(Cutoff::Mono),
            1 => Ok(Cutoff::Lsm(bound)),
            2 => Ok(Cutoff::Tombstone(bound)),
            _ => unreachable!(),
        }
    }
}

impl Cutoff {
    /// Create a cutoff for deduplication, refer to [Cutoff] type for details.
    #[inline]
    pub fn new_mono() -> Cutoff {
        Cutoff::Mono
    }

    /// Create a cutoff for tombstone-compaction, refer to [Cutoff] for details.
    #[inline]
    pub fn new_tombstone(b: Bound<u64>) -> Cutoff {
        Cutoff::Tombstone(b)
    }

    /// Create a cutoff for lsm-compaction, refer to [Cutoff] for details.
    #[inline]
    pub fn new_lsm(b: Bound<u64>) -> Cutoff {
        Cutoff::Lsm(b)
    }

    // TODO: remove this, after full refactor of rdms
    //pub fn new_tombstone_empty() -> Cutoff {
    //    Cutoff::Lsm(Bound::Excluded(std::u64::MIN))
    //}

    // TODO: remove this, after full refactor of rdms
    //pub fn new_lsm_empty() -> Cutoff {
    //    Cutoff::Lsm(Bound::Excluded(std::u64::MIN))
    //}

    /// Return the cutoff bound in sequence number.
    #[inline]
    pub fn to_bound(&self) -> Bound<u64> {
        match self {
            Cutoff::Mono => Bound::Excluded(std::u64::MIN),
            Cutoff::Lsm(b) => *b,
            Cutoff::Tombstone(b) => *b,
        }
    }

    /// Return true, if this cutoff when applied to compaction does nothing to index.
    // TODO: previously it was is_empty(), after full refactor of rdms.
    #[inline]
    pub fn is_noop(&self) -> bool {
        match self {
            Cutoff::Mono => false,
            Cutoff::Lsm(Bound::Excluded(n)) => *n == std::u64::MIN,
            Cutoff::Tombstone(Bound::Excluded(n)) => *n == std::u64::MIN,
            _ => false,
        }
    }
}
