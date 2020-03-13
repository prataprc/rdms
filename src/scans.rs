//! Module `scans` implement iterator types that are useful for constructing
//! complex scans.
//!
//! List of Iterators
//! =================
//!
//! Following is a non-exhaustive list of all iterators implemented in
//! [this](self) module and in other modules.
//!
//! **From [core]**
//!
//! * [VersionIter][core::VersionIter], iterate over older versions of an entry.
//!
//! **From [llrb]**
//!
//! * [Iter][llrb::Iter], returned by `Llrb::iter()` for full table iteration
//!   over [Llrb] index. Note that iteration will block all other operations
//!   in the index.
//! * [IterPWScan][llrb::IterPWScan], returned by `Llrb::pw_scan()` for full
//!   table iteration over [Llrb] index. Unlike `iter()` this won't lock the
//!   index for more than ~ 1ms.
//! * [Range][llrb::Range], returned by `Llrb::range()` for forward scan
//!   from lower-bound to upper-bound.
//! * [Reverse][llrb::Reverse], returned by `Llrb::reverse()` for reverse scan
//!   from upper-bound to lower-bound.
//!
//! **From [mvcc]**
//!
//! * [Iter][mvcc::Iter], returned by `Mvcc::iter()` for full table iteration
//!   over [Mvcc] index. Note that iteration will block garbage collection of
//!   abandoned nodes.
//! * [IterPWScan][mvcc::IterPWScan], returned by `Mvcc::pw_scan()` for full
//!   table iteration over [Mvcc] index. Unlike `iter()` this won't block the
//!   garbage collection for more than ~ 1ms.
//! * [Range][mvcc::Range], returned by `Mvcc::range()` for forward scan from
//!   lower-bound to upper-bound.
//! * [Reverse][mvcc::Reverse], returned by `Mvcc::reverse()` for reverse scan
//!   from upper-bound to lower-bound.
//!
//! **From [lsm]**
//!
//! * [YIter][lsm::YIter], returned by [y_iter][lsm::y_iter] for lsm iteration
//!   used in multi-level indexes like [Dgm].
//! * [YIterVersions][lsm::YIterVersions], returned by
//!   [y_iter_versions][lsm::y_iter_versions] for lsm iteration used in
//!   multi-level indexes like [Dgm].
//!
//! **From [robt]**
//!
//! * [robt::Iter], returned by `robt::Snapshot::iter()` for full table
//!   iteration over [Robt] index.
//! * [robt::Range], returned by `robt::Snapshot::range()` operation.
//! * [robt::Reverse], returned by `robt::Snapshot::reverse()` operation.
//! * `BuildScan`, local to [Robt] index, used while building index.
//! * `CommitScan`, local to [Robt] index, used while building index.
//! * `MZ`, local to [Robt] index, optimization structure for `range()` and
//!   `reverse()` iteration over [Robt] index.
//!
//! **From [scans][self]**
//!
//! * [SkipScan], useful in full-table scan using `pw_scan()` interface.
//!   Additionally, can be configured to filter entries within a key-range and/or
//!   `seqno` range. Used to implement [CommitIterator] for [Llrb] and [Mvcc].
//! * [FilterScans], useful in full-table scan using one or more iterators.
//!   If more than one iterators are supplied Iterators are chained in stack order.
//!   Additionally, can be configured to filter entries within a `seqno` range.
//! * [BitmappedScan], useful to build a bitmap index for all iterated keys.
//! * [CompactScan], useful to filter entries that can be compacted in.
//!

use std::{
    hash::Hash,
    ops::{Bound, RangeBounds},
    vec,
};

use crate::{
    core::{Bloom, CommitIterator, Diff, Entry, IndexIter, PiecewiseScan, Result},
    core::{Cutoff, ScanEntry},
    util,
};

#[allow(unused_imports)]
use crate::{
    core,
    dgm::Dgm,
    llrb::{self, Llrb},
    lsm,
    mvcc::{self, Mvcc},
    robt::{self, Robt},
};

// TODO: benchmark SkipScan and FilterScans and measure the difference.

const SKIP_SCAN_BATCH_SIZE: usize = 1000;

/// Iterator type, for full table iteration of LSM data structure.
///
/// SkipScan achieve full table scan by stitching together piece-wise
/// scan of LSM data-structure, and only selecting mutations (and versions)
/// that are within specified sequence-no range.
///
/// Mitigates following issues.
///
/// * Read references to data-structure is held only for
///   very small period, like few tens of micro-seconds.
/// * Automatically filters mutations that are older than
///   specified sequence-no range, there by saving time for
///   top-level DB components.
/// * Ignores mutations that are newer than the specified
///   sequence-no range, there by providing a stable full
///   table scan.
///
/// Important pre-requist:
///
/// * Applicable only for LSM based data structures.
/// * Data-structure must not suffer any delete/purge
///   operation until full-scan is completed.
/// * Data-structure must implement PiecewiseScan trait.
pub struct SkipScan<K, V, R>
where
    K: Clone + Ord,
    V: Clone + Diff,
    R: PiecewiseScan<K, V>,
{
    reader: R,               // reader handle into index
    seqno_start: Bound<u64>, // pick mutations withing this sequence-no range.
    seqno_end: Bound<u64>,   // pick mutations withing this sequence-no range.
    key_start: Bound<K>,     // pick mutations withing this sequence-no range.
    key_end: Bound<K>,       // pick mutations withing this sequence-no range.

    iter: vec::IntoIter<Result<Entry<K, V>>>,
    batch_size: usize,
    last_batch: bool,
}

enum Refill<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    Ok(Vec<Result<Entry<K, V>>>, Option<K>),
    Retry(K, Vec<Result<Entry<K, V>>>),
    Finish(Vec<Result<Entry<K, V>>>),
}

impl<K, V, R> SkipScan<K, V, R>
where
    K: Clone + Ord,
    V: Clone + Diff,
    R: PiecewiseScan<K, V>,
{
    /// Create a new full table scan using the reader handle. Pick
    /// mutations that are `within` the specified range.
    pub fn new(reader: R) -> SkipScan<K, V, R> {
        SkipScan {
            reader,
            seqno_start: Bound::Unbounded,
            seqno_end: Bound::Unbounded,
            key_start: Bound::Unbounded,
            key_end: Bound::Unbounded,
            iter: vec![].into_iter(),
            batch_size: SKIP_SCAN_BATCH_SIZE,
            last_batch: false,
        }
    }

    /// Set the batch size for each iteration using the reader handle.
    pub fn set_batch_size(&mut self, batch_size: usize) -> Result<&mut Self> {
        self.batch_size = batch_size;
        Ok(self)
    }

    /// Set seqno range to filter out all mutations outside the range.
    pub fn set_seqno_range<G>(&mut self, within: G) -> Result<&mut Self>
    where
        G: RangeBounds<u64>,
    {
        use std::ops::Bound::{Excluded, Included};

        let (start, end) = util::to_start_end(within);
        self.seqno_start = start;
        self.seqno_end = end;
        match (self.seqno_start, self.seqno_end) {
            (Included(s1), Included(s2)) if s1 > s2 => self.batch_size = 0,
            (Included(s1), Excluded(s2)) if s1 >= s2 => self.batch_size = 0,
            (Excluded(s1), Included(s2)) if s1 >= s2 => self.batch_size = 0,
            (Excluded(s1), Excluded(s2)) if s1 >= s2 => self.batch_size = 0,
            _ => (),
        }
        Ok(self)
    }

    /// Set key range to filter out all keys outside the range.
    pub fn set_key_range<G>(&mut self, range: G) -> Result<&mut Self>
    where
        G: RangeBounds<K>,
    {
        self.key_start = match range.start_bound() {
            Bound::Included(key) => Bound::Included(key.clone()),
            Bound::Excluded(key) => Bound::Excluded(key.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };
        self.key_end = match range.end_bound() {
            Bound::Included(key) => Bound::Included(key.clone()),
            Bound::Excluded(key) => Bound::Excluded(key.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };
        Ok(self)
    }

    fn refill(&mut self) -> Refill<K, V> {
        let mut entries: Vec<Result<Entry<K, V>>> = vec![];
        let within = (self.seqno_start.clone(), self.seqno_end.clone());
        match self.reader.pw_scan(self.key_start.clone(), within) {
            Ok(niter) => {
                let mut niter = niter.enumerate();
                loop {
                    match niter.next() {
                        Some((i, Ok(ScanEntry::Found(entry)))) if i <= self.batch_size => {
                            entries.push(Ok(entry))
                        }
                        Some((_, Ok(ScanEntry::Found(entry)))) => {
                            let key_start = Some(entry.to_key());
                            entries.push(Ok(entry));
                            break Refill::Ok(entries, key_start);
                        }
                        Some((_, Ok(ScanEntry::Retry(key)))) => break Refill::Retry(key, entries),
                        Some((_, Err(err))) => {
                            entries.push(Err(err));
                            break Refill::Ok(entries, None);
                        }
                        None => break Refill::Finish(entries),
                    }
                }
            }
            Err(err) => {
                entries.push(Err(err));
                Refill::Ok(entries, None)
            }
        }
    }

    fn is_last_batch(&self, entries: &Vec<Result<Entry<K, V>>>) -> bool {
        match (&self.key_end, entries.last()) {
            (Bound::Unbounded, Some(Ok(_))) => false,
            (Bound::Included(key), Some(Ok(last))) => last.as_key().gt(key),
            (Bound::Excluded(key), Some(Ok(last))) => last.as_key().ge(key),
            (_, _) => true,
        }
    }
}

impl<K, V, R> Iterator for SkipScan<K, V, R>
where
    K: Clone + Ord,
    V: Clone + Diff,
    R: PiecewiseScan<K, V>,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next() {
                Some(Ok(entry)) if !self.last_batch => break Some(Ok(entry)),
                Some(Ok(entry)) => match (entry, &self.key_end) {
                    (entry, Bound::Included(key)) if entry.as_key().le(key) => {
                        break Some(Ok(entry))
                    }
                    (entry, Bound::Excluded(key)) if entry.as_key().lt(key) => {
                        break Some(Ok(entry))
                    }
                    (entry, Bound::Unbounded) => break Some(Ok(entry)),
                    _ => {
                        self.batch_size = 0;
                        self.iter = vec![].into_iter();
                        break None;
                    }
                },
                Some(Err(err)) => {
                    self.batch_size = 0;
                    break Some(Err(err));
                }
                None if self.batch_size == 0 => break None,
                None => {
                    let entries = match self.refill() {
                        Refill::Ok(entries, Some(key_start)) => {
                            self.key_start = Bound::Excluded(key_start);
                            entries
                        }
                        Refill::Ok(entries, None) => entries,
                        Refill::Retry(key, entries) => {
                            self.key_start = Bound::Excluded(key);
                            if entries.len() > 0 {
                                entries
                            } else {
                                continue;
                            }
                        }
                        Refill::Finish(entries) => {
                            self.batch_size = 0;
                            entries
                        }
                    };
                    self.last_batch = self.is_last_batch(&entries);
                    self.iter = entries.into_iter()
                }
            }
        }
    }
}

/// Iterator type, for continuous full table iteration filtering out older and
/// newer mutations.
pub struct FilterScans<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    iters_stack: Vec<I>,
    start: Bound<u64>,
    end: Bound<u64>,
    skip_filter: bool,
}

impl<K, V, I> FilterScans<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    pub fn new<G>(iters_stack: Vec<I>, within: G) -> FilterScans<K, V, I>
    where
        G: RangeBounds<u64>,
    {
        use std::ops::Bound::Unbounded;

        let (start, end, skip_filter) = match util::to_start_end(within) {
            (Unbounded, Unbounded) => (Unbounded, Unbounded, true),
            (start, end) => (start, end, false),
        };
        FilterScans {
            iters_stack,
            start,
            end,
            skip_filter,
        }
    }
}

impl<K, V, I> Iterator for FilterScans<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    type Item = Result<Entry<K, V>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let entry = match self.iters_stack.last_mut() {
                None => break None,
                Some(iter) => loop {
                    match iter.next() {
                        Some(Ok(e)) if self.skip_filter => break Some(Ok(e)),
                        Some(Ok(entry)) => {
                            let (s, e) = (self.start.clone(), self.end.clone());
                            match entry.filter_within(s, e) {
                                Some(entry) => break Some(Ok(entry)),
                                None => (),
                            }
                        }
                        Some(Err(err)) => break Some(Err(err)),
                        None => break None,
                    }
                },
            };

            match entry {
                Some(Ok(entry)) => break Some(Ok(entry)),
                Some(Err(err)) => {
                    self.iters_stack.drain(..);
                    break Some(Err(err));
                }
                None => {
                    self.iters_stack.pop();
                }
            }
        }
    }
}

/// Iterator type, to wrap full-table scanners and generate bitmap index.
///
/// Computes a bitmap of all keys that are iterated over the index `I`. The
/// bitmap type is parameterised as `B`.
pub struct BitmappedScan<K, V, I, B>
where
    K: Clone + Ord + Hash,
    V: Clone + Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
    B: Bloom,
{
    iter: I,
    bitmap: B,
}

impl<K, V, I, B> BitmappedScan<K, V, I, B>
where
    K: Clone + Ord + Hash,
    V: Clone + Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
    B: Bloom,
{
    pub fn new(iter: I) -> BitmappedScan<K, V, I, B> {
        BitmappedScan {
            iter,
            bitmap: <B as Bloom>::create(),
        }
    }

    pub fn close(self) -> Result<(I, B)> {
        Ok((self.iter, self.bitmap))
    }
}

impl<K, V, I, B> Iterator for BitmappedScan<K, V, I, B>
where
    K: Clone + Ord + Hash,
    V: Clone + Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
    B: Bloom,
{
    type Item = Result<Entry<K, V>>;

    #[inline]
    fn next(&mut self) -> Option<Result<Entry<K, V>>> {
        match self.iter.next() {
            Some(Ok(entry)) => {
                self.bitmap.add_key(entry.as_key());
                Some(Ok(entry))
            }
            Some(Err(err)) => Some(Err(err)),
            None => None,
        }
    }
}

/// Iterator type, for continuous full table iteration filtering out
/// older mutations.
pub struct CompactScan<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    iter: I,
    cutoff: Cutoff,
}

impl<K, V, I> CompactScan<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    pub fn new(iter: I, cutoff: Cutoff) -> CompactScan<K, V, I> {
        CompactScan { iter, cutoff }
    }

    pub fn close(self) -> Result<I> {
        Ok(self.iter)
    }
}

impl<K, V, I> Iterator for CompactScan<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    type Item = Result<Entry<K, V>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next() {
                Some(Ok(entry)) => match entry.purge(self.cutoff) {
                    Some(entry) => break Some(Ok(entry)),
                    None => (),
                },
                Some(Err(err)) => break Some(Err(err)),
                None => break None,
            }
        }
    }
}

/// Iterator type, to convert any iterator, or chain of iterators, into
/// CommitIterator trait. It can be used within [CommitIter][core::CommitIter].
///
/// This type assumes that source iterator already _knows_ the _within_
/// sequence-no range to filter out entries.
pub struct CommitWrapper<'a, K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    iters: Vec<IndexIter<'a, K, V>>,
}

impl<'a, K, V> CommitWrapper<'a, K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    pub fn new(iters: Vec<IndexIter<'a, K, V>>) -> CommitWrapper<'a, K, V> {
        CommitWrapper { iters: iters }
    }
}

impl<'a, K, V> CommitIterator<K, V> for CommitWrapper<'a, K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn scan<G>(&mut self, within: G) -> Result<IndexIter<K, V>>
    where
        G: RangeBounds<u64>,
    {
        let mut iters: Vec<IndexIter<'a, K, V>> = self.iters.drain(..).collect();
        iters.reverse();

        Ok(Box::new(FilterScans::new(iters, within)))
    }

    fn scans<G>(&mut self, n_shards: usize, _within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: RangeBounds<u64>,
    {
        let mut iters: Vec<IndexIter<K, V>> = self.iters.drain(..).collect();

        // If there are not enough shards push empty iterators.
        for _ in iters.len()..n_shards {
            let ss = vec![];
            iters.push(Box::new(ss.into_iter()));
        }

        assert_eq!(iters.len(), n_shards);

        Ok(iters)
    }

    fn range_scans<N, G>(&mut self, _: Vec<N>, _within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: RangeBounds<u64>,
        N: RangeBounds<K>,
    {
        Ok(self.iters.drain(..).collect())
    }
}

impl<K, V> CommitIterator<K, V> for std::vec::IntoIter<Result<Entry<K, V>>>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn scan<G>(&mut self, within: G) -> Result<IndexIter<K, V>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let entries: Vec<Result<Entry<K, V>>> = self.collect();
        let iters = vec![entries.into_iter()];
        Ok(Box::new(FilterScans::new(iters, within)))
    }

    fn scans<G>(&mut self, n_shards: usize, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let mut entries = vec![];
        for e in self {
            entries.push(e?)
        }

        let mut iters = vec![];
        for shard in util::as_sharded_array(&entries, n_shards).into_iter() {
            let iter: IndexIter<K, V> = {
                let iter = shard.to_vec().into_iter().map(|e| Ok(e)).into_iter();
                Box::new(FilterScans::new(vec![iter], within.clone()))
            };
            iters.push(iter)
        }

        // If there are not enough shards push empty iterators.
        for _ in iters.len()..n_shards {
            let ss = vec![];
            iters.push(Box::new(ss.into_iter()));
        }

        assert_eq!(iters.len(), n_shards);

        Ok(iters)
    }

    fn range_scans<N, G>(&mut self, ranges: Vec<N>, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
        N: Clone + RangeBounds<K>,
    {
        let mut entries = vec![];
        for e in self {
            entries.push(e?)
        }

        let mut iters = vec![];
        for shard in util::as_part_array(&entries, ranges).into_iter() {
            let iter: IndexIter<K, V> = {
                let iter = shard.into_iter().map(|e| Ok(e)).into_iter();
                Box::new(FilterScans::new(vec![iter], within.clone()))
            };

            iters.push(iter)
        }

        Ok(iters)
    }
}

#[cfg(test)]
#[path = "scans_test.rs"]
mod scans_test;
