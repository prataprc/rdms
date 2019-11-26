//! Module `scans` implement iterator variants that are useful for
//! building and managing complex data-index.

use std::{
    hash::Hash,
    ops::{Bound, RangeBounds},
    vec,
};

use crate::core::{Bloom, Diff, Entry, PiecewiseScan, Result, ScanEntry};

// TODO: benchmark SkipScan and FilterScan and measure the difference.

const SKIP_SCAN_BATCH_SIZE: usize = 1000;

/// SkipScan for full table iteration of LSM data structure.
///
/// SkipScan achieve full table scan by stitching together piece-wise
/// scan of LSM data-structure, only selecting mutations (and versions)
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
pub struct SkipScan<R, K, V, G>
where
    K: Clone + Ord,
    V: Clone + Diff,
    G: Clone + RangeBounds<u64>,
    R: PiecewiseScan<K, V>,
{
    reader: R,      // reader handle into index
    within: G,      // pick mutations withing this sequence-no range.
    from: Bound<K>, // place to start the next batch of scan.
    iter: vec::IntoIter<Result<Entry<K, V>>>,
    batch_size: usize,
}

enum Refill<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    Ok(Vec<Result<Entry<K, V>>>),
    Retry(K, Vec<Result<Entry<K, V>>>),
    Finish(Vec<Result<Entry<K, V>>>),
}

impl<R, K, V, G> SkipScan<R, K, V, G>
where
    K: Clone + Ord,
    V: Clone + Diff,
    G: Clone + RangeBounds<u64>,
    R: PiecewiseScan<K, V>,
{
    /// Create a new full table scan using the reader handle. Pick
    /// mutations that are `within` the specified range.
    pub fn new(reader: R, within: G) -> SkipScan<R, K, V, G> {
        SkipScan {
            reader,
            within,
            from: Bound::Unbounded,
            iter: vec![].into_iter(),
            batch_size: SKIP_SCAN_BATCH_SIZE,
        }
    }

    /// Set the batch size for each iteration using the reader handle.
    pub fn set_batch_size(&mut self, batch_size: usize) {
        self.batch_size = batch_size
    }

    fn refill(&mut self) -> Refill<K, V> {
        let mut entries: Vec<Result<Entry<K, V>>> = vec![];
        match self.reader.pw_scan(self.from.clone(), self.within.clone()) {
            Ok(niter) => {
                let mut niter = niter.enumerate();
                loop {
                    match niter.next() {
                        Some((i, Ok(ScanEntry::Found(entry)))) => {
                            entries.push(Ok(entry));
                            if i >= self.batch_size {
                                break Refill::Ok(entries);
                            }
                        }
                        Some((_, Ok(ScanEntry::Retry(key)))) => {
                            break Refill::Retry(key, entries);
                        }
                        Some((_, Err(err))) => {
                            entries.push(Err(err));
                            break Refill::Ok(entries);
                        }
                        None => break Refill::Finish(entries),
                    }
                }
            }
            Err(err) => {
                entries.push(Err(err));
                Refill::Ok(entries)
            }
        }
    }
}

impl<R, K, V, G> Iterator for SkipScan<R, K, V, G>
where
    K: Clone + Ord,
    V: Clone + Diff,
    G: Clone + RangeBounds<u64>,
    R: PiecewiseScan<K, V>,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next() {
                Some(Ok(entry)) => {
                    self.from = Bound::Excluded(entry.to_key());
                    break Some(Ok(entry));
                }
                Some(Err(err)) => {
                    self.batch_size = 0;
                    break Some(Err(err));
                }
                None if self.batch_size == 0 => break None,
                None => {
                    let entries = match self.refill() {
                        Refill::Ok(entries) => entries,
                        Refill::Retry(key, entries) => {
                            self.from = Bound::Excluded(key);
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
                    self.iter = entries.into_iter()
                }
            }
        }
    }
}

/// FilterScan for continuous full table iteration filtering out older and
/// newer mutations.
pub struct FilterScan<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    iter: I,
    start: Bound<u64>,
    end: Bound<u64>,
}

impl<K, V, I> FilterScan<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    pub fn new<R>(iter: I, within: R) -> FilterScan<K, V, I>
    where
        R: RangeBounds<u64>,
    {
        let start = match within.start_bound() {
            Bound::Included(start) => Bound::Included(*start),
            Bound::Excluded(start) => Bound::Excluded(*start),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end = match within.end_bound() {
            Bound::Included(end) => Bound::Included(*end),
            Bound::Excluded(end) => Bound::Excluded(*end),
            Bound::Unbounded => Bound::Unbounded,
        };
        FilterScan { iter, start, end }
    }
}

impl<K, V, I> Iterator for FilterScan<K, V, I>
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
                Some(Ok(entry)) => {
                    match entry.filter_within(self.start.clone(), self.end.clone()) {
                        Some(entry) => break Some(Ok(entry)),
                        None => (),
                    }
                }
                Some(Err(err)) => break Some(Err(err)),
                None => break None,
            }
        }
    }
}

pub(crate) struct BitmapIter<K, V, I, B>
where
    K: Clone + Ord + Hash,
    V: Clone + Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
    B: Bloom,
{
    iter: I,
    bitmap: B,
}

impl<K, V, I, B> BitmapIter<K, V, I, B>
where
    K: Clone + Ord + Hash,
    V: Clone + Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
    B: Bloom,
{
    pub(crate) fn new(iter: I) -> BitmapIter<K, V, I, B> {
        BitmapIter {
            iter,
            bitmap: <B as Bloom>::create(),
        }
    }

    pub(crate) fn close(self) -> Result<(I, B)> {
        Ok((self.iter, self.bitmap))
    }
}

impl<K, V, I, B> Iterator for BitmapIter<K, V, I, B>
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

/// CompactIter for continuous full table iteration filtering out
/// older mutations.
pub struct CompactIter<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    iter: I,
    cutoff: Bound<u64>,
}

impl<K, V, I> CompactIter<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    pub fn new(iter: I, cutoff: Bound<u64>) -> CompactIter<K, V, I> {
        CompactIter { iter, cutoff }
    }
}

impl<K, V, I> Iterator for CompactIter<K, V, I>
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

#[cfg(test)]
#[path = "scans_test.rs"]
mod scans_test;
