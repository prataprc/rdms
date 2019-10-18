//! Stitch a full table scan from piece-wise iteration.
//!
//! Typically used for memory-only index.

use std::{
    ops::{Bound, RangeBounds},
    vec,
};

use crate::core::{Diff, Entry, FullScan, IndexIter, Result, ScanEntry};

/// SkipScan can be used to stitch piece-wise scanning of LSM
/// data-structure, only selecting mutations (and versions)
/// that are within specified sequence-no range.
///
/// Mitigates following issues.
///
/// a. Read references to data-structure is held only for
///    very small period, like few tens of micro-seconds.
/// b. Automatically filters mutations that are older than
///    specified sequence-no range, there by saving time for
///    top-level DB components.
/// c. Ignores mutations that are newer than the specified
///    sequence-no range, there by providing a stable full
///    table scan.
///
/// Important pre-requist:
///
/// a. Applicable only for LSM based data structures.
/// b. Data-structure must not suffer any delete/purge
///    operation until full-scan is completed.
/// c. Data-structure must implement FullScan trait.
pub(crate) struct SkipScan<'a, M, K, V, G>
where
    K: 'a + Clone + Ord,
    V: 'a + Clone + Diff + From<<V as Diff>::D>,
    G: Clone + RangeBounds<u64>,
    M: FullScan<K, V>,
{
    index: &'a M,   // read reference to index.
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

impl<'a, M, K, V, G> SkipScan<'a, M, K, V, G>
where
    K: 'a + Clone + Ord,
    V: 'a + Clone + Diff + From<<V as Diff>::D>,
    G: Clone + RangeBounds<u64>,
    M: FullScan<K, V>,
{
    const BATCH_SIZE: usize = 1000;

    pub(crate) fn new(index: &'a M, within: G) -> SkipScan<M, K, V, G> {
        SkipScan {
            index,
            within,
            from: Bound::Unbounded,
            iter: vec![].into_iter(),
            batch_size: Self::BATCH_SIZE,
        }
    }

    pub(crate) fn set_batch_size(&mut self, batch_size: usize) {
        self.batch_size = batch_size
    }

    fn refill(&mut self) -> Refill<K, V> {
        let from = self.from.clone();
        let mut entries: Vec<Result<Entry<K, V>>> = vec![];
        match self.index.full_scan(from, self.within.clone()) {
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

impl<'a, M, K, V, G> Iterator for SkipScan<'a, M, K, V, G>
where
    K: 'a + Clone + Ord,
    V: 'a + Clone + Diff + From<<V as Diff>::D>,
    G: Clone + RangeBounds<u64>,
    M: FullScan<K, V>,
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

pub struct FilterScan<'a, K, V>
where
    K: 'a + Clone + Ord,
    V: 'a + Clone + Diff + From<<V as Diff>::D>,
{
    iter: IndexIter<'a, K, V>,
    start: Bound<u64>,
    end: Bound<u64>,
}

impl<'a, K, V> FilterScan<'a, K, V>
where
    K: 'a + Clone + Ord,
    V: 'a + Clone + Diff + From<<V as Diff>::D>,
{
    pub fn new<R>(iter: IndexIter<'a, K, V>, within: R) -> FilterScan<'a, K, V>
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

impl<'a, K, V> Iterator for FilterScan<'a, K, V>
where
    K: 'a + Clone + Ord,
    V: 'a + Clone + Diff + From<<V as Diff>::D>,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next() {
                Some(Ok(entry)) => {
                    let (start, end) = (self.start.clone(), self.end.clone());
                    match entry.filter_within(start, end) {
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

#[cfg(test)]
#[path = "scans_test.rs"]
mod scans_test;
