//! Stitch a full table scan from piece-wise iteration.
//!
//! Typically used for memory-only index.

use std::ops::{Bound, RangeBounds};
use std::vec;

use crate::core::{Diff, Entry, FullScan, Result};

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
        match self.iter.next() {
            Some(Ok(entry)) => {
                self.from = Bound::Excluded(entry.to_key());
                Some(Ok(entry))
            }
            Some(Err(err)) => {
                self.batch_size = 0;
                Some(Err(err))
            }
            None if self.batch_size == 0 => None,
            None => {
                let from = self.from.clone();
                match self.index.full_scan(from, self.within.clone()) {
                    Ok(iter) => {
                        let mut entries: Vec<Result<Entry<K, V>>> = vec![];
                        for (i, item) in iter.enumerate() {
                            if i >= self.batch_size || item.is_err() {
                                entries.push(item);
                                break;
                            }
                            entries.push(item);
                        }
                        self.iter = entries.into_iter();
                        return self.iter.next();
                    }
                    Err(err) => {
                        self.batch_size = 0;
                        Some(Err(err))
                    }
                }
            }
        }
    }
}
