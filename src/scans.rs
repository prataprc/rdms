//! Stitch a full table scan from piece-wise iteration.
//!
//! Typically used for memory-only index.

use std::ops::{Bound, RangeBounds};
use std::vec;

use crate::core::{Diff, Entry, FullScan, Result};

pub struct SkipScan<'a, M, K, V, G>
where
    K: 'a + Clone + Ord,
    V: 'a + Clone + Diff + From<<V as Diff>::D>,
    G: Clone + RangeBounds<u64>,
    M: FullScan<K, V>,
{
    index: &'a M,
    within: G,
    from: Bound<K>,
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

    pub fn new(index: &'a M, within: G) -> SkipScan<M, K, V, G> {
        SkipScan {
            index,
            within,
            from: Bound::Unbounded,
            iter: vec![].into_iter(),
            batch_size: Self::BATCH_SIZE,
        }
    }

    pub fn set_batch_size(&mut self, batch_size: usize) {
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
            None => {
                if self.batch_size == 0 {
                    return None;
                }
                let from = self.from.clone();
                match self.index.full_scan(from, self.within.clone()) {
                    Ok(iter) => {
                        let mut entries: Vec<Result<Entry<K, V>>> = vec![];
                        for (i, item) in iter.enumerate() {
                            if i >= self.batch_size {
                                break;
                            } else if item.is_err() {
                                entries.push(item);
                                self.batch_size = 0;
                                break;
                            }
                            entries.push(item);
                        }
                        self.iter = entries.into_iter();
                        return self.iter.next();
                    }
                    Err(err) => Some(Err(err)),
                }
            }
        }
    }
}
