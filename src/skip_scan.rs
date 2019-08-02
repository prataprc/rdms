//! Stitch a full table scan from piece-wise iteration.
use std::ops::{Bound, RangeBounds};
use std::vec;

use crate::core::{Diff, Entry, Index, Result};

pub struct SkipScan<'a, I, K, V, G>
where
    I: Index<K, V>,
    K: Clone + Ord,
    V: Clone + Diff,
    G: Clone + RangeBounds<u64>,
{
    index: &'a I,
    within: G,
    lower: Bound<K>,
    iter: vec::IntoIter<Result<Entry<K, V>>>,
    batch_size: usize,
}

impl<'a, I, K, V, G> SkipScan<'a, I, K, V, G>
where
    I: Index<K, V>,
    K: Clone + Ord,
    V: Clone + Diff,
    G: Clone + RangeBounds<u64>,
{
    const BATCH_SIZE: usize = 1000;

    pub fn new(index: &'a I, within: G) -> SkipScan<I, K, V, G> {
        SkipScan {
            index,
            within,
            lower: Bound::Unbounded,
            iter: vec![].into_iter(),
            batch_size: Self::BATCH_SIZE,
        }
    }

    pub fn set_batch_size(&mut self, batch_size: usize) {
        self.batch_size = batch_size
    }
}

impl<'a, I, K, V, G> Iterator for SkipScan<'a, I, K, V, G>
where
    I: Index<K, V>,
    K: Clone + Ord,
    V: Clone + Diff,
    G: Clone + RangeBounds<u64>,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok(entry)) => {
                self.lower = Bound::Excluded(entry.to_key());
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
                let range = (self.lower.clone(), Bound::Unbounded);
                match self.index.iter_within(range, self.within.clone()) {
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
