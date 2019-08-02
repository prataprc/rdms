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
    iter: vec::IntoIter<Entry<K, V>>,
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

    fn new(index: &'a I, within: G) -> SkipScan<I, K, V, G> {
        SkipScan {
            index,
            within,
            lower: Bound::Unbounded,
            iter: vec![].into_iter(),
            batch_size: Self::BATCH_SIZE,
        }
    }

    fn set_batch_size(&mut self, batch_size: usize) {
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
        if let Some(entry) = self.iter.next() {
            self.lower = Bound::Excluded(entry.to_key());
            return Some(Ok(entry));
        }
        let range = (self.lower.clone(), Bound::Unbounded);
        match self.index.iter_within(range, self.within.clone()) {
            Ok(iter) => {
                let es: Vec<Entry<K, V>> = iter.take(self.batch_size).collect();
                self.iter = es.into_iter();
                match self.iter.next() {
                    None => None,
                    Some(entry) => Some(Ok(entry)),
                }
            }
            Err(err) => Some(Err(err)),
        }
    }
}
