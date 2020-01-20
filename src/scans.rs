//! Module `scans` implement iterator variants that are useful for
//! building and managing complex data-index.

use std::{
    hash::Hash,
    marker,
    ops::{Bound, RangeBounds},
    vec,
};

use crate::{
    core::{Bloom, CommitIterator, Diff, Entry, IndexIter, PiecewiseScan, Result, ScanEntry},
    error::Error,
    util,
};

// TODO: benchmark SkipScan and FilterScans and measure the difference.

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
    pub fn set_batch_size(&mut self, batch_size: usize) -> &mut Self {
        self.batch_size = batch_size;
        self
    }

    /// Set seqno range to filter out all mutations outside the range.
    pub fn set_seqno_range<G>(&mut self, within: G) -> &mut Self
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
        self
    }

    /// Set key range to filter out all keys outside the range.
    pub fn set_key_range<G>(&mut self, range: G) -> &mut Self
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
        self
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

/// FilterScans for continuous full table iteration filtering out older and
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
        let (start, end) = util::to_start_end(within);
        FilterScans {
            iters_stack,
            start,
            end,
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
                        Some(Ok(entry)) => {
                            match entry.filter_within(self.start.clone(), self.end.clone()) {
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

/// BitmappedScan wrapper for full-table scanners.
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

/// CompactScan for continuous full table iteration filtering out
/// older mutations.
pub struct CompactScan<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    iter: I,
    cutoff: Bound<u64>,
}

impl<K, V, I> CompactScan<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    pub fn new(iter: I, cutoff: Bound<u64>) -> CompactScan<K, V, I> {
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

pub struct IterChain<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    iter: Option<I>,
    iters: Vec<I>,

    _phantom_key: marker::PhantomData<K>,
    _phantom_val: marker::PhantomData<V>,
}

impl<K, V, I> IterChain<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    pub fn new(iters: Vec<I>) -> IterChain<K, V, I> {
        IterChain {
            iter: None,
            iters: iters,

            _phantom_key: marker::PhantomData,
            _phantom_val: marker::PhantomData,
        }
    }
}

impl<K, V, I> Iterator for IterChain<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.iter {
            Some(iter) => match iter.next() {
                Some(item) => Some(item),
                None => {
                    self.iter = None;
                    self.next()
                }
            },
            None if self.iters.len() == 0 => None,
            None => {
                self.iter = Some(self.iters.remove(0));
                self.next()
            }
        }
    }
}

// TODO: right now CommitWrapper ignores the `within`,
// should we make this optional ??

pub struct CommitWrapper<'a, K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    iter: Option<IndexIter<'a, K, V>>,
}

impl<'a, K, V> CommitWrapper<'a, K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    pub fn new(iter: IndexIter<'a, K, V>) -> CommitWrapper<'a, K, V> {
        CommitWrapper { iter: Some(iter) }
    }
}

impl<'a, K, V> CommitIterator<K, V> for CommitWrapper<'a, K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn scan<G>(&mut self, _within: G) -> Result<IndexIter<K, V>>
    where
        G: RangeBounds<u64>,
    {
        match self.iter.take() {
            Some(iter) => Ok(iter),
            None => {
                let msg = format!("CommitIterator::scan() malformed");
                Err(Error::UnInitialized(msg))
            }
        }
    }

    fn scans<G>(&mut self, _: usize, _within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: RangeBounds<u64>,
    {
        match self.iter.take() {
            Some(iter) => Ok(vec![iter]),
            None => {
                let msg = format!("CommitIterator::scans() malformed");
                Err(Error::UnInitialized(msg))
            }
        }
    }

    fn range_scans<N, G>(&mut self, _: Vec<N>, _within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: RangeBounds<u64>,
        N: RangeBounds<K>,
    {
        match self.iter.take() {
            Some(iter) => Ok(vec![iter]),
            None => {
                let msg = format!("CommitIterator::range_scans() malformed");
                Err(Error::UnInitialized(msg))
            }
        }
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

    fn scans<G>(&mut self, shards: usize, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let mut entries = vec![];
        for e in self {
            entries.push(e?)
        }

        let mut iters = vec![];
        for shard in util::as_sharded_array(&entries, shards).into_iter() {
            let iter: IndexIter<K, V> = {
                let iter = shard.to_vec().into_iter().map(|e| Ok(e)).into_iter();
                Box::new(FilterScans::new(vec![iter], within.clone()))
            };
            iters.push(iter)
        }

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
