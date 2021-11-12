use std::{
    cmp,
    convert::{TryFrom, TryInto},
    fmt, hash, marker, time,
};

use crate::{db, robt, Error, Result};

// BuildScan, BitmappedScan, CompactScan

// Iterator wrapper, to wrap full-table scanners and count seqno,
// index-items, deleted items and epoch.
pub struct BuildScan<K, V, I, E>
where
    V: db::Diff,
    I: Iterator<Item = Result<E>>,
    E: TryInto<robt::Entry<K, V>>,
{
    iter: I,

    start: time::SystemTime,
    seqno: u64,
    n_count: u64,
    n_deleted: u64,

    _key: marker::PhantomData<K>,
    _val: marker::PhantomData<V>,
}

impl<K, V, I, E> BuildScan<K, V, I, E>
where
    V: db::Diff,
    I: Iterator<Item = Result<E>>,
    E: TryInto<robt::Entry<K, V>>,
{
    pub fn new(iter: I, seqno: u64) -> BuildScan<K, V, I, E> {
        BuildScan {
            iter,

            start: time::SystemTime::now(),
            seqno,
            n_count: u64::default(),
            n_deleted: u64::default(),

            _key: marker::PhantomData,
            _val: marker::PhantomData,
        }
    }

    // return (build_time, seqno, count, deleted, epoch, iter)
    pub fn unwrap(self) -> Result<(u64, u64, u64, u64, u64, I)> {
        let build_time = {
            let elapsed = err_at!(Fatal, self.start.elapsed())?;
            err_at!(FailConvert, u64::try_from(elapsed.as_nanos()))?
        };
        let epoch = {
            let elapsed = err_at!(Fatal, time::UNIX_EPOCH.elapsed())?;
            err_at!(FailConvert, u64::try_from(elapsed.as_nanos()))?
        };

        let rets = (
            build_time,
            self.seqno,
            self.n_count,
            self.n_deleted,
            epoch,
            self.iter,
        );

        Ok(rets)
    }
}

impl<K, V, I, E> Iterator for BuildScan<K, V, I, E>
where
    V: db::Diff,
    I: Iterator<Item = Result<E>>,
    E: TryInto<robt::Entry<K, V>>,
    <E as TryInto<robt::Entry<K, V>>>::Error: fmt::Display,
{
    type Item = Result<robt::Entry<K, V>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next()? {
            Ok(e) => match e.try_into() {
                Ok(entry) => {
                    self.seqno = cmp::max(self.seqno, entry.to_seqno().unwrap());
                    self.n_count += 1;
                    if entry.is_deleted().unwrap() {
                        self.n_deleted += 1;
                    }
                    Some(Ok(entry))
                }
                Err(err) => Some(err_at!(FailConvert, Err(err))),
            },
            Err(err) => Some(Err(err)),
        }
    }
}

// Iterator wrapper, to wrap full-table scanners and generate bitmap index.
//
// Computes a bitmap of all keys iterated over the index `I`. Bitmap type
// is parameterised as `B`.
pub struct BitmappedScan<K, V, B, I>
where
    V: db::Diff,
    I: Iterator<Item = Result<robt::Entry<K, V>>>,
{
    iter: I,
    bitmap: B,
    _key: marker::PhantomData<K>,
    _val: marker::PhantomData<V>,
}

impl<K, V, B, I> BitmappedScan<K, V, B, I>
where
    V: db::Diff,
    B: db::Bloom,
    I: Iterator<Item = Result<robt::Entry<K, V>>>,
{
    pub fn new(iter: I, bitmap: B) -> BitmappedScan<K, V, B, I> {
        BitmappedScan {
            iter,
            bitmap,
            _key: marker::PhantomData,
            _val: marker::PhantomData,
        }
    }

    pub fn unwrap(mut self) -> Result<(B, I)> {
        self.bitmap.build()?;
        Ok((self.bitmap, self.iter))
    }
}

impl<K, V, B, I> Iterator for BitmappedScan<K, V, B, I>
where
    K: hash::Hash,
    V: db::Diff,
    B: db::Bloom,
    I: Iterator<Item = Result<robt::Entry<K, V>>>,
{
    type Item = Result<robt::Entry<K, V>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next()? {
            Ok(entry) => {
                self.bitmap.add_key(entry.as_key());
                Some(Ok(entry))
            }
            Err(err) => Some(Err(err)),
        }
    }
}

// Iterator type, for continuous full table iteration filtering out
// older mutations.
pub struct CompactScan<K, V, I>
where
    V: db::Diff,
{
    iter: I,
    cutoff: db::Cutoff,

    _key: marker::PhantomData<K>,
    _val: marker::PhantomData<V>,
}

impl<K, V, I> CompactScan<K, V, I>
where
    V: db::Diff,
{
    pub fn new(iter: I, cutoff: db::Cutoff) -> Self {
        CompactScan {
            iter,
            cutoff,

            _key: marker::PhantomData,
            _val: marker::PhantomData,
        }
    }
}

impl<K, V, I> Iterator for CompactScan<K, V, I>
where
    K: Clone,
    V: db::Diff,
    I: Iterator<Item = Result<db::Entry<K, V>>>,
{
    type Item = Result<db::Entry<K, V>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next()? {
                Ok(entry) => {
                    if let Some(entry) = entry.compact(self.cutoff) {
                        break Some(Ok(entry));
                    }
                }
                Err(err) => break Some(Err(err)),
            }
        }
    }
}

#[cfg(test)]
#[path = "scans_test.rs"]
mod scans_test;
