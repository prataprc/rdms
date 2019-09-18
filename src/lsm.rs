//! Implement get() and iter() for LSM indexes.
use std::cmp;

use crate::core::{Diff, Entry, Footprint, IndexIter, Reader, Result};
use crate::error::Error;

// TODO: Due to some complex lifetime conflicts, we cannot implement
// ``get(&Q)`` interface for LsmGet.

pub(crate) type LsmGet<'a, K, V> = Box<dyn Fn(&K) -> Result<Entry<K, V>> + 'a>;

// ``x`` contains newer mutations than ``y``
pub(crate) fn y_get<'a, K, V>(x: LsmGet<'a, K, V>, y: LsmGet<'a, K, V>) -> LsmGet<'a, K, V>
where
    K: 'static + Clone + Ord,
    V: 'static + Clone + Diff,
{
    Box::new(move |key: &K| -> Result<Entry<K, V>> {
        match x(key) {
            Ok(entry) => Ok(entry),
            Err(Error::KeyNotFound) => y(key),
            Err(err) => Err(err),
        }
    })
}

// ``x`` contains newer mutations than ``y``
pub(crate) fn y_get_versions<'a, K, V>(x: LsmGet<'a, K, V>, y: LsmGet<'a, K, V>) -> LsmGet<'a, K, V>
where
    K: 'static + Clone + Ord,
    V: 'static + Clone + Diff + From<<V as Diff>::D> + Footprint,
{
    Box::new(move |key: &K| -> Result<Entry<K, V>> {
        match x(key) {
            Ok(x_entry) => match y(key) {
                Ok(y_entry) => Ok(x_entry.flush_merge(y_entry)),
                res => res,
            },
            Err(Error::KeyNotFound) => y(key),
            res => res,
        }
    })
}

pub(crate) fn y_iter<'a, K, V>(
    mut x: IndexIter<'a, K, V>, // newer
    mut y: IndexIter<'a, K, V>, // older
) -> IndexIter<'a, K, V>
where
    K: 'a + Clone + Ord,
    V: 'a + Clone + Diff,
{
    let x_entry = x.next();
    let y_entry = y.next();
    Box::new(YIter {
        x,
        y,
        x_entry,
        y_entry,
    })
}

pub(crate) fn y_iter_versions<'a, K, V>(
    mut x: IndexIter<'a, K, V>, // newer
    mut y: IndexIter<'a, K, V>, // older
) -> IndexIter<'a, K, V>
where
    K: 'a + Clone + Ord,
    V: 'a + Clone + Diff + From<<V as Diff>::D> + Footprint,
{
    let x_entry = x.next();
    let y_entry = y.next();
    Box::new(YIterVersions {
        x,
        y,
        x_entry,
        y_entry,
    })
}

struct YIter<'a, K, V>
where
    K: 'a + Clone + Ord,
    V: 'a + Clone + Diff,
{
    x: IndexIter<'a, K, V>,
    y: IndexIter<'a, K, V>,
    x_entry: Option<Result<Entry<K, V>>>,
    y_entry: Option<Result<Entry<K, V>>>,
}

impl<'a, K, V> Iterator for YIter<'a, K, V>
where
    K: 'a + Clone + Ord,
    V: 'a + Clone + Diff,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.x_entry.take(), self.y_entry.take()) {
            (Some(Ok(xe)), Some(Ok(ye))) => {
                let mut c = xe.as_key().cmp(ye.as_key());
                if c == cmp::Ordering::Equal {
                    c = xe.to_seqno().cmp(&ye.to_seqno());
                }
                match c {
                    cmp::Ordering::Less => {
                        self.x_entry = self.x.next();
                        self.y_entry = Some(Ok(ye));
                        Some(Ok(xe))
                    }
                    cmp::Ordering::Greater => {
                        self.y_entry = self.y.next();
                        self.x_entry = Some(Ok(xe));
                        Some(Ok(ye))
                    }
                    cmp::Ordering::Equal => unreachable!(),
                }
            }
            (Some(Ok(xe)), Some(Err(Error::KeyNotFound))) => {
                self.x_entry = self.x.next();
                Some(Ok(xe))
            }
            (Some(Err(Error::KeyNotFound)), Some(Ok(ye))) => {
                self.y_entry = self.y.next();
                Some(Ok(ye))
            }
            (Some(Ok(_xe)), Some(Err(err))) => Some(Err(err)),
            (Some(Err(err)), Some(Ok(_ye))) => Some(Err(err)),
            _ => None,
        }
    }
}

struct YIterVersions<'a, K, V>
where
    K: 'a + Clone + Ord,
    V: 'a + Clone + Diff + From<<V as Diff>::D> + Footprint,
{
    x: IndexIter<'a, K, V>,
    y: IndexIter<'a, K, V>,
    x_entry: Option<Result<Entry<K, V>>>,
    y_entry: Option<Result<Entry<K, V>>>,
}

impl<'a, K, V> Iterator for YIterVersions<'a, K, V>
where
    K: 'a + Clone + Ord,
    V: 'a + Clone + Diff + From<<V as Diff>::D> + Footprint,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.x_entry.take(), self.y_entry.take()) {
            (Some(Ok(xe)), Some(Ok(ye))) => match xe.as_key().cmp(ye.as_key()) {
                cmp::Ordering::Less => {
                    self.x_entry = self.x.next();
                    Some(Ok(xe))
                }
                cmp::Ordering::Greater => {
                    self.y_entry = self.y.next();
                    Some(Ok(ye))
                }
                cmp::Ordering::Equal => {
                    self.x_entry = self.x.next();
                    self.y_entry = self.y.next();
                    Some(Ok(xe.flush_merge(ye)))
                }
            },
            (Some(Ok(xe)), Some(Err(Error::KeyNotFound))) => {
                self.x_entry = self.x.next();
                Some(Ok(xe))
            }
            (Some(Err(Error::KeyNotFound)), Some(Ok(ye))) => {
                self.y_entry = self.y.next();
                Some(Ok(ye))
            }
            (Some(Ok(_xe)), Some(Err(err))) => Some(Err(err)),
            (Some(Err(err)), Some(Ok(_ye))) => Some(Err(err)),
            _ => None,
        }
    }
}

pub(crate) fn getter<'a, I, K, V>(index: &'a I) -> LsmGet<'a, K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Reader<K, V>,
{
    Box::new(move |key: &K| -> Result<Entry<K, V>> { index.get(key) })
}

#[cfg(test)]
#[path = "lsm_test.rs"]
mod lsm_test;
