//! Implement get() and iter() for LSM indexes.
use std::{borrow::Borrow, cmp};

use crate::{
    core::{Diff, Entry, Footprint, IndexIter, Reader, Result},
    error::Error,
};

#[allow(dead_code)] // TODO: remove if not required.
pub(crate) type LsmGet<'a, K, V, Q> = Box<dyn FnMut(&Q) -> Result<Entry<K, V>> + 'a>;

// ``x`` contains newer mutations than ``y``, get always fetches the latest
// entry from the newest index.
#[allow(dead_code)] // TODO: remove if not required.
pub(crate) fn y_get<'a, 'b, K, V, Q>(
    mut x: LsmGet<'a, K, V, Q>,
    mut y: LsmGet<'a, K, V, Q>,
) -> LsmGet<'a, K, V, Q>
where
    K: 'static + Clone + Ord + Borrow<Q>,
    V: 'static + Clone + Diff,
    Q: 'a + 'b + Ord + ?Sized,
{
    Box::new(move |key: &Q| -> Result<Entry<K, V>> {
        match x(key) {
            Ok(entry) => Ok(entry),
            Err(Error::KeyNotFound) => y(key),
            Err(err) => Err(err),
        }
    })
}

// ``x`` contains newer mutations than ``y``.
// TODO NOTE: xmerge called by this function assumes that all
// mutations held by each index are mutually exclusive.
#[allow(dead_code)] // TODO: remove if not required.
pub(crate) fn y_get_versions<'a, 'b, K, V, Q>(
    mut x: LsmGet<'a, K, V, Q>,
    mut y: LsmGet<'a, K, V, Q>,
) -> LsmGet<'a, K, V, Q>
where
    K: 'static + Clone + Ord + Footprint + Borrow<Q>,
    V: 'static + Clone + Diff + Footprint,
    Q: 'a + 'b + Ord + ?Sized,
{
    Box::new(move |key: &Q| -> Result<Entry<K, V>> {
        match y(key) {
            Ok(y_entry) => match x(key) {
                Ok(x_entry) => Ok(x_entry.xmerge(y_entry)),
                Err(Error::KeyNotFound) => Ok(y_entry),
                res => res,
            },
            Err(Error::KeyNotFound) => x(key),
            res => res,
        }
    })
}

// ``x`` contains newer mutations than ``y``
pub(crate) fn y_iter<'a, K, V>(
    mut x: IndexIter<'a, K, V>, // newer
    mut y: IndexIter<'a, K, V>, // older
    reverse: bool,
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
        reverse,
    })
}

pub(crate) fn y_iter_versions<'a, K, V>(
    mut x: IndexIter<'a, K, V>, // newer
    mut y: IndexIter<'a, K, V>, // older
    reverse: bool,
) -> IndexIter<'a, K, V>
where
    K: 'a + Clone + Ord + Footprint,
    V: 'a + Clone + Diff + Footprint,
{
    let x_entry = x.next();
    let y_entry = y.next();
    Box::new(YIterVersions {
        x,
        y,
        x_entry,
        y_entry,
        reverse,
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
    reverse: bool,
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
                // println!("yiter next xe {:?} {}", xe.to_key(), xe.to_seqno());
                // println!("yiter next ye {:?} {}", ye.to_key(), ye.to_seqno());
                let cmp = if self.reverse {
                    xe.as_key().cmp(ye.as_key()).reverse()
                } else {
                    xe.as_key().cmp(ye.as_key())
                };
                match cmp {
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
                    cmp::Ordering::Equal => {
                        self.x_entry = self.x.next();
                        self.y_entry = self.y.next();
                        match xe.to_seqno().cmp(&ye.to_seqno()) {
                            cmp::Ordering::Less => Some(Ok(ye)),
                            cmp::Ordering::Greater => Some(Ok(xe)),
                            cmp::Ordering::Equal => Some(Ok(xe)),
                        }
                    }
                }
            }
            (Some(Ok(xe)), None) => {
                self.x_entry = self.x.next();
                Some(Ok(xe))
            }
            (None, Some(Ok(ye))) => {
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
    V: 'a + Clone + Diff,
{
    x: IndexIter<'a, K, V>,
    y: IndexIter<'a, K, V>,
    x_entry: Option<Result<Entry<K, V>>>,
    y_entry: Option<Result<Entry<K, V>>>,
    reverse: bool,
}

impl<'a, K, V> Iterator for YIterVersions<'a, K, V>
where
    K: 'a + Clone + Ord + Footprint,
    V: 'a + Clone + Diff + Footprint,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.x_entry.take(), self.y_entry.take()) {
            (Some(Ok(xe)), Some(Ok(ye))) => {
                // println!("yiter next xe {:?} {}", xe.to_key(), xe.to_seqno());
                // println!("yiter next ye {:?} {}", ye.to_key(), ye.to_seqno());
                let cmp = if self.reverse {
                    xe.as_key().cmp(ye.as_key()).reverse()
                } else {
                    xe.as_key().cmp(ye.as_key())
                };
                match cmp {
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
                    cmp::Ordering::Equal => {
                        // TODO NOTE: xmerge assumes that all mutations
                        // held by each index are mutually exclusive.
                        self.x_entry = self.x.next();
                        self.y_entry = self.y.next();
                        Some(Ok(xe.xmerge(ye)))
                    }
                }
            }
            (Some(Ok(xe)), None) => {
                self.x_entry = self.x.next();
                Some(Ok(xe))
            }
            (None, Some(Ok(ye))) => {
                self.y_entry = self.y.next();
                Some(Ok(ye))
            }
            (Some(Ok(_xe)), Some(Err(err))) => Some(Err(err)),
            (Some(Err(err)), Some(Ok(_ye))) => Some(Err(err)),
            _ => None,
        }
    }
}

#[allow(dead_code)] // TODO: remove if not required.
pub(crate) fn getter<'a, 'b, I, K, V, Q>(index: &'a mut I, versions: bool) -> LsmGet<'a, K, V, Q>
where
    K: Clone + Ord + Borrow<Q>,
    V: Clone + Diff,
    Q: 'b + Ord + ?Sized,
    I: Reader<K, V>,
{
    if versions {
        Box::new(move |key: &Q| -> Result<Entry<K, V>> { index.get_with_versions(key) })
    } else {
        Box::new(move |key: &Q| -> Result<Entry<K, V>> { index.get(key) })
    }
}

#[cfg(test)]
#[path = "lsm_test.rs"]
mod lsm_test;
