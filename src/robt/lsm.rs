//! Module `lsm` implement read API across LSM snapshots of
//! single index instance.

use std::{borrow::Borrow, cmp, hash::Hash};

use crate::{
    core::{Diff, Entry, Footprint, IndexIter, Reader, Result},
    error::Error,
};

pub struct YIter<'a, K, V, I, E>
where
    V: db::Diff<Delta = D>,
    I: Iterator<Item = E>,
    E: Into<Entry<K, V>>,
{
    snap: I,
    iter: Iter<'a, K, V>,
    s_entry: Option<Result<Entry<K, V>>>,
    i_entry: Option<Result<Entry<K, V>>>,
}

impl<'a, K, V, I, E> YIter<'a, K, V, I, E>
where
    V: db::Diff<Delta = D>,
    I: Iterator<Item = E>,
    E: Into<Entry<K, V>>,
{
    fn new(mut snap: I, mut iter: Iter<'a, K, V>) -> YIter<'a, K, V, I, E> {
        let s_entry = snap.next();
        let i_entry = iter.next();
        YIter {
            snap,
            iter,
            s_entry,
            i_entry,
        }
    }
}

impl<'a, K, V, I, E> Iterator for YIter<'a, K, V, I, E>
where
    V: db::Diff<Delta = D>,
    I: Iterator<Item = E>,
    E: Into<Entry<K, V>>,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        let (se, ie) = {
        }
        match (self.s_entry.take(), self.i_entry.take()) {
            (Some(Ok(se)), Some(Ok(ie))) => {
                let cmpval = se.as_key().cmp(ie.as_key());
                //println!(
                //    "yiter next se:{} ie:{} {:?}",
                //    se.to_seqno(),
                //    ie.to_seqno(),
                //    cmpval
                //);
                match cmpval {
                    cmp::Ordering::Less => {
                        self.s_entry = self.x.next();
                        self.i_entry = Some(Ok(ie));
                        Some(Ok(se))
                    }
                    cmp::Ordering::Greater => {
                        self.s_entry = Some(Ok(se));
                        self.i_entry = self.y.next();
                        Some(Ok(ie))
                    }
                    cmp::Ordering::Equal => {
                        // TODO NOTE: xmerge assumes that all mutations
                        // held by each index are mutually exclusive.
                        self.s_entry = self.x.next();
                        self.i_entry = self.y.next();
                        Some(se.xmerge(ie))
                    }
                }
            }
            (Some(Ok(se)), None) => {
                self.s_entry = self.x.next();
                Some(Ok(se))
            }
            (None, Some(Ok(ie))) => {
                self.i_entry = self.y.next();
                Some(Ok(ie))
            }
            (Some(Ok(_xe)), Some(Err(err))) => Some(Err(err)),
            (Some(Err(err)), Some(Ok(_ye))) => Some(Err(err)),
            _ => None,
        }
    }
}

#[allow(dead_code)] // TODO: remove if not required.
pub(crate) fn getter<'a, 'b, I, K, V, Q>(
    index: &'a mut I,
    versions: bool,
) -> LsmGet<'a, K, V, Q>
where
    K: Clone + Ord + Borrow<Q>,
    V: Clone + Diff,
    Q: 'b + Ord + ?Sized + Hash,
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
