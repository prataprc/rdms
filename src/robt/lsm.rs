//! Module `lsm` implement read API across LSM snapshots of
//! single index instance.

use cbordata::FromCbor;

use std::cmp;

use crate::{
    dbs,
    robt::{reader::IterLsm, Entry},
    Result,
};

pub struct YIter<'a, K, V, I, E>
where
    K: Ord + FromCbor,
    V: dbs::Diff + FromCbor,
    <V as dbs::Diff>::Delta: FromCbor,
    I: Iterator<Item = Result<E>>,
    E: Into<Entry<K, V>>,
{
    snap: I,
    iter: IterLsm<'a, K, V>,
    s_entry: Option<Result<Entry<K, V>>>,
    i_entry: Option<Result<Entry<K, V>>>,
}

impl<'a, K, V, I, E> YIter<'a, K, V, I, E>
where
    K: Ord + FromCbor,
    V: dbs::Diff + FromCbor,
    <V as dbs::Diff>::Delta: FromCbor,
    I: Iterator<Item = Result<E>>,
    E: Into<Entry<K, V>>,
{
    pub fn new(mut snap: I, mut iter: IterLsm<'a, K, V>) -> YIter<'a, K, V, I, E> {
        let s_entry = snap.next().map(|re| re.map(|e| e.into()));
        let i_entry = iter.next();
        YIter { snap, iter, s_entry, i_entry }
    }
}

impl<'a, K, V, I, E> Iterator for YIter<'a, K, V, I, E>
where
    K: Clone + Ord + FromCbor,
    V: dbs::Diff + FromCbor,
    <V as dbs::Diff>::Delta: FromCbor + From<V>,
    I: Iterator<Item = Result<E>>,
    E: Into<Entry<K, V>>,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.s_entry.take(), self.i_entry.take()) {
            (Some(Ok(se)), Some(Ok(ie))) => {
                let cmpval = se.as_key().cmp(ie.as_key());
                //println!(
                //    "yiter se:{} ie:{} {:?}",
                //    se.to_seqno(),
                //    ie.to_seqno(),
                //    cmpval
                //);
                match cmpval {
                    cmp::Ordering::Less => {
                        self.s_entry = self.snap.next().map(|re| re.map(|e| e.into()));
                        self.i_entry = Some(Ok(ie));
                        Some(Ok(se))
                    }
                    cmp::Ordering::Greater => {
                        self.i_entry = self.iter.next();
                        self.s_entry = Some(Ok(se));
                        Some(Ok(ie))
                    }
                    cmp::Ordering::Equal => {
                        self.s_entry = self.snap.next().map(|re| re.map(|e| e.into()));
                        self.i_entry = self.iter.next();
                        let (a, b) = (ie.to_seqno().unwrap(), se.to_seqno().unwrap());
                        let (old, new) = if a < b { (ie, se) } else { (se, ie) };
                        Some(Ok(old.commit(new)))
                    }
                }
            }
            (Some(Ok(se)), None) => {
                self.s_entry = self.snap.next().map(|re| re.map(|e| e.into()));
                Some(Ok(se))
            }
            (None, Some(Ok(ie))) => {
                self.i_entry = self.iter.next();
                Some(Ok(ie))
            }
            (Some(Ok(_xe)), Some(Err(err))) => Some(Err(err)),
            (Some(Err(err)), Some(Ok(_ye))) => Some(Err(err)),
            _ => None,
        }
    }
}
