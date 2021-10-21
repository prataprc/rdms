use cbordata::FromCbor;

use std::{borrow::Borrow, convert::TryFrom, fmt, ops::RangeBounds};

use crate::{bitmaps::NoBitmap, db, llrb, robt, Error, Result};

pub enum Index<K, V, B = NoBitmap>
where
    K: FromCbor,
    V: db::Diff + FromCbor,
    <V as db::Diff>::Delta: FromCbor,
    B: db::Bloom,
{
    Llrb { db: llrb::Index<K, V> },
    Robt { db: robt::Index<K, V, B> },
}

impl<K, V, B> Index<K, V, B>
where
    K: FromCbor,
    V: db::Diff + FromCbor,
    <V as db::Diff>::Delta: FromCbor,
    B: db::Bloom,
{
    pub fn from_llrb(db: llrb::Index<K, V>) -> Index<K, V, B> {
        Index::Llrb { db }
    }

    pub fn from_robt(db: robt::Index<K, V, B>) -> Index<K, V, B> {
        Index::Robt { db }
    }

    pub fn set_seqno(&mut self, seqno: u64) -> Option<u64>
    where
        K: Clone,
    {
        match self {
            Index::Llrb { db } => Some(db.set_seqno(seqno)),
            Index::Robt { .. } => None,
        }
    }

    pub fn close(self) -> Result<()> {
        match self {
            Index::Llrb { db } => db.close(),
            Index::Robt { db } => db.close(),
        }
    }

    pub fn purge(self) -> Result<()> {
        match self {
            Index::Llrb { db } => db.purge(),
            Index::Robt { db } => db.purge(),
        }
    }
}

impl<K, V, B> Index<K, V, B>
where
    K: FromCbor,
    V: db::Diff + FromCbor,
    <V as db::Diff>::Delta: FromCbor,
    B: db::Bloom,
{
    pub fn deleted_count(&mut self) -> Option<usize> {
        match self {
            Index::Llrb { db } => Some(db.deleted_count()),
            Index::Robt { db } => Some(db.to_stats().n_deleted),
        }
    }

    pub fn footprint(&mut self) -> Result<usize> {
        match self {
            Index::Llrb { db } => {
                let n = db.footprint()?;
                err_at!(FailConvert, usize::try_from(n))
            }
            Index::Robt { db } => db.footprint(),
        }
    }

    pub fn is_empty(&mut self) -> bool {
        match self {
            Index::Llrb { db } => db.is_empty(),
            Index::Robt { db } => db.is_empty(),
        }
    }

    pub fn len(&mut self) -> usize {
        match self {
            Index::Llrb { db } => db.len(),
            Index::Robt { db } => db.len(),
        }
    }

    pub fn to_name(&mut self) -> String {
        match self {
            Index::Llrb { db } => db.to_name(),
            Index::Robt { db } => db.to_name(),
        }
    }

    pub fn to_seqno(&mut self) -> Option<u64> {
        let seqno = match self {
            Index::Llrb { db } => db.to_seqno(),
            Index::Robt { db } => db.to_seqno(),
        };

        Some(seqno)
    }

    pub fn to_stats(&mut self) -> Result<Stats> {
        let stats = match self {
            Index::Llrb { db } => Stats::Llrb {
                stats: db.to_stats()?,
            },
            Index::Robt { db } => Stats::Robt {
                stats: db.to_stats(),
            },
        };
        Ok(stats)
    }
}

impl<K, V, B> Index<K, V, B>
where
    K: Clone + Ord + db::Footprint + FromCbor,
    V: db::Diff + db::Footprint + FromCbor,
    <V as db::Diff>::Delta: db::Footprint + FromCbor,
    B: db::Bloom,
{
    pub fn set(&self, key: K, value: V) -> Result<db::Wr<K, V>> {
        match self {
            Index::Llrb { db } => db.set(key, value),
            Index::Robt { .. } => {
                err_at!(NotImplemented, msg: "set op not supported for robt:Index")
            }
        }
    }

    pub fn set_cas(&self, key: K, value: V, cas: u64) -> Result<db::Wr<K, V>> {
        match self {
            Index::Llrb { db } => db.set_cas(key, value, cas),
            Index::Robt { .. } => {
                err_at!(NotImplemented, msg: "set_cas op not supported for robt:Index")
            }
        }
    }

    pub fn insert(&self, key: K, value: V) -> Result<db::Wr<K, V>> {
        match self {
            Index::Llrb { db } => db.insert(key, value),
            Index::Robt { .. } => {
                err_at!(NotImplemented, msg: "insert op not supported for robt:Index")
            }
        }
    }

    pub fn insert_cas(&self, key: K, value: V, cas: u64) -> Result<db::Wr<K, V>> {
        match self {
            Index::Llrb { db } => db.insert_cas(key, value, cas),
            Index::Robt { .. } => {
                err_at!(NotImplemented, msg: "insert_cas op not supported for robt:Index")
            }
        }
    }

    pub fn delete<Q>(&self, key: &Q) -> Result<db::Wr<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ToOwned<Owned = K> + ?Sized,
    {
        match self {
            Index::Llrb { db } => db.delete(key),
            Index::Robt { .. } => {
                err_at!(NotImplemented, msg: "delete op not supported for robt:Index")
            }
        }
    }

    pub fn delete_cas<Q>(&self, key: &Q, cas: u64) -> Result<db::Wr<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ToOwned<Owned = K> + ?Sized,
    {
        match self {
            Index::Llrb { db } => db.delete_cas(key, cas),
            Index::Robt { .. } => {
                err_at!(NotImplemented, msg: "delete_cas op not supported for robt:Index")
            }
        }
    }

    pub fn remove<Q>(&self, key: &Q) -> Result<db::Wr<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ToOwned<Owned = K> + ?Sized,
    {
        match self {
            Index::Llrb { db } => db.remove(key),
            Index::Robt { .. } => {
                err_at!(NotImplemented, msg: "remove op not supported for robt:Index")
            }
        }
    }

    pub fn remove_cas<Q>(&self, key: &Q, cas: u64) -> Result<db::Wr<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ToOwned<Owned = K> + ?Sized,
    {
        match self {
            Index::Llrb { db } => db.remove_cas(key, cas),
            Index::Robt { .. } => {
                err_at!(NotImplemented, msg: "remove_cas op not supported for robt:Index")
            }
        }
    }

    pub fn write(&self, op: db::Write<K, V>) -> Result<db::Wr<K, V>> {
        match self {
            Index::Llrb { db } => db.write(op),
            Index::Robt { .. } => {
                err_at!(NotImplemented, msg: "write ops not supported for robt:Index")
            }
        }
    }

    pub fn commit<I>(&self, iter: I, versions: bool) -> Result<usize>
    where
        K: PartialEq,
        I: Iterator<Item = db::Entry<K, V>>,
    {
        match self {
            Index::Llrb { db } => db.commit(iter, versions),
            Index::Robt { .. } => {
                err_at!(NotImplemented, msg: "cannot commit into robt::Index")
            }
        }
    }
}

impl<K, V, B> Index<K, V, B>
where
    K: Clone + FromCbor,
    V: db::Diff + FromCbor,
    <V as db::Diff>::Delta: FromCbor,
    B: db::Bloom,
{
    pub fn get<Q: ?Sized>(&mut self, key: &Q) -> Result<db::Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        match self {
            Index::Llrb { db } => db.get(key),
            Index::Robt { db } => db.get(key),
        }
    }

    pub fn get_versions<Q: ?Sized>(&mut self, key: &Q) -> Result<db::Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        match self {
            Index::Llrb { db } => db.get_versions(key),
            Index::Robt { db } => db.get_versions(key),
        }
    }

    pub fn iter(&mut self) -> Result<Iter<K, V>>
    where
        K: Ord,
    {
        let iter = match self {
            Index::Llrb { db } => Iter::Llrb { iter: db.iter()? },
            Index::Robt { db } => Iter::Robt { iter: db.iter(..)? },
        };

        Ok(iter)
    }

    pub fn iter_versions(&mut self) -> Result<Iter<K, V>>
    where
        K: Ord,
    {
        let iter = match self {
            Index::Llrb { db } => Iter::Llrb {
                iter: db.iter_versions()?,
            },
            Index::Robt { db } => Iter::Robt {
                iter: db.iter_versions(..)?,
            },
        };

        Ok(iter)
    }

    pub fn range<R, Q>(&mut self, range: R) -> Result<Range<K, V, R, Q>>
    where
        K: Clone + Ord + Borrow<Q>,
        R: RangeBounds<Q>,
        Q: ?Sized + Ord + ToOwned<Owned = K>,
    {
        let iter = match self {
            Index::Llrb { db } => Range::Llrb {
                iter: db.range(range)?,
            },
            Index::Robt { db } => Range::Robt {
                iter: db.iter(range)?,
            },
        };

        Ok(iter)
    }

    pub fn range_versions<R, Q>(&mut self, range: R) -> Result<Range<K, V, R, Q>>
    where
        K: Clone + Ord + Borrow<Q>,
        R: RangeBounds<Q>,
        Q: ?Sized + Ord + ToOwned<Owned = K>,
    {
        let iter = match self {
            Index::Llrb { db } => Range::Llrb {
                iter: db.range_versions(range)?,
            },
            Index::Robt { db } => Range::Robt {
                iter: db.iter_versions(range)?,
            },
        };

        Ok(iter)
    }

    pub fn reverse<R, Q>(&mut self, range: R) -> Result<Reverse<K, V, R, Q>>
    where
        K: Ord + Borrow<Q>,
        R: RangeBounds<Q>,
        Q: ?Sized + Ord + ToOwned<Owned = K>,
    {
        let iter = match self {
            Index::Llrb { db } => Reverse::Llrb {
                iter: db.reverse(range)?,
            },
            Index::Robt { db } => Reverse::Robt {
                iter: db.reverse(range)?,
            },
        };

        Ok(iter)
    }

    pub fn reverse_versions<R, Q>(&mut self, range: R) -> Result<Reverse<K, V, R, Q>>
    where
        K: Ord + Borrow<Q>,
        R: RangeBounds<Q>,
        Q: ?Sized + Ord + ToOwned<Owned = K>,
    {
        let iter = match self {
            Index::Llrb { db } => Reverse::Llrb {
                iter: db.reverse_versions(range)?,
            },
            Index::Robt { db } => Reverse::Robt {
                iter: db.reverse_versions(range)?,
            },
        };

        Ok(iter)
    }

    pub fn validate(&mut self) -> Result<()>
    where
        K: Ord + fmt::Debug,
    {
        match self {
            Index::Llrb { db } => db.validate(),
            Index::Robt { db } => db.validate().map(|_| ()),
        }
    }
}

pub enum Stats {
    Llrb { stats: llrb::Stats },
    Robt { stats: robt::Stats },
}

pub enum Iter<'a, K, V>
where
    V: db::Diff,
{
    Llrb { iter: llrb::Iter<K, V> },
    Robt { iter: robt::Iter<'a, K, V> },
}

pub enum Range<'a, K, V, R, Q>
where
    V: db::Diff,
    Q: ?Sized,
{
    Llrb { iter: llrb::Range<K, V, R, Q> },
    Robt { iter: robt::Iter<'a, K, V> },
}

pub enum Reverse<'a, K, V, R, Q>
where
    V: db::Diff,
    Q: ?Sized + Ord,
{
    Llrb { iter: llrb::Reverse<K, V, R, Q> },
    Robt { iter: robt::Iter<'a, K, V> },
}
