use cbordata::FromCbor;

use std::{borrow::Borrow, convert::TryFrom, fmt, ops::RangeBounds};

use crate::{bitmaps::NoBitmap, dbs, llrb, robt, Error, Result};

// Outstanding robt-api:
//  * initial, build_index,
//  * incremental, build_index,
//  * compact
//
//  * open, open_file, set_bitmap
//  * lsm_merge, compact
//  * as_bitmap, print, to_index_location, to_vlog_location

pub enum Index<K, V, B = NoBitmap>
where
    K: FromCbor,
    V: dbs::Diff + FromCbor,
    <V as dbs::Diff>::Delta: FromCbor,
    B: dbs::Bloom,
{
    Llrb { store: llrb::Index<K, V> },
    Robt { store: robt::Index<K, V, B> },
}

impl<K, V, B> Index<K, V, B>
where
    K: FromCbor,
    V: dbs::Diff + FromCbor,
    <V as dbs::Diff>::Delta: FromCbor,
    B: dbs::Bloom,
{
    pub fn from_llrb(store: llrb::Index<K, V>) -> Index<K, V, B> {
        Index::Llrb { store }
    }

    pub fn from_robt(store: robt::Index<K, V, B>) -> Index<K, V, B> {
        Index::Robt { store }
    }

    pub fn try_clone(&self) -> Result<Index<K, V, B>>
    where
        K: Clone,
    {
        let val = match self {
            Index::Llrb { store } => Index::Llrb {
                store: store.clone(),
            },
            Index::Robt { store } => Index::Robt {
                store: store.try_clone()?,
            },
        };

        Ok(val)
    }

    pub fn set_seqno(&mut self, seqno: u64) -> Option<u64>
    where
        K: Clone,
    {
        match self {
            Index::Llrb { store } => Some(store.set_seqno(seqno)),
            Index::Robt { .. } => None,
        }
    }

    pub fn close(self) -> Result<()> {
        match self {
            Index::Llrb { store } => store.close(),
            Index::Robt { store } => store.close(),
        }
    }

    pub fn purge(self) -> Result<()> {
        match self {
            Index::Llrb { store } => store.purge(),
            Index::Robt { store } => store.purge(),
        }
    }
}

impl<K, V, B> Index<K, V, B>
where
    K: FromCbor,
    V: dbs::Diff + FromCbor,
    <V as dbs::Diff>::Delta: FromCbor,
    B: dbs::Bloom,
{
    pub fn as_llrb(&self) -> Option<&llrb::Index<K, V>> {
        match self {
            Index::Llrb { store } => Some(store),
            _ => None,
        }
    }

    pub fn as_robt(&self) -> Option<&robt::Index<K, V, B>> {
        match self {
            Index::Robt { store } => Some(store),
            _ => None,
        }
    }

    pub fn deleted_count(&mut self) -> Option<usize> {
        match self {
            Index::Llrb { store } => Some(store.deleted_count()),
            Index::Robt { store } => Some(store.to_stats().n_deleted),
        }
    }

    pub fn footprint(&mut self) -> Result<usize> {
        match self {
            Index::Llrb { store } => {
                let n = store.footprint()?;
                err_at!(FailConvert, usize::try_from(n))
            }
            Index::Robt { store } => store.footprint(),
        }
    }

    pub fn is_empty(&mut self) -> bool {
        match self {
            Index::Llrb { store } => store.is_empty(),
            Index::Robt { store } => store.is_empty(),
        }
    }

    pub fn is_compacted(&mut self) -> bool {
        match self {
            Index::Llrb { .. } => true,
            Index::Robt { store } => store.is_compacted(),
        }
    }

    pub fn len(&mut self) -> usize {
        match self {
            Index::Llrb { store } => store.len(),
            Index::Robt { store } => store.len(),
        }
    }

    pub fn to_name(&mut self) -> String {
        match self {
            Index::Llrb { store } => store.to_name(),
            Index::Robt { store } => store.to_name(),
        }
    }

    pub fn to_seqno(&mut self) -> Option<u64> {
        let seqno = match self {
            Index::Llrb { store } => store.to_seqno(),
            Index::Robt { store } => store.to_seqno(),
        };

        Some(seqno)
    }

    pub fn to_stats(&mut self) -> Result<Stats> {
        let stats = match self {
            Index::Llrb { store } => Stats::Llrb {
                stats: store.to_stats()?,
            },
            Index::Robt { store } => Stats::Robt {
                stats: store.to_stats(),
            },
        };
        Ok(stats)
    }

    pub fn to_app_metadata(&self) -> Option<Vec<u8>> {
        match self {
            Index::Llrb { .. } => None,
            Index::Robt { store } => Some(store.to_app_metadata()),
        }
    }

    pub fn to_bitmap(&self) -> Option<B>
    where
        B: Clone,
    {
        match self {
            Index::Llrb { .. } => None,
            Index::Robt { store } => Some(store.to_bitmap()),
        }
    }

    pub fn to_root(&self) -> Option<u64> {
        match self {
            Index::Llrb { .. } => None,
            Index::Robt { store } => store.to_root(),
        }
    }
}

impl<K, V, B> Index<K, V, B>
where
    K: Clone + Ord + dbs::Footprint + FromCbor,
    V: dbs::Diff + dbs::Footprint + FromCbor,
    <V as dbs::Diff>::Delta: dbs::Footprint + FromCbor,
    B: dbs::Bloom,
{
    pub fn set(&self, key: K, value: V) -> Result<dbs::Wr<K, V>> {
        match self {
            Index::Llrb { store } => store.set(key, value),
            Index::Robt { .. } => {
                err_at!(NotImplemented, msg: "set op not supported for robt:Index")
            }
        }
    }

    pub fn set_cas(&self, key: K, value: V, cas: u64) -> Result<dbs::Wr<K, V>> {
        match self {
            Index::Llrb { store } => store.set_cas(key, value, cas),
            Index::Robt { .. } => {
                err_at!(NotImplemented, msg: "set_cas op not supported for robt:Index")
            }
        }
    }

    pub fn insert(&self, key: K, value: V) -> Result<dbs::Wr<K, V>> {
        match self {
            Index::Llrb { store } => store.insert(key, value),
            Index::Robt { .. } => {
                err_at!(NotImplemented, msg: "insert op not supported for robt:Index")
            }
        }
    }

    pub fn insert_cas(&self, key: K, value: V, cas: u64) -> Result<dbs::Wr<K, V>> {
        match self {
            Index::Llrb { store } => store.insert_cas(key, value, cas),
            Index::Robt { .. } => {
                err_at!(NotImplemented, msg: "insert_cas op not supported for robt:Index")
            }
        }
    }

    pub fn delete<Q>(&self, key: &Q) -> Result<dbs::Wr<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ToOwned<Owned = K> + ?Sized,
    {
        match self {
            Index::Llrb { store } => store.delete(key),
            Index::Robt { .. } => {
                err_at!(NotImplemented, msg: "delete op not supported for robt:Index")
            }
        }
    }

    pub fn delete_cas<Q>(&self, key: &Q, cas: u64) -> Result<dbs::Wr<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ToOwned<Owned = K> + ?Sized,
    {
        match self {
            Index::Llrb { store } => store.delete_cas(key, cas),
            Index::Robt { .. } => {
                err_at!(NotImplemented, msg: "delete_cas op not supported for robt:Index")
            }
        }
    }

    pub fn remove<Q>(&self, key: &Q) -> Result<dbs::Wr<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ToOwned<Owned = K> + ?Sized,
    {
        match self {
            Index::Llrb { store } => store.remove(key),
            Index::Robt { .. } => {
                err_at!(NotImplemented, msg: "remove op not supported for robt:Index")
            }
        }
    }

    pub fn remove_cas<Q>(&self, key: &Q, cas: u64) -> Result<dbs::Wr<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ToOwned<Owned = K> + ?Sized,
    {
        match self {
            Index::Llrb { store } => store.remove_cas(key, cas),
            Index::Robt { .. } => {
                err_at!(NotImplemented, msg: "remove_cas op not supported for robt:Index")
            }
        }
    }

    pub fn write(&self, op: dbs::Write<K, V>) -> Result<dbs::Wr<K, V>> {
        match self {
            Index::Llrb { store } => store.write(op),
            Index::Robt { .. } => {
                err_at!(NotImplemented, msg: "write ops not supported for robt:Index")
            }
        }
    }

    pub fn commit<I>(&self, iter: I, versions: bool) -> Result<usize>
    where
        K: PartialEq,
        I: Iterator<Item = dbs::Entry<K, V>>,
    {
        match self {
            Index::Llrb { store } => store.commit(iter, versions),
            Index::Robt { .. } => {
                err_at!(NotImplemented, msg: "cannot commit into robt::Index")
            }
        }
    }
}

impl<K, V, B> Index<K, V, B>
where
    K: Clone + FromCbor,
    V: dbs::Diff + FromCbor,
    <V as dbs::Diff>::Delta: FromCbor,
    B: dbs::Bloom,
{
    pub fn get<Q: ?Sized>(&mut self, key: &Q) -> Result<dbs::Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        match self {
            Index::Llrb { store } => store.get(key),
            Index::Robt { store } => store.get(key),
        }
    }

    pub fn get_versions<Q: ?Sized>(&mut self, key: &Q) -> Result<dbs::Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        match self {
            Index::Llrb { store } => store.get_versions(key),
            Index::Robt { store } => store.get_versions(key),
        }
    }

    pub fn iter(&mut self) -> Result<Iter<K, V>>
    where
        K: Ord,
    {
        let iter = match self {
            Index::Llrb { store } => Iter::Llrb {
                iter: store.iter()?,
            },
            Index::Robt { store } => Iter::Robt {
                iter: store.iter(..)?,
            },
        };

        Ok(iter)
    }

    pub fn iter_versions(&mut self) -> Result<Iter<K, V>>
    where
        K: Ord,
    {
        let iter = match self {
            Index::Llrb { store } => Iter::Llrb {
                iter: store.iter_versions()?,
            },
            Index::Robt { store } => Iter::Robt {
                iter: store.iter_versions(..)?,
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
            Index::Llrb { store } => Range::Llrb {
                iter: store.range(range)?,
            },
            Index::Robt { store } => Range::Robt {
                iter: store.iter(range)?,
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
            Index::Llrb { store } => Range::Llrb {
                iter: store.range_versions(range)?,
            },
            Index::Robt { store } => Range::Robt {
                iter: store.iter_versions(range)?,
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
            Index::Llrb { store } => Reverse::Llrb {
                iter: store.reverse(range)?,
            },
            Index::Robt { store } => Reverse::Robt {
                iter: store.reverse(range)?,
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
            Index::Llrb { store } => Reverse::Llrb {
                iter: store.reverse_versions(range)?,
            },
            Index::Robt { store } => Reverse::Robt {
                iter: store.reverse_versions(range)?,
            },
        };

        Ok(iter)
    }

    pub fn validate(&mut self) -> Result<()>
    where
        K: Ord + fmt::Debug,
    {
        match self {
            Index::Llrb { store } => store.validate(),
            Index::Robt { store } => store.validate().map(|_| ()),
        }
    }
}

pub enum Stats {
    Llrb { stats: llrb::Stats },
    Robt { stats: robt::Stats },
}

pub enum Iter<'a, K, V>
where
    V: dbs::Diff,
{
    Llrb { iter: llrb::Iter<K, V> },
    Robt { iter: robt::Iter<'a, K, V> },
}

pub enum Range<'a, K, V, R, Q>
where
    V: dbs::Diff,
    Q: ?Sized,
{
    Llrb { iter: llrb::Range<K, V, R, Q> },
    Robt { iter: robt::Iter<'a, K, V> },
}

pub enum Reverse<'a, K, V, R, Q>
where
    V: dbs::Diff,
    Q: ?Sized + Ord,
{
    Llrb { iter: llrb::Reverse<K, V, R, Q> },
    Robt { iter: robt::Iter<'a, K, V> },
}
