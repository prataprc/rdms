// TODO: flush put blocks into tx channel. Right now we simply unwrap()

use std::{borrow::Borrow, cmp::Ordering, convert::TryInto, marker, ops::Bound};

use crate::{
    core::{self, Diff, Result, Serialize},
    error::Error,
    robt::{Config, Flusher, Stats},
    robt_entry::{MEntry, ZEntry},
};

// Binary format (InterMediate-Block prefix):
//
// *----------------------*
// |      num-entries     |
// *----------------------*
// |    1-entry-offset    |
// *----------------------*
// |        .......       |
// *----------------------*
// |    n-entry-offset    |
// *-------------------*----------------------* 1-entry-offset
// |                MEntry-1                  |
// *-------------------*----------------------* ...
// |                ........                  |
// *-------------------*----------------------* n-entry-offset
// |                MEntry-n                  |
// *------------------------------------------*

pub(crate) enum MBlock<K, V> {
    Encode {
        mblock: Vec<u8>,
        offsets: Vec<u32>,
        first_key: Option<K>,
        m_blocksize: usize,
    },
    Decode {
        block: Vec<u8>,
        count: usize,
        offsets: &'static [u8], // point into block
        phantom_val: marker::PhantomData<V>,
    },
}

// Encode implementation
impl<K, V> MBlock<K, V>
where
    K: Clone + Serialize,
{
    pub(crate) fn new_encode(config: Config) -> MBlock<K, V> {
        MBlock::Encode {
            mblock: Vec::with_capacity(config.m_blocksize),
            offsets: Default::default(),
            first_key: Default::default(),
            m_blocksize: config.m_blocksize,
        }
    }

    pub(crate) fn reset(&mut self) -> Result<()> {
        match self {
            MBlock::Encode {
                mblock,
                offsets,
                first_key,
                ..
            } => {
                mblock.truncate(0);
                offsets.truncate(0);
                first_key.take();
                Ok(())
            }
            MBlock::Decode { .. } => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    pub(crate) fn as_first_key(&self) -> Result<&K> {
        match self {
            MBlock::Encode { first_key, .. } => match first_key.as_ref() {
                Some(fk) => Ok(fk),
                None => err_at!(Fatal, msg: format!("mepty")),
            },
            MBlock::Decode { .. } => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    pub(crate) fn has_first_key(&self) -> Result<bool> {
        match self {
            MBlock::Encode {
                first_key: Some(_), ..
            } => Ok(true),
            MBlock::Encode { .. } => Ok(false),
            MBlock::Decode { .. } => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    pub(crate) fn insertm(&mut self, key: &K, fpos: u64) -> Result<u64> {
        // println!("mblock insertm {:?} {}", key, fpos);
        match self {
            MBlock::Encode {
                mblock,
                offsets,
                first_key,
                m_blocksize,
            } => {
                let offset = mblock.len();
                MEntry::new_m(fpos, key).encode(mblock)?;
                let n = 4 + (offsets.len() + 1) * 4 + mblock.len();
                if n < *m_blocksize {
                    offsets.push(convert_at!(offset)?);
                    first_key.get_or_insert_with(|| key.clone());
                    Ok(convert_at!(offsets.len())?)
                } else {
                    mblock.truncate(offset);
                    Err(Error::__MBlockOverflow(n))
                }
            }
            MBlock::Decode { .. } => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    pub(crate) fn insertz(&mut self, key: &K, fpos: u64) -> Result<u64> {
        // println!("mblock insertz {:?} {}", key, fpos);
        match self {
            MBlock::Encode {
                mblock,
                offsets,
                first_key,
                m_blocksize,
            } => {
                let offset = mblock.len();
                MEntry::new_z(fpos, key).encode(mblock)?;
                let n = 4 + (offsets.len() + 1) * 4 + mblock.len();
                if n < *m_blocksize {
                    offsets.push(convert_at!(offset)?);
                    first_key.get_or_insert_with(|| key.clone());
                    Ok(convert_at!(offsets.len())?)
                } else {
                    mblock.truncate(offset);
                    Err(Error::__MBlockOverflow(n))
                }
            }
            MBlock::Decode { .. } => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    pub(crate) fn finalize(&mut self, stats: &mut Stats) -> Result<u64> {
        match self {
            MBlock::Encode {
                mblock,
                offsets,
                m_blocksize,
                ..
            } => {
                let adjust: u32 = {
                    let m = mblock.len();
                    let adjust = 4 + (offsets.len() * 4);
                    mblock.resize(m + adjust, 0);
                    mblock.copy_within(0..m, adjust);

                    convert_at!(adjust)?
                };
                // encode offset header
                let num: u32 = convert_at!(offsets.len())?;
                mblock[..4].copy_from_slice(&num.to_be_bytes());
                for (i, offset) in offsets.iter().enumerate() {
                    let x = (i + 1) * 4;
                    let offset_bytes = (adjust + offset).to_be_bytes();
                    mblock[x..x + 4].copy_from_slice(&offset_bytes);
                }
                // update statistics
                stats.padding += *m_blocksize - mblock.len();
                stats.m_bytes += *m_blocksize;
                // align blocks
                mblock.resize(*m_blocksize, 0);

                Ok(convert_at!((*m_blocksize))?)
            }
            MBlock::Decode { .. } => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    pub(crate) fn flush<T: Flusher>(&mut self, iflusher: Option<&T>) -> Result<()>
    where
        T: Flusher,
    {
        match self {
            MBlock::Encode { mblock, .. } => match iflusher {
                Some(iflusher) => iflusher.post(mblock.clone())?,
                None => err_at!(Fatal, msg: format!("unreachable"))?,
            },
            MBlock::Decode { .. } => err_at!(Fatal, msg: format!("unreachable"))?,
        }

        Ok(())
    }

    #[cfg(test)]
    fn buffer(&self) -> Result<Vec<u8>> {
        match self {
            MBlock::Encode { mblock, .. } => Ok(mblock.clone()),
            MBlock::Decode { .. } => err_at!(Fatal, msg: format!("unreachable")),
        }
    }
}

// Decode implementation
impl<K, V> MBlock<K, V>
where
    K: Ord + Serialize,
{
    pub(crate) fn new_decode(block: Vec<u8>) -> Result<MBlock<K, V>> {
        let count = u32::from_be_bytes(array_at!(block[..4])?);
        let adjust: usize = convert_at!((4 + (count * 4)))?;
        let offsets = &block[4..adjust] as *const [u8];

        Ok(MBlock::Decode {
            block,
            count: convert_at!(count)?,
            offsets: unsafe { offsets.as_ref().unwrap() },
            phantom_val: marker::PhantomData,
        })
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        match self {
            MBlock::Decode { count, .. } => *count,
            MBlock::Encode { offsets, .. } => offsets.len(),
        }
    }

    // optimized version of find() for mblock. if key is less than the dataset
    // immediately returns with failure.
    pub(crate) fn get<Q>(
        &self,
        key: &Q,
        from: Bound<usize>,
        to: Bound<usize>,
    ) -> Result<MEntry<K>>
    where
        K: Default + Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let f = match from {
            Bound::Included(f) | Bound::Excluded(f) => f,
            Bound::Unbounded => 0,
        };
        let pivot = self.find_pivot(from, to)?;

        //println!(
        //    "mget {:?} {:?} {} {} {:?}",
        //    from,
        //    to,
        //    pivot,
        //    self.len(),
        //    self.to_key(pivot)?
        //);
        match key.cmp(self.to_key(pivot)?.borrow()) {
            Ordering::Less if pivot == 0 => Err(Error::__LessThan),
            Ordering::Less if pivot == f => err_at!(Fatal, msg: format!("unreachable")),
            Ordering::Less => self.get(key, from, Bound::Excluded(pivot)),
            Ordering::Equal => self.to_entry(pivot),
            Ordering::Greater if pivot == f => self.to_entry(pivot),
            Ordering::Greater => self.get(key, Bound::Included(pivot), to),
        }
    }

    pub(crate) fn find<Q>(
        &self,
        key: &Q,
        from: Bound<usize>,
        to: Bound<usize>,
    ) -> Result<MEntry<K>>
    where
        K: Default + Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let f = match from {
            Bound::Included(f) | Bound::Excluded(f) => f,
            Bound::Unbounded => 0,
        };
        let pivot = self.find_pivot(from, to)?;

        //println!(
        //    "mfind {:?} {:?} {} {} {:?}",
        //    from,
        //    to,
        //    pivot,
        //    self.len(),
        //    self.to_key(pivot)?
        //);
        match key.cmp(self.to_key(pivot)?.borrow()) {
            Ordering::Less if pivot == 0 => Err(Error::__LessThan),
            Ordering::Less if pivot == f => err_at!(Fatal, msg: format!("unreachable")),
            Ordering::Less => self.find(key, from, Bound::Excluded(pivot)),
            Ordering::Equal => self.to_entry(pivot),
            Ordering::Greater if pivot == f => self.to_entry(pivot),
            Ordering::Greater => self.find(key, Bound::Included(pivot), to),
        }
    }

    // [from, to)
    fn find_pivot(&self, from: Bound<usize>, to: Bound<usize>) -> Result<usize> {
        let from = match from {
            Bound::Included(from) => from,
            Bound::Unbounded => 0,
            Bound::Excluded(_) => err_at!(Fatal, msg: format!("unreachable"))?,
        };
        let to = match to {
            Bound::Excluded(to) => to,
            Bound::Unbounded => self.len(),
            Bound::Included(_) => err_at!(Fatal, msg: format!("unreachable"))?,
        };
        match to - from {
            n if n > 0 => Ok(from + (n / 2)),
            _ => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    pub fn to_entry(&self, index: usize) -> Result<MEntry<K>> {
        let (block, count, offsets) = match self {
            MBlock::Decode {
                block,
                count,
                offsets,
                ..
            } => (block, *count, offsets),
            MBlock::Encode { .. } => err_at!(Fatal, msg: format!("unreachable"))?,
        };
        if index < count {
            let idx = index * 4;
            let offset: usize =
                convert_at!(u32::from_be_bytes(array_at!(offsets[idx..idx + 4])?))?;
            Ok(MEntry::decode_entry(&block[offset..], index)?)
        } else {
            Err(Error::__MBlockExhausted(index))
        }
    }

    pub(crate) fn last(&self) -> Result<MEntry<K>> {
        let (block, count, offsets) = match self {
            MBlock::Decode {
                block,
                count,
                offsets,
                ..
            } => (block, *count, offsets),
            MBlock::Encode { .. } => err_at!(Fatal, msg: format!("unreachable"))?,
        };
        if count > 0 {
            let index = count - 1;
            let idx = index * 4;
            let offset: usize =
                convert_at!(u32::from_be_bytes(array_at!(offsets[idx..idx + 4])?))?;
            Ok(MEntry::decode_entry(&block[offset..], index)?)
        } else {
            Err(Error::__MBlockExhausted(count))
        }
    }

    pub(crate) fn to_key(&self, index: usize) -> Result<K>
    where
        K: Default,
    {
        let (block, count, offsets) = match self {
            MBlock::Decode {
                block,
                count,
                offsets,
                ..
            } => (block, *count, offsets),
            MBlock::Encode { .. } => err_at!(Fatal, msg: format!("unreachable"))?,
        };
        if index < count {
            let idx = index * 4;
            let offset: usize =
                convert_at!(u32::from_be_bytes(array_at!(offsets[idx..idx + 4])?))?;
            MEntry::decode_key(&block[offset..])
        } else {
            Err(Error::__MBlockExhausted(index))
        }
    }
}

// Binary format (ZBlock prefix):
//
// *----------------------*
// |      num-entries     |
// *----------------------*
// |    1-entry-offset    |
// *----------------------*
// |        .......       |
// *----------------------*
// |    n-entry-offset    |
// *-------------------*----------------------* 1-entry-offset
// |                Entry-1                   |
// *-------------------*----------------------* ...
// |                ........                  |
// *-------------------*----------------------* n-entry-offset
// |                Entry-n                   |
// *------------------------------------------*

pub(crate) enum ZBlock<K, V>
where
    K: Ord + Serialize,
    V: Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    Encode {
        leaf: Vec<u8>, // buffer for z_block
        blob: Vec<u8>, // buffer for vlog
        offsets: Vec<u32>,
        zentries: Vec<ZEntry<K, V>>,
        vpos: u64,
        first_key: Option<K>,
        // configuration
        z_blocksize: usize,
        value_in_vlog: bool,
        delta_ok: bool,
    },
    Decode {
        block: Vec<u8>,
        count: usize,
        offsets: &'static [u8],
        phantom_val: marker::PhantomData<V>,
    },
}

impl<K, V> ZBlock<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    pub(crate) fn new_encode(vpos: u64, config: Config) -> ZBlock<K, V> {
        let z_blocksize = config.z_blocksize;
        let value_in_vlog = config.value_in_vlog;
        let delta_ok = config.delta_ok;

        ZBlock::Encode {
            leaf: Vec::with_capacity(z_blocksize),
            blob: Vec::with_capacity(config.v_blocksize),
            offsets: Vec::with_capacity(64), // TODO: no magic number
            zentries: Vec::with_capacity(64), // TODO: no magic number
            vpos,
            first_key: Default::default(),
            // configuration
            z_blocksize,
            value_in_vlog,
            delta_ok,
        }
    }

    pub(crate) fn reset(&mut self, vpos: u64) -> Result<()> {
        match self {
            ZBlock::Encode {
                leaf,
                blob,
                offsets,
                zentries,
                vpos: vpos_ref,
                first_key,
                ..
            } => {
                leaf.truncate(0);
                blob.truncate(0);
                offsets.truncate(0);
                zentries.truncate(0);
                *vpos_ref = vpos;
                first_key.take();
                Ok(())
            }
            ZBlock::Decode { .. } => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    // TODO: make unwrap into valid error.
    pub(crate) fn as_first_key(&self) -> Result<&K> {
        match self {
            ZBlock::Encode { first_key, .. } => match first_key.as_ref() {
                Some(fk) => Ok(fk),
                None => err_at!(Fatal, msg: format!("empty")),
            },
            ZBlock::Decode { .. } => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    pub(crate) fn has_first_key(&self) -> Result<bool> {
        match self {
            ZBlock::Encode { first_key, .. } => match first_key {
                Some(_) => Ok(true),
                None => Ok(false),
            },
            ZBlock::Decode { .. } => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    pub(crate) fn insert(
        &mut self,
        entry: &core::Entry<K, V>,
        stats: &mut Stats,
    ) -> Result<u64> {
        use crate::robt_entry::ZEntry as DZ;

        match self {
            ZBlock::Encode {
                leaf,
                blob,
                offsets,
                zentries,
                first_key,
                // configuration
                z_blocksize,
                value_in_vlog,
                delta_ok,
                ..
            } => {
                let (leaf_i, blob_i) = (leaf.len(), blob.len());
                let de = match (*value_in_vlog, *delta_ok) {
                    (false, false) => DZ::encode_l(entry, leaf)?,
                    (false, true) => DZ::encode_ld(entry, leaf, blob)?,
                    (true, false) => DZ::encode_lv(entry, leaf, blob)?,
                    (true, true) => DZ::encode_lvd(entry, leaf, blob)?,
                };
                let (k, v, d) = de.to_kvd_stats()?;
                zentries.push(de);

                let n = 4 + ((offsets.len() + 1) * 4) + leaf.len();
                if n < *z_blocksize {
                    stats.key_mem += k;
                    stats.val_mem += v;
                    stats.diff_mem += d;
                    offsets.push(convert_at!(leaf_i)?);
                    first_key.get_or_insert_with(|| entry.as_key().clone());
                    Ok(convert_at!(offsets.len())?)
                } else {
                    leaf.truncate(leaf_i);
                    blob.truncate(blob_i);
                    Err(Error::__ZBlockOverflow(n))
                }
            }
            ZBlock::Decode { .. } => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    pub(crate) fn finalize(&mut self, stats: &mut Stats) -> Result<(u64, u64)> {
        match self {
            ZBlock::Encode {
                leaf,
                blob,
                offsets,
                zentries,
                vpos,
                // configuration
                z_blocksize,
                ..
            } => {
                let adjust: u32 = {
                    let m = leaf.len();
                    let adjust = 4 + (offsets.len() * 4);
                    leaf.resize(m + adjust, 0);
                    leaf.copy_within(0..m, adjust);

                    convert_at!(adjust)?
                };
                // encode offset header
                let num: u32 = convert_at!(offsets.len())?;
                leaf[..4].copy_from_slice(&num.to_be_bytes());
                for (i, offset) in offsets.iter().enumerate() {
                    let x = (i + 1) * 4;
                    let offset_bytes = (adjust + offset).to_be_bytes();
                    leaf[x..x + 4].copy_from_slice(&offset_bytes);
                    // adjust file position offsets for value and delta in vlog.
                    let j: usize = convert_at!((adjust + offset))?;
                    zentries[i].re_encode_fpos(&mut leaf[j..], *vpos)?;
                }
                // update statistics
                stats.padding += *z_blocksize - leaf.len();
                stats.z_bytes += *z_blocksize;
                stats.v_bytes += blob.len();
                // align blocks
                leaf.resize(*z_blocksize, 0);

                Ok((
                    convert_at!(*z_blocksize)?, // full block
                    convert_at!(blob.len())?,
                ))
            }
            ZBlock::Decode { .. } => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    pub(crate) fn flush<T: Flusher>(
        &mut self,
        iflusher: Option<&T>,
        vflusher: Option<&T>,
    ) -> Result<()> {
        match self {
            ZBlock::Encode { leaf, blob, .. } => {
                match iflusher {
                    Some(iflusher) => iflusher.post(leaf.clone())?,
                    None => err_at!(Fatal, msg: format!("unreachable"))?,
                }
                match vflusher {
                    Some(vflusher) => vflusher.post(blob.clone())?,
                    None => (),
                }
            }
            ZBlock::Decode { .. } => err_at!(Fatal, msg: format!("unreachable"))?,
        }
        Ok(())
    }

    #[cfg(test)]
    fn buffer(&self) -> Result<(Vec<u8>, Vec<u8>)> {
        match self {
            ZBlock::Encode { leaf, blob, .. } => Ok((leaf.clone(), blob.clone())),
            ZBlock::Decode { .. } => err_at!(Fatal, msg: format!("unreachable")),
        }
    }
}

impl<K, V> ZBlock<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    pub(crate) fn new_decode(block: Vec<u8>) -> Result<ZBlock<K, V>> {
        let count = u32::from_be_bytes(array_at!(block[..4])?);
        let adjust: usize = convert_at!((4 + (count * 4)))?;
        let offsets = &block[4..adjust] as *const [u8];

        Ok(ZBlock::Decode {
            block,
            count: convert_at!(count)?,
            offsets: unsafe { offsets.as_ref().unwrap() },
            phantom_val: marker::PhantomData,
        })
    }

    #[inline]
    pub(crate) fn len(&self) -> Result<usize> {
        match self {
            ZBlock::Decode { count, .. } => Ok(*count),
            ZBlock::Encode { .. } => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    // return (index-to-entry, core::Entry), if None means key-not found
    pub(crate) fn find<Q>(
        &self,
        key: &Q,
        from: Bound<usize>,
        to: Bound<usize>,
    ) -> Result<(usize, core::Entry<K, V>)>
    where
        K: Default + Borrow<Q>,
        Q: Ord + ?Sized,
        V: Default,
    {
        let f = match from {
            Bound::Included(f) | Bound::Excluded(f) => f,
            Bound::Unbounded => 0,
        };
        let pivot = self.find_pivot(from, to)?;

        //println!(
        //    "zfind {:?} {:?} {} {} {:?}",
        //    from,
        //    to,
        //    pivot,
        //    self.len(),
        //    self.to_key(pivot)?
        //);
        match key.cmp(self.to_key(pivot)?.borrow()) {
            Ordering::Less if pivot == 0 => Err(Error::__LessThan),
            Ordering::Less if pivot == f => err_at!(Fatal, msg: format!("unreachable")),
            Ordering::Less => self.find(key, from, Bound::Excluded(pivot)),
            Ordering::Equal => self.to_entry(pivot),
            Ordering::Greater if pivot == f => Err(Error::__ZBlockExhausted(f)),
            Ordering::Greater => self.find(key, Bound::Included(pivot), to),
        }
    }

    // [from, to)
    fn find_pivot(&self, from: Bound<usize>, to: Bound<usize>) -> Result<usize> {
        let from = match from {
            Bound::Included(from) | Bound::Excluded(from) => from,
            Bound::Unbounded => 0,
        };
        let to = match to {
            Bound::Excluded(to) => to,
            Bound::Unbounded => self.len()?,
            Bound::Included(_) => err_at!(Fatal, msg: format!("unreachable"))?,
        };
        match to - from {
            n if n > 0 => Ok(from + (n / 2)),
            _ => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    pub fn to_entry(&self, index: usize) -> Result<(usize, core::Entry<K, V>)>
    where
        K: Default,
        V: Default,
    {
        let (block, count, offsets) = match self {
            ZBlock::Decode {
                block,
                count,
                offsets,
                ..
            } => (block, *count, offsets),
            ZBlock::Encode { .. } => err_at!(Fatal, msg: format!("unreachable"))?,
        };

        if index < count {
            let idx = index * 4;
            let offset: usize =
                convert_at!(u32::from_be_bytes(array_at!(offsets[idx..idx + 4])?))?;
            let entry = &block[offset..];
            Ok((index, ZEntry::decode_entry(entry)?))
        } else {
            Err(Error::__ZBlockExhausted(index))
        }
    }

    pub fn last(&self) -> Result<(usize, core::Entry<K, V>)>
    where
        K: Default,
        V: Default,
    {
        let (block, count, offsets) = match self {
            ZBlock::Decode {
                block,
                count,
                offsets,
                ..
            } => (block, *count, offsets),
            ZBlock::Encode { .. } => err_at!(Fatal, msg: format!("unreachable"))?,
        };
        if count > 0 {
            let index = count - 1;
            let idx = index * 4;
            let offset: usize =
                convert_at!(u32::from_be_bytes(array_at!(offsets[idx..idx + 4])?))?;
            let entry = &block[offset..];
            Ok((index, ZEntry::decode_entry(entry)?))
        } else {
            Err(Error::__ZBlockExhausted(count))
        }
    }

    fn to_key(&self, index: usize) -> Result<K>
    where
        K: Default,
    {
        let (block, offsets) = match self {
            ZBlock::Decode { block, offsets, .. } => (block, offsets),
            ZBlock::Encode { .. } => err_at!(Fatal, msg: format!("unreachable"))?,
        };
        let idx = index * 4;
        let offset: usize =
            convert_at!(u32::from_be_bytes(array_at!(offsets[idx..idx + 4])?))?;
        let entry = &block[offset..];
        ZEntry::<K, V>::decode_key(entry)
    }
}

#[cfg(test)]
#[path = "robt_index_test.rs"]
mod robt_index_test;
