// TODO: flush put blocks into tx channel. Right now we simply unwrap()

use std::ops::Bound;
use std::{borrow::Borrow, cmp::Ordering, convert::TryInto, fs, marker};

use crate::core::{self, Diff, Serialize};
use crate::error::Error;
use crate::robt_build::Flusher;
use crate::robt_config::Config;
use crate::robt_entry::{DiskEntryM, DiskEntryZ};
use crate::robt_stats::Stats;
use crate::util;

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
        config: Config,
    },
    Decode {
        block: Vec<u8>,
        count: usize,
        offsets: &'static [u8],
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
            config,
        }
    }

    pub(crate) fn reset(&mut self) {
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
            }
            MBlock::Decode { .. } => unreachable!(),
        }
    }

    pub(crate) fn as_first_key(&mut self) -> &K {
        match self {
            MBlock::Encode { first_key, .. } => first_key.as_ref().unwrap(),
            MBlock::Decode { .. } => unreachable!(),
        }
    }

    pub(crate) fn has_first_key(&self) -> bool {
        match self {
            MBlock::Encode {
                first_key: Some(_), ..
            } => true,
            MBlock::Encode { .. } => false,
            MBlock::Decode { .. } => unreachable!(),
        }
    }

    pub(crate) fn insertm(&mut self, key: &K, fpos: u64) -> Result<u64, Error> {
        match self {
            MBlock::Encode {
                mblock,
                offsets,
                first_key,
                config,
            } => {
                let m = mblock.len();
                DiskEntryM::encode_m(Some(fpos), None, key, mblock)?;
                let n = mblock.len();
                if n < config.m_blocksize {
                    offsets.push(m.try_into().unwrap());
                    first_key.get_or_insert_with(|| key.clone());
                    Ok(offsets.len().try_into().unwrap())
                } else {
                    mblock.truncate(m);
                    Err(Error::__MBlockOverflow(n))
                }
            }
            MBlock::Decode { .. } => unreachable!(),
        }
    }

    pub(crate) fn insertz(&mut self, key: &K, fpos: u64) -> Result<u64, Error> {
        match self {
            MBlock::Encode {
                mblock,
                offsets,
                first_key,
                config,
            } => {
                let m = mblock.len();
                DiskEntryM::encode_m(None, Some(fpos), key, mblock)?;
                let n = mblock.len();
                if n < config.m_blocksize {
                    offsets.push(m.try_into().unwrap());
                    first_key.get_or_insert_with(|| key.clone());
                    Ok(offsets.len().try_into().unwrap())
                } else {
                    mblock.truncate(m);
                    Err(Error::__MBlockOverflow(n))
                }
            }
            MBlock::Decode { .. } => unreachable!(),
        }
    }

    pub(crate) fn finalize(&mut self, stats: &mut Stats) -> u64 {
        match self {
            MBlock::Encode {
                mblock,
                offsets,
                config,
                ..
            } => {
                let adjust = 4 + (offsets.len() * 4);
                let x: u32 = adjust.try_into().unwrap();
                offsets.iter_mut().for_each(|offset| *offset += x);
                // adjust space for offset header
                let m = mblock.len();
                mblock.resize(m + adjust, 0);
                mblock.copy_within(0..m, adjust);
                // encode offset header
                let num: u32 = offsets.len().try_into().unwrap();
                &mblock[..4].copy_from_slice(&num.to_be_bytes());
                for (i, offset) in offsets.iter().enumerate() {
                    let x = (i + 1) * 4;
                    mblock[x..x + 4].copy_from_slice(&offset.to_be_bytes());
                }
                // update statistics
                stats.padding += config.m_blocksize - mblock.len();
                stats.m_bytes += config.m_blocksize;
                // align blocks
                mblock.resize(config.m_blocksize, 0);

                config.m_blocksize.try_into().unwrap()
            }
            MBlock::Decode { .. } => unreachable!(),
        }
    }

    pub(crate) fn flush(&mut self, i_flusher: &mut Flusher) {
        match self {
            MBlock::Encode { mblock, .. } => {
                i_flusher.send(mblock.clone());
            }
            MBlock::Decode { .. } => unreachable!(),
        }
    }
}

// Decode implementation
impl<K, V> MBlock<K, V>
where
    K: Ord + Serialize,
{
    pub(crate) fn new_decode(
        fd: &mut fs::File,
        fpos: u64,
        config: &Config,
    ) -> Result<MBlock<K, V>, Error> {
        let n: u64 = config.m_blocksize.try_into().unwrap();
        let block = util::read_buffer(fd, fpos, n, "reading mblock")?;
        let count = u32::from_be_bytes(block[..4].try_into().unwrap());
        let adjust: usize = (4 + (count * 4)).try_into().unwrap();
        let offsets = &block[4..adjust] as *const [u8];

        Ok(MBlock::Decode {
            block,
            count: count.try_into().unwrap(),
            offsets: unsafe { offsets.as_ref().unwrap() },
            phantom_val: marker::PhantomData,
        })
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        match self {
            MBlock::Decode { count, .. } => *count,
            _ => unreachable!(),
        }
    }

    // optimized version of find() for mblock. if key is less than the dataset
    // immediately returns with failure.
    pub(crate) fn get<Q>(
        &self,
        key: &Q,
        from: Bound<usize>,
        to: Bound<usize>,
    ) -> Result<DiskEntryM, Error>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let f = match from {
            Bound::Included(f) | Bound::Excluded(f) => f,
            Bound::Unbounded => 0,
        };
        let pivot = self.find_pivot(from, to);

        match key.cmp(self.to_key(pivot)?.borrow()) {
            Ordering::Less if pivot == 0 => Err(Error::__LessThan),
            Ordering::Less if pivot == f => unreachable!(),
            Ordering::Less => self.find(key, from, Bound::Excluded(pivot)),
            Ordering::Equal => self.to_entry(pivot),
            Ordering::Greater if pivot == f => self.to_entry(pivot),
            Ordering::Greater => self.find(key, Bound::Included(pivot), to),
        }
    }

    pub(crate) fn find<Q>(
        &self,
        key: &Q,
        from: Bound<usize>,
        to: Bound<usize>,
    ) -> Result<DiskEntryM, Error>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let f = match from {
            Bound::Included(f) | Bound::Excluded(f) => f,
            Bound::Unbounded => 0,
        };
        let pivot = self.find_pivot(from, to);

        match key.cmp(self.to_key(pivot)?.borrow()) {
            Ordering::Less if pivot == f => unreachable!(),
            Ordering::Less => self.find(key, from, Bound::Excluded(pivot)),
            Ordering::Equal => self.to_entry(pivot),
            Ordering::Greater if pivot == f => self.to_entry(pivot),
            Ordering::Greater => self.find(key, Bound::Included(pivot), to),
        }
    }

    // [from, to)
    fn find_pivot(&self, from: Bound<usize>, to: Bound<usize>) -> usize {
        let to = match to {
            Bound::Excluded(to) => to,
            Bound::Unbounded => self.len(),
            Bound::Included(_) => unreachable!(),
        };
        let from = match from {
            Bound::Included(from) | Bound::Excluded(from) => from,
            Bound::Unbounded => 0,
        };
        match to - from {
            n if n < 1 => unreachable!(),
            n => from + (n / 2),
        }
    }

    pub fn to_entry(&self, index: usize) -> Result<DiskEntryM, Error> {
        let (block, count, offsets) = match self {
            MBlock::Decode {
                block,
                count,
                offsets,
                ..
            } => (block, *count, offsets),
            _ => unreachable!(),
        };
        if index < count {
            let offset = offsets[index..index + 4].try_into().unwrap();
            let offset: usize = u32::from_be_bytes(offset).try_into().unwrap();
            let mut mentry = DiskEntryM::to_entry(&block[offset..]);
            mentry.set_index(index);
            Ok(mentry)
        } else {
            Err(Error::__MBlockExhausted(index))
        }
    }

    fn to_key(&self, index: usize) -> Result<K, Error> {
        let (block, offsets) = match self {
            MBlock::Decode { block, offsets, .. } => (block, offsets),
            _ => unreachable!(),
        };
        let offset = offsets[index..index + 4].try_into().unwrap();
        let offset: usize = u32::from_be_bytes(offset).try_into().unwrap();
        DiskEntryM::to_key(&block[offset..])
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

pub(crate) enum ZBlock<K, V> {
    Encode {
        leaf: Vec<u8>, // buffer for z_block
        blob: Vec<u8>, // buffer for vlog
        offsets: Vec<u32>,
        des: Vec<DiskEntryZ>,
        vpos: u64,
        first_key: Option<K>,
        config: Config,
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
        ZBlock::Encode {
            leaf: Vec::with_capacity(config.z_blocksize),
            blob: Vec::with_capacity(config.v_blocksize),
            offsets: Default::default(),
            des: Default::default(),
            vpos,
            first_key: Default::default(),
            config,
        }
    }

    pub(crate) fn reset(&mut self, vpos: u64) {
        match self {
            ZBlock::Encode {
                leaf,
                blob,
                offsets,
                des,
                vpos: vpos_ref,
                first_key,
                ..
            } => {
                leaf.truncate(0);
                blob.truncate(0);
                offsets.truncate(0);
                des.truncate(0);
                *vpos_ref = vpos;
                first_key.take();
            }
            ZBlock::Decode { .. } => unreachable!(),
        }
    }

    pub(crate) fn as_first_key(&self) -> &K {
        match self {
            ZBlock::Encode { first_key, .. } => first_key.as_ref().unwrap(),
            ZBlock::Decode { .. } => unreachable!(),
        }
    }

    pub(crate) fn has_first_key(&self) -> bool {
        match self {
            ZBlock::Encode { first_key, .. } => match first_key {
                Some(_) => true,
                None => false,
            },
            ZBlock::Decode { .. } => unreachable!(),
        }
    }

    pub(crate) fn insert(
        &mut self,
        entry: &core::Entry<K, V>,
        stats: &mut Stats, // update build statistics
    ) -> Result<u64, Error> {
        use crate::robt_entry::DiskEntryZ as DZ;

        match self {
            ZBlock::Encode {
                leaf,
                blob,
                offsets,
                des,
                first_key,
                config,
                ..
            } => {
                let (m, x) = (leaf.len(), blob.len());
                let de = match (config.value_in_vlog, config.delta_ok) {
                    (false, false) => DZ::encode_l(entry, leaf, stats)?,
                    (false, true) => DZ::encode_ld(entry, leaf, blob, stats)?,
                    (true, false) => DZ::encode_lv(entry, leaf, blob, stats)?,
                    (true, true) => DZ::encode_lvd(entry, leaf, blob, stats)?,
                };
                des.push(de);

                let n = leaf.len();
                if n < config.z_blocksize {
                    offsets.push(m.try_into().unwrap());
                    first_key.get_or_insert_with(|| entry.as_key().clone());
                    Ok(offsets.len().try_into().unwrap())
                } else {
                    leaf.truncate(m);
                    blob.truncate(x);
                    Err(Error::__ZBlockOverflow(n))
                }
            }
            ZBlock::Decode { .. } => unreachable!(),
        }
    }

    pub(crate) fn finalize(&mut self, stats: &mut Stats) -> (u64, u64) {
        match self {
            ZBlock::Encode {
                leaf,
                blob,
                offsets,
                des,
                vpos,
                config,
                ..
            } => {
                let adjust = 4 + (offsets.len() * 4);
                let x: u32 = adjust.try_into().unwrap();
                offsets.iter_mut().for_each(|offset| *offset += x);
                // adjust the offset and encode
                let m = leaf.len();
                leaf.resize(m + adjust, 0);
                leaf.copy_within(0..m, adjust);
                // encode offset header
                let num: u32 = offsets.len().try_into().unwrap();
                &leaf[..4].copy_from_slice(&num.to_be_bytes());
                for (i, offset) in offsets.iter().enumerate() {
                    let x = (i + 1) * 4;
                    leaf[x..x + 4].copy_from_slice(&offset.to_be_bytes());
                }
                // adjust file position offsets for value and delta in vlog.
                des.iter().for_each(|de| de.encode_fpos(leaf, *vpos));
                // update statistics
                stats.padding += config.z_blocksize - leaf.len();
                stats.z_bytes += config.z_blocksize;
                stats.v_bytes += blob.len();
                // align blocks
                leaf.resize(config.z_blocksize, 0);

                (
                    config.z_blocksize.try_into().unwrap(), // full block
                    blob.len().try_into().unwrap(),
                )
            }
            ZBlock::Decode { .. } => unreachable!(),
        }
    }

    pub(crate) fn flush(&mut self, ifr: &mut Flusher, vfr: Option<&mut Flusher>) {
        match self {
            ZBlock::Encode { leaf, blob, .. } => {
                ifr.send(leaf.clone());
                vfr.map(|flusher| flusher.send(blob.clone()));
            }
            ZBlock::Decode { .. } => unreachable!(),
        }
    }
}

impl<K, V> ZBlock<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    pub(crate) fn new_decode(
        fd: &mut fs::File,
        fpos: u64,
        config: &Config, // open from configuration
    ) -> Result<ZBlock<K, V>, Error> {
        let n: u64 = config.z_blocksize.try_into().unwrap();
        let block = util::read_buffer(fd, fpos, n, "reading zblock")?;
        let count = u32::from_be_bytes(block[..4].try_into().unwrap());
        let adjust: usize = (4 + (count * 4)).try_into().unwrap();
        let offsets = &block[4..adjust] as *const [u8];

        Ok(ZBlock::Decode {
            block,
            count: count.try_into().unwrap(),
            offsets: unsafe { offsets.as_ref().unwrap() },
            phantom_val: marker::PhantomData,
        })
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        match self {
            ZBlock::Decode { count, .. } => *count,
            _ => unreachable!(),
        }
    }

    // return (index-to-entry, core::Entry), if None means key-not found
    pub(crate) fn find<Q>(
        &self,
        key: &Q,
        from: Bound<usize>,
        to: Bound<usize>,
    ) -> Result<(usize, core::Entry<K, V>), Error>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let f = match from {
            Bound::Included(f) | Bound::Excluded(f) => f,
            Bound::Unbounded => 0,
        };
        let pivot = self.find_pivot(from, to);

        match key.cmp(self.to_key(pivot)?.borrow()) {
            Ordering::Less if pivot == f => unreachable!(),
            Ordering::Less => self.find(key, from, Bound::Excluded(pivot)),
            Ordering::Equal => self.to_entry(pivot),
            Ordering::Greater if pivot == f => self.to_entry(pivot),
            Ordering::Greater => self.find(key, Bound::Included(pivot), to),
        }
    }

    // [from, to)
    fn find_pivot(&self, from: Bound<usize>, to: Bound<usize>) -> usize {
        let to = match to {
            Bound::Excluded(to) => to,
            Bound::Unbounded => self.len(),
            Bound::Included(_) => unreachable!(),
        };
        let from = match from {
            Bound::Included(from) | Bound::Excluded(from) => from,
            Bound::Unbounded => 0,
        };
        match to - from {
            n if n < 1 => unreachable!(),
            n => from + (n / 2),
        }
    }

    pub fn to_entry(
        // fetch (index, entry)
        &self,
        index: usize,
    ) -> Result<(usize, core::Entry<K, V>), Error> {
        let (block, count, offsets) = match self {
            ZBlock::Decode {
                block,
                count,
                offsets,
                ..
            } => (block, *count, offsets),
            _ => unreachable!(),
        };

        if index < count {
            let offset = offsets[index..index + 4].try_into().unwrap();
            let offset: usize = u32::from_be_bytes(offset).try_into().unwrap();
            Ok((index, DiskEntryZ::to_entry(&block[offset..])?))
        } else {
            Err(Error::__ZBlockExhausted(index))
        }
    }

    fn to_key(&self, index: usize) -> Result<K, Error> {
        let (block, offsets) = match self {
            ZBlock::Decode { block, offsets, .. } => (block, offsets),
            _ => unreachable!(),
        };
        let offset = offsets[index..index + 4].try_into().unwrap();
        let offset: usize = u32::from_be_bytes(offset).try_into().unwrap();
        DiskEntryZ::to_key(&block[offset..])
    }
}
