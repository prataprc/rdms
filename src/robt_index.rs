// TODO: flush put blocks into tx channel. Right now we simply unwrap()

use std::{borrow::Borrow, cmp::Ordering, convert::TryInto, fs, marker, ops::Bound};

use crate::{
    core::{self, Diff, Result, Serialize},
    error::Error,
    robt::{Config, Flusher, Stats},
    robt_entry::{MEntry, ZEntry},
    util,
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

    pub(crate) fn as_first_key(&self) -> &K {
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
                    offsets.push(offset.try_into().unwrap());
                    first_key.get_or_insert_with(|| key.clone());
                    Ok(offsets.len().try_into().unwrap())
                } else {
                    mblock.truncate(offset);
                    Err(Error::__MBlockOverflow(n))
                }
            }
            MBlock::Decode { .. } => unreachable!(),
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
                    offsets.push(offset.try_into().unwrap());
                    first_key.get_or_insert_with(|| key.clone());
                    Ok(offsets.len().try_into().unwrap())
                } else {
                    mblock.truncate(offset);
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
                m_blocksize,
                ..
            } => {
                let adjust: u32 = {
                    let m = mblock.len();
                    let adjust = 4 + (offsets.len() * 4);
                    mblock.resize(m + adjust, 0);
                    mblock.copy_within(0..m, adjust);

                    adjust.try_into().unwrap()
                };
                // encode offset header
                let num: u32 = offsets.len().try_into().unwrap();
                &mblock[..4].copy_from_slice(&num.to_be_bytes());
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

                (*m_blocksize).try_into().unwrap()
            }
            MBlock::Decode { .. } => unreachable!(),
        }
    }

    pub(crate) fn flush(&mut self, flusher: &mut Flusher) -> Result<()> {
        match self {
            MBlock::Encode { mblock, .. } => flusher.send(mblock.clone()),
            MBlock::Decode { .. } => unreachable!(),
        }
    }

    #[cfg(test)]
    fn buffer(&self) -> Vec<u8> {
        match self {
            MBlock::Encode { mblock, .. } => mblock.clone(),
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
    ) -> Result<MBlock<K, V>> {
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
            MBlock::Encode { offsets, .. } => offsets.len(),
        }
    }

    // optimized version of find() for mblock. if key is less than the dataset
    // immediately returns with failure.
    pub(crate) fn get<Q>(
        &self,
        key: &Q,
        from: Bound<usize>, // unbounded
        to: Bound<usize>,   // unbounded
    ) -> Result<MEntry<K>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let f = match from {
            Bound::Included(f) | Bound::Excluded(f) => f,
            Bound::Unbounded => 0,
        };
        let pivot = self.find_pivot(from, to);

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
            Ordering::Less if pivot == f => unreachable!(),
            Ordering::Less => self.get(key, from, Bound::Excluded(pivot)),
            Ordering::Equal => self.to_entry(pivot),
            Ordering::Greater if pivot == f => self.to_entry(pivot),
            Ordering::Greater => self.get(key, Bound::Included(pivot), to),
        }
    }

    pub(crate) fn find<Q>(
        &self,
        key: &Q,
        from: Bound<usize>, // begins as unbounded
        to: Bound<usize>,   // begins as unbounded
    ) -> Result<MEntry<K>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let f = match from {
            Bound::Included(f) | Bound::Excluded(f) => f,
            Bound::Unbounded => 0,
        };
        let pivot = self.find_pivot(from, to);

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
            Ordering::Less if pivot == f => unreachable!(),
            Ordering::Less => self.find(key, from, Bound::Excluded(pivot)),
            Ordering::Equal => self.to_entry(pivot),
            Ordering::Greater if pivot == f => self.to_entry(pivot),
            Ordering::Greater => self.find(key, Bound::Included(pivot), to),
        }
    }

    // [from, to)
    fn find_pivot(&self, from: Bound<usize>, to: Bound<usize>) -> usize {
        let from = match from {
            Bound::Included(from) => from,
            Bound::Unbounded => 0,
            Bound::Excluded(_) => unreachable!(),
        };
        let to = match to {
            Bound::Excluded(to) => to,
            Bound::Unbounded => self.len(),
            Bound::Included(_) => unreachable!(),
        };
        match to - from {
            n if n > 0 => from + (n / 2),
            _ => unreachable!(),
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
            MBlock::Encode { .. } => unreachable!(),
        };
        if index < count {
            let idx = index * 4;
            let offset = offsets[idx..idx + 4].try_into().unwrap();
            let offset: usize = u32::from_be_bytes(offset).try_into().unwrap();
            Ok(MEntry::decode_entry(&block[offset..], index))
        } else {
            Err(Error::__MBlockExhausted(index))
        }
    }

    fn to_key(&self, index: usize) -> Result<K> {
        let (block, count, offsets) = match self {
            MBlock::Decode {
                block,
                count,
                offsets,
                ..
            } => (block, *count, offsets),
            MBlock::Encode { .. } => unreachable!(),
        };
        if index < count {
            let idx = index * 4;
            let offset = offsets[idx..idx + 4].try_into().unwrap();
            let offset: usize = u32::from_be_bytes(offset).try_into().unwrap();
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
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
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
            offsets: Vec::with_capacity(64),  // TODO: no magic number
            zentries: Vec::with_capacity(64), // TODO: no magic number
            vpos,
            first_key: Default::default(),
            // configuration
            z_blocksize,
            value_in_vlog,
            delta_ok,
        }
    }

    pub(crate) fn reset(&mut self, vpos: u64) {
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
                let (k, v, d) = de.to_kvd_stats();
                zentries.push(de);

                let n = 4 + ((offsets.len() + 1) * 4) + leaf.len();
                if n < *z_blocksize {
                    stats.key_mem += k;
                    stats.val_mem += v;
                    stats.diff_mem += d;
                    offsets.push(leaf_i.try_into().unwrap());
                    first_key.get_or_insert_with(|| entry.as_key().clone());
                    Ok(offsets.len().try_into().unwrap())
                } else {
                    leaf.truncate(leaf_i);
                    blob.truncate(blob_i);
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

                    adjust.try_into().unwrap()
                };
                // encode offset header
                let num: u32 = offsets.len().try_into().unwrap();
                &leaf[..4].copy_from_slice(&num.to_be_bytes());
                for (i, offset) in offsets.iter().enumerate() {
                    let x = (i + 1) * 4;
                    let offset_bytes = (adjust + offset).to_be_bytes();
                    leaf[x..x + 4].copy_from_slice(&offset_bytes);
                    // adjust file position offsets for value and delta in vlog.
                    let j = (adjust + offset) as usize;
                    zentries[i].re_encode_fpos(&mut leaf[j..], *vpos);
                }
                // update statistics
                stats.padding += *z_blocksize - leaf.len();
                stats.z_bytes += *z_blocksize;
                stats.v_bytes += blob.len();
                // align blocks
                leaf.resize(*z_blocksize, 0);

                (
                    (*z_blocksize).try_into().unwrap(), // full block
                    blob.len().try_into().unwrap(),
                )
            }
            ZBlock::Decode { .. } => unreachable!(),
        }
    }

    pub(crate) fn flush(
        &mut self,
        x: &mut Flusher,         // flush to index file
        y: Option<&mut Flusher>, // flush to optional vlog file
    ) -> Result<()> {
        match self {
            ZBlock::Encode { leaf, blob, .. } => {
                x.send(leaf.clone())?;
                y.map(|flusher| flusher.send(blob.clone())).transpose()?;
            }
            ZBlock::Decode { .. } => unreachable!(),
        }
        Ok(())
    }

    #[cfg(test)]
    fn buffer(&self) -> (Vec<u8>, Vec<u8>) {
        match self {
            ZBlock::Encode { leaf, blob, .. } => (leaf.clone(), blob.clone()),
            ZBlock::Decode { .. } => unreachable!(),
        }
    }
}

impl<K, V> ZBlock<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    pub(crate) fn new_decode(
        fd: &mut fs::File,
        fpos: u64,
        config: &Config, // open from configuration
    ) -> Result<ZBlock<K, V>> {
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
            ZBlock::Encode { .. } => unreachable!(),
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
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let f = match from {
            Bound::Included(f) | Bound::Excluded(f) => f,
            Bound::Unbounded => 0,
        };
        let pivot = self.find_pivot(from, to);

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
            Ordering::Less if pivot == f => unreachable!(),
            Ordering::Less => self.find(key, from, Bound::Excluded(pivot)),
            Ordering::Equal => self.to_entry(pivot),
            Ordering::Greater if pivot == f => Err(Error::__ZBlockExhausted(f)),
            Ordering::Greater => self.find(key, Bound::Included(pivot), to),
        }
    }

    // [from, to)
    fn find_pivot(&self, from: Bound<usize>, to: Bound<usize>) -> usize {
        let from = match from {
            Bound::Included(from) | Bound::Excluded(from) => from,
            Bound::Unbounded => 0,
        };
        let to = match to {
            Bound::Excluded(to) => to,
            Bound::Unbounded => self.len(),
            Bound::Included(_) => unreachable!(),
        };
        match to - from {
            n if n > 0 => from + (n / 2),
            _ => unreachable!(),
        }
    }

    pub fn to_entry(
        // fetch (index, entry)
        &self,
        index: usize,
    ) -> Result<(usize, core::Entry<K, V>)> {
        let (block, count, offsets) = match self {
            ZBlock::Decode {
                block,
                count,
                offsets,
                ..
            } => (block, *count, offsets),
            ZBlock::Encode { .. } => unreachable!(),
        };

        if index < count {
            let idx = index * 4;
            let offset = offsets[idx..idx + 4].try_into().unwrap();
            let offset: usize = u32::from_be_bytes(offset).try_into().unwrap();
            let entry = &block[offset..];
            Ok((index, ZEntry::decode_entry(entry)?))
        } else {
            Err(Error::__ZBlockExhausted(index))
        }
    }

    fn to_key(&self, index: usize) -> Result<K> {
        let (block, offsets) = match self {
            ZBlock::Decode { block, offsets, .. } => (block, offsets),
            ZBlock::Encode { .. } => unreachable!(),
        };
        let idx = index * 4;
        let offset = offsets[idx..idx + 4].try_into().unwrap();
        let offset: usize = u32::from_be_bytes(offset).try_into().unwrap();
        let entry = &block[offset..];
        ZEntry::<K, V>::decode_key(entry)
    }
}

#[cfg(test)]
#[path = "robt_index_test.rs"]
mod robt_index_test;
