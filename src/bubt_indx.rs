// TODO: flush put blocks into tx channel. Right now we simply unwrap()

use std::{
    borrow::Borrow,
    convert::{TryFrom, TryInto},
    fs, marker,
    ops::Bound,
};

//use crate::bubt_build::FlushClient;
use crate::bubt_config::Config;
use crate::bubt_entry::DiskEntry;
use crate::bubt_stats::Stats;
use crate::core::{self, Diff, Serialize};
use crate::error::Error;
use crate::{util, vlog};

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

pub(crate) enum MBlock<K, V>
where
    K: Default + Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    Encode {
        mblock: Vec<u8>,
        offsets: Vec<u32>,
        first_key: Option<K>,
        config: Config,
    },
    Decode {
        count: usize,
        adjust: usize,
        offsets: Vec<u8>,
        entries: Vec<u8>,
        phantom_val: marker::PhantomData<V>,
    },
}

impl<K, V> MBlock<K, V>
where
    K: Default + Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    const FLAGS_ZBLOCK: u64 = 0x1000000000000000;

    pub(crate) fn new_encode(config: Config) -> MBlock<K, V> {
        MBlock::Encode {
            mblock: Vec::with_capacity(config.m_blocksize),
            offsets: Default::default(),
            first_key: None,
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

    pub(crate) fn first_key(&mut self) -> Option<K> {
        match self {
            MBlock::Encode { first_key, .. } => first_key.clone(),
            MBlock::Decode { .. } => unreachable!(),
        }
    }

    pub(crate) fn insertz(&mut self, key: &K, fpos: u64) -> Result<usize, Error> {
        match self {
            MBlock::Encode {
                mblock,
                offsets,
                first_key,
                config,
            } => {
                let m = mblock.len();
                DiskEntry::encode_m(None, Some(fpos), key, mblock)?;
                let n = mblock.len();
                if n < config.m_blocksize {
                    offsets.push(m as u32);
                    first_key.get_or_insert_with(|| key.clone());
                    Ok(offsets.len())
                } else {
                    mblock.truncate(m);
                    Err(Error::ZBlockOverflow(n))
                }
            }
            MBlock::Decode { .. } => unreachable!(),
        }
    }

    pub(crate) fn insertm(&mut self, key: &K, fpos: u64) -> Result<usize, Error> {
        match self {
            MBlock::Encode {
                mblock,
                offsets,
                first_key,
                config,
            } => {
                let m = mblock.len();
                DiskEntry::encode_m(Some(fpos), None, key, mblock)?;
                let n = mblock.len();
                if n < config.m_blocksize {
                    offsets.push(m as u32);
                    first_key.get_or_insert_with(|| key.clone());
                    Ok(offsets.len())
                } else {
                    mblock.truncate(m);
                    Err(Error::ZBlockOverflow(n))
                }
            }
            MBlock::Decode { .. } => unreachable!(),
        }
    }

    pub(crate) fn finalize(&mut self, stats: &mut Stats) {
        match self {
            MBlock::Encode {
                mblock,
                offsets,
                config,
                ..
            } => {
                let adjust = 4 + (offsets.len() * 4);
                offsets.iter_mut().for_each(|i| *i += adjust as u32);
                // adjust space for offset header
                let m = mblock.len();
                mblock.resize(m + adjust, 0);
                mblock.copy_within(0..m, adjust);
                // encode offset header
                let num = offsets.len() as u32;
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
            }
            MBlock::Decode { .. } => unreachable!(),
        }
    }

    //pub(crate) fn flush(&mut self, i_flusher: &mut FlushClient) {
    //    match self {
    //        MBlock::Encode { mblock, .. } => {
    //            i_flusher.send(mblock.clone());
    //        }
    //        MBlock::Decode { .. } => unreachable!(),
    //    }
    //}
}

impl<K, V> MBlock<K, V>
where
    K: Default + Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    pub(crate) fn new_decode(
        fd: &mut fs::File,
        fpos: u64,
        config: &Config,
    ) -> Result<MBlock<K, V>, Error> {
        let n: u64 = config.m_blocksize.try_into().unwrap();
        let block = util::read_buffer(fd, fpos, n, "reading mblock")?;
        let count = u32::from_be_bytes(block[..4].try_into().unwrap());
        let adjust = (4 + (count * 4)) as usize;
        Ok(MBlock::Decode {
            count: count as usize,
            adjust,
            offsets: block[4..adjust].to_vec(),
            entries: block[adjust..].to_vec(), // TODO: Avoid copy ?
            phantom_val: marker::PhantomData,
        })
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            MBlock::Decode { count, .. } => *count,
            _ => unreachable!(),
        }
    }

    // return (index-to-child-block, child-is-zblock, core::Entry)
    pub(crate) fn find(
        &self,
        key: &K,
        from: Bound<usize>,
        to: Bound<usize>,
    ) -> Result<(usize, bool, core::Entry<K, V>), Error> {
        let pivot = self.find_pivot(from, to)?;
        match (pivot, from) {
            (0, Bound::Included(f)) => self.entry_at(f),
            (n, _) => {
                if key.lt(self.key_at(n as usize)?.borrow()) {
                    self.find(key, from, Bound::Excluded(pivot as usize))
                } else {
                    self.find(key, Bound::Included(pivot as usize), to)
                }
            }
        }
    }

    fn find_pivot(&self, from: Bound<usize>, to: Bound<usize>) -> Result<isize, Error> {
        let count = match self {
            MBlock::Decode { count, .. } => *count,
            _ => unreachable!(),
        };
        let to = match to {
            Bound::Excluded(to) => to,
            Bound::Unbounded => count,
            Bound::Included(_) => unreachable!(),
        };
        let from = match from {
            Bound::Included(from) | Bound::Excluded(from) => from,
            Bound::Unbounded => 0,
        };
        match to - from {
            1 => Ok(0),
            n => Ok((n / 2).try_into().unwrap()),
        }
    }

    pub fn entry_at(&self, index: usize) -> Result<(usize, bool, core::Entry<K, V>), Error> {
        let (count, adjust, offsets, entries) = match self {
            MBlock::Decode {
                count,
                adjust,
                offsets,
                entries,
                ..
            } => (*count, *adjust, offsets, entries),
            _ => unreachable!(),
        };
        if index < count {
            let offset = offsets[index..index + 4].try_into().unwrap();
            let offset = u32::from_be_bytes(offset) as usize;
            let entry = &entries[offset - adjust..];
            let mut n = 0;
            let a = u64::from_be_bytes(entry[n..n + 8].try_into().unwrap());
            let zchild = (a & Self::FLAGS_ZBLOCK) == Self::FLAGS_ZBLOCK;
            let klen = (a >> 32) as usize;
            n += 8;

            let fpos = u64::from_be_bytes(entry[n..n + 8].try_into().unwrap());
            n += 8;

            let mut key: K = Default::default();
            key.decode(&entry[n..n + klen])?;

            let value = vlog::Value::Reference { fpos, length: 0 };
            let value = core::Value::new_upsert(value, Default::default());
            Ok((index, zchild, core::Entry::new(key, value)))
        } else {
            Err(Error::MBlockExhausted)
        }
    }

    fn key_at(&self, index: usize) -> Result<K, Error> {
        let (adjust, offsets, entries) = match self {
            MBlock::Decode {
                adjust,
                offsets,
                entries,
                ..
            } => (*adjust, offsets, entries),
            _ => unreachable!(),
        };
        let offset = offsets[index..index + 4].try_into().unwrap();
        let offset = u32::from_be_bytes(offset) as usize;
        let entry = &entries[offset - adjust..];
        let mut n = 0;
        let klen = entry[n..n + 8].try_into().unwrap();
        let klen = (u64::from_be_bytes(klen) >> 32) as usize;
        n += 8;

        n += 8;

        let mut key: K = Default::default();
        key.decode(&entry[n..n + klen])?;
        Ok(key)
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
// |                ZEntry-1                  |
// *-------------------*----------------------* ...
// |                ........                  |
// *-------------------*----------------------* n-entry-offset
// |                ZEntry-n                  |
// *------------------------------------------*

pub(crate) enum ZBlock<K, V>
where
    K: Default + Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    Encode {
        leaf: Vec<u8>, // buffer for z_block
        blob: Vec<u8>, // buffer for vlog
        offsets: Vec<u32>,
        des: Vec<DiskEntry>,
        vpos: u64,
        first_key: Option<K>,
        config: Config,
    },
    Decode {
        count: usize,
        adjust: usize,
        offsets: Vec<u8>,
        entries: Vec<u8>,
        phantom_val: marker::PhantomData<V>,
    },
}

impl<K, V> ZBlock<K, V>
where
    K: Default + Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
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

    pub(crate) fn first_key(&self) -> Option<K> {
        match self {
            ZBlock::Encode { first_key, .. } => first_key.clone(),
            ZBlock::Decode { .. } => unreachable!(),
        }
    }

    pub(crate) fn insert(
        &mut self,
        entry: &core::Entry<K, V>,
        s: &mut Stats, // update build statistics
    ) -> Result<usize, Error> {
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
                    (false, false) => DiskEntry::encode_l(entry, leaf, s)?,
                    (false, true) => DiskEntry::encode_ld(entry, leaf, blob, s)?,
                    (true, false) => DiskEntry::encode_lv(entry, leaf, blob, s)?,
                    (true, true) => DiskEntry::encode_lvd(entry, leaf, blob, s)?,
                };
                des.push(de);

                let n = leaf.len();
                if n < config.z_blocksize {
                    offsets.push(m as u32);
                    first_key.get_or_insert_with(|| entry.as_key().clone());
                    Ok(offsets.len())
                } else {
                    leaf.truncate(m);
                    blob.truncate(x);
                    Err(Error::ZBlockOverflow(n))
                }
            }
            ZBlock::Decode { .. } => unreachable!(),
        }
    }

    pub(crate) fn finalize(&mut self, stats: &mut Stats) {
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
                offsets.iter_mut().for_each(|i| *i += adjust as u32);
                // adjust the offset and encode
                let m = leaf.len();
                leaf.resize(m + adjust, 0);
                leaf.copy_within(0..m, adjust);
                // encode offset header
                let num = offsets.len() as u32;
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
            }
            ZBlock::Decode { .. } => unreachable!(),
        }
    }

    //pub(crate) fn flush(
    //    &mut self,
    //    i_flusher: &mut FlushClient,
    //    v_flusher: Option<&mut FlushClient>,
    //) {
    //    match self {
    //        ZBlock::Encode { leaf, blob, ..  } => {
    //            i_flusher.send(leaf.clone());
    //            v_flusher.map(|x| x.send(blob.clone()));
    //        }
    //        ZBlock::Decode { .. } => unreachable!(),
    //    }
    //}
}

impl<K, V> ZBlock<K, V>
where
    K: Default + Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    const VLEN_MASK: u64 = 0x0FFFFFFFFFFFFFFF;
    const DLEN_MASK: u64 = 0x0FFFFFFFFFFFFFFF;

    pub(crate) fn new_decode(
        fd: &mut fs::File,
        fpos: u64,
        config: &Config, // open from configuration
    ) -> Result<ZBlock<K, V>, Error> {
        let n: u64 = config.z_blocksize.try_into().unwrap();
        let block = util::read_buffer(fd, fpos, n, "reading zblock")?;
        let count = u32::from_be_bytes(block[..4].try_into().unwrap());
        let adjust = 4 + (count * 4) as usize;
        Ok(ZBlock::Decode {
            count: count as usize,
            adjust,
            offsets: block[4..adjust].to_vec(),
            entries: block[adjust..].to_vec(), // TODO: Avoid copy ?
            phantom_val: marker::PhantomData,
        })
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            ZBlock::Decode { count, .. } => *count,
            _ => unreachable!(),
        }
    }

    // return (index-to-entry, core::Entry)
    pub(crate) fn find(
        &self,
        key: &K,
        from: Bound<usize>,
        to: Bound<usize>,
    ) -> Result<(usize, core::Entry<K, V>), Error> {
        let pivot = self.find_pivot(from, to)?;
        match (pivot, from) {
            (0, Bound::Included(f)) => self.entry_at(f),
            (n, _) => {
                if key.lt(self.key_at(n as usize)?.borrow()) {
                    self.find(key, from, Bound::Excluded(pivot as usize))
                } else {
                    self.find(key, Bound::Included(pivot as usize), to)
                }
            }
        }
    }

    fn find_pivot(&self, from: Bound<usize>, to: Bound<usize>) -> Result<isize, Error> {
        let count = match self {
            ZBlock::Decode { count, .. } => count,
            _ => unreachable!(),
        };
        let to = match to {
            Bound::Excluded(to) => to as usize,
            Bound::Unbounded => *count,
            Bound::Included(_) => unreachable!(),
        };
        let from = match from {
            Bound::Included(from) | Bound::Excluded(from) => from,
            Bound::Unbounded => 0,
        };
        match to - from {
            1 => Ok(0),
            n => Ok(isize::try_from(n).unwrap() / 2),
        }
    }

    pub fn entry_at(&self, index: usize) -> Result<(usize, core::Entry<K, V>), Error> {
        let (count, adjust, offsets, entries) = match self {
            ZBlock::Decode {
                count,
                adjust,
                offsets,
                entries,
                ..
            } => (*count, *adjust, offsets, entries),
            _ => unreachable!(),
        };
        if index < count {
            let offset = &offsets[index..index + 4];
            let offset = u32::from_be_bytes(offset.try_into().unwrap()) as usize;
            let entry = &entries[offset - adjust..];
            let mut n = 0;
            let a = u64::from_be_bytes(entry[n..n + 8].try_into().unwrap());
            let n_deltas = a & 0xFFFFFFFF;
            let klen = (a >> 32) as usize;
            n += 8;

            let vlen = u64::from_be_bytes(entry[n..n + 8].try_into().unwrap());
            n += 8;
            let vref = (vlen & Self::VLEN_MASK) == Self::VLEN_MASK;
            let vlen: usize = (vlen & (!Self::VLEN_MASK)).try_into().unwrap();

            let seqno = u64::from_be_bytes(entry[n..n + 8].try_into().unwrap());
            n += 8;

            let deleted = entry[n..n + 8].try_into().unwrap();
            let deleted = match u64::from_be_bytes(deleted) {
                0 => None,
                n => Some(n),
            };
            n += 8;

            let mut key: K = Default::default();
            key.decode(&entry[n..n + klen])?;
            n += klen;

            let value = if vref {
                let fpos = entry[n..n + 8].try_into().unwrap();
                let fpos = u64::from_be_bytes(fpos);
                n += 8;
                vlog::Value::Reference {
                    fpos,
                    length: vlen as u64,
                }
            } else {
                let mut value: V = Default::default();
                value.decode(&entry[n..n + vlen])?;
                n += vlen;
                vlog::Value::Native { value }
            };
            let value = match deleted {
                None => core::Value::new_upsert(value, seqno),
                Some(del) => core::Value::new_delete(del),
            };

            let mut deltas: Vec<core::Delta<V>> = vec![];
            for _i in 0..n_deltas {
                let dlen = entry[n..n + 8].try_into().unwrap();
                let mut dlen = u64::from_be_bytes(dlen);
                dlen = dlen & (!Self::DLEN_MASK);
                n += 8;

                let seqno = entry[n..n + 8].try_into().unwrap();
                let seqno = u64::from_be_bytes(seqno);
                n += 8;

                let deleted = entry[n..n + 8].try_into().unwrap();
                let deleted = match u64::from_be_bytes(deleted) {
                    0 => None,
                    n => Some(n),
                };
                n += 8;

                let fpos = entry[n..n + 8].try_into().unwrap();
                let fpos = u64::from_be_bytes(fpos);
                n += 8;

                let delta = vlog::Delta::Reference { fpos, length: dlen };
                match deleted {
                    None => deltas.push(core::Delta::new_upsert(delta, seqno)),
                    Some(del) => deltas.push(core::Delta::new_delete(del)),
                }
            }
            Ok((index, core::Entry::new(key, value)))
        } else {
            Err(Error::ZBlockExhausted)
        }
    }

    fn key_at(&self, index: usize) -> Result<K, Error> {
        let (adjust, offsets, entries) = match self {
            ZBlock::Decode {
                adjust,
                offsets,
                entries,
                ..
            } => (*adjust, offsets, entries),
            _ => unreachable!(),
        };
        let offset = offsets[index..index + 4].try_into().unwrap();
        let offset = u32::from_be_bytes(offset) as usize;
        let entry = &entries[offset - adjust..];
        let mut n = 0;
        let klen = entry[n..n + 8].try_into().unwrap();
        let klen = (u64::from_be_bytes(klen) >> 32) as usize;
        n += 8;

        n += 8 + 8 + 8;

        let mut key: K = Default::default();
        key.decode(&entry[n..n + klen])?;
        Ok(key)
    }
}
