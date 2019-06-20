// TODO: compute diffmem similar to keymem and valmem

use crate::bubt_stats::Stats;
use crate::core::{self, Diff, Serialize};
use crate::error::Error;
use crate::util;
use crate::vlog;

use std::{convert::TryInto, mem};

// Binary format (interMediate-Entry):
//
// *------*------------*----------------------*
// |flags |   reserved |   32-bit key-len     |
// *------*------------*----------------------*
// |             child-block fpos             |
// *-------------------*----------------------*
// |                  key                     |
// *-------------------*----------------------*
//
// Flags:
// * bit 60 set means child-block is a ZBlock.
// * bit 61 reserved
// * bit 62 reserved
// * bit 63 reserved

pub(crate) enum DiskEntryM<K>
where
    K: Serialize,
{
    M { size: usize },
    Entry { z: bool, key: K, fpos: u64 },
}

impl<K> DiskEntryM<K>
where
    K: Serialize,
{
    const ZBLOCK_FLAG: u64 = 0x1000000000000000;
    const KLEN_SHIFT: u64 = 32;

    pub(crate) fn encode_m(
        mfpos: Option<u64>, // file offset point to child m-block
        zfpos: Option<u64>, // file offset point to child z-block
        key: &K,
        mblock: &mut Vec<u8>, // output
    ) -> Result<DiskEntryM<K>, Error>
    where
        K: Clone + Ord + Serialize,
    {
        // encode key
        let m = mblock.len();
        let k = key.encode(mblock);
        if k >= core::Entry::<i32, i32>::KEY_SIZE_LIMIT {
            return Err(Error::KeySizeExceeded(k));
        }
        let n = mblock.len();
        // adjust space for header.
        mblock.resize(n + 16, 0);
        mblock.copy_within(m..m + n, 16);
        // encode header
        let k: u64 = util::try_convert_int(k, "key-len: usize->u64")?;
        let (scratch, fpos) = match (mfpos, zfpos) {
            (Some(fpos), None) => (k.to_be_bytes(), fpos),
            (None, Some(fpos)) => ((k | Self::ZBLOCK_FLAG).to_be_bytes(), fpos),
            _ => unreachable!(),
        };
        mblock[..8].copy_from_slice(&scratch);
        mblock[8..16].copy_from_slice(&fpos.to_be_bytes());
        Ok(DiskEntryM::M {
            size: mblock.len() - m,
        })
    }

    pub fn to_entry(entry: &[u8]) -> Result<DiskEntryM<K>, Error> {
        let hdr1 = u64::from_be_bytes(entry[0..8].try_into().unwrap());
        let z = (hdr1 & Self::ZBLOCK_FLAG) == Self::ZBLOCK_FLAG;
        let klen: usize = (hdr1 >> Self::KLEN_SHIFT).try_into().unwrap();

        let fpos = u64::from_be_bytes(entry[8..15].try_into().unwrap());

        let mut key: K = unsafe { mem::zeroed() };
        key.decode(&entry[24..24 + klen])?;

        Ok(DiskEntryM::Entry { key, fpos, z })
    }

    pub(crate) fn to_key(entry: &[u8]) -> Result<K, Error>
    where
        K: Serialize,
    {
        let hdr1 = u64::from_be_bytes(entry[0..8].try_into().unwrap());
        let klen: usize = (hdr1 >> Self::KLEN_SHIFT).try_into().unwrap();
        let mut key: K = unsafe { mem::zeroed() };
        key.decode(&entry[24..24 + klen])?;
        Ok(key)
    }
}

// Binary format (Delta):
//
// *-----*------------------------------------*
// |flags|      60-bit delta-len              |
// *-----*------------------------------------*
// |              64-bit seqno                |
// *-------------------*----------------------*
// |               delta-fpos                 |
// *------------------------------------------*
//
// Flags:
//
// * bit 60: 0 means delete operation, 1 means upsert operation
// * bit 61: reserved
// * bit 62: reserved
// * bit 63: reserved
//
// NOTE: delta-len includes serialized-diff plus metadata.

struct DiskDelta;

impl DiskDelta {
    const UPSERT_FLAG: u64 = 0x1000000000000000;

    pub(crate) fn encode<V>(
        delta: &core::Delta<V>, // input
        leaf: &mut Vec<u8>,     // output
        blob: &mut Vec<u8>,     // output
    ) -> Result<usize, Error>
    where
        V: Clone + Diff,
        <V as Diff>::D: Serialize,
    {
        match delta.as_ref() {
            core::DeltaTuck::U { delta, seqno } => {
                let m = blob.len();
                vlog::encode_delta(&delta, blob)?;
                let n = blob.len();
                let x: u64 = util::try_convert_int(n, "delta-len: usize->u64")?;
                let x = x | Self::UPSERT_FLAG;
                leaf.extend_from_slice(&x.to_be_bytes()); // diff-len
                leaf.extend_from_slice(&seqno.to_be_bytes());
                let m: u64 = util::try_convert_int(m, "doff: usize->u64")?;
                leaf.extend_from_slice(&m.to_be_bytes()); // fpos
                Ok(n)
            }
            core::DeltaTuck::D { deleted } => {
                leaf.extend_from_slice(&0_u64.to_be_bytes()); // diff-len
                leaf.extend_from_slice(&deleted.to_be_bytes());
                leaf.extend_from_slice(&0_u64.to_be_bytes()); // fpos
                Ok(0)
            }
        }
    }
}

// Binary format (Leaf Entry):
//
// *-------------------*----------------------*
// |  32-bit key len   |   number of deltas   |
// *-------------------*----------------------*
// |flags|      60-bit value-len              |
// *-----*------------------------------------*
// |              64-bit seqno                |
// *-------------------*----------------------*
// |                  key                     |
// *-------------------*----------------------*
// |              value / fpos                |
// *------------------------------------------*
// |                zdelta 1                  |
// *------------------------------------------*
// |                zdelta 2                  |
// *------------------------------------------*
//
// Flags:
// * bit 60: 0 means delete operation, 1 means upsert operation
// * bit 61: 0 means value in leaf-node, 1 means value in vlog-file
// * bit 62: reserved
// * bit 63: reserved
//
// NOTE: if value is fpos, value-len includes serialized-value plus metadata.

pub(crate) enum DiskEntryZ {
    // encode {key, value} entry into Z-Block
    L {
        size: usize,
    },
    // encode {key, value} entry into Z-Block and delta in value-log
    LD {
        size: usize,
        doff: usize,
    },
    // encode key entry into Z-Block, while value in value-log
    LV {
        size: usize,
        voff: usize,
    },
    // encode key entry into Z-Block, while value and delta in value-log
    LVD {
        size: usize,
        voff: usize,
        doff: usize,
    },
}

impl DiskEntryZ {
    const UPSERT_FLAG: u64 = 0x1000000000000000;
    const VLOG_FLAG: u64 = 0x2000000000000000;
    const VLEN_MASK: u64 = 0x0FFFFFFFFFFFFFFF;
    const DLEN_MASK: u64 = 0x0FFFFFFFFFFFFFFF;
    const NDELTA_MASK: u64 = 0xFFFFFFFF;
    const KLEN_SHIFT: u64 = 32;

    pub(crate) fn encode_l<K, V>(
        entry: &core::Entry<K, V>,
        leaf: &mut Vec<u8>,
        stats: &mut Stats,
    ) -> Result<DiskEntryZ, Error>
    where
        K: Clone + Ord + Serialize,
        V: Clone + Diff + Serialize,
        <V as Diff>::D: Serialize,
    {
        let m = leaf.len();
        let klen = DiskEntryZ::encode_key::<K, V>(entry.as_key(), leaf)?;
        stats.keymem += klen;
        let n = leaf.len();
        // adjust space for header.
        leaf.resize(n + 24, 0);
        leaf.copy_within(m..n, 24);
        // encode value
        let (vlen, isd, seqno) = DiskEntryZ::encode_value(entry, leaf)?;
        stats.valmem += vlen;
        // encode header.
        let dlen = 0_usize;
        leaf[..8].copy_from_slice(&DiskEntryZ::encode_hdr1(klen, dlen)?);
        leaf[8..16].copy_from_slice(&DiskEntryZ::encode_hdr2(vlen, isd, false)?);
        leaf[16..24].copy_from_slice(&DiskEntryZ::encode_hdr3(seqno));

        Ok(DiskEntryZ::L {
            size: leaf.len() - m,
        })
    }

    pub(crate) fn encode_ld<K, V>(
        entry: &core::Entry<K, V>,
        leaf: &mut Vec<u8>,
        blob: &mut Vec<u8>,
        stats: &mut Stats,
    ) -> Result<DiskEntryZ, Error>
    where
        K: Clone + Ord + Serialize,
        V: Clone + Diff + Serialize,
        <V as Diff>::D: Serialize,
    {
        let m = leaf.len();
        let klen = DiskEntryZ::encode_key::<K, V>(entry.as_key(), leaf)?;
        stats.keymem += klen;
        let n = leaf.len();
        // adjust space for header.
        leaf.resize(n + 24, 0);
        leaf.copy_within(m..n, 24);
        // encode value
        let (vlen, isd, seqno) = DiskEntryZ::encode_value(entry, leaf)?;
        stats.valmem += vlen;
        // encode header.
        let dlen = entry.to_delta_count();
        leaf[..8].copy_from_slice(&DiskEntryZ::encode_hdr1(klen, dlen)?);
        leaf[8..16].copy_from_slice(&DiskEntryZ::encode_hdr2(vlen, isd, false)?);
        leaf[16..24].copy_from_slice(&DiskEntryZ::encode_hdr3(seqno));
        // encode deltas
        let doff = leaf.len();
        DiskEntryZ::encode_delta(entry, leaf, blob)?;

        Ok(DiskEntryZ::LD {
            size: leaf.len() - m,
            doff,
        })
    }

    pub(crate) fn encode_lv<K, V>(
        entry: &core::Entry<K, V>,
        leaf: &mut Vec<u8>,
        blob: &mut Vec<u8>,
        stats: &mut Stats,
    ) -> Result<DiskEntryZ, Error>
    where
        K: Clone + Ord + Serialize,
        V: Clone + Diff + Serialize,
        <V as Diff>::D: Serialize,
    {
        let m = leaf.len();
        let klen = DiskEntryZ::encode_key::<K, V>(entry.as_key(), leaf)?;
        stats.keymem += klen;
        let n = leaf.len();
        // adjust space for header.
        leaf.resize(n + 24, 0);
        leaf.copy_within(m..n, 24);
        // encode value
        let x = blob.len();
        let (vlen, isd, seqno) = DiskEntryZ::encode_value(entry, blob)?;
        stats.valmem += vlen;
        let voff = leaf.len();
        let x: u64 = util::try_convert_int(x, "voff: usize->u64")?;
        leaf.extend_from_slice(&x.to_be_bytes());
        // encode header.
        let dlen = 0_usize;
        leaf[..8].copy_from_slice(&DiskEntryZ::encode_hdr1(klen, dlen)?);
        leaf[8..16].copy_from_slice(&DiskEntryZ::encode_hdr2(vlen, isd, true)?);
        leaf[16..24].copy_from_slice(&DiskEntryZ::encode_hdr3(seqno));

        Ok(DiskEntryZ::LV {
            size: leaf.len() - m,
            voff,
        })
    }

    pub(crate) fn encode_lvd<K, V>(
        entry: &core::Entry<K, V>,
        leaf: &mut Vec<u8>,
        blob: &mut Vec<u8>,
        stats: &mut Stats,
    ) -> Result<DiskEntryZ, Error>
    where
        K: Clone + Ord + Serialize,
        V: Clone + Diff + Serialize,
        <V as Diff>::D: Serialize,
    {
        let m = leaf.len();
        let klen = DiskEntryZ::encode_key::<K, V>(entry.as_key(), leaf)?;
        stats.keymem += klen;
        let n = leaf.len();
        // adjust space for header.
        leaf.resize(n + 24, 0);
        leaf.copy_within(m..n, 24);
        // encode value
        let x = blob.len();
        let (vlen, isd, seqno) = DiskEntryZ::encode_value(entry, blob)?;
        stats.valmem += vlen;
        let voff = leaf.len();
        let x: u64 = util::try_convert_int(x, "voff: usize->u64")?;
        leaf.extend_from_slice(&x.to_be_bytes());
        // encode header.
        let dlen = entry.to_delta_count();
        leaf[..8].copy_from_slice(&DiskEntryZ::encode_hdr1(klen, dlen)?);
        leaf[8..16].copy_from_slice(&DiskEntryZ::encode_hdr2(vlen, isd, true)?);
        leaf[16..24].copy_from_slice(&DiskEntryZ::encode_hdr3(seqno));
        // encode deltas
        let doff = leaf.len();
        DiskEntryZ::encode_delta(entry, leaf, blob)?;

        Ok(DiskEntryZ::LVD {
            size: leaf.len() - m,
            voff,
            doff,
        })
    }

    #[inline]
    fn encode_hdr1(k: usize, d: usize) -> Result<[u8; 8], Error> {
        let klen: u64 = util::try_convert_int(k, "key-len: usize->u64")?;
        let dlen: u64 = util::try_convert_int(d, "num-deltas usize->u64")?;
        Ok(((klen << Self::KLEN_SHIFT) | dlen).to_be_bytes())
    }

    #[inline]
    fn encode_hdr2(v: usize, isd: bool, vlog: bool) -> Result<[u8; 8], Error> {
        let mut vlen: u64 = util::try_convert_int(v, "value-len: usize->u64")?;
        if !isd {
            vlen |= Self::UPSERT_FLAG;
        }
        if vlog {
            vlen |= Self::VLOG_FLAG;
        }
        Ok(vlen.to_be_bytes())
    }

    #[inline]
    fn encode_hdr3(seqno: u64) -> [u8; 8] {
        seqno.to_be_bytes()
    }

    fn encode_key<K, V>(
        key: &K,           // input
        buf: &mut Vec<u8>, // output
    ) -> Result<usize, Error>
    where
        K: Ord + Clone + Serialize,
        V: Clone + Diff,
    {
        let n = key.encode(buf);
        if n < core::Entry::<i32, i32>::KEY_SIZE_LIMIT {
            Ok(n)
        } else {
            Err(Error::KeySizeExceeded(n))
        }
    }

    fn encode_value<K, V>(
        entry: &core::Entry<K, V>, // input
        blob: &mut Vec<u8>,        // output
    ) -> Result<(usize, bool, u64), Error>
    where
        K: Ord + Clone,
        V: Clone + Diff + Serialize,
    {
        match entry.as_value() {
            core::Value::U { value, seqno } => {
                let vlen = vlog::encode_value(value, blob)?;
                Ok((vlen, false, *seqno))
            }
            core::Value::D { deleted } => Ok((0, true, *deleted)),
        }
    }

    fn encode_delta<K, V>(
        entry: &core::Entry<K, V>, // input
        leaf: &mut Vec<u8>,        // output
        blob: &mut Vec<u8>,        // output
    ) -> Result<usize, Error>
    where
        K: Ord + Clone,
        V: Clone + Diff,
        <V as Diff>::D: Serialize,
    {
        let mut n = 0_usize;
        for delta in entry.as_deltas() {
            n += DiskDelta::encode(delta, leaf, blob)?;
        }
        Ok(n)
    }

    pub(crate) fn encode_fpos(&self, leaf: &mut Vec<u8>, vpos: u64) {
        match self {
            DiskEntryZ::L { .. } => (),
            &DiskEntryZ::LD { doff: d, .. } => {
                // re-encode delta file-position
                let scratch: [u8; 8] = leaf[d + 16..d + 24].try_into().unwrap();
                let mut fpos = u64::from_be_bytes(scratch);
                fpos += vpos;
                leaf[d + 16..d + 24].copy_from_slice(&fpos.to_be_bytes());
            }
            &DiskEntryZ::LV { voff, .. } => {
                // re-encode value file-position
                let scratch: [u8; 8] = leaf[voff..voff + 8].try_into().unwrap();
                let mut fpos = u64::from_be_bytes(scratch);
                fpos += vpos;
                leaf[voff..voff + 8].copy_from_slice(&fpos.to_be_bytes());
            }
            &DiskEntryZ::LVD { voff, doff: d, .. } => {
                // re-encode delta file-position
                let scratch: [u8; 8] = leaf[d + 16..d + 24].try_into().unwrap();
                let mut fpos = u64::from_be_bytes(scratch);
                fpos += vpos;
                leaf[d + 16..d + 24].copy_from_slice(&fpos.to_be_bytes());
                // re-encode value file-position
                let scratch: [u8; 8] = leaf[voff..voff + 8].try_into().unwrap();
                let mut fpos = u64::from_be_bytes(scratch);
                fpos += vpos;
                leaf[voff..voff + 8].copy_from_slice(&fpos.to_be_bytes());
            }
        }
    }

    pub(crate) fn to_delta<V>(buf: &[u8]) -> Result<core::Delta<V>, Error>
    where
        V: Clone + Diff,
    {
        let hdr1 = u64::from_be_bytes(buf[0..8].try_into().unwrap());
        let dlen = hdr1 & Self::DLEN_MASK;
        let is_deleted = (hdr1 & Self::UPSERT_FLAG) == 0;

        let seqno = u64::from_be_bytes(buf[8..16].try_into().unwrap());

        let fpos = u64::from_be_bytes(buf[16..24].try_into().unwrap());

        if is_deleted {
            let delta = vlog::Delta::Reference { fpos, length: dlen };
            Ok(core::Delta::new_upsert(delta, seqno))
        } else {
            Ok(core::Delta::new_delete(seqno))
        }
    }

    pub(crate) fn to_entry<K, V>(e: &[u8]) -> Result<core::Entry<K, V>, Error>
    where
        K: Ord + Clone + Serialize,
        V: Clone + Diff + Serialize,
    {
        let hdr1 = u64::from_be_bytes(e[0..8].try_into().unwrap());
        let n_deltas: usize = (hdr1 & Self::NDELTA_MASK).try_into().unwrap();
        let klen: usize = (hdr1 >> Self::KLEN_SHIFT).try_into().unwrap();

        let hdr2 = u64::from_be_bytes(e[8..16].try_into().unwrap());
        let is_deleted = (hdr2 & Self::UPSERT_FLAG) == 0;
        let is_vlog = (hdr2 & Self::VLOG_FLAG) == 1;
        let vlen = hdr2 & Self::VLEN_MASK;

        let seqno = u64::from_be_bytes(e[16..24].try_into().unwrap());

        let mut key: K = unsafe { mem::zeroed() };
        key.decode(&e[24..24 + klen])?;

        let n = 24 + klen;
        let (n, value) = match (is_deleted, is_vlog) {
            (true, _) => (n, core::Value::new_delete(seqno)),
            (false, true) => {
                let fpos = u64::from_be_bytes(e[n..n + 8].try_into().unwrap());
                let value = vlog::Value::Reference { fpos, length: vlen };
                (n + 8, core::Value::new_upsert(value, seqno))
            }
            (false, false) => {
                let mut value: V = unsafe { mem::zeroed() };
                let vlen: usize = vlen.try_into().unwrap();
                value.decode(&e[n..n + vlen])?;
                let value = vlog::Value::Native { value };
                (n + vlen, core::Value::new_upsert(value, seqno))
            }
        };

        let mut entry = core::Entry::new(key, value);

        let mut deltas: Vec<core::Delta<V>> = vec![];
        for i in 0..n_deltas {
            deltas.push(Self::to_delta(&e[n + (i * 4)..n + (i * 4) + 24])?);
        }
        entry.set_deltas(deltas);
        Ok(entry)
    }

    pub(crate) fn to_key<K>(entry: &[u8]) -> Result<K, Error>
    where
        K: Serialize,
    {
        let hdr1 = u64::from_be_bytes(entry[0..8].try_into().unwrap());
        let klen: usize = (hdr1 >> Self::KLEN_SHIFT).try_into().unwrap();
        let mut key: K = unsafe { mem::zeroed() };
        key.decode(&entry[24..24 + klen])?;
        Ok(key)
    }
}
