// TODO: compute diffmem similar to keymem and valmem

use crate::bubt_stats::Stats;
use crate::core::{self, Diff, Serialize};
use crate::error::Error;
use crate::util;
use crate::vlog;

// Binary format (ZDelta):
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
                let n = vlog::encode_delta(&delta, blob)?;
                let m = (blob.len() as u64) | Self::UPSERT_FLAG;
                leaf.extend_from_slice(&m.to_be_bytes()); // diff-len
                leaf.extend_from_slice(&seqno.to_be_bytes());
                leaf.extend_from_slice(&0_u64.to_be_bytes()); // fpos
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

// Binary format (ZEntry):
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

enum DiskEntry {
    // key, value in leaf node
    L {
        size: usize,
    },
    // key, value in leaf node and delta in value-log
    LD {
        size: usize,
        doff: usize,
    },
    // key in leaf node, value in value-log
    LV {
        size: usize,
        voff: usize,
    },
    // key in leaf node, value and delta in value-log
    LVD {
        size: usize,
        voff: usize,
        doff: usize,
    },
}

impl DiskEntry {
    const UPSERT_FLAG: u64 = 0x1000000000000000;
    const VLOG_FLAG: u64 = 0x2000000000000000;

    pub(crate) fn encode_l<K, V>(
        &mut self,
        entry: &core::Entry<K, V>,
        leaf: &mut Vec<u8>,
        stats: &mut Stats,
    ) -> Result<usize, Error>
    where
        K: Clone + Ord + Serialize,
        V: Clone + Diff + Serialize,
        <V as Diff>::D: Serialize,
    {
        let m = leaf.len();
        let klen = DiskEntry::encode_key(entry, leaf)?;
        stats.keymem += klen;
        let n = leaf.len();
        // adjust space for header.
        leaf.resize(n + 24, 0);
        leaf.copy_within(m..n, 24);
        // encode value
        let (vlen, isd, seqno) = DiskEntry::encode_value(entry, leaf)?;
        stats.valmem += vlen;
        // encode header.
        let dlen = 0_usize;
        leaf[..8].copy_from_slice(&DiskEntry::encode_hdr1(klen, dlen)?);
        leaf[8..16].copy_from_slice(&DiskEntry::encode_hdr2(vlen, isd, false)?);
        leaf[16..24].copy_from_slice(&DiskEntry::encode_hdr3(seqno));

        Ok(leaf.len() - m)
    }

    pub(crate) fn encode_ld<K, V>(
        &mut self,
        entry: &core::Entry<K, V>,
        leaf: &mut Vec<u8>,
        blob: &mut Vec<u8>,
        stats: &mut Stats,
    ) -> Result<(usize, usize), Error>
    where
        K: Clone + Ord + Serialize,
        V: Clone + Diff + Serialize,
        <V as Diff>::D: Serialize,
    {
        let m = leaf.len();
        let klen = DiskEntry::encode_key(entry, leaf)?;
        stats.keymem += klen;
        let n = leaf.len();
        // adjust space for header.
        leaf.resize(n + 24, 0);
        leaf.copy_within(m..n, 24);
        // encode value
        let (vlen, isd, seqno) = DiskEntry::encode_value(entry, leaf)?;
        stats.valmem += vlen;
        // encode header.
        let dlen = entry.to_delta_count();
        leaf[..8].copy_from_slice(&DiskEntry::encode_hdr1(klen, dlen)?);
        leaf[8..16].copy_from_slice(&DiskEntry::encode_hdr2(vlen, isd, false)?);
        leaf[16..24].copy_from_slice(&DiskEntry::encode_hdr3(seqno));
        // encode deltas
        let doff = leaf.len();
        DiskEntry::encode_delta(entry, leaf, blob)?;

        Ok((leaf.len() - m, doff))
    }

    pub(crate) fn encode_lv<K, V>(
        &mut self,
        entry: &core::Entry<K, V>,
        leaf: &mut Vec<u8>,
        blob: &mut Vec<u8>,
        stats: &mut Stats,
    ) -> Result<(usize, usize), Error>
    where
        K: Clone + Ord + Serialize,
        V: Clone + Diff + Serialize,
        <V as Diff>::D: Serialize,
    {
        let m = leaf.len();
        let klen = DiskEntry::encode_key(entry, leaf)?;
        stats.keymem += klen;
        let n = leaf.len();
        // adjust space for header.
        leaf.resize(n + 24, 0);
        leaf.copy_within(m..n, 24);
        // encode value
        let (vlen, isd, seqno) = DiskEntry::encode_value(entry, blob)?;
        stats.valmem += vlen;
        let voff = leaf.len();
        leaf.extend_from_slice(&0_u64.to_be_bytes());
        // encode header.
        let dlen = 0_usize;
        leaf[..8].copy_from_slice(&DiskEntry::encode_hdr1(klen, dlen)?);
        leaf[8..16].copy_from_slice(&DiskEntry::encode_hdr2(vlen, isd, true)?);
        leaf[16..24].copy_from_slice(&DiskEntry::encode_hdr3(seqno));

        Ok((leaf.len() - m, voff))
    }

    pub(crate) fn encode_lvd<K, V>(
        &mut self,
        entry: &core::Entry<K, V>,
        leaf: &mut Vec<u8>,
        blob: &mut Vec<u8>,
        stats: &mut Stats,
    ) -> Result<(usize, usize, usize), Error>
    where
        K: Clone + Ord + Serialize,
        V: Clone + Diff + Serialize,
        <V as Diff>::D: Serialize,
    {
        let m = leaf.len();
        let klen = DiskEntry::encode_key(entry, leaf)?;
        stats.keymem += klen;
        let n = leaf.len();
        // adjust space for header.
        leaf.resize(n + 24, 0);
        leaf.copy_within(m..n, 24);
        // encode value
        let (vlen, isd, seqno) = DiskEntry::encode_value(entry, blob)?;
        stats.valmem += vlen;
        let voff = leaf.len();
        leaf.extend_from_slice(&0_u64.to_be_bytes());
        // encode header.
        let dlen = entry.to_delta_count();
        leaf[..8].copy_from_slice(&DiskEntry::encode_hdr1(klen, dlen)?);
        leaf[8..16].copy_from_slice(&DiskEntry::encode_hdr2(vlen, isd, true)?);
        leaf[16..24].copy_from_slice(&DiskEntry::encode_hdr3(seqno));
        // encode deltas
        let doff = leaf.len();
        DiskEntry::encode_delta(entry, leaf, blob)?;

        Ok((leaf.len() - m, voff, doff))
    }

    #[inline]
    fn encode_hdr1(k: usize, d: usize) -> Result<[u8; 8], Error> {
        let klen: u64 = util::try_convert_int(k, "key-len: usize->u64")?;
        let dlen: u64 = util::try_convert_int(d, "num-deltas usize->u64")?;
        Ok(((klen << 32) | dlen).to_be_bytes())
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
        entry: &core::Entry<K, V>, // input
        buf: &mut Vec<u8>,         // output
    ) -> Result<usize, Error>
    where
        K: Ord + Clone + Serialize,
        V: Clone + Diff,
    {
        let n = entry.as_key().encode(buf);
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
}
