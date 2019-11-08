use std::{convert::TryInto, marker, mem};

use crate::{
    core::{self, Diff, Result, Serialize},
    error::Error,
    vlog,
};

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
//
pub(crate) enum MEntry<'a, K>
where
    K: Serialize,
{
    EncM { fpos: u64, key: &'a K },
    EncZ { fpos: u64, key: &'a K },
    DecM { fpos: u64, index: usize },
    DecZ { fpos: u64, index: usize },
}

impl<'a, K> MEntry<'a, K>
where
    K: Serialize,
{
    const ZBLOCK_FLAG: u64 = 0x1000000000000000;
    const KLEN_MASK: u64 = 0x00000000FFFFFFFF;

    pub(crate) fn new_m(fpos: u64, key: &K) -> MEntry<K> {
        MEntry::EncM { fpos, key }
    }

    pub(crate) fn new_z(fpos: u64, key: &K) -> MEntry<K> {
        MEntry::EncZ { fpos, key }
    }

    pub(crate) fn encode(&self, buf: &mut Vec<u8>) -> Result<usize> {
        let m = buf.len();
        // adjust space for header.
        buf.resize(m + 16, 0);
        // encode key
        let (hdr1, klen, fpos) = match self {
            MEntry::EncM { fpos, key, .. } => {
                let klen: u64 = key.encode(buf).try_into().unwrap();
                let hdr1 = klen.to_be_bytes();
                (hdr1, klen, fpos)
            }
            MEntry::EncZ { fpos, key, .. } => {
                let klen: u64 = key.encode(buf).try_into().unwrap();
                let hdr1 = (klen | Self::ZBLOCK_FLAG).to_be_bytes();
                (hdr1, klen, fpos)
            }
            _ => unreachable!(),
        };
        let klen: usize = klen.try_into().unwrap();
        if klen < core::Entry::<i32, i32>::KEY_SIZE_LIMIT {
            buf[m..m + 8].copy_from_slice(&hdr1);
            buf[m + 8..m + 16].copy_from_slice(&fpos.to_be_bytes());
            Ok(klen + 16)
        } else {
            Err(Error::KeySizeExceeded(klen))
        }
    }
}

impl<'a, K> MEntry<'a, K>
where
    K: 'a + Serialize,
{
    pub(crate) fn decode_entry(entry: &[u8], index: usize) -> MEntry<K> {
        let hdr1 = u64::from_be_bytes(entry[0..8].try_into().unwrap());
        let fpos = u64::from_be_bytes(entry[8..16].try_into().unwrap());
        match (hdr1 & Self::ZBLOCK_FLAG) == Self::ZBLOCK_FLAG {
            false => MEntry::DecM { fpos, index },
            true => MEntry::DecZ { fpos, index },
        }
    }

    pub(crate) fn decode_key(entry: &[u8]) -> Result<K>
    where
        K: 'a + Serialize,
    {
        let klen: usize = {
            let hdr1 = u64::from_be_bytes(entry[0..8].try_into().unwrap());
            (hdr1 & Self::KLEN_MASK).try_into().unwrap()
        };
        let mut key: K = unsafe { mem::zeroed() };
        key.decode(&entry[16..16 + klen])?;
        Ok(key)
    }

    pub(crate) fn is_zblock(&self) -> bool {
        match self {
            &MEntry::EncM { .. } | &MEntry::DecM { .. } => false,
            &MEntry::EncZ { .. } | &MEntry::DecZ { .. } => true,
        }
    }

    pub(crate) fn to_fpos(&self) -> u64 {
        match self {
            &MEntry::EncM { fpos, .. } => fpos,
            &MEntry::EncZ { fpos, .. } => fpos,
            &MEntry::DecM { fpos, .. } => fpos,
            &MEntry::DecZ { fpos, .. } => fpos,
        }
    }

    pub(crate) fn to_index(&self) -> usize {
        match self {
            &MEntry::DecM { index, .. } => index,
            &MEntry::DecZ { index, .. } => index,
            _ => unreachable!(),
        }
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
struct DiskDelta<V>
where
    V: Clone + Diff,
    <V as Diff>::D: Serialize,
{
    value: marker::PhantomData<V>,
}

impl<V> DiskDelta<V>
where
    V: Clone + Diff,
    <V as Diff>::D: Serialize,
{
    const UPSERT_FLAG: u64 = 0x1000000000000000;
    const DLEN_MASK: u64 = 0x0FFFFFFFFFFFFFFF;

    fn encode(delta: &core::Delta<V>, leaf: &mut Vec<u8>, blob: &mut Vec<u8>) -> Result<usize> {
        match delta.as_ref() {
            core::InnerDelta::U { delta, seqno } => {
                let mpos: u64 = blob.len().try_into().unwrap();

                let (hdr1, n) = {
                    let n = delta.encode(blob)?;
                    let hdr1: u64 = n.try_into().unwrap();
                    let hdr1 = hdr1 | Self::UPSERT_FLAG;
                    (hdr1, n)
                };

                leaf.extend_from_slice(&hdr1.to_be_bytes()); // diff-len
                leaf.extend_from_slice(&seqno.to_be_bytes());
                leaf.extend_from_slice(&mpos.to_be_bytes()); // fpos
                Ok(n)
            }
            core::InnerDelta::D { seqno } => {
                leaf.extend_from_slice(&0_u64.to_be_bytes()); // diff-len
                leaf.extend_from_slice(&seqno.to_be_bytes());
                leaf.extend_from_slice(&0_u64.to_be_bytes()); // fpos
                Ok(0)
            }
        }
    }

    fn re_encode_fpos(buf: &mut [u8], vpos: u64) {
        let is_deleted = {
            let scratch: [u8; 8] = buf[..8].try_into().unwrap();
            (u64::from_be_bytes(scratch) & Self::UPSERT_FLAG) == 0
        };
        if !is_deleted {
            let scratch: [u8; 8] = buf[16..24].try_into().unwrap();
            let fpos = vpos + u64::from_be_bytes(scratch);
            buf[16..24].copy_from_slice(&fpos.to_be_bytes());
        }
    }
}

impl<V> DiskDelta<V>
where
    V: Clone + Diff,
    <V as Diff>::D: Serialize,
{
    fn decode_delta(buf: &[u8]) -> Result<core::Delta<V>> {
        let (dlen, is_deleted) = {
            let hdr1 = u64::from_be_bytes(buf[0..8].try_into().unwrap());
            (hdr1 & Self::DLEN_MASK, (hdr1 & Self::UPSERT_FLAG) == 0)
        };

        let seqno = u64::from_be_bytes(buf[8..16].try_into().unwrap());
        let fpos = u64::from_be_bytes(buf[16..24].try_into().unwrap());

        if is_deleted {
            Ok(core::Delta::new_delete(seqno))
        } else {
            let delta = vlog::Delta::new_reference(fpos, dlen, seqno);
            Ok(core::Delta::new_upsert(delta, seqno))
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
pub(crate) enum ZEntry<K, V>
where
    K: Serialize,
    V: Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    // encode {key, value} entry into Z-Block
    EncL {
        k: usize,
        v: usize,
    },
    // encode {key, value} entry into Z-Block and delta in value-log
    EncLD {
        doff: usize,
        n_deltas: usize,
        k: usize,
        v: usize,
        d: usize,
    },
    // encode key entry into Z-Block, while value in value-log
    EncLV {
        voff: usize,
        k: usize,
        v: usize,
    },
    // encode key entry into Z-Block, while value and delta in value-log
    EncLVD {
        voff: usize,
        doff: usize,
        n_deltas: usize,
        k: usize,
        v: usize,
        d: usize,
    },
    _Phantom {
        key: marker::PhantomData<K>,
        value: marker::PhantomData<V>,
    },
}

impl<K, V> ZEntry<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    const UPSERT_FLAG: u64 = 0x1000000000000000;
    const VLOG_FLAG: u64 = 0x2000000000000000;
    const VLEN_MASK: u64 = 0x0FFFFFFFFFFFFFFF;
    const NDELTA_MASK: u64 = 0xFFFFFFFF;
    const KLEN_SHIFT: u64 = 32;

    pub(crate) fn encode_l(entry: &core::Entry<K, V>, leaf: &mut Vec<u8>) -> Result<ZEntry<K, V>> {
        let (n_deltas, is_vlog) = (0_usize, false);
        let (k, v) = Self::encode_leaf1(entry, n_deltas, is_vlog, leaf)?;
        Ok(ZEntry::EncL { k, v })
    }

    pub(crate) fn encode_ld(
        entry: &core::Entry<K, V>,
        leaf: &mut Vec<u8>,
        blob: &mut Vec<u8>,
    ) -> Result<ZEntry<K, V>> {
        let m = leaf.len();
        let (n_deltas, is_vlog) = (entry.to_delta_count(), false);
        let (k, v) = Self::encode_leaf1(entry, n_deltas, is_vlog, leaf)?;
        let doff = leaf.len() - m;
        let d = ZEntry::encode_deltas(entry, leaf, blob)?;
        Ok(ZEntry::EncLD {
            doff,
            n_deltas,
            k,
            v,
            d,
        })
    }

    pub(crate) fn encode_lv(
        entry: &core::Entry<K, V>,
        leaf: &mut Vec<u8>,
        blob: &mut Vec<u8>,
    ) -> Result<ZEntry<K, V>> {
        let (n_deltas, is_vlog) = (0_usize, true);
        let (x, k, v) = Self::encode_leaf2(entry, n_deltas, is_vlog, leaf, blob)?;
        Ok(ZEntry::EncLV { voff: x, k, v })
    }

    pub(crate) fn encode_lvd(
        entry: &core::Entry<K, V>,
        leaf: &mut Vec<u8>,
        blob: &mut Vec<u8>,
    ) -> Result<ZEntry<K, V>> {
        let m = leaf.len();
        let (n_deltas, is_vlog) = (entry.to_delta_count(), true);
        let (x, k, v) = Self::encode_leaf2(entry, n_deltas, is_vlog, leaf, blob)?;
        // encode deltas
        let doff = leaf.len() - m;
        let d = ZEntry::encode_deltas(entry, leaf, blob)?;
        Ok(ZEntry::EncLVD {
            voff: x,
            doff,
            n_deltas,
            k,
            v,
            d,
        })
    }

    fn encode_leaf1(
        entry: &core::Entry<K, V>,
        n_deltas: usize,
        is_vlog: bool,
        leaf: &mut Vec<u8>,
    ) -> Result<(usize, usize)> {
        // adjust space for header.
        let m = leaf.len();
        leaf.resize(m + 24, 0);
        // encode key
        let klen = Self::encode_key(entry.as_key(), leaf)?;
        // encode value
        let (vlen, is_del, seqno) = ZEntry::encode_value_leaf(entry, leaf)?;
        // encode header.
        let hdr = &mut leaf[m..m + 24];
        Self::encode_header(klen, n_deltas, vlen, is_del, is_vlog, seqno, hdr);
        Ok((klen, vlen))
    }

    fn encode_leaf2(
        entry: &core::Entry<K, V>,
        n_deltas: usize,
        is_vlog: bool,
        leaf: &mut Vec<u8>,
        blob: &mut Vec<u8>,
    ) -> Result<(usize, usize, usize)> {
        // adjust space for header.
        let m = leaf.len();
        leaf.resize(m + 24, 0);
        // encode key
        let klen = Self::encode_key(entry.as_key(), leaf)?;
        // encode value
        let pos = blob.len();
        let (vlen, is_del, seqno) = ZEntry::encode_value_vlog(entry, blob)?;
        let voff = leaf.len() - m;
        if !is_del {
            let pos: u64 = pos.try_into().unwrap();
            leaf.extend_from_slice(&pos.to_be_bytes());
        }
        // encode header.
        let hdr = &mut leaf[m..m + 24];
        Self::encode_header(klen, n_deltas, vlen, is_del, is_vlog, seqno, hdr);
        Ok((voff, klen, vlen))
    }

    fn encode_header(
        klen: usize,
        n_deltas: usize,
        vlen: usize,
        is_deleted: bool,
        is_vlog: bool,
        seqno: u64,
        hdr: &mut [u8],
    ) {
        let hdr1 = {
            let klen: u64 = klen.try_into().unwrap();
            let n_deltas: u64 = n_deltas.try_into().unwrap();
            ((klen << Self::KLEN_SHIFT) | n_deltas).to_be_bytes()
        };
        let hdr2 = {
            let mut vlen: u64 = vlen.try_into().unwrap();
            if !is_deleted {
                vlen |= Self::UPSERT_FLAG;
            }
            if is_vlog {
                vlen |= Self::VLOG_FLAG;
            }
            vlen.to_be_bytes()
        };
        let hdr3 = seqno.to_be_bytes();

        hdr[..8].copy_from_slice(&hdr1);
        hdr[8..16].copy_from_slice(&hdr2);
        hdr[16..24].copy_from_slice(&hdr3);
    }

    fn encode_key(key: &K, buf: &mut Vec<u8>) -> Result<usize> {
        let n = key.encode(buf);
        if n > core::Entry::<i32, i32>::KEY_SIZE_LIMIT {
            Err(Error::KeySizeExceeded(n))
        } else {
            Ok(n)
        }
    }

    fn encode_value_leaf(
        entry: &core::Entry<K, V>,
        buf: &mut Vec<u8>,
    ) -> Result<(usize, bool, u64)> {
        match entry.as_value() {
            core::Value::U { value, seqno, .. } => {
                let vlen = value.encode_local(buf)?;
                Ok((vlen, false, *seqno))
            }
            core::Value::D { seqno } => Ok((0, true, *seqno)),
        }
    }

    fn encode_value_vlog(
        entry: &core::Entry<K, V>,
        buf: &mut Vec<u8>,
    ) -> Result<(usize, bool, u64)> {
        match entry.as_value() {
            core::Value::U { value, seqno, .. } => {
                let vlen = value.encode(buf)?;
                Ok((vlen, false, *seqno))
            }
            core::Value::D { seqno } => Ok((0, true, *seqno)),
        }
    }

    fn encode_deltas(
        entry: &core::Entry<K, V>,
        leaf: &mut Vec<u8>,
        blob: &mut Vec<u8>,
    ) -> Result<usize> {
        let mut n = 0_usize;
        for delta in entry.as_deltas() {
            n += DiskDelta::encode(delta, leaf, blob)?;
        }
        Ok(n)
    }

    pub(crate) fn re_encode_fpos(&self, leaf: &mut [u8], vpos: u64) {
        match self {
            ZEntry::EncL { .. } => (),
            &ZEntry::EncLD { doff, n_deltas, .. } => {
                Self::re_encode_d(leaf, vpos, doff, n_deltas);
            }
            &ZEntry::EncLV { voff, .. } => {
                Self::re_encode_v(leaf, vpos, voff);
            }
            &ZEntry::EncLVD {
                voff,
                doff,
                n_deltas,
                ..
            } => {
                Self::re_encode_d(leaf, vpos, doff, n_deltas);
                Self::re_encode_v(leaf, vpos, voff);
            }
            _ => unreachable!(),
        }
    }

    fn re_encode_d(leaf: &mut [u8], vpos: u64, doff: usize, n_deltas: usize) {
        for i in 0..n_deltas {
            let n = doff + (i * 24);
            DiskDelta::<V>::re_encode_fpos(&mut leaf[n..], vpos);
        }
    }

    fn re_encode_v(leaf: &mut [u8], vpos: u64, voff: usize) {
        let is_deleted = {
            let scratch: [u8; 8] = leaf[8..16].try_into().unwrap();
            (u64::from_be_bytes(scratch) & Self::UPSERT_FLAG) == 0
        };
        if !is_deleted {
            let scratch: [u8; 8] = leaf[voff..voff + 8].try_into().unwrap();
            let fpos = u64::from_be_bytes(scratch) + vpos;
            leaf[voff..voff + 8].copy_from_slice(&fpos.to_be_bytes());
        }
    }

    pub(crate) fn to_kvd_stats(&self) -> (usize, usize, usize) {
        match self {
            ZEntry::EncL { k, v, .. } => (*k, *v, 0),
            ZEntry::EncLD { k, v, d, .. } => (*k, *v, *d),
            ZEntry::EncLV { k, v, .. } => (*k, *v, 0),
            ZEntry::EncLVD { k, v, d, .. } => (*k, *v, *d),
            _ => unreachable!(),
        }
    }
}

impl<K, V> ZEntry<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    pub(crate) fn decode_entry(e: &[u8]) -> Result<core::Entry<K, V>> {
        let (klen, n_deltas) = {
            let hdr1 = u64::from_be_bytes(e[0..8].try_into().unwrap());
            let n_deltas: usize = (hdr1 & Self::NDELTA_MASK).try_into().unwrap();
            let klen: usize = (hdr1 >> Self::KLEN_SHIFT).try_into().unwrap();
            (klen, n_deltas)
        };
        let (is_deleted, is_vlog, vlen) = {
            let hdr2 = u64::from_be_bytes(e[8..16].try_into().unwrap());
            (
                (hdr2 & Self::UPSERT_FLAG) == 0,
                (hdr2 & Self::VLOG_FLAG) != 0,
                hdr2 & Self::VLEN_MASK,
            )
        };
        let seqno = u64::from_be_bytes(e[16..24].try_into().unwrap());

        let mut key: K = unsafe { mem::zeroed() };
        key.decode(&e[24..24 + klen])?;

        let n = 24 + klen;
        let (mut n, value) = match (is_deleted, is_vlog) {
            (true, _) => (n, core::Value::new_delete(seqno)),
            (false, true) => {
                let fpos = u64::from_be_bytes(e[n..n + 8].try_into().unwrap());
                let v = Box::new(vlog::Value::new_reference(fpos, vlen, seqno));
                (n + 8, core::Value::new_upsert(v, seqno))
            }
            (false, false) => {
                let mut value: V = unsafe { mem::zeroed() };
                let vlen: usize = vlen.try_into().unwrap();
                value.decode(&e[n..n + vlen])?;
                let value = Box::new(vlog::Value::Native { value });
                (n + vlen, core::Value::new_upsert(value, seqno))
            }
        };

        let mut entry = core::Entry::new(key, value);

        let mut deltas: Vec<core::Delta<V>> = vec![];
        for _i in 0..n_deltas {
            deltas.push(DiskDelta::decode_delta(&e[n..])?);
            n += 24;
        }
        entry.set_deltas(deltas);

        Ok(entry)
    }

    pub(crate) fn decode_key(entry: &[u8]) -> Result<K> {
        let mut key: K = unsafe { mem::zeroed() };

        let klen: usize = {
            let hdr1 = u64::from_be_bytes(entry[0..8].try_into().unwrap());
            (hdr1 >> Self::KLEN_SHIFT).try_into().unwrap()
        };

        key.decode(&entry[24..24 + klen])?;
        Ok(key)
    }
}

#[cfg(test)]
#[path = "robt_entry_test.rs"]
mod robt_entry_test;
