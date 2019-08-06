use std::{convert::TryInto, marker, mem};

use crate::core::{self, Diff, Serialize};
use crate::error::Error;
use crate::robt_stats::Stats;
use crate::vlog;

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

    pub(crate) fn encode(&self, buf: &mut Vec<u8>) -> Result<usize, Error> {
        let m = buf.len();
        // adjust space for header.
        buf.resize(m + 16, 0);
        // encode key
        let (hdr1, klen, fpos) = match self {
            MEntry::EncM { fpos, key, .. } => {
                let klen: u64 = key.encode(buf).try_into().unwrap();
                (klen.to_be_bytes(), klen, fpos)
            }
            MEntry::EncZ { fpos, key, .. } => {
                let klen: u64 = key.encode(buf).try_into().unwrap();
                ((klen | Self::ZBLOCK_FLAG).to_be_bytes(), klen, fpos)
            }
            _ => unreachable!(),
        };
        let klen = klen as usize;
        if klen > core::Entry::<i32, i32>::KEY_SIZE_LIMIT {
            return Err(Error::KeySizeExceeded(klen));
        }
        // encode header
        buf[m..m + 8].copy_from_slice(&hdr1);
        buf[m + 8..m + 16].copy_from_slice(&fpos.to_be_bytes());
        Ok(klen + 16)
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

    pub(crate) fn decode_key(entry: &[u8]) -> Result<K, Error>
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
enum DiskDelta<'a, V>
where
    V: Clone + Diff,
    <V as Diff>::D: Serialize,
{
    E { delta: &'a core::Delta<V> },
}

impl<'a, V> DiskDelta<'a, V>
where
    V: Clone + Diff,
    <V as Diff>::D: Serialize,
{
    const UPSERT_FLAG: u64 = 0x1000000000000000;
    const DLEN_MASK: u64 = 0x0FFFFFFFFFFFFFFF;

    pub(crate) fn new_encode(delta: &core::Delta<V>) -> DiskDelta<V> {
        DiskDelta::E { delta }
    }

    pub(crate) fn encode(
        &self,
        leaf: &mut Vec<u8>, // leaf output buffer
        blob: &mut Vec<u8>, // block output buffer
    ) -> Result<usize, Error> {
        let delta = match self {
            DiskDelta::E { delta } => delta,
        };
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
        let scratch: [u8; 8] = buf[16..24].try_into().unwrap();
        let fpos = vpos + u64::from_be_bytes(scratch);
        buf[16..24].copy_from_slice(&fpos.to_be_bytes());
    }
}

impl<'a, V> DiskDelta<'a, V>
where
    V: 'a + Clone + Diff,
    <V as Diff>::D: Serialize,
{
    fn decode_delta(buf: &[u8]) -> Result<core::Delta<V>, Error> {
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
    EncL,
    // encode {key, value} entry into Z-Block and delta in value-log
    EncLD {
        doff: usize,
        ndeltas: usize,
    },
    // encode key entry into Z-Block, while value in value-log
    EncLV {
        voff: usize,
    },
    // encode key entry into Z-Block, while value and delta in value-log
    EncLVD {
        voff: usize,
        doff: usize,
        ndeltas: usize,
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

    pub(crate) fn encode_l(
        entry: &core::Entry<K, V>,
        leaf: &mut Vec<u8>,
        stats: &mut Stats,
    ) -> Result<ZEntry<K, V>, Error> {
        let (dlen, is_vlog) = (0_usize, false);
        Self::encode_leaf(entry, dlen, is_vlog, leaf, stats)?;
        Ok(ZEntry::EncL)
    }

    pub(crate) fn encode_ld(
        entry: &core::Entry<K, V>,
        leaf: &mut Vec<u8>,
        blob: &mut Vec<u8>,
        stats: &mut Stats,
    ) -> Result<ZEntry<K, V>, Error> {
        let (ndeltas, is_vlog) = (entry.to_delta_count(), false);
        Self::encode_leaf(entry, ndeltas, is_vlog, leaf, stats)?;
        let doff = leaf.len();
        stats.diffmem += ZEntry::encode_delta(entry, leaf, blob)?;
        Ok(ZEntry::EncLD { doff, ndeltas })
    }

    pub(crate) fn encode_lv(
        entry: &core::Entry<K, V>,
        leaf: &mut Vec<u8>,
        blob: &mut Vec<u8>,
        stats: &mut Stats,
    ) -> Result<ZEntry<K, V>, Error> {
        let (dlen, is_vlog) = (0_usize, true);
        match Self::encode_leaf_blob(entry, dlen, is_vlog, leaf, blob, stats) {
            Ok(voff) => Ok(ZEntry::EncLV { voff }),
            Err(err) => Err(err),
        }
    }

    pub(crate) fn encode_lvd(
        entry: &core::Entry<K, V>,
        leaf: &mut Vec<u8>,
        blob: &mut Vec<u8>,
        stats: &mut Stats,
    ) -> Result<ZEntry<K, V>, Error> {
        let (dlen, is_vlog) = (entry.to_delta_count(), true);
        match Self::encode_leaf_blob(entry, dlen, is_vlog, leaf, blob, stats) {
            Ok(voff) => {
                // encode deltas
                let doff = leaf.len();
                stats.diffmem += ZEntry::encode_delta(entry, leaf, blob)?;
                Ok(ZEntry::EncLVD {
                    voff,
                    doff,
                    ndeltas: dlen,
                })
            }
            Err(err) => Err(err),
        }
    }

    fn encode_headers(
        klen: usize,
        dlen: usize,
        vlen: usize,
        is_deleted: bool,
        is_vlog: bool,
        seqno: u64,
        leaf: &mut Vec<u8>,
    ) {
        let hdr1 = {
            let klen: u64 = klen.try_into().unwrap();
            let dlen: u64 = dlen.try_into().unwrap();
            ((klen << Self::KLEN_SHIFT) | dlen).to_be_bytes()
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
        leaf[..8].copy_from_slice(&hdr1);
        leaf[8..16].copy_from_slice(&hdr2);
        leaf[16..24].copy_from_slice(&hdr3);
    }

    fn encode_leaf(
        entry: &core::Entry<K, V>, // input
        dlen: usize,               // input
        is_vlog: bool,             // input
        leaf: &mut Vec<u8>,        // output
        stats: &mut Stats,
    ) -> Result<(), Error> {
        // adjust space for header.
        let m = leaf.len();
        leaf.resize(m + 24, 0);
        // encode key
        let klen = Self::encode_key(entry.as_key(), leaf)?;
        stats.keymem += klen;
        // encode value
        let (vlen, is_deleted, seqno) = ZEntry::encode_value(entry, leaf)?;
        stats.valmem += vlen;
        // encode header.
        Self::encode_headers(klen, dlen, vlen, is_deleted, is_vlog, seqno, leaf);
        Ok(())
    }

    fn encode_leaf_blob(
        entry: &core::Entry<K, V>, // input
        dlen: usize,               // input
        is_vlog: bool,             // input
        leaf: &mut Vec<u8>,        // output
        blob: &mut Vec<u8>,        // output
        stats: &mut Stats,
    ) -> Result<usize, Error> {
        // adjust space for header.
        let m = leaf.len();
        leaf.resize(m + 24, 0);
        // encode key
        let klen = Self::encode_key(entry.as_key(), leaf)?;
        stats.keymem += klen;
        // encode value
        let pos = blob.len();
        let (vlen, is_deleted, seqno) = ZEntry::encode_value(entry, blob)?;
        stats.valmem += vlen;
        let voff = leaf.len();
        let pos: u64 = pos.try_into().unwrap();
        leaf.extend_from_slice(&pos.to_be_bytes());
        // encode header.
        Self::encode_headers(klen, dlen, vlen, is_deleted, is_vlog, seqno, leaf);
        Ok(voff)
    }

    fn encode_key(
        key: &K,           // input
        buf: &mut Vec<u8>, // output
    ) -> Result<usize, Error> {
        let n = key.encode(buf);
        if n > core::Entry::<i32, i32>::KEY_SIZE_LIMIT {
            Err(Error::KeySizeExceeded(n))
        } else {
            Ok(n)
        }
    }

    fn encode_value(
        entry: &core::Entry<K, V>, // input
        blob: &mut Vec<u8>,        // output
    ) -> Result<(usize, bool, u64), Error> {
        match entry.as_value() {
            core::Value::U { value, seqno } => {
                let vlen = value.encode(blob)?;
                Ok((vlen, false, *seqno))
            }
            core::Value::D { seqno } => Ok((0, true, *seqno)),
        }
    }

    fn encode_delta(
        entry: &core::Entry<K, V>, // input
        leaf: &mut Vec<u8>,        // output
        blob: &mut Vec<u8>,        // output
    ) -> Result<usize, Error> {
        let mut n = 0_usize;
        for delta in entry.as_deltas() {
            n += DiskDelta::new_encode(delta).encode(leaf, blob)?;
        }
        Ok(n)
    }

    pub(crate) fn re_encode_fpos(&self, leaf: &mut Vec<u8>, vpos: u64) {
        match self {
            ZEntry::EncL => (),
            &ZEntry::EncLD { doff, ndeltas } => {
                Self::re_encode_d(leaf, vpos, doff, ndeltas);
            }
            &ZEntry::EncLV { voff } => {
                Self::re_encode_v(leaf, vpos, voff);
            }
            &ZEntry::EncLVD {
                voff,
                doff,
                ndeltas,
            } => {
                Self::re_encode_d(leaf, vpos, doff, ndeltas);
                Self::re_encode_v(leaf, vpos, voff);
            }
            _ => unreachable!(),
        }
    }

    fn re_encode_d(leaf: &mut Vec<u8>, vpos: u64, doff: usize, ndeltas: usize) {
        for i in 0..ndeltas {
            let n = doff + (i * 24);
            DiskDelta::<V>::re_encode_fpos(&mut leaf[n..], vpos);
        }
    }

    fn re_encode_v(leaf: &mut Vec<u8>, vpos: u64, voff: usize) {
        let scratch: [u8; 8] = leaf[voff..voff + 8].try_into().unwrap();
        let fpos = u64::from_be_bytes(scratch) + vpos;
        leaf[voff..voff + 8].copy_from_slice(&fpos.to_be_bytes());
    }
}

impl<K, V> ZEntry<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    pub(crate) fn decode_entry(e: &[u8]) -> Result<core::Entry<K, V>, Error> {
        let (klen, ndeltas) = {
            let hdr1 = u64::from_be_bytes(e[0..8].try_into().unwrap());
            let ndeltas: usize = (hdr1 & Self::NDELTA_MASK).try_into().unwrap();
            let klen: usize = (hdr1 >> Self::KLEN_SHIFT).try_into().unwrap();
            (klen, ndeltas)
        };
        let (is_deleted, is_vlog, vlen) = {
            let hdr2 = u64::from_be_bytes(e[8..16].try_into().unwrap());
            (
                (hdr2 & Self::UPSERT_FLAG) == 0,
                (hdr2 & Self::VLOG_FLAG) == 1,
                hdr2 & Self::VLEN_MASK,
            )
        };
        let seqno = u64::from_be_bytes(e[16..24].try_into().unwrap());

        let mut key: K = unsafe { mem::zeroed() };
        key.decode(&e[24..24 + klen])?;

        let n = 24 + klen;
        let (mut n, value) = match (is_deleted, is_vlog) {
            (true, _) => (n, Box::new(core::Value::new_delete(seqno))),
            (false, true) => {
                let fpos = u64::from_be_bytes(e[n..n + 8].try_into().unwrap());
                let value = vlog::Value::new_reference(fpos, vlen, seqno);
                (n + 8, Box::new(core::Value::new_upsert(value, seqno)))
            }
            (false, false) => {
                let mut value: V = unsafe { mem::zeroed() };
                let vlen: usize = vlen.try_into().unwrap();
                value.decode(&e[n..n + vlen])?;
                let value = vlog::Value::Native { value };
                (n + vlen, Box::new(core::Value::new_upsert(value, seqno)))
            }
        };

        let mut entry = core::Entry::new(key, value);

        let mut deltas: Vec<core::Delta<V>> = vec![];
        for _i in 0..ndeltas {
            deltas.push(DiskDelta::decode_delta(&e[n..])?);
            n += 24;
        }
        entry.set_deltas(deltas);

        Ok(entry)
    }

    pub(crate) fn decode_key(entry: &[u8]) -> Result<K, Error> {
        let mut key: K = unsafe { mem::zeroed() };

        let klen: usize = {
            let hdr1 = u64::from_be_bytes(entry[0..8].try_into().unwrap());
            (hdr1 >> Self::KLEN_SHIFT).try_into().unwrap()
        };

        key.decode(&entry[24..24 + klen])?;
        Ok(key)
    }
}
