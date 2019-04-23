use std::fs;

use crate::traits::{AsDelta, Serialize, Diff, AsEntry};


struct ZDelta<V> {
    delta: vlog::Delta<V>,
    seqno: u64,
    is_deleted: bool,
}

enum ZBlock<'a, K, V> {
    Encode {
        indx_block: Vec<u8>,
        num_entries: u32,
        entries: Vec<&'a [u8]>,
        vlog_block: Vec<u8>,
    },
}

impl<'a, K, V> ZBlock<'a, K, V> {
    fn insert(&mut self, entry: &bubt_build::Entry) -> Result<bool, BognError> {
    }

    fn reset(&mut self) {
    }
}

// Binary format (ZDelta):
//
// *-----*------------------------------------*
// |flags|      60-bit delta-len              |
// *-----*------------------------------------*
// |              64-bit seqno                |
// *-------------------*----------------------*
// |               delta-fpos                 |
// *------------------------------------------*

// Binary format (ZEntry):
//
// |  32-bit key len   |   number of deltas   |
// *-------------------*----------------------*
// |                  key                     |
// *-----*------------------------------------*
// |flags|      60-bit value-len              |
// *-----*------------------------------------*
// |              64-bit seqno                |
// *-------------------*----------------------*
// |              value / fpos                |
// *------------------------------------------*
// |                zdelta 1                  |
// *------------------------------------------*
// |                zdelta 2                  |
// *------------------------------------------*

pub struct ZEntry<K, V>
where
    V: Default + Clone + Serialize + Diff,
{
    key: K,
    value: ZValue,
    deltas: Vec<ZDelta>,
}


impl<K, V> ZEntry<K, V>
where
    V: Default + Clone + Serialize + Diff,
{
    const MASK_TOTAL_LEN: u32 = 0xFFFFFFFF00000000;
    const MASK_KEY_LEN: u32 = 0x00000000FFFFFFFF;

    fn new(key: K, value: ZValue) -> ZEntry {
        ZEntry{key, value, deltas: vec![]}
    }

    fn append_delta(&mut self, delta: ZDelta) {
        self.push(delta);
    }

    fn value_ref(&self) -> &V {
        self.value.value_ref()
    }

    fn encode(&self, file: &fs::File, buf: Vec<u8>) -> Vec<u8> {
        self.value.value_ref()
    }

    fn decode(buf: &[u8]) -> Result<(ZEntry, u64), BognError> {
        let mut scratch = [0_u8; 8];
        // hdr1
        scratch.copy_from_slice(&buf[..8]);
        let (hdr1, off) = (u64::from_be_bytes(scratch), 8);
        scratch.copy_from_slice(&buf[off..off+8]);
        let (n_deltas, off) = (u64::from_be_bytes(scratch), off+8);
        let total_len = hdr1 & Self::MASK_TOTAL_LEN;
        let key_len = hdr1 & Self::MASK_KEY_LEN;
        if (off + key_len) > buf.len() {
            return Err(BognError::BubtZEntryKeyOverflow(key_len));
        }
        let (key, off) = (K::decode(buf[off..off+key_len])?, off+key_len);

        let vlen = ZValue::value_len(buf[off..off+8);
        if (off + vlen) > buf.len() {
            return Err(BognError::BubtZEntryValueOverflow(vlen));
        }
        let (value, zvlen) = ZValue::decode(buf[off..off+vlen)?;
        let off += zvlen;

        let deltas = vec![];
        for _i in 0..n_deltas {
            let dlen = ZDelta::disk_len();
            if (off + dlen) > buf.len() {
                return Err(BognError::BubtZEntryDeltaOverflow(vlen));
            }
            let (delta, dlen) = ZDelta::decode(buf[off..off+dlen)?;
            let off += dlen;
            deltas.push(delta);
        }
        ZEntry{key, value, deltas}
    }

    fn entry_len(buf: &[u8]) -> u64 {
        let mut scratch = [0_u8; 8];
        scratch.copy_from_slice(&buf[..8]);
        (u64::from_be_bytes(scratch) & Self::MASK_TOTAL_LEN) >> 32
    }

    fn entry_key(buf: &[u8]) -> K {
        let mut scratch = [0_u8; 8];
        scratch.copy_from_slice(&buf[..8]);
        let klen = u64::from_be_bytes(scratch) & Self::MASK_KEY_LEN;
        K::decode(&buf[8..8+klen]).ok().unwrap()
    }
}

impl<K,V> AsEntry<K,V> for ZEntry<K,V>
where
    V: Default + Clone + Serialize + Diff,
{
    pub fn key(&self) -> K {
        self.key
    }

    pub fn key_ref(&self) -> *K {
        &self.key
    }

    pub fn value(&self) -> Option<V> {
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted
    }

    pub fn seqno(&self) -> u64 {
        self.seqno
    }
}

impl<K, V> ZEntry<K, V> {
    fn encode(&self, buf: Vec<u8>) -> (Vec<u8>, u64) {
        // TODO:
    }

    fn decode(&mut self, buf: &[u8]) -> Result<u64, BognError> {
        // TODO:
    }
}
