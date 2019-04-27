use std::{ffi, mem};

use crate::core::{self, Diff, Serialize};
use crate::vlog;

#[derive(Default)]
pub struct Config {
    pub dir: String,
    pub m_blocksize: usize,
    pub z_blocksize: usize,
    pub v_blocksize: usize,
    pub tomb_purge: Option<u64>,
    pub vlog_file: Option<ffi::OsString>,
    pub value_in_vlog: bool,
}

//use std::fs;

// Binary format (ZDelta):
//
// *-----*------------------------------------*
// |flags|      60-bit delta-len              |
// *-----*------------------------------------*
// |              64-bit seqno                |
// *-------------------*----------------------*
// |          64-bit delete seqno             |
// *-------------------*----------------------*
// |               delta-fpos                 |
// *------------------------------------------*
//
// Flags:
//
// * bit 60 reserved
// * bit 61 reserved
// * bit 62 reserved
// * bit 63 reserved
//
// If deleted seqno is ZERO, then that version was never deleted.

// Binary format (ZEntry):
//
// *-------------------*----------------------*
// |  32-bit key len   |   number of deltas   |
// *-------------------*----------------------*
// |flags|      60-bit value-len              |
// *-----*------------------------------------*
// |              64-bit seqno                |
// *-----*------------------------------------*
// |          64-bit delete seqno             |
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
// * bit 60 set = value in vlog-file.
// * bit 61 reserved
// * bit 62 reserved
// * bit 63 reserved
//
// If deleted seqno is ZERO, then that version was never deleted.

pub(crate) enum ZBlock {
    Encode {
        i_block: Vec<u8>,
        v_block: Vec<u8>,
        num_entries: u32,
        offsets: Vec<u32>,
        vpos: u64,
        // working buffers
        k_buf: Vec<u8>,
        v_buf: Vec<u8>,
        d_bufs: Vec<Vec<u8>>,
        config: Config,
    },
}

impl ZBlock {
    const DELTA_HEADER: usize = 8 + 8 + 8 + 8;
    const ENTRY_HEADER: usize = 8 + 8 + 8 + 8;
    const FLAGS_VLOG: u64 = 0x1000000000000000;

    pub(crate) fn new_encode(vpos: u64, config: Config) -> ZBlock {
        ZBlock::Encode {
            i_block: Vec::with_capacity(config.z_blocksize),
            v_block: Vec::with_capacity(config.v_blocksize),
            num_entries: Default::default(),
            offsets: Default::default(),
            vpos,
            // working buffers
            k_buf: Default::default(),
            v_buf: Default::default(),
            d_bufs: Default::default(),
            config,
        }
    }

    pub(crate) fn reset(&mut self, vpos: u64) {
        match self {
            ZBlock::Encode {
                i_block,
                v_block,
                num_entries,
                offsets,
                vpos: vpos_ref,
                ..
            } => {
                i_block.truncate(0);
                v_block.truncate(0);
                *num_entries = Default::default();
                offsets.truncate(0);
                *vpos_ref = vpos;
            }
        }
    }

    pub(crate) fn insert<K, V>(&mut self, entry: &core::Entry<K, V>) -> bool
    where
        K: Clone + Ord + Serialize,
        V: Default + Clone + Diff + Serialize,
    {
        let mut size = Self::ENTRY_HEADER;
        size += self.encode_key(entry);
        size += self.try_encode_value(entry);
        size += self.try_encode_deltas(entry);
        size += self.compute_next_offset();

        match self {
            ZBlock::Encode { i_block, .. } => {
                if (i_block.len() + size) < i_block.capacity() {
                    self.encode_entry(entry);
                    true
                } else {
                    false
                }
            }
        }
    }

    fn encode_key<K, V>(&mut self, entry: &core::Entry<K, V>) -> usize
    where
        K: Clone + Ord + Serialize,
        V: Default + Clone + Diff + Serialize,
    {
        match self {
            ZBlock::Encode { k_buf, .. } => {
                k_buf.truncate(0);
                entry.key_ref().encode(k_buf);
                k_buf.len()
            }
        }
    }

    fn try_encode_value<K, V>(&mut self, entry: &core::Entry<K, V>) -> usize
    where
        K: Clone + Ord + Serialize,
        V: Default + Clone + Diff + Serialize,
    {
        match self {
            ZBlock::Encode { config, .. } if !config.value_in_vlog => 8,
            ZBlock::Encode { v_buf, .. } => Self::encode_value(v_buf, entry),
        }
    }

    fn encode_value<K, V>(
        v_buf: &mut Vec<u8>, /* encode value of its file position */
        entry: &core::Entry<K, V>,
    ) -> usize
    where
        K: Clone + Ord + Serialize,
        V: Default + Clone + Diff + Serialize,
    {
        v_buf.truncate(0);
        let value = match entry.vlog_value_ref() {
            vlog::Value::Native { value } => value,
            vlog::Value::Reference { .. } => panic!("impossible situation"),
            vlog::Value::Backup { .. } => panic!("impossible situation"),
        };
        value.encode(v_buf);
        v_buf.len()
    }

    fn try_encode_deltas<K, V>(&mut self, entry: &core::Entry<K, V>) -> usize
    where
        K: Clone + Ord + Serialize,
        V: Default + Clone + Diff + Serialize,
    {
        match self {
            ZBlock::Encode { config, .. } if config.vlog_file.is_none() => 0,
            ZBlock::Encode { d_bufs, .. } => Self::encode_deltas(d_bufs, entry),
        }
    }

    fn encode_deltas<K, V>(
        d_bufs: &mut Vec<Vec<u8>>, /* list of buffers for delta encoding */
        entry: &core::Entry<K, V>,
    ) -> usize
    where
        K: Clone + Ord + Serialize,
        V: Default + Clone + Diff + Serialize,
    {
        let mut entry_size = 0;
        d_bufs.truncate(0);
        for (i, delta) in entry.deltas_ref().iter().enumerate() {
            d_bufs[i].truncate(0);
            let d = match delta.vlog_delta_ref() {
                vlog::Delta::Native { delta } => delta,
                vlog::Delta::Reference { .. } => panic!("impossible situation"),
                vlog::Delta::Backup { .. } => panic!("impossible situation"),
            };
            d.encode(&mut d_bufs[i]);
            entry_size += Self::DELTA_HEADER;
        }
        entry_size
    }

    fn compute_next_offset(&self) -> usize {
        match self {
            ZBlock::Encode {
                num_entries,
                offsets,
                ..
            } => {
                let size = mem::size_of_val(num_entries);
                size + (offsets.len() * size)
            }
        }
    }

    fn encode_entry<K, V>(&mut self, entry: &core::Entry<K, V>)
    where
        K: Clone + Ord + Serialize,
        V: Default + Clone + Diff + Serialize,
    {
        self.start_encode_entry();

        let (i_block, v_block, vpos, k_buf, v_buf, d_bufs, config) = match self {
            ZBlock::Encode {
                i_block,
                v_block,
                vpos,
                k_buf,
                v_buf,
                d_bufs,
                config,
                ..
            } => (i_block, v_block, vpos, k_buf, v_buf, d_bufs, config),
        };

        let klen = k_buf.len() as u64;
        let num_deltas = d_bufs.len() as u64;
        let vlen = v_buf.len() as u64;
        Self::encode_header(i_block, klen, num_deltas, vlen, entry, config);

        // key
        i_block.extend_from_slice(k_buf);
        // value
        if config.value_in_vlog {
            let scratch = (*vpos + (v_block.len() as u64)).to_be_bytes();
            i_block.extend_from_slice(&scratch);

            let scratch = (v_buf.len() as u64).to_be_bytes();
            v_block.extend_from_slice(&scratch);
            v_block.extend_from_slice(v_buf);
        } else {
            i_block.extend_from_slice(v_buf);
        }
        // deltas
        if config.vlog_file.is_some() {
            let deltas = entry.deltas_ref();
            for (i, d_buf) in d_bufs.iter().enumerate() {
                let scratch1 = (*vpos + (v_block.len() as u64)).to_be_bytes();

                let scratch2 = (d_buf.len() as u64).to_be_bytes();
                v_block.extend_from_slice(&scratch2);
                v_block.extend_from_slice(d_buf);

                // encode delta in entry
                let delta = &deltas[i];
                let scratch = (d_buf.len() as u64).to_be_bytes();
                i_block.extend_from_slice(&scratch);
                let scratch = delta.born_seqno().to_be_bytes();
                i_block.extend_from_slice(&scratch);
                let scratch = delta.dead_seqno().unwrap_or(0).to_be_bytes();
                i_block.extend_from_slice(&scratch);
                i_block.extend_from_slice(&scratch1);
            }
        }
    }

    fn start_encode_entry(&mut self) {
        match self {
            ZBlock::Encode {
                i_block,
                num_entries,
                offsets,
                ..
            } => {
                *num_entries += 1;
                offsets.push(i_block.len() as u32); // adjust this during flush
            }
        }
    }

    fn encode_header<K, V>(
        i_block: &mut Vec<u8>,
        klen: u64,
        num_deltas: u64,
        vlen: u64,
        entry: &core::Entry<K, V>,
        config: &Config,
    ) where
        K: Clone + Ord + Serialize,
        V: Default + Clone + Diff + Serialize,
    {
        // header field 1, klen and number-of-deltas
        let hdr1 = (klen << 32) | num_deltas;
        let scratch = hdr1.to_be_bytes();
        i_block.extend_from_slice(&scratch);
        // header field 2, value len
        let hdr2 = if config.value_in_vlog {
            vlen | Self::FLAGS_VLOG
        } else {
            vlen
        };
        let scratch = hdr2.to_be_bytes();
        i_block.extend_from_slice(&scratch);
        // header field 3
        let scratch = entry.born_seqno().to_be_bytes();
        i_block.extend_from_slice(&scratch);
        // header field 4
        let scratch = entry.dead_seqno().unwrap_or(0).to_be_bytes();
        i_block.extend_from_slice(&scratch);
    }
}

//pub struct ZEntry<K, V>
//where
//    V: Default + Clone + Serialize + Diff,
//{
//    key: K,
//    value: ZValue,
//    deltas: Vec<ZDelta>,
//}
//
//
//impl<K, V> ZEntry<K, V>
//where
//    V: Default + Clone + Serialize + Diff,
//{
//    const MASK_TOTAL_LEN: u32 = 0xFFFFFFFF00000000;
//    const MASK_KEY_LEN: u32 = 0x00000000FFFFFFFF;
//
//    fn new(key: K, value: ZValue) -> ZEntry {
//        ZEntry{key, value, deltas: vec![]}
//    }
//
//    fn append_delta(&mut self, delta: ZDelta) {
//        self.push(delta);
//    }
//
//    fn value_ref(&self) -> &V {
//        self.value.value_ref()
//    }
//
//    fn encode(&self, file: &fs::File, buf: Vec<u8>) -> Vec<u8> {
//        self.value.value_ref()
//    }
//
//    fn decode(buf: &[u8]) -> Result<(ZEntry, u64), BognError> {
//        let mut scratch = [0_u8; 8];
//        // hdr1
//        scratch.copy_from_slice(&buf[..8]);
//        let (hdr1, off) = (u64::from_be_bytes(scratch), 8);
//        scratch.copy_from_slice(&buf[off..off+8]);
//        let (n_deltas, off) = (u64::from_be_bytes(scratch), off+8);
//        let total_len = hdr1 & Self::MASK_TOTAL_LEN;
//        let key_len = hdr1 & Self::MASK_KEY_LEN;
//        if (off + key_len) > buf.len() {
//            return Err(BognError::BubtZEntryKeyOverflow(key_len));
//        }
//        let (key, off) = (K::decode(buf[off..off+key_len])?, off+key_len);
//
//        let vlen = ZValue::value_len(buf[off..off+8);
//        if (off + vlen) > buf.len() {
//            return Err(BognError::BubtZEntryValueOverflow(vlen));
//        }
//        let (value, zvlen) = ZValue::decode(buf[off..off+vlen)?;
//        let off += zvlen;
//
//        let deltas = vec![];
//        for _i in 0..n_deltas {
//            let dlen = ZDelta::disk_len();
//            if (off + dlen) > buf.len() {
//                return Err(BognError::BubtZEntryDeltaOverflow(vlen));
//            }
//            let (delta, dlen) = ZDelta::decode(buf[off..off+dlen)?;
//            let off += dlen;
//            deltas.push(delta);
//        }
//        ZEntry{key, value, deltas}
//    }
//
//    fn entry_len(buf: &[u8]) -> u64 {
//        let mut scratch = [0_u8; 8];
//        scratch.copy_from_slice(&buf[..8]);
//        (u64::from_be_bytes(scratch) & Self::MASK_TOTAL_LEN) >> 32
//    }
//
//    fn entry_key(buf: &[u8]) -> K {
//        let mut scratch = [0_u8; 8];
//        scratch.copy_from_slice(&buf[..8]);
//        let klen = u64::from_be_bytes(scratch) & Self::MASK_KEY_LEN;
//        K::decode(&buf[8..8+klen]).ok().unwrap()
//    }
//}
