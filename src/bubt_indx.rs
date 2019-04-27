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
//
// Binary format (ZBlock):
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
                size + ((offsets.len() + 1) * size)
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

        // header
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

// Binary format (MEntry):
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
// * bit 60 set = means child-block is a ZBlock.
// * bit 61 reserved
// * bit 62 reserved
// * bit 63 reserved
//
// Binary format (MBlock):
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

pub(crate) enum MBlock {
    Encode {
        i_block: Vec<u8>,
        num_entries: u32,
        offsets: Vec<u32>,
        // working buffer
        k_buf: Vec<u8>,
        config: Config,
    },
}

impl MBlock {
    const ENTRY_HEADER: usize = 8 + 8;
    const FLAGS_ZBLOCK: u64 = 0x1000000000000000;

    pub(crate) fn new_encode(config: Config) -> MBlock {
        MBlock::Encode {
            i_block: Vec::with_capacity(config.m_blocksize),
            num_entries: Default::default(),
            offsets: Default::default(),
            k_buf: Default::default(),
            config,
        }
    }

    pub(crate) fn reset(&mut self) {
        match self {
            MBlock::Encode {
                i_block,
                num_entries,
                offsets,
                ..
            } => {
                i_block.truncate(0);
                *num_entries = Default::default();
                offsets.truncate(0);
            }
        }
    }

    pub(crate) fn insert<K: Serialize>(
        &mut self,
        key: &K,
        child_fpos: u64,
        zblock: bool, /*  child_fpos points to Z-block */
    ) -> bool {
        let mut size = Self::ENTRY_HEADER;
        size += self.encode_key(key);
        size += self.compute_next_offset();

        match self {
            MBlock::Encode { i_block, .. } => {
                if (i_block.len() + size) < i_block.capacity() {
                    self.encode_entry(child_fpos, zblock);
                    true
                } else {
                    false
                }
            }
        }
    }

    fn encode_key<K: Serialize>(&mut self, key: &K) -> usize {
        match self {
            MBlock::Encode { k_buf, .. } => {
                k_buf.truncate(0);
                key.encode(k_buf);
                k_buf.len()
            }
        }
    }

    fn compute_next_offset(&self) -> usize {
        match self {
            MBlock::Encode {
                num_entries,
                offsets,
                ..
            } => {
                let size = mem::size_of_val(num_entries);
                size + ((offsets.len() + 1) * size)
            }
        }
    }

    fn encode_entry(
        &mut self,
        child_fpos: u64,
        zblock: bool, /* child_fpos points to Z-block */
    ) {
        self.start_encode_entry();

        let (i_block, k_buf, config) = match self {
            MBlock::Encode {
                i_block,
                k_buf,
                config,
                ..
            } => (i_block, k_buf, config),
        };

        // header
        let klen = k_buf.len() as u64;
        Self::encode_header(i_block, klen, child_fpos, zblock);
        // key
        i_block.extend_from_slice(k_buf);
    }

    fn start_encode_entry(&mut self) {
        match self {
            MBlock::Encode {
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

    fn encode_header(
        i_block: &mut Vec<u8>,
        klen: u64,
        child_fpos: u64,
        zblock: bool, /* child_fpos points to Z-block*/
    ) {
        // header field 1, klen and flags.
        let hdr1 = if zblock {
            klen | Self::FLAGS_ZBLOCK
        } else {
            klen
        };
        let scratch = hdr1.to_be_bytes();
        i_block.extend_from_slice(&scratch);
        // header field 2, child_fpos
        let scratch = child_fpos.to_be_bytes();
        i_block.extend_from_slice(&scratch);
    }
}
