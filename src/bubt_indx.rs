// TODO: flush put blocks into tx channel. Right now we simply unwrap()

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

enum ZBlock<K, V>
where
    K: Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
{
    Encode {
        i_block: Vec<u8>, // buffer for z_block
        v_block: Vec<u8>, // buffer for vlog
        offsets: Vec<u32>,
        vpos: u64,
        // working buffers
        first_key: Option<K>,
        k_buf: Vec<u8>,
        v_buf: Vec<u8>,
        d_bufs: Vec<Vec<u8>>,
        config: Config,

        phantom_val: marker::PhantomData<V>,
    },
}

impl<K, V> ZBlock<K, V>
where
    K: Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
{
    const DELTA_HEADER: usize = 8 + 8 + 8 + 8;
    const ENTRY_HEADER: usize = 8 + 8 + 8 + 8;
    const FLAGS_VLOG: u64 = 0x1000000000000000;

    fn new_encode(vpos: u64, config: Config) -> ZBlock<K, V> {
        ZBlock::Encode {
            i_block: Vec::with_capacity(config.z_blocksize),
            v_block: Vec::with_capacity(config.v_blocksize),
            offsets: Default::default(),
            vpos,
            first_key: None,
            // working buffers
            k_buf: Default::default(),
            v_buf: Default::default(),
            d_bufs: Default::default(),
            config,
            phantom_val: marker::PhantomData,
        }
    }

    fn reset(&mut self, vpos: u64) {
        match self {
            ZBlock::Encode {
                i_block,
                v_block,
                offsets,
                vpos: vpos_ref,
                first_key,
                ..
            } => {
                i_block.truncate(0);
                v_block.truncate(0);
                offsets.truncate(0);
                *vpos_ref = vpos;
                *first_key = None;
            }
        }
    }

    fn first_key(&self) -> Option<K> {
        match self {
            ZBlock::Encode { first_key, .. } => first_key.clone(),
        }
    }

    fn insert(&mut self, entry: &Entry<K, V>, stats: &mut Stats) -> Result<()> {
        let mut size = Self::ENTRY_HEADER;
        let kmem = self.encode_key(entry);
        let (vmem1, vmem2) = self.try_encode_value(entry);
        let (dmem1, dmem2) = self.try_encode_deltas(entry);
        size += kmem + vmem1 + dmem1 + self.compute_next_offset();
        stats.keymem += kmem;
        stats.valmem += vmem2 + dmem2;

        match self {
            ZBlock::Encode {
                i_block, first_key, ..
            } => {
                let (req, cap) = ((i_block.len() + size), i_block.capacity());
                if req < cap {
                    first_key.get_or_insert(entry.key());
                    self.encode_entry(entry, vmem2 as u64);
                    Ok(())
                } else {
                    Err(BognError::ZBlockOverflow(req - cap))
                }
            }
        }
    }

    fn encode_key(&mut self, entry: &Entry<K, V>) -> usize {
        match self {
            ZBlock::Encode { k_buf, .. } => {
                k_buf.truncate(0);
                entry.key_ref().encode(k_buf);
                k_buf.len()
            }
        }
    }

    fn try_encode_value(&mut self, entry: &Entry<K, V>) -> (usize, usize) {
        match self {
            ZBlock::Encode { v_buf, config, .. } => {
                let vmem = Self::encode_value(v_buf, entry);
                let hmem = if config.value_in_vlog { 8 } else { vmem };
                (hmem, vmem)
            }
        }
    }

    fn encode_value(v_buf: &mut Vec<u8>, entry: &Entry<K, V>) -> usize {
        v_buf.truncate(0);
        match entry.vlog_value_ref() {
            vlog::Value::Native { value } => {
                value.encode(v_buf);
                v_buf.len()
            }
            vlog::Value::Reference { length, .. } => *length as usize,
            vlog::Value::Backup { .. } => panic!("impossible situation"),
        }
    }

    fn try_encode_deltas(&mut self, entry: &Entry<K, V>) -> (usize, usize) {
        match self {
            ZBlock::Encode { d_bufs, config, .. } => match config.vlog_file {
                None => (0, 0),
                Some(_) => Self::encode_deltas(d_bufs, entry),
            },
        }
    }

    fn encode_deltas(
        d_bufs: &mut Vec<Vec<u8>>, /* list of buffers for delta encoding */
        entry: &Entry<K, V>,
    ) -> (usize, usize) {
        let (mut entry_size, mut dmem) = (0, 0);
        d_bufs.truncate(0);
        for (i, delta) in entry.deltas_ref().iter().enumerate() {
            d_bufs[i].truncate(0);
            let length = match delta.vlog_delta_ref() {
                vlog::Delta::Native { delta } => {
                    delta.encode(&mut d_bufs[i]);
                    d_bufs[i].len()
                }
                vlog::Delta::Reference { length, .. } => *length as usize,
                vlog::Delta::Backup { .. } => panic!("impossible situation"),
            };
            entry_size += Self::DELTA_HEADER;
            dmem += length;
        }
        (entry_size, dmem)
    }

    fn compute_next_offset(&self) -> usize {
        match self {
            ZBlock::Encode { offsets, .. } => 4 + ((offsets.len() + 1) * 4),
        }
    }

    fn encode_entry(&mut self, entry: &Entry<K, V>, vlen: u64) {
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
        Self::encode_header(i_block, klen, num_deltas, vlen, entry, config);

        // key
        i_block.extend_from_slice(k_buf);
        // value
        match entry.vlog_value_ref() {
            vlog::Value::Native { .. } if config.value_in_vlog => {
                let scratch = (*vpos + (v_block.len() as u64)).to_be_bytes();
                i_block.extend_from_slice(&scratch);
                v_block.extend_from_slice(&(v_buf.len() as u64).to_be_bytes());
                v_block.extend_from_slice(v_buf);
            }
            vlog::Value::Native { .. } => {
                i_block.extend_from_slice(v_buf);
            }
            vlog::Value::Reference { fpos, .. } => {
                i_block.extend_from_slice(&fpos.to_be_bytes());
            }
            vlog::Value::Backup { .. } => unreachable!(),
        };

        // deltas
        if config.vlog_file.is_some() {
            for (i, delta) in entry.deltas_ref().iter().enumerate() {
                let (len, bseq, dseq, fpos) = match delta.vlog_delta_ref() {
                    vlog::Delta::Native { .. } => {
                        let fpos = *vpos + (v_block.len() as u64);
                        let d_buf = &d_bufs[i];
                        let scratch = (d_buf.len() as u64).to_be_bytes();
                        v_block.extend_from_slice(&scratch);
                        v_block.extend_from_slice(d_buf);
                        (
                            d_buf.len() as u64,
                            delta.born_seqno(),
                            delta.dead_seqno().unwrap_or(0),
                            fpos,
                        )
                    }
                    vlog::Delta::Reference { fpos, length } => (
                        *length,
                        delta.born_seqno(),
                        delta.dead_seqno().unwrap_or(0),
                        *fpos,
                    ),
                    vlog::Delta::Backup { .. } => unreachable!(),
                };
                // encode delta in entry
                i_block.extend_from_slice(&len.to_be_bytes());
                i_block.extend_from_slice(&bseq.to_be_bytes());
                i_block.extend_from_slice(&dseq.to_be_bytes());
                i_block.extend_from_slice(&fpos.to_be_bytes());
            }
        }
    }

    fn start_encode_entry(&mut self) {
        match self {
            ZBlock::Encode {
                i_block, offsets, ..
            } => {
                offsets.push(i_block.len() as u32); // adjust this in finalize
            }
        }
    }

    fn encode_header(
        i_block: &mut Vec<u8>,
        klen: u64,
        num_deltas: u64,
        vlen: u64,
        entry: &Entry<K, V>,
        config: &Config,
    ) {
        // key header field, klen and number-of-deltas
        let hdr1 = (klen << 32) | num_deltas;
        i_block.extend_from_slice(&hdr1.to_be_bytes());
        // value header field 1, value len
        let hdr2 = if config.value_in_vlog {
            vlen | Self::FLAGS_VLOG
        } else {
            vlen
        };
        i_block.extend_from_slice(&hdr2.to_be_bytes());
        // value header field 2
        i_block.extend_from_slice(&entry.born_seqno().to_be_bytes());
        // value header field 3
        i_block.extend_from_slice(&entry.dead_seqno().unwrap_or(0).to_be_bytes());
    }

    fn finalize(&mut self, stats: &mut Stats) -> (usize, usize) {
        match self {
            ZBlock::Encode {
                i_block,
                v_block,
                config,
                offsets,
                ..
            } => {
                // adjust the offset and encode
                let adjust = 4 + (offsets.len() * 4);
                offsets.iter_mut().for_each(|x| *x += adjust as u32);
                // encode.
                let ln = i_block.len();
                i_block.resize(config.z_blocksize, 0);
                i_block.copy_within(0..ln, adjust);
                let mut n = 4;
                &i_block[..n].copy_from_slice(&(offsets.len() as u32).to_be_bytes());
                for offset in offsets {
                    i_block[n..n + 4].copy_from_slice(&offset.to_be_bytes());
                    n += 4;
                }
                stats.padding += i_block.capacity() - (adjust + ln);
                stats.z_bytes += config.z_blocksize;
                stats.v_bytes += v_block.len();
                (config.z_blocksize, v_block.len())
            }
        }
    }

    fn flush(&mut self, i_flusher: &mut FlushClient, v_flusher: Option<&mut FlushClient>) {
        match self {
            ZBlock::Encode {
                i_block, v_block, ..
            } => {
                i_flusher.send(i_block.clone());
                v_flusher.map(|x| x.send(v_block.clone()));
            }
        }
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

enum MBlock<K>
where
    K: Clone + Ord + Serialize,
{
    Encode {
        i_block: Vec<u8>,
        offsets: Vec<u32>,
        // working buffer
        first_key: Option<K>,
        k_buf: Vec<u8>,
        config: Config,
    },
}

impl<K> MBlock<K>
where
    K: Clone + Ord + Serialize,
{
    const ENTRY_HEADER: usize = 8 + 8;
    const FLAGS_ZBLOCK: u64 = 0x1000000000000000;

    fn new_encode(config: Config) -> MBlock<K> {
        MBlock::Encode {
            i_block: Vec::with_capacity(config.m_blocksize),
            offsets: Default::default(),
            first_key: None,
            k_buf: Default::default(),
            config,
        }
    }

    fn reset(&mut self) {
        match self {
            MBlock::Encode {
                i_block,
                offsets,
                first_key,
                ..
            } => {
                i_block.truncate(0);
                offsets.truncate(0);
                *first_key = None;
            }
        }
    }

    fn first_key(&self) -> Option<K> {
        match self {
            MBlock::Encode { first_key, .. } => first_key.clone(),
        }
    }

    fn insertz(&mut self, key: &K, child_fpos: u64) -> Result<bool> {
        let mut size = Self::ENTRY_HEADER;
        size += self.encode_key(key);
        size += self.compute_next_offset();

        match self {
            MBlock::Encode {
                i_block, first_key, ..
            } => {
                let (req, cap) = ((i_block.len() + size), i_block.capacity());
                if req < cap {
                    first_key.get_or_insert(key.clone());
                    self.encode_entry(child_fpos, true /*zblock*/);
                    Ok(true)
                } else {
                    Err(BognError::ZBlockOverflow(req - cap))
                }
            }
        }
    }

    fn insertm(&mut self, key: &K, child_fpos: u64) -> Result<bool> {
        let mut size = Self::ENTRY_HEADER;
        size += self.encode_key(&key);
        size += self.compute_next_offset();

        match self {
            MBlock::Encode {
                i_block, first_key, ..
            } => {
                let (req, cap) = ((i_block.len() + size), i_block.capacity());
                if req < cap {
                    first_key.get_or_insert(key.clone());
                    self.encode_entry(child_fpos, false /*zblock*/);
                    Ok(true)
                } else {
                    Err(BognError::ZBlockOverflow(req - cap))
                }
            }
        }
    }

    fn encode_key(&mut self, key: &K) -> usize {
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
            MBlock::Encode { offsets, .. } => 4 + ((offsets.len() + 1) * 4),
        }
    }

    fn encode_entry(
        &mut self,
        child_fpos: u64,
        zblock: bool, /* child_fpos points to Z-block */
    ) {
        self.start_encode_entry();

        match self {
            MBlock::Encode { i_block, k_buf, .. } => {
                // header field 1, klen and flags.
                let hdr1 = if zblock {
                    (k_buf.len() as u64) | Self::FLAGS_ZBLOCK
                } else {
                    k_buf.len() as u64
                };
                i_block.extend_from_slice(&hdr1.to_be_bytes());
                // header field 2, child_fpos
                i_block.extend_from_slice(&child_fpos.to_be_bytes());
                i_block.extend_from_slice(k_buf);
            }
        };
    }

    fn start_encode_entry(&mut self) {
        match self {
            MBlock::Encode {
                i_block, offsets, ..
            } => {
                offsets.push(i_block.len() as u32); // adjust this during finalize
            }
        }
    }

    fn finalize(&mut self, stats: &mut Stats) -> usize {
        match self {
            MBlock::Encode {
                i_block,
                offsets,
                config,
                ..
            } => {
                // adjust the offset and encode
                let adjust = 4 + (offsets.len() * 4);
                offsets.iter_mut().for_each(|x| *x += adjust as u32);
                // encode.
                let ln = i_block.len();
                i_block.resize(config.m_blocksize, 0);
                i_block.copy_within(0..ln, adjust);
                let mut n = 4;
                &i_block[..n].copy_from_slice(&(offsets.len() as u32).to_be_bytes());
                for offset in offsets {
                    i_block[n..n + 4].copy_from_slice(&offset.to_be_bytes());
                    n += 4;
                }

                stats.padding += i_block.capacity() - (adjust + ln);
                stats.m_bytes += config.m_blocksize;
                config.m_blocksize
            }
        }
    }

    fn flush(&mut self, i_flusher: &mut FlushClient) {
        match self {
            MBlock::Encode { i_block, .. } => {
                i_flusher.send(i_block.clone());
            }
        }
    }
}