use crate::core::{self, Diff, Serialize};
use crate::error::Error;
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
// If deleted seqno is ZERO, then that version was never deleted.

enum EncodeDelta {
    U { hdr: Vec<u8>, blob: Vec<u8> },
    D { hdr: Vec<u8> },
}

impl EncodeDelta {
    const UPSERT_FLAG: u64 = 0x1000000000000000;

    #[inline]
    fn dlen_hdr(blob: &Vec<u8>) -> [u8; 8] {
        ((blob.len() as u64) | Self::UPSERT_FLAG).to_be_bytes()
    }

    fn encode<V>(delta: &core::Delta<V>) -> Result<EncodeDelta, Error>
    where
        V: Clone + Diff,
        <V as Diff>::D: Serialize,
    {
        match delta.as_ref() {
            core::DeltaTuck::U { delta, seqno } => {
                let mut hdr = vec![];
                let blob = vlog::encode_delta(&delta)?;
                hdr.extend_from_slice(&Self::dlen_hdr(&blob)); // diff-len
                hdr.extend_from_slice(&seqno.to_be_bytes());
                hdr.extend_from_slice(&0_u64.to_be_bytes()); // fpos
                Ok(EncodeDelta::U { hdr, blob })
            }
            core::DeltaTuck::D { deleted } => {
                let mut hdr = vec![];
                hdr.extend_from_slice(&0_u64.to_be_bytes()); // diff-len
                hdr.extend_from_slice(&deleted.to_be_bytes());
                hdr.extend_from_slice(&0_u64.to_be_bytes()); // fpos
                Ok(EncodeDelta::D { hdr })
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
// * bit 61: 1 means value in vlog-file, 0 means value in leaf node
// * bit 62: reserved
// * bit 63: reserved

enum EncodeEntry {
    Vlog {
        leaf: Vec<u8>,
        voff: usize,
        doff: usize,
        blob: Vec<u8>,
    },
    Local {
        leaf: Vec<u8>,
        doff: usize,
        blob: Vec<u8>,
    },
}

impl EncodeEntry {
    const UPSERT_FLAG: u64 = 0x1000000000000000;
    const VLOG_FLAG: u64 = 0x2000000000000000;

    #[inline]
    fn vlen_hdr(blob: &Vec<u8>, del: bool, vlog: bool) -> [u8; 8] {
        let mut vlen = blob.len() as u64;
        if !del {
            vlen |= Self::UPSERT_FLAG;
        }
        if vlog {
            vlen |= Self::VLOG_FLAG;
        }
        vlen.to_be_bytes()
    }

    fn encode<K, V>(
        entry: &core::Entry<K, V>,
        vlog: bool, /* whether value to be separate buffer*/
    ) -> Result<EncodeEntry, Error>
    where
        K: Clone + Ord + Serialize,
        V: Clone + Diff + Serialize,
        <V as Diff>::D: Serialize,
    {
        use crate::util::try_convert_int;

        // encode key
        let mut leaf = vec![];
        let klen = entry.as_key().encode(&mut leaf);
        if klen < core::Entry::<i32, i32>::KEY_SIZE_LIMIT {
            return Err(Error::KeySizeExceeded(klen));
        }
        // encode value
        let (mut blob, del, seqno) = match entry.as_value() {
            core::Value::U { value, seqno } => {
                let blob = vlog::encode_value(value)?;
                (blob, false, seqno)
            }
            core::Value::D { deleted } => (vec![], true, deleted),
        };

        // adjust leaf
        leaf.resize(klen + 24, 0);
        leaf.copy_within(0..klen, 24);

        // encode klen
        let klen: u64 = try_convert_int(klen, "key-len: usize->u64")?;
        let dlen = entry.to_delta_count();
        let dlen: u64 = try_convert_int(dlen, "num-deltas usize->u64")?;
        leaf[..8].copy_from_slice(&((klen << 32) | dlen).to_be_bytes());
        // encode vlen
        leaf[8..16].copy_from_slice(&Self::vlen_hdr(&blob, del, vlog));
        // encode seqno
        leaf[16..24].copy_from_slice(&seqno.to_be_bytes());
        // encode value
        let (voff, doff) = if vlog {
            leaf.extend_from_slice(&0_u64.to_be_bytes());
            (leaf.len() - 8, leaf.len())
        } else {
            leaf.extend_from_slice(&blob[8..]); // skip the value-len
            blob.resize(0, 0);
            (0, leaf.len())
        };
        // encode deltas
        for delta in entry.as_deltas() {
            match EncodeDelta::encode(delta)? {
                EncodeDelta::U {
                    hdr,
                    blob: delta_blob,
                } => {
                    blob.extend_from_slice(&delta_blob);
                    leaf.extend_from_slice(&hdr);
                }
                EncodeDelta::D { hdr } => {
                    leaf.extend_from_slice(&hdr);
                }
            }
        }
        Ok(if vlog {
            EncodeEntry::Vlog {
                leaf,
                voff,
                doff,
                blob,
            }
        } else {
            EncodeEntry::Local { leaf, doff, blob }
        })
    }
}
