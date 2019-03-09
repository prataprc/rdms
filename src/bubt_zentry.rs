use std::fs::File;

use crate::traits::{AsDelta, Serialize, Diff, AsEntry};

pub struct ZDelta1<'a, V>
where
    V: Default + Clone + Diff,
{
    len: u64,
    seqno: u64,
    deleted: Option<u64>,
    fpos: u64,
}

// Delta:
//
// |flags|      60-bit delta-len              |
// *-----*------------------------------------*
// |              64-bit seqno                |
// *-------------------*----------------------*
// |                fpos                      |
// *------------------------------------------*
//
// Flags: 3-2-1-0  D - Deleted
//            D R  R - File Reference, always 1.
impl<V> ZDelta1<V>
where
    V: Serialize,
{
    const FLAG_FILEREF: u64 = 0x1000000000000000
    const FLAG_DELETED: u64 = 0x2000000000000000
    const MASK_LEN: u64 = 0xF000000000000000

    fn new_bytes(buf: &[u8]) -> ZDelta1<'a, V> {
        ZDelta1::Bytes(buf)
    }

    fn encode(len: u64, seqno: u64, fpos: u64, deleted: bool, buf: Vec<u8>) -> Vec<u8> {
        if len > 1152921504606846975 {
            panic!("delta length {} cannot be > 2^60", len);
        }
        let hdr1 = Self::FLAG_FILEREF | len;
        if deleted {
            hdr1 |= Self::FLAG_DELETED;
        }
        buf.extend_from_slice(&hdr1.to_be_bytes());
        buf.extend_from_slice(&seqno.to_be_bytes());
        buf.extend_from_slice(&fpos.to_be_bytes());
    }

    fn decode(buf: &[u8]) -> ZDelta1 {
        let mut scratch = [0_u8; 8];
        // hdr1
        scratch.copy_from_slice(&buf[..8]);
        let hdr1 = u64::from_be_bytes(scratch);
        let len = hdr1 & Self::MASK_LEN;
        let is_deleted = hdr1 & Self::FLAG_DELETED;
        // seqno
        scratch.copy_from_slice(&buf[8..16]);
        let seqno = u64::from_be_bytes(scratch);
        // fpos
        scratch.copy_from_slice(&buf[16..24]);
        let fpos = u64::from_be_bytes(scratch);

        if len > 1152921504606846975 {
            panic!("delta length {} cannot be > 2^60", len);
        }

        if is_deleted {
            ZDelta1{ len, seqno, fpos, deleted: Some(seqno) };
        } else {
            ZDelta1{ len, seqno, fpos, deleted: None };
        }
    }
}

impl<V> AsVersion<V> for ZValue1<V>
where
    V: Serialize,
{
}

impl<V> ZValue1<V>
where
    V: Serialize,
{
    pub fn encode(&self) -> Result<Vec<u8>, BognError> {
        match self {
            Native{value} => value.encode(),
            File{_} => {
                let msg = "encode called when ZValue1 is File:fpos".to_string();
                BognError::BubtEncode(msg)
            },
        }
    }

    pub fn decode(&self, fd: &mut File) -> Result<Self, BognError> {
        use std::io::SeekFrom;

        match self {
            File{fpos, len} => {
                let mut buf = Vec::with_capacity(len);
                buf.resize(len, 0);
                fd.seek(SeekFrom::Start(fpos))?;
                fd.read_exact(&mut buf)?;
                Ok(Native{value: V::decode(&buf)}?)
            },
            Native{value} => {
                let msg = "value0 already decoded!".to_string();
                BognError::BubtDecode(msg)
            }
        }
    }
}

pub struct ZEntry1<'a, K, V>
where
    V: Serialize,
{
    Parsed{key: K, value: V, seqno: u64, deleted: Option<u64>, deltas: Vec<ZDelta1<V>>},
    Bytes(&'a [u8]),
}

impl<'a, K, V> ZEntry1<'a, K, V>
where
    V: Serialize,
{
    pub fn new_value(key: K, value: V) -> ZEntry1<'a, K,V> {
        ZEntry1::Parsed {
            key: K,
            value: V,
            seqno: Default::default(),
            deleted: Default::default(),
            deltas: Default::default(),
        }
    }

    pub fn new_bytes(buf: &[u8]) -> ZEntry1<'a, K,V> {
        ZEntry1::Bytes(buf)
    }
}

impl<K,V> AsEntry<K,V> for ZEntry1<K,V>
where
    V: Serialize,
{
    pub fn key(&self) -> K {
        self.key
    }

    pub fn key_ref(&self) -> *K {
        &self.key
    }

    pub fn value(&self) -> Option<V> {
        self.value
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted
    }

    pub fn seqno(&self) -> u64 {
        self.seqno
    }
}

// |  32-bit total len |   32-bit key-len     |
// *-------------------*----------------------*
// |                  key                     |
// *-----*------------------------------------*
// |flags|      60-bit value-len              |
// *-----*------------------------------------*
// |              64-bit seqno                |
// *-------------------*----------------------*
// |             value / fpos                 |
// *------------------------------------------*
// |                delta 1                   |
// *------------------------------------------*
// |                delta 2                   |
// *------------------------------------------*
//
impl<K, V> ZEntry1<K, V> {
    fn encode(&self, buf: Vec<u8>) -> (Vec<u8>, u64) {
        // TODO:
    }

    fn decode(&mut self, buf: &[u8]) -> Result<u64, BognError> {
        // TODO:
    }
}
