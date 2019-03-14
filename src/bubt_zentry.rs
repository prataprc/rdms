use std::fs;

use crate::traits::{AsDelta, Serialize, Diff, AsEntry};

pub struct ZDelta1<V>
where
    V: Default + Clone + Serialize + Diff,
{
    len: u64,
    seqno: u64,
    deleted: Option<u64>,
    fpos: u64,
    delta: Option<<V as Diff>::D>,
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
    V: Default + Clone + Serialize + Diff,
{
    const FLAG_FILEREF: u64 = 0x1000000000000000
    const FLAG_DELETED: u64 = 0x2000000000000000
    const MASK_LEN: u64 = 0xF000000000000000
    const DELTA_LEN: u64 = 24;

    fn encode(len: u64, seqno: u64, fpos: u64, deleted: bool, buf: Vec<u8>) -> Result<Vec<u8>, BognError> {
        if len > 1152921504606846975 {
            return Err(BognError::BubtDeltaOverflow(len));
        }
        let hdr1 = Self::FLAG_FILEREF | len;
        if deleted {
            hdr1 |= Self::FLAG_DELETED;
        }
        buf.extend_from_slice(&hdr1.to_be_bytes());
        buf.extend_from_slice(&seqno.to_be_bytes());
        buf.extend_from_slice(&fpos.to_be_bytes());
        Ok(buf)
    }

    fn decode(buf: &[u8]) -> Result<(ZDelta1, u64), BognError> {
        let mut scratch = [0_u8; 8];
        // hdr1
        scratch.copy_from_slice(&buf[..8]);
        let hdr1 = u64::from_be_bytes(scratch);
        let len = hdr1 & Self::MASK_LEN;
        // seqno
        scratch.copy_from_slice(&buf[8..16]);
        let seqno = u64::from_be_bytes(scratch);
        // fpos
        scratch.copy_from_slice(&buf[16..24]);
        let fpos = u64::from_be_bytes(scratch);

        if len > 1152921504606846975 {
            return Err(BognError::BubtDeltaOverflow(len));
        }
        let deleted = if hdr1 & Self::FLAG_DELETED {
            Some(seqno)
        } else {
            None
        }
        (Ok(ZDelta1{len, seqno, fpos, deleted, delta: None}), Self::DELTA_LEN)
    }

    fn set_delta(&mut self, delta: delta: <V as Diff>::D) {
        self.delta =  delta;
    }

    fn disk_len() -> u64 {
        Self::DELTA_LEN
    }

    fn fetch_delta(&self, file: &fs::File, mut buf: Vec<u8>) -> Result<(<V as Diff>::D, Vec<u8>), BognError> {
        use std::io::SeekFrom;

        buf.resize(self.len, 0);
        file.seek(SeekFrom::Start(self.fpos))?;
        file.read_exact(&mut buf)?;
        Ok((D::decode(&buf)?, buf))
    }
}

impl<V> AsDelta<V> for ZDelta1<V>
where
    V: Default + Clone + Serialize + Diff,
{
    fn delta(&self) -> <V as Diff>::D {
        self.delta.unwrap_or_else(|| Default::default())
    }

    fn seqno(&self) -> u64 {
        self.deleted.unwrap_or(self.seqno)
    }

    fn is_deleted(&self) -> bool {
        self.deleted.is_some()
    }
}

pub struct ZValue1<V>
where
    V: Default + Clone + Serialize + Diff,
{
    len: u64,
    seqno: u64,
    deleted: Option<u64>,
    fpos: u64,
    value: Option<V>,
}

// *-----*------------------------------------*
// |flags|      60-bit value-len              |
// *-----*------------------------------------*
// |              64-bit seqno                |
// *-------------------*----------------------*
// |             value / fpos                 |
// *-------------------*----------------------*

impl<V> ZValue1<V>
where
    V: Default + Clone + Serialize + Diff,
{
    fn new(seqno: u64, deleted: Option<u64>, fpos: u64, value: Option<V>) -> ZValue1 {
        ZValue1{
            len: 0,
            seqno,
            deleted,
            fpos,
            value,
        }
    }

    fn encode_with_fpos(&self, len: u64, buf: Vec<u8>) -> Result<Vec<u8>, BognError> {
        if len > 1152921504606846975 {
            return Err(BognError::BubtValueOverflow(len));
        }
        let hdr1 = Self::FLAG_FILEREF | len;
        if self.deleted.is_some() {
            hdr1 |= Self::FLAG_DELETED;
        }
        let seqno = self.deleted.unwrap_or(self.seqno);
        buf.extend_from_slice(&hdr1.to_be_bytes());
        buf.extend_from_slice(&seqno.to_be_bytes());
        buf.extend_from_slice(&fpos.to_be_bytes());
        Ok(buf)
    }

    fn encode_with_value(&self, buf: Vec<u8>) -> Result<Vec<u8>,  BognError> {
        let valbuf = self.value.unwrap().encode(vec![]);
        let len = valbuf.len();
        if len > 1152921504606846975 {
            return Err(BognError::BubtValueOverflow(len));
        }
        let hdr1 = len;
        if self.deleted.is_some() {
            hdr1 |= Self::FLAG_DELETED;
        }
        let seqno = self.deleted.unwrap_or(self.seqno);
        buf.extend_from_slice(&hdr1.to_be_bytes());
        buf.extend_from_slice(&seqno.to_be_bytes());
        buf.extend_from_slice(&valbuf);
        Ok(buf)
    }

    fn decode(buf: &[u8]) -> Result<(ZValue1, u64), BognError> {
        let mut scratch = [0_u8; 8];
        // hdr1
        scratch.copy_from_slice(&buf[..8]);
        let hdr1 = u64::from_be_bytes(scratch);
        let vlen = hdr1 & Self::MASK_LEN;
        // seqno
        scratch.copy_from_slice(&buf[8..16]);
        let seqno = u64::from_be_bytes(scratch);

        if vlen > 1152921504606846975 {
            return Err(BognError::BubtValueOverflow(vlen));
        }

        let (fpos, value, len) = if hdr1 & Self::FLAG_FILEREF {
            scratch.copy_from_slice(&buf[16..24]);
            (u64::from_be_bytes(scratch), None, 24)
        } else {
            (Default::default(), Some(V::decode(&buf[16..16+vlen])?), 16+vlen)
        }
        let deleted = if hdr1 & Self::FLAG_DELETED {
            Some(seqno)
        } else {
            None
        }
        (Ok(ZValue1{len: vlen, seqno, fpos, deleted, value}), len)
    }

    fn value_len(buf: &[u8]) -> u64 {
        let mut scratch = [0_u8; 8];
        scratch.copy_from_slice(&buf[..8]);
        u64::from_be_bytes(scratch) & Self::MASK_LEN
    }

    fn value_ref(&mut self) -> &V {
        self.value.as_ref().unwrap()
    }

    fn fetch_value_ref(&mut self, file: &fs::File, mut buf: Vec<u8>) -> Result<(&V, Vec<u8>), BognError> {
        use std::io::SeekFrom;

        match self.value {
            Some(value) => (Ok(value, buf)),
            None => {
                buf.resize(self.len, 0);
                file.seek(SeekFrom::Start(self.fpos))?;
                file.read_exact(&mut buf)?;
                self.value = V::decode(&buf)?;
                Ok((&self.value, buf))
            }
        }
    }
}

pub struct ZEntry1<K, V>
where
    V: Default + Clone + Serialize + Diff,
{
    key: K,
    value: ZValue1,
    deltas: Vec<ZDelta1>,
}

// |  32-bit total len |   32-bit key-len     |
// *-------------------*----------------------*
// |            number of deltas              |
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

impl<K, V> ZEntry1<K, V>
where
    V: Default + Clone + Serialize + Diff,
{
    const MASK_TOTAL_LEN: u32 = 0xFFFFFFFF00000000;
    const MASK_KEY_LEN: u32 = 0x00000000FFFFFFFF;

    fn new(key: K, value: ZValue1) -> ZEntry1 {
        ZEntry1{key, value, deltas: vec![]}
    }

    fn append_delta(&mut self, delta: ZDelta1) {
        self.push(delta);
    }

    fn value_ref(&self) -> &V {
        self.value.value_ref()
    }

    fn encode(&self, file: &fs::File, buf: Vec<u8>) -> Vec<u8> {
        self.value.value_ref()
    }

    fn decode(buf: &[u8]) -> Result<(ZEntry1, u64), BognError> {
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

        let vlen = ZValue1::value_len(buf[off..off+8);
        if (off + vlen) > buf.len() {
            return Err(BognError::BubtZEntryValueOverflow(vlen));
        }
        let (value, zvlen) = ZValue1::decode(buf[off..off+vlen)?;
        let off += zvlen;

        let deltas = vec![];
        for _i in 0..n_deltas {
            let dlen = ZDelta1::disk_len();
            if (off + dlen) > buf.len() {
                return Err(BognError::BubtZEntryDeltaOverflow(vlen));
            }
            let (delta, dlen) = ZDelta1::decode(buf[off..off+dlen)?;
            let off += dlen;
            deltas.push(delta);
        }
        ZEntry1{key, value, deltas}
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

impl<K,V> AsEntry<K,V> for ZEntry1<K,V>
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

impl<K, V> ZEntry1<K, V> {
    fn encode(&self, buf: Vec<u8>) -> (Vec<u8>, u64) {
        // TODO:
    }

    fn decode(&mut self, buf: &[u8]) -> Result<u64, BognError> {
        // TODO:
    }
}
