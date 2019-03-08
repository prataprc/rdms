use std::fs::File;

use crate::traits::{AsVersion, Serialize};

pub enum ZValue1<V>
where
    V: Serialize,
{
    Value{seqno: u64, deleted: bool, value: V},
    FileRef{seqno: u64, deleted: bool, fpos: u64, len: u64},
}

impl<V> ZValue1<V>
where
    V: Serialize,
{
    fn new_native(value: V) -> ZValue1<V> {
        ZValue1::Native{value}
    }

    fn new_ref(seqno: u64, deleted: bool, fpos: u64, len: u64) -> ZValue1<V> {
        ZValue1::FileRef{seqno, deleted, fpos, len}
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

pub struct ZEntry1<K, V>
where
    V: Serialize,
{
    key: K,
    versions: Vec<ZValue1<V>>,
}

impl<K, V> ZEntry1<K, V>
where
    V: Serialize,
{
    pub fn new(key: K, versions: Vec<ZValue1<V>>, seqno: u64, deleted: bool) {
        ZEntry1 {
            key,
            versions,
            seqno,
            deleted: false,
        }
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

    pub fn value(&self) -> Optional<V> {
        self.value
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted
    }

    pub fn seqno(&self) -> u64 {
        self.seqno
    }
}

// |     4-bits flags  | 60-bits seqno        |
// *-------------------*----------------------*
// |           64-bit key-len                 |
// *------------------------------------------*
// |                   key                    |
// *------------------------------------------*
// |             value (optional)             |
// *------------------------------------------*
impl<K, V> ZNode<K, V> {
    pub fn encode(&self, buf: &mut [u8]) -> u64 {
        // TODO
    }

    pub fn decode(&mut self, buf: &[u8]) -> Result<u64, BognError> {
        // TODO
    }
}
