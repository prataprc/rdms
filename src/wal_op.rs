use std::convert::TryInto;

use crate::core::Serialize;
use crate::error::Error;

enum OpType {
    Set = 1,
    SetCAS,
    Delete,
}

impl From<u64> for OpType {
    fn from(value: u64) -> OpType {
        match value {
            1 => OpType::Set,
            2 => OpType::SetCAS,
            3 => OpType::Delete,
            _ => unreachable!(),
        }
    }
}

enum Op<K, V>
where
    K: Serialize,
    V: Serialize,
{
    Data { op: DataOp<K, V> },
    // Config { op: ConfigOp },
}

enum DataOp<K, V>
where
    K: Serialize,
    V: Serialize,
{
    Set { key: K, value: V },
    SetCAS { cas: u64, key: K, value: V },
    Delete { key: K },
}

impl<K, V> DataOp<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn new_set(key: K, value: V) -> DataOp<K, V> {
        DataOp::Set { key, value }
    }

    fn new_set_cas(key: K, value: V, cas: u64) -> DataOp<K, V> {
        DataOp::SetCAS { cas, key, value }
    }

    fn new_delete(key: K) -> DataOp<K, V> {
        DataOp::Delete { key }
    }

    fn op_type(buf: Vec<u8>) -> OpType {
        let hdr1 = u64::from_be_bytes(buf[..8].try_into().unwrap());
        ((hdr1 >> 32) & 0x00FFFFFF).into()
    }

    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        match self {
            DataOp::Set { key, value } => Self::encode_set(buf, key, value),
            DataOp::SetCAS { key, value, cas } => {
                let n = Self::encode_set_cas(buf, key, value, *cas);
                n
            }
            DataOp::Delete { key } => Self::encode_delete(buf, key),
        }
    }

    fn decode(&mut self, buf: &[u8]) -> Result<usize, Error> {
        match self {
            DataOp::Set { key, value } => Self::decode_set(buf, key, value),
            DataOp::SetCAS { key, value, cas } => {
                let n = Self::decode_set_cas(buf, key, value, cas);
                n
            }
            DataOp::Delete { key } => Self::decode_delete(buf, key),
        }
    }
}

// +--------------------------------+-------------------------------+
// | flags    |         op-type     |       key-len                 |
// +--------------------------------+-------------------------------+
// |                            value-len                           |
// +----------------------------------------------------------------+
// |                               key                              |
// +----------------------------------------------------------------+
// |                              value                             |
// +----------------------------------------------------------------+
//
// flags:     bits, 63, 62, 61, 60, 59, 58, 57, 56 reserved.
// op-type:   24-bit
// key-len:   32-bit
// value-len: 64-bit
//
impl<K, V> DataOp<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn encode_set(buf: &mut Vec<u8>, key: &K, value: &V) -> usize {
        buf.resize(16, 0);

        let klen: u64 = key.encode(buf).try_into().unwrap();
        let vlen: u64 = value.encode(buf).try_into().unwrap();

        let optype = OpType::Set as u64;
        let hdr1: u64 = (optype << 32) | klen;
        buf[..8].copy_from_slice(&hdr1.to_be_bytes());

        buf[8..16].copy_from_slice(&vlen.to_be_bytes());

        (klen + vlen + 16).try_into().unwrap()
    }

    fn decode_set(buf: &[u8], k: &mut K, v: &mut V) -> Result<usize, Error> {
        let hdr1 = u64::from_be_bytes(buf[..8].try_into().unwrap());
        let vlen: usize = u64::from_be_bytes(buf[8..16].try_into().unwrap())
            .try_into()
            .unwrap();

        let klen: usize = (hdr1 & 0xFFFFFFFF).try_into().unwrap();
        k.decode(&buf[16..16 + klen])?;
        v.decode(&buf[16 + klen..16 + klen + vlen])?;

        Ok((klen + vlen + 16).try_into().unwrap())
    }
}

// +--------------------------------+-------------------------------+
// | flags    |         op-type     |       key-len                 |
// +--------------------------------+-------------------------------+
// |                            value-len                           |
// +--------------------------------+-------------------------------+
// |                               cas                              |
// +----------------------------------------------------------------+
// |                               key                              |
// +----------------------------------------------------------------+
// |                              value                             |
// +----------------------------------------------------------------+
//
// flags:     bits, 63, 62, 61, 60, 59, 58, 57, 56 reserved.
// op-type:   24-bit
// key-len:   32-bit
// value-len: 64-bit
//
impl<K, V> DataOp<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn encode_set_cas(
        buf: &mut Vec<u8>,
        key: &K,
        value: &V,
        cas: u64, // cas is seqno
    ) -> usize {
        buf.resize(24, 0);

        let klen: u64 = key.encode(buf).try_into().unwrap();
        let vlen: u64 = value.encode(buf).try_into().unwrap();

        let optype = OpType::SetCAS as u64;
        let hdr1: u64 = (optype << 32) | klen;
        buf[..8].copy_from_slice(&hdr1.to_be_bytes());

        buf[8..16].copy_from_slice(&vlen.to_be_bytes());
        buf[16..24].copy_from_slice(&cas.to_be_bytes());

        (klen + vlen + 24).try_into().unwrap()
    }

    fn decode_set_cas(
        buf: &[u8],
        k: &mut K,
        v: &mut V,
        cas: &mut u64, // reference
    ) -> Result<usize, Error> {
        let hdr1 = u64::from_be_bytes(buf[..8].try_into().unwrap());
        let vlen: usize = u64::from_be_bytes(buf[8..16].try_into().unwrap())
            .try_into()
            .unwrap();
        *cas = u64::from_be_bytes(buf[16..24].try_into().unwrap());

        let klen: usize = (hdr1 & 0xFFFFFFFF).try_into().unwrap();
        k.decode(&buf[24..24 + klen])?;
        v.decode(&buf[24 + klen..24 + klen + vlen])?;

        Ok((klen + vlen + 24).try_into().unwrap())
    }
}

// +--------------------------------+-------------------------------+
// | flags    |         op-type     |       key-len                 |
// +----------------------------------------------------------------+
// |                               key                              |
// +----------------------------------------------------------------+
//
// flags:   bits, 63, 62, 61, 60, 59, 58, 57, 56 reserved.
// op-type: 24-bit
// key-len: 32-bit
//
impl<K, V> DataOp<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn encode_delete(buf: &mut Vec<u8>, key: &K) -> usize {
        buf.resize(8, 0);

        let klen: u64 = key.encode(buf).try_into().unwrap();

        let optype = OpType::Delete as u64;
        let hdr1: u64 = (optype << 32) | klen;
        buf[..8].copy_from_slice(&hdr1.to_be_bytes());

        (klen + 8).try_into().unwrap()
    }

    fn decode_delete(buf: &[u8], key: &mut K) -> Result<usize, Error> {
        let hdr1 = u64::from_be_bytes(buf[..8].try_into().unwrap());
        let klen: usize = (hdr1 & 0xFFFFFFFF).try_into().unwrap();
        key.decode(&buf[8..8 + klen])?;

        Ok((klen + 8).try_into().unwrap())
    }
}
