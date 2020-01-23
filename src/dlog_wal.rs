use std::{convert::TryInto, fmt, result};

use crate::{
    core::{Result, Serialize},
    dlog, dlog_entry, util,
};

#[derive(Clone, Default, PartialEq)]
pub(crate) struct State;

impl<K, V> dlog::DlogState<Op<K, V>> for State
where
    K: Default + Serialize,
    V: Default + Serialize,
{
    type Key = K;
    type Val = V;
    type Op = Op<K, V>;

    fn on_add_entry(&mut self, _entry: &dlog_entry::Entry<Op<K, V>>) -> () {
        ()
    }
}

impl Serialize for State {
    fn encode(&self, _buf: &mut Vec<u8>) -> Result<usize> {
        Ok(0)
    }

    fn decode(&mut self, _buf: &[u8]) -> Result<usize> {
        Ok(0)
    }
}
#[derive(PartialEq, Debug)]
enum OpType {
    // Data operations
    Set = 1,
    SetCAS,
    Delete,
    // Config operations
    // TBD
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

#[derive(Clone)]
pub(crate) enum Op<K, V>
where
    K: Default + Serialize,
    V: Default + Serialize,
{
    // Data operations
    Set { key: K, value: V },
    SetCAS { key: K, value: V, cas: u64 },
    Delete { key: K },
}

impl<K, V> Default for Op<K, V>
where
    K: Default + Serialize,
    V: Default + Serialize,
{
    fn default() -> Self {
        Op::Delete {
            key: Default::default(),
        }
    }
}

impl<K, V> PartialEq for Op<K, V>
where
    K: PartialEq + Default + Serialize,
    V: PartialEq + Default + Serialize,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Op::Set {
                    key: key1,
                    value: value1,
                },
                Op::Set {
                    key: key2,
                    value: value2,
                },
            ) => key1 == key2 && value1 == value2,
            (
                Op::SetCAS { key, value, cas },
                Op::SetCAS {
                    key: k,
                    value: v,
                    cas: c,
                },
            ) => key.eq(k) && value.eq(v) && cas.eq(c),
            (Op::Delete { key }, Op::Delete { key: k }) => key == k,
            _ => false,
        }
    }
}

impl<K, V> fmt::Debug for Op<K, V>
where
    K: Default + Serialize + fmt::Debug,
    V: Default + Serialize + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            Op::Set { key: k, value: v } => {
                write!(f, "<Op::Set<key: {:?} value: {:?}>", k, v)?;
            }
            Op::SetCAS {
                key: k,
                value: v,
                cas,
            } => {
                write!(f, "Op::Set<key:{:?} val:{:?} cas:{}>", k, v, cas)?;
            }
            Op::Delete { key } => {
                write!(f, "Op::Set< key: {:?}>", key)?;
            }
        }
        Ok(())
    }
}

impl<K, V> Op<K, V>
where
    K: Default + Serialize,
    V: Default + Serialize,
{
    pub(crate) fn new_set(key: K, value: V) -> Op<K, V> {
        Op::Set { key, value }
    }

    pub(crate) fn new_set_cas(key: K, value: V, cas: u64) -> Op<K, V> {
        Op::SetCAS { cas, key, value }
    }

    pub(crate) fn new_delete(key: K) -> Op<K, V> {
        Op::Delete { key }
    }

    fn op_type(buf: &[u8]) -> Result<OpType> {
        util::check_remaining(buf, 8, "wal op-type")?;
        let hdr1 = u64::from_be_bytes(buf[..8].try_into()?);
        Ok(((hdr1 >> 32) & 0x00FFFFFF).into())
    }
}

impl<K, V> Serialize for Op<K, V>
where
    K: Default + Serialize,
    V: Default + Serialize,
{
    fn encode(&self, buf: &mut Vec<u8>) -> Result<usize> {
        Ok(match self {
            Op::Set { key, value } => {
                let n = Self::encode_set(buf, key, value)?;
                n
            }
            Op::SetCAS { key, value, cas } => {
                let n = Self::encode_set_cas(buf, key, value, *cas)?;
                n
            }
            Op::Delete { key } => {
                let n = Self::encode_delete(buf, key)?;
                n
            }
        })
    }

    fn decode(&mut self, buf: &[u8]) -> Result<usize> {
        let key: K = Default::default();
        *self = match Self::op_type(buf)? {
            OpType::Set => Op::new_set(key, Default::default()),
            OpType::SetCAS => Op::new_set_cas(key, Default::default(), Default::default()),
            OpType::Delete => Op::new_delete(key),
        };

        match self {
            Op::Set { key, value } => Self::decode_set(buf, key, value),
            Op::SetCAS { key, value, cas } => Self::decode_set_cas(buf, key, value, cas),
            Op::Delete { key } => Self::decode_delete(buf, key),
        }
    }
}

// +--------------------------------+-------------------------------+
// | reserved |         op-type     |       key-len                 |
// +--------------------------------+-------------------------------+
// |                            value-len                           |
// +----------------------------------------------------------------+
// |                               key                              |
// +----------------------------------------------------------------+
// |                              value                             |
// +----------------------------------------------------------------+
//
// reserved:  bits 63, 62, 61, 60, 59, 58, 57, 56
// op-type:   24-bit
// key-len:   32-bit
// value-len: 64-bit
//
impl<K, V> Op<K, V>
where
    K: Default + Serialize,
    V: Default + Serialize,
{
    fn encode_set(buf: &mut Vec<u8>, key: &K, value: &V) -> Result<usize> {
        let n = buf.len();
        buf.resize(n + 16, 0);

        let klen: u64 = key.encode(buf)?.try_into()?;
        let hdr1: u64 = ((OpType::Set as u64) << 32) | klen;
        let vlen: u64 = value.encode(buf)?.try_into()?;

        buf[n..n + 8].copy_from_slice(&hdr1.to_be_bytes());
        buf[n + 8..n + 16].copy_from_slice(&vlen.to_be_bytes());

        Ok((klen + vlen + 16).try_into()?)
    }

    fn decode_set(buf: &[u8], k: &mut K, v: &mut V) -> Result<usize> {
        let mut n = 16;
        let (klen, vlen) = {
            util::check_remaining(buf, 16, "wal op-set-hdr")?;
            let hdr1 = u64::from_be_bytes(buf[..8].try_into()?);
            let klen: usize = (hdr1 & 0xFFFFFFFF).try_into()?;
            let vlen = u64::from_be_bytes(buf[8..16].try_into()?);
            let vlen: usize = vlen.try_into()?;
            (klen, vlen)
        };

        n += {
            util::check_remaining(buf, n + klen, "wal op-set-key")?;
            k.decode(&buf[n..n + klen])?;
            klen
        };

        n += {
            util::check_remaining(buf, n + vlen, "wal op-set-value")?;
            v.decode(&buf[n..n + vlen])?;
            vlen
        };

        Ok(n)
    }
}

// +--------------------------------+-------------------------------+
// | reserved |         op-type     |       key-len                 |
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
// reserved:  bits 63, 62, 61, 60, 59, 58, 57, 56
// op-type:   24-bit
// key-len:   32-bit
// value-len: 64-bit
//
impl<K, V> Op<K, V>
where
    K: Default + Serialize,
    V: Default + Serialize,
{
    fn encode_set_cas(
        buf: &mut Vec<u8>,
        key: &K,
        value: &V,
        cas: u64, // cas is seqno
    ) -> Result<usize> {
        let n = buf.len();
        buf.resize(n + 24, 0);

        let klen: u64 = key.encode(buf)?.try_into()?;
        let hdr1: u64 = ((OpType::SetCAS as u64) << 32) | klen;
        let vlen: u64 = value.encode(buf)?.try_into()?;

        buf[n..n + 8].copy_from_slice(&hdr1.to_be_bytes());
        buf[n + 8..n + 16].copy_from_slice(&vlen.to_be_bytes());
        buf[n + 16..n + 24].copy_from_slice(&cas.to_be_bytes());

        Ok((klen + vlen + 24).try_into()?)
    }

    fn decode_set_cas(
        buf: &[u8],
        key: &mut K,
        value: &mut V,
        cas: &mut u64, // reference
    ) -> Result<usize> {
        let mut n = 24;
        let (klen, vlen, cas_seqno) = {
            util::check_remaining(buf, n, "wal op-setcas-hdr")?;
            let hdr1 = u64::from_be_bytes(buf[..8].try_into()?);
            let klen: usize = (hdr1 & 0xFFFFFFFF).try_into()?;
            let vlen = u64::from_be_bytes(buf[8..16].try_into()?);
            let vlen: usize = vlen.try_into()?;
            let cas = u64::from_be_bytes(buf[16..24].try_into()?);
            (klen, vlen, cas)
        };
        *cas = cas_seqno;

        n += {
            util::check_remaining(buf, n + klen, "wal op-setcas-key")?;
            key.decode(&buf[n..n + klen])?;
            klen
        };

        n += {
            util::check_remaining(buf, n + vlen, "wal op-setcas-value")?;
            value.decode(&buf[n..n + vlen])?;
            vlen
        };

        Ok(n)
    }
}

// +--------------------------------+-------------------------------+
// | reserved |         op-type     |       key-len                 |
// +----------------------------------------------------------------+
// |                               key                              |
// +----------------------------------------------------------------+
//
// reserved: bits 63, 62, 61, 60, 59, 58, 57, 56
// op-type:  24-bit
// key-len:  32-bit
//
impl<K, V> Op<K, V>
where
    K: Default + Serialize,
    V: Default + Serialize,
{
    fn encode_delete(buf: &mut Vec<u8>, key: &K) -> Result<usize> {
        let n = buf.len();
        buf.resize(n + 8, 0);

        let klen = {
            let klen: u64 = key.encode(buf)?.try_into()?;
            let hdr1: u64 = ((OpType::Delete as u64) << 32) | klen;
            buf[n..n + 8].copy_from_slice(&hdr1.to_be_bytes());
            klen
        };

        Ok((klen + 8).try_into()?)
    }

    fn decode_delete(buf: &[u8], key: &mut K) -> Result<usize> {
        let mut n = 8;
        let klen: usize = {
            util::check_remaining(buf, n, "wal op-delete-hdr1")?;
            let hdr1 = u64::from_be_bytes(buf[..n].try_into()?);
            (hdr1 & 0xFFFFFFFF).try_into()?
        };

        n += {
            util::check_remaining(buf, n + klen, "wal op-delete-key")?;
            key.decode(&buf[n..n + klen])?;
            klen
        };

        Ok(n)
    }
}

#[cfg(test)]
#[path = "dlog_wal_test.rs"]
mod dlog_wal_test;
