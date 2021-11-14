//! [Work in progress]

use std::{convert::TryInto, fmt, result};

use crate::{
    core::{Result, Serialize},
    dlog,
    dlog_entry::DEntry,
    error::Error,
};

// term value when not using consensus
const NIL_TERM: u64 = 0;

// default node name.
const DEFAULT_NODE: &'static str = "no-consensus";

#[derive(Clone, PartialEq)]
pub(crate) struct State {
    // Term is current term for all entries in a batch.
    term: u64,
    // Committed says index upto this index-seqno is
    // replicated and persisted in majority of participating nodes,
    // should always match with first-index of a previous batch.
    committed: u64,
    // Persisted says index upto this index-seqno is persisted
    // in the snapshot, Should always match first-index of a committed
    // batch.
    persisted: u64,
    // List of participating nodes.
    config: Vec<String>,
    // Votedfor is the leader's address in which this batch
    // was created.
    votedfor: String,
}

impl Default for State {
    fn default() -> Self {
        State {
            term: NIL_TERM,
            committed: Default::default(),
            persisted: Default::default(),
            config: Default::default(),
            votedfor: DEFAULT_NODE.to_string(),
        }
    }
}

impl<K, V> dlog::DlogState<Op<K, V>> for State
where
    K: Default + Serialize,
    V: Default + Serialize,
{
    type Key = K;
    type Val = V;

    // TODO: add test cases for this.
    fn on_add_entry(&mut self, _entry: &DEntry<Op<K, V>>) -> () {
        todo!()
    }

    fn to_type(&self) -> String {
        "raft".to_string()
    }
}

impl Serialize for State {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<usize> {
        buf.extend_from_slice(&self.term.to_be_bytes());
        buf.extend_from_slice(&self.committed.to_be_bytes());
        buf.extend_from_slice(&self.persisted.to_be_bytes());
        let mut n = 24;

        let count: u16 = convert_at!(self.config.len())?;
        buf.extend_from_slice(&count.to_be_bytes());
        n += 2;
        for cnf in self.config.iter() {
            let b = cnf.as_bytes();
            {
                let len: u16 = convert_at!(b.len())?;
                buf.extend_from_slice(&len.to_be_bytes());
            }
            buf.extend_from_slice(b);
            n += 2 + b.len();
        }

        let b = self.votedfor.as_bytes();
        let len: u16 = convert_at!(b.len())?;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(b);
        n += 2 + b.len();

        Ok(n)
    }

    fn decode(&mut self, buf: &[u8]) -> Result<usize> {
        use std::str::from_utf8;

        check_remaining!(buf, 24, "raft-batch-config")?;
        self.term = u64::from_be_bytes(array_at!(buf[0..8])?);
        self.committed = u64::from_be_bytes(array_at!(buf[8..16])?);
        self.persisted = u64::from_be_bytes(array_at!(buf[16..24])?);
        let mut n = 24;

        let count = u16::from_be_bytes(array_at!(buf[n..n + 2])?);
        self.config = Vec::with_capacity(convert_at!(count)?);
        n += 2;

        for _i in 0..count {
            check_remaining!(buf, n + 2, "raft-batch-config")?;

            let m: usize = convert_at!(u16::from_be_bytes(array_at!(buf[n..n + 2])?))?;
            n += 2;

            check_remaining!(buf, n + m, "raft-batch-config")?;

            let s = err_at!(InvalidInput, from_utf8(&buf[n..n + m]))?;
            self.config.push(s.to_string());
            n += m;
        }

        check_remaining!(buf, n + 2, "raft-batch-votedfor")?;

        let m: usize = convert_at!(u16::from_be_bytes(array_at!(buf[n..n + 2])?))?;
        n += 2;

        check_remaining!(buf, n + m, "raft-batch-votedfor")?;
        self.votedfor = err_at!(
            //
            InvalidInput,
            from_utf8(&buf[n..n + m])
        )?
        .to_string();
        n += m;

        Ok(n)
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
        check_remaining!(buf, 8, "raft-op-type")?;
        let hdr1 = u64::from_be_bytes(array_at!(buf[..8])?);
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

        let klen: u64 = convert_at!(key.encode(buf)?)?;
        let hdr1: u64 = ((OpType::Set as u64) << 32) | klen;
        let vlen: u64 = convert_at!(value.encode(buf)?)?;

        buf[n..n + 8].copy_from_slice(&hdr1.to_be_bytes());
        buf[n + 8..n + 16].copy_from_slice(&vlen.to_be_bytes());

        Ok(convert_at!((klen + vlen + 16))?)
    }

    fn decode_set(buf: &[u8], k: &mut K, v: &mut V) -> Result<usize> {
        let mut n = 16;
        let (klen, vlen) = {
            check_remaining!(buf, 16, "raft-op-set-hdr")?;
            let hdr1 = u64::from_be_bytes(array_at!(buf[..8])?);
            let klen: usize = convert_at!((hdr1 & 0xFFFFFFFF))?;
            let vlen = u64::from_be_bytes(array_at!(buf[8..16])?);
            let vlen: usize = convert_at!(vlen)?;
            (klen, vlen)
        };

        n += {
            check_remaining!(buf, n + klen, "raft-op-set-key")?;
            k.decode(&buf[n..n + klen])?;
            klen
        };

        n += {
            check_remaining!(buf, n + vlen, "raft-op-set-value")?;
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

        let klen: u64 = convert_at!(key.encode(buf)?)?;
        let hdr1: u64 = ((OpType::SetCAS as u64) << 32) | klen;
        let vlen: u64 = convert_at!(value.encode(buf)?)?;

        buf[n..n + 8].copy_from_slice(&hdr1.to_be_bytes());
        buf[n + 8..n + 16].copy_from_slice(&vlen.to_be_bytes());
        buf[n + 16..n + 24].copy_from_slice(&cas.to_be_bytes());

        Ok(convert_at!((klen + vlen + 24))?)
    }

    fn decode_set_cas(
        buf: &[u8],
        key: &mut K,
        value: &mut V,
        cas: &mut u64, // reference
    ) -> Result<usize> {
        let mut n = 24;
        let (klen, vlen, cas_seqno) = {
            check_remaining!(buf, n, "raft-op-setcas-hdr")?;
            let hdr1 = u64::from_be_bytes(array_at!(buf[..8])?);
            let klen: usize = convert_at!((hdr1 & 0xFFFFFFFF))?;
            let vlen = u64::from_be_bytes(array_at!(buf[8..16])?);
            let vlen: usize = convert_at!(vlen)?;
            let cas = u64::from_be_bytes(array_at!(buf[16..24])?);
            (klen, vlen, cas)
        };
        *cas = cas_seqno;

        n += {
            check_remaining!(buf, n + klen, "raft-op-setcas-key")?;
            key.decode(&buf[n..n + klen])?;
            klen
        };

        n += {
            check_remaining!(buf, n + vlen, "raft-op-setcas-value")?;
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
            let klen: u64 = convert_at!(key.encode(buf)?)?;
            let hdr1: u64 = ((OpType::Delete as u64) << 32) | klen;
            buf[n..n + 8].copy_from_slice(&hdr1.to_be_bytes());
            klen
        };

        Ok(convert_at!((klen + 8))?)
    }

    fn decode_delete(buf: &[u8], key: &mut K) -> Result<usize> {
        let mut n = 8;
        let klen: usize = {
            check_remaining!(buf, n, "raft-op-delete-hdr1")?;
            let hdr1 = u64::from_be_bytes(array_at!(buf[..n])?);
            convert_at!((hdr1 & 0xFFFFFFFF))?
        };

        n += {
            check_remaining!(buf, n + klen, "raft-op-delete-key")?;
            key.decode(&buf[n..n + klen])?;
            klen
        };

        Ok(n)
    }
}

//#[cfg(test)]
//#[path = "raft_log_test.rs"]
//mod raft_log_test;
