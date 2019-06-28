use std::convert::TryInto;

use crate::core::Serialize;
use crate::error::Error;

enum EntryType {
    Op = 1,
    Term,
    Client,
}

impl From<u64> for EntryType {
    fn from(value: u64) -> EntryType {
        match value {
            1 => EntryType::Op,
            2 => EntryType::Term,
            3 => EntryType::Client,
            _ => unreachable!(),
        }
    }
}

pub(crate) enum Entry<K, V>
where
    K: Serialize,
    V: Serialize,
{
    Op {
        op: Op<K, V>,
    },
    Term {
        // Term in which the entry is created.
        term: u64,
        // Index seqno for this entry.
        index: u64,
        // Operation on host data structure.
        op: Op<K, V>,
    },
    Client {
        // Term in which the entry is created.
        term: u64,
        // Index seqno for this entry. This will be monotonically increasing
        // number without any break.
        index: u64,
        // Id of client applying this entry. To deal with false negatives.
        id: u64,
        // Client seqno monotonically increasing number. To deal with
        // false negatives.
        ceqno: u64,
        // Operation on host data structure.
        op: Op<K, V>,
    },
}

impl<K, V> Entry<K, V>
where
    K: Serialize,
    V: Serialize,
{
    pub(crate) fn new_op(op: Op<K, V>) -> Entry<K, V> {
        Entry::Op { op }
    }

    pub(crate) fn new_term(op: Op<K, V>, term: u64, index: u64) -> Entry<K, V> {
        Entry::Term { op, term, index }
    }

    pub(crate) fn new_client(
        op: Op<K, V>,
        term: u64,
        index: u64,
        id: u64,    // client id
        ceqno: u64, // client seqno
    ) -> Entry<K, V> {
        Entry::Client {
            op,
            term,
            index,
            id,
            ceqno,
        }
    }

    fn entry_type(buf: Vec<u8>) -> EntryType {
        let hdr1 = u64::from_be_bytes(buf[..8].try_into().unwrap());
        (hdr1 & 0x00000000000000FF).into()
    }

    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        match self {
            Entry::Op { op } => Self::encode_op(buf, op),
            Entry::Term { op, term, index } => {
                let n = Self::encode_term(buf, op, *term, *index);
                n
            }
            Entry::Client {
                op,
                term,
                index,
                id,
                ceqno,
            } => {
                let n = Self::encode_client(buf, op, *term, *index, *id, *ceqno);
                n
            }
        }
    }

    fn decode(&mut self, buf: &[u8]) -> Result<usize, Error> {
        match self {
            Entry::Op { op } => Self::decode_op(buf, op),
            Entry::Term { op, term, index } => {
                let res = Self::decode_term(buf, op, term, index);
                res
            }
            Entry::Client {
                op,
                term,
                index,
                id,
                ceqno,
            } => {
                let res = Self::decode_client(buf, op, term, index, id, ceqno);
                res
            }
        }
    }
}

// +------------------------------------------------------+---------+
// |                            reserved                  |   type  |
// +----------------------------------------------------------------+
// |                           entry-bytes                          |
// +----------------------------------------------------------------+
impl<K, V> Entry<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn encode_op(buf: &mut Vec<u8>, op: &Op<K, V>) -> usize {
        let n = buf.len();
        buf.resize(n + 8, 0);

        buf[n..n + 8].copy_from_slice(&(EntryType::Op as u64).to_be_bytes());

        op.encode(buf) + 8
    }

    fn decode_op(buf: &[u8], op: &mut Op<K, V>) -> Result<usize, Error> {
        op.decode(&buf[8..])
    }
}

// +------------------------------------------------------+---------+
// |                            reserved                  |   type  |
// +----------------------------------------------------------------+
// |                            term                                |
// +----------------------------------------------------------------+
// |                            index                               |
// +----------------------------------------------------------------+
// |                         entry-bytes                            |
// +----------------------------------------------------------------+
impl<K, V> Entry<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn encode_term(
        buf: &mut Vec<u8>,
        op: &Op<K, V>, // op
        term: u64,
        index: u64,
    ) -> usize {
        let n = buf.len();
        buf.resize(n + 24, 0);

        buf[n..n + 8].copy_from_slice(&(EntryType::Term as u64).to_be_bytes());
        buf[n + 8..n + 16].copy_from_slice(&term.to_be_bytes());
        buf[n + 16..n + 24].copy_from_slice(&index.to_be_bytes());

        op.encode(buf) + 24
    }

    fn decode_term(
        buf: &[u8],
        op: &mut Op<K, V>,
        term: &mut u64,
        index: &mut u64,
    ) -> Result<usize, Error> {
        *term = u64::from_be_bytes(buf[8..16].try_into().unwrap());
        *index = u64::from_be_bytes(buf[16..24].try_into().unwrap());
        op.decode(buf)
    }
}

// +------------------------------------------------------+---------+
// |                            reserved                  |   type  |
// +----------------------------------------------------------------+
// |                            term                                |
// +----------------------------------------------------------------+
// |                            index                               |
// +----------------------------------------------------------------+
// |                          client-id                             |
// +----------------------------------------------------------------+
// |                         client-seqno                           |
// +----------------------------------------------------------------+
// |                         entry-bytes                            |
// +----------------------------------------------------------------+
impl<K, V> Entry<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn encode_client(
        buf: &mut Vec<u8>,
        op: &Op<K, V>,
        term: u64,
        index: u64,
        id: u64,
        ceqno: u64,
    ) -> usize {
        let n = buf.len();
        buf.resize(n + 40, 0);

        buf[n..n + 8].copy_from_slice(&(EntryType::Client as u64).to_be_bytes());
        buf[n + 8..n + 16].copy_from_slice(&term.to_be_bytes());
        buf[n + 16..n + 24].copy_from_slice(&index.to_be_bytes());
        buf[n + 24..n + 32].copy_from_slice(&id.to_be_bytes());
        buf[n + 32..n + 40].copy_from_slice(&ceqno.to_be_bytes());

        op.encode(buf) + 40
    }

    fn decode_client(
        buf: &[u8],
        op: &mut Op<K, V>,
        term: &mut u64,
        index: &mut u64,
        id: &mut u64,
        ceqno: &mut u64,
    ) -> Result<usize, Error> {
        *term = u64::from_be_bytes(buf[8..16].try_into().unwrap());
        *index = u64::from_be_bytes(buf[16..24].try_into().unwrap());
        *id = u64::from_be_bytes(buf[24..32].try_into().unwrap());
        *ceqno = u64::from_be_bytes(buf[32..40].try_into().unwrap());
        op.decode(buf)
    }
}

/************************ Operations within entry ***********************/

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

pub(crate) enum Op<K, V>
where
    K: Serialize,
    V: Serialize,
{
    // Data operations
    Set { key: K, value: V },
    SetCAS { key: K, value: V, cas: u64 },
    Delete { key: K },
    // Config operations,
    // TBD
}

impl<K, V> Op<K, V>
where
    K: Serialize,
    V: Serialize,
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

    fn op_type(buf: Vec<u8>) -> OpType {
        let hdr1 = u64::from_be_bytes(buf[..8].try_into().unwrap());
        ((hdr1 >> 32) & 0x00FFFFFF).into()
    }

    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        match self {
            Op::Set { key, value } => Self::encode_set(buf, key, value),
            Op::SetCAS { key, value, cas } => {
                let n = Self::encode_set_cas(buf, key, value, *cas);
                n
            }
            Op::Delete { key } => Self::encode_delete(buf, key),
        }
    }

    fn decode(&mut self, buf: &[u8]) -> Result<usize, Error> {
        match self {
            Op::Set { key, value } => Self::decode_set(buf, key, value),
            Op::SetCAS { key, value, cas } => {
                let res = Self::decode_set_cas(buf, key, value, cas);
                res
            }
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
    K: Serialize,
    V: Serialize,
{
    fn encode_set(buf: &mut Vec<u8>, key: &K, value: &V) -> usize {
        let n = buf.len();
        buf.resize(n + 16, 0);

        let klen: u64 = key.encode(buf).try_into().unwrap();
        let vlen: u64 = value.encode(buf).try_into().unwrap();

        let optype = OpType::Set as u64;
        let hdr1: u64 = (optype << 32) | klen;
        buf[n..n + 8].copy_from_slice(&hdr1.to_be_bytes());

        buf[n + 8..n + 16].copy_from_slice(&vlen.to_be_bytes());

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
    K: Serialize,
    V: Serialize,
{
    fn encode_set_cas(
        buf: &mut Vec<u8>,
        key: &K,
        value: &V,
        cas: u64, // cas is seqno
    ) -> usize {
        let n = buf.len();
        buf.resize(n + 24, 0);

        let klen: u64 = key.encode(buf).try_into().unwrap();
        let vlen: u64 = value.encode(buf).try_into().unwrap();

        let optype = OpType::SetCAS as u64;
        let hdr1: u64 = (optype << 32) | klen;
        buf[n..n + 8].copy_from_slice(&hdr1.to_be_bytes());

        buf[n + 8..n + 16].copy_from_slice(&vlen.to_be_bytes());
        buf[n + 16..n + 24].copy_from_slice(&cas.to_be_bytes());

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
    K: Serialize,
    V: Serialize,
{
    fn encode_delete(buf: &mut Vec<u8>, key: &K) -> usize {
        let n = buf.len();
        buf.resize(n + 8, 0);

        let klen: u64 = key.encode(buf).try_into().unwrap();

        let optype = OpType::Delete as u64;
        let hdr1: u64 = (optype << 32) | klen;
        buf[n..n + 8].copy_from_slice(&hdr1.to_be_bytes());

        (klen + 8).try_into().unwrap()
    }

    fn decode_delete(buf: &[u8], key: &mut K) -> Result<usize, Error> {
        let hdr1 = u64::from_be_bytes(buf[..8].try_into().unwrap());
        let klen: usize = (hdr1 & 0xFFFFFFFF).try_into().unwrap();
        key.decode(&buf[8..8 + klen])?;

        Ok((klen + 8).try_into().unwrap())
    }
}
