// Takes care of, batching entries, serializing and appending them to disk,
// commiting the appended batch(es).

use std::{convert::TryInto, fs, mem, path};

use crate::core::Serialize;
use crate::error::Error;
use crate::llrb_index::Llrb;
use crate::wal_entry::Entry;

const BATCH_MARKER: &'static str = "vawval-treatment";

// <{name}-shard-{num}>/
// ..
// <{name}-shard-{num}>/
struct Wal<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    name: String,
    seqno: u64,
    shards: Shard<K, V>,
}

// <{name}-shard-{num}>/{name}-shard{num}-journal-{num}.log
//                      ..
//                      {name}-shard{num}-journal-{num}.log
struct Shard<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    num: usize,
    dir: path::PathBuf,
    journals: Vec<Journal<K, V>>,
}

// <{name}-shard-{num}>/{name}-shard{num}-journal-{num}.log
struct Journal<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    num: usize,
    file: String,
    fd: fs::File,
    index: u64,                      // first index-seqno in this journal.
    batches: Llrb<u64, Batch<K, V>>, // batches sorted by index-seqno.
}

enum BatchType {
    Native = 1,
    Refer,
}

#[derive(Clone)]
enum Batch<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    Refer {
        fpos: u64,
        len: usize,
        start_index: u64, // index-seqno of first entry in this journal
    },
    Native {
        // state: term is current term for all entries in a batch.
        term: u64,
        // state: committed says index upto this index-seqno is
        // replicated and persisted in majority of participating nodes,
        // should always match with first-index of a previous batch.
        committed: u64,
        // state: persisted says index upto this index-seqno is persisted
        // in the snapshot, Should always match first-index of a committed
        // batch.
        persisted: u64,
        // state: List of participating entities.
        config: Vec<String>,
        // state: votedfor is the leader's address in which this batch
        // was created.
        votedfor: String,
        // list of entries in this batch.
        entries: Vec<Entry<K, V>>,
    },
}

impl<K, V> Batch<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    fn new(config: Vec<String>, term: u64, votedfor: String) -> Batch<K, V> {
        Batch::Native {
            config,
            term,
            committed: Default::default(),
            persisted: Default::default(),
            votedfor,
            entries: vec![],
        }
    }

    fn new_refer(fpos: u64, len: usize, index: u64) -> Batch<K, V> {
        Batch::Refer {
            fpos,
            len,
            start_index: index,
        }
    }

    fn set_term(&mut self, new_term: u64, voted_for: String) -> &mut Batch<K, V> {
        match self {
            Batch::Native { term, votedfor, .. } => {
                *term = new_term;
                *votedfor = voted_for;
            }
            _ => unreachable!(),
        }
        self
    }

    fn set_committed(&mut self, index: u64) -> &mut Batch<K, V> {
        match self {
            Batch::Native { committed, .. } => *committed = index,
            _ => unreachable!(),
        }
        self
    }

    fn set_persisted(&mut self, index: u64) -> &mut Batch<K, V> {
        match self {
            Batch::Native { persisted, .. } => *persisted = index,
            _ => unreachable!(),
        }
        self
    }

    fn add_entry(&mut self, entry: Entry<K, V>) -> &mut Batch<K, V> {
        match self {
            Batch::Native { entries, .. } => entries.push(entry),
            _ => unreachable!(),
        }
        self
    }

    //fn fetch(self) -> Batch {
    //    match self {
    //        Batch::Native {
    //    }
    //}
}

// +--------------------------------+-------------------------------+
// |                              length                            |
// +--------------------------------+-------------------------------+
// |                              term                              |
// +--------------------------------+-------------------------------+
// |                            committed                           |
// +----------------------------------------------------------------+
// |                            persisted                           |
// +----------------------------------------------------------------+
// |                              config                            |
// +----------------------------------------------------------------+
// |                             votedfor                           |
// +----------------------------------------------------------------+
// |                             entry-len                          |
// +--------------------------------+-------------------------------+
// |                              entry                             |
// +--------------------------------+-------------------------------+
// |                            .........                           |
// +--------------------------------+-------------------------------+
// |                              .....                             |
// +--------------------------------+-------------------------------+
// |                             entry-len                          |
// +--------------------------------+-------------------------------+
// |                              entry                             |
// +--------------------------------+-------------------------------+
// |                            BATCH_MARKER                        |
// +----------------------------------------------------------------+
// |                              length                            |
// +----------------------------------------------------------------+
//
impl<K, V> Serialize for Batch<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        match self {
            Batch::Native {
                term,
                committed,
                persisted,
                config,
                votedfor,
                entries,
            } => {
                let n = buf.len();
                buf.resize(n + 32, 0);

                buf[n + 8..n + 16].copy_from_slice(&term.to_be_bytes());
                buf[n + 16..n + 24].copy_from_slice(&committed.to_be_bytes());
                buf[n + 24..n + 32].copy_from_slice(&persisted.to_be_bytes());

                let mut m = Self::encode_config(buf, config);
                m += Self::encode_votedfor(buf, votedfor);

                m += entries.iter().map(|e| e.encode(buf)).sum::<usize>();

                buf.extend_from_slice(BATCH_MARKER.as_bytes());

                let length: u64 = m.try_into().unwrap();
                let scratch = length.to_be_bytes();
                buf[n..8].copy_from_slice(&scratch);
                buf.extend_from_slice(&scratch);

                m + 32 + BATCH_MARKER.as_bytes().len() + 8
            }
            _ => unreachable!(),
        }
    }

    fn decode(&mut self, buf: &[u8]) -> Result<usize, Error> {
        // TBD
        Ok(0)
    }
}

impl<K, V> Batch<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    fn encode_config(buf: &mut Vec<u8>, config: &Vec<String>) -> usize {
        let count: u16 = config.len().try_into().unwrap();
        buf.extend_from_slice(&count.to_be_bytes());
        let mut n = mem::size_of_val(&count);

        for c in config {
            let len: u16 = c.as_bytes().len().try_into().unwrap();
            n += mem::size_of_val(&len);
            buf.extend_from_slice(&len.to_be_bytes());
            buf.extend_from_slice(c.as_bytes());
            n += c.as_bytes().len();
        }
        n
    }

    fn encode_votedfor(buf: &mut Vec<u8>, s: &str) -> usize {
        let len: u16 = s.as_bytes().len().try_into().unwrap();
        let mut n = mem::size_of_val(&len);
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(s.as_bytes());
        n += s.as_bytes().len();
        n
    }

    fn validate(buf: &[u8]) -> Result<u64, Error> {
        let length = u64::from_be_bytes(buf[..8].try_into().unwrap());

        let (m, n) = (buf.len() - 8, buf.len());
        let len = u64::from_be_bytes(buf[m..n].try_into().unwrap());
        if len != length {
            let msg = format!("length mismatch, {} {}", len, length);
            return Err(Error::InvalidBatch(msg));
        }

        let (m, n) = (buf.len() - 8 - BATCH_MARKER.len(), buf.len() - 8);
        if BATCH_MARKER.as_bytes() != &buf[m..n] {
            let msg = format!("invalid batch-marker {:?}", &buf[m..n]);
            return Err(Error::InvalidBatch(msg));
        }

        Ok(length)
    }
}
