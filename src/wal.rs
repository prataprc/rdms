// Takes care of, batching entries, serializing and appending them to disk,
// commiting the appended batch(es).

use std::{convert::TryInto, fs, mem, path};

use crate::core::Serialize;
use crate::error::Error;
use crate::llrb_index::Llrb;
use crate::util;
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
        length: usize,
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

    fn new_refer(fpos: u64, length: usize, index: u64) -> Batch<K, V> {
        Batch::Refer {
            fpos,
            length,
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

    fn start_index(&self) -> u64 {
        match self {
            Batch::Refer { start_index, .. } => *start_index,
            Batch::Native { entries, .. } => entries[0].index(),
        }
    }
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
// |                           start_index                          |
// +----------------------------------------------------------------+
// |                             n-entries                          |
// +----------------------------------------------------------------+
// |                              config                            |
// +----------------------------------------------------------------+
// |                             votedfor                           |
// +--------------------------------+-------------------------------+
// |                              entries                           |
// +--------------------------------+-------------------------------+
// |                            BATCH_MARKER                        |
// +----------------------------------------------------------------+
// |                              length                            |
// +----------------------------------------------------------------+
//
// NOTE: There should atleast one entry in the batch before it is persisted.
impl<K, V> Batch<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    fn encode_native(&self, buf: &mut Vec<u8>) -> usize {
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
                buf.resize(n + 48, 0);

                buf[n + 8..n + 16].copy_from_slice(&term.to_be_bytes());
                buf[n + 16..n + 24].copy_from_slice(&committed.to_be_bytes());
                buf[n + 24..n + 32].copy_from_slice(&persisted.to_be_bytes());
                let start_index = entries[0].index();
                buf[n + 32..n + 40].copy_from_slice(&start_index.to_be_bytes());
                let nentries: u64 = entries.len().try_into().unwrap();
                buf[n + 40..n + 48].copy_from_slice(&nentries.to_be_bytes());

                let mut m = Self::encode_config(buf, config);
                m += Self::encode_votedfor(buf, votedfor);

                m += entries.iter().map(|e| e.encode(buf)).sum::<usize>();

                buf.extend_from_slice(BATCH_MARKER.as_bytes());

                let length: u64 = m.try_into().unwrap();
                buf.extend_from_slice(&length.to_be_bytes());

                48 + m + BATCH_MARKER.as_bytes().len() + 8
            }
            _ => unreachable!(),
        }
    }

    fn decode_refer(&mut self, buf: &[u8], fpos: u64) -> Result<usize, Error> {
        util::check_remaining(buf, 40, "batch-refer-hdr")?;

        let length = Self::validate(buf)?;
        let start_index = u64::from_be_bytes(buf[32..40].try_into().unwrap());
        *self = Batch::Refer {
            fpos,
            length,
            start_index,
        };
        Ok(length)
    }

    fn decode_native(&mut self, buf: &[u8]) -> Result<usize, Error> {
        util::check_remaining(buf, 48, "batch-native-hdr")?;

        let length = Self::validate(buf)?;

        let term = u64::from_be_bytes(buf[8..16].try_into().unwrap());
        let committed = u64::from_be_bytes(buf[16..24].try_into().unwrap());
        let persisted = u64::from_be_bytes(buf[24..32].try_into().unwrap());
        let _start_index = u64::from_be_bytes(buf[32..40].try_into().unwrap());
        let nentries = u64::from_be_bytes(buf[40..48].try_into().unwrap());
        let mut n = 48;

        let (config, m) = Self::decode_config(&buf[n..])?;
        n += m;
        let (votedfor, m) = Self::decode_votedfor(&buf[n..])?;
        n += m;

        let nentries: usize = nentries.try_into().unwrap();
        let mut entries = Vec::with_capacity(nentries);
        for _i in 0..nentries {
            let mut entry: Entry<K, V> = unsafe { mem::zeroed() };
            n += entry.decode(&buf[n..])?;
            entries.push(entry);
        }

        *self = Batch::Native {
            term,
            committed,
            persisted,
            config,
            votedfor,
            entries,
        };
        Ok(length)
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

    fn decode_config(buf: &[u8]) -> Result<(Vec<String>, usize), Error> {
        util::check_remaining(buf, 2, "batch-config")?;
        let count = u16::from_be_bytes(buf[..2].try_into().unwrap());
        let mut config = Vec::with_capacity(count.try_into().unwrap());
        let mut n = 2;
        for _i in 0..count {
            util::check_remaining(buf, n + 2, "batch-config")?;
            let len = u16::from_be_bytes(buf[n..n + 2].try_into().unwrap());
            n += 2;

            util::check_remaining(buf, n + (len as usize), "batch-config")?;
            let s = std::str::from_utf8(&buf[n..n + (len as usize)])?;
            config.push(s.to_string());
            n += len as usize;
        }
        Ok((config, n))
    }

    fn encode_votedfor(buf: &mut Vec<u8>, s: &str) -> usize {
        let len: u16 = s.as_bytes().len().try_into().unwrap();
        let mut n = mem::size_of_val(&len);
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(s.as_bytes());
        n += s.as_bytes().len();
        n
    }

    fn decode_votedfor(buf: &[u8]) -> Result<(String, usize), Error> {
        util::check_remaining(buf, 2, "batch-votedfor")?;
        let len = u16::from_be_bytes(buf[..2].try_into().unwrap());
        let n = 2;
        let len: usize = len.try_into().unwrap();
        util::check_remaining(buf, n + len, "batch-votedfor")?;
        Ok((std::str::from_utf8(&buf[n..n + len])?.to_string(), n + len))
    }

    fn validate(buf: &[u8]) -> Result<usize, Error> {
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

        let length: usize = length.try_into().unwrap();
        Ok(length)
    }
}
