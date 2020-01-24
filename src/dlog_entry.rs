use lazy_static::lazy_static;

use std::{convert::TryInto, fmt, fs, result};

use crate::{
    core::{Result, Serialize},
    dlog::DlogState,
    error::Error,
    util,
};

include!("dlog_marker.rs");

#[derive(Clone)]
pub(crate) enum Batch<S, T>
where
    S: Default + Serialize + DlogState<T>,
    T: Default + Serialize,
{
    // Reference to immutable batch in log file,
    Refer {
        // position in log-file where the batch starts.
        fpos: u64,
        // length of the batch block
        length: usize,
        // index-seqno of first entry in this batch.
        start_index: u64,
        // index-seqno of last entry in this batch.
        last_index: u64,
    },
    // Current active batch. Once flush is called, it becomes a
    // ``Refer`` varaint and hence immutable.
    Active {
        // batch current state.
        state: S,
        // list of entries in this batch.
        entries: Vec<Entry<T>>,
    },
}

impl<S, T> PartialEq for Batch<S, T>
where
    S: PartialEq + Default + Serialize + DlogState<T>,
    T: PartialEq + Default + Serialize,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Batch::Refer {
                    fpos: f1,
                    length: n1,
                    start_index: s1,
                    last_index: l1,
                },
                Batch::Refer {
                    fpos: f2,
                    length: n2,
                    start_index: s2,
                    last_index: l2,
                },
            ) => f1 == f2 && n1 == n2 && s1 == s2 && l1 == l2,
            (
                Batch::Active {
                    state: s1,
                    entries: e1,
                },
                Batch::Active {
                    state: s2,
                    entries: e2,
                },
            ) => s1 == s2 && e1.eq(e2),
            _ => false,
        }
    }
}

impl<S, T> Batch<S, T>
where
    S: Default + Serialize + DlogState<T>,
    T: Default + Serialize,
{
    pub(crate) fn default_active() -> Batch<S, T> {
        Batch::Active {
            state: Default::default(),
            entries: vec![],
        }
    }

    pub(crate) fn new_refer(
        fpos: u64,
        length: usize,
        start_index: u64,
        last_index: u64,
    ) -> Batch<S, T> {
        Batch::Refer {
            fpos,
            length,
            start_index,
            last_index,
        }
    }

    pub(crate) fn add_entry(&mut self, entry: Entry<T>) {
        match self {
            Batch::Active { state, entries } => {
                state.on_add_entry(&entry);
                entries.push(entry);
            }
            _ => unreachable!(),
        }
    }
}

impl<S, T> Batch<S, T>
where
    S: Default + Serialize + DlogState<T>,
    T: Default + Serialize,
{
    pub(crate) fn to_start_index(&self) -> Option<u64> {
        match self {
            Batch::Refer { start_index, .. } => Some(*start_index),
            Batch::Active { entries, .. } => {
                let index = entries.first().map(|entry| entry.to_index());
                index
            }
        }
    }

    pub(crate) fn to_last_index(&self) -> Option<u64> {
        match self {
            Batch::Refer { last_index, .. } => Some(*last_index),
            Batch::Active { entries, .. } => {
                let index = entries.last().map(|entry| entry.to_index());
                index
            }
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Batch::Active { entries, .. } => entries.len(),
            _ => unreachable!(),
        }
    }

    pub(crate) fn into_entries(self) -> Vec<Entry<T>> {
        match self {
            Batch::Active { entries, .. } => entries,
            Batch::Refer { .. } => unreachable!(),
        }
    }

    pub(crate) fn into_active(mut self, fd: &mut fs::File) -> Result<Batch<S, T>> {
        match self {
            Batch::Refer { fpos, length, .. } => {
                let n: u64 = length.try_into()?;
                let buf = util::read_buffer(fd, fpos, n, "fetching batch")?;
                self.decode_active(&buf)?;

                Ok(self)
            }
            Batch::Active { .. } => Ok(self),
        }
    }
}

// +----------------------------------------------------------------+
// |                              length                            |
// +----------------------------------------------------------------+
// |                            start_index                         |
// +----------------------------------------------------------------+
// |                            last_index                          |
// +----------------------------------------------------------------+
// |                            state-bytes                         |
// +----------------------------------------------------------------+
// |                             n-entries                          |
// +--------------------------------+-------------------------------+
// |                              entries                           |
// +--------------------------------+-------------------------------+
// |                            BATCH_MARKER                        |
// +----------------------------------------------------------------+
// |                              length                            |
// +----------------------------------------------------------------+
//
// NOTE: `length` value includes 8-byte length-prefix and 8-byte length-suffix.
impl<S, T> Batch<S, T>
where
    S: Default + Serialize + DlogState<T>,
    T: Default + Serialize,
{
    pub(crate) fn encode_active(&self, buf: &mut Vec<u8>) -> Result<usize> {
        match self {
            Batch::Active { state, entries } => {
                buf.resize(buf.len() + 8, 0); // adjust for length
                let mut n = 8;

                let start_index = match entries.first() {
                    Some(entry) => entry.to_index(),
                    None => 0,
                };
                buf.extend_from_slice(&start_index.to_be_bytes());
                let last_index = match entries.last() {
                    Some(entry) => entry.to_index(),
                    None => 0,
                };
                buf.extend_from_slice(&last_index.to_be_bytes());
                n += 16;

                n += state.encode(buf)?;

                let nentries: u64 = entries.len().try_into()?;
                buf.extend_from_slice(&nentries.to_be_bytes());
                n += 8;
                for entry in entries.iter() {
                    n += entry.encode(buf)?;
                }

                buf.extend_from_slice(BATCH_MARKER.as_ref());
                n += BATCH_MARKER.len();

                n += 8; // suffix length

                let length: u64 = n.try_into()?;
                buf[..8].copy_from_slice(&length.to_be_bytes());
                buf.extend_from_slice(&length.to_be_bytes());

                Ok(n)
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn decode_refer(&mut self, buf: &[u8], fpos: u64) -> Result<usize> {
        util::check_remaining(buf, 24, "dlog batch-refer-hdr")?;

        let length = Self::validate(buf)?;
        let start_index = u64::from_be_bytes(buf[32..40].try_into()?);
        let last_index = u64::from_be_bytes(buf[40..48].try_into()?);

        *self = Batch::Refer {
            fpos,
            length,
            start_index,
            last_index,
        };

        Ok(length)
    }

    fn decode_active(&mut self, buf: &[u8]) -> Result<usize> {
        util::check_remaining(buf, 24, "dlog batch-active-hdr")?;

        let length = Self::validate(buf)?;
        let mut n = 24;

        let mut state: S = Default::default();
        n += state.decode(buf)?;

        let nentries = u64::from_be_bytes(buf[n..n + 8].try_into()?);
        n += 8;

        let entries = {
            let mut entries = Vec::with_capacity(nentries.try_into()?);
            for _i in 0..entries.capacity() {
                let mut entry: Entry<T> = Default::default();
                n += entry.decode(&buf[n..])?;
                entries.push(entry);
            }

            entries
        };

        *self = Batch::Active { state, entries };

        Ok(length)
    }

    fn validate(buf: &[u8]) -> Result<usize> {
        let (a, z): (usize, usize) = {
            let n = u64::from_be_bytes(buf[..8].try_into()?).try_into()?;
            (n, u64::from_be_bytes(buf[n - 8..n].try_into()?).try_into()?)
        };
        if a != z {
            let msg = format!("batch length mismatch, {} {}", a, z);
            return Err(Error::InvalidDlog(msg));
        }

        let (m, n) = (a - 8 - BATCH_MARKER.len(), a - 8);
        if BATCH_MARKER.as_slice() != &buf[m..n] {
            let msg = format!("batch-marker {:?}", &buf[m..n]);
            return Err(Error::InvalidDlog(msg));
        }

        Ok(a)
    }
}

#[derive(Clone, Default, PartialEq)]
pub(crate) struct Entry<T>
where
    T: Default + Serialize,
{
    // Index seqno for this entry. This will be monotonically
    // increasing number.
    index: u64,
    // Operation to be logged.
    op: T,
}

impl<T> fmt::Debug for Entry<T>
where
    T: Default + Serialize + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "Entry<term: index:{}  op:{:?}>", self.index, self.op)
    }
}

impl<T> Entry<T>
where
    T: Default + Serialize,
{
    pub(crate) fn new(index: u64, op: T) -> Entry<T> {
        Entry { index, op }
    }

    pub(crate) fn to_index(&self) -> u64 {
        self.index
    }

    pub(crate) fn into_op(self) -> T {
        self.op
    }
}

// +----------------------------------------------------------------+
// |                            index                               |
// +----------------------------------------------------------------+
// |                           op-bytes                             |
// +----------------------------------------------------------------+
//
impl<T> Serialize for Entry<T>
where
    T: Default + Serialize,
{
    fn encode(&self, buf: &mut Vec<u8>) -> Result<usize> {
        buf.extend_from_slice(&self.index.to_be_bytes());
        let mut n = 8;

        n += self.op.encode(buf)?;
        Ok(n)
    }

    fn decode(&mut self, buf: &[u8]) -> Result<usize> {
        util::check_remaining(buf, 8, "dlog entry-index")?;
        self.index = u64::from_be_bytes(buf[0..8].try_into()?);

        let n = 8;
        Ok(n + self.op.decode(&buf[n..])?)
    }
}

#[cfg(test)]
#[path = "dlog_entry_test.rs"]
mod dlog_entry_test;
