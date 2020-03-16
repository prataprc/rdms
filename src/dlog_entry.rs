use lazy_static::lazy_static;

use std::{
    convert::TryInto,
    fmt, fs,
    io::{self, Read, Seek},
    result,
};

use crate::{
    core::{Result, Serialize},
    dlog::DlogState,
    error::Error,
};

include!("dlog_marker.rs");

#[derive(Clone)]
pub(crate) enum Batch<S, T> {
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
        entries: Vec<DEntry<T>>,
    },
}

impl<S, T> Default for Batch<S, T> {
    fn default() -> Batch<S, T> {
        Batch::Refer {
            fpos: Default::default(),
            length: Default::default(),
            start_index: Default::default(),
            last_index: Default::default(),
        }
    }
}

impl<S, T> PartialEq for Batch<S, T>
where
    S: PartialEq,
    T: PartialEq,
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

impl<S, T> Batch<S, T> {
    pub(crate) fn default_active() -> Batch<S, T>
    where
        S: Default,
    {
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

    pub(crate) fn add_entry(&mut self, entry: DEntry<T>) -> Result<()>
    where
        S: DlogState<T>,
    {
        match self {
            Batch::Active { state, entries } => {
                state.on_add_entry(&entry);
                entries.push(entry);
                Ok(())
            }
            _ => err_at!(Fatal, msg: format!("unreachable")),
        }
    }
}

impl<S, T> Batch<S, T> {
    pub(crate) fn to_first_index(&self) -> Option<u64> {
        match self {
            Batch::Refer { start_index, .. } => Some(*start_index),
            Batch::Active { entries, .. } => {
                let index = entries.first().map(|entry| entry.index);
                index
            }
        }
    }

    pub(crate) fn to_last_index(&self) -> Option<u64> {
        match self {
            Batch::Refer { last_index, .. } => Some(*last_index),
            Batch::Active { entries, .. } => {
                let index = entries.last().map(|entry| entry.index);
                index
            }
        }
    }

    pub(crate) fn len(&self) -> Result<usize> {
        match self {
            Batch::Active { entries, .. } => Ok(entries.len()),
            _ => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    pub(crate) fn into_entries(self) -> Result<Vec<DEntry<T>>> {
        match self {
            Batch::Active { entries, .. } => Ok(entries),
            Batch::Refer { .. } => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    pub(crate) fn into_active(mut self, fd: &mut fs::File) -> Result<Batch<S, T>>
    where
        S: Default + Serialize,
        T: Default + Serialize,
    {
        match self {
            Batch::Refer { fpos, length, .. } => {
                let n: u64 = convert_at!(length)?;
                let buf = read_buffer!(fd, fpos, n, "fetching batch")?;
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
// |                         DLOG_BATCH_MARKER                      |
// +----------------------------------------------------------------+
// |                              length                            |
// +----------------------------------------------------------------+
//
// NOTE: `length` value includes 8-byte length-prefix and 8-byte length-suffix.
impl<S, T> Batch<S, T>
where
    S: Serialize,
    T: Serialize,
{
    pub(crate) fn encode_active(&self, buf: &mut Vec<u8>) -> Result<usize> {
        match self {
            Batch::Active { state, entries } => {
                buf.resize(buf.len() + 8, 0); // adjust for length
                let mut n = 8;

                let start_index = match entries.first() {
                    Some(entry) => entry.index,
                    None => 0,
                };
                buf.extend_from_slice(&start_index.to_be_bytes());
                let last_index = match entries.last() {
                    Some(entry) => entry.index,
                    None => 0,
                };
                buf.extend_from_slice(&last_index.to_be_bytes());
                n += 16;

                n += state.encode(buf)?;

                let nentries: u64 = convert_at!(entries.len())?;
                buf.extend_from_slice(&nentries.to_be_bytes());
                n += 8;
                for entry in entries.iter() {
                    n += entry.encode(buf)?;
                }

                buf.extend_from_slice(DLOG_BATCH_MARKER.as_ref());
                n += DLOG_BATCH_MARKER.len();

                n += 8; // suffix length

                let length: u64 = convert_at!(n)?;
                buf[..8].copy_from_slice(&length.to_be_bytes());
                buf.extend_from_slice(&length.to_be_bytes());

                Ok(n)
            }
            _ => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    pub(crate) fn decode_refer(&mut self, buf: &[u8], fpos: u64) -> Result<usize> {
        check_remaining!(buf, 24, "dlog-batch-refer-hdr")?;

        let length = Self::validate(buf)?;
        let start_index = u64::from_be_bytes(array_at!(buf[8..16])?);
        let last_index = u64::from_be_bytes(array_at!(buf[16..24])?);

        *self = Batch::Refer {
            fpos,
            length,
            start_index,
            last_index,
        };

        Ok(length)
    }

    fn decode_active(&mut self, buf: &[u8]) -> Result<usize>
    where
        S: Default,
        T: Default,
    {
        check_remaining!(buf, 24, "dlog-batch-active-hdr")?;

        let length = Self::validate(buf)?;
        let mut n = 24;

        let mut state: S = Default::default();
        n += state.decode(buf)?;

        let nentries = u64::from_be_bytes(array_at!(buf[n..n + 8])?);
        n += 8;

        let entries = {
            let mut entries = Vec::with_capacity(convert_at!(nentries)?);
            for _i in 0..entries.capacity() {
                let mut entry: DEntry<T> = Default::default();
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
            let n = convert_at!(u64::from_be_bytes(array_at!(buf[..8])?))?;
            (
                n,
                convert_at!(u64::from_be_bytes(array_at!(buf[n - 8..n])?))?,
            )
        };
        if a != z {
            let msg = format!("batch length mismatch, {} {}", a, z);
            return Err(Error::InvalidDlog(msg));
        }

        let (m, n) = (a - 8 - DLOG_BATCH_MARKER.len(), a - 8);
        if DLOG_BATCH_MARKER.as_slice() != &buf[m..n] {
            let msg = format!("batch-marker {:?}", &buf[m..n]);
            return Err(Error::InvalidDlog(msg));
        }

        Ok(a)
    }
}

#[derive(Clone, PartialEq)]
pub struct DEntry<T> {
    // Index seqno for this entry. This will be monotonically
    // increasing number.
    index: u64,
    // Operation to be logged.
    op: T,
}

impl<T> Default for DEntry<T>
where
    T: Default,
{
    fn default() -> DEntry<T> {
        DEntry {
            index: Default::default(),
            op: Default::default(),
        }
    }
}

impl<T> fmt::Debug for DEntry<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "DEntry<term: index:{}  op:{:?}>", self.index, self.op)
    }
}

impl<T> DEntry<T> {
    pub(crate) fn new(index: u64, op: T) -> DEntry<T> {
        DEntry { index, op }
    }

    #[inline]
    pub(crate) fn into_index_op(self) -> (u64, T) {
        (self.index, self.op)
    }
}

// +----------------------------------------------------------------+
// |                            index                               |
// +----------------------------------------------------------------+
// |                           op-bytes                             |
// +----------------------------------------------------------------+
//
impl<T> Serialize for DEntry<T>
where
    T: Serialize,
{
    fn encode(&self, buf: &mut Vec<u8>) -> Result<usize> {
        buf.extend_from_slice(&self.index.to_be_bytes());
        let mut n = 8;

        n += self.op.encode(buf)?;
        Ok(n)
    }

    fn decode(&mut self, buf: &[u8]) -> Result<usize> {
        check_remaining!(buf, 8, "dlog-entry-index")?;
        self.index = u64::from_be_bytes(array_at!(buf[0..8])?);

        let n = 8;
        Ok(n + self.op.decode(&buf[n..])?)
    }
}

#[cfg(test)]
#[path = "dlog_entry_test.rs"]
mod dlog_entry_test;
