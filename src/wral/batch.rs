use arbitrary::{Arbitrary, Unstructured};
use cbordata::Cborize;

use std::{
    cmp,
    fmt::{self, Display},
    fs,
    io::{self, Read, Seek},
    ops, result, vec,
};

use crate::{
    util,
    wral::{self, state},
    Error, Result,
};

pub struct Worker<S> {
    state: S,
    entries: Vec<wral::Entry>, // collection of entries in latest batch
    batches: Vec<Index>,       // collection older batches
}

impl<S> Worker<S> {
    pub fn new(state: S) -> Worker<S> {
        Worker {
            state,
            entries: Vec::default(),
            batches: Vec::default(),
        }
    }

    pub fn add_entry(&mut self, entry: wral::Entry) -> Result<()>
    where
        S: state::State,
    {
        self.state.on_add_entry(&entry)?;
        self.entries.push(entry);
        Ok(())
    }

    pub fn flush(&mut self, file: &mut fs::File) -> Result<Option<Index>>
    where
        S: state::State,
    {
        if !self.entries.is_empty() {
            let fpos = err_at!(IOError, file.metadata())?.len();
            let first_seqno = self.entries.first().map(wral::Entry::to_seqno).unwrap();
            let last_seqno = self.entries.last().map(wral::Entry::to_seqno).unwrap();
            let batch = Batch {
                first_seqno,
                last_seqno,
                state: util::into_cbor_bytes(self.state.clone())?,
                entries: self.entries.drain(..).collect(),
            };

            let length = {
                let data = util::into_cbor_bytes(batch)?;
                util::files::sync_write(file, &data)?;
                data.len()
            };

            let index = Index::new(fpos, length, first_seqno, last_seqno);
            self.batches.push(index.clone());
            Ok(Some(index))
        } else {
            Ok(None)
        }
    }
}

impl<S> Worker<S> {
    pub fn to_last_seqno(&self) -> Option<u64> {
        match self.entries.len() {
            0 => self.batches.last().map(|index| index.last_seqno),
            _ => self.entries.last().map(wral::Entry::to_seqno),
        }
    }

    pub fn as_index(&self) -> &[Index] {
        &self.batches
    }

    pub fn as_entries(&self) -> &[wral::Entry] {
        &self.entries
    }

    pub fn len_batches(&self) -> usize {
        self.batches.len()
    }

    pub fn as_state(&self) -> &S {
        &self.state
    }

    pub fn unwrap(self) -> (Vec<Index>, Vec<wral::Entry>, S) {
        (self.batches, self.entries, self.state)
    }
}

// Batch of entries on disk or in-memory.
#[derive(Debug, Clone, Eq, PartialEq, Cborize)]
pub struct Batch {
    // index-seqno of first entry in this batch.
    first_seqno: u64,
    // index-seqno of last entry in this batch.
    last_seqno: u64,
    // state as serialized bytes, shall be in cbor format.
    state: Vec<u8>,
    // list of entries in this batch.
    entries: Vec<wral::Entry>,
}

impl<'a> arbitrary::Arbitrary<'a> for Batch {
    fn arbitrary(u: &mut Unstructured) -> arbitrary::Result<Self> {
        let mut entries: Vec<wral::Entry> = u.arbitrary()?;
        entries.dedup_by(|a, b| a.to_seqno() == b.to_seqno());
        entries.sort();

        let first_seqno: u64 = entries.first().map(|e| e.to_seqno()).unwrap_or(0);
        let last_seqno: u64 = entries.last().map(|e| e.to_seqno()).unwrap_or(0);

        let batch = Batch {
            first_seqno,
            last_seqno,
            state: u.arbitrary()?,
            entries,
        };
        Ok(batch)
    }
}

impl Display for Batch {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "batch<{}..{}]>", self.first_seqno, self.last_seqno)
    }
}

impl PartialOrd for Batch {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Batch {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.first_seqno.cmp(&other.first_seqno)
    }
}

impl Batch {
    const ID: u32 = 0x0;

    pub fn from_index(index: Index, file: &mut fs::File) -> Result<Batch> {
        err_at!(IOError, file.seek(io::SeekFrom::Start(index.fpos)))?;
        let mut buf = vec![0; index.length];
        err_at!(IOError, file.read_exact(&mut buf))?;
        Ok(util::from_cbor_bytes(&buf)?.0)
    }

    #[inline]
    pub fn as_state_bytes(&self) -> &[u8] {
        &self.state
    }

    #[inline]
    pub fn to_first_seqno(&self) -> u64 {
        self.first_seqno
    }

    #[inline]
    pub fn to_last_seqno(&self) -> u64 {
        self.last_seqno
    }

    pub fn into_iter(
        self,
        range: ops::RangeInclusive<u64>,
    ) -> vec::IntoIter<wral::Entry> {
        self.entries
            .into_iter()
            .filter(|e| range.contains(&e.to_seqno()))
            .collect::<Vec<wral::Entry>>()
            .into_iter()
    }
}

// Index of a batch on disk.
#[derive(Debug, Clone, Eq, PartialEq, Arbitrary)]
pub struct Index {
    // offset in file, where the batch starts.
    fpos: u64,
    // length from offset that spans the entire batch.
    length: usize,
    // first seqno in the batch.
    first_seqno: u64,
    // last seqno in the batch.
    last_seqno: u64,
}

impl Index {
    pub fn new(fpos: u64, length: usize, first_seqno: u64, last_seqno: u64) -> Index {
        Index {
            fpos,
            length,
            first_seqno,
            last_seqno,
        }
    }

    #[inline]
    pub fn to_first_seqno(&self) -> u64 {
        self.first_seqno
    }

    #[inline]
    pub fn to_last_seqno(&self) -> u64 {
        self.last_seqno
    }
}

#[cfg(test)]
#[path = "batch_test.rs"]
mod batch_test;
