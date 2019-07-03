use std::{
    convert::TryInto,
    ffi, fs,
    io::{self, Read, Seek},
    mem, path,
};

use crate::core::Serialize;
use crate::error::Error;
use crate::util;
use crate::wal_entry::Entry;

const BATCH_MARKER: &'static str = "vawval-treatment";

pub struct Shard<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    name: String,
    id: usize,
    journals: Vec<Journal<K, V>>,
}

impl<K, V> Shard<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    pub(crate) fn new(name: String, id: usize) -> Shard<K, V> {
        Shard {
            name,
            id,
            journals: vec![],
        }
    }

    #[inline]
    pub(crate) fn add_journal(&mut self, jrn: Journal<K, V>) {
        self.journals.push(jrn)
    }

    #[inline]
    pub(crate) fn id(&self) -> usize {
        self.id
    }
}

pub(crate) struct Journal<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    name: String,
    id: usize,
    num: usize,
    // {name}-shard-{id}-journal-{num}.log
    path: ffi::OsString,
    fd: Option<fs::File>,
    index: u64,                // first index-seqno in this journal.
    batches: Vec<Batch<K, V>>, // batches sorted by index-seqno.
}

impl<K, V> Journal<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    pub(crate) fn create(
        name: String,
        id: usize, // shard id
    ) -> Result<Journal<K, V>, Error> {
        let path = format!("{}-shard-{}-journal-1", name, id);
        let mut opts = fs::OpenOptions::new();
        let fd = opts.append(true).create_new(true).open(&path)?;
        Ok(Journal {
            name,
            id,
            num: 1,
            path: <String as AsRef<ffi::OsStr>>::as_ref(&path).to_os_string(),
            fd: Some(fd),
            index: Default::default(),
            batches: vec![],
        })
    }

    pub(crate) fn load(
        name: String,
        file_path: ffi::OsString, // full path
    ) -> Result<Option<Journal<K, V>>, Error> {
        let (id, num) = match Self::file_parts(&file_path) {
            Some((nm, id, num)) if nm == name => (id, num),
            _ => return Ok(None),
        };
        let batches = Self::load_batches(&file_path)?;
        let mut jrn = Journal {
            name,
            id,
            num,
            path: file_path,
            fd: None,
            index: Default::default(),
            batches: Default::default(),
        };
        jrn.index = batches[0].start_index();
        jrn.batches = batches;
        Ok(Some(jrn))
    }

    fn load_batches(path: &ffi::OsString) -> Result<Vec<Batch<K, V>>, Error> {
        let mut batches = vec![];

        let mut fd = util::open_file_r(&path)?;
        let mut block = Vec::with_capacity(10 * 1024 * 1024);
        block.resize(block.capacity(), 0);

        let (mut fpos, till) = (0_u64, fd.metadata()?.len());
        while fpos < till {
            fd.seek(io::SeekFrom::Start(fpos))?;
            let n = fd.read(&mut block)?;
            if n < block.len() && (fpos + (n as u64)) < till {
                let msg = format!("journal block at {}", fpos);
                return Err(Error::PartialRead(msg));
            }
            let mut m = 0_usize;
            while m < n {
                let mut batch: Batch<K, V> = unsafe { mem::zeroed() };
                m += batch.decode_refer(&block[m..], fpos + (m as u64))?;
                batches.push(batch);
            }
            fpos += n as u64;
        }
        Ok(batches)
    }

    fn file_parts(file_path: &ffi::OsString) -> Option<(String, usize, usize)> {
        let filename = path::Path::new(&file_path)
            .file_name()?
            .to_os_string()
            .into_string()
            .ok()?;

        let mut iter = filename.split('_');
        let name = iter.next()?;
        let shard = iter.next()?;
        let id = iter.next()?;
        let journal = iter.next()?;
        let num = iter.next()?;
        if shard != "shard" || journal != "journal" {
            return None;
        }
        let id = id.parse().ok()?;
        let num = num.parse().ok()?;
        Some((name.to_string(), id, num))
    }

    #[inline]
    pub(crate) fn id(&self) -> usize {
        self.id
    }

    pub(crate) fn purge(self) -> Result<(), Error> {
        fs::remove_file(&self.path)?;
        Ok(())
    }
}

// Active batch become Closed batch, and Closed batch become Refer batch.
enum BatchType {
    Refer = 1,
    Closed,
    Active,
}

#[derive(Clone)]
enum Batch<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    // Reference into the log file where the batch is persisted.
    Refer {
        // position in log-file where the batch starts.
        fpos: u64,
        // length of the batch block
        length: usize,
        // index-seqno of first entry in this batch.
        start_index: u64,
    },
    // Same as active batch, except that it is immutable.
    Closed {
        // position in log-file where the batch starts.
        fpos: u64,
        // length of the batch block
        length: usize,
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
    Active {
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
        Batch::Active {
            config,
            term,
            committed: Default::default(),
            persisted: Default::default(),
            votedfor,
            entries: vec![],
        }
    }

    fn set_term(&mut self, new_term: u64, voted_for: String) -> &mut Batch<K, V> {
        match self {
            Batch::Active { term, votedfor, .. } => {
                *term = new_term;
                *votedfor = voted_for;
            }
            _ => unreachable!(),
        }
        self
    }

    fn set_committed(&mut self, index: u64) -> &mut Batch<K, V> {
        match self {
            Batch::Active { committed, .. } => *committed = index,
            _ => unreachable!(),
        }
        self
    }

    fn set_persisted(&mut self, index: u64) -> &mut Batch<K, V> {
        match self {
            Batch::Active { persisted, .. } => *persisted = index,
            _ => unreachable!(),
        }
        self
    }

    fn add_entry(&mut self, entry: Entry<K, V>) -> &mut Batch<K, V> {
        match self {
            Batch::Active { entries, .. } => entries.push(entry),
            _ => unreachable!(),
        }
        self
    }
}

impl<K, V> Batch<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    fn start_index(&self) -> u64 {
        match self {
            Batch::Refer { start_index, .. } => *start_index,
            Batch::Active { entries, .. } => entries[0].index(),
            Batch::Closed { entries, .. } => entries[0].index(),
        }
    }

    fn fetch(self, fd: &mut fs::File) -> Result<Batch<K, V>, Error> {
        match self {
            Batch::Refer { fpos, length, .. } => {
                let n: u64 = length.try_into().unwrap();
                let buf = util::read_buffer(fd, fpos, n, "fetching batch")?;
                let mut batch: Batch<K, V> = unsafe { mem::zeroed() };
                batch.decode_native(&buf)?;
                Ok(batch)
            }
            Batch::Closed { .. } => Ok(self),
            Batch::Active { .. } => Ok(self),
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
            Batch::Active {
                term,
                committed,
                persisted,
                config,
                votedfor,
                entries,
            } => {
                buf.resize(buf.len() + 8, 0); // adjust for length
                buf.extend_from_slice(&term.to_be_bytes());
                buf.extend_from_slice(&committed.to_be_bytes());
                buf.extend_from_slice(&persisted.to_be_bytes());
                let start_index = entries[0].index();
                buf.extend_from_slice(&start_index.to_be_bytes());
                let nentries: u64 = entries.len().try_into().unwrap();
                buf.extend_from_slice(&nentries.to_be_bytes());

                let mut m = Self::encode_config(buf, config);
                m += Self::encode_votedfor(buf, votedfor);

                m += entries.iter().map(|e| e.encode(buf)).sum::<usize>();

                buf.extend_from_slice(BATCH_MARKER.as_bytes());

                let n = 48 + m + BATCH_MARKER.as_bytes().len() + 8;
                let length: u64 = n.try_into().unwrap();
                buf.extend_from_slice(&length.to_be_bytes());
                buf[..8].copy_from_slice(&length.to_be_bytes());

                n
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

        *self = Batch::Active {
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

            let m = len as usize;
            util::check_remaining(buf, n + m, "batch-config")?;
            let s = std::str::from_utf8(&buf[n..n + m])?;
            config.push(s.to_string());
            n += m;
        }
        Ok((config, n))
    }

    fn encode_votedfor(buf: &mut Vec<u8>, s: &str) -> usize {
        let len: u16 = s.as_bytes().len().try_into().unwrap();
        buf.extend_from_slice(&len.to_be_bytes());
        let mut n = mem::size_of_val(&len);

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
        let n: usize = length.try_into().unwrap();
        let m = n - 8;

        let len = u64::from_be_bytes(buf[m..n].try_into().unwrap());
        if len != length {
            let msg = format!("batch length mismatch, {} {}", len, length);
            return Err(Error::InvalidWAL(msg));
        }

        let (m, n) = (m - BATCH_MARKER.len(), m);
        if BATCH_MARKER.as_bytes() != &buf[m..n] {
            let msg = format!("batch-marker {:?}", &buf[m..n]);
            return Err(Error::InvalidWAL(msg));
        }

        let length: usize = length.try_into().unwrap();
        Ok(length)
    }
}
