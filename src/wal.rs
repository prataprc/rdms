// Takes care of, batching entries, serializing and appending them to disk,
// commiting the appended batch(es).

use std::convert::TryInto;
use std::sync::atomic::AtomicU64;
use std::{
    collections::HashMap,
    ffi, fs,
    io::{self, Read, Seek},
    mem, path, vec,
};

use crate::core::{Diff, Serialize, Writer};
use crate::{error::Error, util};

pub struct Wal<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    name: String,
    index: AtomicU64,
    nshards: (usize, usize), // (configured, active)
    journals: Vec<Journal<K, V>>,
}

impl<K, V> Wal<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    pub fn create(
        name: String,
        dir: ffi::OsString,
        nshards: usize, // number of shards
    ) -> Result<Wal<K, V>, Error> {
        // purge existing journals for name.
        for item in fs::read_dir(&dir)? {
            let file_name = item?.file_name();
            match Journal::<K, V>::load(name.clone(), file_name)? {
                Some(jrn) => jrn.purge()?,
                None => (),
            }
        }
        // create this WAL. later shards/journals can be added.
        Ok(Wal {
            name,
            index: AtomicU64::new(0),
            nshards: (nshards, 0),
            journals: vec![],
        })
    }

    pub fn load(name: String, dir: ffi::OsString) -> Result<Wal<K, V>, Error> {
        let mut shards: HashMap<usize, bool> = HashMap::new();
        let mut journals = vec![];
        for item in fs::read_dir(&dir)? {
            let dentry = item?;
            // can this be a journal file ?
            if let Some(jrn) = Journal::load(name.clone(), dentry.file_name())? {
                match shards.get_mut(&jrn.id()) {
                    Some(_) => (),
                    None => {
                        shards.insert(jrn.id(), true);
                    }
                }
                journals.push(jrn);
            }
        }
        let mut shards: Vec<usize> = shards.into_iter().map(|(k, _)| k).collect();
        shards.sort();
        for (i, id) in shards.iter().enumerate() {
            if i != id - 1 {
                let msg = format!("invalid shard at {}", i);
                return Err(Error::InvalidWAL(msg));
            }
        }

        Ok(Wal {
            name,
            index: AtomicU64::new(0),
            nshards: (shards.len(), 0),
            journals,
        })
    }

    pub fn spawn_shard(&mut self) -> Result<Shard<K, V>, Error> {
        if self.nshards.1 < self.nshards.0 {
            let id = self.nshards.1 + 1;
            let mut shard = Shard::<K, V>::new(self.name.clone(), id);

            // remove journals for this shard.
            let journals: Vec<Journal<K, V>> =
                self.journals.drain_filter(|jrn| jrn.id() == id).collect();
            journals.into_iter().for_each(|jrn| shard.add_journal(jrn));

            self.nshards.1 += 1;
            Ok(shard)
        } else {
            Err(Error::InvalidWAL(format!("exceeding the shard limit")))
        }
    }
}

impl<K, V> Wal<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    pub fn replay<W: Writer<K, V>>(self, mut w: W) -> Result<usize, Error> {
        let active = self.nshards.1;
        if active > 0 {
            let msg = format!("cannot replay with active shards {}", active);
            return Err(Error::InvalidWAL(msg));
        }
        let mut nentries = 0;
        for journal in self.journals.iter() {
            for entry in journal.to_iter()? {
                let entry = entry?;
                let index = entry.index();
                match entry.into_op() {
                    Op::Set { key, value } => {
                        w.set(key, value, index);
                    }
                    Op::SetCAS { key, value, cas } => {
                        w.set_cas(key, value, cas, index).ok();
                    }
                    Op::Delete { key } => {
                        w.delete(&key, index);
                    }
                }
                nentries += 1;
            }
        }
        Ok(nentries)
    }

    pub fn purge(self) -> Result<(), Error> {
        for jrn in self.journals.into_iter() {
            jrn.purge()?;
        }
        Ok(())
    }
}

enum EntryType {
    Term = 1,
    Client,
}

impl From<u64> for EntryType {
    fn from(value: u64) -> EntryType {
        match value {
            1 => EntryType::Term,
            2 => EntryType::Client,
            _ => unreachable!(),
        }
    }
}

#[derive(Clone)]
pub(crate) enum Entry<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
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
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    fn entry_type(buf: Vec<u8>) -> Result<EntryType, Error> {
        util::check_remaining(&buf, 8, "entry-type")?;
        let hdr1 = u64::from_be_bytes(buf[..8].try_into().unwrap());
        Ok((hdr1 & 0x00000000000000FF).into())
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

    pub(crate) fn index(&self) -> u64 {
        match self {
            Entry::Term { index, .. } => *index,
            Entry::Client { index, .. } => *index,
        }
    }

    pub(crate) fn into_op(self) -> Op<K, V> {
        match self {
            Entry::Term { op, .. } => op,
            Entry::Client { op, .. } => op,
        }
    }
}

impl<K, V> Serialize for Entry<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        match self {
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
// |                            term                                |
// +----------------------------------------------------------------+
// |                            index                               |
// +----------------------------------------------------------------+
// |                         entry-bytes                            |
// +----------------------------------------------------------------+
impl<K, V> Entry<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    fn encode_term(
        buf: &mut Vec<u8>,
        op: &Op<K, V>, // op
        term: u64,
        index: u64,
    ) -> usize {
        buf.extend_from_slice(&(EntryType::Term as u64).to_be_bytes());
        buf.extend_from_slice(&term.to_be_bytes());
        buf.extend_from_slice(&index.to_be_bytes());
        24 + op.encode(buf)
    }

    fn decode_term(
        buf: &[u8],
        op: &mut Op<K, V>,
        term: &mut u64,
        index: &mut u64,
    ) -> Result<usize, Error> {
        util::check_remaining(buf, 24, "entry-term-hdr")?;
        *term = u64::from_be_bytes(buf[8..16].try_into().unwrap());
        *index = u64::from_be_bytes(buf[16..24].try_into().unwrap());
        Ok(24 + op.decode(&buf[24..])?)
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
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    fn encode_client(
        buf: &mut Vec<u8>,
        op: &Op<K, V>,
        term: u64,
        index: u64,
        id: u64,
        ceqno: u64,
    ) -> usize {
        buf.extend_from_slice(&(EntryType::Client as u64).to_be_bytes());
        buf.extend_from_slice(&term.to_be_bytes());
        buf.extend_from_slice(&index.to_be_bytes());
        buf.extend_from_slice(&id.to_be_bytes());
        buf.extend_from_slice(&ceqno.to_be_bytes());
        40 + op.encode(buf)
    }

    fn decode_client(
        buf: &[u8],
        op: &mut Op<K, V>,
        term: &mut u64,
        index: &mut u64,
        id: &mut u64,
        ceqno: &mut u64,
    ) -> Result<usize, Error> {
        util::check_remaining(buf, 40, "entry-client-hdr")?;
        *term = u64::from_be_bytes(buf[8..16].try_into().unwrap());
        *index = u64::from_be_bytes(buf[16..24].try_into().unwrap());
        *id = u64::from_be_bytes(buf[24..32].try_into().unwrap());
        *ceqno = u64::from_be_bytes(buf[32..40].try_into().unwrap());
        Ok(40 + op.decode(&buf[40..])?)
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

#[derive(Clone)]
pub(crate) enum Op<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
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
    K: Clone + Serialize,
    V: Clone + Serialize,
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

    fn op_type(buf: Vec<u8>) -> Result<OpType, Error> {
        util::check_remaining(&buf, 8, "entry-type")?;
        let hdr1 = u64::from_be_bytes(buf[..8].try_into().unwrap());
        Ok(((hdr1 >> 32) & 0x00FFFFFF).into())
    }
}

impl<K, V> Serialize for Op<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
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
    K: Clone + Serialize,
    V: Clone + Serialize,
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
        util::check_remaining(buf, 16, "op-set-hdr")?;
        let hdr1 = u64::from_be_bytes(buf[..8].try_into().unwrap());
        let vlen: usize = u64::from_be_bytes(buf[8..16].try_into().unwrap())
            .try_into()
            .unwrap();
        let mut n = 16;

        let klen: usize = (hdr1 & 0xFFFFFFFF).try_into().unwrap();
        util::check_remaining(buf, n + klen, "op-set-key")?;
        k.decode(&buf[n..n + klen])?;
        n += klen;

        util::check_remaining(buf, n + vlen, "op-set-value")?;
        v.decode(&buf[n..n + vlen])?;
        n += vlen;

        Ok(n.try_into().unwrap())
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
    K: Clone + Serialize,
    V: Clone + Serialize,
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
        util::check_remaining(buf, 24, "op-setcas-hdr")?;
        let hdr1 = u64::from_be_bytes(buf[..8].try_into().unwrap());
        let vlen: usize = u64::from_be_bytes(buf[8..16].try_into().unwrap())
            .try_into()
            .unwrap();
        *cas = u64::from_be_bytes(buf[16..24].try_into().unwrap());
        let mut n = 24;

        let klen: usize = (hdr1 & 0xFFFFFFFF).try_into().unwrap();
        util::check_remaining(buf, n + klen, "op-setcas-key")?;
        k.decode(&buf[n..n + klen])?;
        n += klen;

        util::check_remaining(buf, n + vlen, "op-setcas-value")?;
        v.decode(&buf[n..n + vlen])?;
        n += vlen;

        Ok(n.try_into().unwrap())
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
    K: Clone + Serialize,
    V: Clone + Serialize,
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
        util::check_remaining(buf, 8, "op-delete-hdr1")?;
        let hdr1 = u64::from_be_bytes(buf[..8].try_into().unwrap());
        let mut n = 8;

        let klen: usize = (hdr1 & 0xFFFFFFFF).try_into().unwrap();
        util::check_remaining(buf, n + klen, "op-delete-key")?;
        key.decode(&buf[n..n + klen])?;
        n += klen;

        Ok(n.try_into().unwrap())
    }
}

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
            batches: Default::default(),
        };
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

    pub(crate) fn to_iter(&self) -> Result<JournalIter<K, V>, Error> {
        let mut opts = fs::OpenOptions::new();
        let fd = opts.append(true).create_new(true).open(&self.path)?;
        Ok(JournalIter {
            fd,
            batches: self.batches.clone().into_iter(),
            entries: vec![].into_iter(),
        })
    }
}

pub(crate) struct JournalIter<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    fd: fs::File,
    batches: vec::IntoIter<Batch<K, V>>,
    entries: vec::IntoIter<Entry<K, V>>,
}

impl<K, V> Iterator for JournalIter<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    type Item = Result<Entry<K, V>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.entries.next() {
            None => match self.batches.next() {
                None => None,
                Some(batch) => {
                    let batch = match batch.fetch(&mut self.fd) {
                        Err(err) => return Some(Err(err)),
                        Ok(batch) => batch,
                    };
                    self.entries = batch.into_entries().into_iter();
                    self.next()
                }
            },
            Some(entry) => Some(Ok(entry)),
        }
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

    fn into_entries(self) -> Vec<Entry<K, V>> {
        match self {
            Batch::Refer { .. } => unreachable!(),
            Batch::Active { entries, .. } => entries,
            Batch::Closed { entries, .. } => entries,
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
