use std::{
    cmp,
    convert::{TryFrom, TryInto},
    ffi, fmt, fs,
    io::Write,
    mem, path, result,
    sync::{atomic::AtomicU64, mpsc, Arc},
    thread, vec,
};

use crate::{
    core::{Result, Serialize},
    dlog::DlogState,
    dlog_entry::{Batch, Entry as DEntry},
    error::Error,
    thread as rt,
    util,
};

// default size for flush buffer.
const FLUSH_SIZE: usize = 1 * 1024 * 1024;

// default block size while loading the Dlog/Journal batches.
const DLOG_BLOCK_SIZE: usize = 10 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct JournalFile(ffi::OsString);

impl JournalFile {
    fn next(self) -> JournalFile {
        let (s, typ, shard_id, num): (String, String, usize, usize) =
            TryFrom::try_from(self).unwrap();
        From::from((s, typ, shard_id, num + 1))
    }
}

impl From<(String, String, usize, usize)> for JournalFile {
    fn from((s, typ, sid, num): (String, String, usize, usize)) -> JournalFile {
        let jfile = format!("{}-{}-shard-{}-journal-{}.dlog", s, typ, sid, num);
        let jfile: &ffi::OsStr = jfile.as_ref();
        JournalFile(jfile.to_os_string())
    }
}

impl TryFrom<JournalFile> for (String, String, usize, usize) {
    type Error = Error;

    fn try_from(jfile: JournalFile) -> Result<(String, String, usize, usize)> {
        use crate::error::Error::InvalidFile;

        let err = format!("{:?} not dlog name", jfile.0);

        let check_file = |jfile: JournalFile| -> Option<String> {
            let fname = path::Path::new(&jfile.0);
            match fname.extension()?.to_str()? {
                "dlog" => Some(fname.file_stem()?.to_str()?.to_string()),
                _ => None,
            }
        };

        let stem = check_file(jfile.clone()).ok_or(InvalidFile(err.clone()))?;
        let parts: Vec<&str> = stem.split('-').collect();

        if parts.len() == 6 {
            match &parts[..] {
                [name, typ, "shard", shard_id, "journal", num] => {
                    let shard_id: usize = shard_id.parse()?;
                    let num: usize = num.parse()?;
                    Ok((name.to_string(), typ.to_string(), shard_id, num))
                }
                _ => Err(InvalidFile(err.clone())),
            }
        } else {
            Err(InvalidFile(err.clone()))
        }
    }
}

impl fmt::Display for JournalFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{:?}", self.0)
    }
}

impl fmt::Debug for JournalFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{:?}", self.0)
    }
}

// shards are monotonically increasing number from 1 to N
struct Shard<S, T>
where
    S: Default + Serialize + DlogState<T>,
    T: Default + Serialize,
{
    dir: ffi::OsString,
    name: String,
    shard_id: usize,
    journal_limit: usize,

    dlog_index: Arc<AtomicU64>,
    journals: Vec<Journal<S, T>>,
    active: Journal<S, T>,
}

impl<S, T> Shard<S, T>
where
    S: Default + Serialize + DlogState<T>,
    T: Default + Serialize,
{
    fn create(
        dir: ffi::OsString,
        name: String,
        shard_id: usize,
        index: Arc<AtomicU64>,
        journal_limit: usize,
    ) -> Result<Shard<S, T>> {
        // purge existing journals for this shard.
        for item in fs::read_dir(&dir)? {
            let file_name = item?.file_name();
            let (n, id) = (name.clone(), shard_id);
            match Journal::<S, T>::new_cold(n, id, dir.clone(), file_name) {
                Some(journal) => journal.purge()?,
                None => (),
            }
        }

        let (d, n) = (dir.clone(), name.clone());
        let active = Journal::<S, T>::new_active(d, n, shard_id, 1)?;

        Ok(Shard {
            dir,
            name,
            shard_id,
            journal_limit,

            dlog_index: index,
            journals: vec![],
            active,
        })
    }

    fn load(
        dir: ffi::OsString,
        name: String,
        shard_id: usize,
        index: Arc<AtomicU64>,
        journal_limit: usize,
    ) -> Result<Shard<S, T>> {
        let mut journals = vec![];

        for item in fs::read_dir(&dir)? {
            let file_name = item?.file_name();
            let (n, id) = (name.clone(), shard_id);
            match Journal::<S, T>::new_archive(n, id, dir.clone(), file_name) {
                Some(journal) => journals.push(journal),
                None => (),
            }
        }

        let active = {
            let num = match journals.last() {
                Some(journal) => journal.to_journal_num().unwrap_or(1),
                None => 1,
            };
            Journal::<S, T>::new_active(dir.clone(), name.clone(), shard_id, num)?
        };

        Ok(Shard {
            dir,
            name,
            shard_id,
            journal_limit,

            dlog_index: index,
            journals,
            active,
        })
    }
}

impl<S, T> Shard<S, T>
where
    S: 'static + Send + Default + Serialize + DlogState<T>,
    T: 'static + Send + Default + Serialize,
{
    fn spawn<Q, R, Self>(self) -> rt::Thread<Q, R, Self> {
        rt::Thread::new(move |rx| move || self.routine(rx))
    }

    fn routine(
        mut self,
        rx: mpsc::Receiver<(OpRequest<T>, Option<mpsc::Sender<OpResponse>>)>,
    ) -> Result<Self>
    where
        S: 'static + Send + Default + Serialize + DlogState<T>,
        T: 'static + Send + Default + Serialize,
    {
        loop {
            let mut cmds = vec![];
            loop {
                match rx.try_recv() {
                    Ok(cmd) => cmds.push(cmd),
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => break,
                }
            }

            match self.do_cmds(cmds) {
                Ok(false) => (),
                Ok(true) => break Ok(self),
                Err(err) => break Err(err),
            }
        }
    }

    // return true if main loop should exit.
    fn do_cmds(
        &mut self,
        cmds: Vec<(OpRequest<T>, Option<mpsc::Sender<OpResponse>>)>,
    ) -> Result<bool> {
        use std::sync::atomic::Ordering::AcqRel;

        for cmd in cmds {
            match cmd {
                (OpRequest::Op { op }, Some(caller)) => {
                    let index = self.dlog_index.fetch_add(1, AcqRel);
                    self.active.add_entry(DEntry::new(index, op));
                    caller.send(OpResponse::new_index(index));
                }
                (OpRequest::PurgeTill { before }, Some(caller)) => {
                    let before = self.do_purge_till(before)?;
                    caller.send(OpResponse::new_purged(before))?;
                }
                (OpRequest::Close, None) => return Ok(true),
                _ => unreachable!(),
            }
        }

        match self.active.flush1(self.journal_limit)? {
            None => (),
            Some((buffer, batch)) => {
                self.rotate_journal()?;
                self.active.flush2(&buffer, batch)?;
            }
        }

        Ok(false)
    }

    // return index or io::Error.
    fn do_purge_till(&mut self, before: u64) -> Result<u64> {
        for _ in 0..self.journals.len() {
            match self.journals[0].to_last_index() {
                Some(last_index) if last_index < before => {
                    self.journals.remove(0).purge()?;
                }
                _ => break,
            }
        }

        Ok(before)
    }

    fn rotate_journal(&mut self) -> Result<()> {
        let new_active = {
            let (d, n, i) = (self.dir.clone(), self.name.clone(), self.shard_id);
            let num = self.active.to_journal_num().unwrap_or(1);
            Journal::<S, T>::new_active(d, n, i, num)?
        };

        self.journals
            .push(mem::replace(&mut self.active, new_active));

        Ok(())
    }
}
struct Journal<S, T>
where
    S: Default + Serialize + DlogState<T>,
    T: Default + Serialize,
{
    shard_id: usize,
    num: usize,               // starts from 1
    file_path: ffi::OsString, // dir/{name}-shard-{shard_id}-journal-{num}.log

    inner: InnerJournal<S, T>,
}

enum InnerJournal<S, T>
where
    S: Default + Serialize + DlogState<T>,
    T: Default + Serialize,
{
    Active {
        file_path: ffi::OsString,
        fd: fs::File,
        batches: Vec<Batch<S, T>>,
        active: Batch<S, T>,
    },
    Archive {
        file_path: ffi::OsString,
        batches: Vec<Batch<S, T>>,
    },
    Cold {
        file_path: ffi::OsString,
    },
}

impl<S, T> Journal<S, T>
where
    S: Default + Serialize + DlogState<T>,
    T: Default + Serialize,
{
    fn new_active(
        dir: ffi::OsString,
        name: String,
        shard_id: usize,
        num: usize,
    ) -> Result<Journal<S, T>> {
        let file: JournalFile = {
            let state: S = Default::default();
            (name.to_string(), state.to_type(), shard_id, num).into()
        };

        let file_path = {
            let mut file_path = path::PathBuf::new();
            file_path.push(&dir);
            file_path.push(&file.0);
            file_path
        };

        fs::remove_file(&file_path).ok(); // cleanup a single journal file

        let file_path: &ffi::OsStr = file_path.as_ref();
        let file_path = file_path.to_os_string();
        let fd = {
            let mut opts = fs::OpenOptions::new();
            opts.append(true).create_new(true).open(&file_path)?
        };

        Ok(Journal {
            shard_id,
            num,
            file_path: file_path.clone(),

            inner: InnerJournal::Active {
                file_path,
                fd: fd,
                batches: Default::default(),
                active: Batch::default_active(),
            },
        })
    }

    fn new_archive(
        name: String,
        shard_id: usize,
        dir: ffi::OsString,
        fname: ffi::OsString,
    ) -> Option<Journal<S, T>> {
        let (nm, _, id, num): (String, String, usize, usize) =
            TryFrom::try_from(JournalFile(fname.clone())).ok()?;

        if nm == name && id == shard_id {
            let file_path = {
                let mut fp = path::PathBuf::new();
                fp.push(&dir);
                fp.push(fname);
                fp.into_os_string()
            };

            let mut batches = vec![];
            let mut fd = util::open_file_r(&file_path).ok()?;
            let (mut fpos, till) = (0_usize, fd.metadata().ok()?.len() as usize);
            while fpos < till {
                let n = cmp::min(DLOG_BLOCK_SIZE, till - fpos) as u64;
                let block = util::read_buffer(
                    //
                    &mut fd,
                    fpos as u64,
                    n,
                    "journal block read",
                )
                .ok()?;

                let mut m = 0_usize;
                while m < block.len() {
                    let mut batch: Batch<S, T> = Batch::default_active();
                    m += batch.decode_refer(&block[m..], (fpos + m) as u64).ok()?;
                    batches.push(batch);
                }
                fpos += block.len();
            }

            Some(Journal {
                shard_id,
                num,
                file_path: file_path.clone(),

                inner: InnerJournal::Archive {
                    file_path: file_path.clone(),
                    batches,
                },
            })
        } else {
            None
        }
    }

    // don't load the batches. use this only for purging the journal.
    fn new_cold(
        name: String,
        shard_id: usize,
        dir: ffi::OsString,
        fname: ffi::OsString,
    ) -> Option<Journal<S, T>> {
        let (nm, _, id, num): (String, String, usize, usize) =
            TryFrom::try_from(JournalFile(fname.clone())).ok()?;

        if nm == name && id == shard_id {
            let file_path = {
                let mut fp = path::PathBuf::new();
                fp.push(&dir);
                fp.push(fname);
                fp.into_os_string()
            };

            Some(Journal {
                shard_id,
                num,
                file_path: file_path.clone(),

                inner: InnerJournal::Cold {
                    file_path: file_path.clone(),
                },
            })
        } else {
            None
        }
    }

    fn purge(self) -> Result<()> {
        match self.inner {
            InnerJournal::Cold { file_path } => fs::remove_file(&file_path)?,
            _ => unreachable!(),
        }
        Ok(())
    }

    #[cfg(test)]
    fn open(&mut self) -> Result<()> {
        self.fd = Some({
            let mut opts = fs::OpenOptions::new();
            opts.read(true).write(false).open(&self.file_path)?
        });
        Ok(())
    }
}

impl<S, T> Journal<S, T>
where
    S: Default + Serialize + DlogState<T>,
    T: Default + Serialize,
{
    fn shard_id(&self) -> usize {
        self.shard_id
    }

    fn to_start_index(&self) -> Option<u64> {
        let batches = match &self.inner {
            InnerJournal::Active { batches, .. } => batches,
            InnerJournal::Archive { batches, .. } => batches,
            _ => unreachable!(),
        };
        batches.first()?.to_start_index()
    }

    fn to_last_index(&self) -> Option<u64> {
        let batches = match &self.inner {
            InnerJournal::Active { batches, .. } => batches,
            InnerJournal::Archive { batches, .. } => batches,
            _ => unreachable!(),
        };
        batches.last()?.to_last_index()
    }

    fn into_iter(mut self) -> Result<BatchIter<S, T>> {
        let (fd, batches) = match self.inner {
            InnerJournal::Active { fd, batches, .. } => (fd, batches),
            InnerJournal::Archive { file_path, batches } => {
                let mut opts = fs::OpenOptions::new();
                let fd = opts.read(true).write(false).open(file_path)?;
                (fd, batches)
            }
            _ => unreachable!(),
        };

        Ok(BatchIter {
            fd: fd,
            batches: batches.into_iter(),
            entries: vec![].into_iter(),
        })
    }

    fn to_journal_num(&self) -> Option<usize> {
        let p = path::Path::new(&self.file_path);
        let (_, _, _, num): (String, String, usize, usize) =
            TryFrom::try_from(JournalFile(p.file_name()?.to_os_string())).ok()?;
        Some(num)
    }

    fn add_entry(&mut self, entry: DEntry<T>) {
        match &mut self.inner {
            InnerJournal::Active { active, .. } => active.add_entry(entry),
            _ => unreachable!(),
        }
    }

    fn into_archive(mut self) -> Option<Self> {
        use InnerJournal::{Active, Archive, Cold};

        match self.inner {
            Active {
                file_path, batches, ..
            } => {
                self.inner = Archive { file_path, batches };
                Some(self)
            }
            Cold { file_path } => {
                let (dir, fname) = {
                    let fp = path::Path::new(&file_path);
                    let fname = fp.file_name()?.to_os_string();
                    let dir = fp.parent()?.as_os_str().to_os_string();
                    (dir, fname)
                };

                let (name, _, shard_id, _): (String, String, usize, usize) =
                    TryFrom::try_from(JournalFile(fname.clone())).ok()?;

                Some(Self::new_archive(name, shard_id, dir, fname)?)
            }
            _ => unreachable!(),
        }
    }

    fn into_cold(mut self) -> Option<Self> {
        use InnerJournal::{Archive, Cold};

        self.inner = match self.inner {
            Archive { file_path, .. } => Cold { file_path },
            _ => unreachable!(),
        };
        Some(self)
    }
}

impl<S, T> Journal<S, T>
where
    S: Default + Serialize + DlogState<T>,
    T: Default + Serialize,
{
    fn flush1(&mut self, lmt: usize) -> Result<Option<(Vec<u8>, Batch<S, T>)>> {
        let (file_path, fd, batches, active, exceeded) = match &mut self.inner {
            InnerJournal::Active {
                file_path,
                fd,
                batches,
                active,
            } => {
                let limit: u64 = lmt.try_into()?;
                let exceeded = fd.metadata()?.len() > limit;
                (file_path, fd, batches, active, exceeded)
            }
            _ => unreachable!(),
        };

        let mut buffer = Vec::with_capacity(FLUSH_SIZE);
        let want = active.encode_active(&mut buffer)?;

        match exceeded {
            true if active.len() > 0 => {
                // rotate journal files.
                let a = active.to_start_index().unwrap();
                let z = active.to_last_index().unwrap();
                let batch = Batch::new_refer(0, want, a, z);
                Ok(Some((buffer, batch)))
            }
            false if active.len() > 0 => {
                let fpos = fd.metadata()?.len();
                let n = fd.write(&buffer)?;
                if want != n {
                    let f = file_path.clone();
                    let msg = format!("wal-flush: {:?}, {}/{}", f, want, n);
                    Err(Error::PartialWrite(msg))
                } else {
                    fd.sync_all()?; // TODO: <- disk bottle-neck

                    let a = active.to_start_index().unwrap();
                    let z = active.to_last_index().unwrap();
                    let batch = Batch::new_refer(fpos, want, a, z);
                    batches.push(batch);
                    *active = Batch::default_active();
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    fn flush2(&mut self, buffer: &[u8], mut batch: Batch<S, T>) -> Result<()> {
        let (file_path, fd, batches, active) = match &mut self.inner {
            InnerJournal::Active {
                file_path,
                fd,
                batches,
                active,
            } => (file_path, fd, batches, active),
            _ => unreachable!(),
        };

        let length = buffer.len();
        let fpos = fd.metadata()?.len();
        let n = fd.write(&buffer)?;
        if length == n {
            fd.sync_all()?; // TODO: <- disk bottle-neck

            let a = batch.to_start_index().unwrap();
            let z = batch.to_last_index().unwrap();
            batch = Batch::new_refer(fpos, length, a, z);
            batches.push(batch);
            *active = Batch::default_active();
            Ok(())
        } else {
            let f = file_path.clone();
            let msg = format!("wal-flush: {:?}, {}/{}", f, length, n);
            Err(Error::PartialWrite(msg))
        }
    }
}

enum OpRequest<T>
where
    T: Default + Serialize,
{
    Op { op: T },
    PurgeTill { before: u64 },
    Close,
}

impl<T> OpRequest<T>
where
    T: Default + Serialize,
{
    fn new_op(op: T) -> OpRequest<T> {
        OpRequest::Op { op }
    }

    fn new_purge_till(before: u64) -> OpRequest<T> {
        OpRequest::PurgeTill { before }
    }

    fn new_close() -> OpRequest<T> {
        OpRequest::Close
    }
}

#[derive(PartialEq)]
enum OpResponse {
    Index(u64),
    Purged(u64),
}

impl OpResponse {
    fn new_index(index: u64) -> OpResponse {
        OpResponse::Index(index)
    }

    fn new_purged(index: u64) -> OpResponse {
        OpResponse::Purged(index)
    }
}

struct BatchIter<S, T>
where
    S: Default + Serialize + DlogState<T>,
    T: Default + Serialize,
{
    fd: fs::File,
    batches: vec::IntoIter<Batch<S, T>>,
    entries: vec::IntoIter<DEntry<T>>,
}

impl<S, T> Iterator for BatchIter<S, T>
where
    S: Default + Serialize + DlogState<T>,
    T: Default + Serialize,
{
    type Item = Result<DEntry<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.entries.next() {
            None => match self.batches.next() {
                None => None,
                Some(batch) => {
                    let batch = match batch.into_active(&mut self.fd) {
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
