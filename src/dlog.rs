//! Module `dlog` implement write-ahead-logging for [Rdms] index.
//!
//! Takes care of batching write operations, serializing, appending
//! them to disk, and finally commiting the appended batch(es). A
//! single `Dlog` can be managed using ``n`` number of shards, where
//! each shard manages the log using a set of journal-files.
//!
//! **Shards**:
//!
//! Every shard is managed in a separate thread and each shard serializes
//! the log-operations, batches them if possible, flushes them and return
//! a index-sequence-no for each operation back to the caller. Basic idea
//! behind shard is to match with I/O concurrency available in modern SSDs.
//!
//! **Journals**:
//!
//! A shard of `Dlog` is organized into ascending list of journal files,
//! where each journal file do not exceed the configured size-limit.
//! Journal files are append only and flushed in batches when ever
//! possible. Journal files are purged once `Dlog` is notified about
//! durability guarantee uptill an index-sequence-no.
//!
//! A Typical WAL operation-cycles fall under one of the following catogaries:
//!
//! * Initial WAL cycle, when new WAL is created on disk.
//! * Reload WAL cycle, when opening an existing WAL on disk.
//! * Replay WAL cycle, when entries WAL needs to be replayed on DB.
//! * Purge WAL cycle, when an existing WAL needs to totally purged.
//!
//! **Initial WAL cycle**:
//!
//! ```compile_fail
//!                                        +--------------+
//!     Wal::create() -> spawn_writer() -> | purge_till() |
//!                                        |    close()   |
//!                                        +--------------+
//! ```
//!
//! **Reload WAL cycle**:
//!
//! ```compile_fail
//!                                      +--------------+
//!     Wal::load() -> spawn_writer() -> | purge_till() |
//!                                      |    close()   |
//!                                      +--------------+
//! ```
//!
//! **Replay WAL cycle**:
//!
//! ```compile_fail
//!     Wal::load() -> replay() -> close()
//! ```
//!
//! **Purge cycle**:
//!
//! ```compile_fail
//!     +---------------+
//!     | Wal::create() |
//!     |     or        | ---> Wal::purge()
//!     | Wal::load()   |
//!     +---------------+
//! ```
//!

use std::{
    borrow::Borrow,
    cmp,
    collections::HashMap,
    convert::TryInto,
    ffi, fs,
    io::{self, Read, Seek, Write},
    mem, path, result,
    sync::{atomic::AtomicU64, mpsc, Arc},
    thread, vec,
};

use crate::{
    core::{Diff, Replay, Result, Serialize},
    dlog_entry::{Entry, Op},
    {error::Error, util},
};

// TODO: review unwrap() and ok() and `as` conversion code.

pub(crate) trait DlogState<T> {
    type Key: Default + Serialize;
    type Val: Default + Serialize;
    type Op: Default + Serialize;

    fn on_add_entry(&mut self, entry: dlog_entry::Entry<Self::Op>) -> ();
}

// default size for flush buffer.
const FLUSH_SIZE: usize = 1 * 1024 * 1024;

// default limit for each journal file size.
const JOURNAL_LIMIT: usize = 1 * 1024 * 1024 * 1024;

// default block size while loading the WAl/Journal batches.
const WAL_BLOCK_SIZE: usize = 10 * 1024 * 1024;

pub trait DlogState<T> {
    type Key: Default + Serialize,
    type Vey: Default + Serialize,
    type T: Default + Serialize,

    fn on_add_entry(&mut self, entry: dlog_entry::Entry<Self::T>) -> ()
}

/// Write ahead logging for [Rdms] index.
///
/// Wal type is generic enough to be used outside this package. To know
/// more about write-ahead-logging and its use-cases refer to the [wal]
/// module documentation.
///
/// [wal]: crate::wal
/// [Rdms]: crate::Rdms
pub struct Wal<K, V>
where
    K: Serialize,
    V: Serialize,
{
    dir: ffi::OsString,
    name: String,
    index: Arc<Box<AtomicU64>>,
    shards: Vec<mpsc::Sender<OpRequest<K, V>>>,
    threads: Vec<thread::JoinHandle<Result<u64>>>,
    journals: Vec<Journal<K, V>>,
    // configuration
    journal_limit: usize,
}

impl<K, V> Drop for Wal<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn drop(&mut self) {
        if self.shards.len() > 0 || self.threads.len() > 0 {
            panic!("Try closing Wal `{}` with Wal::close()", self.name);
        }
    }
}

impl<K, V> Wal<K, V>
where
    K: Serialize,
    V: Serialize,
{
    /// Create a new [`Wal`] instance under directory ``dir``, using specified
    /// number of shards ``nshards`` and ``name`` must be unique if more than
    /// only [`Wal`] instances are going to be created under the same ``dir``.
    pub fn create(
        dir: ffi::OsString,
        name: String,
        nshards: usize, // number of shards
    ) -> Result<Wal<K, V>> {
        // purge existing journals for name.
        for item in fs::read_dir(&dir)? {
            let file_name = item?.file_name();
            match Journal::<K, V>::shallow_load(name.clone(), file_name)? {
                Some(journal) => journal.purge()?,
                None => (),
            }
        }
        // curate input parameters.
        if nshards == 0 {
            let msg = format!("invalid nshards: {}", nshards);
            return Err(Error::InvalidWAL(msg));
        }

        // create this WAL. later shards/journals can be added.
        Ok(Wal {
            dir,
            name,
            index: Arc::new(Box::new(AtomicU64::new(1))),
            shards: vec![],
            threads: Vec::with_capacity(nshards),
            journals: vec![],
            journal_limit: JOURNAL_LIMIT,
        })
    }

    /// Load an existing [`Wal`] instance identified by ``name`` under
    /// directory ``dir``.
    pub fn load(dir: ffi::OsString, name: String) -> Result<Wal<K, V>> {
        // gather all the journals.
        let mut shards: HashMap<usize, bool> = HashMap::new();
        let mut journals = vec![];
        let mut index = 0;
        for item in fs::read_dir(&dir)? {
            let file_path = {
                let mut file = path::PathBuf::new();
                file.push(dir.clone());
                file.push(item?.file_name());
                file.as_path().as_os_str().to_os_string()
            };
            // can this be a journal file ?
            if let Some(jrn) = Journal::load(name.clone(), file_path)? {
                match shards.get_mut(&jrn.shard_id()) {
                    Some(_) => (),
                    None => {
                        shards.insert(jrn.shard_id(), true);
                    }
                }
                index = cmp::max(index, jrn.to_last_index().unwrap_or(0));
                journals.push(jrn);
            }
        }
        // shards are monotonically increasing number from 1 to N
        let mut ss: Vec<usize> = shards.into_iter().map(|(k, _)| k).collect();
        ss.sort();
        for (i, id) in ss.iter().enumerate() {
            if i != id - 1 {
                let msg = format!("invalid shard at {}", i);
                return Err(Error::InvalidWAL(msg));
            }
        }

        Ok(Wal {
            dir,
            name,
            index: Arc::new(Box::new(AtomicU64::new(index + 1))),
            shards: vec![],
            threads: Vec::with_capacity(ss.len()),
            journals,
            journal_limit: JOURNAL_LIMIT,
        })
    }

    /// Set journal file limit to ``limit``, exceeding which, the current
    /// journal file shall be closed and made immutable. A new journal file
    /// will be added to the set of journal files and all new write
    /// operations shall be flushed to new journal file.
    pub fn set_journal_limit(&mut self, limit: usize) -> &mut Self {
        self.journal_limit = limit;
        self
    }
}

impl<K, V> Wal<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    fn is_active(&self) -> bool {
        (self.threads.len() + self.shards.len()) > 0
    }

    /// When DB suffer a crash and looses latest set of mutations, [`Wal`]
    /// can be used to fetch the latest set of mutations and replay them on
    /// DB. Return total number of operations replayed on DB.
    pub fn replay<W: Replay<K, V>>(mut self, db: &mut W) -> Result<usize> {
        // validate
        if self.is_active() {
            let msg = format!("cannot replay with active shards");
            return Err(Error::InvalidWAL(msg));
        }
        if self.journals.len() == 0 {
            return Ok(0);
        }
        // sort-merge journals from different shards.
        let journal = self.journals.remove(0);
        let mut iter = ReplayIter::new_journal(journal.into_iter()?);
        for journal in self.journals.drain(..) {
            let y = ReplayIter::new_journal(journal.into_iter()?);
            iter = ReplayIter::new_merge(iter, y);
        }
        let mut ops = 0;
        for entry in iter {
            let entry = entry?;
            let index = entry.to_index();
            match entry.into_op() {
                Op::Set { key, value } => {
                    db.set_index(key, value, index)?;
                }
                Op::SetCAS { key, value, cas } => {
                    db.set_cas_index(key, value, cas, index)?;
                }
                Op::Delete { key } => {
                    db.delete_index(key, index)?;
                }
            }
            ops += 1;
        }
        Ok(ops)
    }
}

impl<K, V> Wal<K, V>
where
    K: 'static + Send + Serialize,
    V: 'static + Send + Serialize,
{
    /// Spawn a new thread and return a [`Writer`] handle. Returned Writer
    /// can be shared with any number of threads to inject write operations
    /// into [`Wal`] instance. Also note that ``spawn_writer`` api can be
    /// called only for configured number of shards, ``nshards``.
    pub fn spawn_writer(&mut self) -> Result<Writer<K, V>> {
        if self.threads.len() < self.threads.capacity() {
            let (tx, rx) = mpsc::channel();

            let (dir, id) = (self.dir.clone(), self.threads.len() + 1);
            let name = self.name.clone();
            let index = Arc::clone(&self.index);
            let mut shard = Shard::<K, V>::new(dir, name, id, index);

            // remove journals for this shard.
            let journals: Vec<Journal<K, V>> = self
                .journals
                .drain_filter(|jrn| jrn.shard_id() == id)
                .collect();

            shard
                .add_journals(journals) // shall sort the journal order.
                .set_journal_limit(self.journal_limit);

            // check whether journals are in proper order
            if shard.journals.len() > 0 {
                let m = shard.journals.len() - 1;
                let zi = shard.journals[1..].iter();
                let iter = shard.journals[..m].iter().zip(zi);
                for (x, y) in iter {
                    let a = x.to_start_index().unwrap_or(0);
                    let b = x.to_last_index().unwrap_or(0);
                    let c = y.to_start_index().unwrap_or(0);
                    let d = y.to_last_index().unwrap_or(0);

                    let yes = a > 0 && b > 0 && a > b;
                    let yes = yes || c > 0 && d > 0 && c > d;
                    let yes = yes || b > 0 && c > 0 && (b + 1) > c;
                    if yes {
                        let msg = format!("journals/batches are un-ordered");
                        return Err(Error::InvalidWAL(msg));
                    };
                }
            }

            // spawn the shard
            self.threads.push(shard.spawn(rx)?);
            self.shards.push(tx.clone());

            Ok(Writer::new(tx))
        } else {
            Err(Error::InvalidWAL(format!("exceeding the shard limit")))
        }
    }

    /// Purge all journal files whose ``last_index`` is  less than ``before``.
    pub fn purge_till(&mut self, before: u64) -> Result<()> {
        if self.shards.len() != self.threads.capacity() {
            panic!("spawn_writers for all shards and try purge_till() API");
        }
        for shard_tx in self.shards.iter() {
            let (tx, rx) = mpsc::sync_channel(1);
            shard_tx.send(OpRequest::new_purge_till(before, tx))?;
            rx.recv()?;
        }
        Ok(())
    }

    /// Close the [`Wal`] instance. It is possible to get back the [`Wal`]
    /// instance using the [`Wal::load`] constructor. To purge the instance use
    /// [`Wal::purge`] api.
    pub fn close(&mut self) -> Result<u64> {
        // wait for the threads to exit, note that threads could have ended
        // when close() was called on WAL or Writer, or due panic or error.
        while let Some(tx) = self.shards.pop() {
            // ignore if send returns an error
            // TODO: log error here.
            tx.send(OpRequest::new_close()).ok();
        }
        // wait for the threads to exit.
        let mut index = 0_u64;
        while let Some(thread) = self.threads.pop() {
            index = cmp::max(index, thread.join()??);
        }
        Ok(index)
    }

    /// Purge this ``Wal`` instance and all its memory and disk footprints.
    pub fn purge(mut self) -> Result<()> {
        self.close()?;
        if self.threads.len() > 0 {
            let msg = "cannot purge with active shards".to_string();
            Err(Error::InvalidWAL(msg))
        } else {
            while let Some(journal) = self.journals.pop() {
                journal.purge()?;
            }
            Ok(())
        }
    }
}

enum ReplayIter<K, V>
where
    K: Serialize,
    V: Serialize,
{
    JournalIter {
        iter: BatchIter<K, V>,
    },
    MergeIter {
        x: Box<ReplayIter<K, V>>,
        y: Box<ReplayIter<K, V>>,
        x_entry: Option<Result<Entry<K, V>>>,
        y_entry: Option<Result<Entry<K, V>>>,
    },
}

impl<K, V> ReplayIter<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn new_journal(iter: BatchIter<K, V>) -> ReplayIter<K, V> {
        ReplayIter::JournalIter { iter }
    }

    fn new_merge(
        mut x: ReplayIter<K, V>, // journal iterator
        mut y: ReplayIter<K, V>, // journal iterator
    ) -> ReplayIter<K, V> {
        let x_entry = x.next();
        let y_entry = y.next();
        ReplayIter::MergeIter {
            x: Box::new(x),
            y: Box::new(y),
            x_entry,
            y_entry,
        }
    }
}

impl<K, V> Iterator for ReplayIter<K, V>
where
    K: Serialize,
    V: Serialize,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ReplayIter::JournalIter { iter } => iter.next(),
            ReplayIter::MergeIter {
                x,
                y,
                x_entry,
                y_entry,
            } => match (x_entry.take(), y_entry.take()) {
                (Some(Ok(xe)), Some(Ok(ye))) => {
                    let c = xe.to_index().cmp(&ye.to_index());
                    match c {
                        cmp::Ordering::Less => {
                            *x_entry = x.next();
                            *y_entry = Some(Ok(ye));
                            Some(Ok(xe))
                        }
                        cmp::Ordering::Greater => {
                            *y_entry = y.next();
                            *x_entry = Some(Ok(xe));
                            Some(Ok(ye))
                        }
                        cmp::Ordering::Equal => unreachable!(),
                    }
                }
                (Some(Ok(xe)), None) => {
                    *x_entry = x.next();
                    Some(Ok(xe))
                }
                (None, Some(Ok(ye))) => {
                    *y_entry = y.next();
                    Some(Ok(ye))
                }
                (_, Some(Err(err))) => Some(Err(err)),
                (Some(Err(err)), _) => Some(Err(err)),
                _ => None,
            },
        }
    }
}

/// Writer handle for [`Wal`] instance.
///
/// There can be a maximum of ``nshard`` number of Writer handles and each
/// writer handle can be shared across any number of threads.
pub struct Writer<K, V>
where
    K: Serialize,
    V: Serialize,
{
    tx: mpsc::Sender<OpRequest<K, V>>,
}

impl<K, V> Writer<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn new(tx: mpsc::Sender<OpRequest<K, V>>) -> Writer<K, V> {
        Writer { tx }
    }

    /// Append ``set`` operation into the log. Return the sequence-no
    /// for this mutation.
    pub fn set(&self, key: K, value: V) -> Result<u64> {
        let (resp_tx, resp_rx) = mpsc::sync_channel(1);
        self.tx.send(OpRequest::new_set(key, value, resp_tx))?;
        match resp_rx.recv()? {
            Opresp::Result(res) => res,
        }
    }

    /// Append ``set_cas`` operation into the log. Return the sequence-no
    /// for this mutation.
    pub fn set_cas(&self, key: K, value: V, cas: u64) -> Result<u64> {
        let (resp_tx, resp_rx) = mpsc::sync_channel(1);
        self.tx
            .send(OpRequest::new_set_cas(key, value, cas, resp_tx))?;
        match resp_rx.recv()? {
            Opresp::Result(res) => res,
        }
    }

    /// Append ``delete`` operation into the log. Return the sequence-no
    /// for this mutation.
    pub fn delete<Q>(&self, key: &Q) -> Result<u64>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + ?Sized,
    {
        let (resp_tx, resp_rx) = mpsc::sync_channel(1);
        self.tx
            .send(OpRequest::new_delete(key.to_owned(), resp_tx))?;
        match resp_rx.recv()? {
            Opresp::Result(res) => res,
        }
    }
}

// shards are monotonically increasing number from 1 to N
struct Shard<K, V>
where
    K: Serialize,
    V: Serialize,
{
    dir: ffi::OsString,
    name: String,
    id: usize,
    wal_index: Arc<Box<AtomicU64>>,
    journals: Vec<Journal<K, V>>,
    active: Option<Journal<K, V>>,
    journal_limit: usize,
}

impl<K, V> Shard<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn new(
        dir: ffi::OsString, // wal directory
        name: String,
        id: usize,
        index: Arc<Box<AtomicU64>>,
    ) -> Shard<K, V> {
        Shard {
            dir,
            name,
            id,
            wal_index: index,
            journals: vec![],
            active: None,
            journal_limit: JOURNAL_LIMIT,
        }
    }

    fn add_journals(&mut self, mut journals: Vec<Journal<K, V>>) -> &mut Self {
        journals.sort_by_key(|journal| journal.num);
        self.journals = journals;
        self
    }

    fn set_journal_limit(&mut self, limit: usize) -> &mut Self {
        self.journal_limit = limit;
        self
    }

    fn next_journal_num(&self, start: usize) -> usize {
        self.journals.last().map(|jrn| jrn.num + 1).unwrap_or(start)
    }
}

fn thread_shard<K, V>(
    mut shard: Shard<K, V>,
    rx: mpsc::Receiver<OpRequest<K, V>>, // shard commands
) -> Result<u64>
where
    K: 'static + Send + Serialize,
    V: 'static + Send + Serialize,
{
    let mut index = 0_u64;
    let mut cmds = vec![];
    loop {
        let res = Shard::receive_cmds(&rx, &mut cmds);
        match shard.do_cmds(&mut index, cmds) {
            Ok(false) => (),
            Ok(true) => break Ok(index),
            Err(err) => break Err(err),
        }
        cmds = vec![];
        match res {
            Err(mpsc::TryRecvError::Empty) => match rx.recv() {
                Ok(cmd) => cmds.push(cmd),
                Err(_) => break Ok(index),
            },
            Err(mpsc::TryRecvError::Disconnected) => break Ok(index),
            _ => unreachable!(),
        }
    }
}

impl<K, V> Shard<K, V>
where
    K: 'static + Send + Serialize,
    V: 'static + Send + Serialize,
{
    fn spawn(
        mut self,
        rx: mpsc::Receiver<OpRequest<K, V>>, // spawn thread to handle rx-msgs
    ) -> Result<thread::JoinHandle<Result<u64>>> {
        let start = 1;
        let (name, num) = (self.name.clone(), self.next_journal_num(start));
        let (id, dir) = (self.id, self.dir.clone());
        self.active = Some(Journal::create(dir, name, id, num)?);
        Ok(thread::spawn(move || thread_shard(self, rx)))
    }

    fn receive_cmds(
        rx: &mpsc::Receiver<OpRequest<K, V>>,
        cmds: &mut Vec<OpRequest<K, V>>,
    ) -> result::Result<(), mpsc::TryRecvError> {
        loop {
            match rx.try_recv() {
                Ok(cmd) => cmds.push(cmd),
                Err(err) => break Err(err),
            }
        }
    }

    // return true if main loop should exit.
    fn do_cmds(
        &mut self,
        index: &mut u64,
        cmds: Vec<OpRequest<K, V>>, // gather a batch of commands/entries
    ) -> Result<bool> {
        use std::sync::atomic::Ordering::AcqRel;

        for cmd in cmds {
            match cmd {
                OpRequest::Close => {
                    return Ok(true);
                }
                OpRequest::PurgeTill { before, caller } => {
                    match self.handle_purge_till(before) {
                        ok @ Ok(_) => caller.send(Opresp::new_result(ok)).ok(),
                        Err(e) => {
                            let s = format!("purge-before {}: {:?}", before, e);
                            caller.send(Opresp::new_result(Err(e))).ok();
                            return Err(Error::InvalidWAL(s));
                        }
                    };
                }
                cmd => {
                    *index = self.wal_index.fetch_add(1, AcqRel);
                    self.active.as_mut().unwrap().append_op(*index, cmd)?;
                }
            }
        }
        match self.active.as_mut().unwrap().flush1(self.journal_limit)? {
            None => (),
            Some((buffer, batch)) => {
                self.rotate_journal()?;
                self.active.as_mut().unwrap().flush2(&buffer, batch)?;
            }
        }
        Ok(false)
    }

    fn rotate_journal(&mut self) -> Result<()> {
        // forget the old active.
        let mut active = self.active.take().unwrap();
        active.freeze();
        self.journals.push(active);

        // new journal file.
        let j = {
            let name = self.name.clone();
            let num = self.next_journal_num(1 /*start*/);
            let (id, dir) = (self.id, self.dir.clone());
            Journal::create(dir, name, id, num)?
        };

        self.active = Some(j);
        Ok(())
    }

    // return index or io::Error.
    fn handle_purge_till(&mut self, before: u64) -> Result<u64> {
        let jrns: Vec<usize> = self
            .journals
            .iter()
            .enumerate()
            .filter_map(|(i, jrn)| match jrn.to_last_index() {
                Some(last_index) if last_index < before => Some(i),
                _ => None,
            })
            .collect();
        for i in jrns.into_iter() {
            self.journals.remove(i).purge()?;
        }
        Ok(before)
    }
}

enum OpRequest<K, V>
where
    K: Serialize,
    V: Serialize,
{
    Set {
        key: K,
        value: V,
        caller: mpsc::SyncSender<Opresp>,
    },
    SetCAS {
        key: K,
        value: V,
        cas: u64,
        caller: mpsc::SyncSender<Opresp>,
    },
    Delete {
        key: K,
        caller: mpsc::SyncSender<Opresp>,
    },
    PurgeTill {
        before: u64,
        caller: mpsc::SyncSender<Opresp>,
    },
    Close,
}

impl<K, V> OpRequest<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn new_set(
        key: K,
        value: V,
        caller: mpsc::SyncSender<Opresp>, // response channel
    ) -> OpRequest<K, V> {
        OpRequest::Set { key, value, caller }
    }

    fn new_set_cas(
        key: K,
        value: V,
        cas: u64,
        caller: mpsc::SyncSender<Opresp>, // response channel
    ) -> OpRequest<K, V> {
        OpRequest::SetCAS {
            key,
            value,
            cas,
            caller,
        }
    }

    fn new_delete(
        key: K,
        caller: mpsc::SyncSender<Opresp>, // reponse channel
    ) -> OpRequest<K, V> {
        OpRequest::Delete { key, caller }
    }

    fn new_purge_till(
        before: u64,                      // purge all entries with seqno <= u64
        caller: mpsc::SyncSender<Opresp>, // response channel
    ) -> OpRequest<K, V> {
        OpRequest::PurgeTill { before, caller }
    }

    fn new_close() -> OpRequest<K, V> {
        OpRequest::Close
    }
}

enum Opresp {
    Result(Result<u64>),
}

impl PartialEq for Opresp {
    fn eq(&self, other: &Opresp) -> bool {
        match (self, other) {
            (Opresp::Result(Ok(x)), Opresp::Result(Ok(y))) => x == y,
            _ => false,
        }
    }
}

impl Opresp {
    fn new_result(res: Result<u64>) -> Opresp {
        Opresp::Result(res)
    }
}

struct Journal<K, V>
where
    K: Serialize,
    V: Serialize,
{
    shard_id: usize,
    num: usize,               // starts from 1
    file_path: ffi::OsString, // {name}-shard-{shard_id}-journal-{num}.log
    fd: Option<fs::File>,
    batches: Vec<Batch<K, V>>, // batches sorted by index-seqno.
    active: Batch<K, V>,
}

impl<K, V> Journal<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn parts_to_file_name(name: &str, shard_id: usize, num: usize) -> String {
        format!("{}-wal-shard-{}-journal-{}.wal", name, shard_id, num)
    }

    fn file_name_to_parts(
        file_path: &ffi::OsString, // directory path and file-name
    ) -> Option<(String, usize, usize)> {
        let fname = path::Path::new(&file_path).file_name()?;
        let fname: &path::Path = fname.as_ref();
        let fname = fname.file_stem()?.to_os_string().into_string().ok()?;
        let mut iter = fname.split('-');

        let name = iter.next()?.to_string();
        let wal_name = iter.next()?.to_string();
        let shard = iter.next()?;
        let shard_id = iter.next()?;
        let journal = iter.next()?;
        let num = iter.next()?;
        if shard != "shard" || wal_name != "wal" || journal != "journal" {
            None
        } else {
            Some((name, shard_id.parse().ok()?, num.parse().ok()?))
        }
    }

    fn create(
        dir: ffi::OsString,
        name: String,
        shard_id: usize,
        num: usize,
    ) -> Result<Journal<K, V>> {
        let file = Self::parts_to_file_name(&name, shard_id, num);
        let mut file_path = path::PathBuf::new();
        file_path.push(&dir);
        file_path.push(&file);

        fs::remove_file(&file_path).ok(); // cleanup a single journal file

        let file_path: &ffi::OsStr = file_path.as_ref();
        let file_path = file_path.to_os_string();
        Ok(Journal {
            shard_id,
            num,
            file_path: file_path.clone(),
            fd: Some({
                let mut opts = fs::OpenOptions::new();
                opts.append(true).create_new(true).open(&file_path)?
            }),
            batches: Default::default(),
            active: Batch::new(),
        })
    }

    fn load(
        name: String,
        file_path: ffi::OsString, // directory path and file
    ) -> Result<Option<Journal<K, V>>> {
        // load batches are reference to file.
        let batches = {
            let mut batches = vec![];
            let mut fd = util::open_file_r(&file_path)?;

            let (mut fpos, till) = (0_u64, fd.metadata()?.len());
            while fpos < till {
                let block = {
                    let mut block = Vec::with_capacity(WAL_BLOCK_SIZE);
                    block.resize(block.capacity(), Default::default());
                    fd.seek(io::SeekFrom::Start(fpos))?;
                    let n = fd.read(&mut block)?;
                    if n < block.len() && (fpos + (n as u64)) < till {
                        let msg = format!("journal block at {}", fpos);
                        return Err(Error::PartialRead(msg));
                    }
                    block.truncate(n);
                    block
                };

                let mut m = 0_usize;
                while m < block.len() {
                    let mut batch: Batch<K, V> = unsafe { mem::zeroed() };
                    m += batch.decode_refer(&block[m..], fpos + (m as u64))?;
                    batches.push(batch);
                }
                fpos += block.len() as u64;
            }
            batches
        };

        match Self::file_name_to_parts(&file_path) {
            Some((nm, shard_id, num)) if nm == name => Ok(Some(Journal {
                shard_id,
                num,
                file_path: file_path.clone(),
                fd: Default::default(),
                batches,
                active: Batch::new(),
            })),
            _ => Ok(None),
        }
    }

    #[cfg(test)]
    fn open(&mut self) -> Result<()> {
        self.fd = Some({
            let mut opts = fs::OpenOptions::new();
            opts.read(true).write(false).open(&self.file_path)?
        });
        Ok(())
    }

    // don't load the batches. use this only for purging the journal.
    fn shallow_load(
        name: String,
        file_path: ffi::OsString, // full path
    ) -> Result<Option<Journal<K, V>>> {
        match Self::file_name_to_parts(&file_path) {
            Some((nm, shard_id, num)) if nm == name => Ok(Some(Journal {
                shard_id,
                num,
                file_path: file_path,
                fd: Default::default(),
                batches: Default::default(),
                active: Batch::new(),
            })),
            _ => Ok(None),
        }
    }
}

impl<K, V> Journal<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn shard_id(&self) -> usize {
        self.shard_id
    }

    fn to_start_index(&self) -> Option<u64> {
        self.batches.first()?.to_start_index()
    }

    fn to_last_index(&self) -> Option<u64> {
        self.batches.last()?.to_last_index()
    }

    fn to_current_term(&self) -> u64 {
        self.active.to_current_term()
    }

    fn exceed_limit(&self, journal_limit: usize) -> Result<bool> {
        let limit: u64 = journal_limit.try_into()?;
        Ok(self.fd.as_ref().unwrap().metadata()?.len() > limit)
    }

    fn into_iter(mut self) -> Result<BatchIter<K, V>> {
        self.fd.take();
        Ok(BatchIter {
            fd: {
                let mut opts = fs::OpenOptions::new();
                opts.read(true).write(false).open(self.file_path)?
            },
            batches: self.batches.into_iter(),
            entries: vec![].into_iter(),
        })
    }

    fn freeze(&mut self) {
        self.fd.take();
    }

    fn purge(self) -> Result<()> {
        fs::remove_file(&self.file_path)?;
        Ok(())
    }
}

impl<K, V> Journal<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn append_op(&mut self, index: u64, cmd: OpRequest<K, V>) -> Result<()> {
        match cmd {
            OpRequest::Set { key, value, caller } => {
                self.append_set(index, key, value);
                caller.send(Opresp::new_result(Ok(index)))?;
            }
            OpRequest::SetCAS {
                key,
                value,
                cas,
                caller,
            } => {
                self.append_set_cas(index, key, value, cas);
                caller.send(Opresp::new_result(Ok(index)))?;
            }
            OpRequest::Delete { key, caller } => {
                self.append_delete(index, key);
                caller.send(Opresp::new_result(Ok(index)))?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn append_set(&mut self, index: u64, key: K, value: V) {
        let op = Op::new_set(key, value);
        let entry = Entry::new_term(op, self.to_current_term(), index);
        self.active.add_entry(entry);
    }

    fn append_set_cas(&mut self, index: u64, key: K, value: V, cas: u64) {
        let op = Op::new_set_cas(key, value, cas);
        let entry = Entry::new_term(op, self.to_current_term(), index);
        self.active.add_entry(entry);
    }

    fn append_delete(&mut self, index: u64, key: K) {
        let op = Op::new_delete(key);
        let entry = Entry::new_term(op, self.to_current_term(), index);
        self.active.add_entry(entry);
    }

    fn flush1(&mut self, lmt: usize) -> Result<Option<(Vec<u8>, Batch<K, V>)>> {
        let mut buffer = Vec::with_capacity(FLUSH_SIZE);
        let want = self.active.encode_active(&mut buffer)?;

        match self.exceed_limit(lmt - want) {
            Ok(true) if self.active.len() > 0 => {
                // rotate journal files.
                let a = self.active.to_start_index().unwrap();
                let z = self.active.to_last_index().unwrap();
                let batch = Batch::new_refer(0, want, a, z);
                Ok(Some((buffer, batch)))
            }
            Ok(false) if self.active.len() > 0 => {
                let fd = self.fd.as_mut().unwrap();
                let fpos = fd.metadata()?.len();
                let n = fd.write(&buffer)?;
                if want != n {
                    let f = self.file_path.clone();
                    let msg = format!("wal-flush: {:?}, {}/{}", f, want, n);
                    Err(Error::PartialWrite(msg))
                } else {
                    fd.sync_all()?; // TODO: <- disk bottle-neck

                    let a = self.active.to_start_index().unwrap();
                    let z = self.active.to_last_index().unwrap();
                    let batch = Batch::new_refer(fpos, want, a, z);
                    self.batches.push(batch);
                    self.active = Batch::new();
                    Ok(None)
                }
            }
            Err(err) => Err(err),
            _ => Ok(None),
        }
    }

    fn flush2(&mut self, buffer: &[u8], mut batch: Batch<K, V>) -> Result<()> {
        let length = buffer.len();
        let fd = self.fd.as_mut().unwrap();
        let fpos = fd.metadata()?.len();
        let n = fd.write(&buffer)?;
        if length == n {
            fd.sync_all()?; // TODO: <- disk bottle-neck

            let a = batch.to_start_index().unwrap();
            let z = batch.to_last_index().unwrap();
            batch = Batch::new_refer(fpos, length, a, z);
            self.batches.push(batch);
            self.active = Batch::new();
            Ok(())
        } else {
            let f = self.file_path.clone();
            let msg = format!("wal-flush: {:?}, {}/{}", f, length, n);
            Err(Error::PartialWrite(msg))
        }
    }
}

struct BatchIter<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fd: fs::File,
    batches: vec::IntoIter<Batch<K, V>>,
    entries: vec::IntoIter<Entry<K, V>>,
}

impl<K, V> Iterator for BatchIter<K, V>
where
    K: Serialize,
    V: Serialize,
{
    type Item = Result<Entry<K, V>>;

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

#[cfg(test)]
#[path = "dlog_test.rs"]
mod dlog_test;
