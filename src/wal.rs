//! Write-Ahead-Logging for Rdms index.
//!
//! Takes care of batching write operations, serializing, appending
//! them to disk, and finally commiting the appended batch(es). A
//! single WAL can be managed using ``n`` number of shards, where
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
//! A shard of WAL is organized into ascending list of journal files,
//! where each journal file do not exceed the configured size-limit.
//! Journal files are append only and flushed in batches when ever
//! possible. Journal files are purged once WAL is notified about
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
use std::convert::TryInto;
use std::sync::atomic::AtomicU64;
use std::{
    borrow::Borrow,
    cmp,
    collections::HashMap,
    ffi, fmt, fs,
    io::{self, Read, Seek, Write},
    mem, path, result,
    sync::{mpsc, Arc},
    thread, vec,
};

use lazy_static::lazy_static;

use crate::core::{Diff, Replay, Result, Serialize};
use crate::{error::Error, util};

include!("wal_marker.rs");

// default node name.
const DEFAULT_NODE: &'static str = "no-consensus";

// default size for flush buffer.
const FLUSH_SIZE: usize = 1 * 1024 * 1024;

// default limit for each journal file size.
const JOURNAL_LIMIT: usize = 1 * 1024 * 1024 * 1024;

// term value when not using consensus
const NIL_TERM: u64 = 0;

// default block size while loading the WAl/Journal batches.
const WAL_BLOCK_SIZE: usize = 10 * 1024 * 1024;

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
    K: Send + Serialize,
    V: Send + Serialize,
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
    K: Send + Serialize,
    V: Send + Serialize,
{
    fn drop(&mut self) {
        if self.shards.len() > 0 || self.threads.len() > 0 {
            panic!("Try closing Wal `{}` with Wal::close()", self.name);
        }
    }
}

impl<K, V> Wal<K, V>
where
    K: Send + Serialize,
    V: Send + Serialize,
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
    K: Clone + Ord + Send + Serialize,
    V: Clone + Diff + Send + Serialize,
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
                        cmp::Ordering::Equal => unreachable!(),
                        cmp::Ordering::Greater => {
                            *y_entry = y.next();
                            *x_entry = Some(Ok(xe));
                            Some(Ok(ye))
                        }
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
    K: Send + Serialize,
    V: Send + Serialize,
{
    tx: mpsc::Sender<OpRequest<K, V>>,
}

impl<K, V> Writer<K, V>
where
    K: Send + Serialize,
    V: Send + Serialize,
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
        let name = self.name.clone();
        let num = self.next_journal_num(1 /*start*/);
        let (id, dir) = (self.id, self.dir.clone());
        let j = Journal::create(dir, name, id, num)?;
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
    K: Send + Serialize,
    V: Send + Serialize,
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
    K: Send + Serialize,
    V: Send + Serialize,
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
        let limit: u64 = journal_limit.try_into().unwrap();
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
    K: Send + Serialize,
    V: Send + Serialize,
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
        let want = self.active.encode_active(&mut buffer);

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

#[derive(Clone)]
enum Batch<K, V>
where
    K: Serialize,
    V: Serialize,
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
    K: Serialize,
    V: Serialize,
{
    fn new() -> Batch<K, V> {
        Batch::Active {
            config: Default::default(),
            term: NIL_TERM,
            committed: Default::default(),
            persisted: Default::default(),
            votedfor: DEFAULT_NODE.to_string(),
            entries: vec![],
        }
    }

    fn new_refer(fpos: u64, length: usize, a: u64, z: u64) -> Batch<K, V> {
        Batch::Refer {
            fpos,
            length,
            start_index: a,
            last_index: z,
        }
    }

    #[allow(dead_code)] // TODO: remove this once consensus in integrated.
    fn set_config(&mut self, cnfg: &Vec<String>) -> &mut Batch<K, V> {
        match self {
            Batch::Active { config, .. } => {
                config.truncate(0);
                config.extend_from_slice(cnfg);
            }
            _ => unreachable!(),
        }
        self
    }

    #[allow(dead_code)] // TODO: remove this once consensus in integrated.
    fn set_term(&mut self, trm: u64, voted_for: String) -> &mut Batch<K, V> {
        match self {
            Batch::Active { term, votedfor, .. } => {
                *term = trm;
                *votedfor = voted_for;
            }
            _ => unreachable!(),
        }
        self
    }

    #[allow(dead_code)] // TODO: remove this once consensus in integrated.
    fn set_committed(&mut self, index: u64) -> &mut Batch<K, V> {
        match self {
            Batch::Active { committed, .. } => *committed = index,
            _ => unreachable!(),
        }
        self
    }

    #[allow(dead_code)] // TODO: remove this once consensus in integrated.
    fn set_persisted(&mut self, index: u64) -> &mut Batch<K, V> {
        match self {
            Batch::Active { persisted, .. } => *persisted = index,
            _ => unreachable!(),
        }
        self
    }

    fn add_entry(&mut self, entry: Entry<K, V>) {
        match self {
            Batch::Active { entries, .. } => entries.push(entry),
            _ => unreachable!(),
        }
    }
}

impl<K, V> Batch<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn to_start_index(&self) -> Option<u64> {
        match self {
            Batch::Refer { start_index, .. } => Some(*start_index),
            Batch::Active { entries, .. } => {
                let index = entries.first().map(|entry| entry.to_index());
                index
            }
        }
    }

    fn to_last_index(&self) -> Option<u64> {
        match self {
            Batch::Refer { last_index, .. } => Some(*last_index),
            Batch::Active { entries, .. } => {
                let index = entries.last().map(|entry| entry.to_index());
                index
            }
        }
    }

    fn to_current_term(&self) -> u64 {
        match self {
            Batch::Active { term, .. } => *term,
            _ => unreachable!(),
        }
    }

    fn len(&self) -> usize {
        match self {
            Batch::Active { entries, .. } => entries.len(),
            _ => unreachable!(),
        }
    }

    fn into_entries(self) -> Vec<Entry<K, V>> {
        match self {
            Batch::Active { entries, .. } => entries,
            Batch::Refer { .. } => unreachable!(),
        }
    }

    fn into_active(self, fd: &mut fs::File) -> Result<Batch<K, V>> {
        match self {
            Batch::Refer { fpos, length, .. } => {
                let n: u64 = length.try_into().unwrap();
                let buf = util::read_buffer(fd, fpos, n, "fetching batch")?;
                let mut batch: Batch<K, V> = unsafe { mem::zeroed() };
                batch.decode_active(&buf)?;
                Ok(batch)
            }
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
// |                           last_index                           |
// +----------------------------------------------------------------+
// |                            n-entries                           |
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
// NOTE: `length` value includes 8-byte length-prefix and 8-byte length-suffix.
impl<K, V> Batch<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn encode_active(&self, buf: &mut Vec<u8>) -> usize {
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
                let sindex = entries.first().map(|e| e.to_index()).unwrap_or(0);
                buf.extend_from_slice(&sindex.to_be_bytes());
                let lindex = entries.last().map(|e| e.to_index()).unwrap_or(0);
                buf.extend_from_slice(&lindex.to_be_bytes());
                let nentries: u64 = entries.len().try_into().unwrap();
                buf.extend_from_slice(&nentries.to_be_bytes());

                let mut m = Self::encode_config(config, buf);
                m += Self::encode_votedfor(votedfor, buf);

                m += entries.iter().map(|e| e.encode(buf)).sum::<usize>();

                buf.extend_from_slice(BATCH_MARKER.as_ref());

                let n = 56 + m + BATCH_MARKER.len() + 8;
                let length: u64 = n.try_into().unwrap();
                buf[..8].copy_from_slice(&length.to_be_bytes());
                buf.extend_from_slice(&length.to_be_bytes());

                n
            }
            _ => unreachable!(),
        }
    }

    fn decode_refer(&mut self, buf: &[u8], fpos: u64) -> Result<usize> {
        util::check_remaining(buf, 56, "batch-refer-hdr")?;
        let length = Self::validate(buf)?;
        let start_index = u64::from_be_bytes(buf[32..40].try_into().unwrap());
        let last_index = u64::from_be_bytes(buf[40..48].try_into().unwrap());
        *self = Batch::Refer {
            fpos,
            length,
            start_index,
            last_index,
        };
        Ok(length)
    }

    fn decode_active(&mut self, buf: &[u8]) -> Result<usize> {
        util::check_remaining(buf, 48, "batch-active-hdr")?;
        let length = Self::validate(buf)?;
        let term = u64::from_be_bytes(buf[8..16].try_into().unwrap());
        let committed = u64::from_be_bytes(buf[16..24].try_into().unwrap());
        let persisted = u64::from_be_bytes(buf[24..32].try_into().unwrap());
        let _start_index = u64::from_be_bytes(buf[32..40].try_into().unwrap());
        let _last_index = u64::from_be_bytes(buf[40..48].try_into().unwrap());
        let nentries = u64::from_be_bytes(buf[48..56].try_into().unwrap());
        let mut n = 56;

        let (config, m) = Self::decode_config(&buf[n..])?;
        n += m;
        let (votedfor, m) = Self::decode_votedfor(&buf[n..])?;
        n += m;

        let entries = {
            let mut entries = Vec::with_capacity(nentries.try_into().unwrap());
            for _i in 0..entries.capacity() {
                let mut entry: Entry<K, V> = unsafe { mem::zeroed() };
                n += entry.decode(&buf[n..])?;
                entries.push(entry);
            }
            entries
        };

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
    K: Serialize,
    V: Serialize,
{
    fn encode_config(config: &Vec<String>, buf: &mut Vec<u8>) -> usize {
        let count: u16 = config.len().try_into().unwrap();
        buf.extend_from_slice(&count.to_be_bytes());
        let mut n = mem::size_of_val(&count);

        for c in config {
            let len: u16 = c.as_bytes().len().try_into().unwrap();
            buf.extend_from_slice(&len.to_be_bytes());
            buf.extend_from_slice(c.as_bytes());
            n += mem::size_of_val(&len) + c.as_bytes().len();
        }
        n
    }

    fn decode_config(buf: &[u8]) -> Result<(Vec<String>, usize)> {
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

    fn encode_votedfor(s: &str, buf: &mut Vec<u8>) -> usize {
        let len: u16 = s.as_bytes().len().try_into().unwrap();
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(s.as_bytes());
        mem::size_of_val(&len) + s.as_bytes().len()
    }

    fn decode_votedfor(buf: &[u8]) -> Result<(String, usize)> {
        util::check_remaining(buf, 2, "batch-votedfor")?;
        let len = u16::from_be_bytes(buf[..2].try_into().unwrap());
        let n = 2;

        let len: usize = len.try_into().unwrap();
        util::check_remaining(buf, n + len, "batch-votedfor")?;
        Ok((std::str::from_utf8(&buf[n..n + len])?.to_string(), n + len))
    }

    fn validate(buf: &[u8]) -> Result<usize> {
        let (a, z): (usize, usize) = {
            let n = u64::from_be_bytes(buf[..8].try_into().unwrap())
                .try_into()
                .unwrap();
            (
                n,
                u64::from_be_bytes(buf[n - 8..n].try_into().unwrap())
                    .try_into()
                    .unwrap(),
            )
        };
        if a != z {
            let msg = format!("batch length mismatch, {} {}", a, z);
            return Err(Error::InvalidWAL(msg));
        }

        let (m, n) = (a - 8 - BATCH_MARKER.len(), a - 8);
        if BATCH_MARKER.as_slice() != &buf[m..n] {
            let msg = format!("batch-marker {:?}", &buf[m..n]);
            return Err(Error::InvalidWAL(msg));
        }
        Ok(a)
    }
}

/************************ Entry ***********************/

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
enum Entry<K, V>
where
    K: Serialize,
    V: Serialize,
{
    Term {
        // Term in which the entry is created.
        term: u64,
        // Index seqno for this entry. This will be monotonically
        // increasing number.
        index: u64,
        // Operation on host data structure.
        op: Op<K, V>,
    },
    Client {
        // Term in which the entry is created.
        term: u64,
        // Index seqno for this entry. This will be monotonically
        // increasing number.
        index: u64,
        // Id of client applying this entry. To deal with false negatives.
        id: u64,
        // Client seqno monotonically increased by client. To deal
        // with false negatives.
        ceqno: u64,
        // Operation on host data structure.
        op: Op<K, V>,
    },
}

impl<K, V> PartialEq for Entry<K, V>
where
    K: PartialEq + Serialize,
    V: PartialEq + Serialize,
{
    fn eq(&self, other: &Entry<K, V>) -> bool {
        match (self, other) {
            (
                Entry::Term {
                    term: t1,
                    index: i1,
                    op: op1,
                },
                Entry::Term {
                    term: t2,
                    index: i2,
                    op: op2,
                },
            ) => t1 == t2 && i1 == i2 && op1.eq(&op2),
            (
                Entry::Client {
                    term: t1,
                    index: i1,
                    id: id1,
                    ceqno: n1,
                    op: op1,
                },
                Entry::Client {
                    term: t2,
                    index: i2,
                    id: id2,
                    ceqno: n2,
                    op: op2,
                },
            ) => t1 == t2 && i1 == i2 && id1 == id2 && n1 == n2 && op1.eq(&op2),
            _ => false,
        }
    }
}

impl<K, V> fmt::Debug for Entry<K, V>
where
    K: Serialize + fmt::Debug,
    V: Serialize + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            Entry::Term { term, index, op } => write!(
                f,
                "Entry::Term<term: {} index: {}  op: {:?}>",
                term, index, op
            ),
            Entry::Client {
                term,
                index,
                id,
                ceqno,
                op,
            } => write!(
                f,
                "Entry::Term<term: {} index: {}  id: {} ceqno: {} op: {:?}>",
                term, index, id, ceqno, op
            ),
        }
    }
}

impl<K, V> Entry<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn new_term(op: Op<K, V>, term: u64, index: u64) -> Entry<K, V> {
        Entry::Term { op, term, index }
    }

    fn new_client(
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

    fn entry_type(buf: &[u8]) -> Result<EntryType> {
        util::check_remaining(buf, 8, "entry-type")?;
        let hdr1 = u64::from_be_bytes(buf[..8].try_into().unwrap());
        Ok((hdr1 & 0x00000000000000FF).into())
    }

    fn to_index(&self) -> u64 {
        match self {
            Entry::Term { index, .. } => *index,
            Entry::Client { index, .. } => *index,
        }
    }

    fn into_op(self) -> Op<K, V> {
        match self {
            Entry::Term { op, .. } => op,
            Entry::Client { op, .. } => op,
        }
    }
}

impl<K, V> Serialize for Entry<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        match self {
            Entry::Term { op, term, index } => {
                let n = Self::encode_term(op, *term, *index, buf);
                n
            }
            Entry::Client {
                op,
                term,
                index,
                id,
                ceqno,
            } => {
                let n = Self::encode_client(op, *term, *index, *id, *ceqno, buf);
                n
            }
        }
    }

    fn decode(&mut self, buf: &[u8]) -> Result<usize> {
        *self = match Self::entry_type(buf)? {
            EntryType::Term => {
                let op: Op<K, V> = unsafe { mem::zeroed() };
                let term: u64 = unsafe { mem::zeroed() };
                let index: u64 = unsafe { mem::zeroed() };
                Self::new_term(op, term, index)
            }
            EntryType::Client => {
                let op: Op<K, V> = unsafe { mem::zeroed() };
                let term: u64 = unsafe { mem::zeroed() };
                let index: u64 = unsafe { mem::zeroed() };
                let id: u64 = unsafe { mem::zeroed() };
                let ceqno: u64 = unsafe { mem::zeroed() };
                Self::new_client(op, term, index, id, ceqno)
            }
        };

        match self {
            Entry::Term { term, index, op } => {
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
    K: Serialize,
    V: Serialize,
{
    fn encode_term(
        op: &Op<K, V>, // op
        term: u64,
        index: u64,
        buf: &mut Vec<u8>,
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
    ) -> Result<usize> {
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
    K: Serialize,
    V: Serialize,
{
    fn encode_client(
        op: &Op<K, V>,
        term: u64,
        index: u64,
        id: u64,
        ceqno: u64,
        buf: &mut Vec<u8>,
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
    ) -> Result<usize> {
        util::check_remaining(buf, 40, "entry-client-hdr")?;
        *term = u64::from_be_bytes(buf[8..16].try_into().unwrap());
        *index = u64::from_be_bytes(buf[16..24].try_into().unwrap());
        *id = u64::from_be_bytes(buf[24..32].try_into().unwrap());
        *ceqno = u64::from_be_bytes(buf[32..40].try_into().unwrap());
        Ok(40 + op.decode(&buf[40..])?)
    }
}

/************************ Operations within entry ***********************/

#[derive(PartialEq, Debug)]
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
enum Op<K, V>
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

impl<K, V> PartialEq for Op<K, V>
where
    K: PartialEq + Serialize,
    V: PartialEq + Serialize,
{
    fn eq(&self, other: &Op<K, V>) -> bool {
        match (self, other) {
            (Op::Set { key: k1, value: v1 }, Op::Set { key: k2, value: v2 }) => {
                k1 == k2 && v1 == v2
            }
            (
                Op::SetCAS {
                    key: k1,
                    value: v1,
                    cas: n1,
                },
                Op::SetCAS {
                    key: k2,
                    value: v2,
                    cas: n2,
                },
            ) => k1 == k2 && v1 == v2 && n1 == n2,
            (Op::Delete { key: k1 }, Op::Delete { key: k2 }) => k1 == k2,
            _ => false,
        }
    }
}

impl<K, V> fmt::Debug for Op<K, V>
where
    K: Serialize + fmt::Debug,
    V: Serialize + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            Op::Set { key: k, value: v } => {
                write!(f, "<Op::Set<key: {:?} value: {:?}>", k, v)?;
            }
            Op::SetCAS {
                key: k,
                value: v,
                cas,
            } => {
                write!(f, "Op::Set<key:{:?} val:{:?} cas:{}>", k, v, cas)?;
            }
            Op::Delete { key } => {
                write!(f, "Op::Set< key: {:?}>", key)?;
            }
        }
        Ok(())
    }
}

impl<K, V> Op<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn new_set(key: K, value: V) -> Op<K, V> {
        Op::Set { key, value }
    }

    fn new_set_cas(key: K, value: V, cas: u64) -> Op<K, V> {
        Op::SetCAS { cas, key, value }
    }

    fn new_delete(key: K) -> Op<K, V> {
        Op::Delete { key }
    }

    fn op_type(buf: &[u8]) -> Result<OpType> {
        util::check_remaining(buf, 8, "op-type")?;
        let hdr1 = u64::from_be_bytes(buf[..8].try_into().unwrap());
        Ok(((hdr1 >> 32) & 0x00FFFFFF).into())
    }
}

impl<K, V> Serialize for Op<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        match self {
            Op::Set { key, value } => {
                let n = Self::encode_set(buf, key, value);
                n
            }
            Op::SetCAS { key, value, cas } => {
                let n = Self::encode_set_cas(buf, key, value, *cas);
                n
            }
            Op::Delete { key } => {
                let n = Self::encode_delete(buf, key);
                n
            }
        }
    }

    fn decode(&mut self, buf: &[u8]) -> Result<usize> {
        *self = match Self::op_type(buf)? {
            OpType::Set => {
                // key, value
                Op::new_set(unsafe { mem::zeroed() }, unsafe { mem::zeroed() })
            }
            OpType::SetCAS => {
                let key: K = unsafe { mem::zeroed() };
                let value: V = unsafe { mem::zeroed() };
                Op::new_set_cas(key, value, unsafe { mem::zeroed() })
            }
            OpType::Delete => {
                // key
                Op::new_delete(unsafe { mem::zeroed() })
            }
        };

        match self {
            Op::Set { key, value } => {
                let n = Self::decode_set(buf, key, value);
                n
            }
            Op::SetCAS { key, value, cas } => {
                let n = Self::decode_set_cas(buf, key, value, cas);
                n
            }
            Op::Delete { key } => {
                let n = Self::decode_delete(buf, key);
                n
            }
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
        let hdr1: u64 = ((OpType::Set as u64) << 32) | klen;
        let vlen: u64 = value.encode(buf).try_into().unwrap();

        buf[n..n + 8].copy_from_slice(&hdr1.to_be_bytes());
        buf[n + 8..n + 16].copy_from_slice(&vlen.to_be_bytes());

        (klen + vlen + 16).try_into().unwrap()
    }

    fn decode_set(buf: &[u8], k: &mut K, v: &mut V) -> Result<usize> {
        let mut n = 16;
        let (klen, vlen) = {
            util::check_remaining(buf, 16, "op-set-hdr")?;
            let hdr1 = u64::from_be_bytes(buf[..8].try_into().unwrap());
            let klen: usize = (hdr1 & 0xFFFFFFFF).try_into().unwrap();
            let vlen = u64::from_be_bytes(buf[8..16].try_into().unwrap());
            let vlen: usize = vlen.try_into().unwrap();
            (klen, vlen)
        };

        n += {
            util::check_remaining(buf, n + klen, "op-set-key")?;
            k.decode(&buf[n..n + klen])?;
            klen
        };

        n += {
            util::check_remaining(buf, n + vlen, "op-set-value")?;
            v.decode(&buf[n..n + vlen])?;
            vlen
        };

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
        let hdr1: u64 = ((OpType::SetCAS as u64) << 32) | klen;
        let vlen: u64 = value.encode(buf).try_into().unwrap();

        buf[n..n + 8].copy_from_slice(&hdr1.to_be_bytes());
        buf[n + 8..n + 16].copy_from_slice(&vlen.to_be_bytes());
        buf[n + 16..n + 24].copy_from_slice(&cas.to_be_bytes());

        (klen + vlen + 24).try_into().unwrap()
    }

    fn decode_set_cas(
        buf: &[u8],
        key: &mut K,
        value: &mut V,
        cas: &mut u64, // reference
    ) -> Result<usize> {
        let mut n = 24;
        let (klen, vlen, cas_seqno) = {
            util::check_remaining(buf, n, "op-setcas-hdr")?;
            let hdr1 = u64::from_be_bytes(buf[..8].try_into().unwrap());
            let klen: usize = (hdr1 & 0xFFFFFFFF).try_into().unwrap();
            let vlen = u64::from_be_bytes(buf[8..16].try_into().unwrap());
            let vlen: usize = vlen.try_into().unwrap();
            let cas = u64::from_be_bytes(buf[16..24].try_into().unwrap());
            (klen, vlen, cas)
        };
        *cas = cas_seqno;

        n += {
            util::check_remaining(buf, n + klen, "op-setcas-key")?;
            key.decode(&buf[n..n + klen])?;
            klen
        };

        n += {
            util::check_remaining(buf, n + vlen, "op-setcas-value")?;
            value.decode(&buf[n..n + vlen])?;
            vlen
        };

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
    K: Serialize,
    V: Serialize,
{
    fn encode_delete(buf: &mut Vec<u8>, key: &K) -> usize {
        let n = buf.len();
        buf.resize(n + 8, 0);

        let klen = {
            let klen: u64 = key.encode(buf).try_into().unwrap();
            let hdr1: u64 = ((OpType::Delete as u64) << 32) | klen;
            buf[n..n + 8].copy_from_slice(&hdr1.to_be_bytes());
            klen
        };

        (klen + 8).try_into().unwrap()
    }

    fn decode_delete(buf: &[u8], key: &mut K) -> Result<usize> {
        let mut n = 8;
        let klen: usize = {
            util::check_remaining(buf, n, "op-delete-hdr1")?;
            let hdr1 = u64::from_be_bytes(buf[..n].try_into().unwrap());
            (hdr1 & 0xFFFFFFFF).try_into().unwrap()
        };

        n += {
            util::check_remaining(buf, n + klen, "op-delete-key")?;
            key.decode(&buf[n..n + klen])?;
            klen
        };

        Ok(n.try_into().unwrap())
    }
}

#[cfg(test)]
#[path = "wal_test.rs"]
mod wal_test;
