//! Package implement WRite-Ahead-Logging.
//!
//! Entries are added to `Wal` journal. Journals automatically rotate
//! and are numbered from ZERO.

use cbordata::FromCbor;

use std::{
    ffi, fs,
    marker::PhantomData,
    mem, ops, path,
    sync::{Arc, RwLock},
    vec,
};

use crate::{
    util,
    wral::{
        self,
        journal::{IterJournal, Journal},
        journals::{Journals, Req, Res},
        Config,
    },
    Error, Result,
};

/// Write ahead logging.
pub struct Wal<S = wral::NoState> {
    config: Config,

    w: Arc<RwLock<Journals<S>>>,
    th: Arc<util::thread::Thread<Req, Res, Result<u64>>>,
    tx: util::thread::Tx<Req, Res>,
}

impl<S> Clone for Wal<S> {
    fn clone(&self) -> Wal<S> {
        Wal {
            config: self.config.clone(),

            w: Arc::clone(&self.w),
            th: Arc::clone(&self.th),
            tx: self.tx.clone(),
        }
    }
}

impl<S> Wal<S> {
    /// Create a new Write-Ahead-Log instance, while create a new journal, older
    /// journals matching the `name` shall be purged.
    pub fn create(config: Config, state: S) -> Result<Wal<S>>
    where
        S: wral::State,
    {
        // try creating the directory, if it does not exist.
        fs::create_dir_all(&config.dir).ok();

        // purge existing journals for this shard.
        for item in err_at!(IOError, fs::read_dir(&config.dir))? {
            let file_path: path::PathBuf = {
                let file_name = err_at!(IOError, item)?.file_name();
                [config.dir.clone(), file_name.clone()].iter().collect()
            };
            if let Some(journal) =
                Journal::<S>::load_cold(&config.name, file_path.as_ref())
            {
                journal.purge().ok();
            };
        }

        let num = 0;
        let journal = Journal::start(&config.dir, &config.name, num, state)?;

        let seqno = 1;
        let (w, th, tx) = Journals::start(config.clone(), seqno, vec![], journal);

        let val = Wal {
            config,

            w,
            th: Arc::new(th),
            tx,
        };

        Ok(val)
    }

    /// Load an existing journal under `dir`, matching `name`. Files that
    /// don't match the journal file-name structure or journals with
    /// corrupted batch or corrupted state shall be ignored.
    ///
    /// Application state shall be loaded from the last batch of the
    /// last journal.
    pub fn load(config: Config) -> Result<Wal<S>>
    where
        S: wral::State,
    {
        let mut journals: Vec<(Journal<S>, u64, S)> = vec![];
        for item in err_at!(IOError, fs::read_dir(&config.dir))? {
            let file_path: path::PathBuf = {
                let file_name = err_at!(IOError, item)?.file_name();
                [config.dir.clone(), file_name.clone()].iter().collect()
            };
            if let Some((journal, state)) =
                Journal::load(&config.name, file_path.as_ref())
            {
                let seqno = journal.to_last_seqno().unwrap();
                journals.push((journal, seqno, state));
            };
        }

        journals.sort_by(|(_, a, _), (_, b, _)| a.cmp(b));

        let (mut seqno, num, state) = match journals.last() {
            Some((j, seqno, state)) => (*seqno, j.to_journal_number(), state.clone()),
            None => (0, 0, S::default()),
        };
        seqno += 1;
        let num = num.saturating_add(1);
        let journal = Journal::start(&config.dir, &config.name, num, state)?;

        let journals: Vec<Journal<S>> = journals.into_iter().map(|(j, _, _)| j).collect();
        let (w, th, tx) = Journals::start(config.clone(), seqno, journals, journal);

        let val = Wal {
            config,

            w,
            th: Arc::new(th),
            tx,
        };

        Ok(val)
    }

    /// Close the [Wal] instance.
    pub fn close(self) -> Result<Option<u64>> {
        match Arc::try_unwrap(self.th) {
            Ok(th) => {
                mem::drop(self.tx);
                th.join()??;

                match Arc::try_unwrap(self.w) {
                    Ok(w) => Ok(Some(err_at!(IPCFail, w.into_inner())?.close()?)),
                    Err(_) => Ok(None),
                }
            }
            Err(_) => Ok(None),
        }
    }

    /// Close the [Wal] instance and purge it.
    pub fn purge(self) -> Result<Option<u64>> {
        match Arc::try_unwrap(self.th) {
            Ok(th) => {
                mem::drop(self.tx);
                th.join()??;
                println!("jon ok");
                match Arc::try_unwrap(self.w) {
                    Ok(w) => Ok(Some(err_at!(IPCFail, w.into_inner())?.purge()?)),
                    Err(_) => Ok(None),
                }
            }
            Err(_) => Ok(None),
        }
    }
}

impl<S> Wal<S> {
    /// Add a operation to WAL, operations are pre-serialized and opaque to
    /// Wal instances. Return the sequence-number for this operation.
    pub fn add_op(&self, op: &[u8]) -> Result<u64> {
        let req = Req::AddEntry { op: op.to_vec() };
        let Res::Seqno(seqno) = self.tx.request(req)?;
        Ok(seqno)
    }

    /// Commit outstanding operations into disc and return the latest seqno.
    pub fn commit(&self) -> Result<u64> {
        let req = Req::Commit;
        let Res::Seqno(seqno) = self.tx.request(req)?;
        Ok(seqno)
    }
}

impl<S> Wal<S> {
    /// Iterate over all entries in this Wal instance, entries can span
    /// across multiple journal files. Iteration will start from lowest
    /// sequence-number to highest.
    pub fn iter(&self) -> Result<impl Iterator<Item = Result<wral::Entry>>>
    where
        S: Clone + FromCbor,
    {
        self.range(..)
    }

    /// Iterate over entries whose sequence number fall within the specified `range`.
    pub fn range<R>(&self, range: R) -> Result<impl Iterator<Item = Result<wral::Entry>>>
    where
        S: Clone + FromCbor,
        R: ops::RangeBounds<u64>,
    {
        let (range, journals) = match Self::range_bound_to_range_inclusive(range) {
            Some(range) => {
                let rd = err_at!(Fatal, self.w.read())?;
                let mut journals = vec![];
                for jn in rd.journals.iter() {
                    if jn.is_open() {
                        journals.push(jn.to_location());
                    }
                }
                if rd.journal.is_open() {
                    journals.push(rd.journal.to_location());
                }
                (range, journals)
            }
            None => ((0..=0), vec![]),
        };

        Ok(Iter {
            name: self.config.name.clone(),
            range,
            journal: None,
            journals: journals.into_iter(),
            _state: PhantomData::<S>,
        })
    }

    fn range_bound_to_range_inclusive<R>(range: R) -> Option<ops::RangeInclusive<u64>>
    where
        R: ops::RangeBounds<u64>,
    {
        let start = match range.start_bound() {
            ops::Bound::Excluded(start) if *start < u64::MAX => Some(*start + 1),
            ops::Bound::Excluded(_) => None,
            ops::Bound::Included(start) => Some(*start),
            ops::Bound::Unbounded => Some(0),
        }?;
        let end = match range.end_bound() {
            ops::Bound::Excluded(0) => None,
            ops::Bound::Excluded(end) => Some(*end - 1),
            ops::Bound::Included(end) => Some(*end),
            ops::Bound::Unbounded => Some(u64::MAX),
        }?;
        Some(start..=end)
    }
}

struct Iter<S> {
    name: String,
    range: ops::RangeInclusive<u64>,
    journal: Option<IterJournal>,
    journals: vec::IntoIter<ffi::OsString>,
    _state: PhantomData<S>,
}

macro_rules! next_journal_file {
    ($self:expr) => {{
        let jnfile = $self.journals.next()?;
        match Journal::<S>::load(&$self.name, &jnfile) {
            Some((jn, _)) => {
                let iter = IterJournal::from_journal(&jn, $self.range.clone());
                match iter {
                    Ok(iter) => iter,
                    Err(e) => return Some(
                        err_at!(Fatal, msg: "iter on invalid journal {:?} {}", jnfile, e)
                    ),
                }
            }
            None => {
                return Some(
                    err_at!(Fatal, msg: "invalid journal {:?}", jnfile)
                );
            }
        }
    }};
}

impl<S> Iterator for Iter<S>
where
    S: Clone + FromCbor,
{
    type Item = Result<wral::Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut journal = match self.journal.take() {
            Some(journal) => journal,
            None => next_journal_file!(self),
        };

        loop {
            match journal.next() {
                Some(item) => {
                    self.journal = Some(journal);
                    break Some(item);
                }
                None => {
                    journal = next_journal_file!(self);
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "wal_test.rs"]
mod wal_test;
