use cbordata::{Cbor, FromCbor};

use std::{
    convert::TryFrom,
    ffi,
    fmt::{self, Display},
    fs, ops, path, result, vec,
};

use crate::{
    wral::{self, batch, files, state},
    Error, Result,
};

// A journal is uniquely located by specifying the (`dir`, `name`, `num`). Where,
// `dir` is the directory in which the journal is located, `name` is the unique
// name for the journal and `num` is the journal's rotating number. Note that,
// once a journal file exceeds a limit, it shall be archived and new journal file
// created with next `num` number.
//
// A journal can be in one of the three state:
// * `Working`: entries can be added and batches flushed.
// * `Archive`: useful only for the purpose of locating previous batches.
// * `Cold`: only for backup.
pub struct Journal<S> {
    name: String,
    num: usize,
    location: ffi::OsString, // dir/{name}-journal-{num}.dat
    inner: InnerJournal<S>,
}

impl<S> Display for Journal<S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{:?}", files::make_filename(&self.name, self.num))
    }
}

// A journal can be in three state, Working, Archive and cold.
enum InnerJournal<S> {
    // Active journal, the latest journal, in the journal-set.
    Working {
        worker: batch::Worker<S>,
        file: Option<fs::File>,
    },
    // All journals except lastest journal are archives, which means only
    // the metadata for each batch shall be stored.
    Archive {
        index: Vec<batch::Index>,
        state: S,
    },
    // Cold journals are colder than archives, that is, they are not
    // required by the application, may be as frozen-backup.
    Cold,
}

impl<S> Journal<S> {
    /// Start a new journal under directory `dir`, with initial `state`. If a file
    /// already exists with (name, num) under dir, then that journal shall be removed.
    ///
    /// Returned journal shall be in `Working` state.
    pub fn start(
        dir: &ffi::OsStr,
        name: &str,
        num: usize,
        state: S,
    ) -> Result<Journal<S>> {
        let location: path::PathBuf = {
            let file: ffi::OsString = files::make_filename(name, num);
            [dir, &file].iter().collect()
        };

        fs::remove_file(&location).ok(); // cleanup a single journal file

        Ok(Journal {
            name: name.to_string(),
            num,
            location: location.into_os_string(),
            inner: InnerJournal::Working {
                worker: batch::Worker::new(state),
                file: None,
            },
        })
    }

    /// Assume that location points to a valid journal in a journal-set identified
    /// by `name`. If not the case return None.
    ///
    /// Returned journal shall be in `Archive` state.
    pub fn load(name: &str, location: &ffi::OsStr) -> Option<(Journal<S>, S)>
    where
        S: Clone + FromCbor,
    {
        let os_file = path::Path::new(location);
        let (nm, num) = files::unwrap_filename(os_file.file_name()?)?;

        if nm != name {
            return None;
        }

        let mut file = {
            let mut opts = fs::OpenOptions::new();
            err_at!(IOError, opts.read(true).open(os_file)).ok()?
        };

        let (mut state, mut index, mut fpos) = (vec![], vec![], 0_usize);
        let len = file.metadata().ok()?.len();

        while u64::try_from(fpos).ok()? < len {
            let (val, n) = Cbor::decode(&mut file).ok()?;
            let batch = batch::Batch::from_cbor(val).ok()?;
            index.push(batch::Index::new(
                u64::try_from(fpos).ok()?,
                n,
                batch.to_first_seqno(),
                batch.to_last_seqno(),
            ));
            state = batch.as_state_bytes().to_vec();
            fpos += n
        }

        if index.is_empty() {
            return None;
        }

        let (val, _) = Cbor::decode(&mut state.as_slice()).ok()?;
        let state = S::from_cbor(val).ok()?;

        let journal = Journal {
            name: name.to_string(),
            num,
            location: location.to_os_string(),
            inner: InnerJournal::Archive {
                index,
                state: state.clone(),
            },
        };

        Some((journal, state))
    }

    pub fn load_cold(name: &str, location: &ffi::OsStr) -> Option<Journal<S>> {
        let os_file = path::Path::new(location);
        let (nm, num) = files::unwrap_filename(os_file.file_name()?)?;

        if nm != name {
            return None;
        }

        let journal = Journal {
            name: name.to_string(),
            num,
            location: location.to_os_string(),
            inner: InnerJournal::Cold,
        };
        Some(journal)
    }

    pub fn into_archive(mut self) -> (Self, Vec<wral::Entry>, S)
    where
        S: Clone,
    {
        let (inner, entries, state) = match self.inner {
            InnerJournal::Working { worker, .. } => {
                let (index, entries, state) = worker.unwrap();
                let inner = InnerJournal::Archive {
                    index,
                    state: state.clone(),
                };
                (inner, entries, state)
            }
            _ => unreachable!(),
        };
        self.inner = inner;
        (self, entries, state)
    }

    pub fn purge(self) -> Result<()> {
        if self.is_open() || self.is_cold() {
            err_at!(IOError, fs::remove_file(&self.location))?;
        }
        Ok(())
    }
}

impl<S> Journal<S> {
    pub fn add_entry(&mut self, entry: wral::Entry) -> Result<()>
    where
        S: state::State,
    {
        match &mut self.inner {
            InnerJournal::Working { worker, .. } => worker.add_entry(entry),
            InnerJournal::Archive { .. } => unreachable!(),
            InnerJournal::Cold => unreachable!(),
        }
    }

    pub fn flush(&mut self) -> Result<()>
    where
        S: state::State,
    {
        match &mut self.inner {
            InnerJournal::Working { worker, file } if file.is_some() => {
                worker.flush(file.as_mut().unwrap())?;
                Ok(())
            }
            InnerJournal::Working { worker, file } if worker.is_flush_required() => {
                let jfile = {
                    let mut opts = fs::OpenOptions::new();
                    let location = self.location.clone();
                    err_at!(IOError, opts.append(true).create_new(true).open(&location))?
                };
                *file = Some(jfile);
                worker.flush(file.as_mut().unwrap())?;
                Ok(())
            }
            InnerJournal::Working { .. } => Ok(()),
            InnerJournal::Archive { .. } => unreachable!(),
            InnerJournal::Cold { .. } => unreachable!(),
        }
    }
}

impl<S> Journal<S> {
    pub fn to_journal_number(&self) -> usize {
        self.num
    }

    pub fn len_batches(&self) -> usize {
        match &self.inner {
            InnerJournal::Working { worker, .. } => worker.len_batches(),
            InnerJournal::Archive { index, .. } => index.len(),
            InnerJournal::Cold { .. } => unreachable!(),
        }
    }

    pub fn to_last_seqno(&self) -> Option<u64> {
        match &self.inner {
            InnerJournal::Working { worker, .. } => worker.to_last_seqno(),
            InnerJournal::Archive { index, .. } if index.is_empty() => None,
            InnerJournal::Archive { index, .. } => {
                index.last().map(batch::Index::to_last_seqno)
            }
            _ => None,
        }
    }

    pub fn file_size(&self) -> Result<usize> {
        let n = match &self.inner {
            InnerJournal::Working { file: None, .. } => 0,
            InnerJournal::Working { file, .. } => {
                let m = err_at!(IOError, file.as_ref().unwrap().metadata())?;
                err_at!(FailConvert, usize::try_from(m.len()))?
            }
            InnerJournal::Archive { .. } => unreachable!(),
            InnerJournal::Cold => unreachable!(),
        };
        Ok(n)
    }

    pub fn as_state(&self) -> &S {
        match &self.inner {
            InnerJournal::Working { worker, .. } => worker.as_state(),
            InnerJournal::Archive { state, .. } => state,
            InnerJournal::Cold => unreachable!(),
        }
    }

    pub fn to_location(&self) -> ffi::OsString {
        self.location.clone()
    }

    pub fn is_open(&self) -> bool {
        match &self.inner {
            InnerJournal::Working { file: None, .. } => false,
            InnerJournal::Working { .. } => true,
            InnerJournal::Archive { .. } => true,
            InnerJournal::Cold { .. } => false,
        }
    }

    pub fn is_cold(&self) -> bool {
        match &self.inner {
            InnerJournal::Working { .. } => false,
            InnerJournal::Archive { .. } => false,
            InnerJournal::Cold { .. } => true,
        }
    }
}

pub struct IterJournal {
    range: ops::RangeInclusive<u64>,
    batch: vec::IntoIter<wral::Entry>,   // iter variable
    index: vec::IntoIter<batch::Index>,  // list of all batches
    entries: vec::IntoIter<wral::Entry>, // list of entries in latest batch
    file: fs::File,
}

impl IterJournal {
    pub fn from_journal<S>(
        journal: &Journal<S>,
        range: ops::RangeInclusive<u64>,
    ) -> Result<IterJournal> {
        let (index, entries) = match &journal.inner {
            InnerJournal::Working { worker, .. } => {
                (worker.as_index().to_vec(), worker.as_entries().to_vec())
            }
            InnerJournal::Archive { index, .. } => (index.to_vec(), vec![]),
            InnerJournal::Cold => unreachable!(),
        };
        let batch: vec::IntoIter<wral::Entry> = vec![].into_iter();
        let index = index
            .into_iter()
            .skip_while(|i| i.to_last_seqno() < *range.start())
            .take_while(|i| i.to_first_seqno() <= *range.end())
            .collect::<Vec<batch::Index>>()
            .into_iter();
        let entries = entries
            .into_iter()
            .filter(|e| range.contains(&e.to_seqno()))
            .collect::<Vec<wral::Entry>>()
            .into_iter();

        let file = {
            let mut opts = fs::OpenOptions::new();
            err_at!(IOError, opts.read(true).open(&journal.location))?
        };

        Ok(IterJournal {
            range,
            batch,
            index,
            entries,
            file,
        })
    }
}

impl Iterator for IterJournal {
    type Item = Result<wral::Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.batch.next() {
            Some(entry) => Some(Ok(entry)),
            None => match self.index.next() {
                Some(index) => match batch::Batch::from_index(index, &mut self.file) {
                    Ok(batch) => {
                        self.batch = batch.into_iter(self.range.clone());
                        self.next()
                    }
                    Err(err) => Some(Err(err)),
                },
                None => self.entries.next().map(Ok),
            },
        }
    }
}

#[cfg(test)]
#[path = "journal_test.rs"]
mod journal_test;
