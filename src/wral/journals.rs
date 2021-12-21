use cbordata::{FromCbor, IntoCbor};

use std::{
    borrow::BorrowMut,
    sync::{
        atomic::{AtomicU64, Ordering::SeqCst},
        Arc, RwLock,
    },
};

use crate::{
    util::thread,
    wral::{self, journal::Journal, state, Config},
    Error, Result,
};

#[derive(Debug)]
pub enum Req {
    // serialized opaque entry to be logged into the journal
    AddEntry { op: Vec<u8> },
}

#[derive(Debug)]
pub enum Res {
    // monotonously increasing seqno
    Seqno(u64),
}

// Journals can be concurrently accessed.
pub struct Journals<S> {
    config: Config,
    seqno: Arc<AtomicU64>,
    pub journals: Vec<Journal<S>>,
    pub journal: Journal<S>,
}

type StartJournals<S> = (
    Arc<RwLock<Journals<S>>>,
    thread::Thread<Req, Res, Result<u64>>,
    thread::Tx<Req, Res>,
);

impl<S> Journals<S> {
    pub(crate) fn start(
        config: Config,
        seqno: u64,
        journals: Vec<Journal<S>>,
        journal: Journal<S>,
    ) -> StartJournals<S>
    where
        S: state::State,
    {
        let seqno = Arc::new(AtomicU64::new(seqno));
        let journals = Arc::new(RwLock::new(Journals {
            config: config.clone(),
            seqno: Arc::clone(&seqno),
            journals,
            journal,
        }));
        let name = format!("wral-journals-{}", config.name);
        let thread_w = Arc::clone(&journals);
        let th = thread::Thread::new_sync(
            &name,
            wral::SYNC_BUFFER,
            move |rx: thread::Rx<Req, Res>| {
                || {
                    let l = MainLoop {
                        config,
                        seqno,
                        journals: thread_w,
                        rx,
                    };
                    l.run()
                }
            },
        );
        let tx = th.to_tx();

        (journals, th, tx)
    }

    pub fn close(&self) -> Result<u64> {
        let n_batches: usize = self.journals.iter().map(|j| j.len_batches()).sum();
        let (_n, _m) = match self.journal.len_batches() {
            0 => (self.journals.len(), n_batches),
            n => (self.journals.len() + 1, n_batches + n),
        };
        Ok(self.seqno.load(SeqCst).saturating_sub(1))
    }

    pub fn purge(mut self) -> Result<u64> {
        self.close()?;

        for j in self.journals.drain(..) {
            j.purge()?
        }
        self.journal.purge()?;

        Ok(self.seqno.load(SeqCst).saturating_sub(1))
    }
}

struct MainLoop<S> {
    config: Config,
    seqno: Arc<AtomicU64>,
    journals: Arc<RwLock<Journals<S>>>,
    rx: thread::Rx<Req, Res>,
}

impl<S> MainLoop<S>
where
    S: Clone + IntoCbor + FromCbor + state::State,
{
    fn run(self) -> Result<u64> {
        use std::sync::mpsc::TryRecvError;

        // block for the first request.
        'a: while let Ok(req) = self.rx.recv() {
            // then get as many outstanding requests as possible from
            // the channel.
            let mut reqs = vec![req];
            loop {
                match self.rx.try_recv() {
                    Ok(req) => reqs.push(req),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break 'a,
                }
            }
            // and then start processing it in batch.
            let mut journals = err_at!(Fatal, self.journals.write())?;

            let mut items = vec![];
            for req in reqs.into_iter() {
                match req {
                    (Req::AddEntry { op }, tx) => {
                        let seqno = self.seqno.fetch_add(1, SeqCst);
                        journals.journal.add_entry(wral::Entry::new(seqno, op))?;
                        items.push((seqno, tx))
                    }
                }
            }
            journals.journal.flush()?;

            for (seqno, tx) in items.into_iter() {
                if let Some(tx) = tx {
                    err_at!(IPCFail, tx.send(Res::Seqno(seqno)))?;
                }
            }

            if journals.journal.file_size()? > self.config.journal_limit {
                Self::rotate(journals.borrow_mut())?;
            }
        }

        Ok(self.seqno.load(SeqCst).saturating_sub(1))
    }
}

impl<S> MainLoop<S>
where
    S: Clone,
{
    fn rotate(journals: &mut Journals<S>) -> Result<()> {
        use std::mem;

        // new journal
        let journal = {
            let num = journals.journal.to_journal_number().saturating_add(1);
            let state = journals.journal.as_state().clone();
            Journal::start(&journals.config.dir, &journals.config.name, num, state)?
        };
        // replace with current journal
        let journal = mem::replace(&mut journals.journal, journal);
        let (journal, entries, _) = journal.into_archive();
        if !entries.is_empty() {
            err_at!(Fatal, msg: "unflushed entries {}", entries.len())?
        }
        journals.journals.push(journal);
        Ok(())
    }
}
