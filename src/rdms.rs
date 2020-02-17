//! Module `rdms` implement a full-featured storage index.
//!
//! [Rdms] can be composed using underlying components and mechanisms defined
//! in [core] module.

use log::error;

use std::{
    convert, fmt, marker,
    ops::Bound,
    sync::{self, mpsc, Arc, MutexGuard},
    thread,
    time::{Duration, SystemTime},
};

#[allow(unused_imports)]
use crate::core;
use crate::{
    core::{CommitIter, CommitIterator, Diff, Entry, Footprint, Index},
    core::{Cutoff, Result, Validate},
    thread as rt,
};

/// Default commit interval, _30 minutes_. Refer to set_commit_interval()
/// method for more detail.
pub const COMMIT_INTERVAL: usize = 30 * 60; // 30 minutes

/// Index type, composable index type. Check module documentation for
/// full set of features.
pub struct Rdms<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Index<K, V>,
{
    name: String,

    index: Option<Arc<sync::Mutex<I>>>,
    auto_commit: Option<rt::Thread<(), (), ()>>,

    _key: marker::PhantomData<K>,
    _value: marker::PhantomData<V>,
}

impl<K, V, I> Drop for Rdms<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Index<K, V>,
{
    fn drop(&mut self) {
        match self.auto_commit.take() {
            Some(auto_commit) => match auto_commit.close_wait() {
                Err(err) => error!(
                    target: "rdms  ",
                    "{:?}, auto-commit {:?}", self.name, err
                ),
                Ok(_) => (),
            },
            None => (),
        }
    }
}

impl<K, V, I> Rdms<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Index<K, V>,
{
    /// Create a new `Rdms` instance, identified by `name` using an underlying
    /// `index`.
    pub fn new<S>(name: S, index: I) -> Result<Box<Rdms<K, V, I>>>
    where
        S: AsRef<str>,
    {
        let value = Box::new(Rdms {
            name: name.as_ref().to_string(),

            auto_commit: None,
            index: Some(Arc::new(sync::Mutex::new(index))),

            _key: marker::PhantomData,
            _value: marker::PhantomData,
        });
        Ok(value)
    }

    /// Close this `Rdms` instance. The life-cycle of the index finishes when
    /// close is called. Note that, only memory resources will be cleared by
    /// this call. To cleanup persisted data (in disk) use the `purge()`.
    pub fn close(mut self) -> Result<()> {
        self.do_close()?;
        match Arc::try_unwrap(self.index.take().unwrap()) {
            Ok(index) => index.into_inner().unwrap().close()?,
            Err(_) => unreachable!(),
        }
        Ok(())
    }

    /// Purge this index along with disk data.
    pub fn purge(mut self) -> Result<()> {
        self.do_close()?;
        match Arc::try_unwrap(self.index.take().unwrap()) {
            Ok(index) => index.into_inner().unwrap().close()?,
            Err(_) => unreachable!(),
        }
        Ok(())
    }

    fn do_close(&mut self) -> Result<()> {
        match self.auto_commit.take() {
            Some(auto_commit) => auto_commit.close_wait()?,
            None => (),
        }
        Ok(())
    }
}

impl<K, V, I> Rdms<K, V, I>
where
    K: Send + Clone + Ord + Footprint,
    V: Send + Clone + Diff + Footprint,
    I: 'static + Send + Index<K, V>,
{
    /// Set interval in time duration, for invoking auto commit.
    /// Calling this method will spawn an auto compaction thread.
    pub fn set_commit_interval(&mut self, interval: Duration) -> &mut Rdms<K, V, I> {
        self.auto_commit = match self.auto_commit.take() {
            Some(auto_commit) => Some(auto_commit),
            None if interval.as_secs() > 0 => {
                let index = Arc::clone(self.index.as_ref().unwrap());
                Some(rt::Thread::new(move |rx| {
                    move || auto_commit::<K, V, I>(index, interval, rx)
                }))
            }
            None => None,
        };
        self
    }
}

impl<K, V, I> Rdms<K, V, I>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    I: Index<K, V>,
{
    fn as_index(&self) -> Result<MutexGuard<I>> {
        use crate::error::Error::ThreadFail;

        self.index
            .as_ref()
            .unwrap()
            .lock()
            .map_err(|err| ThreadFail(format!("rdms lock poisened, {:?}", err)))
    }
}

impl<K, V, I> Rdms<K, V, I>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    I: Index<K, V>,
{
    pub fn to_name(&self) -> Result<String> {
        Ok(self.name.to_string())
    }

    pub fn to_metadata(&self) -> Result<Vec<u8>> {
        let index = self.as_index()?;
        index.to_metadata()
    }

    pub fn to_seqno(&self) -> Result<u64> {
        let index = self.as_index()?;
        index.to_seqno()
    }

    pub fn set_seqno(&mut self, seqno: u64) -> Result<()> {
        let mut index = self.as_index()?;
        index.set_seqno(seqno)
    }

    pub fn to_reader(&mut self) -> Result<<I as Index<K, V>>::R> {
        let mut index = self.as_index()?;
        index.to_reader()
    }

    pub fn to_writer(&mut self) -> Result<<I as Index<K, V>>::W> {
        let mut index = self.as_index()?;
        index.to_writer()
    }

    pub fn commit<C, F>(&mut self, scanner: CommitIter<K, V, C>, metacb: F) -> Result<()>
    where
        C: CommitIterator<K, V>,
        F: Fn(Vec<u8>) -> Vec<u8>,
    {
        let mut index = self.as_index()?;
        index.commit(scanner, metacb)
    }

    pub fn compact<F>(&mut self, cutoff: Cutoff, metacb: F) -> Result<usize>
    where
        F: Fn(Vec<u8>) -> Vec<u8>,
    {
        let mut index = self.as_index()?;
        index.compact(cutoff, metacb)
    }
}

impl<K, V, T, I> Validate<T> for Box<Rdms<K, V, I>>
where
    K: Clone + Ord + Footprint + fmt::Debug,
    V: Clone + Diff + Footprint,
    I: Index<K, V> + Validate<T>,
    T: fmt::Display,
{
    fn validate(&mut self) -> Result<T> {
        let mut index = self.as_index()?;
        index.validate()
    }
}

// TODO: return some valid stats.
fn auto_commit<K, V, I>(
    index: Arc<sync::Mutex<I>>,
    interval: Duration,
    rx: rt::Rx<(), ()>,
) -> Result<()>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    I: Index<K, V>,
{
    use crate::error::Error::ThreadFail;

    let mut elapsed = Duration::new(0, 0);
    loop {
        if elapsed < interval {
            thread::sleep(interval - elapsed);
        }

        match rx.try_recv() {
            Err(mpsc::TryRecvError::Empty) => (),
            Err(mpsc::TryRecvError::Disconnected) => break Ok(()),
            Ok(_) => unreachable!(),
        }

        elapsed = {
            let start = SystemTime::now();

            let mut indx = match index.lock() {
                Ok(index) => index,
                Err(err) => {
                    let msg = format!("rdms lock poisened, {:?}", err);
                    return Err(ThreadFail(msg));
                }
            };

            let scanner = {
                let empty: Vec<Result<Entry<K, V>>> = vec![];
                let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
                CommitIter::new(empty.into_iter(), within)
            };
            match indx.commit(scanner, convert::identity) {
                Ok(_) => (),
                Err(err) => {
                    error!(target: "rdms  ", "commit failed {:?}", err);
                    break Err(err);
                }
            }

            let compute_elapsed = || -> Result<Duration> {
                Ok(match start.elapsed() {
                    Ok(elapsed) => Ok(elapsed),
                    Err(err) => {
                        error!(target: "rdms  ", "elapsed failed {:?}", err);
                        Err(err)
                    }
                }?)
            };

            compute_elapsed()?
        };
    }
}
