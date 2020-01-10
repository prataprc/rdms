//! Module `rdms` implement a full-featured storage index.
//!
//! [Rdms] can be composed using underlying components and mechanisms defined
//! in `core` module.

use log::error;

use std::{
    convert, fmt, marker,
    ops::Bound,
    sync::{self, Arc, MutexGuard},
    thread,
    time::{Duration, SystemTime},
};

use crate::core::{CommitIter, CommitIterator, Diff, Entry, Footprint, Index, Result, Validate};

/// Default commit interval, in seconds. Refer to set_commit_interval()
/// method for more detail.
pub const COMMIT_INTERVAL: usize = 30 * 60; // 30 minutes

/// Index keys and corresponding values. Check module documentation for
/// the full set of features.
pub struct Rdms<K, V, I>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    I: Index<K, V>,
{
    name: String,

    index: Arc<sync::Mutex<I>>,
    _key: marker::PhantomData<K>,
    _value: marker::PhantomData<V>,
}

impl<K, V, I> Rdms<K, V, I>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    I: Index<K, V>,
{
    pub fn new<S>(name: S, index: I) -> Result<Box<Rdms<K, V, I>>>
    where
        S: AsRef<str>,
    {
        let value = Box::new(Rdms {
            name: name.as_ref().to_string(),

            index: Arc::new(sync::Mutex::new(index)),
            _key: marker::PhantomData,
            _value: marker::PhantomData,
        });
        Ok(value)
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
    pub fn set_commit_interval(&mut self, interval: Duration) {
        let index = Arc::clone(&self.index);
        thread::spawn(move || auto_commit::<K, V, I>(index, interval));
    }
}

impl<K, V, I> Drop for Rdms<K, V, I>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    I: Index<K, V>,
{
    fn drop(&mut self) {
        // place holder
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
        index.set_seqno(seqno);
        Ok(())
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

    pub fn compact<F>(&mut self, cutoff: Bound<u64>, metacb: F) -> Result<usize>
    where
        F: Fn(Vec<Vec<u8>>) -> Vec<u8>,
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

fn auto_commit<K, V, I>(index: Arc<sync::Mutex<I>>, interval: Duration) -> Result<()>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    I: Index<K, V>,
{
    use crate::error::Error::ThreadFail;

    let mut elapsed = Duration::new(0, 0);
    let initial_count = Arc::strong_count(&index);
    loop {
        if elapsed < interval {
            thread::sleep(interval - elapsed);
        }
        if Arc::strong_count(&index) < initial_count {
            break Ok(()); // cascading quite,
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
