//! Module `rdms` implement a full-featured storage index.
//!
//! [Rdms] can be composed using underlying components and mechanisms defined
//! in `core` module.

use std::{
    convert, fmt, marker,
    ops::Bound,
    sync::{self, Arc},
    thread,
    time::{Duration, SystemTime},
};

use crate::core::{CommitIterator, Diff, Entry, Footprint, Index, Result, Validate};

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

impl<K, V, I> AsRef<sync::Mutex<I>> for Rdms<K, V, I>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    I: Index<K, V>,
{
    fn as_ref(&self) -> &sync::Mutex<I> {
        &self.index
    }
}

impl<K, V, I> Rdms<K, V, I>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    I: Index<K, V>,
{
    pub fn to_name(&self) -> String {
        self.name.to_string()
    }

    pub fn to_metadata(&self) -> Result<Vec<u8>> {
        let index = self.index.lock().unwrap();
        index.to_metadata()
    }

    pub fn to_seqno(&self) -> u64 {
        let index = self.index.lock().unwrap();
        index.to_seqno()
    }

    pub fn set_seqno(&mut self, seqno: u64) {
        let mut index = self.index.lock().unwrap();
        index.set_seqno(seqno)
    }

    pub fn to_reader(&mut self) -> Result<<I as Index<K, V>>::R> {
        let mut index = self.index.lock().unwrap();
        index.to_reader()
    }

    pub fn to_writer(&mut self) -> Result<<I as Index<K, V>>::W> {
        let mut index = self.index.lock().unwrap();
        index.to_writer()
    }

    pub fn commit<C, F>(&mut self, scanner: C, metacb: F) -> Result<()>
    where
        C: CommitIterator<K, V>,
        F: Fn(Vec<u8>) -> Vec<u8>,
    {
        let mut index = self.index.lock().unwrap();
        index.commit(scanner, metacb)
    }

    pub fn compact<F>(&mut self, cutoff: Bound<u64>, metacb: F) -> Result<usize>
    where
        F: Fn(Vec<Vec<u8>>) -> Vec<u8>,
    {
        let mut index = self.index.lock().unwrap();
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
        let mut index = self.index.lock().unwrap();
        index.validate()
    }
}

//impl<K, V> Rdms<K, V, Box<mvcc::Mvcc<K, V>>>
//where
//    K: Clone + Ord + Footprint + fmt::Debug,
//    V: Clone + Diff + Footprint,
//{
//    pub fn validate(&self) -> Result<mvcc::Stats> {
//        (&*self.index).validate()
//    }
//}

fn auto_commit<K, V, I>(index: Arc<sync::Mutex<I>>, interval: Duration)
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    I: Index<K, V>,
{
    let mut elapsed = Duration::new(0, 0);
    let initial_count = Arc::strong_count(&index);
    loop {
        if elapsed < interval {
            thread::sleep(interval - elapsed);
        }
        if Arc::strong_count(&index) < initial_count {
            break; // cascading quite,
        }

        elapsed = {
            let start = SystemTime::now();
            let mut indx = index.lock().unwrap();
            let empty: Vec<Result<Entry<K, V>>> = vec![];
            indx.commit(empty.into_iter(), convert::identity).unwrap();
            start.elapsed().ok().unwrap()
        };
    }
}
