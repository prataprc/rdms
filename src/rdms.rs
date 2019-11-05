use std::{
    ffi, fmt, marker,
    ops::Bound,
    thread,
    time::{Duration, SystemTime},
};

use crate::{
    core::{Diff, Footprint, Index, IndexIter, Result},
    llrb, mvcc,
    sync::CCMu,
    types::EmptyIter,
};

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

    commit_mu: CCMu,
    index: I,
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
        let mut index = Box::new(Rdms {
            name: name.as_ref().to_string(),

            commit_mu: CCMu::uninit(),
            index,
            _key: marker::PhantomData,
            _value: marker::PhantomData,
        });
        let ptr = unsafe {
            // transmute self as void pointer.
            Box::from_raw(&mut *index as *mut Rdms<K, V, I> as *mut ffi::c_void)
        };
        index.commit_mu = CCMu::init_with_ptr(ptr);
        Ok(index)
    }

    /// Set interval in time duration, for invoking auto commit.
    /// Calling this method will spawn an auto compaction thread.
    pub fn set_commit_interval(&mut self, interval: Duration) {
        let mu = CCMu::clone(&self.commit_mu);
        thread::spawn(move || auto_commit::<K, V, I>(mu, interval));
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

    pub fn to_metadata(&mut self) -> Result<Vec<u8>> {
        self.index.to_metadata()
    }

    pub fn to_seqno(&mut self) -> u64 {
        self.index.to_seqno()
    }

    pub fn set_seqno(&mut self, seqno: u64) {
        self.index.set_seqno(seqno)
    }

    pub fn to_reader(&mut self) -> Result<<I as Index<K, V>>::R> {
        self.index.to_reader()
    }

    pub fn to_writer(&mut self) -> Result<<I as Index<K, V>>::W> {
        self.index.to_writer()
    }

    pub fn commit(&mut self, iter: IndexIter<K, V>, meta: Vec<u8>) -> Result<isize> {
        self.index.commit(iter, meta)
    }

    pub fn compact(&mut self, cutoff: Bound<u64>) -> Result<isize> {
        self.index.compact(cutoff)
    }
}

impl<K, V> Rdms<K, V, Box<llrb::Llrb<K, V>>>
where
    K: Clone + Ord + Footprint + fmt::Debug,
    V: Clone + Diff + Footprint,
{
    pub fn validate(&self) -> Result<llrb::Stats> {
        (&*self.index).validate()
    }
}

impl<K, V> Rdms<K, V, Box<mvcc::Mvcc<K, V>>>
where
    K: Clone + Ord + Footprint + fmt::Debug,
    V: Clone + Diff + Footprint,
{
    pub fn validate(&self) -> Result<mvcc::Stats> {
        (&*self.index).validate()
    }
}

fn auto_commit<K, V, I>(ccmu: CCMu, interval: Duration)
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    I: Index<K, V>,
{
    let phantom_key: marker::PhantomData<K> = marker::PhantomData;
    let phantom_val: marker::PhantomData<V> = marker::PhantomData;

    let mut elapsed = Duration::new(0, 0);
    let initial_count = ccmu.strong_count();
    loop {
        if elapsed < interval {
            thread::sleep(interval - elapsed);
        }
        if ccmu.strong_count() < initial_count {
            break; // cascading quit.
        }

        let start = SystemTime::now();
        let rdms = unsafe {
            // unsafe
            (ccmu.get_ptr() as *mut Rdms<K, V, I>).as_mut().unwrap()
        };
        let iter = Box::new(EmptyIter {
            _phantom_key: &phantom_key,
            _phantom_val: &phantom_val,
        });
        let meta = vec![];
        rdms.commit(iter, meta).unwrap();
        elapsed = start.elapsed().ok().unwrap();
    }
}
