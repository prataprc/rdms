use std::{
    ffi, marker, thread,
    time::{Duration, SystemTime},
};

use crate::{
    core::{Diff, Footprint, Index, IndexIter, Result},
    sync::CCMu,
    types::EmptyIter,
};

/// Default commit interval, in seconds, for auto-commit.
pub const COMMIT_INTERVAL: usize = 30 * 60; // 30 minutes

/// Default compact interval, in seconds, for auto-compact.
/// If initialized to ZERO, then auto-compact is disabled and
/// applications are expected to manually call the compact() method.
pub const COMPACT_INTERVAL: usize = 120 * 60; // 2 hours

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
    pub fn new<S>(name: S, index: I) -> Result<Rdms<K, V, I>>
    where
        S: AsRef<str>,
    {
        Ok(Rdms {
            name: name.as_ref().to_string(),

            commit_mu: CCMu::uninit(),
            index,
            _key: marker::PhantomData,
            _value: marker::PhantomData,
        })
    }

    // Set interval in time duration, for invoking auto commit.
    pub fn set_commit_interval(&mut self, interval: Duration) {
        let ptr = unsafe {
            // transmute self as void pointer.
            Box::from_raw(self as *mut Rdms<K, V, I> as *mut ffi::c_void)
        };
        self.commit_mu = CCMu::init_with_ptr(ptr);
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

    pub fn commit(&mut self, iter: IndexIter<K, V>, meta: Vec<u8>) -> Result<()> {
        self.index.commit(iter, meta)
    }

    pub fn compact(&mut self) -> Result<()> {
        self.index.compact()
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
    loop {
        if elapsed < interval {
            thread::sleep(interval - elapsed);
        }
        let rdms = match ccmu.start_op() {
            (false, _) => break,
            (true, ptr) => unsafe {
                // unsafe type cast
                (ptr as *mut Rdms<K, V, I>).as_mut().unwrap()
            },
        };

        let start = SystemTime::now();
        let iter = Box::new(EmptyIter {
            _phantom_key: &phantom_key,
            _phantom_val: &phantom_val,
        });
        let meta = vec![];
        rdms.commit(iter, meta).unwrap(); // TODO: log error
        elapsed = start.elapsed().ok().unwrap();

        ccmu.fin_op()
    }
}
