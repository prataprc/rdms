use std::{
    ffi, fmt, marker,
    ops::Bound,
    result,
    sync::{self},
    thread,
    time::{Duration, SystemTime},
};

use crate::{
    core::{Diff, Footprint, Index, IndexIter, PiecewiseScan, Result},
    lsm,
    scans::{self, SkipScan},
    sync::CCMu,
    types::Empty,
};

#[derive(Clone)]
struct Name(String);

impl From<String> for Name {
    fn from(name: String) -> Name {
        Name(format!("{}-backup", name))
    }
}

impl From<Name> for String {
    fn from(name: Name) -> String {
        let parts: Vec<&str> = name.0.split('-').collect();
        let name = parts[..(parts.len() - 1)].join("-");
        name
    }
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

pub struct Backup<K, V, M, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    M: Index<K, V>,
    D: Index<K, V>,
{
    dir: ffi::OsString,
    name: String,
    compact_ratio: f64,
    pw_batch: usize,

    compact_mu: CCMu,
    pair: sync::Mutex<(M, D)>,

    _phantom_key: marker::PhantomData<K>,
    _phantom_val: marker::PhantomData<V>,
}

impl<K, V, M, D> Backup<K, V, M, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    M: Index<K, V>,
    D: Index<K, V>,
    <M as Index<K, V>>::R: PiecewiseScan<K, V>,
{
    /// Default threshold between usefull data in disk and total disk
    /// footprint, below which disk backup shall be compacted.
    pub const COMPACT_RATIO: f64 = 0.5;

    pub fn new(
        dir: &ffi::OsStr, // directory path
        name: &str,
        mem: M,
        disk: D,
    ) -> Result<Box<Backup<K, V, M, D>>> {
        let mut ix = Box::new(Backup {
            dir: dir.to_os_string(),
            name: name.to_string(),
            compact_ratio: Self::COMPACT_RATIO,
            pw_batch: scans::SKIP_SCAN_BATCH_SIZE,

            compact_mu: CCMu::uninit(),
            pair: sync::Mutex::new((mem, disk)),

            _phantom_key: marker::PhantomData,
            _phantom_val: marker::PhantomData,
        });
        let ptr = unsafe {
            // transmute self as void pointer.
            Box::from_raw(&mut *ix as *mut Backup<K, V, M, D> as *mut ffi::c_void)
        };
        ix.compact_mu = CCMu::init_with_ptr(ptr);
        Ok(ix)
    }

    pub fn set_pw_batch_size(&mut self, batch: usize) {
        self.pw_batch = batch
    }

    /// Set threshold between useful data in disk and total disk
    /// footprint, below which disk backup shall be compacted.
    pub fn set_compact_ratio(&mut self, ratio: f64) {
        self.compact_ratio = ratio;
    }

    /// Set interval in time duration, for invoking disk compaction.
    pub fn set_compact_interval(&mut self, interval: Duration) {
        let mu = CCMu::clone(&self.compact_mu);
        thread::spawn(move || auto_compact::<K, V, M, D>(mu, interval));
    }
}

impl<K, V, M, D> Backup<K, V, M, D>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    M: Index<K, V> + Footprint,
    D: Index<K, V> + Footprint,
{
    fn mem_footprint(&self) -> Result<isize> {
        let pair = self.pair.lock().unwrap();
        pair.0.footprint()
    }
    fn disk_footprint(&self) -> Result<isize> {
        let pair = self.pair.lock().unwrap();
        pair.1.footprint()
    }
}

impl<K, V, M, D> Footprint for Backup<K, V, M, D>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    M: Index<K, V> + Footprint,
    D: Index<K, V> + Footprint,
{
    fn footprint(&self) -> Result<isize> {
        Ok(self.disk_footprint()? + self.mem_footprint()?)
    }
}

impl<K, V, M, D> Index<K, V> for Backup<K, V, M, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    M: Index<K, V>,
    D: Index<K, V>,
    <M as Index<K, V>>::R: PiecewiseScan<K, V>,
{
    type W = M::W;
    type R = M::R;
    type O = Empty;

    fn to_name(&self) -> String {
        self.name.clone()
    }

    fn to_root(&self) -> Empty {
        Empty
    }

    fn to_metadata(&self) -> Result<Vec<u8>> {
        let pair = self.pair.lock().unwrap();
        pair.1.to_metadata()
    }

    fn to_seqno(&self) -> u64 {
        let pair = self.pair.lock().unwrap();
        pair.0.to_seqno()
    }

    fn set_seqno(&mut self, seqno: u64) {
        let mut pair = self.pair.lock().unwrap();
        pair.0.set_seqno(seqno)
    }

    fn to_writer(&mut self) -> Result<Self::W> {
        let mut pair = self.pair.lock().unwrap();
        pair.0.to_writer()
    }

    fn to_reader(&mut self) -> Result<Self::R> {
        let mut pair = self.pair.lock().unwrap();
        pair.0.to_reader()
    }

    fn commit(&mut self, scanner: IndexIter<K, V>, meta: Vec<u8>) -> Result<usize> {
        let mut pair = self.pair.lock().unwrap();

        let within = (
            Bound::Included(pair.0.to_seqno()),
            Bound::Excluded(pair.1.to_seqno()),
        );
        let mut pw_scan = SkipScan::new(pair.0.to_reader()?, within);
        pw_scan.set_batch_size(self.pw_batch);
        let no_reverse = false;
        let scanner = lsm::y_iter(scanner, Box::new(pw_scan), no_reverse);
        pair.1.commit(scanner, meta)
    }

    fn compact(&mut self, cutoff: Bound<u64>) -> Result<usize> {
        let mut pair = self.pair.lock().unwrap();
        pair.1.compact(cutoff)
    }
}

fn auto_compact<K, V, M, D>(ccmu: CCMu, interval: Duration)
where
    K: Clone + Ord,
    V: Clone + Diff,
    M: Index<K, V>,
    D: Index<K, V>,
    <M as Index<K, V>>::R: PiecewiseScan<K, V>,
{
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
        let backup = unsafe {
            (ccmu.get_ptr() as *mut Backup<K, V, M, D>)
                .as_mut()
                .unwrap()
        };
        backup.compact(Bound::Unbounded).unwrap(); // TODO: log error
        elapsed = start.elapsed().ok().unwrap();
    }
}
