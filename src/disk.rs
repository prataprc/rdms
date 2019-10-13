use std::{
    borrow::Borrow,
    ffi, fs, mem,
    ops::{Bound, RangeBounds},
    path,
    sync::{
        self,
        atomic::{AtomicBool, Ordering::SeqCst},
        Arc,
    },
};

use crate::core::{Diff, DurableIndex, Entry, Footprint};
use crate::core::{IndexIter, Reader, Result, ScanIter, Serialize};

const NLEVELS: usize = 16;

struct Levels<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    levels: [State<K, V>; NLEVELS],
    reloads: Vec<Arc<AtomicBool>>,
}

impl<K, V> Levels<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    fn new() -> Levels<K, V> {
        Levels {
            levels: Self::new_initial_states(),
            reloads: vec![],
        }
    }

    fn new_initial_states() -> [State<K, V>; NLEVELS]
    where
        K: Clone + Ord + Serialize,
        V: Clone + Diff + Serialize,
        <V as Diff>::D: Serialize,
    {
        [
            State::None,
            State::None,
            State::None,
            State::None,
            State::None,
            State::None,
            State::None,
            State::None,
            State::None,
            State::None,
            State::None,
            State::None,
            State::None,
            State::None,
            State::None,
            State::None,
        ]
    }

    fn put(&mut self, levels: [State<K, V>; NLEVELS]) {
        self.levels = levels;
        for reload in self.reloads.iter() {
            reload.store(true, SeqCst)
        }
    }

    fn get(&self) -> [State<K, V>; NLEVELS] {
        self.levels.clone()
    }

    fn new_reader(&mut self) -> ([State<K, V>; NLEVELS], Arc<AtomicBool>) {
        let levels = self.levels.clone();
        let reload = Arc::new(AtomicBool::new(false));
        self.reloads.push(Arc::clone(&reload));
        (levels, reload)
    }
}

impl<K, V> Footprint for Levels<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    fn footprint(&self) -> isize {
        self.levels
            .iter()
            .map(|level| match level {
                State::Active(level) => level.footprint(),
                State::Compact(level) => level.footprint(),
                State::Flush(level) => level.footprint(),
                State::None => 0,
            })
            .sum()
    }
}

struct Robt<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    dir: ffi::OsString,
    name: String,
    mu: sync::Mutex<Levels<K, V>>,
}

impl<K, V> Robt<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    fn new(dir: &ffi::OsStr, name: &str) -> Result<Robt<K, V>> {
        Ok(Robt {
            dir: dir.to_os_string(),
            name: name.to_string(),
            mu: sync::Mutex::new(Levels::new()),
        })
    }

    fn open(dir: &ffi::OsStr, name: &str) -> Result<Robt<K, V>> {
        //for item in fs::read_dir(&dir)? {
        //    let file_name = item?.file_name().to_str().unwrap();
        //    let name = match Config::to_name(file_name) {
        //        None => continue
        //        Some(name) => name
        //    };
        //    let snapshot = Snapshot::open(dir, &name)?;
        //}

        Ok(Robt {
            dir: dir.to_os_string(),
            name: name.to_string(),
            mu: sync::Mutex::new(Levels::new()),
        })
    }
}

impl<K, V> Footprint for Robt<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    fn footprint(&self) -> isize {
        let levels = self.mu.lock().unwrap(); // on poison panic.
        levels.footprint()
    }
}

impl<K, V> DurableIndex<K, V> for Robt<K, V>
where
    K: Send + Sync + Clone + Ord + Footprint + Serialize,
    V: Send + Sync + Clone + Diff + Footprint + Serialize,
    <V as Diff>::D: Serialize,
{
    type R = Snapshots<K, V>;

    fn commit(&mut self, iter: ScanIter<K, V>) -> Result<usize> {
        Ok(0)
    }

    fn compact(&mut self, tombstone_purge: Bound<u64>) -> Result<()> {
        Ok(())
    }

    fn to_reader(&mut self) -> Result<Self::R> {
        let mut levels = self.mu.lock().unwrap();
        let (ls, reload) = levels.new_reader();
        Ok(Snapshots::new(ls, reload))
    }
}

struct Snapshots<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    reload: Arc<AtomicBool>,
    levels: [State<K, V>; NLEVELS],
}

impl<K, V> Snapshots<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    fn new(
        levels: [State<K, V>; NLEVELS], // reference levels
        reload: Arc<AtomicBool>,
    ) -> Snapshots<K, V> {
        Snapshots { reload, levels }
    }
}

impl<K, V> Reader<K, V> for Snapshots<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    fn get<Q>(&self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        panic!("to be implemented")
    }

    fn iter(&self) -> Result<IndexIter<K, V>> {
        panic!("to be implemented")
    }

    fn range<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        panic!("to be implemented")
    }

    fn reverse<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        panic!("to be implemented")
    }

    fn get_with_versions<Q>(&self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        panic!("to be implemented")
    }

    fn iter_with_versions(&self) -> Result<IndexIter<K, V>> {
        panic!("to be implemented")
    }

    fn range_with_versions<'a, R, Q>(&'a self, r: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        panic!("to be implemented")
    }

    fn reverse_with_versions<'a, R, Q>(&'a self, r: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        panic!("to be implemented")
    }
}

#[derive(Clone)]
enum State<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    Flush(Level<K, V>),
    Compact(Level<K, V>),
    Active(Level<K, V>),
    None,
}

impl<K, V> Default for State<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    fn default() -> State<K, V> {
        State::None
    }
}

#[derive(Default)]
struct Level<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    dir: ffi::OsString,
    index_file: Arc<String>,
    vlog_file: Option<Arc<String>>,
    snapshot: Option<mem::ManuallyDrop<Snapshot<K, V>>>,
}

impl<K, V> Level<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    fn new(
        dir: &ffi::OsStr, // path to index/vlog files
        index_file: Arc<String>,
        vlog_file: Option<Arc<String>>,
    ) -> Level<K, V> {
        Level {
            dir: dir.to_os_string(),
            index_file,
            vlog_file,
            snapshot: Default::default(),
        }
    }

    fn load_snapshot(&mut self) -> Result<()> {
        if self.snapshot.is_some() {
            panic!("snapshot already loaded")
        }
        let file_name = path::Path::new(self.index_file.as_ref())
            .file_name()
            .unwrap();
        let name = Config::to_name(file_name.to_str().unwrap()).unwrap();
        self.snapshot = Some(
            // snapshot shall be manually dropped, because the arder of drop
            // is very important, between snapshot and file-cleanup.
            mem::ManuallyDrop::new(Snapshot::open(&self.dir, &name)?),
        );
        Ok(())
    }

    fn to_file_parts(file_name: &str) -> Option<(String, usize, usize)> {
        let name = Config::to_name(file_name)?;
        let parts: Vec<&str> = name.split('-').collect();
        let mut parts = parts.into_iter();
        let name = parts.next()?;
        let level: usize = parts.next()?.parse().ok()?;
        let file_no: usize = parts.next()?.parse().ok()?;
        Some((name.to_string(), level, file_no))
    }

    fn to_index_name(&self) -> String {
        self.index_file.as_ref().clone()
    }

    fn to_vlog_name(&self) -> Option<String> {
        self.vlog_file.as_ref().map(|f| f.to_string())
    }
}

impl<K, V> Footprint for Level<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    fn footprint(&self) -> isize {
        let file: &str = self.index_file.as_ref();
        let mut footprint = fs::metadata(file).unwrap().len();
        footprint += match &self.vlog_file {
            Some(vlog_file) => {
                let file: &str = vlog_file.as_ref();
                fs::metadata(file).unwrap().len()
            }
            None => 0,
        };
        footprint.try_into().unwrap()
    }
}

impl<K, V> Clone for Level<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    fn clone(&self) -> Level<K, V> {
        if self.snapshot.is_some() {
            panic!("cannot clone a level that has an open snapshot");
        }

        Level {
            dir: self.dir.clone(),
            index_file: Arc::clone(&self.index_file),
            vlog_file: self.vlog_file.as_ref().map(|x| Arc::clone(x)),
            snapshot: None,
        }
    }
}

impl<K, V> Drop for Level<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    fn drop(&mut self) {
        // manuall drop snapshot object here.
        match self.snapshot.take() {
            Some(mut snapshot) => unsafe {
                // order of drop is important with respect to file-cleanup.
                mem::ManuallyDrop::drop(&mut snapshot)
            },
            None => (),
        }

        // and cleanup the older snapshots if there are not more references.
        if Arc::strong_count(&self.index_file) == 1 {
            let f = Config::stitch_index_file(&self.dir, &self.index_file);
            fs::remove_file(f).unwrap();
        }

        match &self.vlog_file {
            Some(vlog_file) if Arc::strong_count(&vlog_file) == 1 => {
                let f = Config::stitch_index_file(&self.dir, &vlog_file);
                fs::remove_file(f).unwrap();
            }
            Some(_) | None => (),
        }
    }
}
