use std::{
    borrow::Borrow,
    ffi, fs, mem,
    ops::{Bound, RangeBounds},
    path,
    marker,
    sync::{
        self,
        atomic::{AtomicBool, Ordering::SeqCst},
        Arc,
    },
};

use crate::core::{Diff, DurableIndex, Entry, Footprint};
use crate::core::{IndexIter, Reader, Result, ScanIter, Serialize};
use crate::error::Error;
use crate::robt::Config;

const NLEVELS: usize = 16;

#[derive(Clone)]
enum RState<K, V, D>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    D: DurableIndex,
{
    Flush(RLevel<K, V, D>),
    Compact(RLevel<K, V, D>),
    Active(RLevel<K, V, D>),
    None,
}

impl<K, V> Default for RState<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    fn default() -> RState<K, V> {
        RState::None
    }
}

struct Backup<K, V, F>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    F: IndexFactory<K, V>,
    F::I: DurableIndex,
{
    disk_factory: F,
    levels: [State<K, V>; NLEVELS],
    reloads: Vec<Arc<AtomicBool>>,
}

impl<K, V> Backup<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    fn new() -> Backup<K, V> {
        Backup {
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

impl<K, V> Footprint for Backup<K, V>
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
    mu: sync::Mutex<Backup<K, V>>,
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
            mu: sync::Mutex::new(Backup::new()),
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
            mu: sync::Mutex::new(Backup::new()),
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

impl<K, V> DurableIndex<K, V> for Dgm<K, V> {
    type R = DgmReader<K, V>;

    fn commit(&mut self, iter: ScanIter<K, V>) -> Result<usize> {
        Ok(0)
    }

    fn compact(&mut self) -> Result<()> {
        Ok(())
    }

    fn to_reader(&mut self) -> Result<Self::R> {
        let mut levels = self.mu.lock().unwrap();
        let (ls, reload) = levels.new_reader();
        Ok(DgmReader::new(ls, reload))
    }
}

struct DgmReader<K, V, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    id: usize,
    _refn: Arc<u32>;
    dgm: Box<ffi::c_void>, // Box<Llrb<K, V>>
    phantom_disk: marker::PhantomData<D>,
}

impl<K,V,D> Drop for DgmReader<K,V,D>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn drop(&mut self) {
        Box::leak(self.dgm);
    }
}

impl<K,V,D> AsRef<Dgm<K,V,D>> for DgmReader<K,V,D>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn as_ref(&self) -> &Dgm<K,V,D> {
        // transmute void pointer to mutable reference into index.
        let index_ptr = self.dgm.as_ref();
        let index_ptr = index_ptr as *const ffi::c_void;
        (index_ptr as *const Dgm<K, V, D>).as_ref().unwrap()
    }
}

impl<K, V> DgmReader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn new(id: usize, dgm: Box<ffi::c_void>, _refn: Arc<u32>) -> DgmReader<K, V> {
        DgmReader { id, dgm, _refn, phantom_disk: marker::PhantomData }
    }
}

impl<K, V> Reader<K, V> for DgmReader<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn get<Q>(&self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let rlevels = self.get_snapshots();

        for rlevel in self.rlevels.iter() {
            let rlevel = match rlevel {
                RState::Flush(rlevel) => rlevel,
                RState::Compact(rlevel) => rlevel,
                RState::Active(rlevel) => rlevel,
                RState::None => continue,
            };
            match rlevel.reader.get(key) {
                Ok(entry) => return Ok(entry),
                Err(Error::KeyNotFound) => continue
                Err(err) => return Err(err),
            }
        }
    }

    fn iter(&self) -> Result<IndexIter<K, V>> {
        let iters: Vec<IndexIter<K,V>> = vec![];
        for rlevel in self.rlevels.iter() {
            let rlevel = match rlevel {
                RState::Flush(rlevel) => rlevel,
                RState::Compact(rlevel) => rlevel,
                RState::Active(rlevel) => rlevel,
                RState::None => continue,
            };
            iters.push(rlevel.reader.iter()?);
        }
        match iters.len() {
            0 => {
                let entries: Vec<Result<Entry<K,V>>> = vec![];
                Box::new(entries.into_iter())
            }
            1 => iters[0],
            _ => {
                let (mut iter, reverse) = (iters[0], true);
                for older = iters[1..].into_iter() {
                    iter = y_iter(iter, older, reverse);
                }
                Ok(iter)
            }
        }
    }

    fn range<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let iters: Vec<IndexIter<K,V>> = vec![];
        for rlevel in self.rlevels.iter() {
            let rlevel = match rlevel {
                RState::Flush(rlevel) => rlevel,
                RState::Compact(rlevel) => rlevel,
                RState::Active(rlevel) => rlevel,
                RState::None => continue,
            };
            iters.push(rlevel.reader.range(range)?);
        }
        match iters.len() {
            0 => {
                let entries: Vec<Result<Entry<K,V>>> = vec![];
                Box::new(entries.into_iter())
            }
            1 => iters[0],
            _ => {
                let (mut iter, reverse) = (iters[0], true);
                for older = iters[1..].into_iter() {
                    iter = y_iter(iter, older, reverse);
                }
                Ok(iter)
            }
        }
    }

    fn reverse<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let iters: Vec<IndexIter<K,V>> = vec![];
        for rlevel in self.rlevels.iter() {
            let rlevel = match rlevel {
                RState::Flush(rlevel) => rlevel,
                RState::Compact(rlevel) => rlevel,
                RState::Active(rlevel) => rlevel,
                RState::None => continue,
            };
            iters.push(rlevel.reader.reverse(range)?);
        }
        match iters.len() {
            0 => {
                let entries: Vec<Result<Entry<K,V>>> = vec![];
                Box::new(entries.into_iter())
            }
            1 => iters[0],
            _ => {
                let (mut iter, reverse) = (iters[0], true);
                for older = iters[1..].into_iter() {
                    iter = y_iter(iter, older, reverse);
                }
                Ok(iter)
            }
        }
    }

    fn get_with_versions<Q>(&self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut entries: Vec<Entry<K,V>> = vec![]
        for rlevel in self.rlevels.iter() {
            let rlevel = match rlevel {
                RState::Flush(rlevel) => rlevel,
                RState::Compact(rlevel) => rlevel,
                RState::Active(rlevel) => rlevel,
                RState::None => continue,
            };
            match rlevel.reader.get_versions(key) {
                Ok(entry) => entries.push(entry),
                Err(Error::KeyNotFound) => continue,
                Err(err) => return Err(err),
            }
        }
        match entries.len() {
            0 => Err(Error::KeyNotFound),
            1 => Ok(entries[0]),
            _ => {
                let entry = entries[0];
                for older in entries[1..].into_iter() {
                    entry.flush_merge(older)
                }
                Ok(entry)
            }
        }
    }

    fn iter_with_versions(&self) -> Result<IndexIter<K, V>> {
        let iters: Vec<IndexIter<K,V>> = vec![];
        for rlevel in self.rlevels.iter() {
            let rlevel = match rlevel {
                RState::Flush(rlevel) => rlevel,
                RState::Compact(rlevel) => rlevel,
                RState::Active(rlevel) => rlevel,
                RState::None => continue,
            };
            iters.push(rlevel.reader.iter_with_versions()?);
        }
        match iters.len() {
            0 => {
                let entries: Vec<Result<Entry<K,V>>> = vec![];
                Box::new(entries.into_iter())
            }
            1 => iters[0],
            _ => {
                let (mut iter, reverse) = (iters[0], true);
                for older = iters[1..].into_iter() {
                    iter = y_iter_versions(iter, older, reverse);
                }
                Ok(iter)
            }
        }
    }

    fn range_with_versions<'a, R, Q>(&'a self, r: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let iters: Vec<IndexIter<K,V>> = vec![];
        for rlevel in self.rlevels.iter() {
            let rlevel = match rlevel {
                RState::Flush(rlevel) => rlevel,
                RState::Compact(rlevel) => rlevel,
                RState::Active(rlevel) => rlevel,
                RState::None => continue,
            };
            iters.push(rlevel.reader.range_with_versions(range)?);
        }
        match iters.len() {
            0 => {
                let entries: Vec<Result<Entry<K,V>>> = vec![];
                Box::new(entries.into_iter())
            }
            1 => iters[0],
            _ => {
                let (mut iter, reverse) = (iters[0], true);
                for older = iters[1..].into_iter() {
                    iter = y_iter_versions(iter, older, reverse);
                }
                Ok(iter)
            }
        }
    }

    fn reverse_with_versions<'a, R, Q>(&'a self, r: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let iters: Vec<IndexIter<K,V>> = vec![];
        for rlevel in self.rlevels.iter() {
            let rlevel = match rlevel {
                RState::Flush(rlevel) => rlevel,
                RState::Compact(rlevel) => rlevel,
                RState::Active(rlevel) => rlevel,
                RState::None => continue,
            };
            iters.push(rlevel.reader.reverse_with_versions(range)?);
        }
        match iters.len() {
            0 => {
                let entries: Vec<Result<Entry<K,V>>> = vec![];
                Box::new(entries.into_iter())
            }
            1 => iters[0],
            _ => {
                let (mut iter, reverse) = (iters[0], true);
                for older = iters[1..].into_iter() {
                    iter = y_iter_versions(iter, older, reverse);
                }
                Ok(iter)
            }
        }
    }
}

// Holds a single snapshot for a disk type D.
#[derive(Default)]
struct RLevel<K, V, D>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    D: DurableIndex<K, V>,
{
    dir: ffi::OsString,
    index_file: Arc<String>,        // full path/file_name
    vlog_file: Option<Arc<String>>, // full path/file_name
    reader: mem::ManuallyDrop<D::R>,
}

impl<K, V, D> RLevel<K, V, D>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    D: DurableIndex<K,V>,
{
    fn new(
        dir: &ffi::OsStr, // path to index/vlog files
        index_file: Arc<String>,
        vlog_file: Option<Arc<String>>,
        reader: D::R,
    ) -> RLevel<K, V> {
        RLevel {
            dir: dir.to_os_string(),
            index_file,
            vlog_file,
            reader: ManuallyDrop::new(reader),
        }
    }

    // TODO: cleanup afterwards.
    fn load_snapshot(&mut self) -> Result<()> {
        let file_name = path::Path::new(self.index_file.as_ref())
            .file_name()
            .ok_or(Err(Error::InvalidFile("no file name".to_string())));
        let name = match file_name.to_str() {
            Some(file_name) => match Config::to_name(file_name) {
                Some(name) => Ok(name),
                None => {
                    let msg = "robt not an index file".to_string();
                    Err(Error::InvalidFile(msg))
                }
            },
            None => {
                let msg = format!("robt invalid index file {:?}", file_name);
                Err(Error::InvalidFile(msg))
            }
        }?;
        mem::ManuallyDrop::new(Snapshot::open(&self.dir, &name)?);
        Ok(())
    }

    fn to_file_parts(file_name: &str) -> Option<(String, usize, usize)> {
        let parts = {
            let name = Config::to_name(file_name)?;
            let parts: Vec<&str> = name.split('-').collect();
            parts.into_iter()
        };
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

impl<K, V, D> Footprint for RLevel<K, V, D>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    D: DurableIndex<K, V>,
{
    fn footprint(&self) -> Result<isize> {
        let mut footprint = {
            let file: &str = self.index_file.as_ref();
            fs::metadata(file)?.len()
        };
        footprint += match &self.vlog_file {
            Some(vlog_file) => {
                let file: &str = vlog_file.as_ref();
                fs::metadata(file)?.len()
            }
            None => 0,
        };
        Ok(footprint.try_into()?)
    }
}

impl<K, V, D> Drop for RLevel<K, V, D>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    D: DurableIndex<K, V>,
{
    fn drop(&mut self) {
        // manually drop reader object here.
        // order of drop is important with respect to file-cleanup.
        unsafe { mem::ManuallyDrop::drop(&mut self.reader) }

        // and cleanup the older snapshots if there are no more references.
        if Arc::strong_count(&self.index_file) == 1 {
            let f = Config::stitch_index_file(&self.dir, &self.index_file);
            fs::remove_file(f).ok();
        }

        match &self.vlog_file {
            Some(vlog_file) if Arc::strong_count(&vlog_file) == 1 => {
                let f = Config::stitch_index_file(&self.dir, &vlog_file);
                fs::remove_file(f).ok();
            }
            Some(_) | None => (),
        }
    }
}
