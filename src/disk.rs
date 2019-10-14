use std::{
    borrow::Borrow,
    convert::TryInto,
    ffi, fs, marker, mem,
    ops::RangeBounds,
    sync::{self, Arc},
};

use crate::core::{Diff, DiskIndexFactory, DurableIndex, Entry, Footprint};
use crate::core::{IndexIter, Reader, Result, Serialize};
use crate::error::Error;
use crate::lsm;

const NLEVELS: usize = 16;

struct Snapshots<K, V, D>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    D: DurableIndex<K, V>,
{
    mu: sync::Mutex<u32>,
    rlevels: RStates<K, V, D>,
}

impl<K, V, D> Snapshots<K, V, D>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    D: DurableIndex<K, V>,
{
    fn new(rlevels: RStates<K, V, D>) -> Snapshots<K, V, D> {
        Snapshots {
            mu: sync::Mutex::new(0xC0FFEE),
            rlevels,
        }
    }

    fn get_rlevels(&self) -> (sync::MutexGuard<u32>, &RStates<K, V, D>) {
        (self.mu.lock().unwrap(), &self.rlevels)
    }

    fn set_rlevels(&mut self, rlevels: RStates<K, V, D>) {
        let _guard = self.mu.lock();
        self.rlevels = rlevels; // TODO: check, old array to be dropped here
    }
}

struct Dgm<K, V, F>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    F: DiskIndexFactory<K, V>,
    F::I: DurableIndex<K, V>,
{
    disk_factory: F,

    mu: sync::Mutex<u32>,
    levels: RStates<K, V, F::I>,
    rsnapshots: Vec<Snapshots<K, V, F::I>>,
}

impl<K, V, F> Dgm<K, V, F>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    F: DiskIndexFactory<K, V>,
    F::I: DurableIndex<K, V>,
{
    fn new(factory: F) -> Dgm<K, V, F> {
        Dgm {
            disk_factory: factory,
            mu: sync::Mutex::new(0xC0FFEE),
            levels: Default::default(),
            rsnapshots: Default::default(),
        }
    }

    fn set_new_levels(&mut self, levels: RStates<K, V, F::I>) {
        let _guard = self.mu.lock().unwrap();

        for i in 0..self.rsnapshots.len() {
            let s_levels = levels.clone();
        }
    }
}

impl<K, V, F> Footprint for Dgm<K, V, F>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    F: DiskIndexFactory<K, V>,
    F::I: DurableIndex<K, V>,
{
    fn footprint(&self) -> Result<isize> {
        let mut footprint: isize = Default::default();
        for level in self.levels.iter() {
            footprint += match level {
                RState::Active(level) => level.footprint()?,
                RState::Compact(level) => level.footprint()?,
                RState::Flush(level) => level.footprint()?,
                RState::None => 0,
            };
        }
        Ok(footprint)
    }
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

struct DgmReader<K, V, F>
where
    K: Clone + Ord,
    V: Clone + Diff,
    F: DiskIndexFactory<K, V>,
    F::I: DurableIndex<K, V>,
{
    id: usize,
    dgm: Option<Box<ffi::c_void>>, // Box<Dgm<K, V>>

    phantom_key: marker::PhantomData<K>,
    phantom_val: marker::PhantomData<V>,
    phantom_factory: marker::PhantomData<F>,
}

impl<K, V, F> Drop for DgmReader<K, V, F>
where
    K: Clone + Ord,
    V: Clone + Diff,
    F: DiskIndexFactory<K, V>,
    F::I: DurableIndex<K, V>,
{
    fn drop(&mut self) {
        Box::leak(self.dgm.take().unwrap());
    }
}

impl<K, V, F> DgmReader<K, V, F>
where
    K: Clone + Ord,
    V: Clone + Diff,
    F: DiskIndexFactory<K, V>,
    F::I: DurableIndex<K, V>,
{
    fn new(id: usize, dgm: Box<ffi::c_void>) -> DgmReader<K, V, F> {
        DgmReader {
            id,
            dgm: Some(dgm),
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
            phantom_factory: marker::PhantomData,
        }
    }
}

impl<K, V, F> AsRef<Dgm<K, V, F>> for DgmReader<K, V, F>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    F: DiskIndexFactory<K, V>,
    F::I: DurableIndex<K, V>,
{
    fn as_ref(&self) -> &Dgm<K, V, F> {
        // transmute void pointer to mutable reference into index.
        let index_ptr = self.dgm.as_ref().unwrap().as_ref();
        let index_ptr = index_ptr as *const ffi::c_void;
        unsafe { (index_ptr as *const Dgm<K, V, F>).as_ref().unwrap() }
    }
}

impl<K, V, F> Reader<K, V> for DgmReader<K, V, F>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize + From<<V as Diff>::D> + Footprint,
    <V as Diff>::D: Serialize,
    F: DiskIndexFactory<K, V>,
    F::I: DurableIndex<K, V>,
{
    fn get<Q>(&self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let dgm: &Dgm<K, V, F> = self.as_ref();
        let (_guard, rlevels) = dgm.rsnapshots[self.id].get_rlevels();

        for rlevel in rlevels.iter() {
            let rlevel = match rlevel {
                RState::Active(level) => rlevel,
                RState::Compact(level) => rlevel,
                RState::Flush(level) => rlevel,
                RState::None => continue,
            };
            match rlevel.s.as_ref().unwrap().get(key) {
                Ok(entry) => return Ok(entry),
                Err(Error::KeyNotFound) => continue,
                Err(err) => return Err(err),
            }
        }
        Err(Error::KeyNotFound)
    }

    fn iter(&self) -> Result<IndexIter<K, V>> {
        let dgm: &Dgm<K, V, F> = self.as_ref();
        let (_guard, rlevels) = dgm.rsnapshots[self.id].get_rlevels();

        let mut iters: Vec<IndexIter<K, V>> = vec![];
        for rlevel in rlevels.iter() {
            let rlevel = match rlevel {
                RState::Active(level) => rlevel,
                RState::Compact(level) => rlevel,
                RState::Flush(level) => rlevel,
                RState::None => continue,
            };
            iters.push(rlevel.s.as_ref().unwrap().iter()?);
        }

        match iters.len() {
            0 => {
                let entries: Vec<Result<Entry<K, V>>> = vec![];
                Ok(Box::new(entries.into_iter()))
            }
            1 => Ok(iters.remove(0)),
            _ => {
                let (mut iter, reverse) = (iters.remove(0), true);
                for older in iters.drain(..) {
                    iter = lsm::y_iter(iter, older, reverse);
                }
                Ok(iter)
            }
        }
    }

    fn range<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let dgm: &Dgm<K, V, F> = self.as_ref();
        let (_guard, rlevels) = dgm.rsnapshots[self.id].get_rlevels();

        let mut iters: Vec<IndexIter<K, V>> = vec![];
        for rlevel in rlevels.iter() {
            let rlevel = match rlevel {
                RState::Active(level) => rlevel,
                RState::Compact(level) => rlevel,
                RState::Flush(level) => rlevel,
                RState::None => continue,
            };
            iters.push(rlevel.s.as_ref().unwrap().range(range.clone())?);
        }

        match iters.len() {
            0 => {
                let entries: Vec<Result<Entry<K, V>>> = vec![];
                Ok(Box::new(entries.into_iter()))
            }
            1 => Ok(iters.remove(0)),
            _ => {
                let (mut iter, reverse) = (iters.remove(0), true);
                for older in iters.drain(..) {
                    iter = lsm::y_iter(iter, older, reverse);
                }
                Ok(iter)
            }
        }
    }

    fn reverse<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let dgm: &Dgm<K, V, F> = self.as_ref();
        let (_guard, rlevels) = dgm.rsnapshots[self.id].get_rlevels();

        let mut iters: Vec<IndexIter<K, V>> = vec![];
        for rlevel in rlevels.iter() {
            let rlevel = match rlevel {
                RState::Active(level) => rlevel,
                RState::Compact(level) => rlevel,
                RState::Flush(level) => rlevel,
                RState::None => continue,
            };
            iters.push(rlevel.s.as_ref().unwrap().reverse(range.clone())?);
        }

        match iters.len() {
            0 => {
                let entries: Vec<Result<Entry<K, V>>> = vec![];
                Ok(Box::new(entries.into_iter()))
            }
            1 => Ok(iters.remove(0)),
            _ => {
                let (mut iter, reverse) = (iters.remove(0), true);
                for older in iters.drain(..) {
                    iter = lsm::y_iter(iter, older, reverse);
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
        let dgm: &Dgm<K, V, F> = self.as_ref();
        let (_guard, rlevels) = dgm.rsnapshots[self.id].get_rlevels();

        let mut entries: Vec<Entry<K, V>> = vec![];
        for rlevel in rlevels.iter() {
            let rlevel = match rlevel {
                RState::Active(level) => rlevel,
                RState::Compact(level) => rlevel,
                RState::Flush(level) => rlevel,
                RState::None => continue,
            };
            match rlevel.s.as_ref().unwrap().get_with_versions(key) {
                Ok(entry) => entries.push(entry),
                Err(Error::KeyNotFound) => continue,
                Err(err) => return Err(err),
            }
        }

        match entries.len() {
            0 => Err(Error::KeyNotFound),
            1 => Ok(entries.remove(0)),
            _ => {
                let mut entry = entries.remove(0);
                for older in entries.drain(..) {
                    entry = entry.flush_merge(older);
                }
                Ok(entry)
            }
        }
    }

    fn iter_with_versions(&self) -> Result<IndexIter<K, V>> {
        let dgm: &Dgm<K, V, F> = self.as_ref();
        let (_guard, rlevels) = dgm.rsnapshots[self.id].get_rlevels();

        let mut iters: Vec<IndexIter<K, V>> = vec![];
        for rlevel in rlevels.iter() {
            let rlevel = match rlevel {
                RState::Active(level) => rlevel,
                RState::Compact(level) => rlevel,
                RState::Flush(level) => rlevel,
                RState::None => continue,
            };
            iters.push(rlevel.s.as_ref().unwrap().iter_with_versions()?);
        }

        match iters.len() {
            0 => {
                let entries: Vec<Result<Entry<K, V>>> = vec![];
                Ok(Box::new(entries.into_iter()))
            }
            1 => Ok(iters.remove(0)),
            _ => {
                let (mut iter, reverse) = (iters.remove(0), true);
                for older in iters.drain(..) {
                    iter = lsm::y_iter_versions(iter, older, reverse);
                }
                Ok(iter)
            }
        }
    }

    fn range_with_versions<'a, R, Q>(&'a self, r: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let dgm: &Dgm<K, V, F> = self.as_ref();
        let (_guard, rlevels) = dgm.rsnapshots[self.id].get_rlevels();

        let mut iters: Vec<IndexIter<K, V>> = vec![];
        for rlevel in rlevels.iter() {
            let rlevel = match rlevel {
                RState::Active(level) => rlevel,
                RState::Compact(level) => rlevel,
                RState::Flush(level) => rlevel,
                RState::None => continue,
            };
            iters.push(rlevel.s.as_ref().unwrap().range_with_versions(r.clone())?);
        }

        match iters.len() {
            0 => {
                let entries: Vec<Result<Entry<K, V>>> = vec![];
                Ok(Box::new(entries.into_iter()))
            }
            1 => Ok(iters.remove(0)),
            _ => {
                let (mut iter, reverse) = (iters.remove(0), true);
                for older in iters.drain(..) {
                    iter = lsm::y_iter_versions(iter, older, reverse);
                }
                Ok(iter)
            }
        }
    }

    fn reverse_with_versions<'a, R, Q>(&'a self, r: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let dgm: &Dgm<K, V, F> = self.as_ref();
        let (_guard, rlevels) = dgm.rsnapshots[self.id].get_rlevels();

        let mut iters: Vec<IndexIter<K, V>> = vec![];
        for rlevel in rlevels.iter() {
            let rlevel = match rlevel {
                RState::Active(level) => rlevel,
                RState::Compact(level) => rlevel,
                RState::Flush(level) => rlevel,
                RState::None => continue,
            };
            iters.push(
                rlevel
                    .s
                    .as_ref()
                    .unwrap()
                    .reverse_with_versions(r.clone())?,
            );
        }

        match iters.len() {
            0 => {
                let entries: Vec<Result<Entry<K, V>>> = vec![];
                Ok(Box::new(entries.into_iter()))
            }
            1 => Ok(iters.remove(0)),
            _ => {
                let (mut iter, reverse) = (iters.remove(0), true);
                for older in iters.drain(..) {
                    iter = lsm::y_iter_versions(iter, older, reverse);
                }
                Ok(iter)
            }
        }
    }
}

type RStates<K, V, D> = [RState<K, V, D>; NLEVELS];

#[derive(Clone)]
enum RState<K, V, D>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    D: DurableIndex<K, V>,
{
    Flush(RLevel<K, V, D>),
    Compact(RLevel<K, V, D>),
    Active(RLevel<K, V, D>),
    None,
}

impl<K, V, D> Default for RState<K, V, D>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    D: DurableIndex<K, V>,
{
    fn default() -> RState<K, V, D> {
        RState::None
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
    index_file: Arc<ffi::OsString>,        // full path/file_name
    vlog_file: Option<Arc<ffi::OsString>>, // full path/file_name
    s: Option<mem::ManuallyDrop<D::R>>,
}

impl<K, V, D> RLevel<K, V, D>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    D: DurableIndex<K, V>,
{
    fn new(
        dir: &ffi::OsStr, // path to index/vlog files
        index_file: Arc<String>,
        vlog_file: Option<Arc<String>>,
        s: D::R,
    ) -> RLevel<K, V, D> {
        RLevel {
            dir: dir.to_os_string(),
            index_file,
            vlog_file,
            s: Some(mem::ManuallyDrop::new(s)),
        }
    }

    fn open(&mut self) -> Result<Robt<K, V>> {
        let index_file = {
            let index_file: &ffi::OsString = self.index_file.as_ref();
            match path::Path::new(index_file).file_name() {
                Ok(file_name) => Ok(file_name),
                Err(_) => {
                    let msg = format!("no file name found in {:?}", index_file);
                    Err(Error::InvalidFile(msg))
                }
            }
        }?;
        let name = match index_file.to_str() {
            Some(index_file) => match Config::to_name(index_file) {
                Some(name) => Ok(name),
                None => {
                    let msg = "not an index file".to_string();
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

    //fn to_file_parts(file_name: &str) -> Option<(String, usize, usize)> {
    //    let mut parts = {
    //        let name = Config::to_name(file_name)?;
    //        let parts: Vec<&str> = name.split('-').collect();
    //        parts.into_iter()
    //    };
    //    let name = parts.next()?;
    //    let level: usize = parts.next()?.parse().ok()?;
    //    let file_no: usize = parts.next()?.parse().ok()?;
    //    Some((name.to_string(), level, file_no))
    //}

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
        let mut footprint = fs::metadata(self.index_file.as_ref())?.len();
        footprint += match &self.vlog_file {
            Some(vlog_file) => fs::metadata(vlog_file.as_ref())?.len(),
            None => 0,
        };
        Ok(footprint.try_into().unwrap())
    }
}

impl<K, V, D> Drop for RLevel<K, V, D>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    D: DurableIndex<K, V>,
{
    fn drop(&mut self) {
        // manually drop s object here.
        // order of drop is important with respect to file-cleanup.
        unsafe { mem::ManuallyDrop::drop(&mut self.s.take().unwrap()) }

        // and cleanup the older snapshots if there are no more references.
        if Arc::strong_count(&self.index_file) == 1 {
            fs::remove_file(Arc::get_mut(&mut self.index_file).unwrap()).ok();
        }
        match &mut self.vlog_file {
            Some(vlog_file) if Arc::strong_count(vlog_file) == 1 => {
                fs::remove_file(Arc::get_mut(vlog_file).unwrap()).ok();
            }
            Some(_) | None => (),
        }
    }
}

impl<K, V, D> Clone for RLevel<K, V, D>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    D: DurableIndex<K, V>,
{
    fn clone(&self) -> RLevel<K, V, D> {
        let vlog_file = self.vlog_file.as_ref().map(|x| Arc::clone(&x));
        RLevel {
            dir: self.dir.clone(),
            index_file: Arc::clone(&self.index_file),
            vlog_file,
            s: None,
        }
    }
}
