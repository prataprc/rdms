use std::{
    borrow::Borrow,
    convert::TryInto,
    ffi, fs, marker, mem,
    ops::RangeBounds,
    sync::{self, Arc},
};

use crate::core::{Diff, DiskIndexFactory, DurableIndex, Entry, Footprint};
use crate::core::{IndexIter, Reader, Result, Serialize};
use crate::lsm;
use crate::{error::Error, panic::Panic};

const NLEVELS: usize = 16;

#[derive(Clone)]
enum Snapshot<K, V, D>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    D: DurableIndex<K, V>,
{
    Flush(D),
    Compact(D),
    Active(D),
    Dead(String),
    None,
}

impl <K,V,D> Snapshot {
    fn to_name(name: &str, level: usize, file_no: usize) -> String {
        format!("{}-{}-{}", name, level, file_no);
    }

    fn to_parts(full_name: &str) -> Option<(String, usize, usize)> {
        let mut parts = {
            let parts: Vec<&str> = fill_name.split('-').collect();
            parts.into_iter()
        };
        let name = parts.next()?;
        let level: usize = parts.next()?.parse().ok()?;
        let file_no: usize = parts.next()?.parse().ok()?;
        Some((name.to_string(), level, file_no))
    }
}

impl<K, V, D> Default for Snapshot<K, V, D>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    D: DurableIndex<K, V>,
{
    fn default() -> Snapshot<K, V, D> {
        Snapshot::None
    }
}

struct CommitData<K, V, D>
    D: DurableIndex<K, V>,
{
    d1: Option<(usize, D::R)>,
    disk: (usize, D)
}

struct Dgm<K, V, F>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    F: DiskIndexFactory<K, V>,
    F::I: DurableIndex<K, V>,
{
    dir: ffi::OsString,
    name: String,
    mem_ratio: f64,
    disk_ratio: f64,
    disk_factory: F,

    mu: sync::Mutex<u32>,
    levels: Vec<Snapshot<F::I>>, // snapshots
    readers: Vec<Vec<F::I::R>>,
}

impl<K, V, F> Dgm<K, V, F>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    F: DiskIndexFactory<K, V>,
    F::I: DurableIndex<K, V>,
{
    const MEM_RATIO: f64 = 0.5;
    const DISK_RATIO: f64 = 0.5;

    pub fn new(
        dir: &ffi::OsStr, // directory path
        name: &str, factory: F) -> Result<Dgm<K, V, F>> {
        fs::remove_dir_all(dir)?;
        fs::create_dir_all(dir)?;

        Dgm {
            dir: dir.to_os_string(),
            name: name.to_string(),
            mem_ratio: MEM_RATIO,
            disk_ratio: DISK_RATIO,
            disk_factory: factory,

            mu: sync::Mutex::new(0xC0FFEE),
            levels: Default::default(),
            readers: Default::default(),
        }
    }

    pub fn open(dir: &ffi::OsStr, // directory path
    name: &str, factory: F) -> Result<Dgm<K, V, F>> {
        let levels = vec![];
        for item in fs::read_dir(dir)? {
            match factory::open(dir, item?)? {
                Err(_) => continue, // TODO how to handle this error
                Ok(index) => levels.push(index),
            }
        }

        Dgm {
            dir: dir.to_os_string(),
            name: name.to_string(),
            mem_ratio: MEM_RATIO,
            disk_ratio: DISK_RATIO,
            disk_factory: factory,

            mu: sync::Mutex::new(0xC0FFEE),
            levels,
            readers: Default::default(),
        }
    }

    pub fn set_mem_ratio(&mut self, ratio: f64) {
        self.mem_ratio = ratio
    }

    pub fn set_disk_ratio(&mut self, ratio: f64) {
        self.disk_ratio = ratio
    }

    fn take_readers(&mut self, id: usize) -> Vec<F::I::R> {
        let _guard = self.mu.lock().unwrap();

        let readers: Vec<F::I::R> = self.readers[id].drain(..).collect();
        readers
    }

    fn reset_readers(&mut self, id: usize) {
        let _guard = self.mu.lock().unwrap();

        let _old_readers = self.take_readers();
        for level in self.levels.iter() {
            self.readers[i].push(level.to_reader()?);
        }
    }
}

impl<K, V, F> Footprint for Dgm<K, V, F>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    F: DiskIndexFactory<K, V>,
    F::I: DurableIndex<K, V> + Footprint,
{
    fn footprint(&self) -> Result<isize> {
        let mut footprint: isize = Default::default();
        for level in self.levels.iter() {
            footprint += match level {
                Snapshot::Flush(level) => level.footprint()?,
                Snapshot::Compact(level) => level.footprint()?,
                Snapshot::Active(level) => level.footprint()?,
                Snapshot::Dead(_) => 0,
                Snapshot::None => 0,
            };
        }
        Ok(footprint)
    }
}

impl<K, V> Dgm<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    F: DiskIndexFactory<K, V>,
    F::I: DurableIndex<K, V> + Footprint,
{
    fn gather_commit(&self, footprint: usize) -> Result<CommitData<K,V,D>> {
        use Snapshot::{Flush, Compact, Active}

        let _guard = self.mu.lock().unwrap();

        // fetch the marker offset where we want to commit.
        let iter = self.levels.iter().enumerate();
        let offset = loop {
            let (off, level) = match iter.next() {
                Some((off, level)) => (off, level),
                None => self.levels.len() - 1
            };
            match level {
                Snapshot::Dead(_) | Snapshot::None => (),
                Flush(_) | Compact(_) | Active(_) if offset == 0 => {
                    let msg = format!("exhausted all levels !!");
                    return Err(Error::Dgm(msg))
                }
                Flush(_) | Compact(_) => break offset - 1,
                Active(_) => break off,
            }
        };

        let (d1_off, disk_off) = match &self.levels[offset] {
            // This slot is free, and we don't have to do merge-commit.
            Snapshot::Dead(name) | Snapshot::None => (None, offset),
            // This slot is free, detect fall_back using mem_ratio.
            Snapshot::Active(level) => {
                let r = footprint as f64 / (level.footprint()? as f64);
                if r < self.mem_ratio {
                    (None, offset-1)
                } else {
                    (Some(offset), offset)
                }
            },
            Flush | Compact => unreachable!(),
        };

        let d1 = match d1_off {
            None => None,
            Some(d1_off) => {
                let r = self.levels[d1_off].to_reader()?;
                Some((d1_off, r))
            }
        };

        let disk_name = match &self.levels[disk_off] {
            Snapshot::None => Snapshot::to_name(&self.name, disk_off, 0),
            Snapshot::Dead(name) => name,
            Snapshot::Active(level) => level.to_name(),
        };

        let (_, l, file_no) = Snapshot::to_parts(disk_name);
        if disk_off != l {
            let msg = format!("offset/level {} != {}", disk_off, l);
            return Err(Error::Dgm(msg))
        }
        let name = Snapshot::to_name(&self.name, disk_off, file_no + 1);
        let disk = (disk_off, self.factory::new(&self.dir, &self.name));
        CommitData{ d1, disk }
    }
}

impl<K, V> Dgm<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    F: DiskIndexFactory<K, V>,
    F::I: DurableIndex<K, V> + Footprint,
{
    // to be called when a new snapshot is created with fresh set of files,
    // without any compaction.
    pub fn flush(
        &mut self,
        iter: IndexIter<K, V>, meta: Vec<u8>,
        footprint: usize
    ) -> Result<()> {
        // gather commit data
        let mut data = self.gather_commit(footprint);
        // do commit
        match &data.d1 {
            Some((offset, r)) if *offset == data.disk.0 => {
                let iter = lsm::y_iter(iter, r.iter());
                let prepare = self.levels[offset].prepare_compact();
                data.disk.1.compact(iter, meta, prepare)?;
            }
            Some((_, r)) | None => {
                let iter = lsm::y_iter_versions(iter, r.iter());
                data.disk.1.commit(iter, meta)?;
            }
        };
        // update local fields and reader snapshots.
        {
            let _guard = self.mu.lock().unwrap();
            let (offset, disk) = data.disk;
            let self.levels[offset] = Snapshot::Active(disk);
        }
        (0..self.readers.len()).for_each(|i| self.reset_readers(i))
        Ok(())
    }

    /// Compact disk snapshots if there are any.
    pub fn compact(&mut self) -> Result<()> {
        // TBD
    }

    pub fn to_reader(&mut self) -> Result<Self::R> {
        let _guard = self.mu.lock().unwrap();

        let readers = vec![];
        for level in self.levels.iter() {
            readers.push(level.to_reader());
        }
        self.readers.push(readers);
        let dgm = unsafe {
            // transmute self as void pointer.
            Box::from_raw(self as *mut Dgm<K, V> as *mut ffi::c_void)
        };
        Ok(DgmReader::new(self.readers.len()-1, dgm))
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

impl<K, V, F> AsMut<Dgm<K, V, F>> for DgmReader<K, V, F>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    F: DiskIndexFactory<K, V>,
    F::I: DurableIndex<K, V>,
{
    fn as_mut(&mut self) -> &mut Dgm<K, V, F> {
        // transmute void pointer to mutable reference into index.
        let index_ptr = self.dgm.as_mut().unwrap().as_mut();
        let index_ptr = index_ptr as *mut ffi::c_void;
        unsafe { (index_ptr as *mut Dgm<K, V, F>).as_mut().unwrap() }
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
        let dgm: &mut Dgm<K, V, F> = self.as_mut();
        let rlevels = dgm.take_readers(self.id);

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
        let dgm: &mut Dgm<K, V, F> = self.as_mut();
        let rlevels = dgm.take_readers(self.id);

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
        let dgm: &mut Dgm<K, V, F> = self.as_mut();
        let rlevels = dgm.take_readers(self.id);

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
        let dgm: &mut Dgm<K, V, F> = self.as_mut();
        let rlevels = dgm.take_readers(self.id);

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
        let dgm: &mut Dgm<K, V, F> = self.as_mut();
        let rlevels = dgm.take_readers(self.id);

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
        let dgm: &mut Dgm<K, V, F> = self.as_mut();
        let rlevels = dgm.take_readers(self.id);

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
        let dgm: &mut Dgm<K, V, F> = self.as_mut();
        let rlevels = dgm.take_readers(self.id);

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
        let dgm: &mut Dgm<K, V, F> = self.as_mut();
        let rlevels = dgm.take_readers(self.id);

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
