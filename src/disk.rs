use std::{
    borrow::Borrow,
    ffi, fs, marker,
    ops::RangeBounds,
    sync::{self},
};

use crate::core::{Diff, DiskIndexFactory, DurableIndex, Entry, Footprint};
use crate::core::{IndexIter, Reader, Result, Serialize};
use crate::error::Error;
use crate::lsm;

const NLEVELS: usize = 16;

type Dr<K, V, F> = <<F as DiskIndexFactory<K, V>>::I as DurableIndex<K, V>>::R;

#[derive(Clone)]
enum Snapshot<K, V, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    Flush(D),
    Compact(D),
    Active(D),
    Dead(String),
    None,
    __P(marker::PhantomData<K>, marker::PhantomData<V>),
}

impl<K, V, D> Snapshot<K, V, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    D: DurableIndex<K, V>,
{
    fn to_name(name: &str, level: usize, file_no: usize) -> String {
        format!("{}-{}-{}", name, level, file_no)
    }

    fn to_parts(full_name: &str) -> Option<(String, usize, usize)> {
        let mut parts = {
            let parts: Vec<&str> = full_name.split('-').collect();
            parts.into_iter()
        };
        let name = parts.next()?;
        let level: usize = parts.next()?.parse().ok()?;
        let file_no: usize = parts.next()?.parse().ok()?;
        Some((name.to_string(), level, file_no))
    }

    fn next_name(&self, default: String) -> String {
        let name = match self {
            Snapshot::Flush(d) => d.to_name(),
            Snapshot::Compact(d) => d.to_name(),
            Snapshot::Active(d) => d.to_name(),
            Snapshot::Dead(name) => name.to_string(),
            Snapshot::None => default,
            _ => unreachable!(),
        };
        match Self::to_parts(&name) {
            Some((name, lvl, file_no)) => Self::to_name(&name, lvl, file_no + 1),
            None => default,
        }
    }
}

impl<K, V, D> Default for Snapshot<K, V, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn default() -> Snapshot<K, V, D> {
        Snapshot::None
    }
}

impl<K, V, D> Footprint for Snapshot<K, V, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    D: Footprint,
{
    fn footprint(&self) -> Result<isize> {
        match self {
            Snapshot::Flush(d) => d.footprint(),
            Snapshot::Compact(d) => d.footprint(),
            Snapshot::Active(d) => d.footprint(),
            Snapshot::Dead(String) => Ok(0),
            Snapshot::None => Ok(0),
            _ => unreachable!(),
        }
    }
}

struct FlushData<K, V, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    D: DurableIndex<K, V>,
{
    d1: Option<(usize, Option<D::R>)>,
    disk: Option<(usize, D)>,
}

impl<K, V, D> Default for FlushData<K, V, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    D: DurableIndex<K, V>,
{
    fn default() -> FlushData<K, V, D> {
        FlushData {
            d1: Default::default(),
            disk: Default::default(),
        }
    }
}

struct CompactData<K, V, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    D: DurableIndex<K, V>,
{
    d1: Option<(usize, Option<D::R>)>,
    d2: Option<(usize, Option<D::R>)>,
    disk: Option<(usize, D)>,
}

impl<K, V, D> Default for CompactData<K, V, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    D: DurableIndex<K, V>,
{
    fn default() -> CompactData<K, V, D> {
        CompactData {
            d1: Default::default(),
            d2: Default::default(),
            disk: Default::default(),
        }
    }
}

struct Dgm<K, V, F>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    F: DiskIndexFactory<K, V>,
{
    dir: ffi::OsString,
    name: String,
    mem_ratio: f64,
    disk_ratio: f64,
    disk_factory: F,

    mu: sync::Mutex<u32>,
    levels: [Snapshot<K, V, F::I>>; NLEVELS] // snapshots
    readers: Vec<Vec<Dr<K, V, F>>>,
}

impl<K, V, F> Dgm<K, V, F>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    F: DiskIndexFactory<K, V>,
{
    const MEM_RATIO: f64 = 0.5;
    const DISK_RATIO: f64 = 0.5;

    pub fn new(
        dir: &ffi::OsStr, // directory path
        name: &str,
        factory: F,
    ) -> Result<Dgm<K, V, F>> {
        fs::remove_dir_all(dir)?;
        fs::create_dir_all(dir)?;

        Ok(Dgm {
            dir: dir.to_os_string(),
            name: name.to_string(),
            mem_ratio: Self::MEM_RATIO,
            disk_ratio: Self::DISK_RATIO,
            disk_factory: factory,

            mu: sync::Mutex::new(0xC0FFEE),
            levels: Default::default(),
            readers: Default::default(),
        })
    }

    pub fn open(
        dir: &ffi::OsStr, // directory path
        name: &str,
        factory: F,
    ) -> Result<Dgm<K, V, F>> {
        let levels = vec![];
        for item in fs::read_dir(dir)? {
            match factory.open(dir, item?) {
                Err(_) => continue, // TODO how to handle this error
                Ok(index) => {
                    let (_, n, _) = Snapshot::to_parts(&index.to_name());
                    levels.push(Snapshot::Active(index)),
                }
            }
        }

        Ok(Dgm {
            dir: dir.to_os_string(),
            name: name.to_string(),
            mem_ratio: Self::MEM_RATIO,
            disk_ratio: Self::DISK_RATIO,
            disk_factory: factory,

            mu: sync::Mutex::new(0xC0FFEE),
            levels,
            readers: Default::default(),
        })
    }

    pub fn set_mem_ratio(&mut self, ratio: f64) {
        self.mem_ratio = ratio
    }

    pub fn set_disk_ratio(&mut self, ratio: f64) {
        self.disk_ratio = ratio
    }

    fn take_readers(&mut self, id: usize) -> Vec<Dr<K, V, F>> {
        let _guard = self.mu.lock().unwrap();

        let readers: Vec<Dr<K, V, F>> = self.readers[id].drain(..).collect();
        readers
    }

    fn reset_readers(&mut self, id: usize) {
        let _guard = self.mu.lock().unwrap();

        let _old_readers = self.take_readers(id);
        for level in self.levels.iter() {
            let reader = match self {
                Snapshot::Flush(d) => d.to_reader()?,
                Snapshot::Compact(d) => d.to_reader()?,
                Snapshot::Active(d) => d.to_reader()?,
                Snapshot::Dead(name) => continue,
                Snapshot::None => continue,
                _ => unreachable!(),
            };
            self.readers[id].push(reader);
        }
    }
}

impl<K, V, F> Footprint for Dgm<K, V, F>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    F: DiskIndexFactory<K, V>,
    F::I: Footprint,
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
                _ => unreachable!(),
            };
        }
        Ok(footprint)
    }
}

impl<K, V, F> Dgm<K, V, F>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    F: DiskIndexFactory<K, V>,
    F::I: Footprint,
{
    fn gather_flush(&self, footprint: usize) -> Result<FlushData<K, V, F::I>> {
        let _guard = self.mu.lock().unwrap();

        match &self.levels[0] {
            Snapshot::Flush(_) | Snapshot::Compact(_) | Snapshot::Active(_) => {
                let msg = format!("exhausted all levels !!");
                return Err(Error::Dgm(msg));
            }
            _ => (),
        }

        let f = footprint as f64;
        let mut data: FlushData<K, V, F::I> = Default::default();
        let iter = self.levels.iter().enumerate();
        iter.next(); // skip the first level, it must be empty
        for (n, level) in iter {
            data = match data {
                // first: gather, if any, a disk level that needs to be read.
                data @ FlushData {
                    d1: None,
                    disk: None,
                } => match level {
                    Snapshot::Compact(_) => {
                        let default = Snapshot::to_name(self.name, n - 1, 0);
                        let name = self.levels[n - 1].next_name(default);
                        let d = self.factory.new(&self.dir, &name);
                        FlushData {
                            d1: Some((n - 1, None)),
                            disk: Some((n - 1, d)),
                        }
                    }
                    Snapshot::Active(d) => {
                        let footprint = level.footprint().ok().unwrap() as f64;
                        let d1 = if (f / footprint) < self.mem_ratio {
                            Some((n, None))
                        } else {
                            Some((n, Some(d.to_reader()?)))
                        };
                        FlushData { d1, disk: None }
                    }
                    Snapshot::Dead(_) | Snapshot::None => data,
                    Snapshot::Flush(_) | _ => unreachable!(),
                },
                // second: gather a disk level that needs to be written to.
                FlushData {
                    d1: d1 @ Some(_),
                    disk: None,
                } => match level {
                    Snapshot::Compact(_) | Snapshot::Active(_) => {
                        let default = Snapshot::to_name(self.name, n - 1, 0);
                        let name = self.levels[n - 1].next_name(default);
                        let d = self.factory.new(&self.dir, &name);
                        FlushData {
                            d1,
                            disk: Some((n - 1, d)),
                        }
                    }
                    Snapshot::Dead(_) | Snapshot::None => FlushData { d1, None },
                    Snapshot::Flush(_) | _ => unreachable!(),
                },
                // okey dokey
                data => return Ok(data),
            };
        }
        unreachable!()
    }

    fn gather_compact(&self, footprint: usize) -> Result<CompactData<K, V, F::I>> {
        let _guard = self.mu.lock().unwrap();

        let f = footprint as f64;
        let mut data: CompactData<K, V, F::I> = Default::default();
        let iter = self.levels.iter().enumerate();
        for (n, level) in iter {
            data = match data {
                // first: gather the lower disk level that needs to be merged.
                data @ CompactData {
                    d1: None,
                    d2: None,
                    disk: None,
                } => match level {
                    Snapshot::Active(d) => {
                        let d1 = Some((n, Some(d.to_reader()?)));
                        CompactData {
                            d1,
                            d2: None,
                            disk: None,
                        }
                    }
                    Snapshot::Compact(_) | Snapshot::Flush(_) => data,
                    Snapshot::Dead(_) | Snapshot::None => data,
                    _ => unreachable!(),
                },
                // second: gather the upper disk level that needs to be merged.
                CompactData {
                    d1: d1 @ Some(_),
                    d2: None,
                    disk: None,
                } => match level {
                    Snapshot::Compact(_) => Default::default(),
                    Snapshot::Active(d) => {
                        let d2 = Some((n, Some(d.to_reader()?)));
                        CompactData { d1, d2, disk: None }
                    }
                    Snapshot::Dead(_) | Snapshot::None => CompactData {
                        d1,
                        d2: None,
                        disk: None,
                    },
                    Snapshot::Flush(_) | _ => unreachable!(),
                },
                // third: gather the target disk level.
                CompactData {
                    d1: d1 @ Some(_),
                    d2: d2 @ Some(_),
                    disk: None,
                } => match level {
                    Snapshot::Compact(_) | Snapshot::Active(_) => {
                        let default = Snapshot::to_name(self.name, n - 1, 0);
                        let name = self.levels[n - 1].next_name(default);
                        let d = self.factory.new(&self.dir, &name);
                        CompactData {
                            d1,
                            d2,
                            disk: Some((n - 1, d)),
                        }
                    }
                    Snapshot::Dead(_) => CompactData { d1, d2, disk: None },
                    Snapshot::None => CompactData { d1, d2, disk: None },
                    Snapshot::Flush(_) | _ => unreachable!(),
                },
                // okey dokey
                data => return data,
            };
        }
        unreachable!();
    }
}

impl<K, V, F> Dgm<K, V, F>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    F: DiskIndexFactory<K, V>,
    F::I: Footprint,
{
    // to be called when a new snapshot is created with fresh set of files,
    // without any compaction.
    // pub fn flush(
    //     &mut self,
    //     iter: IndexIter<K, V>, meta: Vec<u8>,
    //     footprint: usize
    // ) -> Result<()> {
    //     // gather commit data
    //     let mut data = self.gather_commit(footprint);
    //     // do commit
    //     match &data.d1 {
    //         Some((offset, r)) if *offset == data.disk.0 => {
    //             let iter = lsm::y_iter(iter, r.iter());
    //             let prepare = self.levels[offset].prepare_compact();
    //             data.disk.1.compact(iter, meta, prepare)?;
    //         }
    //         Some((_, r)) | None => {
    //             let iter = lsm::y_iter_versions(iter, r.iter());
    //             data.disk.1.commit(iter, meta)?;
    //         }
    //     };
    //     // update local fields and reader snapshots.
    //     {
    //         let _guard = self.mu.lock().unwrap();
    //         let (offset, disk) = data.disk;
    //         let self.levels[offset] = Snapshot::Active(disk);
    //     }
    //     (0..self.readers.len()).for_each(|i| self.reset_readers(i))
    //     Ok(())
    // }

    /// Compact disk snapshots if there are any.
    pub fn compact(&mut self) -> Result<()> {
        // TBD
    }

    pub fn to_reader(&mut self) -> Result<DgmReader<K, V, F>> {
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
        Ok(DgmReader::new(self.readers.len() - 1, dgm))
    }
}

struct DgmReader<K, V, F>
where
    K: Clone + Ord,
    V: Clone + Diff,
    F: DiskIndexFactory<K, V>,
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
{
    fn get<Q>(&self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let dgm: &mut Dgm<K, V, F> = self.as_mut();
        let readers = dgm.take_readers(self.id);

        for r in readers.iter() {
            match r.get(key) {
                Ok(entry) => return Ok(entry),
                Err(Error::KeyNotFound) => continue,
                Err(err) => return Err(err),
            }
        }
        Err(Error::KeyNotFound)
    }

    fn iter(&self) -> Result<IndexIter<K, V>> {
        let dgm: &mut Dgm<K, V, F> = self.as_mut();
        let readers = dgm.take_readers(self.id);

        let mut iters: Vec<IndexIter<K, V>> = vec![];
        for r in readers.iter() {
            iters.push(r.iter()?);
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
        let readers = dgm.take_readers(self.id);

        let mut iters: Vec<IndexIter<K, V>> = vec![];
        for r in readers.iter() {
            iters.push(r.range(range.clone())?);
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
        let readers = dgm.take_readers(self.id);

        let mut iters: Vec<IndexIter<K, V>> = vec![];
        for r in readers.iter() {
            iters.push(r.reverse(range.clone())?);
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
        let readers = dgm.take_readers(self.id);

        let mut entries: Vec<Entry<K, V>> = vec![];
        for r in readers.iter() {
            match r.get_with_versions(key) {
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
        let readers = dgm.take_readers(self.id);

        let mut iters: Vec<IndexIter<K, V>> = vec![];
        for r in readers.iter() {
            iters.push(r.iter_with_versions()?);
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
        let readers = dgm.take_readers(self.id);

        let mut iters: Vec<IndexIter<K, V>> = vec![];
        for r in readers.iter() {
            iters.push(r.range_with_versions(r.clone())?);
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
        let readers = dgm.take_readers(self.id);

        let mut iters: Vec<IndexIter<K, V>> = vec![];
        for r in readers.iter() {
            iters.push(r.reverse_with_versions(r.clone())?);
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
