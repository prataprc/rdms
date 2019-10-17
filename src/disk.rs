use std::{
    borrow::Borrow,
    ffi, fs, marker,
    ops::RangeBounds,
    sync::{self},
};

use crate::core::{Diff, DiskIndexFactory, DurableIndex, Entry, Footprint};
use crate::core::{IndexIter, Reader, Result, Serialize};
use crate::error::Error;
use crate::{lsm, types::EmptyIter};

const NLEVELS: usize = 16;

type Dr<K, V, F> = <<F as DiskIndexFactory<K, V>>::I as DurableIndex<K, V>>::R;

struct OuterSnapshot<K, V, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    snapshot: Option<Snapshot<K, V, D>>,
}

impl<K, V, D> Default for OuterSnapshot<K, V, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn default() -> OuterSnapshot<K, V, D> {
        OuterSnapshot { snapshot: None }
    }
}

impl<K, V, D> OuterSnapshot<K, V, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    D: DurableIndex<K, V>,
{
    fn to_parts(&self) -> Option<(String, usize, usize)> {
        let name = match &self.snapshot {
            Some(Snapshot::Flush(d)) => d.to_name(),
            Some(Snapshot::Compact(d)) => d.to_name(),
            Some(Snapshot::Active(d)) => d.to_name(),
            Some(Snapshot::Dead(name)) => name.to_string(),
            None | _ => unreachable!(),
        };
        let mut parts = {
            let parts: Vec<&str> = name.split('-').collect();
            parts.into_iter()
        };

        let name = parts.next()?;
        let level: usize = parts.next()?.parse().ok()?;
        let file_no: usize = parts.next()?.parse().ok()?;
        Some((name.to_string(), level, file_no))
    }

    fn next_name(&self, default: String) -> String {
        let name = match &self.snapshot {
            Some(Snapshot::Flush(d)) => d.to_name(),
            Some(Snapshot::Compact(d)) => d.to_name(),
            Some(Snapshot::Active(d)) => d.to_name(),
            Some(Snapshot::Dead(name)) => name.to_string(),
            None => default.clone(),
            _ => unreachable!(),
        };
        match Snapshot::<K, V, D>::split_parts(&name) {
            Some((name, lvl, file_no)) => {
                // next name
                Snapshot::<K, V, D>::make_name(&name, lvl, file_no + 1)
            }
            None => default,
        }
    }
}

impl<K, V, D> Footprint for OuterSnapshot<K, V, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    D: Footprint,
{
    fn footprint(&self) -> Result<isize> {
        match &self.snapshot {
            Some(Snapshot::Flush(d)) => d.footprint(),
            Some(Snapshot::Compact(d)) => d.footprint(),
            Some(Snapshot::Active(d)) => d.footprint(),
            Some(Snapshot::Dead(_)) => Ok(0),
            None => Ok(0),
            _ => unreachable!(),
        }
    }
}

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
    __P(marker::PhantomData<K>, marker::PhantomData<V>),
}

impl<K, V, D> Snapshot<K, V, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    D: DurableIndex<K, V>,
{
    fn make_name(name: &str, level: usize, file_no: usize) -> String {
        format!("{}-{}-{}", name, level, file_no)
    }

    fn split_parts(full_name: &str) -> Option<(String, usize, usize)> {
        let mut parts = {
            let parts: Vec<&str> = full_name.split('-').collect();
            parts.into_iter()
        };
        let name = parts.next()?;
        let level: usize = parts.next()?.parse().ok()?;
        let file_no: usize = parts.next()?.parse().ok()?;
        Some((name.to_string(), level, file_no))
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
    factory: F,

    mu: sync::Mutex<u32>,
    levels: [OuterSnapshot<K, V, F::I>; NLEVELS], // snapshots
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
            factory: factory,

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
        let mut levels: [OuterSnapshot<K, V, F::I>; NLEVELS] = [
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
        ];

        for item in fs::read_dir(dir)? {
            match factory.open(dir, item?) {
                Err(_) => continue, // TODO how to handle this error
                Ok(index) => {
                    let (level, file_no) = {
                        let parts = Snapshot::<K, V, F::I>::split_parts(&index.to_name());
                        match parts {
                            Some((_, level, file_no)) => (level, file_no),
                            None => continue,
                        }
                    };
                    let index = match levels[level].snapshot.take() {
                        None => Snapshot::Active(index),
                        Some(Snapshot::Active(old)) => {
                            let parts = Snapshot::<K, V, F::I>::split_parts(&old.to_name());
                            if let Some((_, _, old_no)) = parts {
                                if old_no < file_no {
                                    Snapshot::Active(index)
                                } else {
                                    Snapshot::Active(old)
                                }
                            } else {
                                Snapshot::Active(old)
                            }
                        }
                        _ => unreachable!(),
                    };
                    levels[level] = OuterSnapshot {
                        snapshot: Some(index),
                    };
                }
            }
        }

        Ok(Dgm {
            dir: dir.to_os_string(),
            name: name.to_string(),
            mem_ratio: Self::MEM_RATIO,
            disk_ratio: Self::DISK_RATIO,
            factory: factory,

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

    fn put_readers(&mut self, id: usize, readers: Vec<Dr<K, V, F>>) {
        let _guard = self.mu.lock().unwrap();
        if self.readers.len() == 0 {
            for reader in readers.into_iter() {
                self.readers[id].push(reader)
            }
        }
    }

    fn reset_readers(&mut self, id: usize) {
        let _old_readers = self.take_readers(id);

        let _guard = self.mu.lock().unwrap();
        for level in self.levels.iter_mut() {
            if let Some(snapshot) = &mut level.snapshot {
                let reader = match snapshot {
                    Snapshot::Flush(d) => d.to_reader(),
                    Snapshot::Compact(d) => d.to_reader(),
                    Snapshot::Active(d) => d.to_reader(),
                    Snapshot::Dead(_) => continue,
                    _ => unreachable!(),
                };
                if let Ok(reader) = reader {
                    self.readers[id].push(reader);
                }
            }
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
            footprint += level.footprint()?;
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
    fn gather_flush(&mut self, footprint: usize) -> Result<FlushData<K, V, F::I>> {
        let _guard = self.mu.lock().unwrap();

        match &self.levels[0].snapshot {
            Some(Snapshot::Flush(_)) | Some(Snapshot::Compact(_)) | Some(Snapshot::Active(_)) => {
                let msg = format!("exhausted all levels !!");
                return Err(Error::Dgm(msg));
            }
            _ => (),
        }

        let f = footprint as f64;
        let mut data: FlushData<K, V, F::I> = Default::default();
        let mut iter = self.levels.iter_mut().enumerate();
        iter.next(); // skip the first level, it must be empty
        for (n, level) in iter {
            data = match data {
                // first: gather, if any, a disk level that needs to be read.
                data @ FlushData {
                    d1: None,
                    disk: None,
                } => match &mut level.snapshot {
                    Some(Snapshot::Compact(_)) => {
                        let default = Snapshot::<K, V, F::I>::make_name(&self.name, n - 1, 0);
                        let name = self.levels[n - 1].next_name(default);
                        let d = self.factory.new(&self.dir, &name);
                        FlushData {
                            d1: Some((n - 1, None)),
                            disk: Some((n - 1, d)),
                        }
                    }
                    Some(Snapshot::Active(ref mut d)) => {
                        let footprint = level.footprint().ok().unwrap() as f64;
                        let d1 = if (f / footprint) < self.mem_ratio {
                            Some((n, None))
                        } else {
                            Some((n, Some(d.to_reader()?)))
                        };
                        FlushData { d1, disk: None }
                    }
                    Some(Snapshot::Dead(_)) | None => data,
                    Some(Snapshot::Flush(_)) | _ => unreachable!(),
                },
                // second: gather a disk level that needs to be written to.
                FlushData {
                    d1: d1 @ Some(_),
                    disk: None,
                } => match level.snapshot {
                    Some(Snapshot::Compact(_)) | Some(Snapshot::Active(_)) => {
                        let default = Snapshot::<K, V, F::I>::make_name(&self.name, n - 1, 0);
                        let name = self.levels[n - 1].next_name(default);
                        let d = self.factory.new(&self.dir, &name);
                        FlushData {
                            d1,
                            disk: Some((n - 1, d)),
                        }
                    }
                    Some(Snapshot::Dead(_)) => FlushData { d1, disk: None },
                    None => FlushData { d1, disk: None },
                    Some(Snapshot::Flush(_)) | _ => unreachable!(),
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
                } => match level.snapshot {
                    Some(Snapshot::Active(d)) => {
                        let d1 = Some((n, Some(d.to_reader()?)));
                        CompactData {
                            d1,
                            d2: None,
                            disk: None,
                        }
                    }
                    Some(Snapshot::Compact(_)) => data,
                    Some(Snapshot::Flush(_)) => data,
                    Some(Snapshot::Dead(_)) | None => data,
                    _ => unreachable!(),
                },
                // second: gather the upper disk level that needs to be merged.
                CompactData {
                    d1: d1 @ Some(_),
                    d2: None,
                    disk: None,
                } => match level.snapshot {
                    Some(Snapshot::Compact(_)) => Default::default(),
                    Some(Snapshot::Active(d)) => {
                        let d2 = Some((n, Some(d.to_reader()?)));
                        CompactData { d1, d2, disk: None }
                    }
                    Some(Snapshot::Dead(_)) | None => CompactData {
                        d1,
                        d2: None,
                        disk: None,
                    },
                    Some(Snapshot::Flush(_)) | _ => unreachable!(),
                },
                // third: gather the target disk level.
                CompactData {
                    d1: d1 @ Some(_),
                    d2: d2 @ Some(_),
                    disk: None,
                } => match level.snapshot {
                    None => CompactData { d1, d2, disk: None },
                    Some(Snapshot::Compact(_)) | Some(Snapshot::Active(_)) => {
                        let default = Snapshot::<K, V, F::I>::make_name(&self.name, n - 1, 0);
                        let name = self.levels[n - 1].next_name(default);
                        let d = self.factory.new(&self.dir, &name);
                        CompactData {
                            d1,
                            d2,
                            disk: Some((n - 1, d)),
                        }
                    }
                    Some(Snapshot::Dead(_)) => CompactData { d1, d2, disk: None },
                    Some(Snapshot::Flush(_)) | _ => unreachable!(),
                },
                // okey dokey
                data => return Ok(data),
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
        Ok(())
    }

    pub fn to_reader(&mut self) -> Result<DgmReader<K, V, F>> {
        let _guard = self.mu.lock().unwrap();

        let readers = vec![];
        for level in self.levels.iter() {
            if let Some(snapshot) = level.snapshot {
                let reader = match snapshot {
                    Snapshot::Flush(d) => d.to_reader()?,
                    Snapshot::Compact(d) => d.to_reader()?,
                    Snapshot::Active(d) => d.to_reader()?,
                    Snapshot::Dead(_) => continue,
                    _ => unreachable!(),
                };
                readers.push(reader);
            }
        }
        self.readers.push(readers);
        let dgm = unsafe {
            // transmute self as void pointer.
            Box::from_raw(self as *mut Dgm<K, V, F> as *mut ffi::c_void)
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

    fn empty_iter(&self) -> Result<IndexIter<K, V>> {
        Ok(Box::new(EmptyIter {
            _phantom_key: &self.phantom_key,
            _phantom_val: &self.phantom_val,
        }))
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
        let dgm: &mut Dgm<K, V, F> = {
            // transmute void pointer to mutable reference into index.
            let index_ptr = self.dgm.as_mut().unwrap().as_mut();
            let index_ptr = index_ptr as *mut ffi::c_void;
            unsafe { (index_ptr as *mut Dgm<K, V, F>).as_mut().unwrap() }
        };
        let readers = dgm.take_readers(self.id);

        for reader in readers.iter() {
            match reader.get(key) {
                Ok(entry) => return Ok(entry),
                Err(Error::KeyNotFound) => continue,
                Err(err) => return Err(err),
            }
        }
        Err(Error::KeyNotFound)
    }

    fn iter(&self) -> Result<IndexIter<K, V>> {
        let dgm: &mut Dgm<K, V, F> = {
            // transmute void pointer to mutable reference into index.
            let index_ptr = self.dgm.as_mut().unwrap().as_mut();
            let index_ptr = index_ptr as *mut ffi::c_void;
            unsafe { (index_ptr as *mut Dgm<K, V, F>).as_mut().unwrap() }
        };
        let readers = dgm.take_readers(self.id);

        let mut iters: Vec<IndexIter<K, V>> = vec![];
        for reader in readers.iter() {
            iters.push(reader.iter()?);
        }

        let iter = match iters.len() {
            0 => self.empty_iter(),
            1 => Ok(iters.remove(0)),
            _ => {
                let (mut iter, reverse) = (iters.remove(0), true);
                for older in iters.drain(..) {
                    iter = lsm::y_iter(iter, older, reverse);
                }
                Ok(iter)
            }
        };

        dgm.put_readers(self.id, readers);
        iter
    }

    fn range<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let dgm: &mut Dgm<K, V, F> = {
            // transmute void pointer to mutable reference into index.
            let index_ptr = self.dgm.as_mut().unwrap().as_mut();
            let index_ptr = index_ptr as *mut ffi::c_void;
            unsafe { (index_ptr as *mut Dgm<K, V, F>).as_mut().unwrap() }
        };
        let readers = dgm.take_readers(self.id);

        let mut iters: Vec<IndexIter<K, V>> = vec![];
        for reader in readers.iter() {
            iters.push(reader.range(range.clone())?);
        }

        let iter = match iters.len() {
            0 => self.empty_iter(),
            1 => Ok(iters.remove(0)),
            _ => {
                let (mut iter, reverse) = (iters.remove(0), true);
                for older in iters.drain(..) {
                    iter = lsm::y_iter(iter, older, reverse);
                }
                Ok(iter)
            }
        };

        dgm.put_readers(self.id, readers);
        iter
    }

    fn reverse<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let dgm: &mut Dgm<K, V, F> = {
            // transmute void pointer to mutable reference into index.
            let index_ptr = self.dgm.as_mut().unwrap().as_mut();
            let index_ptr = index_ptr as *mut ffi::c_void;
            unsafe { (index_ptr as *mut Dgm<K, V, F>).as_mut().unwrap() }
        };
        let readers = dgm.take_readers(self.id);

        let mut iters: Vec<IndexIter<K, V>> = vec![];
        for reader in readers.iter() {
            iters.push(reader.reverse(range.clone())?);
        }

        let iter = match iters.len() {
            0 => self.empty_iter(),
            1 => Ok(iters.remove(0)),
            _ => {
                let (mut iter, reverse) = (iters.remove(0), true);
                for older in iters.drain(..) {
                    iter = lsm::y_iter(iter, older, reverse);
                }
                Ok(iter)
            }
        };

        dgm.put_readers(self.id, readers);
        iter
    }

    fn get_with_versions<Q>(&self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let dgm: &mut Dgm<K, V, F> = {
            // transmute void pointer to mutable reference into index.
            let index_ptr = self.dgm.as_mut().unwrap().as_mut();
            let index_ptr = index_ptr as *mut ffi::c_void;
            unsafe { (index_ptr as *mut Dgm<K, V, F>).as_mut().unwrap() }
        };
        let readers = dgm.take_readers(self.id);

        let mut entries: Vec<Entry<K, V>> = vec![];
        for reader in readers.iter() {
            match reader.get_with_versions(key) {
                Ok(entry) => entries.push(entry),
                Err(Error::KeyNotFound) => continue,
                Err(err) => return Err(err),
            }
        }

        let entry = match entries.len() {
            0 => Err(Error::KeyNotFound),
            1 => Ok(entries.remove(0)),
            _ => {
                let mut entry = entries.remove(0);
                for older in entries.drain(..) {
                    entry = entry.flush_merge(older);
                }
                Ok(entry)
            }
        };

        dgm.put_readers(self.id, readers);
        entry
    }

    fn iter_with_versions(&self) -> Result<IndexIter<K, V>> {
        let dgm: &mut Dgm<K, V, F> = {
            // transmute void pointer to mutable reference into index.
            let index_ptr = self.dgm.as_mut().unwrap().as_mut();
            let index_ptr = index_ptr as *mut ffi::c_void;
            unsafe { (index_ptr as *mut Dgm<K, V, F>).as_mut().unwrap() }
        };
        let readers = dgm.take_readers(self.id);

        let mut iters: Vec<IndexIter<K, V>> = vec![];
        for reader in readers.iter() {
            iters.push(reader.iter_with_versions()?);
        }

        let iter = match iters.len() {
            0 => self.empty_iter(),
            1 => Ok(iters.remove(0)),
            _ => {
                let (mut iter, reverse) = (iters.remove(0), true);
                for older in iters.drain(..) {
                    iter = lsm::y_iter_versions(iter, older, reverse);
                }
                Ok(iter)
            }
        };

        dgm.put_readers(self.id, readers);
        iter
    }

    fn range_with_versions<'a, R, Q>(&'a self, r: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let dgm: &mut Dgm<K, V, F> = {
            // transmute void pointer to mutable reference into index.
            let index_ptr = self.dgm.as_mut().unwrap().as_mut();
            let index_ptr = index_ptr as *mut ffi::c_void;
            unsafe { (index_ptr as *mut Dgm<K, V, F>).as_mut().unwrap() }
        };
        let readers = dgm.take_readers(self.id);

        let mut iters: Vec<IndexIter<K, V>> = vec![];
        for reader in readers.iter() {
            iters.push(reader.range_with_versions(r.clone())?);
        }

        let iter = match iters.len() {
            0 => self.empty_iter(),
            1 => Ok(iters.remove(0)),
            _ => {
                let (mut iter, reverse) = (iters.remove(0), true);
                for older in iters.drain(..) {
                    iter = lsm::y_iter_versions(iter, older, reverse);
                }
                Ok(iter)
            }
        };

        dgm.put_readers(self.id, readers);
        iter
    }

    fn reverse_with_versions<'a, R, Q>(&'a self, r: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let dgm: &mut Dgm<K, V, F> = {
            // transmute void pointer to mutable reference into index.
            let index_ptr = self.dgm.as_mut().unwrap().as_mut();
            let index_ptr = index_ptr as *mut ffi::c_void;
            unsafe { (index_ptr as *mut Dgm<K, V, F>).as_mut().unwrap() }
        };
        let readers = dgm.take_readers(self.id);

        let mut iters: Vec<IndexIter<K, V>> = vec![];
        for reader in readers.iter() {
            iters.push(reader.reverse_with_versions(r.clone())?);
        }

        let iter = match iters.len() {
            0 => self.empty_iter(),
            1 => Ok(iters.remove(0)),
            _ => {
                let (mut iter, reverse) = (iters.remove(0), true);
                for older in iters.drain(..) {
                    iter = lsm::y_iter_versions(iter, older, reverse);
                }
                Ok(iter)
            }
        };

        dgm.put_readers(self.id, readers);
        iter
    }
}
