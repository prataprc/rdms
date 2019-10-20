use std::{
    borrow::Borrow,
    ffi, fs, marker,
    ops::RangeBounds,
    sync::{self, Arc},
};

use crate::{
    core::{
        Diff, DiskIndexFactory, DurableIndex, Entry, Footprint, IndexIter, Reader, Result,
        Serialize,
    },
    error::Error,
    lsm,
    types::{Empty, EmptyIter},
};

/// Maximum number of levels to be used for disk indexes.
pub const NLEVELS: usize = 16;

type Levels<K, V, D> = [OuterSnapshot<K, V, D>; NLEVELS];

type Dr<K, V, F> = <<F as DiskIndexFactory<K, V>>::I as DurableIndex<K, V>>::R;

pub struct Dgm<K, V, F>
where
    K: Clone + Ord,
    V: Clone + Diff,
    F: DiskIndexFactory<K, V>,
{
    dir: ffi::OsString,
    name: String,
    mem_ratio: f64,
    disk_ratio: f64,
    factory: F,

    levels: sync::Mutex<Levels<K, V, F::I>>, // snapshots
    readers: Vec<Arc<sync::Mutex<Vec<Dr<K, V, F>>>>>,
}

impl<K, V, F> Dgm<K, V, F>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    F: DiskIndexFactory<K, V>,
{
    /// Default ratio threshold between memory index footprint and
    /// the latest disk index footprint, below which a newer level
    /// shall be created.
    pub const MEM_RATIO: f64 = 0.5;
    /// Default ratio threshold between a disk index footprint and
    /// the next-level disk index footprint, above which the two
    /// levels shall be compacted into a single index.
    pub const DISK_RATIO: f64 = 0.5;

    pub fn new(
        dir: &ffi::OsStr, // directory path
        name: &str,
        factory: F,
    ) -> Result<Dgm<K, V, F>> {
        fs::remove_dir_all(dir)?;
        fs::create_dir_all(dir)?;

        let index = Dgm {
            dir: dir.to_os_string(),
            name: name.to_string(),
            mem_ratio: Self::MEM_RATIO,
            disk_ratio: Self::DISK_RATIO,
            factory: factory,

            levels: Default::default(),
            readers: Default::default(),
        };

        Ok(index)
    }

    pub fn open(
        dir: &ffi::OsStr, // directory path
        name: &str,
        factory: F,
    ) -> Result<Dgm<K, V, F>> {
        let mut levels: Levels<K, V, F::I> = Default::default();

        for item in fs::read_dir(dir)? {
            match factory.open(dir, item?) {
                Err(_) => continue, // TODO how to handle this error
                Ok(index) => {
                    let (level, file_no) = {
                        let parts = Snapshot::<K, V, F::I>::split_parts(
                            // name into name+parts
                            &index.to_name(),
                        );
                        match parts {
                            Some((_, level, file_no)) => (level, file_no),
                            None => continue,
                        }
                    };
                    let index = match levels[level].snapshot.take() {
                        None => Snapshot::Active(index),
                        Some(Snapshot::Active(old)) => {
                            let parts = Snapshot::<K, V, F::I>::split_parts(
                                // name into name+parts
                                &old.to_name(),
                            );
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
                    levels[level] = OuterSnapshot::new(index);
                }
            }
        }

        Ok(Dgm {
            dir: dir.to_os_string(),
            name: name.to_string(),
            mem_ratio: Self::MEM_RATIO,
            disk_ratio: Self::DISK_RATIO,
            factory: factory,

            levels: sync::Mutex::new(levels),
            readers: Default::default(),
        })
    }

    pub fn set_mem_ratio(&mut self, ratio: f64) {
        self.mem_ratio = ratio
    }

    pub fn set_disk_ratio(&mut self, ratio: f64) {
        self.disk_ratio = ratio
    }
}

impl<K, V, F> Dgm<K, V, F>
where
    K: Clone + Ord,
    V: Clone + Diff,
    F: DiskIndexFactory<K, V>,
{
    fn reset_readers(&mut self, levels: &mut Levels<K, V, F::I>) -> Result<()> {
        for readers in self.readers.iter_mut() {
            if Arc::strong_count(&readers) == 1 {
                // this reader-thread has dropped out.
                continue;
            }
            let mut rs = readers.lock().unwrap();
            rs.drain(..);
            for level in levels.iter_mut() {
                if let Some(snapshot) = &mut level.snapshot {
                    let r = match snapshot {
                        Snapshot::Flush(d) => d.to_reader()?,
                        Snapshot::Compact(d) => d.to_reader()?,
                        Snapshot::Active(d) => d.to_reader()?,
                        Snapshot::Dead(_) => continue,
                        _ => unreachable!(),
                    };
                    rs.push(r);
                }
            }
        }
        Ok(())
    }
}

impl<K, V, F> Footprint for Dgm<K, V, F>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    F: DiskIndexFactory<K, V>,
    F::I: Footprint,
{
    fn footprint(&self) -> Result<isize> {
        let levels = self.levels.lock().unwrap();

        let mut footprint: isize = Default::default();
        for level in levels.iter() {
            footprint += level.footprint()?;
        }
        Ok(footprint)
    }
}

struct CommitData<K, V, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    D: DurableIndex<K, V>,
{
    d1: Option<(usize, Option<D::R>)>,
    disk: Option<(usize, D)>,
}

impl<K, V, D> Default for CommitData<K, V, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    D: DurableIndex<K, V>,
{
    fn default() -> CommitData<K, V, D> {
        CommitData {
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

impl<K, V, F> DurableIndex<K, V> for Dgm<K, V, F>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize + Footprint + From<<V as Diff>::D>,
    <V as Diff>::D: Serialize,
    F: DiskIndexFactory<K, V>,
    F::I: DurableIndex<K, V> + Footprint,
{
    type R = DgmReader<K, V, F>;

    type C = Empty;

    fn to_name(&self) -> String {
        self.name.clone()
    }

    fn commit<M>(
        &mut self,
        mem_index: &M, // reference to memory index.
        iter: IndexIter<K, V>,
        meta: Vec<u8>,
    ) -> Result<()>
    where
        M: Footprint,
    {
        let mut levels = self.levels.lock().unwrap();
        let data = self.gather_commit(mem_index.footprint()?, &mut levels)?;

        let (disk_off, mut disk) = match data.disk {
            // first commit
            None => {
                let name = Snapshot::<K, V, F::I>::make_name(&self.name, 0, 0);
                let d = self.factory.new(&self.dir, &name);
                (levels.len() - 1, d)
            }
            // subsequent commits
            Some((disk_off, disk)) => (disk_off, disk),
        };
        let (d1_off, mut d1_r) = match data.d1 {
            // first commit to disk,
            None => (levels.len() - 1, None),
            // mem-only commit to disk.
            Some((d1_off, None)) => (d1_off, None),
            // merge commit (between mem and a disk level) to disk.
            Some((d1_off, d1_r)) => (d1_off, d1_r),
        };
        levels[d1_off] = match levels[d1_off].snapshot.take() {
            None => Default::default(),
            Some(s) => match s.to_disk() {
                Ok(d1) => OuterSnapshot::new(Snapshot::Flush(d1)),
                Err(s) => OuterSnapshot::new(s),
            },
        };

        if d1_off != disk_off && d1_r.is_some() {
            let d1_iter = d1_r.as_mut().unwrap().iter_with_versions()?;
            let iter = lsm::y_iter_versions(iter, d1_iter, false /*reverse*/);
            disk.commit(mem_index, iter, meta)
        } else if d1_r.is_some() {
            let d1_iter = d1_r.as_mut().unwrap().iter()?;
            let iter = lsm::y_iter(iter, d1_iter, false /*reverse*/);
            disk.commit(mem_index, iter, meta)
        } else {
            disk.commit(mem_index, iter, meta)
        }
    }

    fn prepare_compact(&self) -> Result<Self::C> {
        Ok(Empty)
    }

    fn compact(&mut self, _: IndexIter<K, V>, _: Vec<u8>, _: Self::C) -> Result<()> {
        Ok(())
    }

    fn to_reader(&mut self) -> Result<DgmReader<K, V, F>> {
        let mut levels = self.levels.lock().unwrap();

        // create a new set of snapshot-reader
        let mut readers = vec![];
        for level in levels.iter_mut() {
            if let Some(snapshot) = &mut level.snapshot {
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
        let readers = Arc::new(sync::Mutex::new(readers));
        self.readers.push(Arc::clone(&readers));
        Ok(DgmReader::new(&self.name, readers))
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
    fn gather_commit(
        &self,
        f: isize,
        levels: &mut Levels<K, V, F::I>,
    ) -> Result<CommitData<K, V, F::I>> {
        use Snapshot::{Active, Compact, Flush};

        match &levels[0].snapshot {
            Some(Flush(_)) | Some(Compact(_)) | Some(Active(_)) => {
                let msg = format!("exhausted all levels !!");
                return Err(Error::Dgm(msg));
            }
            _ => (),
        }

        let mut data: CommitData<K, V, F::I> = Default::default();
        let mut iter = levels.iter_mut().enumerate();
        let mut prev_level_name = match iter.next() {
            Some((n, level)) => level.next_name(&self.name, n),
            _ => unreachable!(),
        };
        for (n, level) in iter {
            let footprint = level.footprint().ok().unwrap() as f64;
            data = match data {
                // first: gather, if any, a disk level that needs to be read.
                data @ CommitData {
                    d1: None,
                    disk: None,
                } => match &mut level.snapshot {
                    Some(Snapshot::Compact(_)) => {
                        let d = self.factory.new(&self.dir, &prev_level_name);
                        CommitData {
                            d1: Some((n - 1, None)),
                            disk: Some((n - 1, d)),
                        }
                    }
                    Some(Snapshot::Active(ref mut d)) => {
                        let d1 = if (f as f64 / footprint) < self.mem_ratio {
                            Some((n, None))
                        } else {
                            Some((n, Some(d.to_reader()?)))
                        };
                        CommitData { d1, disk: None }
                    }
                    Some(Snapshot::Dead(_)) | None => data,
                    Some(Snapshot::Flush(_)) | _ => unreachable!(),
                },
                // second: gather a disk level that needs to be written to.
                CommitData {
                    d1: d1 @ Some(_),
                    disk: None,
                } => match &mut level.snapshot {
                    Some(Snapshot::Compact(_)) | Some(Snapshot::Active(_)) => {
                        let d = self.factory.new(&self.dir, &prev_level_name);
                        CommitData {
                            d1,
                            disk: Some((n - 1, d)),
                        }
                    }
                    Some(Snapshot::Dead(_)) => CommitData { d1, disk: None },
                    None => CommitData { d1, disk: None },
                    Some(Snapshot::Flush(_)) | _ => unreachable!(),
                },
                // okey dokey
                data => return Ok(data),
            };
            prev_level_name = level.next_name(&self.name, n);
        }
        unreachable!()
    }

    fn gather_compact(
        &self,
        f: isize,
        levels: &mut Levels<K, V, F::I>,
    ) -> Result<CompactData<K, V, F::I>> {
        let mut data: CompactData<K, V, F::I> = Default::default();
        let iter = levels.iter_mut().enumerate();
        let mut prev_level_name = "".to_string();
        for (n, level) in iter {
            data = match data {
                // first: gather the lower disk level that needs to be merged.
                data @ CompactData {
                    d1: None,
                    d2: None,
                    disk: None,
                } => match &mut level.snapshot {
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
                } => match &mut level.snapshot {
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
                } => match &mut level.snapshot {
                    None => CompactData { d1, d2, disk: None },
                    Some(Snapshot::Compact(_)) | Some(Snapshot::Active(_)) => {
                        let d = self.factory.new(&self.dir, &prev_level_name);
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
            prev_level_name = level.next_name(&self.name, n);
        }
        unreachable!();
    }
}

pub struct DgmReader<K, V, F>
where
    K: Clone + Ord,
    V: Clone + Diff,
    F: DiskIndexFactory<K, V>,
{
    name: String,
    rs: Arc<sync::Mutex<Vec<Dr<K, V, F>>>>,

    phantom_key: marker::PhantomData<K>,
    phantom_val: marker::PhantomData<V>,
    phantom_factory: marker::PhantomData<F>,
}

impl<K, V, F> DgmReader<K, V, F>
where
    K: Clone + Ord,
    V: Clone + Diff,
    F: DiskIndexFactory<K, V>,
{
    fn new(
        name: &str,
        rs: Arc<sync::Mutex<Vec<Dr<K, V, F>>>>, // reader snapshots.
    ) -> DgmReader<K, V, F> {
        DgmReader {
            rs,
            name: name.to_string(),

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

    fn get_readers(&self) -> Result<Vec<Dr<K, V, F>>> {
        if Arc::strong_count(&self.rs) == 1 {
            let msg = format!("main `Dgm` thread {} has returned", self.name);
            Err(Error::ThreadFail(msg))
        } else {
            let mut rs = self.rs.lock().unwrap();
            let rs: Vec<Dr<K, V, F>> = rs.drain(..).collect();
            Ok(rs)
        }
    }

    fn put_readers(&self, readers: Vec<Dr<K, V, F>>) {
        let mut rs = self.rs.lock().unwrap();
        // if rs.len() > 0, means Dgm has updated its snapshot/levels
        // to newer set of snapshots.
        if rs.len() == 0 {
            readers.into_iter().for_each(|r| rs.push(r));
        }
        // otherwise drop the reader snapshots here.
    }
}

impl<K, V, F> Reader<K, V> for DgmReader<K, V, F>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize + From<<V as Diff>::D> + Footprint,
    <V as Diff>::D: Serialize,
    F: DiskIndexFactory<K, V>,
{
    fn get<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut readers = self.get_readers()?;
        let entry = {
            let mut iter = readers.iter_mut();
            loop {
                match iter.next() {
                    None => break Err(Error::KeyNotFound),
                    Some(r) => match r.get(key) {
                        Ok(entry) => break Ok(entry),
                        Err(Error::KeyNotFound) => (),
                        Err(err) => break Err(err),
                    },
                }
            }
        };
        self.put_readers(readers);
        entry
    }

    fn iter(&mut self) -> Result<IndexIter<K, V>> {
        let mut dgmi = DgmIter::new(self, self.get_readers()?)?;
        let no_reverse = false;
        for reader in dgmi.readers.iter_mut() {
            let iter = unsafe {
                let reader = reader as *mut Dr<K, V, F>;
                reader.as_mut().unwrap().iter()?
            };
            dgmi.iter = Some(
                // fold with next level.
                lsm::y_iter(iter, dgmi.iter.take().unwrap(), no_reverse),
            );
        }
        Ok(Box::new(dgmi))
    }

    fn range<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let mut dgmi = DgmIter::new(self, self.get_readers()?)?;
        let no_reverse = false;
        for reader in dgmi.readers.iter_mut() {
            let iter = unsafe {
                let reader = reader as *mut Dr<K, V, F>;
                reader.as_mut().unwrap().range(range.clone())?
            };
            dgmi.iter = Some(
                // fold with next level.
                lsm::y_iter(iter, dgmi.iter.take().unwrap(), no_reverse),
            );
        }
        Ok(Box::new(dgmi))
    }

    fn reverse<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let mut dgmi = DgmIter::new(self, self.get_readers()?)?;
        let no_reverse = true;
        for reader in dgmi.readers.iter_mut() {
            let iter = unsafe {
                let reader = reader as *mut Dr<K, V, F>;
                reader.as_mut().unwrap().reverse(range.clone())?
            };
            dgmi.iter = Some(
                // fold with next level.
                lsm::y_iter(iter, dgmi.iter.take().unwrap(), no_reverse),
            );
        }
        Ok(Box::new(dgmi))
    }

    fn get_with_versions<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut readers = self.get_readers()?;
        let mut entries: Vec<Entry<K, V>> = vec![];

        let mut iter = readers.iter_mut();
        let res = loop {
            match iter.next() {
                None => break Ok(()),
                Some(reader) => match reader.get_with_versions(key) {
                    Ok(entry) => entries.push(entry),
                    Err(Error::KeyNotFound) => (),
                    Err(err) => break Err(err),
                },
            }
        };
        self.put_readers(readers);
        res?;

        match entries.len() {
            0 => Err(Error::KeyNotFound),
            1 => Ok(entries.remove(0)),
            _ => {
                let entry = entries.remove(0);
                let entry = entries
                    .into_iter()
                    .fold(entry, |entry, older| entry.flush_merge(older));
                Ok(entry)
            }
        }
    }

    fn iter_with_versions(&mut self) -> Result<IndexIter<K, V>> {
        let mut dgmi = DgmIter::new(self, self.get_readers()?)?;
        let no_reverse = false;
        for reader in dgmi.readers.iter_mut() {
            let iter = unsafe {
                let reader = reader as *mut Dr<K, V, F>;
                reader.as_mut().unwrap().iter_with_versions()?
            };
            dgmi.iter = Some(
                // fold with next level.
                lsm::y_iter(iter, dgmi.iter.take().unwrap(), no_reverse),
            );
        }
        Ok(Box::new(dgmi))
    }

    fn range_with_versions<'a, R, Q>(
        &'a mut self,
        range: R, // between lower and upper bound
    ) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let mut dgmi = DgmIter::new(self, self.get_readers()?)?;
        let no_reverse = false;
        for reader in dgmi.readers.iter_mut() {
            let iter = unsafe {
                let reader = reader as *mut Dr<K, V, F>;
                reader
                    .as_mut()
                    .unwrap()
                    .range_with_versions(range.clone())?
            };
            dgmi.iter = Some(
                // fold with next level.
                lsm::y_iter(iter, dgmi.iter.take().unwrap(), no_reverse),
            );
        }
        Ok(Box::new(dgmi))
    }

    fn reverse_with_versions<'a, R, Q>(
        &'a mut self,
        range: R, // between upper and lower bound
    ) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let mut dgmi = DgmIter::new(self, self.get_readers()?)?;
        let no_reverse = true;
        for reader in dgmi.readers.iter_mut() {
            let iter = unsafe {
                let reader = reader as *mut Dr<K, V, F>;
                reader
                    .as_mut()
                    .unwrap()
                    .reverse_with_versions(range.clone())?
            };
            dgmi.iter = Some(
                // fold with next level.
                lsm::y_iter(iter, dgmi.iter.take().unwrap(), no_reverse),
            );
        }
        Ok(Box::new(dgmi))
    }
}

struct DgmIter<'a, K, V, F>
where
    K: Clone + Ord,
    V: Clone + Diff,
    F: DiskIndexFactory<K, V>,
{
    dgmr: &'a DgmReader<K, V, F>,
    readers: Vec<Dr<K, V, F>>,
    iter: Option<IndexIter<'a, K, V>>,
}

impl<'a, K, V, F> DgmIter<'a, K, V, F>
where
    K: Clone + Ord,
    V: Clone + Diff,
    F: DiskIndexFactory<K, V>,
{
    fn new(
        dgmr: &DgmReader<K, V, F>,
        readers: Vec<Dr<K, V, F>>, // forward array of readers
    ) -> Result<DgmIter<K, V, F>> {
        let mut dgmi = DgmIter {
            dgmr,
            readers,
            iter: Some(dgmr.empty_iter()?),
        };
        dgmi.readers.reverse();
        Ok(dgmi)
    }
}

impl<'a, K, V, F> Drop for DgmIter<'a, K, V, F>
where
    K: Clone + Ord,
    V: Clone + Diff,
    F: DiskIndexFactory<K, V>,
{
    fn drop(&mut self) {
        self.dgmr.put_readers(self.readers.drain(..).collect());
    }
}

impl<'a, K, V, F> Iterator for DgmIter<'a, K, V, F>
where
    K: Clone + Ord,
    V: Clone + Diff,
    F: DiskIndexFactory<K, V>,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.iter {
            Some(iter) => iter.next(),
            None => None,
        }
    }
}

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
    fn new(index: Snapshot<K, V, D>) -> OuterSnapshot<K, V, D> {
        OuterSnapshot {
            snapshot: Some(index),
        }
    }

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

    fn next_name(&self, name: &str, level: usize) -> String {
        let name = match &self.snapshot {
            Some(Snapshot::Flush(d)) => d.to_name(),
            Some(Snapshot::Compact(d)) => d.to_name(),
            Some(Snapshot::Active(d)) => d.to_name(),
            Some(Snapshot::Dead(name)) => name.to_string(),
            None => Snapshot::<K, V, D>::make_name(name, level, 0),
            _ => unreachable!(),
        };
        match Snapshot::<K, V, D>::split_parts(&name) {
            Some((name, level, file_no)) => {
                // next name
                Snapshot::<K, V, D>::make_name(&name, level, file_no + 1)
            }
            None => Snapshot::<K, V, D>::make_name(&name, level, 0),
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

    fn to_disk(self) -> std::result::Result<D, Self> {
        use Snapshot::{Active, Compact, Flush};

        match self {
            Flush(d) => Ok(d),
            Compact(d) => Ok(d),
            Active(d) => Ok(d),
            s => Err(s),
        }
    }
}
