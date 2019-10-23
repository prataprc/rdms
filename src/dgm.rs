use std::{
    borrow::Borrow,
    ffi, fmt, fs, marker, mem,
    ops::{DerefMut, RangeBounds},
    result,
    sync::{self, Arc},
};

use crate::{
    core::{Diff, DiskIndexFactory, Entry, Footprint, Index, IndexIter, Reader},
    core::{Result, Serialize, WriteIndexFactory, Writer},
    error::Error,
    lsm,
    types::EmptyIter,
};
use log::{debug, info};

#[derive(Clone)]
struct Name(String);

impl Name {
    fn next(self) -> Name {
        match From::from(self) {
            Some((s, n)) => From::from((s, n + 1)),
            None => unreachable!(),
        }
    }
}

impl From<(String, usize)> for Name {
    fn from((name, level): (String, usize)) -> Name {
        Name(format!("{}-dgmlevel-{}", name, level))
    }
}

impl From<Name> for Option<(String, usize)> {
    fn from(name: Name) -> Option<(String, usize)> {
        let parts: Vec<&str> = name.0.split('-').collect();
        if parts.len() < 3 {
            None
        } else if parts[parts.len() - 2] != "dgmlevel" {
            None
        } else {
            let level = parts[parts.len() - 1].parse::<usize>().ok()?;
            let name = parts[..(parts.len() - 3)].join("-");
            Some((name, level))
        }
    }
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

/// Maximum number of levels to be used for disk indexes.
pub const NLEVELS: usize = 16;

enum Snapshot<K, V, I>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    I: Index<K, V>,
{
    // memory snapshot that handles all the write operation.
    Write(I),
    // memory snapshot that is waiting to be flushed to disk.
    Flush(I),
    // disk snapshot that is being commited with new batch of entries.
    Commit(I),
    // disk snapshot that is being compacted.
    Compact(I),
    // disk snapshot that is in active state, for either commit or compact.
    Active(I),
    // empty slot
    None,
    // ignore
    _Phantom(marker::PhantomData<K>, marker::PhantomData<V>),
}

impl<K, V, I> Default for Snapshot<K, V, I>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    I: Index<K, V>,
{
    fn default() -> Snapshot<K, V, I> {
        Snapshot::None
    }
}

impl<K, V, I> fmt::Display for Snapshot<K, V, I>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    I: Index<K, V>,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        use Snapshot::{Active, Commit, Compact, Flush, Write};

        match self {
            Write(m) => write!(f, "write/{}", m.to_name()),
            Flush(m) => write!(f, "flush/{}", m.to_name()),
            Commit(d) => write!(f, "commit/{}", d.to_name()),
            Compact(d) => write!(f, "compact/{}", d.to_name()),
            Active(d) => write!(f, "active/{}", d.to_name()),
            Snapshot::None => write!(f, "none"),
            _ => unreachable!(),
        }
    }
}

impl<K, V, I> Footprint for Snapshot<K, V, I>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    I: Index<K, V>,
{
    fn footprint(&self) -> Result<isize> {
        use Snapshot::{Active, Commit, Compact, Flush, Write};

        match self {
            Write(m) => m.footprint(),
            Flush(m) => m.footprint(),
            Commit(d) => d.footprint(),
            Compact(d) => d.footprint(),
            Active(d) => d.footprint(),
            Snapshot::None => Ok(0),
            _ => unreachable!(),
        }
    }
}

impl<K, V, I> Snapshot<K, V, I>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    I: Index<K, V>,
{
    fn to_disk(self) -> Option<I> {
        use Snapshot::{Active, Commit, Compact, Flush, Write};

        match self {
            Commit(d) | Compact(d) | Active(d) => Some(d),
            Write(_) | Flush(_) | Snapshot::None => None,
            _ => unreachable!(),
        }
    }

    fn as_mut_disk(&mut self) -> Option<&mut I> {
        use Snapshot::{Active, Commit, Compact, Flush, Write};

        match self {
            Commit(d) | Compact(d) | Active(d) => Some(d),
            Write(_) | Flush(_) | Snapshot::None => None,
            _ => unreachable!(),
        }
    }

    fn as_mut_memory(&mut self) -> Option<&mut I> {
        use Snapshot::{Active, Commit, Compact, Flush, Write};

        match self {
            Write(m) | Flush(m) => Some(m),
            Commit(_) | Compact(_) | Active(_) => None,
            Snapshot::None => None,
            _ => unreachable!(),
        }
    }

    fn swap_with_newer(&mut self, mut index: I) {
        use Snapshot::Active;

        let disk = mem::replace(self, Default::default());
        let index = match disk {
            Active(mut disk) => {
                if index.to_seqno() <= disk.to_seqno() {
                    Active(disk)
                } else {
                    Active(index)
                }
            }
            Snapshot::None => Active(index),
            _ => unreachable!(),
        };
        mem::replace(self, index);
    }
}

// type alias to array of snapshots.
#[derive(Default)]
struct Levels<K, V, M, D>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    m0: Snapshot<K, V, M::I>,         // write index
    m1: Option<Snapshot<K, V, M::I>>, // flush index
    disks: [Snapshot<K, V, D::I>; NLEVELS],
}

impl<K, V, M, D> Levels<K, V, M, D>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    fn is_commit_exhausted(&mut self) -> bool {
        use Snapshot::{Active, Commit, Compact};

        match self.disks[0] {
            Snapshot::None => false,
            Active(_) => false,
            Commit(_) | Compact(_) => true,
            _ => unreachable!(),
        }
    }
}

// type alias to reader associated type for each snapshot (aka disk-index)
struct Rs<K, V, M, D>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    r_m0: <M::I as Index<K, V>>::R,
    r_m1: Option<<M::I as Index<K, V>>::R>,
    r_disks: Vec<<D::I as Index<K, V>>::R>,
}

pub struct Dgm<K, V, M, D>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    dir: ffi::OsString,
    name: String,
    mem_ratio: f64,
    disk_ratio: f64,
    mem_factory: M,
    disk_factory: D,

    levels: sync::Mutex<Levels<K, V, M, D>>, // snapshots
    writers: Vec<Arc<sync::Mutex<<M::I as Index<K, V>>::W>>>,
    readers: Vec<Arc<sync::Mutex<Rs<K, V, M, D>>>>,
}

impl<K, V, M, D> Dgm<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    /// Default threshold between memory index footprint and
    /// the latest disk index footprint, below which a newer level
    /// shall be created, for commiting new entries.
    pub const MEM_RATIO: f64 = 0.5;
    /// Default threshold between a disk index footprint and
    /// the next-level disk index footprint, above which the two
    /// levels shall be compacted into a single index.
    pub const DISK_RATIO: f64 = 0.5;

    pub fn new(
        dir: &ffi::OsStr, // directory path
        name: &str,
        mem_factory: M,
        disk_factory: D,
    ) -> Result<Dgm<K, V, M, D>> {
        fs::remove_dir_all(dir)?;
        fs::create_dir_all(dir)?;

        let levels = Levels {
            m0: Snapshot::Write(mem_factory.new(name)?),
            m1: None,
            disks: Default::default(),
        };

        Ok(Dgm {
            dir: dir.to_os_string(),
            name: name.to_string(),
            mem_ratio: Self::MEM_RATIO,
            disk_ratio: Self::DISK_RATIO,
            mem_factory,
            disk_factory,

            levels: sync::Mutex::new(levels),
            writers: Default::default(),
            readers: Default::default(),
        })
    }

    pub fn open(
        dir: &ffi::OsStr, // directory path
        name: &str,
        mem_factory: M,
        disk_factory: D,
    ) -> Result<Dgm<K, V, M, D>> {
        let mut disks: [Snapshot<K, V, D::I>; NLEVELS] = Default::default();

        for item in fs::read_dir(dir)? {
            let item = item?;
            let mf = item.file_name();
            let (level, d) = match disk_factory.open(dir, Some(mf.clone())) {
                Ok(index) => {
                    let dgmname = Name(index.to_name());
                    let sn: Option<(String, usize)> = dgmname.into();
                    let (n, level) = match sn {
                        None => {
                            debug!(target: "dgm", "not dgm file {:?}", mf);
                            continue;
                        }
                        Some(sn) => sn,
                    };
                    if name != &n {
                        debug!(target: "dgm", "not dgm file {:?}", mf);
                        continue;
                    }
                    (level, index)
                }
                Err(err) => {
                    debug!(
                        target: "dgm",
                        "{} disk_factory.open(): {:?}", name, err
                    );
                    continue;
                }
            };
            disks[level].swap_with_newer(d);
        }

        let levels = Levels {
            m0: Snapshot::Write(mem_factory.new(name)?),
            m1: None,
            disks,
        };

        Ok(Dgm {
            dir: dir.to_os_string(),
            name: name.to_string(),
            mem_ratio: Self::MEM_RATIO,
            disk_ratio: Self::DISK_RATIO,
            mem_factory,
            disk_factory,

            levels: sync::Mutex::new(levels),
            writers: Default::default(),
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

impl<K, V, M, D> Dgm<K, V, M, D>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    fn cleanup_handles(&mut self) {
        // cleanup dropped writer threads.
        let dropped: Vec<usize> = self
            .writers
            .iter()
            .enumerate()
            .filter_map(|(i, w)| {
                if Arc::strong_count(w) == 1 {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();
        for i in dropped.into_iter().rev() {
            self.writers.remove(i);
        }

        // cleanup dropped reader threads.
        let dropped: Vec<usize> = self
            .readers
            .iter()
            .enumerate()
            .filter_map(|(i, r)| {
                if Arc::strong_count(r) == 1 {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();
        for i in dropped.into_iter().rev() {
            self.readers.remove(i);
        }
    }

    // should be called while holding the levels lock.
    fn shift_into_writers(&self, mut m0: M::I) -> Result<M::I> {
        // block all the writer threads.
        let mut handles = vec![];
        for writer in self.writers.iter() {
            // Arc<Mutex<Option<>>>
            handles.push(writer.lock().unwrap())
        }

        // now replace a new writer handle created from the new m0 snapshot.
        for handle in handles.iter_mut() {
            let _old_w = mem::replace(handle.deref_mut(), m0.to_writer()?);
            // drop the old writer, and unblock the corresponding writer thread.
        }

        Ok(m0)
    }

    // should be called while holding the levels lock.
    fn shift_in(
        &self,
        levels: &mut Levels<K, V, M, D>,
        m0: M::I, // new memory index
    ) -> Result<()> {
        // block all the readers.
        let mut rss = vec![];
        for readers in self.readers.iter() {
            rss.push(readers.lock().unwrap());
        }

        // shift memory snapshot into writers
        let m0 = self.shift_into_writers(m0)?;
        levels.m1 = Some(mem::replace(&mut levels.m0, Default::default()));
        mem::replace(&mut levels.m0, Snapshot::Write(m0));

        // update readers and unblock them one by one.
        for rs in rss.iter_mut() {
            rs.r_m0 = levels.m0.as_mut_memory().unwrap().to_reader()?;
            rs.r_m1 = {
                let m1 = levels.m1.as_mut().unwrap();
                Some(m1.as_mut_memory().unwrap().to_reader()?)
            };
            rs.r_disks.drain(..);
            for disk in levels.disks.iter_mut() {
                if let Some(d) = disk.as_mut_disk() {
                    rs.r_disks.push(d.to_reader()?)
                }
            }
        }
        Ok(())
    }

    fn disk_footprint(&self) -> Result<isize> {
        let levels = self.levels.lock().unwrap();

        let mut footprint: isize = Default::default();
        for disk in levels.disks.iter() {
            footprint += disk.footprint()?;
        }
        Ok(footprint)
    }

    fn mem_footprint(&self) -> Result<isize> {
        let levels = self.levels.lock().unwrap();
        Ok(levels.m0.footprint()?
            + match &levels.m1 {
                None => 0,
                Some(m) => m.footprint()?,
            })
    }

    fn commit_level(&self, levels: &mut Levels<K, V, M, D>) -> Result<usize> {
        use Snapshot::{Active, Commit, Compact, Flush, Write};

        if levels.is_commit_exhausted() {
            let msg = format!("exhausted all levels !!");
            return Err(Error::Dgm(msg));
        }

        let mf = levels.m0.footprint()? as f64;
        let mut iter = levels.disks.iter_mut().enumerate();
        loop {
            match iter.next() {
                None => break Ok(levels.disks.len() - 1), // first commit
                Some((level, disk)) => {
                    let df = disk.footprint()? as f64;
                    match disk {
                        Compact(_) => break Ok(level - 1),
                        Active(_) => {
                            break if (mf / df) < self.mem_ratio {
                                Ok(level - 1)
                            } else {
                                Ok(level)
                            }
                        }
                        Snapshot::None => (),
                        Write(_) | Flush(_) | Commit(_) => unreachable!(),
                        _ => unreachable!(),
                    }
                }
            }
        }
    }

    fn compact_at(levels: &mut Levels<K, V, M, D>) -> Result<[usize; 3]> {
        use Snapshot::{Active, Commit, Compact, Flush, Write};

        let mut disk_iter = levels.disks.iter_mut().enumerate();
        let d1_level = loop {
            match disk_iter.next() {
                None => break 0, // empty
                Some((_, Write(_))) => unreachable!(),
                Some((_, Flush(_))) => unreachable!(),
                Some((_, Commit(_))) => continue, // commit in-progress
                Some((_, Compact(_))) => unreachable!(),
                Some((level, Active(_))) => break level,
                Some((_, Snapshot::None)) => continue,
                _ => unreachable!(),
            }
        };

        let d2_level = loop {
            match disk_iter.next() {
                None => break d1_level, // single disk compaction
                Some((_, Write(_))) => unreachable!(),
                Some((_, Flush(_))) => unreachable!(),
                Some((_, Commit(_))) => unreachable!(),
                Some((_, Compact(_))) => unreachable!(),
                Some((level, Active(_))) => break level,
                Some((_, Snapshot::None)) => continue,
                _ => unreachable!(),
            }
        };

        let disk_level = loop {
            match disk_iter.next() {
                None => break d2_level, // double disk compaction
                Some((_, Write(_))) => unreachable!(),
                Some((_, Flush(_))) => unreachable!(),
                Some((_, Commit(_))) => unreachable!(),
                Some((_, Compact(_))) => unreachable!(),
                Some((level, Active(_))) => break level - 1,
                Some((_, Snapshot::None)) => continue,
                _ => unreachable!(),
            }
        };
        Ok([d1_level, d2_level, disk_level])
    }
}

impl<K, V, M, D> Footprint for Dgm<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint + From<<V as Diff>::D>,
    <V as Diff>::D: Serialize,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    fn footprint(&self) -> Result<isize> {
        Ok(self.disk_footprint()? + self.mem_footprint()?)
    }
}

impl<K, V, M, D> Index<K, V> for Dgm<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint + From<<V as Diff>::D>,
    <V as Diff>::D: Serialize,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    type W = DgmWriter<K, V, <M::I as Index<K, V>>::W>;
    type R = DgmReader<K, V, M, D>;

    fn to_name(&self) -> String {
        self.name.clone()
    }

    // TODO: do we need to persist the disk-levels info in a master file ?
    fn to_file_name(&self) -> Option<ffi::OsString> {
        None
    }

    fn to_metadata(&mut self) -> Result<Vec<u8>> {
        let mut levels = self.levels.lock().unwrap();
        levels.m0.as_mut_memory().unwrap().to_metadata()
    }

    fn to_seqno(&mut self) -> u64 {
        let mut levels = self.levels.lock().unwrap();
        levels.m0.as_mut_memory().unwrap().to_seqno()
    }

    fn set_seqno(&mut self, seqno: u64) {
        let mut levels = self.levels.lock().unwrap();
        levels.m0.as_mut_memory().unwrap().set_seqno(seqno);
    }

    fn to_writer(&mut self) -> Result<Self::W> {
        let mut levels = self.levels.lock().unwrap();

        // create a new set of snapshot-reader
        let w = levels.m0.as_mut_memory().unwrap().to_writer()?;
        let arc_w = Arc::new(sync::Mutex::new(w));
        self.writers.push(Arc::clone(&arc_w));
        Ok(DgmWriter::new(&self.name, arc_w))
    }

    fn to_reader(&mut self) -> Result<Self::R> {
        let mut levels = self.levels.lock().unwrap();

        // create a new set of snapshot-reader
        let r_m0 = levels.m0.as_mut_memory().unwrap().to_reader()?;
        let r_m1 = match levels.m1.as_mut() {
            Some(m) => Some(m.as_mut_memory().unwrap().to_reader()?),
            None => None,
        };
        let mut r_disks = vec![];
        for disk in levels.disks.iter_mut() {
            match disk.as_mut_disk() {
                Some(d) => r_disks.push(d.to_reader()?),
                None => continue,
            }
        }
        let rs = Rs {
            r_m0,
            r_m1,
            r_disks,
        };

        let arc_rs = Arc::new(sync::Mutex::new(rs));
        self.readers.push(Arc::clone(&arc_rs));
        Ok(DgmReader::new(&self.name, arc_rs))
    }

    fn commit(mut self, iter: IndexIter<K, V>, meta: Vec<u8>) -> Result<Self> {
        use Snapshot::{Active, Commit, Compact, Flush, Write};

        self.cleanup_handles();

        let (level, disk) = {
            let mut levels = self.levels.lock().unwrap(); // lock with compact
            let d = Default::default();

            self.shift_in(&mut levels, self.mem_factory.new(&self.name)?)?;

            // find a commit level.
            let level = self.commit_level(&mut levels)?;
            let disk = mem::replace(&mut levels.disks[level], d);
            let disk = match disk {
                Snapshot::None => {
                    let name: Name = (self.name.clone(), level).into();
                    self.disk_factory.new(&self.dir, &name.to_string())?
                }
                Active(disk) => {
                    let master_file = disk.to_file_name();
                    levels.disks[level] = Snapshot::Commit(disk);
                    self.disk_factory.open(&self.dir, master_file)?
                }
                Write(_) | Flush(_) | Commit(_) | Compact(_) => unreachable!(),
                _ => unreachable!(),
            };
            (level, disk)
        };

        let disk = disk.commit(iter, meta)?;

        // update the readers
        {
            let mut levels = self.levels.lock().unwrap(); // lock with compact
            levels.disks[level] = Snapshot::Active(disk);

            for readers in self.readers.iter_mut() {
                let mut rs = readers.lock().unwrap();
                rs.r_m1 = None;
                rs.r_disks.drain(..);
                for disk in levels.disks.iter_mut() {
                    match disk {
                        Write(_) | Flush(_) => unreachable!(),
                        Commit(_) => unreachable!(),
                        Compact(d) => rs.r_disks.push(d.to_reader()?),
                        Active(d) => rs.r_disks.push(d.to_reader()?),
                        Snapshot::None => (),
                        _ => unreachable!(),
                    }
                }
            }
        }
        Ok(self)
    }

    fn compact(mut self) -> Result<Self> {
        use Snapshot::{Active, Commit, Compact, Flush, Write};

        self.cleanup_handles();

        let (mut r1, mut r2, meta, level, disk) = {
            let mut levels = self.levels.lock().unwrap(); // lock with compact

            // find compact levels
            let [l1, l2, level] = Self::compact_at(&mut levels)?;
            let empty = (l1 == 0) && (l2 == 0) && (level == 0);
            let mut compact = l1 == l2 && l2 == level;
            compact = compact && level == levels.disks.len() - 1;

            if empty {
                (None, None, None, None, None)
            } else if compact {
                let d: Snapshot<K, V, D::I> = Default::default();
                let disk = match mem::replace(&mut levels.disks[level], d) {
                    Active(disk) => {
                        let master_file = disk.to_file_name();
                        levels.disks[level] = Snapshot::Compact(disk);
                        self.disk_factory.open(&self.dir, master_file)?
                    }
                    _ => unreachable!(),
                };

                (None, None, None, Some(level), Some(disk))
            } else {
                let d: Snapshot<K, V, D::I> = Default::default();
                let mut d1 = match mem::replace(&mut levels.disks[l1], d) {
                    Active(d) => Snapshot::Compact(d),
                    _ => unreachable!(),
                };
                let d: Snapshot<K, V, D::I> = Default::default();
                let mut d2 = match mem::replace(&mut levels.disks[l2], d) {
                    Active(d) => Snapshot::Compact(d),
                    _ => unreachable!(),
                };
                let (r1, r2) = match (&mut d1, &mut d2) {
                    (Compact(d1), Compact(d2)) => (
                        // get the reader handles
                        d1.to_reader()?,
                        d2.to_reader()?,
                    ),
                    _ => unreachable!(),
                };
                let meta = d1.as_mut_disk().unwrap().to_metadata()?;
                levels.disks[l1] = d1;
                levels.disks[l1] = d2;

                let d: Snapshot<K, V, D::I> = Default::default();
                let disk = match mem::replace(&mut levels.disks[level], d) {
                    Snapshot::None => {
                        let name: Name = (self.name.clone(), level).into();
                        self.disk_factory.new(&self.dir, &name.0)?
                    }
                    Active(disk) => {
                        let master_file = disk.to_file_name();
                        levels.disks[level] = Snapshot::Compact(disk);
                        self.disk_factory.open(&self.dir, master_file)?
                    }
                    _ => unreachable!(),
                };
                (
                    Some((l1, r1)),
                    Some((l2, r2)),
                    Some(meta),
                    Some(level),
                    Some(disk),
                )
            }
        };

        let disk = match (r1.as_mut(), r2.as_mut(), meta, disk) {
            (None, None, None, None) => return Ok(self),
            (None, None, None, Some(disk)) => disk.compact()?,
            (Some(r1), Some(r2), Some(meta), Some(disk)) => {
                let no_reverse = false;
                let (iter1, iter2) = (r1.1.iter()?, r2.1.iter()?);
                let iter = lsm::y_iter_versions(iter1, iter2, no_reverse);
                disk.commit(iter, meta)?
            }
            _ => unreachable!(),
        };

        // update the readers
        {
            let mut levels = self.levels.lock().unwrap(); // lock with compact
            match (r1, r2) {
                (Some((l1, _)), Some((l2, _))) => {
                    levels.disks[l1] = Default::default();
                    levels.disks[l2] = Default::default();
                }
                (None, None) => (),
                _ => unreachable!(),
            }
            levels.disks[level.unwrap()] = Snapshot::Active(disk);

            for readers in self.readers.iter_mut() {
                let mut rs = readers.lock().unwrap();
                rs.r_disks.drain(..);
                for disk in levels.disks.iter_mut() {
                    match disk {
                        Write(_) | Flush(_) | Compact(_) => unreachable!(),
                        Commit(d) => rs.r_disks.push(d.to_reader()?),
                        Active(d) => rs.r_disks.push(d.to_reader()?),
                        Snapshot::None => (),
                        _ => unreachable!(),
                    }
                }
            }
        }
        Ok(self)
    }
}

pub struct DgmWriter<K, V, W>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    W: Writer<K, V>,
{
    name: String,
    w: Arc<sync::Mutex<W>>,
    _phantom_key: marker::PhantomData<K>,
    _phantom_val: marker::PhantomData<V>,
}

impl<K, V, W> DgmWriter<K, V, W>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    W: Writer<K, V>,
{
    fn new(name: &str, w: Arc<sync::Mutex<W>>) -> DgmWriter<K, V, W> {
        DgmWriter {
            name: name.to_string(),
            w,
            _phantom_key: marker::PhantomData,
            _phantom_val: marker::PhantomData,
        }
    }
}

impl<K, V, W> Writer<K, V> for DgmWriter<K, V, W>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    W: Writer<K, V>,
{
    fn set(&mut self, key: K, value: V) -> Result<Option<Entry<K, V>>> {
        let mut w = self.w.lock().unwrap();
        w.set(key, value)
    }

    fn set_cas(&mut self, k: K, v: V, cas: u64) -> Result<Option<Entry<K, V>>> {
        let mut w = self.w.lock().unwrap();
        w.set_cas(k, v, cas)
    }

    fn delete<Q>(&mut self, key: &Q) -> Result<Option<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        let mut w = self.w.lock().unwrap();
        w.delete(key)
    }
}

pub struct DgmReader<K, V, M, D>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    name: String,
    rs: Arc<sync::Mutex<Rs<K, V, M, D>>>,
    _phantom_key: marker::PhantomData<K>,
    _phantom_val: marker::PhantomData<V>,
}

impl<K, V, M, D> DgmReader<K, V, M, D>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    fn new(
        name: &str,
        rs: Arc<sync::Mutex<Rs<K, V, M, D>>>, // pre-build reader
    ) -> DgmReader<K, V, M, D> {
        DgmReader {
            name: name.to_string(),
            rs,
            _phantom_key: marker::PhantomData,
            _phantom_val: marker::PhantomData,
        }
    }

    fn empty_iter(&self) -> Result<IndexIter<K, V>> {
        Ok(Box::new(EmptyIter {
            _phantom_key: &self._phantom_key,
            _phantom_val: &self._phantom_val,
        }))
    }

    fn get_readers(&self) -> Result<sync::MutexGuard<Rs<K, V, M, D>>> {
        match Arc::strong_count(&self.rs) {
            n if n > 1 => Ok(self.rs.lock().unwrap()),
            1 => {
                let msg = format!("main `Dgm` thread {} returned", self.name);
                Err(Error::ThreadFail(msg))
            }
            _ => unreachable!(),
        }
    }
}

impl<K, V, M, D> Reader<K, V> for DgmReader<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + From<<V as Diff>::D> + Footprint,
    <V as Diff>::D: Serialize,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    fn get<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut rs = self.get_readers()?;
        match rs.r_m0.get(key) {
            Ok(entry) => return Ok(entry),
            Err(Error::KeyNotFound) => (),
            Err(err) => return Err(err),
        }

        if let Some(m1) = &mut rs.r_m1 {
            match m1.get(key) {
                Ok(entry) => return Ok(entry),
                Err(Error::KeyNotFound) => (),
                Err(err) => return Err(err),
            }
        }

        let mut iter = rs.r_disks.iter_mut();
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
    }

    fn iter(&mut self) -> Result<IndexIter<K, V>> {
        let mut dgmi = DgmIter::new(self, self.get_readers()?)?;
        let no_reverse = false;

        for disk in dgmi.rs.r_disks.iter_mut().rev() {
            let iter = unsafe {
                let disk = disk as *mut <D::I as Index<K, V>>::R;
                disk.as_mut().unwrap().iter()?
            };
            dgmi.iter = lsm::y_iter(iter, dgmi.iter, no_reverse);
        }

        if let Some(m1) = &mut dgmi.rs.r_m1 {
            let iter = unsafe {
                let m1 = m1 as *mut <M::I as Index<K, V>>::R;
                m1.as_mut().unwrap().iter()?
            };
            dgmi.iter = lsm::y_iter(iter, dgmi.iter, no_reverse);
        }
        let iter = unsafe {
            let m0 = &mut dgmi.rs.r_m0 as *mut <M::I as Index<K, V>>::R;
            m0.as_mut().unwrap().iter()?
        };
        dgmi.iter = lsm::y_iter(iter, dgmi.iter, no_reverse);

        Ok(Box::new(dgmi))
    }

    fn range<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        Ok(self.empty_iter()?)

        //let mut dgmi = DgmIter::new(self, self.get_readers()?)?;
        //let no_reverse = false;

        //for reader in dgmi.readers.iter_mut() {
        //    let iter = unsafe {
        //        let reader = reader as *mut Dr<K, V, F>;
        //        reader.as_mut().unwrap().range(range.clone())?
        //    };
        //    dgmi.iter = Some(
        //        // fold with next level.
        //        lsm::y_iter(iter, dgmi.iter.take().unwrap(), no_reverse),
        //    );
        //}
        //Ok(Box::new(dgmi))
    }

    fn reverse<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        Ok(self.empty_iter()?)

        //let mut dgmi = DgmIter::new(self, self.get_readers()?)?;
        //let no_reverse = true;
        //for reader in dgmi.readers.iter_mut() {
        //    let iter = unsafe {
        //        let reader = reader as *mut Dr<K, V, F>;
        //        reader.as_mut().unwrap().reverse(range.clone())?
        //    };
        //    dgmi.iter = Some(
        //        // fold with next level.
        //        lsm::y_iter(iter, dgmi.iter.take().unwrap(), no_reverse),
        //    );
        //}
        //Ok(Box::new(dgmi))
    }

    fn get_with_versions<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut rs = self.get_readers()?;
        let m0_entry = match rs.r_m0.get_with_versions(key) {
            Ok(entry) => Some(entry),
            Err(Error::KeyNotFound) => None,
            Err(err) => return Err(err),
        };

        let mut entry = match &mut rs.r_m1 {
            Some(m1) => match m1.get_with_versions(key) {
                Ok(m1_entry) => match m0_entry {
                    Some(m0_entry) => Some(m0_entry.flush_merge(m1_entry)),
                    None => Some(m1_entry),
                },
                Err(Error::KeyNotFound) => m0_entry,
                Err(err) => return Err(err),
            },
            None => m0_entry,
        };

        let mut iter = rs.r_disks.iter_mut();
        entry = loop {
            entry = match iter.next() {
                None => break entry,
                Some(r) => match r.get_with_versions(key) {
                    Ok(e) => match entry {
                        Some(entry) => Some(entry.flush_merge(e)),
                        None => Some(e),
                    },
                    Err(Error::KeyNotFound) => entry,
                    Err(err) => return Err(err),
                },
            }
        };

        match entry {
            Some(entry) => Ok(entry),
            None => Err(Error::KeyNotFound),
        }
    }

    fn iter_with_versions(&mut self) -> Result<IndexIter<K, V>> {
        Ok(self.empty_iter()?)

        //let mut dgmi = DgmIter::new(self, self.get_readers()?)?;
        //let no_reverse = false;
        //for reader in dgmi.readers.iter_mut() {
        //    let iter = unsafe {
        //        let reader = reader as *mut Dr<K, V, F>;
        //        reader.as_mut().unwrap().iter_with_versions()?
        //    };
        //    dgmi.iter = Some(
        //        // fold with next level.
        //        lsm::y_iter(iter, dgmi.iter.take().unwrap(), no_reverse),
        //    );
        //}
        //Ok(Box::new(dgmi))
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
        Ok(self.empty_iter()?)

        //let mut dgmi = DgmIter::new(self, self.get_readers()?)?;
        //let no_reverse = false;
        //for reader in dgmi.readers.iter_mut() {
        //    let iter = unsafe {
        //        let reader = reader as *mut Dr<K, V, F>;
        //        reader
        //            .as_mut()
        //            .unwrap()
        //            .range_with_versions(range.clone())?
        //    };
        //    dgmi.iter = Some(
        //        // fold with next level.
        //        lsm::y_iter(iter, dgmi.iter.take().unwrap(), no_reverse),
        //    );
        //}
        //Ok(Box::new(dgmi))
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
        Ok(self.empty_iter()?)

        //let mut dgmi = DgmIter::new(self, self.get_readers()?)?;
        //let no_reverse = true;
        //for reader in dgmi.readers.iter_mut() {
        //    let iter = unsafe {
        //        let reader = reader as *mut Dr<K, V, F>;
        //        reader
        //            .as_mut()
        //            .unwrap()
        //            .reverse_with_versions(range.clone())?
        //    };
        //    dgmi.iter = Some(
        //        // fold with next level.
        //        lsm::y_iter(iter, dgmi.iter.take().unwrap(), no_reverse),
        //    );
        //}
        //Ok(Box::new(dgmi))
    }
}

struct DgmIter<'a, K, V, M, D>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    dgmr: &'a DgmReader<K, V, M, D>,
    rs: sync::MutexGuard<'a, Rs<K, V, M, D>>,
    iter: IndexIter<'a, K, V>,
}

impl<'a, K, V, M, D> DgmIter<'a, K, V, M, D>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    fn new(
        dgmr: &'a DgmReader<K, V, M, D>,
        rs: sync::MutexGuard<'a, Rs<K, V, M, D>>,
    ) -> Result<DgmIter<'a, K, V, M, D>> {
        let mut dgmi = DgmIter {
            dgmr,
            rs,
            iter: dgmr.empty_iter()?,
        };
        dgmi.rs.r_disks.reverse();
        Ok(dgmi)
    }
}

impl<'a, K, V, M, D> Iterator for DgmIter<'a, K, V, M, D>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

#[cfg(test)]
#[path = "dgm_test.rs"]
mod dgm_test;
