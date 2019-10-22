use std::{
    borrow::Borrow,
    ffi, fs, marker,
    ops::RangeBounds,
    sync::{self, Arc},
    mem,
};

use crate::{
    core::{Diff, DiskIndexFactory, Entry, Footprint, Index, IndexIter, Reader},
    core::{Result, Serialize},
    error::Error,
    lsm,
    types::{Empty, EmptyIter},
};

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
    K: Clone + Ord,
    V: Clone + Diff,
    I: Index<K,V>
{
    // memory snapshot that handles all the write operation.
    Write(I<K, V>),
    // memory snapshot that is waiting to be flushed to disk.
    Flush(I<K, V>),
    // disk snapshot that is being commited with new batch of entries.
    Commit(Option<I<K, V>>),
    // disk snapshot that is being compacted.
    Compact(I<K, V>),
    // disk snapshot that is in active state, for either commit or compact.
    Active(I<K, V>),
    // empty slot
    None,
}

impl<K, V, I> Default for Snapshot<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Index<K,V>
{
    fn default() -> Snapshot<K, V, I> {
        Snapshot::None
    }
}

impl<K, V, I> Footprint for Snapshot<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Index<K,V>,
{
    fn footprint(&self) -> Result<isize> {
        use Snapshot::{Write, Flush, Commit, Compact, Active};

        match self {
            Write(m) => m.footprint(),
            Flush(m) => m.footprint(),
            Commit(Some(d)) => d.footprint(),
            Commit(None) => d.footprint(),
            Compact(d) => d.footprint(),
            Active(d) => d.footprint(),
            Snapshot::None => Ok(0)
        }
    }
}

impl<K, V, I> Snapshot<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Index<K,V>,
{
    fn to_disk(self) -> Option<D::I<K, V>> {
        use Snapshot::{Write, Flush, Commit, Compact, Active};

        match self {
            Commit(Some(d)) => Some(d),
            Commit(None) => Some(d),
            Compact(d) => Some(d),
            Active(d) => Some(d),
            Write(_) | Flush(_) | Snapshot::None => None,
        }
    }

    fn to_memory(self) -> Option<D::I<K, V>> {
        use Snapshot::{Write, Flush, Commit, Compact, Active};

        match self {
            Write(m) | Flush(m) => Some(m),
            Commit(_) | Compact(_) | Active(_) => None,
            Snapshot::None => None
        }
    }

    fn is_older(&self, oth: &I) -> bool {
        use Snapshot::{Writer, Flush, Commit, Compact, Active };

        match self {
            Commit(Some(index)) | Compact(index) | Active(index) => {
                let (self_seqno, oth_seqno) = (index.to_seqno(), oth.to_seqno());
                if self_seqno < oth_seqno {
                    true
                } else if self_seqno == oth_seqno {
                    index.footprint() < oth.footprint()
                }
            }
            Commit(None) | Snapshot::None => true,
            Write | Flush => unreachable!(),
        }
    }
}

// type alias to array of snapshots.
struct Levels<K, V, M, D>
    K: Clone + Ord,
    V: Clone + Diff,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    m0: Snapshot<K,V,M::I>, // write index
    m1: Option<Snapshot<K,V,M::I>>, // flush index
    disks: [Snapshot<K, V, D::I>; NLEVELS],
}

impl<K,V,M,D> Levels<K,V,M,D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    fn as_first_disk(&mut self) -> &mut D::I {
        use Snapshot::{Commit, Compact, Active};

        for disk in self.disks.iter_mut() {
            match disk {
                Commit(Some(d)) | Compact(d) | Active(d) => return d,
                Commit(None) | Snapshot::None => continue,
                _ => unreachable!(),
            }
        }
    }

    fn is_exhausted(&mut self) -> bool {
        use Snapshot::{Write, Flush, Commit, Compact, Active,;

        match levels.as_first_disk() {
            Commit(_) | Compact(_)) => true,
            Active(_) | Snapshot::None => false,
            _ => unreachable!(),
        }
    }
}

// type alias to reader associated type for each snapshot (aka disk-index)
struct Rs<K, V, M, D>
    K: Clone + Ord,
    V: Clone + Diff,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    r_m0: Snapshot<K,V,<M::I as Index>::R>,
    r_m1: Option<Snapshot<K,V,<M::I as Index>::R>>,
    disks: Vec<Snapshot<K, V, <D::I as Index>::R>>,
}

pub struct Dgm<K, V, M, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
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
    writers: Vec<Arc<sync::Mutex<Option<<<M::I as Index>::W>>>>>,
    readers: Vec<Arc<sync::Mutex<Rs<K, V, M, D>>>>>,
}

impl<K, V, M, D> Dgm<K, V, M, D>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
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

        Ok(Dgm {
            dir: dir.to_os_string(),
            name: name.to_string(),
            mem_ratio: Self::MEM_RATIO,
            disk_ratio: Self::DISK_RATIO,
            mem_factory,
            disk_factory,

            levels: Default::default(),
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
        let mut disks: [Snapshot<K,V,D::I>; NLEVELS] = Default::default();

        for item in fs::read_dir(dir)? {
            let item = item?;
            let item_name = item.file_name();
            let (level, index) = match disk_factory.open(dir, item) {
                Ok(index) => {
                    let dgmname = Name(index.to_name());
                    let sn: Option<(String, usize)> = dgmname.into();
                    let (n, level) = match sn {
                        None => {
                            debug!(target: "dgm", "not dgm file {}", item_name)
                            continue;
                        }
                        Some(sn) => sn,
                    }
                    if name != &n {
                        debug!(target: "dgm", "not dgm file {}", item_name)
                        continue;
                    }
                    (level, index)
                },
                Err(err) => {
                    debug!(
                        target: "dgm",
                        "{} disk_factory.open(): {:?}", name, err
                    );
                    continue,
                }
            };
            let disk = mem::replace(&mut disk[level], Default::default());
            disks[level] = match disk {
                Some(Snapshot::Active(other)) if index.is_older(other) {
                    Snapshot::Active(other)
                },
                None | _ => Snapshot::Active(index),
            };
        }

        Ok(Dgm {
            dir: dir.to_os_string(),
            name: name.to_string(),
            mem_ratio: Self::MEM_RATIO,
            disk_ratio: Self::DISK_RATIO,
            mem_factory,
            disk_factory,

            levels: Default::default(),
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

impl<K, V, M, D> Dgm<K, V, M,D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    fn cleanup_handles(&mut self) {
        // cleanup dropped writer threads.
        let dropped_writers = vec![];
        let dropped: Vec<usize> = self.writers
            .iter()
            .enumerate()
            .filter_map(|(i, w)| 
                if Arc::strong_count(w) == 1 { Some(i) } else { None }
            ).collect();
        dropped.reverse().into_iter().for_each(|i| self.writers.remove(i));

        // cleanup dropped reader threads.
        let dropped_readers = vec![];
        let dropped: Vec<usize> = self.readers
            .iter()
            .enumerate()
            .filter_map(|(i, r)|
                if Arc::strong_count(r) == 1 { Some(i) } else { None }
            ).collect();
        dropped.reverse().into_iter().for_each(|i| self.readers.remove(i));
    }

    // should be called while holding the levels lock.
    fn shift_into_writers(&mut self, m0:  M::I) Result<M::I> {
        // block all the writer threads.
        let mut handles = vec![];
        for writer in self.writers.iter() { // Arc<Mutex<Option<>>>
            handles.push(writer.lock().unwrap())
        }

        // now insert a new writer handle created from the new m0 snapshot.
        for handle in handles.iter_mut() {
            handle.replace(m0.to_writer()?); // drop the old writer
            // unblock the corresponding writer thread.
        }

        Ok(m0)
    }

    // should be called while holding the levels lock.
    fn shift_in(&mut self, m0: M::I) -> Result<()> {
        self.cleanup_handles();

        // block all the readers.
        let mut rss = vec![];
        for readers in self.readers.iter_mut() {
            rss.push(readers.lock().unwrap());
        }

        // shift memory snapshot into writers
        if levels.m1.is_some() {
            unreachable!()
        }
        let m0 = self.shift_into_writers(m0);
        level.m1 = m0;
        level.m0 = m0;

        // update readers and unblock them one by one.
        for rs in rss.iter_mut() {
            rs.r_m0 = levels.m0.to_reader()?;
            rs.r_m1 = Some(levels.m1.to_reader()?);
            rs.disks.drain(..);
            for disk in levels.disks.iter_mut() {
                match disk {
                    Write | Flush => unreachable!(),
                    Commit(d) | Compact(d) | Active(d) => {
                        rs.disks.push(d.to_reader()?)
                    }
                    Snapshot::None => (),
                }
            }
        }
        Ok(())
    }

    fn disk_footprint(&self) -> Result<isize> {
        let levels = self.levels.lock().unwrap();

        let mut footprint: isize = Default::default();
        for disk in levels.disk.iter() {
            footprint += disk.footprint()?;
        }
        Ok(footprint)
    }

    fn mem_footprint(&self) -> Result<isize> {
        let levels = self.levels.lock().unwrap();
        Ok(levels.m0.footprint()? + match levels.m1 {
            None => 0,
            Some(m) => m.footprint()?,
        })
    }

    fn total_footprint(&self) -> Result<isize> {
        Ok(self.disk_footprint()? + self.mem_footprint()?)
    }

    fn commit_level(&self, levels: &mut Levels<K, V, M, D>) -> Result<usize> {
        use Snapshot::{Write, Flush, Commit, Compact, Active};

        if levels.as_first_disk().is_exhausted() {
            let msg = format!("exhausted all levels !!");
            return Err(Error::Dgm(msg));
        }

        let mf = levels.m0.footprint()?  as f64;
        for (level, disk) in levels.disks.iter_mut().enumerate() {
            let df = disk.footprint()? as f64;
            match disk {
                Write(_) | Flush(_) | Commit(_) => unreachable!(),
                Compact(_) | Active(_) if (mf / df) < self.mem_ratio => level-1,
                Active(_) => level,
                Snapshot::None => (),
            }
        }
        // first commit
        Ok(levels.len()-1)
    }

    fn compact_at(&self, levels: &mut Levels<K, V, M, D>,) -> Result<[3]usize> {
        use Snapshot::{Writer, Flush, Commit, Compact, Active};

        let disks = levels.disks.iter_mut().enumerate();
        let d1_level = loop {
            match disks.next() {
                None = return Ok([0,0,0]),
                Some((_, Writer)) => unreachable!(),
                Some((_, Flush)) => unreachable!(),
                Some((_, Commit)) => continue,
                Some((_, Compact)) => unreachable!(),
                Some((level, Active)) => break Some(level),
                Some((_, Snapshot::None)) => continue,
            }
        };
        if d1_level == (levels.disks.len() - 1) {
            return [d1_level, d1_level, d1_level]
        }

        let d2_level = loop {
            match disks.next() {
                None = return Ok([0,0,0]),
                Some((_, Writer)) => unreachable!(),
                Some((_, Flush)) => unreachable!(),
                Some((_, Commit)) => unreachable!(),
                Some((_, Compact)) => unreachable!(),
                Some((level, Active)) => break Some(level),
                Some((_, Snapshot::None)) => continue,
            }
        };

        let disk_level = loop {
            match disks.next() {
                None = break Some(levels.len() - 1),
                Some((_, Writer)) => unreachable!(),
                Some((_, Flush)) => unreachable!(),
                Some((_, Commit)) => unreachable!(),
                Some((_, Compact)) => unreachable!(),
                Some((level, Active)) => break Some(level-1),
                Some((_, Snapshot::None)) => continue,
            }
        };
        Ok([d1_level, d2_level, disk_level])
    }
}

impl<K, V, M, D> Index<K, V> for Dgm<K, V, M, D>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize + Footprint + From<<V as Diff>::D>,
    <V as Diff>::D: Serialize,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    type W = DgmWriter<K,V,<<M::I as Index>::W>;
    type R = DgmReader<K,V,<<M::I as Index>::R>;

    fn to_name(&self) -> String {
        self.name.clone()
    }

    fn to_metadata(&mut self) -> Result<Vec<u8>> {
        let mut levels = self.levels.lock().unwrap();
        levels.as_first_disk().to_metadata()
    }

    fn to_seqno(&self) -> String {
        let mut levels = self.levels.lock().unwrap();
        levels.m0.to_seqno()
    }

    fn set_seqno(&mut self, seqno: u64) {
        let mut levels = self.levels.lock().unwrap();
        levels.m0.set_seqno(seqno);
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

    /// Create a new write handle, for multi-threading. Note that not all
    /// indexes allow concurrent writers. Refer to index API for more details.
    fn to_writer(&mut self) -> Result<Self::W> {
        panic!("not supported")
    }

    fn commit(mut self, iter: IndexIter<K, V>, meta: Vec<u8>) -> Result<Self> {
        use Snapshot::{Writer, Flush, Commit, Compact, Active };

        let (level, disk) = {
            let mut levels = self.levels.lock().unwrap(); // lock with compact

            self.shift_in(self.mem_factory(&self.name)?)?;

            // find a commit level.
            let level = self.commit_level(&mut levels)?;
            let disk = mem::replace(&mut levels.disks[level], Default::default);
            let disk = match disk {
                Snapshot::None = {
                    let name: Name = (self.name.clone(), level).into();
                    self.disk_factory.new(&self.dir, &name.to_string())?
                }
                Active(disk) => {
                    let name = disk.to_name();
                    levels.disks[level] = Snapshot::Commit(disk);
                    self.disk_factory.open(&self.dir, &name.to_string())?
                }
                Writer | Flush | Commit | Compact => unreachable!(),
            };
            (level, disk)
        };

        let disk = disk.commit(iter, meta)?;

        // update the readers
        {
            let mut levels = self.levels.lock().unwrap(); // lock with compact
            levels.disks[level] = Snapshot::Active(disk);

            for readers in self.readers.iter_mut() {
                let rs = readers.lock().unwrap();
                rs.r_m1 = None,
                rs.disks.drain(..);
                for disk in levels.disks.iter_mut() {
                    rs.disks.push(match disk {
                        Write | Flush | Commit(d) => unreachable!(),
                        Compact(d) | Active(d) => d.to_reader()?,
                        Snapshot::None => (),
                    })
                }
            }
        }
        self
    }

    fn compact(mut self) -> Result<Self> {
        use Snapshot::{Writer, Flush, Commit, Compact, Active };

        let (iter, meta, l1, l2, level, disk) = {
            let mut levels = self.levels.lock().unwrap(); // lock with compact
            let d: Snapshot<K, V, D::I> = Default::default();

            // find compact levels
            let [l1, l2, level] = match self.compact_at(&mut levels)? {
                [0, 0, 0] => return Ok(self),
                value => value,
            };

            if (l1 == l2) && (l2 == level) && level {
                let disk = match mem::replace(&mut levels.disks[level], d) {
                    Active(disk) => {
                        let name = disk.to_name();
                        levels.disks[level] = Snapshot::Compact(disk),
                        self.disk_factory.open(&self.dir, &name.to_string())?
                    }
                    _ => unreachable!(),
                };
                (None, None, None, None, level, disk)

            } else {
                let d1 = match mem::replace(&mut levels.disks[l1], d) {
                    Active(d) => Snapshot::Compact(d),
                };
                let d2 = match mem::replace(&mut levels.disks[l2], d) {
                    Active(d) => Snapshot::Compact(d),
                };
                let (r1, r2) = match (&mut d1, (&mut d2) {
                    (Compact(d1), Compact(d2))  => (
                        // get the reader handles
                        d1.to_reader()?, d2.to_reader()
                    ),
                    Writer | Flush | Commit | Compact  => unreachable!(),
                    Snapshot::None => unreachable!(),
                };
                let meta = d1.to_metadata();
                levels.disks[l1] = d1;
                levels.disks[l1] = d2;

                let reverse = false;
                let iter = lsm::y_iter_versions(iter, r1.iter()?, reverse);
                let iter = lsm::y_iter_versions(iter, r2.iter()?, reverse);

                let disk = match mem::replace(&mut levels.disks[level], d) {
                    Snapshot::None => {
                        let name: Name = (self.name.clone(), level).into();
                        self.disk_factory.new(&self.dir, &name.to_string())?
                    }
                    Active(disk) => {
                        let name = disk.to_name();
                        levels.disks[level] = Snapshot::Compact(disk);
                        self.disk_factory.open(&self.dir, &name.to_string())?
                    },
                };
                (Some(iter), Some(meta), Some(l1), Some(l2), level, disk)
            }
        };

        let disk = match (iter, meta) {
            (None, None) => disk.compact()?,
            (Some(iter), Some(meta)) => disk.commit(iter, meta)?,
        };

        // update the readers
        {
            let mut levels = self.levels.lock().unwrap(); // lock with compact
            levels.disks[l1] = Default::default();
            levels.disks[l2] = Default::default();
            levels.disks[level] = Snapshot::Active(disk);

            for readers in self.readers.iter_mut() {
                let rs = readers.lock().unwrap();
                rs.disks.drain(..);
                for disk in levels.disks.iter_mut() {
                    rs.disks.push(match disk {
                        Write | Flush | Compact(d) => unreachable!(),
                        Commit(d) | Active(d) => d.to_reader()?,
                        Snapshot::None => (),
                    })
                }
            }
        }
        self
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

#[cfg(test)]
#[path = "dgm_test.rs"]
mod dgm_test;
