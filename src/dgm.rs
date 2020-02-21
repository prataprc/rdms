//! Module `dgm` implement data-indexing optimized for
//! disk-greater-than-memory.

use log::{error, info};
use toml;

use std::{
    borrow::Borrow,
    cmp,
    convert::{self, TryFrom, TryInto},
    ffi, fmt, fs,
    hash::Hash,
    io::{Read, Write},
    marker, mem,
    ops::{Bound, DerefMut, RangeBounds},
    path, result,
    sync::{mpsc, Arc, Mutex, MutexGuard},
    thread, time,
    time::{Duration, SystemTime},
};

use crate::{
    core::{self, Cutoff, Writer},
    core::{CommitIter, CommitIterator, Result, Serialize, WriteIndexFactory},
    core::{Diff, DiskIndexFactory, Entry, Footprint, Index, IndexIter, Reader},
    error::Error,
    lsm, thread as rt, util,
};

#[derive(Clone)]
pub struct Config {
    mem_ratio: f64,
    disk_ratio: f64,
    compact_interval: Duration,
}

impl Config {
    /// Default threshold between memory index footprint and
    /// the latest disk index footprint, below which a newer level
    /// shall be created, for commiting latest batch of entries.
    /// Refer to [set_mem_ratio][Config::set_mem_ratio] method for details.
    pub const MEM_RATIO: f64 = 0.5;

    /// Default threshold between a disk index footprint and
    /// the next-level disk index footprint, above which the two
    /// levels shall be compacted into a single disk-index.
    /// Refer to [set_disk_ratio][Config::set_disk_ratio] method for details.
    pub const DISK_RATIO: f64 = 0.5;

    /// Default interval in time duration, for invoking disk compaction
    /// between dgm disk-levels.
    /// Refer to [set_compact_interval][Config::set_compact_interval] method
    /// for details.
    pub const COMPACT_INTERVAL: Duration = Duration::from_secs(1800);

    /// Set threshold between memory index footprint and the latest disk
    /// index footprint, below which a newer level shall be created,
    /// for commiting new entries.
    pub fn set_mem_ratio(&mut self, ratio: f64) {
        self.mem_ratio = ratio;
    }

    /// Set threshold between a disk index footprint and the next-level disk
    /// index footprint, above which the two levels shall be compacted
    /// into a single index.
    pub fn set_disk_ratio(&mut self, ratio: f64) {
        self.disk_ratio = ratio;
    }

    /// Set interval in time duration, for invoking disk compaction
    /// between dgm disk-levels. Calling this method will spawn an auto
    /// compaction thread.
    pub fn set_compact_interval(&mut self, interval: Duration) {
        self.compact_interval = interval
    }
}

impl From<Root> for Config {
    fn from(root: Root) -> Config {
        Config {
            mem_ratio: root.mem_ratio,
            disk_ratio: root.disk_ratio,
            compact_interval: root.compact_interval,
        }
    }
}

#[derive(Clone, Default)]
struct Root {
    version: usize,

    levels: usize,
    mem_ratio: f64,
    disk_ratio: f64,
    compact_interval: time::Duration, // in seconds.
    lsm_cutoff: Option<Bound<u64>>,
    tombstone_cutoff: Option<Bound<u64>>,
}

impl From<Config> for Root {
    fn from(config: Config) -> Root {
        Root {
            version: 0,
            levels: NLEVELS,
            mem_ratio: config.mem_ratio,
            disk_ratio: config.disk_ratio,
            compact_interval: config.compact_interval,
            lsm_cutoff: Default::default(),
            tombstone_cutoff: Default::default(),
        }
    }
}

impl TryFrom<Root> for Vec<u8> {
    type Error = crate::error::Error;

    fn try_from(root: Root) -> Result<Vec<u8>> {
        use toml::Value::{self, Array, Float, Integer, String as TomlStr};

        let text = {
            let mut dict = toml::map::Map::new();

            let version: i64 = root.version.try_into()?;
            let levels: i64 = root.levels.try_into()?;
            let mem_ratio: f64 = root.mem_ratio.into();
            let disk_ratio: f64 = root.disk_ratio.into();
            let c_interval: i64 = root.compact_interval.as_secs().try_into()?;

            dict.insert("version".to_string(), Integer(version));
            dict.insert("levels".to_string(), Integer(levels));
            dict.insert("mem_ratio".to_string(), Float(mem_ratio));
            dict.insert("disk_ratio".to_string(), Float(disk_ratio));
            dict.insert("compact_interval".to_string(), Integer(c_interval));

            let lsm_cutoff = Array(match root.lsm_cutoff {
                Some(cutoff) => {
                    let (arg1, arg2) = match cutoff {
                        Bound::Excluded(cutoff) => ("excluded", cutoff),
                        Bound::Included(cutoff) => ("included", cutoff),
                        Bound::Unbounded => unreachable!(),
                    };
                    vec![TomlStr(arg1.to_string()), TomlStr(arg2.to_string())]
                }
                None => {
                    //
                    vec![TomlStr("none".to_string()), TomlStr(0.to_string())]
                }
            });
            dict.insert("lsm_cutoff".to_string(), lsm_cutoff);

            let tombstone_cutoff = Array(match root.tombstone_cutoff {
                Some(cutoff) => {
                    let (arg1, arg2) = match cutoff {
                        Bound::Excluded(cutoff) => ("excluded", cutoff),
                        Bound::Included(cutoff) => ("included", cutoff),
                        Bound::Unbounded => unreachable!(),
                    };
                    vec![TomlStr(arg1.to_string()), TomlStr(arg2.to_string())]
                }
                None => {
                    //
                    vec![TomlStr("none".to_string()), TomlStr(0.to_string())]
                }
            });
            dict.insert("tombstone_cutoff".to_string(), tombstone_cutoff);

            Value::Table(dict).to_string()
        };

        Ok(text.as_bytes().to_vec())
    }
}

impl TryFrom<Vec<u8>> for Root {
    type Error = crate::error::Error;

    fn try_from(bytes: Vec<u8>) -> Result<Root> {
        use crate::error::Error::InvalidFile;

        let err1 = InvalidFile(format!("dgm, not a table"));
        let err2 = format!("dgm, fault in config field");

        let text = std::str::from_utf8(&bytes)?.to_string();

        let value: toml::Value = text
            .parse()
            .map_err(|_| InvalidFile(format!("dgm, invalid root file")))?;

        let dict = value.as_table().ok_or(err1)?;
        let mut root: Root = Default::default();

        root.version = {
            let field = dict.get("version");
            field
                .ok_or(InvalidFile(err2.clone()))?
                .as_integer()
                .ok_or(InvalidFile(err2.clone()))?
                .try_into()?
        };
        root.levels = {
            let field = dict.get("levels");
            field
                .ok_or(InvalidFile(err2.clone()))?
                .as_integer()
                .ok_or(InvalidFile(err2.clone()))?
                .try_into()?
        };
        root.mem_ratio = {
            let field = dict.get("mem_ratio");
            field
                .ok_or(InvalidFile(err2.clone()))?
                .as_float()
                .ok_or(InvalidFile(err2.clone()))?
                .try_into()?
        };
        root.disk_ratio = {
            let field = dict.get("disk_ratio");
            field
                .ok_or(InvalidFile(err2.clone()))?
                .as_float()
                .ok_or(InvalidFile(err2.clone()))?
                .try_into()?
        };
        root.compact_interval = {
            let field = dict.get("compact_interval");
            let duration: u64 = field
                .ok_or(InvalidFile(err2.clone()))?
                .as_integer()
                .ok_or(InvalidFile(err2.clone()))?
                .try_into()?;
            time::Duration::from_secs(duration)
        };
        root.lsm_cutoff = {
            let field = dict.get("lsm_cutoff").ok_or(InvalidFile(err2.clone()))?;
            let arr = field.as_array().ok_or(InvalidFile(err2.clone()))?;
            let bound = arr[0].as_str().ok_or(InvalidFile(err2.clone()))?;
            let cutoff: u64 = {
                let cutoff = &arr[1].as_str().ok_or(InvalidFile(err2.clone()))?;
                cutoff.parse()?
            };
            match bound {
                "excluded" => Some(Bound::Excluded(cutoff)),
                "included" => Some(Bound::Included(cutoff)),
                "unbounded" => Some(Bound::Unbounded),
                "none" => None,
                _ => unreachable!(),
            }
        };
        root.tombstone_cutoff = {
            let field = dict
                .get("tombstone_cutoff")
                .ok_or(InvalidFile(err2.clone()))?;
            let arr = field.as_array().ok_or(InvalidFile(err2.clone()))?;
            let bound = arr[0].as_str().ok_or(InvalidFile(err2.clone()))?;
            let cutoff: u64 = {
                let cutoff = &arr[1].as_str().ok_or(InvalidFile(err2.clone()))?;
                cutoff.parse()?
            };
            match bound {
                "excluded" => Some(Bound::Excluded(cutoff)),
                "included" => Some(Bound::Included(cutoff)),
                "unbounded" => Some(Bound::Unbounded),
                "none" => None,
                _ => unreachable!(),
            }
        };

        Ok(root)
    }
}

impl Root {
    fn to_next(&self) -> Root {
        let mut new_root = self.clone();
        new_root.version += 1;
        new_root
    }

    fn update_cutoff(&mut self, cutoff: Cutoff, tip_seqno: u64) {
        use std::ops::Bound::{Excluded, Included, Unbounded};

        match cutoff {
            Cutoff::Lsm(n_cutoff) => match self.lsm_cutoff.clone() {
                None => self.lsm_cutoff = Some(n_cutoff),
                Some(o) => {
                    let range = (Unbounded, o.clone());
                    self.lsm_cutoff = Some(match n_cutoff {
                        Excluded(n) if range.contains(&n) => o,
                        Excluded(n) => Excluded(n),
                        Included(n) if range.contains(&n) => o,
                        Included(n) => Included(n),
                        Unbounded => Included(tip_seqno),
                    });
                }
            },
            Cutoff::Tombstone(n_cutoff) => match self.tombstone_cutoff.clone() {
                None => self.tombstone_cutoff = Some(n_cutoff),
                Some(o) => {
                    let range = (Unbounded, o.clone());
                    self.tombstone_cutoff = Some(match n_cutoff {
                        Excluded(n) if range.contains(&n) => o,
                        Excluded(n) => Excluded(n),
                        Included(n) if range.contains(&n) => o,
                        Included(n) => Included(n),
                        Unbounded => Included(tip_seqno),
                    });
                }
            },
        }
    }
}

#[derive(Clone)]
struct RootFileName(ffi::OsString);

impl From<(String, usize)> for RootFileName {
    fn from((name, version): (String, usize)) -> RootFileName {
        let file_name = format!("{}-dgm-{:03}.root", name, version);
        let name: &ffi::OsStr = file_name.as_ref();
        RootFileName(name.to_os_string())
    }
}

impl TryFrom<RootFileName> for (String, usize) {
    type Error = Error;

    fn try_from(fname: RootFileName) -> Result<(String, usize)> {
        use crate::error::Error::InvalidFile;
        let err = format!("{} not dgm root file", fname);

        let check_file = |fname: RootFileName| -> Option<(String, usize)> {
            let fname = path::Path::new(&fname.0);
            match fname.extension()?.to_str()? {
                "root" => {
                    let stem = fname.file_stem()?.to_str()?.to_string();
                    let parts: Vec<&str> = stem.split('-').collect();
                    if parts.len() >= 3 {
                        match &parts[parts.len() - 2..] {
                            ["dgm", ver] => {
                                let ver = ver.parse::<usize>().ok()?;
                                let s = parts[..(parts.len() - 2)].join("-");
                                Some((s, ver))
                            }
                            _ => None,
                        }
                    } else {
                        None
                    }
                }
                _ => None,
            }
        };

        check_file(fname).ok_or(InvalidFile(err.clone()))
    }
}

impl From<RootFileName> for ffi::OsString {
    fn from(name: RootFileName) -> ffi::OsString {
        name.0
    }
}

impl fmt::Display for RootFileName {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self.0.to_str() {
            Some(s) => write!(f, "{}", s),
            None => write!(f, "{:?}", self.0),
        }
    }
}

#[derive(Clone)]
struct LevelName(String);

impl From<(String, usize)> for LevelName {
    fn from((name, level): (String, usize)) -> LevelName {
        LevelName(format!("{}-dgmlevel-{}", name, level))
    }
}

impl TryFrom<LevelName> for (String, usize) {
    type Error = Error;

    fn try_from(name: LevelName) -> Result<(String, usize)> {
        let parts: Vec<&str> = name.0.split('-').collect();
        let err = Error::InvalidFile(format!("{} not dgm level", name));

        if parts.len() >= 3 {
            match &parts[parts.len() - 2..] {
                ["dgmlevel", level] => {
                    let level = level.parse::<usize>().map_err(|_| err)?;
                    let s = parts[..(parts.len() - 2)].join("-");
                    Ok((s, level))
                }
                _ => Err(err),
            }
        } else {
            Err(err)
        }
    }
}

impl fmt::Display for LevelName {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for LevelName {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{:?}", self.0)
    }
}

/// Maximum number of levels to be used for disk indexes.
pub const NLEVELS: usize = 16;

/// Default threshold between memory index footprint and
/// the latest disk index footprint, below which a newer level
/// shall be created, for commiting latest batch of entries.
/// Refer to [set_mem_ratio][Dgm::set_mem_ratio] method for details.
pub const MEM_RATIO: f64 = 0.5;

/// Default threshold between a disk index footprint and
/// the next-level disk index footprint, above which the two
/// levels shall be compacted into a single disk-index.
/// Refer to [set_disk_ratio][Dgm::set_disk_ratio] method for details.
pub const DISK_RATIO: f64 = 0.5;

/// Default interval in time duration, for invoking disk compaction
/// between dgm disk-levels.
/// Refer to [set_compact_interval][Dgm::set_compact_interval] method
/// for details.
pub const COMPACT_INTERVAL: Duration = Duration::from_secs(1800);

pub struct Dgm<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
    M::I: Footprint,
    D::I: Footprint + Clone,
{
    name: String,
    auto_compact: Option<rt::Thread<(), (), ()>>,
    inner: Arc<Mutex<InnerDgm<K, V, M, D>>>,
}

struct InnerDgm<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    dir: ffi::OsString,
    name: String,
    mem_factory: M,
    disk_factory: D,
    root_file: ffi::OsString,
    root: Root,

    m0: Snapshot<K, V, M::I>,         // write index
    m1: Option<Snapshot<K, V, M::I>>, // flush index
    disks: Vec<Snapshot<K, V, D::I>>, // NLEVELS

    writers: Vec<Arc<Mutex<<M::I as Index<K, V>>::W>>>,
    readers: Vec<Arc<Mutex<Rs<K, V, M, D>>>>,
}

impl<K, V, M, D> InnerDgm<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
    M::I: Index<K, V> + Footprint,
    D::I: Index<K, V> + Footprint + Clone,
{
    fn cleanup_writers(&mut self) -> Result<()> {
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

        Ok(())
    }

    fn cleanup_readers(&mut self) -> Result<()> {
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

        Ok(())
    }

    fn shift_in_m0(&mut self) -> Result<()> {
        // block all the readers.
        let mut rs = vec![];
        for r in self.readers.iter() {
            rs.push(r.lock().unwrap());
        }

        // shift memory snapshot into writers
        let m0 = self.shift_into_m0_writers(self.mem_factory.new(&self.name)?)?;
        let m0 = Snapshot::new_write(m0);

        self.m1 = match mem::replace(&mut self.m0, m0) {
            Snapshot::Write(m1) => Some(Snapshot::new_flush(m1)),
            _ => unreachable!(),
        };

        // update readers and unblock them one by one.
        for r in rs.iter_mut() {
            r.r_m0 = self.m0.as_mut_m0()?.to_reader()?;
            r.r_m1 = match &mut self.m1 {
                Some(m1) => Some(m1.as_mut_m1()?.to_reader()?),
                None => None,
            };

            r.r_disks.drain(..);
            for disk in self.disks.iter_mut() {
                if let Some(d) = disk.as_mut_disk()? {
                    r.r_disks.push(d.to_reader()?)
                }
            }
        }

        Ok(())
    }

    // should be called while holding the levels lock.
    fn shift_into_m0_writers(&self, mut m0: M::I) -> Result<M::I> {
        // block all the writer threads.
        let mut handles = vec![];
        for writer in self.writers.iter() {
            handles.push(writer.lock().unwrap())
        }

        // now replace old writer handle created from the new m0 snapshot.
        for handle in handles.iter_mut() {
            let _old_w = mem::replace(handle.deref_mut(), m0.to_writer()?);
            // drop the old writer,
        }

        // unblock writers on exit.
        Ok(m0)
    }

    fn commit_level(&mut self) -> Result<usize> {
        use Snapshot::{Active, Commit, Compact, Flush, Write};

        let msg = format!("dgm: exhausted all levels !!");

        if self.is_commit_exhausted() {
            return Err(Error::DiskIndexFail(msg));
        }

        let mf = self.m0.footprint()? as f64;
        let mut iter = self.disks.iter_mut().enumerate();
        loop {
            match iter.next() {
                None => break Ok(self.disks.len() - 1), // first commit
                Some((_, Snapshot::None)) => (),        // continue loop
                Some((lvl, disk)) => {
                    let df = disk.footprint()? as f64;
                    match disk {
                        Compact(_) => break Ok(lvl - 1),
                        Active(_) if (mf / df) < self.root.mem_ratio => {
                            if lvl == 0 {
                                break Err(Error::DiskIndexFail(msg));
                            } else {
                                break Ok(lvl - 1);
                            }
                        }
                        Active(_) => break Ok(lvl),
                        Write(_) | Flush(_) | Commit(_) => unreachable!(),
                        _ => unreachable!(),
                    }
                }
            }
        }
    }

    fn is_commit_exhausted(&self) -> bool {
        use Snapshot::{Active, Commit, Compact};

        match self.disks[0] {
            Snapshot::None => false,
            Active(_) => false,
            Commit(_) | Compact(_) => true,
            _ => unreachable!(),
        }
    }

    fn move_to_commit(&mut self, level: usize) -> Result<()> {
        let d = mem::replace(&mut self.disks[level], Default::default());
        let d = match d {
            Snapshot::Active(d) => d,
            Snapshot::None => {
                let name: LevelName = (self.name.clone(), level).into();
                let name = name.to_string();
                self.disk_factory.new(&self.dir, &name)?
            }
            _ => unreachable!(),
        };

        self.disks[level] = Snapshot::new_commit(d);
        Ok(())
    }

    fn active_compact_levels(disks: &[Snapshot<K, V, D::I>]) -> Vec<usize> {
        // ignore empty levels in the begining.
        let mut disks = disks
            .iter()
            .enumerate()
            .skip_while(|(_, disk)| match disk {
                Snapshot::None => true,
                _ => false,
            })
            .collect::<Vec<(usize, &Snapshot<K, V, D::I>)>>();

        let mut res = vec![];
        if disks.len() > 0 {
            // ignore the commit level.
            if disks[0].1.is_commit() {
                disks.remove(0);
            }
            // pick only active levels, skip empty levels, validate on the go.
            for (level, disk) in disks.iter() {
                match disk {
                    Snapshot::Active(_) => res.push(*level),
                    Snapshot::None => continue,
                    _ => unreachable!(),
                }
            }
        }

        res
    }

    fn find_compact_levels(
        disks: &[Snapshot<K, V, D::I>],
        disk_ratio: f64,
    ) -> Result<Option<(Vec<usize>, usize)>> {
        let mut levels = Self::active_compact_levels(disks);

        loop {
            match levels.len() {
                0 | 1 => {
                    let mut levels = Self::active_compact_levels(disks);
                    break if levels.len() > 0 {
                        Ok(Some((vec![], levels.remove(levels.len() - 1))))
                    } else {
                        Ok(None)
                    };
                }
                _n => {
                    let target_level = levels.remove(levels.len() - 1);
                    let ratio = {
                        let t_footprint = disks[target_level].footprint()?;
                        let mut footprint = 0;
                        for level in levels.clone().into_iter() {
                            footprint += disks[level].footprint()?;
                        }
                        (t_footprint as f64) / (footprint as f64)
                    };

                    if ratio > disk_ratio {
                        break Ok(Some((levels, target_level)));
                    }
                }
            }
        }
    }

    fn compact_levels(&mut self) -> Result<Option<(Vec<usize>, usize)>> {
        let levels = {
            let disk_ratio = self.root.disk_ratio;
            Self::find_compact_levels(&self.disks, disk_ratio)?
        };

        let (levels, src_levels, dst_level) = match levels {
            None => return Ok(None),
            Some((ss, d)) if ss.len() == 0 => (vec![d], ss, d),
            Some((ss, d)) => {
                let mut levels = ss.clone();
                levels.push(d);
                (levels, ss, d)
            }
        };

        for level in levels {
            let d = mem::replace(&mut self.disks[level], Default::default());
            let d = match d {
                Snapshot::Active(d) => d,
                _ => unreachable!(),
            };
            self.disks[level] = Snapshot::new_compact(d);
        }

        let d = mem::replace(&mut self.disks[dst_level], Default::default());
        let d = match d {
            Snapshot::Active(d) => d,
            _ => unreachable!(),
        };
        self.disks[dst_level] = Snapshot::new_compact(d);

        Ok(Some((src_levels, dst_level)))
    }

    fn repopulate_readers(&mut self) -> Result<()> {
        let mut r_diskss = vec![];
        for _ in 0..self.readers.len() {
            let mut r_disks = vec![];
            for disk in self.disks.iter_mut() {
                match disk.as_mut_disk()? {
                    Some(d) => r_disks.push(d.to_reader()?),
                    None => (),
                }
            }
            r_diskss.push(r_disks);
        }

        for readers in self.readers.iter() {
            let mut rs = readers.lock().unwrap();
            rs.r_m1 = None;
            rs.r_disks.drain(..);
            for r_disk in r_diskss.remove(0).into_iter() {
                rs.r_disks.push(r_disk)
            }
        }

        Ok(())
    }

    fn do_compact_commit(
        &mut self,
        src_levels: Vec<usize>,
        dst_level: usize, // target
    ) -> Result<(
        Vec<<<D as DiskIndexFactory<K, V>>::I as Index<K, V>>::R>,
        D::I,
    )> {
        assert!(
            src_levels.clone().into_iter().all(|l| l < dst_level),
            "src_levels:{:?} dst_level:{}",
            src_levels,
            dst_level
        );

        let mut rs = vec![];
        for level in src_levels.clone().into_iter() {
            let d = self.disks[level].as_mut_disk()?.unwrap();
            rs.push(d.to_reader()?);
        }

        let disk = self.disks[dst_level].as_disk()?.unwrap().clone();

        Ok((rs, disk))
    }
}

enum Snapshot<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
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
    // empty slot, TODO: better to replace this with Option<Snapshot> ?
    None,
    // ignore
    _Phantom(marker::PhantomData<K>, marker::PhantomData<V>),
}

impl<K, V, I> Default for Snapshot<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Index<K, V>,
{
    fn default() -> Snapshot<K, V, I> {
        Snapshot::None
    }
}

impl<K, V, I> fmt::Display for Snapshot<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
    I: Index<K, V>,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        use Snapshot::{Active, Commit, Compact, Flush, Write};

        match self {
            Write(m) => write!(f, "write/{}", m.to_name().unwrap()),
            Flush(m) => write!(f, "flush/{}", m.to_name().unwrap()),
            Commit(d) => write!(f, "commit/{}", d.to_name().unwrap()),
            Compact(d) => write!(f, "compact/{}", d.to_name().unwrap()),
            Active(d) => write!(f, "active/{}", d.to_name().unwrap()),
            Snapshot::None => write!(f, "none"),
            _ => unreachable!(),
        }
    }
}

impl<K, V, I> Footprint for Snapshot<K, V, I>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    I: Index<K, V> + Footprint,
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
    K: Clone + Ord,
    V: Clone + Diff,
    I: Index<K, V>,
{
    #[inline]
    fn new_write(index: I) -> Snapshot<K, V, I> {
        Snapshot::Write(index)
    }

    #[inline]
    fn new_flush(index: I) -> Snapshot<K, V, I> {
        Snapshot::Flush(index)
    }

    #[inline]
    fn new_active(index: I) -> Snapshot<K, V, I> {
        Snapshot::Active(index)
    }

    #[inline]
    fn new_commit(index: I) -> Snapshot<K, V, I> {
        Snapshot::Commit(index)
    }

    #[inline]
    fn new_compact(index: I) -> Snapshot<K, V, I> {
        Snapshot::Compact(index)
    }

    fn is_active(&self) -> bool {
        match self {
            Snapshot::Active(_) => true,
            _ => false,
        }
    }

    fn is_commit(&self) -> bool {
        match self {
            Snapshot::Commit(_) => true,
            _ => false,
        }
    }

    fn is_none(&self) -> bool {
        match self {
            Snapshot::None => true,
            _ => false,
        }
    }

    fn as_disk(&self) -> Result<Option<&I>> {
        use Snapshot::{Active, Commit, Compact, Flush, Write};

        match self {
            Commit(d) => Ok(Some(d)),
            Compact(d) => Ok(Some(d)),
            Active(d) => Ok(Some(d)),
            Snapshot::None => Ok(None),
            Write(_) | Flush(_) => {
                let msg = format!("dgm disk not commit/compact/active snapshot");
                Err(Error::UnexpectedFail(msg))
            }
            _ => unreachable!(),
        }
    }

    fn as_mut_disk(&mut self) -> Result<Option<&mut I>> {
        use Snapshot::{Active, Commit, Compact, Flush, Write};

        match self {
            Commit(d) => Ok(Some(d)),
            Compact(d) => Ok(Some(d)),
            Active(d) => Ok(Some(d)),
            Snapshot::None => Ok(None),
            Write(_) | Flush(_) => {
                let msg = format!("dgm disk not commit/compact/active snapshot");
                Err(Error::UnexpectedFail(msg))
            }
            _ => unreachable!(),
        }
    }

    fn as_m0(&self) -> Result<&I> {
        match self {
            Snapshot::Write(m) => Ok(m),
            _ => {
                let msg = format!("dgm m0 not a write snapshot");
                Err(Error::UnexpectedFail(msg))
            }
        }
    }

    fn as_mut_m0(&mut self) -> Result<&mut I> {
        match self {
            Snapshot::Write(m) => Ok(m),
            _ => {
                let msg = format!("dgm m0 not a write snapshot");
                Err(Error::UnexpectedFail(msg))
            }
        }
    }

    fn as_m1(&self) -> Result<&I> {
        match self {
            Snapshot::Flush(m) => Ok(m),
            _ => {
                let msg = format!("dgm m0 not a write snapshot");
                Err(Error::UnexpectedFail(msg))
            }
        }
    }

    fn as_mut_m1(&mut self) -> Result<&mut I> {
        match self {
            Snapshot::Flush(m) => Ok(m),
            _ => {
                let msg = format!("dgm m0 not a flush snapshot");
                Err(Error::UnexpectedFail(msg))
            }
        }
    }
}

impl<K, V, M, D> Drop for Dgm<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
    M::I: Footprint,
    D::I: Footprint + Clone,
{
    fn drop(&mut self) {
        loop {
            match self.as_inner() {
                Ok(nnr) => {
                    let w = nnr.writers.iter().any(|w| Arc::strong_count(w) > 1);
                    let r = nnr.readers.iter().any(|r| Arc::strong_count(r) > 1);
                    if w == false && r == false {
                        break;
                    }
                }
                Err(err) => {
                    error!(target: "dgm   ", "lock {:?}", err);
                    break;
                }
            };
            error!(target: "dgm   ", "{:?}, open read/write handles", self.name);
            thread::sleep(time::Duration::from_millis(10)); // TODO: no magic
        }

        match self.auto_compact.take() {
            Some(auto_compact) => match auto_compact.close_wait() {
                Err(err) => error!(
                    target: "dgm   ", "{:?}, auto-compact {:?}", self.name, err
                ),
                Ok(_) => (),
            },
            None => (),
        }
    }
}

impl<K, V, M, D> Dgm<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
    M::I: Footprint,
    D::I: Footprint + Clone,
{
    /// Create a new Dgm instance on disk. Supplied directory `dir` will be
    /// removed, if it already exist, and new directory shall be created.
    pub fn new(
        dir: &ffi::OsStr, // directory path
        name: &str,
        mem_factory: M,
        disk_factory: D,
        config: Config,
    ) -> Result<Box<Dgm<K, V, M, D>>>
    where
        K: 'static + Send,
        V: 'static + Send,
        M: 'static + Send,
        D: 'static + Send,
        <M as WriteIndexFactory<K, V>>::I: 'static + Send,
        <<M as WriteIndexFactory<K, V>>::I as Index<K, V>>::R: 'static + Send,
        <<M as WriteIndexFactory<K, V>>::I as Index<K, V>>::W: 'static + Send,
        <D as DiskIndexFactory<K, V>>::I: 'static + Send,
        <<D as DiskIndexFactory<K, V>>::I as Index<K, V>>::R: 'static + Send,
        <<D as DiskIndexFactory<K, V>>::I as Index<K, V>>::W: 'static + Send,
    {
        fs::remove_dir_all(dir)?;
        fs::create_dir_all(dir)?;

        let root: Root = config.clone().into();
        let root_file = Self::new_root_file(dir, name, root.clone())?;

        let disks = {
            let mut disks: Vec<Snapshot<K, V, D::I>> = vec![];
            (0..NLEVELS).for_each(|_| disks.push(Default::default()));
            disks
        };

        let m0 = Snapshot::new_write(mem_factory.new(name)?);
        let inner = InnerDgm {
            dir: dir.to_os_string(),
            name: name.to_string(),
            mem_factory,
            disk_factory,
            root_file,
            root,

            m0,
            m1: None,
            disks,

            writers: Default::default(),
            readers: Default::default(),
        };

        let mut index = Box::new(Dgm {
            name: name.to_string(),
            auto_compact: Default::default(),
            inner: Arc::new(Mutex::new(inner)),
        });

        index.start_auto_compact()?;

        Ok(index)
    }

    pub fn open(
        dir: &ffi::OsStr, // directory path
        name: &str,
        mem_factory: M,
        disk_factory: D,
    ) -> Result<Box<Dgm<K, V, M, D>>>
    where
        K: 'static + Send,
        V: 'static + Send,
        M: 'static + Send,
        D: 'static + Send,
        <M as WriteIndexFactory<K, V>>::I: 'static + Send,
        <<M as WriteIndexFactory<K, V>>::I as Index<K, V>>::R: 'static + Send,
        <<M as WriteIndexFactory<K, V>>::I as Index<K, V>>::W: 'static + Send,
        <D as DiskIndexFactory<K, V>>::I: 'static + Send,
        <<D as DiskIndexFactory<K, V>>::I as Index<K, V>>::R: 'static + Send,
        <<D as DiskIndexFactory<K, V>>::I as Index<K, V>>::W: 'static + Send,
    {
        let root = Self::find_root_file(dir, name)?;
        let root_file: RootFileName = (name.to_string(), root.version).into();

        let mut disks: Vec<Snapshot<K, V, D::I>> = vec![];
        (0..NLEVELS).for_each(|_| disks.push(Default::default()));

        for level in 0..root.levels {
            let level_name: LevelName = (name.to_string(), level).into();
            disks[level] = {
                let d = disk_factory.open(dir, &level_name.to_string())?;
                Snapshot::new_active(d)
            };
        }

        let config = root.clone().into();

        if disks.iter().any(|s| s.is_active()) == false {
            // no active disk snapshots found, create a new instance.
            Self::new(dir, name, mem_factory, disk_factory, config)
        } else {
            let m0 = Snapshot::new_write(mem_factory.new(name)?);
            let inner = InnerDgm {
                dir: dir.to_os_string(),
                name: name.to_string(),
                mem_factory,
                disk_factory,
                root_file: root_file.into(),
                root,

                m0,
                m1: None,
                disks,

                writers: Default::default(),
                readers: Default::default(),
            };

            let mut index = Box::new(Dgm {
                name: name.to_string(),
                auto_compact: Default::default(),
                inner: Arc::new(Mutex::new(inner)),
            });

            index.start_auto_compact()?;

            Ok(index)
        }
    }

    fn start_auto_compact(&mut self) -> Result<()>
    where
        K: 'static + Send,
        V: 'static + Send,
        M: 'static + Send,
        D: 'static + Send,
        <M as WriteIndexFactory<K, V>>::I: 'static + Send,
        <<M as WriteIndexFactory<K, V>>::I as Index<K, V>>::R: 'static + Send,
        <<M as WriteIndexFactory<K, V>>::I as Index<K, V>>::W: 'static + Send,
        <D as DiskIndexFactory<K, V>>::I: 'static + Send,
        <<D as DiskIndexFactory<K, V>>::I as Index<K, V>>::R: 'static + Send,
        <<D as DiskIndexFactory<K, V>>::I as Index<K, V>>::W: 'static + Send,
    {
        let root = {
            let inner = self.as_inner()?;
            inner.root.clone()
        };
        let name = {
            let inner = self.as_inner()?;
            inner.name.clone()
        };
        self.auto_compact = if root.compact_interval.as_secs() > 0 {
            let inner = Arc::clone(&self.inner);
            Some(rt::Thread::new(move |rx| {
                || auto_compact::<K, V, M, D>(name, root, inner, rx)
            }))
        } else {
            None
        };

        Ok(())
    }

    fn new_root_file(
        //
        dir: &ffi::OsStr,
        name: &str,
        root: Root,
    ) -> Result<ffi::OsString> {
        let root_file: ffi::OsString = {
            let rootf: RootFileName = (name.to_string().into(), 0_usize).into();
            let mut rootp = path::PathBuf::from(dir);
            rootp.push(&rootf.0);
            rootp.into_os_string()
        };

        let data: Vec<u8> = root.try_into()?;

        let mut fd = util::create_file_a(root_file.clone())?;
        fd.write(&data)?;
        Ok(root_file.into())
    }

    fn find_root_file(dir: &ffi::OsStr, name: &str) -> Result<Root> {
        use crate::error::Error::InvalidFile;

        let mut versions = vec![];
        for item in fs::read_dir(dir)? {
            match item {
                Ok(item) => {
                    let root_file = RootFileName(item.file_name());
                    match root_file.try_into() {
                        Ok((nm, ver)) if nm == name => versions.push(ver),
                        _ => continue,
                    }
                }
                _ => continue,
            }
        }

        let version = {
            let err = InvalidFile(format!("dgm, missing root file"));
            versions.into_iter().max().ok_or(err)
        }?;

        let root_file = {
            let file: RootFileName = (name.to_string(), version).into();
            let mut rootp = path::PathBuf::from(dir);
            rootp.push(&file.0);
            rootp.into_os_string()
        };

        let mut fd = util::open_file_r(&root_file)?;
        let mut bytes = vec![];
        fd.read_to_end(&mut bytes)?;

        Ok(bytes.try_into()?)
    }
}

impl<K, V, M, D> Dgm<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
    M::I: Footprint,
    D::I: Footprint + Clone,
{
    fn as_inner(&self) -> Result<MutexGuard<InnerDgm<K, V, M, D>>> {
        match self.inner.lock() {
            Ok(value) => Ok(value),
            Err(err) => {
                let msg = format!("Dgm.as_inner(), poisonlock {:?}", err);
                Err(Error::ThreadFail(msg))
            }
        }
    }

    fn disk_footprint(&self) -> Result<isize> {
        let inner = self.as_inner()?;

        let mut footprint: isize = Default::default();
        for disk in inner.disks.iter() {
            footprint += disk.footprint()?;
        }
        Ok(footprint)
    }

    fn mem_footprint(&self) -> Result<isize> {
        let inner = self.as_inner()?;

        Ok(inner.m0.footprint()?
            + match &inner.m1 {
                None => 0,
                Some(m) => m.footprint()?,
            })
    }

    fn to_disk_seqno(&self) -> Result<u64> {
        let inner = self.as_inner()?;

        for d in inner.disks.iter() {
            match d.as_disk()? {
                Some(d) => return d.to_seqno(),
                None => (),
            }
        }

        Ok(std::u64::MIN)
    }

    fn do_compact<F>(
        inner: &Arc<Mutex<InnerDgm<K, V, M, D>>>,
        cutoff: Cutoff,
        metacb: F,
    ) -> Result<usize>
    where
        F: Fn(Vec<u8>) -> Vec<u8>,
    {
        {
            let mut inn = to_inner_lock(inner)?;
            inn.cleanup_writers()?;
            inn.cleanup_readers()?;
        }

        let (src_levels, dst_level) = {
            let mut inn = to_inner_lock(inner)?;

            match inn.compact_levels()? {
                None => return Ok(0),
                Some((src_levels, dst_level)) => (src_levels, dst_level),
            }
        };

        if src_levels.len() == 0 {
            let mut high_disk = {
                let inn = to_inner_lock(inner)?;
                inn.disks[dst_level].as_disk()?.unwrap().clone()
            };
            high_disk.compact(cutoff, metacb)
        } else {
            let (mut rs, disk) = {
                let mut inn = to_inner_lock(inner)?;
                inn.do_compact_commit(src_levels.clone(), dst_level)?
            };
            let no_reverse = false;

            let mut rs_refs: Vec<&mut <<D as DiskIndexFactory<K, V>>::I as Index<K, V>>::R> =
                rs.iter_mut().collect();
            let mut y_iter = rs_refs.remove(0).iter_with_versions()?;
            while rs_refs.len() > 0 {
                let iter = rs_refs.remove(0).iter_with_versions()?;
                y_iter = lsm::y_iter_versions(y_iter, iter, no_reverse);
            }

            let todo = "check below";
            // disk.commit(y_iter, metacb)?; TODO

            {
                let mut inn = to_inner_lock(inner)?;

                // update the disk-levels.
                for level in src_levels.clone().into_iter() {
                    mem::replace(&mut inn.disks[level], Default::default());
                }
                let disk = Snapshot::new_active(disk);
                mem::replace(&mut inn.disks[dst_level], disk);

                inn.repopulate_readers()?;
            }

            Ok(0)
        }
    }
}

impl<K, V, M, D> Footprint for Dgm<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
    M::I: Footprint,
    D::I: Footprint + Clone,
{
    fn footprint(&self) -> Result<isize> {
        Ok(self.disk_footprint()? + self.mem_footprint()?)
    }
}

impl<K, V, M, D> Index<K, V> for Dgm<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
    M::I: Footprint,
    D::I: Footprint + Clone,
    <M::I as Index<K, V>>::R: CommitIterator<K, V>,
{
    type W = DgmWriter<K, V, <M::I as Index<K, V>>::W>;
    type R = DgmReader<K, V, M, D>;

    fn to_name(&self) -> Result<String> {
        let inner = self.as_inner()?;

        Ok(inner.name.clone())
    }

    fn to_metadata(&self) -> Result<Vec<u8>> {
        let inner = self.as_inner()?;

        inner.m0.as_m0()?.to_metadata()
    }

    fn to_seqno(&self) -> Result<u64> {
        let inner = self.as_inner()?;

        inner.m0.as_m0()?.to_seqno()
    }

    fn set_seqno(&mut self, seqno: u64) -> Result<()> {
        let mut inner = self.as_inner()?;

        inner.m0.as_mut_m0()?.set_seqno(seqno)
    }

    fn to_writer(&mut self) -> Result<Self::W> {
        // create a new set of snapshot-reader
        let mut inner = self.as_inner()?;

        let w = inner.m0.as_mut_m0()?.to_writer()?;
        let arc_w = Arc::new(Mutex::new(w));
        inner.writers.push(Arc::clone(&arc_w));
        Ok(DgmWriter::new(&inner.name, arc_w))
    }

    fn to_reader(&mut self) -> Result<Self::R> {
        let mut inner = self.as_inner()?;

        let r_m0 = inner.m0.as_mut_m0()?.to_reader()?;

        let r_m1 = match inner.m1.as_mut() {
            Some(m) => Some(m.as_mut_m1()?.to_reader()?),
            None => None,
        };

        let mut r_disks = vec![];
        for disk in inner.disks.iter_mut() {
            match disk.as_mut_disk()? {
                Some(d) => r_disks.push(d.to_reader()?),
                None => (),
            }
        }

        let rs = Rs {
            r_m0,
            r_m1,
            r_disks,
        };

        let arc_rs = Arc::new(Mutex::new(rs));
        inner.readers.push(Arc::clone(&arc_rs));
        Ok(DgmReader::new(&inner.name, arc_rs))
    }

    fn commit<C, F>(&mut self, _: CommitIter<K, V, C>, metacb: F) -> Result<()>
    where
        C: CommitIterator<K, V>,
        F: Fn(Vec<u8>) -> Vec<u8>,
    {
        {
            let mut inner = self.as_inner()?;
            inner.cleanup_writers()?;
            inner.cleanup_readers()?;
        }

        if self.to_disk_seqno()? == self.to_seqno()? {
            return Ok(());
        }

        let (mut d, r_m1, level) = {
            let mut inner = self.as_inner()?;

            inner.shift_in_m0();
            let level = inner.commit_level()?;

            inner.move_to_commit(level)?;

            let d = inner.disks[level].as_disk()?.unwrap().clone();

            let r_m1 = match &mut inner.m1 {
                Some(m1) => Some(m1.as_mut_m1()?.to_reader()?),
                None => None,
            };
            (d, r_m1, level)
        };

        let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
        match r_m1 {
            Some(r_m1) => {
                let iter = core::CommitIter::new(r_m1, within);
                d.commit(iter, metacb)?;
            }
            None => (),
        }

        {
            let mut inner = self.as_inner()?;
            let disk = Snapshot::new_active(d);
            mem::replace(&mut inner.disks[level], disk);
            inner.m1 = None;
            inner.repopulate_readers()?;
        }

        Ok(())
    }

    fn compact<F>(&mut self, cutoff: Cutoff, metacb: F) -> Result<usize>
    where
        F: Fn(Vec<u8>) -> Vec<u8>,
    {
        Self::do_compact(&self.inner, cutoff, metacb)
    }

    fn close(self) -> Result<()> {
        unimplemented!()
    }

    fn purge(self) -> Result<()> {
        unimplemented!()
    }
}

pub struct DgmWriter<K, V, W>
where
    K: Clone + Ord,
    V: Clone + Diff,
    W: Writer<K, V>,
{
    name: String,
    w: Arc<Mutex<W>>,

    _phantom_key: marker::PhantomData<K>,
    _phantom_val: marker::PhantomData<V>,
}

impl<K, V, W> DgmWriter<K, V, W>
where
    K: Clone + Ord,
    V: Clone + Diff,
    W: Writer<K, V>,
{
    fn new(name: &str, w: Arc<Mutex<W>>) -> DgmWriter<K, V, W> {
        DgmWriter {
            name: name.to_string(),
            w,

            _phantom_key: marker::PhantomData,
            _phantom_val: marker::PhantomData,
        }
    }

    fn as_writer(&self) -> Result<MutexGuard<W>> {
        match self.w.lock() {
            Ok(value) => Ok(value),
            Err(err) => {
                let msg = format!("DgmWriter.as_writer(), poisonlock {:?}", err);
                Err(Error::ThreadFail(msg))
            }
        }
    }
}

impl<K, V, W> Writer<K, V> for DgmWriter<K, V, W>
where
    K: Clone + Ord,
    V: Clone + Diff,
    W: Writer<K, V>,
{
    fn set(&mut self, key: K, value: V) -> Result<Option<Entry<K, V>>> {
        let mut w = self.as_writer()?;
        w.set(key, value)
    }

    fn set_cas(&mut self, k: K, v: V, cas: u64) -> Result<Option<Entry<K, V>>> {
        let mut w = self.as_writer()?;
        w.set_cas(k, v, cas)
    }

    fn delete<Q>(&mut self, key: &Q) -> Result<Option<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        let mut w = self.as_writer()?;
        w.delete(key)
    }
}

// type alias to reader associated type for each snapshot (aka disk-index)
struct Rs<K, V, M, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    r_m0: <M::I as Index<K, V>>::R,
    r_m1: Option<<M::I as Index<K, V>>::R>,
    r_disks: Vec<<D::I as Index<K, V>>::R>,
}

pub struct DgmReader<K, V, M, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    name: String,
    rs: Arc<Mutex<Rs<K, V, M, D>>>,

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
    fn new(name: &str, rs: Arc<Mutex<Rs<K, V, M, D>>>) -> DgmReader<K, V, M, D> {
        DgmReader {
            name: name.to_string(),
            rs,

            _phantom_key: marker::PhantomData,
            _phantom_val: marker::PhantomData,
        }
    }

    fn as_reader(&self) -> Result<MutexGuard<Rs<K, V, M, D>>> {
        match self.rs.lock() {
            Ok(value) => Ok(value),
            Err(err) => {
                let msg = format!("DgmReader.as_reader(), poisonlock {:?}", err);
                Err(Error::ThreadFail(msg))
            }
        }
    }

    fn merge_iters<'a>(
        mut iters: Vec<IndexIter<'a, K, V>>,
        reverse: bool,
        versions: bool,
    ) -> IndexIter<'a, K, V>
    where
        K: 'a,
        V: 'a,
    {
        match iters.len() {
            0 => unreachable!(),
            1 => iters.remove(0),
            _ => {
                let mut iter = iters.remove(0);
                for iter1 in iters.into_iter() {
                    iter = if versions {
                        lsm::y_iter_versions(iter, iter1, reverse)
                    } else {
                        lsm::y_iter(iter, iter1, reverse)
                    };
                }
                iter
            }
        }
    }
}

impl<K, V, M, D> Reader<K, V> for DgmReader<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    fn get<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + Hash,
    {
        let mut rs = self.as_reader()?;

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
                Some(disk) => match disk.get(key) {
                    Ok(entry) => break Ok(entry),
                    Err(Error::KeyNotFound) => (),
                    Err(err) => break Err(err),
                },
                None => break Err(Error::KeyNotFound),
            }
        }
    }

    fn iter(&mut self) -> Result<IndexIter<K, V>> {
        let mut rs = self.as_reader()?;

        let mut iters: Vec<IndexIter<K, V>> = vec![];

        unsafe {
            let m0 = &mut rs.r_m0 as *mut <M::I as Index<K, V>>::R;
            iters.push(m0.as_mut().unwrap().iter()?)
        }

        if let Some(m1) = &mut rs.r_m1 {
            unsafe {
                let m1 = m1 as *mut <M::I as Index<K, V>>::R;
                iters.push(m1.as_mut().unwrap().iter()?);
            }
        }

        for disk in rs.r_disks.iter_mut() {
            unsafe {
                let disk = disk as *mut <D::I as Index<K, V>>::R;
                iters.push(disk.as_mut().unwrap().iter()?);
            }
        }

        let iter = Self::merge_iters(iters, false /*reverse*/, false /*ver*/);
        Ok(Box::new(DgmIter::new(self, rs, iter)))
    }

    fn range<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let mut rs = self.as_reader()?;

        let mut iters: Vec<IndexIter<K, V>> = vec![];

        unsafe {
            let m0 = &mut rs.r_m0 as *mut <M::I as Index<K, V>>::R;
            iters.push(m0.as_mut().unwrap().range(range.clone())?)
        }

        if let Some(m1) = &mut rs.r_m1 {
            unsafe {
                let m1 = m1 as *mut <M::I as Index<K, V>>::R;
                iters.push(m1.as_mut().unwrap().range(range.clone())?)
            };
        }

        for disk in rs.r_disks.iter_mut().rev() {
            unsafe {
                let disk = disk as *mut <D::I as Index<K, V>>::R;
                iters.push(disk.as_mut().unwrap().range(range.clone())?)
            }
        }

        let iter = Self::merge_iters(iters, false /*reverse*/, false /*ver*/);
        Ok(Box::new(DgmIter::new(self, rs, iter)))
    }

    fn reverse<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let mut rs = self.as_reader()?;

        let mut iters: Vec<IndexIter<K, V>> = vec![];

        unsafe {
            let m0 = &mut rs.r_m0 as *mut <M::I as Index<K, V>>::R;
            iters.push(m0.as_mut().unwrap().reverse(range.clone())?)
        };

        if let Some(m1) = &mut rs.r_m1 {
            unsafe {
                let m1 = m1 as *mut <M::I as Index<K, V>>::R;
                iters.push(m1.as_mut().unwrap().reverse(range.clone())?)
            };
        }

        for disk in rs.r_disks.iter_mut().rev() {
            unsafe {
                let disk = disk as *mut <D::I as Index<K, V>>::R;
                iters.push(disk.as_mut().unwrap().reverse(range.clone())?)
            };
        }

        let iter = Self::merge_iters(iters, true /*reverse*/, false /*ver*/);
        Ok(Box::new(DgmIter::new(self, rs, iter)))
    }

    fn get_with_versions<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + Hash,
    {
        let mut rs = self.as_reader()?;

        let m0_entry = match rs.r_m0.get_with_versions(key) {
            Ok(entry) => Ok(Some(entry)),
            Err(Error::KeyNotFound) => Ok(None),
            Err(err) => Err(err),
        }?;

        let mut entry = match &mut rs.r_m1 {
            Some(m1) => match (m1.get_with_versions(key), m0_entry) {
                (Ok(m1_e), Some(m0_e)) => Ok(Some(m0_e.xmerge(m1_e)?)),
                (Ok(m1_e), None) => Ok(Some(m1_e)),
                (Err(Error::KeyNotFound), Some(m0_e)) => Ok(Some(m0_e)),
                (Err(Error::KeyNotFound), None) => Ok(None),
                (Err(err), _) => Err(err),
            },
            None => Ok(m0_entry),
        }?;

        let mut iter = rs.r_disks.iter_mut();
        let entry = loop {
            entry = match iter.next() {
                Some(disk) => match (disk.get_with_versions(key), entry) {
                    (Ok(e), Some(entry)) => Ok(Some(entry.xmerge(e)?)),
                    (Ok(e), None) => Ok(Some(e)),
                    (Err(Error::KeyNotFound), Some(entry)) => Ok(Some(entry)),
                    (Err(err), _) => Err(err),
                },
                None => break entry,
            }?;
        };

        entry.ok_or(Error::KeyNotFound)
    }

    fn iter_with_versions(&mut self) -> Result<IndexIter<K, V>> {
        let mut rs = self.as_reader()?;

        let mut iters: Vec<IndexIter<K, V>> = vec![];

        unsafe {
            let m0 = &mut rs.r_m0 as *mut <M::I as Index<K, V>>::R;
            iters.push(m0.as_mut().unwrap().iter_with_versions()?)
        }

        if let Some(m1) = &mut rs.r_m1 {
            unsafe {
                let m1 = m1 as *mut <M::I as Index<K, V>>::R;
                iters.push(m1.as_mut().unwrap().iter_with_versions()?);
            }
        }

        for disk in rs.r_disks.iter_mut() {
            unsafe {
                let disk = disk as *mut <D::I as Index<K, V>>::R;
                iters.push(disk.as_mut().unwrap().iter_with_versions()?);
            }
        }

        let iter = Self::merge_iters(iters, false /*reverse*/, true /*ver*/);
        Ok(Box::new(DgmIter::new(self, rs, iter)))
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
        let mut rs = self.as_reader()?;

        let mut iters: Vec<IndexIter<K, V>> = vec![];

        unsafe {
            let m0 = &mut rs.r_m0 as *mut <M::I as Index<K, V>>::R;
            iters.push(m0.as_mut().unwrap().range_with_versions(range.clone())?)
        }

        if let Some(m1) = &mut rs.r_m1 {
            unsafe {
                let m1 = m1 as *mut <M::I as Index<K, V>>::R;
                let range = range.clone();
                iters.push(m1.as_mut().unwrap().range_with_versions(range)?)
            };
        }

        for disk in rs.r_disks.iter_mut().rev() {
            unsafe {
                let disk = disk as *mut <D::I as Index<K, V>>::R;
                let range = range.clone();
                iters.push(disk.as_mut().unwrap().range_with_versions(range)?)
            }
        }

        let iter = Self::merge_iters(iters, false /*reverse*/, true /*ver*/);
        Ok(Box::new(DgmIter::new(self, rs, iter)))
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
        let mut rs = self.as_reader()?;

        let mut iters: Vec<IndexIter<K, V>> = vec![];

        unsafe {
            let m0 = &mut rs.r_m0 as *mut <M::I as Index<K, V>>::R;
            let range = range.clone();
            iters.push(m0.as_mut().unwrap().reverse_with_versions(range)?)
        };

        if let Some(m1) = &mut rs.r_m1 {
            unsafe {
                let m1 = m1 as *mut <M::I as Index<K, V>>::R;
                let range = range.clone();
                iters.push(m1.as_mut().unwrap().reverse_with_versions(range)?)
            };
        }

        for disk in rs.r_disks.iter_mut().rev() {
            unsafe {
                let disk = disk as *mut <D::I as Index<K, V>>::R;
                let range = range.clone();
                iters.push(disk.as_mut().unwrap().reverse_with_versions(range)?)
            };
        }

        let iter = Self::merge_iters(iters, true /*reverse*/, true /*ver*/);
        Ok(Box::new(DgmIter::new(self, rs, iter)))
    }
}

struct DgmIter<'a, K, V, M, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    _dgmr: &'a DgmReader<K, V, M, D>,
    _rs: MutexGuard<'a, Rs<K, V, M, D>>,
    iter: IndexIter<'a, K, V>,
}

impl<'a, K, V, M, D> DgmIter<'a, K, V, M, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    fn new(
        _dgmr: &'a DgmReader<K, V, M, D>,
        _rs: MutexGuard<'a, Rs<K, V, M, D>>,
        iter: IndexIter<'a, K, V>,
    ) -> DgmIter<'a, K, V, M, D> {
        DgmIter { _dgmr, _rs, iter }
    }
}

impl<'a, K, V, M, D> Iterator for DgmIter<'a, K, V, M, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

fn auto_compact<K, V, M, D>(
    name: String,
    root: Root,
    inner: Arc<Mutex<InnerDgm<K, V, M, D>>>,
    rx: rt::Rx<(), ()>,
) -> Result<()>
where
    K: 'static + Send + Clone + Ord + Serialize + Footprint,
    V: 'static + Send + Clone + Diff + Serialize + Footprint,
    M: 'static + Send + WriteIndexFactory<K, V>,
    D: 'static + Send + DiskIndexFactory<K, V>,
    <M as WriteIndexFactory<K, V>>::I: 'static + Send + Footprint,
    <<M as WriteIndexFactory<K, V>>::I as Index<K, V>>::R: 'static + Send,
    <<M as WriteIndexFactory<K, V>>::I as Index<K, V>>::W: 'static + Send,
    <D as DiskIndexFactory<K, V>>::I: 'static + Send + Footprint + Clone,
    <<D as DiskIndexFactory<K, V>>::I as Index<K, V>>::R: 'static + Send,
    <<D as DiskIndexFactory<K, V>>::I as Index<K, V>>::W: 'static + Send,
{
    info!(
        target: "dgm   ",
        "{}, auto-compacting thread started with interval {:?}",
        name, root.compact_interval,
    );

    let mut elapsed = Duration::new(0, 0);
    loop {
        let interval = {
            let interval = ((root.compact_interval * 2) + elapsed) / 2;
            cmp::min(interval, elapsed)
        };
        match rx.recv_timeout(interval) {
            Ok(_) => unreachable!(),
            Err(mpsc::RecvTimeoutError::Timeout) => (),
            Err(mpsc::RecvTimeoutError::Disconnected) => break Ok(()),
        }

        let start = SystemTime::now();

        let res = Dgm::do_compact(
            //
            &inner,
            Cutoff::new_lsm_empty(),
            convert::identity,
        );

        match res {
            Ok(_) => info!(target: "dgm   ", "{:?}, compaction done", name),
            Err(err) => info!(
                target: "dgm   ", "{:?}, compaction err, {:?}", name, err
            ),
        }

        elapsed = start.elapsed().ok().unwrap();
    }
}

fn to_inner_lock<K, V, M, D>(
    inner: &Arc<Mutex<InnerDgm<K, V, M, D>>>,
) -> Result<MutexGuard<InnerDgm<K, V, M, D>>>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
    M::I: Footprint,
    D::I: Footprint + Clone,
{
    match inner.lock() {
        Ok(value) => Ok(value),
        Err(err) => {
            let msg = format!("Dgm.as_inner(), poisonlock {:?}", err);
            Err(Error::ThreadFail(msg))
        }
    }
}

//#[cfg(test)]
//#[path = "dgm_test.rs"]
//mod dgm_test;
