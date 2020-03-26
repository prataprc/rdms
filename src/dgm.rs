//! Module `dgm` implement data-indexing optimized for
//! disk-greater-than-memory.

// TODO: Delete/Set/SetCAS does lookup into disk snapshot for the
// old value, which means returned value can be None, while there
// is an older value. May be we have to provide a separate API ?

use log::{debug, error, info};
use toml;

use std::{
    borrow::Borrow,
    cmp,
    convert::{TryFrom, TryInto},
    ffi, fmt, fs,
    hash::Hash,
    io::{Read, Write},
    marker, mem,
    ops::{Bound, DerefMut, RangeBounds},
    path, result,
    sync::{mpsc, Arc, Mutex, MutexGuard},
    thread, time,
};

use crate::{
    core::{self, Cutoff, Validate, Writer},
    core::{CommitIter, CommitIterator, Result, Serialize, WriteIndexFactory},
    core::{Diff, DiskIndexFactory, Entry, Footprint, Index, IndexIter, Reader},
    error::Error,
    lsm, scans, thread as rt, util,
};

const N_COMMITS: usize = 2;

/// Configuration type for Dgm indexes.
#[derive(Clone, Debug, PartialEq)]
pub struct Config {
    lsm: bool,
    m0_limit: Option<usize>,
    mem_ratio: f64,
    disk_ratio: f64,
    commit_interval: Option<time::Duration>,
    compact_interval: Option<time::Duration>,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            lsm: false,
            m0_limit: Default::default(),
            mem_ratio: Self::MEM_RATIO,
            disk_ratio: Self::DISK_RATIO,
            commit_interval: Some(Self::COMMIT_INTERVAL),
            compact_interval: Some(Self::COMPACT_INTERVAL),
        }
    }
}

impl Config {
    /// Maximum number of levels to be used for disk indexes.
    pub const NLEVELS: usize = 16;

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

    /// Default interval in time duration, for invoking memory commit
    /// between m0 level and disk level.
    /// Refer to [set_commit_interval][Config::set_commit_interval] method
    /// for details.
    pub const COMMIT_INTERVAL: time::Duration = time::Duration::from_secs(2);

    /// Default interval in time duration, for invoking disk compaction
    /// between dgm disk-levels.
    /// Refer to [set_compact_interval][Config::set_compact_interval] method
    /// for details.
    pub const COMPACT_INTERVAL: time::Duration = time::Duration::from_secs(10);

    /// Set entire Dgm index for log-structured-merge. This means
    /// the oldest level (snapshot) will preserve all previous mutations
    /// to an entry, until they are compacted off with cutoff.
    pub fn set_lsm(&mut self, lsm: bool) -> Result<&mut Self> {
        self.lsm = lsm;
        Ok(self)
    }

    /// Set maximum footprint for m0 level, beyond which a commit
    /// shall be triggered.
    pub fn set_m0_limit(&mut self, limit: usize) -> Result<&mut Self> {
        self.m0_limit = Some(limit);
        Ok(self)
    }

    /// Set threshold between memory index footprint and the latest disk
    /// index footprint, below which a newer level shall be created,
    /// for commiting new entries.
    pub fn set_mem_ratio(&mut self, ratio: f64) -> Result<&mut Self> {
        self.mem_ratio = ratio;
        Ok(self)
    }

    /// Set threshold between a disk index footprint and the next-level disk
    /// index footprint, above which the two levels shall be compacted
    /// into a single index.
    pub fn set_disk_ratio(&mut self, ratio: f64) -> Result<&mut Self> {
        self.disk_ratio = ratio;
        Ok(self)
    }

    /// Set interval in time duration, for commiting memory batch into
    /// disk snapshot. Calling this method will spawn an auto
    /// compaction thread.
    pub fn set_commit_interval(&mut self, interval: time::Duration) -> Result<&mut Self> {
        self.commit_interval = Some(interval);
        Ok(self)
    }

    /// Set interval in time duration, for invoking disk compaction
    /// between dgm disk-levels. Calling this method will spawn an auto
    /// compaction thread.
    pub fn set_compact_interval(&mut self, interval: time::Duration) -> Result<&mut Self> {
        self.compact_interval = Some(interval);
        Ok(self)
    }
}

impl From<Root> for Config {
    fn from(root: Root) -> Config {
        Config {
            lsm: root.lsm,
            m0_limit: root.m0_limit,
            mem_ratio: root.mem_ratio,
            disk_ratio: root.disk_ratio,
            commit_interval: root.commit_interval,
            compact_interval: root.compact_interval,
        }
    }
}

#[derive(Clone, Default, Debug, PartialEq)]
struct Root {
    version: usize,
    levels: usize,
    lsm_cutoff: Option<Bound<u64>>,
    tombstone_cutoff: Option<Bound<u64>>,

    lsm: bool,
    m0_limit: Option<usize>,
    mem_ratio: f64,
    disk_ratio: f64,
    commit_interval: Option<time::Duration>,  // in seconds.
    compact_interval: Option<time::Duration>, // in seconds.
}

impl From<Config> for Root {
    fn from(config: Config) -> Root {
        Root {
            version: 0,
            levels: Config::NLEVELS,
            lsm_cutoff: Default::default(),
            tombstone_cutoff: Default::default(),

            lsm: config.lsm,
            m0_limit: config.m0_limit,
            mem_ratio: config.mem_ratio,
            disk_ratio: config.disk_ratio,
            commit_interval: config.commit_interval,
            compact_interval: config.compact_interval,
        }
    }
}

impl TryFrom<Root> for Vec<u8> {
    type Error = Error;

    fn try_from(root: Root) -> Result<Vec<u8>> {
        use toml::Value::{self, Array, Boolean, Float, Integer, String as S};

        let text = {
            let mut dict = toml::map::Map::new();

            let version: i64 = convert_at!(root.version)?;
            let levels: i64 = convert_at!(root.levels)?;
            let m0_limit: i64 = match root.m0_limit {
                Some(m0_limit) => convert_at!(m0_limit)?,
                None => -1,
            };
            let mem_ratio: f64 = root.mem_ratio.into();
            let disk_ratio: f64 = root.disk_ratio.into();
            let m_interval: i64 = match root.commit_interval {
                Some(interval) => convert_at!(interval.as_secs())?,
                None => -1,
            };
            let c_interval: i64 = match root.compact_interval {
                Some(interval) => convert_at!(interval.as_secs())?,
                None => -1,
            };

            dict.insert("version".to_string(), Integer(version));
            dict.insert("levels".to_string(), Integer(levels));

            dict.insert("lsm".to_string(), Boolean(root.lsm));
            dict.insert("m0_limit".to_string(), Integer(m0_limit));
            dict.insert("mem_ratio".to_string(), Float(mem_ratio));
            dict.insert("disk_ratio".to_string(), Float(disk_ratio));
            dict.insert("commit_interval".to_string(), Integer(m_interval));
            dict.insert("compact_interval".to_string(), Integer(c_interval));

            let (arg1, arg2) = match root.lsm_cutoff {
                Some(cutoff) => match cutoff {
                    Bound::Excluded(cutoff) => Ok(("excluded", cutoff)),
                    Bound::Included(cutoff) => Ok(("included", cutoff)),
                    Bound::Unbounded => {
                        err_at!(Fatal, msg: format!("root has Unbounded lsm-cutoff"))
                    }
                },
                None => Ok(("none", 0)),
            }?;
            dict.insert(
                "lsm_cutoff".to_string(),
                Array(vec![S(arg1.to_string()), S(arg2.to_string())]),
            );

            let (arg1, arg2) = match root.tombstone_cutoff {
                Some(cutoff) => match cutoff {
                    Bound::Excluded(cutoff) => Ok(("excluded", cutoff)),
                    Bound::Included(cutoff) => Ok(("included", cutoff)),
                    Bound::Unbounded => {
                        err_at!(Fatal, msg: format!("root has Unbounded lsm-cutoff"))
                    }
                },
                None => Ok(("none", 0)),
            }?;
            dict.insert(
                "tombstone_cutoff".to_string(),
                Array(vec![S(arg1.to_string()), S(arg2.to_string())]),
            );

            Value::Table(dict).to_string()
        };

        Ok(text.as_bytes().to_vec())
    }
}

impl TryFrom<Vec<u8>> for Root {
    type Error = crate::error::Error;

    fn try_from(bytes: Vec<u8>) -> Result<Root> {
        use std::str::from_utf8;

        let to_usize = |key: &str, dict: &toml::value::Table| -> Result<usize> {
            match dict.get(key) {
                Some(field) => match field.as_integer() {
                    Some(field) => convert_at!(field),
                    None => err_at!(InvalidFile, msg: format!("{}:{}", key, field)),
                },
                None => err_at!(InvalidFile, msg: format!("{} in root", key)),
            }
        };
        let to_i64 = |key: &str, dict: &toml::value::Table| -> Result<i64> {
            match dict.get(key) {
                Some(field) => match field.as_integer() {
                    Some(field) => Ok(field),
                    None => err_at!(InvalidFile, msg: format!("{}:{}", key, field)),
                },
                None => err_at!(InvalidFile, msg: format!("{} in root", key)),
            }
        };
        let to_f64 = |key: &str, dict: &toml::value::Table| -> Result<f64> {
            match dict.get(key) {
                Some(field) => match field.as_float() {
                    Some(value) => Ok(value),
                    None => err_at!(InvalidFile, msg: format!("{}:{}", key, field)),
                },
                None => err_at!(InvalidFile, msg: format!("{} in root", key)),
            }
        };
        let to_bool = |key: &str, dict: &toml::value::Table| -> Result<bool> {
            match dict.get(key) {
                Some(field) => match field.as_bool() {
                    Some(value) => Ok(value),
                    None => err_at!(InvalidFile, msg: format!("{}:{}", key, field)),
                },
                None => err_at!(InvalidFile, msg: format!("{} in root", key)),
            }
        };

        let text = err_at!(InvalidFile, from_utf8(&bytes))?.to_string();
        let value = parse_at!(text, toml::Value)?;
        let dict = match value.as_table() {
            Some(table) => Ok(table),
            None => err_at!(InvalidFile, msg: format!("no table")),
        }?;
        let mut root: Root = Default::default();

        root.version = to_usize("version", dict)?;
        root.levels = to_usize("levels", dict)?;
        root.lsm = to_bool("lsm", dict)?;
        root.m0_limit = match to_i64("m0_limit", dict)? {
            m0_limit if m0_limit < 0 => None,
            m0_limit => Some(convert_at!(m0_limit)?),
        };
        root.mem_ratio = to_f64("mem_ratio", dict)?;
        root.disk_ratio = to_f64("disk_ratio", dict)?;
        root.commit_interval = match to_i64("commit_interval", dict)? {
            duration if duration < 0 => None,
            duration => Some(time::Duration::from_secs(convert_at!(duration)?)),
        };
        root.compact_interval = match to_i64("compact_interval", dict)? {
            duration if duration < 0 => None,
            duration => Some(time::Duration::from_secs(convert_at!(duration)?)),
        };
        root.lsm_cutoff = match dict.get("lsm_cutoff") {
            Some(field) => match field.as_array() {
                Some(array) => match array.as_slice() {
                    [bound, cutoff] => match (bound.as_str(), cutoff.as_str()) {
                        (Some("excluded"), Some(c)) => {
                            Ok(Some(Bound::Excluded(parse_at!(c, u64)?)))
                        }
                        (Some("included"), Some(c)) => {
                            Ok(Some(Bound::Included(parse_at!(c, u64)?)))
                        }
                        (Some("unbounded"), _) => Ok(Some(Bound::Unbounded)),
                        (Some("none"), _) => Ok(None),
                        (b, c) => err_at!(InvalidFile, msg: format!("b:{:?} c:{:?}", b, c)),
                    },
                    _ => err_at!(InvalidFile, msg: format!("invalid root")),
                },
                None => err_at!(InvalidFile, msg: format!("invalid root")),
            },
            None => err_at!(InvalidFile, msg: format!("invalid root")),
        }?;
        root.tombstone_cutoff = match dict.get("tombstone_cutoff") {
            Some(field) => match field.as_array() {
                Some(array) => match array.as_slice() {
                    [bound, cutoff] => match (bound.as_str(), cutoff.as_str()) {
                        (Some("excluded"), Some(c)) => {
                            Ok(Some(Bound::Excluded(parse_at!(c, u64)?)))
                        }
                        (Some("included"), Some(c)) => {
                            Ok(Some(Bound::Included(parse_at!(c, u64)?)))
                        }
                        (Some("unbounded"), _) => Ok(Some(Bound::Unbounded)),
                        (Some("none"), _) => Ok(None),
                        _ => err_at!(InvalidFile, msg: format!("invalid root")),
                    },
                    _ => err_at!(InvalidFile, msg: format!("invalid root")),
                },
                None => err_at!(InvalidFile, msg: format!("invalid root")),
            },
            None => err_at!(InvalidFile, msg: format!("invalid root")),
        }?;

        Ok(root)
    }
}

impl Root {
    fn to_next(&self) -> Root {
        let mut new_root = self.clone();
        new_root.version += 1;
        new_root
    }

    fn to_cutoff(&self, n_high_compacts: usize) -> Cutoff {
        match n_high_compacts % 2 {
            0 if self.tombstone_cutoff.is_some() => {
                let c = self.tombstone_cutoff.as_ref().unwrap().clone();
                Cutoff::new_tombstone(c)
            }
            0 if self.lsm_cutoff.is_some() => {
                let c = self.lsm_cutoff.as_ref().unwrap().clone();
                Cutoff::new_lsm(c)
            }
            1 if self.lsm_cutoff.is_some() => {
                let c = self.lsm_cutoff.as_ref().unwrap().clone();
                Cutoff::new_lsm(c)
            }
            _ => Cutoff::new_lsm_empty(),
        }
    }

    fn update_cutoff(&mut self, cutoff: Cutoff, tip_seqno: u64) -> Result<()> {
        use std::ops::Bound::{Excluded, Included, Unbounded};

        let cutoff = match cutoff {
            Cutoff::Lsm(Bound::Unbounded) => {
                let cutoff = Bound::Included(tip_seqno);
                Cutoff::new_lsm(cutoff)
            }
            Cutoff::Tombstone(Bound::Unbounded) => {
                let cutoff = Bound::Included(tip_seqno);
                Cutoff::new_tombstone(cutoff)
            }
            cutoff => cutoff,
        };

        match cutoff {
            Cutoff::Lsm(n_cutoff) => match self.lsm_cutoff.clone() {
                None => {
                    self.lsm_cutoff = Some(n_cutoff);
                    Ok(())
                }
                Some(o) => {
                    let range = (Unbounded, o.clone());
                    self.lsm_cutoff = Some(match n_cutoff {
                        Excluded(n) if range.contains(&n) => o,
                        Excluded(n) => Excluded(n),
                        Included(n) if range.contains(&n) => o,
                        Included(n) => Included(n),
                        Unbounded => Included(tip_seqno),
                    });
                    Ok(())
                }
            },
            Cutoff::Tombstone(n_cutoff) => match self.tombstone_cutoff.clone() {
                None => {
                    self.tombstone_cutoff = Some(n_cutoff);
                    Ok(())
                }
                Some(o) => {
                    let range = (Unbounded, o.clone());
                    self.tombstone_cutoff = Some(match n_cutoff {
                        Excluded(n) if range.contains(&n) => o,
                        Excluded(n) => Excluded(n),
                        Included(n) if range.contains(&n) => o,
                        Included(n) => Included(n),
                        Unbounded => Included(tip_seqno),
                    });
                    Ok(())
                }
            },
            Cutoff::Mono => err_at!(Fatal, msg: format!("unreachable")),
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

        match check_file(fname) {
            Some(val) => Ok(val),
            None => err_at!(InvalidFile, msg: format!("not root file")),
        }
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

#[derive(Clone, PartialEq)]
struct LevelName(String);

impl From<(String, usize)> for LevelName {
    fn from((name, level): (String, usize)) -> LevelName {
        LevelName(format!("{}-dgmlevel-{:03}", name, level))
    }
}

impl TryFrom<LevelName> for (String, usize) {
    type Error = Error;

    fn try_from(name: LevelName) -> Result<(String, usize)> {
        let parts: Vec<&str> = name.0.split('-').collect();
        if parts.len() >= 3 {
            match &parts[parts.len() - 2..] {
                ["dgmlevel", level] => {
                    let level = parse_at!(level, usize)?;
                    let s = parts[..(parts.len() - 2)].join("-");
                    Ok((s, level))
                }
                _ => err_at!(InvalidFile, msg: format!("invalid level")),
            }
        } else {
            err_at!(InvalidFile, msg: format!("invalid level"))
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

/// Dgm type index, optimized for holding data-set both in memory and disk.
pub struct Dgm<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    name: String,
    auto_commit: Option<rt::Thread<String, Result<()>, ()>>,
    auto_compact: Option<rt::Thread<String, Result<usize>, ()>>,
    inner: Arc<Mutex<InnerDgm<K, V, M, D>>>,
}

struct InnerDgm<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    dir: ffi::OsString,
    name: String,
    mem_factory: M,
    disk_factory: D,
    root_file: ffi::OsString,
    root: Root,

    n_high_compacts: usize,
    n_ccommits: usize,
    n_compacts: usize,
    m0: Snapshot<K, V, M::I>,         // write index
    m1: Option<Snapshot<K, V, M::I>>, // flush index
    disks: Vec<Snapshot<K, V, D::I>>, // NLEVELS

    writers: Vec<
        Arc<
            Mutex<
                Ws<
                    K,
                    V,
                    <M::I as Index<K, V>>::W,
                    <M::I as Index<K, V>>::R,
                    <D::I as Index<K, V>>::R,
                >,
            >,
        >,
    >,
    readers: Vec<Arc<Mutex<Rs<K, V, <M::I as Index<K, V>>::R, <D::I as Index<K, V>>::R>>>>,
}

impl<K, V, M, D> InnerDgm<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    fn to_disk_seqno(&self) -> Result<u64> {
        for d in self.disks.iter() {
            match d.as_disk()? {
                Some(d) => return d.to_seqno(),
                None => (),
            }
        }

        Ok(std::u64::MIN)
    }

    fn to_disk_metadata(&self) -> Result<Vec<u8>> {
        for d in self.disks.iter() {
            match d.as_disk()? {
                Some(d) => return d.to_metadata(),
                None => (),
            }
        }

        Ok(vec![])
    }

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

    fn shift_into_m0(&mut self) -> Result<()> {
        // block all the readers.
        let mut r_handles = vec![];
        for reader in self.readers.iter() {
            r_handles.push(reader.lock().unwrap());
        }

        {
            // block all the writer threads.
            let mut w_handles = vec![];
            for writer in self.writers.iter() {
                w_handles.push(writer.lock().unwrap())
            }

            // prepare a new memory snapshot
            let mut m0 = self.mem_factory.new(&self.name)?;
            self.m1 = match mem::replace(&mut self.m0, Default::default()) {
                Snapshot::Write(m1) => {
                    // update m0 with latest seqno and metadata
                    m0.set_seqno(m1.to_seqno()?)?;

                    let metadata = m1.to_metadata()?;
                    m0.commit(core::CommitIter::new_empty(), move |_| metadata.clone())?;

                    Ok(Some(Snapshot::new_flush(m1)))
                }
                _ => {
                    let msg = format!("Dgm.shift_into_m0() not write snapshot");
                    err_at!(Fatal, msg: msg)
                }
            }?;
            // update the m0 writer.
            mem::replace(&mut self.m0, Snapshot::new_write(m0));

            // now replace old writer handle created from the new m0 snapshot.
            for handle in w_handles.iter_mut() {
                let old = handle.deref_mut();

                old.rs.r_m0 = self.m0.as_mut_m0()?.to_reader()?;
                old.rs.r_m1 = match &mut self.m1 {
                    Some(m1) => Some(m1.as_mut_m1()?.to_reader()?),
                    None => None,
                };
                old.rs.r_disks.drain(..);
                for disk in self.disks.iter_mut() {
                    if let Some(d) = disk.as_mut_disk()? {
                        old.rs.r_disks.push(d.to_reader()?)
                    }
                }

                mem::replace(&mut old.w, self.m0.as_mut_m0()?.to_writer()?);
                // drop the old writer,
            }
        }

        // update readers and unblock them one by one.
        for r in r_handles.iter_mut() {
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

    fn is_commit_exhausted(&self) -> Result<bool> {
        use Snapshot::{Active, Commit, Compact};

        match self.disks[0] {
            Snapshot::None => Ok(false),
            Active(_) => Ok(true),
            Commit(_) | Compact(_) => Ok(true),
            _ => err_at!(Fatal, msg: format!("Dgm.is_commit_exhausted()")),
        }
    }

    fn commit_level(&mut self) -> Result<usize> {
        use Snapshot::{Active, Compact};

        if self.is_commit_exhausted()? {
            err_at!(Fatal, msg: format!("exhausted all levels !!"))?
        }

        let mf = self.m0.footprint()? as f64;
        let mut iter = self.disks.iter_mut().enumerate();
        loop {
            match iter.next() {
                None => break Ok(self.disks.len() - 1), // first commit
                Some((_, Snapshot::None)) => (),        // continue loop
                Some((lvl, disk)) => {
                    let df = disk.footprint()? as f64;
                    // println!("mf:{}, df:{}", mf, df);
                    match disk {
                        Compact(_) => break Ok(lvl - 1),
                        Active(_) => {
                            if (mf / df) < self.root.mem_ratio {
                                break Ok(lvl - 1);
                            } else {
                                break Ok(lvl);
                            }
                        }
                        _ => {
                            let msg = format!("Dgm.commit_level() not disk");
                            break err_at!(Fatal, msg: msg);
                        }
                    }
                }
            }
        }
    }

    fn move_to_commit(&mut self, level: usize) -> Result<()> {
        let d = mem::replace(&mut self.disks[level], Default::default());
        let d = match d {
            Snapshot::Active(d) => Ok(d),
            Snapshot::None => {
                let name: LevelName = (self.name.clone(), level).into();
                let name = name.to_string();
                Ok(self.disk_factory.new(&self.dir, &name)?)
            }
            _ => err_at!(Fatal, msg: format!("Dgm.move_to_commit() bad state")),
        }?;

        self.disks[level] = Snapshot::new_commit(d);
        Ok(())
    }

    fn active_compact_levels(disks: &[Snapshot<K, V, D::I>]) -> Result<Vec<usize>> {
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
                    _ => return err_at!(Fatal, msg: format!("unreachable")),
                }
            }
        }

        Ok(res)
    }

    fn find_compact_levels(
        disks: &[Snapshot<K, V, D::I>],
        disk_ratio: f64,
    ) -> Result<Option<(Vec<usize>, usize)>> {
        let mut levels = Self::active_compact_levels(disks)?;

        match levels.len() {
            0 | 1 => Ok(None),
            _n => loop {
                let target_level = levels.remove(levels.len() - 1);
                let ratio = {
                    let t_footprint = disks[target_level].footprint()?;
                    let mut footprint = 0;
                    for level in levels.clone().into_iter() {
                        footprint += disks[level].footprint()?;
                    }
                    // println!("compact_fp {} {}", footprint, t_footprint);
                    (footprint as f64) / (t_footprint as f64)
                };

                if ratio > disk_ratio {
                    break Ok(Some((levels, target_level)));
                } else if levels.len() == 1 {
                    break Ok(None);
                }
            },
        }
    }

    fn compact_levels(
        &mut self, // return (levels, sources, target)
    ) -> Result<Option<(Vec<usize>, Vec<usize>, usize)>> {
        let levels = {
            let disk_ratio = self.root.disk_ratio;
            Self::find_compact_levels(&self.disks, disk_ratio)?
        };

        match levels {
            None if self.n_ccommits < N_COMMITS => Ok(None),
            None => match Self::active_compact_levels(&self.disks)?.pop() {
                None => Ok(None),
                Some(d) => Ok(Some((vec![d], vec![], d))),
            },
            Some((ss, d)) if ss.len() == 0 => Ok(Some((vec![d], ss, d))),
            Some((ss, d)) => {
                let mut levels = ss.clone();
                levels.push(d);
                Ok(Some((levels, ss, d)))
            }
        }
    }

    fn move_to_compact(&mut self, levels: &[usize]) -> Result<()> {
        for level in levels.to_vec().into_iter() {
            let d = mem::replace(&mut self.disks[level], Default::default());
            let d = match d {
                Snapshot::Active(d) => d,
                _ => return err_at!(Fatal, msg: format!("unreachable")),
            };
            self.disks[level] = Snapshot::new_compact(d);
        }

        Ok(())
    }

    fn repopulate_readers(&mut self, commit: bool) -> Result<()> {
        {
            let mut r_diskss = vec![];
            for _ in 0..self.writers.len() {
                let mut r_disks = vec![];
                for disk in self.disks.iter_mut() {
                    match disk.as_mut_disk()? {
                        Some(d) => r_disks.push(d.to_reader()?),
                        None => (),
                    }
                }
                r_diskss.push(r_disks);
            }

            for writer in self.writers.iter() {
                let mut w = writer.lock().unwrap();
                if commit {
                    w.rs.r_m1 = None; // for commit.
                }
                w.rs.r_disks.drain(..);
                for r_disk in r_diskss.remove(0).into_iter() {
                    w.rs.r_disks.push(r_disk)
                }
            }
        }

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
            if commit {
                rs.r_m1 = None; // for commit.
            }
            rs.r_disks.drain(..);
            for r_disk in r_diskss.remove(0).into_iter() {
                rs.r_disks.push(r_disk)
            }
        }

        Ok(())
    }

    fn do_compact_disks(
        &self,
        s_levels: &[usize],
        d_level: usize,
    ) -> Result<(
        Vec<<D as DiskIndexFactory<K, V>>::I>,
        <D as DiskIndexFactory<K, V>>::I,
    )> {
        assert!(
            s_levels.to_vec().into_iter().all(|l| l < d_level),
            "s_levels:{:?} d_level:{}",
            s_levels,
            d_level
        );

        let mut src_disks = vec![];
        for s_level in s_levels.to_vec().into_iter() {
            src_disks.push(self.disks[s_level].as_disk()?.unwrap().clone());
        }
        let d = self.disks[d_level].as_disk()?.unwrap().clone();

        Ok((src_disks, d))
    }
}

enum Snapshot<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
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
    // empty slot.
    None,
    // ignore
    _Phantom(marker::PhantomData<K>, marker::PhantomData<V>),
}

impl<K, V, I> Default for Snapshot<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
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
    I: Footprint,
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
            _ => err_at!(Fatal, msg: format!("unreachable")),
        }
    }
}

impl<K, V, I> Snapshot<K, V, I>
where
    K: Clone + Ord,
    V: Clone + Diff,
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

    fn as_disk(&self) -> Result<Option<&I>> {
        use Snapshot::{Active, Commit, Compact, Flush, Write};

        match self {
            Commit(d) => Ok(Some(d)),
            Compact(d) => Ok(Some(d)),
            Active(d) => Ok(Some(d)),
            Snapshot::None => Ok(None),
            Write(_) | Flush(_) => err_at!(Fatal, msg: format!("not disk snapshot ")),
            _ => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    fn as_mut_disk(&mut self) -> Result<Option<&mut I>> {
        use Snapshot::{Active, Commit, Compact, Flush, Write};

        match self {
            Commit(d) => Ok(Some(d)),
            Compact(d) => Ok(Some(d)),
            Active(d) => Ok(Some(d)),
            Snapshot::None => Ok(None),
            Write(_) | Flush(_) => err_at!(Fatal, msg: format!("not disk snapshot")),
            _ => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    fn as_m0(&self) -> Result<&I> {
        match self {
            Snapshot::Write(m) => Ok(m),
            _ => err_at!(Fatal, msg: format!("m0 not write snapshot")),
        }
    }

    fn as_mut_m0(&mut self) -> Result<&mut I> {
        match self {
            Snapshot::Write(m) => Ok(m),
            _ => err_at!(Fatal, msg: format!("m0 not write snapshot")),
        }
    }

    fn as_mut_m1(&mut self) -> Result<&mut I> {
        match self {
            Snapshot::Flush(m) => Ok(m),
            _ => err_at!(Fatal, msg: format!("m0 not flush snapshot")),
        }
    }
}

impl<K, V, M, D> Drop for Dgm<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    fn drop(&mut self) {
        loop {
            match self.as_inner() {
                Ok(mut nnr) => {
                    let w = nnr.writers.iter().any(|w| Arc::strong_count(w) > 1);
                    let r = nnr.readers.iter().any(|r| Arc::strong_count(r) > 1);
                    if w == false && r == false {
                        for w in nnr.writers.drain(..) {
                            mem::drop(w)
                        }
                        for r in nnr.readers.drain(..) {
                            mem::drop(r)
                        }
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

        match self.auto_commit.take() {
            Some(auto_commit) => match auto_commit.close_wait() {
                Err(err) => error!(
                    target: "dgm   ", "{:?}, auto-commit {:?}", self.name, err
                ),
                Ok(_) => (),
            },
            None => (),
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
        fs::remove_dir_all(dir).ok();
        err_at!(IoError, fs::create_dir_all(dir))?;

        let root: Root = config.clone().into();
        let root_file = Self::new_root_file(dir, name, root.clone())?;

        let disks = {
            let mut disks: Vec<Snapshot<K, V, D::I>> = vec![];
            (0..Config::NLEVELS).for_each(|_| disks.push(Default::default()));
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

            n_high_compacts: Default::default(),
            n_ccommits: Default::default(),
            n_compacts: Default::default(),
            m0,
            m1: None,
            disks,

            writers: Default::default(),
            readers: Default::default(),
        };

        let mut index = Box::new(Dgm {
            name: name.to_string(),
            auto_commit: Default::default(),
            auto_compact: Default::default(),
            inner: Arc::new(Mutex::new(inner)),
        });

        index.start_auto_commit()?;
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
        let (root, root_file) = Self::find_root_file(dir, name)?;

        let mut disks: Vec<Snapshot<K, V, D::I>> = vec![];
        (0..Config::NLEVELS).for_each(|_| disks.push(Default::default()));

        for level in 0..root.levels {
            let level_name = {
                let level_name: LevelName = (name.to_string(), level).into();
                level_name.to_string()
            };
            disks[level] = match disk_factory.open(dir, &level_name) {
                Ok(d) => Snapshot::new_active(d),
                Err(_) => Default::default(),
            };
            // println!("dgm open {} {}", level, disks[level].is_active());
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
                root_file,
                root,

                n_high_compacts: Default::default(),
                n_ccommits: Default::default(),
                n_compacts: Default::default(),
                m0,
                m1: None,
                disks,

                writers: Default::default(),
                readers: Default::default(),
            };
            let mut index = Box::new(Dgm {
                name: name.to_string(),
                auto_commit: Default::default(),
                auto_compact: Default::default(),
                inner: Arc::new(Mutex::new(inner)),
            });
            // update m0 with latest seqno and metadata
            {
                let mut inner = index.as_inner()?;
                let latest_seqno = inner.to_disk_seqno()?;
                inner.m0.as_mut_m0()?.set_seqno(latest_seqno)?;

                let metadata = inner.to_disk_metadata()?;
                inner
                    .m0
                    .as_mut_m0()?
                    .commit(core::CommitIter::new_empty(), |_| metadata.clone())?;
            }

            index.start_auto_commit()?;
            index.start_auto_compact()?;

            Ok(index)
        }
    }

    fn start_auto_commit(&mut self) -> Result<()>
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

        self.auto_commit = {
            let inner = Arc::clone(&self.inner);
            Some(rt::Thread::new(
                format!("dgm-auto-commit-{}", name),
                move |rx| || auto_commit::<K, V, M, D>(name, root, inner, rx),
            ))
        };

        Ok(())
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

        self.auto_compact = match root.compact_interval {
            Some(_) => {
                let inner = Arc::clone(&self.inner);
                Some(rt::Thread::new(
                    format!("dgm-auto-compact-{}", name),
                    move |rx| || auto_compact::<K, V, M, D>(name, root, inner, rx),
                ))
            }
            None => None,
        };

        Ok(())
    }
}

impl<K, V, M, D> Dgm<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    fn as_inner(&self) -> Result<MutexGuard<InnerDgm<K, V, M, D>>> {
        match self.inner.lock() {
            Ok(value) => Ok(value),
            Err(err) => err_at!(Fatal, msg: format!("poisened lock {}", err)),
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

    fn do_commit(inner: &Arc<Mutex<InnerDgm<K, V, M, D>>>) -> Result<()> {
        let (metadata, mut d, r_m1, level) = {
            let mut inn = to_inner_lock(inner)?;

            if inn.m0.as_m0()?.to_seqno()? == inn.to_disk_seqno()? {
                return Ok(());
            }

            let level = inn.commit_level()?;
            inn.shift_into_m0()?;

            inn.move_to_commit(level)?;

            let d = inn.disks[level].as_disk()?.unwrap().clone();

            let r_m1 = match &mut inn.m1 {
                Some(m1) => Some(m1.as_mut_m1()?.to_reader()?),
                None => None,
            };
            (inn.m0.as_mut_m0()?.to_metadata()?, d, r_m1, level)
        };
        // println!("do_commit {}", level);

        let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
        match r_m1 {
            Some(r_m1) => {
                let iter = core::CommitIter::new(r_m1, within);
                d.commit(iter, |_| metadata.clone())?;
            }
            None => (),
        }

        {
            let mut inn = to_inner_lock(inner)?;
            let disk = Snapshot::new_active(d);
            mem::replace(&mut inn.disks[level], disk);
            // don't drop _m1 before repopulate_readers().
            let _m1 = mem::replace(&mut inn.m1, None);
            inn.repopulate_readers(true /*commit*/)?;

            let root_file = inn.root_file.clone();
            inn.root = inn.root.to_next();
            inn.root_file = Self::new_root_file(
                //
                &inn.dir,
                &inn.name,
                inn.root.clone(),
            )?;
            err_at!(IoError, fs::remove_file(&root_file))?;

            // println!("do_commit d_disk:{} ver:{}", level, inn.root.version);
        }

        Ok(())
    }

    fn do_compact(inner: &Arc<Mutex<InnerDgm<K, V, M, D>>>, cutoff: Cutoff) -> Result<usize> {
        match cutoff {
            Cutoff::Mono => err_at!(InvalidInput, msg: format!("can't have mono-cutoff")),
            _ => Ok(()),
        }?;

        let (cutoff, levels, s_levels, d_level) = {
            let mut inn = to_inner_lock(inner)?;

            let (levels, s_levels, d_level) = match inn.compact_levels()? {
                None => return Ok(0),
                Some((levels, src_levels, dst_level)) => {
                    //
                    (levels, src_levels, dst_level)
                }
            };

            let cutoff = if d_level == (Config::NLEVELS - 1) && !inn.root.lsm {
                inn.n_high_compacts += 1;

                Cutoff::new_mono()
            } else if d_level == (Config::NLEVELS - 1) {
                inn.n_high_compacts += 1;

                let tip_seqno = inn.m0.as_m0()?.to_seqno()?;
                inn.root.update_cutoff(cutoff, tip_seqno)?;
                inn.root.to_cutoff(inn.n_high_compacts)
            } else {
                // remember the cutoff, don't apply for intermediate compaction.
                {
                    let tip_seqno = inn.m0.as_m0()?.to_seqno()?;
                    inn.root.update_cutoff(cutoff, tip_seqno)?;
                }
                Cutoff::new_lsm_empty()
            };

            (cutoff, levels, s_levels, d_level)
        };

        //println!(
        //    "do_compact levels:{:?} s_levels:{:?} d_level:{}",
        //    levels, s_levels, d_level
        //);

        if s_levels.len() == 0 {
            // println!("1, {:?} {}", cutoff, d_level);
            Self::do_compact1(inner, cutoff, levels, d_level)
        } else {
            // println!("2, {:?} {:?} {}", cutoff, s_levels, d_level);
            Self::do_compact2(inner, levels, s_levels, d_level)
        }
    }

    fn do_compact1(
        inner: &Arc<Mutex<InnerDgm<K, V, M, D>>>,
        cutoff: Cutoff,
        levels: Vec<usize>,
        d_level: usize,
    ) -> Result<usize> {
        let mut high_disk = {
            let mut inn = to_inner_lock(inner)?;
            inn.move_to_compact(&levels)?;

            inn.root = inn.root.to_next();
            inn.disks[d_level].as_disk()?.unwrap().clone()
        };

        let res = high_disk.compact(cutoff);

        {
            let mut inn = to_inner_lock(inner)?;

            let disk = Snapshot::new_active(high_disk);
            mem::replace(&mut inn.disks[d_level], disk);

            inn.repopulate_readers(false /*commit*/)?;
            inn.n_ccommits = Default::default();
            inn.n_compacts += 1;

            let root_file = inn.root_file.clone();
            inn.root_file = Self::new_root_file(
                //
                &inn.dir,
                &inn.name,
                inn.root.clone(),
            )?;
            err_at!(IoError, fs::remove_file(&root_file))?;

            // println!("do_compact compact ver:{}", inn.root.version);
        }

        res
    }

    fn do_compact2(
        inner: &Arc<Mutex<InnerDgm<K, V, M, D>>>,
        levels: Vec<usize>,
        s_levels: Vec<usize>,
        d_level: usize,
    ) -> Result<usize> {
        let (s_disks, mut disk) = {
            let mut inn = to_inner_lock(inner)?;

            inn.move_to_compact(&levels)?;

            inn.root = inn.root.to_next();
            let (s_disks, disk) = inn.do_compact_disks(&s_levels, d_level)?;
            (s_disks, disk)
        };
        let metadata = match s_disks.first() {
            Some(s_disk) => s_disk.to_metadata()?,
            _ => return err_at!(Fatal, msg: format!("unreachable")),
        };

        let scanner = {
            let scanner = CommitScanner::<K, V, D::I>::new(s_disks)?;
            let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
            core::CommitIter::new(scanner, within)
        };
        disk.commit(scanner, |_| metadata.clone())?;

        let compacted_disks = {
            let mut inn = to_inner_lock(inner)?;

            let mut compacted_disks = vec![];
            for level in s_levels.clone().into_iter() {
                let d = mem::replace(&mut inn.disks[level], Default::default());
                match d {
                    Snapshot::Compact(d) => compacted_disks.push(d),
                    _ => return err_at!(Fatal, msg: format!("unreachable")),
                }
            }
            let disk = Snapshot::new_active(disk);
            mem::replace(&mut inn.disks[d_level], disk);

            inn.repopulate_readers(false /*commit*/)?;
            inn.n_ccommits += 1;

            let root_file = inn.root_file.clone();
            inn.root_file = Self::new_root_file(
                //
                &inn.dir,
                &inn.name,
                inn.root.clone(),
            )?;
            err_at!(IoError, fs::remove_file(&root_file))?;

            //println!(
            //    "do_compact commit s_levels:{:?} d_level:{} ver:{}",
            //    s_levels, d_level, inn.root.version
            //);

            compacted_disks
        };

        for d in compacted_disks.into_iter() {
            d.purge()?;
        }

        Ok(0)
    }

    fn new_root_file(
        //
        dir: &ffi::OsStr,
        name: &str,
        root: Root,
    ) -> Result<ffi::OsString> {
        let root_file: ffi::OsString = {
            let rf: RootFileName = (name.to_string().into(), root.version).into();
            let mut rootp = path::PathBuf::from(dir);
            rootp.push(&rf.0);
            rootp.into_os_string()
        };

        let data: Vec<u8> = root.try_into()?;

        let mut fd = util::create_file_a(root_file.clone())?;
        write_file!(fd, &data, root_file.clone(), "dgm-root-file")?;
        Ok(root_file.into())
    }

    fn find_root_file(dir: &ffi::OsStr, name: &str) -> Result<(Root, ffi::OsString)> {
        let mut versions = vec![];
        for item in err_at!(IoError, fs::read_dir(dir))? {
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

        let version = match versions.into_iter().max() {
            Some(version) => Ok(version),
            None => err_at!(InvalidFile, msg: format!("not root file")),
        }?;

        let root_file = {
            let file: RootFileName = (name.to_string(), version).into();
            let mut rootp = path::PathBuf::from(dir);
            rootp.push(&file.0);
            rootp.into_os_string()
        };

        let mut fd = util::open_file_r(&root_file)?;
        let mut bytes = vec![];
        err_at!(IoError, fd.read_to_end(&mut bytes))?;

        Ok((bytes.try_into()?, root_file))
    }
}

impl<K, V, M, D> Footprint for Dgm<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    fn footprint(&self) -> Result<isize> {
        Ok(self.disk_footprint()? + self.mem_footprint()?)
    }
}

impl<K, V, M, D, A, B> Validate<Stats<A, B>> for Box<Dgm<K, V, M, D>>
where
    K: Clone + Ord + Serialize + Footprint + fmt::Debug,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    A: fmt::Display,
    B: fmt::Display,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
    M::I: Validate<A>,
    D::I: Validate<B>,
{
    fn validate(&mut self) -> Result<Stats<A, B>> {
        self.as_mut().validate()
    }
}

impl<K, V, M, D, A, B> Validate<Stats<A, B>> for Dgm<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint + fmt::Debug,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    A: fmt::Display,
    B: fmt::Display,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
    M::I: Validate<A>,
    D::I: Validate<B>,
{
    fn validate(&mut self) -> Result<Stats<A, B>> {
        let mut inner = self.as_inner()?;

        let root = inner.root.clone();

        if inner.n_ccommits > N_COMMITS {
            let msg = format!("validate, {} commited to highest level", inner.n_ccommits);
            err_at!(Fatal, msg: msg)
        } else {
            Ok(())
        }?;

        let m0 = inner.m0.as_mut_m0()?;
        let _ = m0.validate()?; // TODO: handle return.
        let mut m0_r = m0.to_reader()?;
        let mut seqnos = vec![validate_snapshot(m0_r.iter()?, true, None, None)?];

        match &mut inner.m1 {
            Some(m1) => {
                let m1 = m1.as_mut_m1()?;
                let _ = m1.validate()?; // TODO: handle return
                let mut m1_r = m1.to_reader()?;
                seqnos.push(validate_snapshot(m1_r.iter()?, true, None, None)?);
            }
            None => (),
        }

        let mut disks = vec![];
        for disk in inner.disks.iter_mut() {
            match disk.as_mut_disk()? {
                Some(disk) => disks.push(disk),
                None => (),
            }
        }

        let n = disks.len();
        if n > 0 {
            for disk in disks.drain(..n - 1) {
                let _ = disk.validate()?; // TODO: handle return
                let mut disk = disk.to_reader()?;
                seqnos.push(validate_snapshot(disk.iter()?, true, None, None)?);
            }
            // validate the last disk snapshot.
            let disk = disks.remove(0);
            let _ = disk.validate()?; // TODO: handle return
            {
                let mut disk = disk.to_reader()?;
                let lc = root.lsm_cutoff.clone();
                let tc = root.tombstone_cutoff.clone();
                if inner.n_ccommits == 0 && inner.n_compacts > 0 {
                    seqnos.push(validate_snapshot(disk.iter()?, root.lsm, lc, tc)?);
                } else {
                    seqnos.push(validate_snapshot(disk.iter()?, true, None, None)?);
                }
            }
        }

        {
            let n = seqnos.len();
            let iter = seqnos[..n - 1]
                .to_vec()
                .into_iter()
                .zip(seqnos[1..].to_vec().into_iter());
            for (x, y) in iter {
                match y.start_bound() {
                    Bound::Included(y) if x.contains(y) => {
                        let msg = format!("overlapping snapshot {:?}", seqnos);
                        err_at!(Fatal, msg: msg)?;
                    }
                    Bound::Included(_) => (),
                    _ => err_at!(Fatal, msg: format!("unreachable"))?,
                }
            }
        }

        Ok(Stats {
            _phantom_key: marker::PhantomData,
            _phantom_val: marker::PhantomData,
        })
    }
}

fn validate_snapshot<K, V>(
    iter: IndexIter<K, V>,
    lsm: bool,
    lsm_cutoff: Option<Bound<u64>>,
    tombstone_cutoff: Option<Bound<u64>>,
) -> Result<(Bound<u64>, Bound<u64>)>
where
    K: Clone + Ord + fmt::Debug,
    V: Clone + Diff,
{
    use Bound::{Excluded, Included, Unbounded};

    let mut min_seqno = std::u64::MAX;
    let mut max_seqno = std::u64::MIN;
    for entry in iter {
        let entry = entry?;
        if !lsm && entry.is_deleted() {
            let msg = format!("{:?}/{}", entry.to_key(), entry.to_seqno());
            err_at!(Fatal, msg: msg)?;
        } else if !lsm && entry.as_deltas().len() > 0 {
            let msg = format!("old versions in non-lsm");
            err_at!(Fatal, msg: msg)?;
        }

        let mut seqnos: Vec<u64> = entry.as_deltas().iter().map(|d| d.to_seqno()).collect();
        seqnos.insert(0, entry.to_seqno());
        max_seqno = cmp::max(
            max_seqno,
            seqnos.clone().into_iter().max().unwrap_or(max_seqno),
        );
        min_seqno = cmp::min(
            min_seqno,
            seqnos.clone().into_iter().min().unwrap_or(min_seqno),
        );

        let l_ok = match lsm_cutoff {
            Some(cutoff) => match cutoff {
                Included(lseqno) if min_seqno <= lseqno => false,
                Excluded(lseqno) if min_seqno < lseqno => false,
                Unbounded => return err_at!(Fatal, msg: format!("unreachable")),
                _ => true,
            },
            None => true,
        };
        let t_ok = match tombstone_cutoff {
            Some(cutoff) if entry.is_deleted() => match cutoff {
                Included(tseqno) if max_seqno <= tseqno => false,
                Excluded(tseqno) if max_seqno < tseqno => false,
                Unbounded => return err_at!(Fatal, msg: format!("unreachable")),
                _ => true,
            },
            _ => true,
        };
        if !l_ok && !t_ok {
            let msg = format!("entry < lsm/tombstone cutoff");
            err_at!(Fatal, msg: msg)?;
        }
    }

    Ok((Bound::Included(min_seqno), Bound::Included(max_seqno)))
}

macro_rules! do_close_purge {
    ($s:expr, $func:ident) => {{
        match $s.auto_commit.take() {
            Some(auto_commit) => auto_commit.close_wait()?,
            None => (),
        }
        match $s.auto_compact.take() {
            Some(auto_compact) => auto_compact.close_wait()?,
            None => (),
        }

        let mut inner = loop {
            let inner = $s.as_inner()?;
            let ok = inner.disks.iter().all(|disk| match disk {
                Snapshot::Active(_) | Snapshot::None => true,
                _ => false,
            });
            if ok {
                break inner;
            }
            thread::sleep(time::Duration::from_secs(1));
        };

        let w = inner.writers.iter().any(|w| Arc::strong_count(w) > 1);
        let r = inner.readers.iter().any(|r| Arc::strong_count(r) > 1);
        if w == true || r == true {
            err_at!(APIMisuse, msg: format!("active read/write handles"))?
        }

        for writer in inner.writers.drain(..) {
            mem::drop(writer);
        }
        for reader in inner.readers.drain(..) {
            mem::drop(reader);
        }

        match mem::replace(&mut inner.m0, Default::default()) {
            Snapshot::Write(m0) => m0.$func()?,
            _ => err_at!(Fatal, msg: format!("m0 not write"))?,
        }
        match inner.m1.take() {
            Some(Snapshot::Flush(m1)) => m1.$func()?,
            Some(_) => err_at!(Fatal, msg: format!("m1 not flush"))?,
            None => (),
        }
        for disk in inner.disks.drain(..) {
            match disk {
                Snapshot::Active(disk) => disk.$func()?,
                Snapshot::None => (),
                _ => err_at!(Fatal, msg: format!("unreachable"))?,
            }
        }

        Ok(())
    }};
}

impl<K, V, M, D> Index<K, V> for Box<Dgm<K, V, M, D>>
where
    K: Clone + Ord + Hash + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    type W = DgmWriter<
        K,
        V,
        <M::I as Index<K, V>>::W,
        <M::I as Index<K, V>>::R,
        <D::I as Index<K, V>>::R,
    >;
    type R = DgmReader<K, V, <M::I as Index<K, V>>::R, <D::I as Index<K, V>>::R>;

    fn to_name(&self) -> Result<String> {
        self.as_ref().to_name()
    }

    fn to_metadata(&self) -> Result<Vec<u8>> {
        self.as_ref().to_metadata()
    }

    fn to_seqno(&self) -> Result<u64> {
        self.as_ref().to_seqno()
    }

    fn set_seqno(&mut self, seqno: u64) -> Result<()> {
        self.as_mut().set_seqno(seqno)
    }

    fn to_writer(&mut self) -> Result<Self::W> {
        self.as_mut().to_writer()
    }

    fn to_reader(&mut self) -> Result<Self::R> {
        self.as_mut().to_reader()
    }

    fn commit<C, F>(&mut self, scanner: CommitIter<K, V, C>, metacb: F) -> Result<()>
    where
        C: CommitIterator<K, V>,
        F: Fn(Vec<u8>) -> Vec<u8>,
    {
        self.as_mut().commit(scanner, metacb)
    }

    fn compact(&mut self, cutoff: Cutoff) -> Result<usize> {
        self.as_mut().compact(cutoff)
    }

    fn close(self) -> Result<()> {
        (*self).close()
    }

    fn purge(self) -> Result<()> {
        (*self).purge()
    }
}

impl<K, V, M, D> Index<K, V> for Dgm<K, V, M, D>
where
    K: Clone + Ord + Hash + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    type W = DgmWriter<
        K,
        V,
        <M::I as Index<K, V>>::W,
        <M::I as Index<K, V>>::R,
        <D::I as Index<K, V>>::R,
    >;
    type R = DgmReader<K, V, <M::I as Index<K, V>>::R, <D::I as Index<K, V>>::R>;

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

        let m0_seqno = inner.m0.as_m0()?.to_seqno()?;
        let disk_seqno = inner.to_disk_seqno()?;
        Ok(cmp::max(m0_seqno, disk_seqno))
    }

    fn set_seqno(&mut self, seqno: u64) -> Result<()> {
        let mut inner = self.as_inner()?;

        inner.m0.as_mut_m0()?.set_seqno(seqno)
    }

    fn to_writer(&mut self) -> Result<Self::W> {
        // create a new set of snapshot-reader
        let mut inner = self.as_inner()?;

        let w = inner.m0.as_mut_m0()?.to_writer()?;

        let rs = {
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
            Rs {
                r_m0,
                r_m1,
                r_disks,

                _phantom_key: marker::PhantomData,
                _phantom_val: marker::PhantomData,
            }
        };

        let arc_w = Arc::new(Mutex::new(Ws { w, rs }));
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

            _phantom_key: marker::PhantomData,
            _phantom_val: marker::PhantomData,
        };

        let arc_rs = Arc::new(Mutex::new(rs));
        inner.readers.push(Arc::clone(&arc_rs));

        Ok(DgmReader::new(&inner.name, arc_rs))
    }

    fn commit<C, F>(&mut self, scanner: CommitIter<K, V, C>, metacb: F) -> Result<()>
    where
        C: CommitIterator<K, V>,
        F: Fn(Vec<u8>) -> Vec<u8>,
    {
        {
            let mut inner = self.as_inner()?;
            let m0 = inner.m0.as_mut_m0()?;
            m0.commit(scanner, metacb)?;
        }
        Self::do_commit(&self.inner)
    }

    fn compact(&mut self, cutoff: Cutoff) -> Result<usize> {
        Self::do_compact(&self.inner, cutoff)
    }

    fn close(mut self) -> Result<()> {
        do_close_purge!(self, close)
    }

    fn purge(mut self) -> Result<()> {
        do_close_purge!(self, purge)
    }
}

/// Writer handle into Dgm index.
pub struct DgmWriter<K, V, W, A, B>
where
    K: Clone + Ord,
    V: Clone + Diff,
    W: Writer<K, V>,
    A: Reader<K, V>,
    B: Reader<K, V>,
{
    name: String,
    w: Arc<Mutex<Ws<K, V, W, A, B>>>,

    _phantom_key: marker::PhantomData<K>,
    _phantom_val: marker::PhantomData<V>,
}

impl<K, V, W, A, B> DgmWriter<K, V, W, A, B>
where
    K: Clone + Ord,
    V: Clone + Diff,
    W: Writer<K, V>,
    A: Reader<K, V>,
    B: Reader<K, V>,
{
    fn new(name: &str, w: Arc<Mutex<Ws<K, V, W, A, B>>>) -> DgmWriter<K, V, W, A, B> {
        let w = DgmWriter {
            name: name.to_string(),
            w,

            _phantom_key: marker::PhantomData,
            _phantom_val: marker::PhantomData,
        };
        debug!(target: "dgm   ", "{}, new write handle ...", w.name);
        w
    }

    fn as_writer(&self) -> Result<MutexGuard<Ws<K, V, W, A, B>>> {
        match self.w.lock() {
            Ok(value) => Ok(value),
            Err(err) => err_at!(Fatal, msg: format!("poisened lock {}", err)),
        }
    }
}

impl<K, V, W, A, B> Writer<K, V> for DgmWriter<K, V, W, A, B>
where
    K: Clone + Ord + Hash + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    W: Writer<K, V>,
    A: Reader<K, V>,
    B: Reader<K, V>,
{
    fn set(&mut self, key: K, value: V) -> Result<Option<Entry<K, V>>> {
        let mut w_rs = self.as_writer()?;
        w_rs.w.set(key, value)
    }

    fn set_cas(&mut self, key: K, value: V, cas: u64) -> Result<Option<Entry<K, V>>> {
        let mut w_rs = self.as_writer()?;

        match Rs::get(&mut w_rs.rs, &key) {
            Ok(old) if cas == old.to_seqno() => Ok(()),
            Err(Error::KeyNotFound) if cas == 0 => Ok(()),
            Ok(old) => Err(Error::InvalidCAS(old.to_seqno())),
            Err(Error::KeyNotFound) => Err(Error::InvalidCAS(0)),
            Err(err) => Err(err),
        }?;

        w_rs.w.set(key, value)
    }

    fn delete<Q>(&mut self, key: &Q) -> Result<Option<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        let mut w_rs = self.as_writer()?;
        w_rs.w.delete(key)
    }
}

struct Ws<K, V, W, A, B>
where
    K: Clone + Ord,
    V: Clone + Diff,
    W: Writer<K, V>,
    A: Reader<K, V>,
    B: Reader<K, V>,
{
    w: W,
    rs: Rs<K, V, A, B>,
}

// type alias to reader associated type for each snapshot (aka disk-index)
struct Rs<K, V, M, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    M: Reader<K, V>,
    D: Reader<K, V>,
{
    r_m0: M,
    r_m1: Option<M>,
    r_disks: Vec<D>,

    _phantom_key: marker::PhantomData<K>,
    _phantom_val: marker::PhantomData<V>,
}

impl<K, V, M, D> Rs<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: Reader<K, V>,
    D: Reader<K, V>,
{
    fn get<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + Hash,
    {
        match self.r_m0.get(key) {
            Ok(entry) => return Ok(entry),
            Err(Error::KeyNotFound) => (),
            Err(err) => return Err(err),
        }

        if let Some(m1) = &mut self.r_m1 {
            match m1.get(key) {
                Ok(entry) => return Ok(entry),
                Err(Error::KeyNotFound) => (),
                Err(err) => return Err(err),
            }
        }

        let mut iter = self.r_disks.iter_mut();
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

    fn iter(mut rs: MutexGuard<Rs<K, V, M, D>>) -> Result<IndexIter<K, V>> {
        let mut iters: Vec<IndexIter<K, V>> = vec![];

        let m0 = unsafe { (&mut rs.r_m0 as *mut M).as_mut().unwrap() };
        iters.push(m0.iter()?);

        if let Some(m1) = &mut rs.r_m1 {
            let m1 = unsafe { (m1 as *mut M).as_mut().unwrap() };
            iters.push(m1.iter()?);
        }

        for disk in rs.r_disks.iter_mut() {
            let disk = unsafe { (disk as *mut D).as_mut().unwrap() };
            iters.push(disk.iter()?);
        }

        let iter = Self::merge_iters(iters, false /*reverse*/, false /*ver*/)?;
        Ok(Box::new(DgmIter::new(rs, iter)))
    }

    fn range<'a, R, Q>(mut rs: MutexGuard<'a, Rs<K, V, M, D>>, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let mut iters: Vec<IndexIter<K, V>> = vec![];

        let m0 = unsafe { (&mut rs.r_m0 as *mut M).as_mut().unwrap() };
        iters.push(m0.range(range.clone())?);

        if let Some(m1) = &mut rs.r_m1 {
            let m1 = unsafe { (m1 as *mut M).as_mut().unwrap() };
            iters.push(m1.range(range.clone())?);
        }

        for disk in rs.r_disks.iter_mut().rev() {
            let disk = unsafe { (disk as *mut D).as_mut().unwrap() };
            iters.push(disk.range(range.clone())?)
        }

        let iter = Self::merge_iters(iters, false /*reverse*/, false /*ver*/)?;
        Ok(Box::new(DgmIter::new(rs, iter)))
    }

    fn reverse<'a, R, Q>(
        mut rs: MutexGuard<'a, Rs<K, V, M, D>>,
        range: R,
    ) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let mut iters: Vec<IndexIter<K, V>> = vec![];

        let m0 = unsafe { (&mut rs.r_m0 as *mut M).as_mut().unwrap() };
        iters.push(m0.reverse(range.clone())?);

        if let Some(m1) = &mut rs.r_m1 {
            let m1 = unsafe { (m1 as *mut M).as_mut().unwrap() };
            iters.push(m1.reverse(range.clone())?);
        }

        for disk in rs.r_disks.iter_mut().rev() {
            let disk = unsafe { (disk as *mut D).as_mut().unwrap() };
            iters.push(disk.reverse(range.clone())?)
        }

        let iter = Self::merge_iters(iters, true /*reverse*/, false /*ver*/)?;
        Ok(Box::new(DgmIter::new(rs, iter)))
    }

    fn get_with_versions<Q>(mut rs: MutexGuard<Rs<K, V, M, D>>, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + Hash,
    {
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
                    (Err(Error::KeyNotFound), None) => Ok(None),
                    (Err(err), _) => Err(err),
                },
                None => break entry,
            }?;
        };

        entry.ok_or(Error::KeyNotFound)
    }

    fn iter_with_versions(mut rs: MutexGuard<Rs<K, V, M, D>>) -> Result<IndexIter<K, V>> {
        let mut iters: Vec<IndexIter<K, V>> = vec![];

        let m0 = unsafe { (&mut rs.r_m0 as *mut M).as_mut().unwrap() };
        iters.push(m0.iter_with_versions()?);

        if let Some(m1) = &mut rs.r_m1 {
            let m1 = unsafe { (m1 as *mut M).as_mut().unwrap() };
            iters.push(m1.iter_with_versions()?);
        }

        for disk in rs.r_disks.iter_mut() {
            let disk = unsafe { (disk as *mut D).as_mut().unwrap() };
            iters.push(disk.iter_with_versions()?);
        }

        let iter = Self::merge_iters(iters, false /*reverse*/, true /*ver*/)?;
        Ok(Box::new(DgmIter::new(rs, iter)))
    }

    fn range_with_versions<'a, R, Q>(
        mut rs: MutexGuard<'a, Rs<K, V, M, D>>,
        range: R, // between lower and upper bound
    ) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let mut iters: Vec<IndexIter<K, V>> = vec![];

        let m0 = unsafe { (&mut rs.r_m0 as *mut M).as_mut().unwrap() };
        iters.push(m0.range_with_versions(range.clone())?);

        if let Some(m1) = &mut rs.r_m1 {
            let m1 = unsafe { (m1 as *mut M).as_mut().unwrap() };
            iters.push(m1.range_with_versions(range.clone())?)
        }

        for disk in rs.r_disks.iter_mut().rev() {
            let disk = unsafe { (disk as *mut D).as_mut().unwrap() };
            iters.push(disk.range_with_versions(range.clone())?);
        }

        let iter = Self::merge_iters(iters, false /*reverse*/, true /*ver*/)?;
        Ok(Box::new(DgmIter::new(rs, iter)))
    }

    fn reverse_with_versions<'a, R, Q>(
        mut rs: MutexGuard<'a, Rs<K, V, M, D>>,
        range: R, // between upper and lower bound
    ) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let mut iters: Vec<IndexIter<K, V>> = vec![];

        let m0 = unsafe { (&mut rs.r_m0 as *mut M).as_mut().unwrap() };
        iters.push(m0.reverse_with_versions(range.clone())?);

        if let Some(m1) = &mut rs.r_m1 {
            let m1 = unsafe { (m1 as *mut M).as_mut().unwrap() };
            let range = range.clone();
            iters.push(m1.reverse_with_versions(range)?);
        }

        for disk in rs.r_disks.iter_mut().rev() {
            let disk = unsafe { (disk as *mut D).as_mut().unwrap() };
            let range = range.clone();
            iters.push(disk.reverse_with_versions(range)?);
        }

        let iter = Self::merge_iters(iters, true /*reverse*/, true /*ver*/)?;
        Ok(Box::new(DgmIter::new(rs, iter)))
    }

    fn merge_iters<'a>(
        mut iters: Vec<IndexIter<'a, K, V>>,
        reverse: bool,
        versions: bool,
    ) -> Result<IndexIter<'a, K, V>>
    where
        K: 'a,
        V: 'a,
    {
        iters.reverse();

        match iters.len() {
            1 => Ok(iters.remove(0)),
            n if n > 1 => {
                let mut older_iter = iters.remove(0);
                for newer_iter in iters.into_iter() {
                    older_iter = if versions {
                        lsm::y_iter_versions(newer_iter, older_iter, reverse)
                    } else {
                        lsm::y_iter(newer_iter, older_iter, reverse)
                    };
                }
                Ok(older_iter)
            }
            _ => err_at!(Fatal, msg: format!("unreachable")),
        }
    }
}

/// Reader handle into Dgm index.
pub struct DgmReader<K, V, M, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    M: Reader<K, V>,
    D: Reader<K, V>,
{
    name: String,
    rs: Arc<Mutex<Rs<K, V, M, D>>>,
}

impl<K, V, M, D> DgmReader<K, V, M, D>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    M: Reader<K, V>,
    D: Reader<K, V>,
{
    fn new(name: &str, rs: Arc<Mutex<Rs<K, V, M, D>>>) -> DgmReader<K, V, M, D> {
        let r = DgmReader {
            name: name.to_string(),
            rs,
        };
        debug!(target: "dgm   ", "{}, new read handle ...", r.name);
        r
    }

    fn as_reader(&self) -> Result<MutexGuard<Rs<K, V, M, D>>> {
        match self.rs.lock() {
            Ok(value) => Ok(value),
            Err(err) => err_at!(Fatal, msg: format!("poisened lock {}", err)),
        }
    }
}

impl<K, V, M, D> Reader<K, V> for DgmReader<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: Reader<K, V>,
    D: Reader<K, V>,
{
    fn get<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + Hash,
    {
        let mut rs = self.as_reader()?;
        Rs::get(rs.deref_mut(), key)
    }

    fn iter(&mut self) -> Result<IndexIter<K, V>> {
        let rs = self.as_reader()?;
        Rs::iter(rs)
    }

    fn range<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let rs = self.as_reader()?;
        Rs::range(rs, range)
    }

    fn reverse<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let rs = self.as_reader()?;
        Rs::reverse(rs, range)
    }

    fn get_with_versions<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + Hash,
    {
        let rs = self.as_reader()?;
        Rs::get_with_versions(rs, key)
    }

    fn iter_with_versions(&mut self) -> Result<IndexIter<K, V>> {
        let rs = self.as_reader()?;
        Rs::iter_with_versions(rs)
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
        let rs = self.as_reader()?;
        Rs::range_with_versions(rs, range)
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
        let rs = self.as_reader()?;
        Rs::reverse_with_versions(rs, range)
    }
}

impl<K, V, M, D> CommitIterator<K, V> for DgmReader<K, V, M, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    M: Reader<K, V>,
    D: Reader<K, V>,
{
    fn scan<G>(&mut self, _within: G) -> Result<IndexIter<K, V>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let msg = format!("<DgmReader as CommitIterator>.scan; {}", self.name);
        err_at!(NotImplemented, msg: msg)
    }

    fn scans<G>(&mut self, _n_shards: usize, _within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let msg = format!("<DgmReader as CommitIterator>.scans; {}", self.name);
        err_at!(NotImplemented, msg: msg)
    }

    fn range_scans<N, G>(&mut self, _ranges: Vec<N>, _within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
        N: Clone + RangeBounds<K>,
    {
        let msg = format!("<DgmReader as CommitIterator>.range_scans; {}", self.name);
        err_at!(NotImplemented, msg: msg)
    }
}

struct DgmIter<'a, K, V, M, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    M: Reader<K, V>,
    D: Reader<K, V>,
{
    _rs: MutexGuard<'a, Rs<K, V, M, D>>,
    iter: IndexIter<'a, K, V>,
}

impl<'a, K, V, M, D> DgmIter<'a, K, V, M, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    M: Reader<K, V>,
    D: Reader<K, V>,
{
    fn new(
        _rs: MutexGuard<'a, Rs<K, V, M, D>>,
        iter: IndexIter<'a, K, V>,
    ) -> DgmIter<'a, K, V, M, D> {
        DgmIter { _rs, iter }
    }
}

impl<'a, K, V, M, D> Iterator for DgmIter<'a, K, V, M, D>
where
    K: Clone + Ord,
    V: Clone + Diff,
    M: Reader<K, V>,
    D: Reader<K, V>,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

struct CommitScanner<K, V, I>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    I: Index<K, V> + Footprint + Clone,
{
    src_disks: Vec<I>,
    rs: Vec<I::R>,
}

impl<K, V, I> CommitScanner<K, V, I>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    I: Index<K, V> + Footprint + Clone,
{
    fn new(mut src_disks: Vec<I>) -> Result<CommitScanner<K, V, I>> {
        src_disks.reverse();

        let mut rs = vec![];
        for disk in src_disks.iter_mut() {
            rs.push(disk.to_reader()?);
        }

        Ok(CommitScanner { src_disks, rs })
    }
}

impl<K, V, I> CommitIterator<K, V> for CommitScanner<K, V, I>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    I: CommitIterator<K, V> + Index<K, V> + Footprint + Clone,
{
    fn scan<G>(&mut self, within: G) -> Result<IndexIter<K, V>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let no_reverse = false;

        match self.rs.len() {
            0 => err_at!(Fatal, msg: format!("unreachable")),
            1 => Ok(self.rs[0].iter_with_versions()?),
            _n => {
                let r = unsafe {
                    let r = &mut self.rs[0];
                    (r as *mut <I as Index<K, V>>::R).as_mut().unwrap()
                };
                let mut y_iter = r.iter_with_versions()?;
                for r in self.rs[1..].iter_mut() {
                    let r = unsafe {
                        let r = r as *mut <I as Index<K, V>>::R;
                        r.as_mut().unwrap()
                    };
                    let iter = r.iter_with_versions()?;
                    y_iter = lsm::y_iter_versions(iter, y_iter, no_reverse);
                }
                Ok(Box::new(scans::FilterScans::new(
                    vec![y_iter],
                    within.clone(),
                )))
            }
        }
    }

    fn scans<G>(&mut self, n_shards: usize, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let no_reverse = false;

        let mut result_iters = vec![];
        for disk in self.src_disks.iter_mut() {
            let iters = disk.scans(n_shards, within.clone())?;
            assert_eq!(iters.len(), n_shards);
            if result_iters.len() == 0 {
                result_iters = iters;
            } else {
                let ziter = {
                    let new_iters = iters.into_iter();
                    let old_iters = result_iters.into_iter();
                    new_iters.zip(old_iters)
                };
                result_iters = vec![];
                for (new_iter, old_iter) in ziter {
                    result_iters.push(
                        //
                        lsm::y_iter_versions(new_iter, old_iter, no_reverse),
                    );
                }
            }
        }

        Ok(result_iters)
    }

    fn range_scans<N, G>(&mut self, ranges: Vec<N>, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        N: Clone + RangeBounds<K>,
        G: Clone + RangeBounds<u64>,
    {
        let no_reverse = false;

        let mut result_iters = vec![];
        for disk in self.src_disks.iter_mut() {
            let iters = disk.range_scans(ranges.clone(), within.clone())?;
            assert_eq!(iters.len(), ranges.len());
            if result_iters.len() == 0 {
                result_iters = iters;
            } else {
                let ziter = {
                    let new_iters = iters.into_iter();
                    let old_iters = result_iters.into_iter();
                    new_iters.zip(old_iters)
                };
                result_iters = vec![];
                for (new_iter, old_iter) in ziter {
                    result_iters.push(
                        //
                        lsm::y_iter_versions(new_iter, old_iter, no_reverse),
                    );
                }
            }
        }

        Ok(result_iters)
    }
}

fn auto_commit<K, V, M, D>(
    name: String,
    root: Root,
    inner: Arc<Mutex<InnerDgm<K, V, M, D>>>,
    rx: rt::Rx<String, Result<()>>,
) -> Result<()>
where
    K: 'static + Send + Clone + Ord + Serialize + Footprint,
    V: 'static + Send + Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: 'static + Send + WriteIndexFactory<K, V>,
    D: 'static + Send + DiskIndexFactory<K, V>,
    <M as WriteIndexFactory<K, V>>::I: 'static + Send + Footprint,
    <<M as WriteIndexFactory<K, V>>::I as Index<K, V>>::R: 'static + Send,
    <<M as WriteIndexFactory<K, V>>::I as Index<K, V>>::W: 'static + Send,
    <D as DiskIndexFactory<K, V>>::I: 'static + Send + CommitIterator<K, V> + Footprint + Clone,
    <<D as DiskIndexFactory<K, V>>::I as Index<K, V>>::R: 'static + Send,
    <<D as DiskIndexFactory<K, V>>::I as Index<K, V>>::W: 'static + Send,
{
    let c_interval = root.commit_interval.unwrap_or(Config::COMMIT_INTERVAL);

    info!(
        target: "dgm   ",
        "{}, auto-commit thread started with interval {:?}",
        name, c_interval,
    );

    let mut elapsed = time::Duration::new(0, 0);
    loop {
        let (cmd, resp_tx) = {
            let interval = {
                let elapsed = cmp::min(c_interval, elapsed);
                c_interval - elapsed
            };
            match rx.recv_timeout(interval) {
                Ok((cmd, resp_tx)) if cmd == "do_commit" => (cmd, resp_tx),
                Ok(_) => break err_at!(Fatal, msg: format!("unreachable")),
                Err(mpsc::RecvTimeoutError::Timeout) => ("".to_string(), None),
                Err(mpsc::RecvTimeoutError::Disconnected) => break Ok(()),
            }
        };

        let ok_to_commit = {
            let mut inner = to_inner_lock(&inner)?;
            // first let us do some cleaning.
            inner.cleanup_writers()?;
            inner.cleanup_readers()?;
            // and then check whether to commit.
            ok_commit(inner.m0.footprint()?, inner.root.m0_limit)?
        };

        let start = time::SystemTime::now();

        match cmd.as_str() {
            "do_commit" if ok_to_commit => {
                let res = Dgm::do_commit(&inner);
                match res {
                    Ok(_) => info!(target: "dgm   ", "{:?}, commit done", name),
                    Err(err) => {
                        error!(
                            target: "dgm   ", "{:?}, commit err:{:?}", name, err
                        );
                        break Err(err);
                    }
                };
                match resp_tx {
                    Some(tx) => err_at!(IPCFail, tx.send(res))?,
                    None => (),
                }
            }
            "" => (),
            _ => break err_at!(Fatal, msg: format!("unreachable")),
        }

        elapsed = start.elapsed().ok().unwrap();
    }
}

fn auto_compact<K, V, M, D>(
    name: String,
    root: Root,
    inner: Arc<Mutex<InnerDgm<K, V, M, D>>>,
    rx: rt::Rx<String, Result<usize>>,
) -> Result<()>
where
    K: 'static + Send + Clone + Ord + Serialize + Footprint,
    V: 'static + Send + Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: 'static + Send + WriteIndexFactory<K, V>,
    D: 'static + Send + DiskIndexFactory<K, V>,
    <M as WriteIndexFactory<K, V>>::I: 'static + Send + Footprint,
    <<M as WriteIndexFactory<K, V>>::I as Index<K, V>>::R: 'static + Send,
    <<M as WriteIndexFactory<K, V>>::I as Index<K, V>>::W: 'static + Send,
    <D as DiskIndexFactory<K, V>>::I: 'static + Send + CommitIterator<K, V> + Footprint + Clone,
    <<D as DiskIndexFactory<K, V>>::I as Index<K, V>>::R: 'static + Send,
    <<D as DiskIndexFactory<K, V>>::I as Index<K, V>>::W: 'static + Send,
{
    let compact_interval = root.compact_interval.unwrap();

    info!(
        target: "dgm   ",
        "{}, auto-compacting thread started with interval {:?}",
        name, compact_interval,
    );

    let mut elapsed = time::Duration::new(0, 0);
    loop {
        let resp_tx = {
            let interval = {
                let elapsed = cmp::min(compact_interval, elapsed);
                compact_interval - elapsed
            };
            match rx.recv_timeout(interval) {
                Ok((cmd, resp_tx)) if cmd == "do_compact" => resp_tx,
                Ok(_) => break err_at!(Fatal, msg: format!("unreachable")),
                Err(mpsc::RecvTimeoutError::Timeout) => None,
                Err(mpsc::RecvTimeoutError::Disconnected) => break Ok(()),
            }
        };

        let start = time::SystemTime::now();

        let res = Dgm::do_compact(&inner, Cutoff::new_lsm_empty());

        match resp_tx {
            Some(tx) => err_at!(IPCFail, tx.send(res))?,
            None => match res {
                Ok(n) => info!(
                    target: "dgm   ", "{:?}, compact done: {}", name, n
                ),
                Err(err) => {
                    info!(
                        target: "dgm   ", "{:?}, compact err, {:?}", name, err
                    );
                    break Err(err);
                }
            },
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
    <V as Diff>::D: Serialize,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
    M::I: Footprint,
    D::I: Footprint + Clone,
{
    match inner.lock() {
        Ok(value) => Ok(value),
        Err(err) => err_at!(Fatal, msg: format!("poisened lock {}", err)),
    }
}

/// TODO: populate with meaningful stats for Dgm index.
pub struct Stats<A, B>
where
    A: fmt::Display,
    B: fmt::Display,
{
    _phantom_key: marker::PhantomData<A>,
    _phantom_val: marker::PhantomData<B>,
}

impl<A, B> fmt::Display for Stats<A, B>
where
    A: fmt::Display,
    B: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "Dgm::Stats<>")
    }
}

fn ok_commit(m0_fp: isize, m0_limit: Option<usize>) -> Result<bool> {
    match m0_limit {
        Some(0) => {
            let m = match sys_info::mem_info() {
                Ok(m) => Ok(m),
                Err(err) => err_at!(SystemFail, msg: format!("{:?}", err)),
            }?;
            Ok((m0_fp * 3) > (m.avail as isize)) // TODO: no magic formula
        }
        Some(m0_limit) if m0_fp > (m0_limit as isize) => Ok(true),
        Some(_) | None => Ok(false),
    }
}

#[cfg(test)]
#[path = "dgm_test.rs"]
mod dgm_test;
