//! Module `robt` implement a read-only-btree for disk based indexes.
//!
//! ROBT instances shall have an index file and an optional value-log-file,
//! refer to [Config] for more information.
//!
//! **Index-file format**:
//!
//! ```text
//! *------------------------------------------* SeekFrom::End(0)
//! |                marker-length             |
//! *------------------------------------------* SeekFrom::End(-8)
//! |                stats-length              |
//! *------------------------------------------* SeekFrom::End(-16)
//! |             app-metadata-length          |
//! *------------------------------------------* SeekFrom::End(-24)
//! |                bitmap-length             |
//! *------------------------------------------* SeekFrom::End(-32)
//! |                  root-fpos               |
//! *------------------------------------------* SeekFrom::MetaBlock
//! *                 meta-blocks              *
//! *                    ...                   *
//! *------------------------------------------*
//! *                btree-blocks              *
//! *                    ...                   *
//! *                    ...                   *
//! *------------------------------------------* 0
//! ```
//!
//! Tip of the index file contain 40-byte header providing
//! following details:
//! * Index statistics
//! * Application metadata
//! * Bitmap length, to optimize missing key lookups.
//! * File-position for btree's root-block.
//!
//! Total length of `metadata-blocks` can be computed based on
//! `marker-length`, `stats-length`, `app-metadata-length`, `bitmap-length`.
//!
//! [Config]: crate::robt::Config
//!

use fs2::FileExt;
use lazy_static::lazy_static;
use log::{debug, error, info};

use std::{
    borrow::Borrow,
    cmp,
    convert::{TryFrom, TryInto},
    ffi, fmt, fs,
    hash::Hash,
    io::{self, Read, Seek, Write},
    marker, mem,
    ops::{Bound, Deref, RangeBounds},
    path, result,
    str::FromStr,
    sync::{self, mpsc, Arc, MutexGuard},
    thread, time,
};

#[allow(unused_imports)] // for documentation
use crate::rdms::Rdms;
use crate::{
    core::Cutoff,
    core::{self, Bloom, CommitIterator, Index, Serialize, ToJson, Validate},
    core::{Diff, DiskIndexFactory, Entry, Footprint, IndexIter, Reader, Result},
    error::Error,
    panic::Panic,
    robt_entry::MEntry,
    robt_index::{MBlock, ZBlock},
    scans, thread as rt, util,
};

include!("robt_marker.rs");

pub(crate) trait Flusher {
    fn post(&self, msg: Vec<u8>) -> Result<()>;
}

#[derive(Clone)]
struct Name(String);

impl Name {
    fn next(self) -> Name {
        let (s, ver): (String, usize) = TryFrom::try_from(self).unwrap();
        From::from((s, ver + 1))
    }

    #[allow(dead_code)] // TODO: remove if not required.
    fn previous(self) -> Option<Name> {
        let (s, ver): (String, usize) = TryFrom::try_from(self).unwrap();
        if ver == 0 {
            None
        } else {
            Some(From::from((s, ver - 1)))
        }
    }
}

impl From<(String, usize)> for Name {
    fn from((s, ver): (String, usize)) -> Name {
        Name(format!("{}-robt-{:03}", s, ver))
    }
}

impl TryFrom<Name> for (String, usize) {
    type Error = Error;

    fn try_from(name: Name) -> Result<(String, usize)> {
        let parts: Vec<&str> = name.0.split('-').collect();
        if parts.len() >= 3 {
            match &parts[parts.len() - 2..] {
                ["robt", ver] => {
                    let ver = parse_at!(ver, usize)?;
                    let s = parts[..(parts.len() - 2)].join("-");
                    Ok((s, ver))
                }
                _ => err_at!(InvalidFile, msg: format!("invalid name")),
            }
        } else {
            err_at!(InvalidFile, msg: format!("invalid name"))
        }
    }
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for Name {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone)]
struct IndexFileName(ffi::OsString);

impl From<Name> for IndexFileName {
    fn from(name: Name) -> IndexFileName {
        let file_name = format!("{}.indx", name.0);
        let name: &ffi::OsStr = file_name.as_ref();
        IndexFileName(name.to_os_string())
    }
}

impl TryFrom<IndexFileName> for Name {
    type Error = Error;

    fn try_from(fname: IndexFileName) -> Result<Name> {
        let check_file = |fname: IndexFileName| -> Option<String> {
            let fname = path::Path::new(&fname.0);
            match fname.extension()?.to_str()? {
                "indx" => Some(fname.file_stem()?.to_str()?.to_string()),
                _ => None,
            }
        };

        let name = match check_file(fname.clone()) {
            Some(name) => Ok(name),
            None => err_at!(InvalidFile, msg: format!("{}", fname)),
        }?;
        Ok(Name(name))
    }
}

impl From<IndexFileName> for ffi::OsString {
    fn from(name: IndexFileName) -> ffi::OsString {
        name.0
    }
}

impl fmt::Display for IndexFileName {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self.0.to_str() {
            Some(s) => write!(f, "{}", s),
            None => write!(f, "{:?}", self.0),
        }
    }
}

#[derive(Clone)]
struct VlogFileName(ffi::OsString);

impl From<Name> for VlogFileName {
    fn from(name: Name) -> VlogFileName {
        let file_name = format!("{}.vlog", name.0);
        let name: &ffi::OsStr = file_name.as_ref();
        VlogFileName(name.to_os_string())
    }
}

impl TryFrom<VlogFileName> for Name {
    type Error = Error;

    fn try_from(fname: VlogFileName) -> Result<Name> {
        let check_file = |fname: VlogFileName| -> Option<String> {
            let fname = path::Path::new(&fname.0);
            match fname.extension()?.to_str()? {
                "vlog" => Some(fname.file_stem()?.to_str()?.to_string()),
                _ => None,
            }
        };

        let name = match check_file(fname.clone()) {
            Some(name) => Ok(name),
            None => err_at!(InvalidFile, msg: format!("invalid file")),
        }?;
        Ok(Name(name))
    }
}

impl fmt::Display for VlogFileName {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self.0.to_str() {
            Some(s) => write!(f, "{}", s),
            None => write!(f, "{:?}", self.0),
        }
    }
}

/// Return a factory to construct Robt instances with given ``config``.
/// Refer [DiskIndexFactory] for more details.
pub fn robt_factory<K, V, B>(config: Config) -> RobtFactory<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    RobtFactory {
        config,

        _phantom_key: marker::PhantomData,
        _phantom_val: marker::PhantomData,
        _phantom_bitmap: marker::PhantomData,
    }
}

/// Factory type, to construct pre-configured Robt instances.
///
/// Refer [DiskIndexFactory] for more details.
pub struct RobtFactory<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    config: Config,

    _phantom_key: marker::PhantomData<K>,
    _phantom_val: marker::PhantomData<V>,
    _phantom_bitmap: marker::PhantomData<B>,
}

impl<K, V, B> DiskIndexFactory<K, V> for RobtFactory<K, V, B>
where
    K: Default + Clone + Ord + Hash + Footprint + Serialize,
    V: Default + Clone + Diff + Footprint + Serialize,
    <V as Diff>::D: Default + Serialize,
    B: Bloom,
{
    type I = Robt<K, V, B>;

    fn new(&self, dir: &ffi::OsStr, name: &str) -> Result<Robt<K, V, B>> {
        let mut config = self.config.clone();
        config.name = name.to_string();

        Robt::new(dir, name, config)
    }

    fn open(&self, dir: &ffi::OsStr, name: &str) -> Result<Robt<K, V, B>> {
        debug!(
            target: "robtfc",
            "{}, open from {:?} ...", name, dir,
        );

        Robt::open(dir, name)
    }

    fn to_type(&self) -> String {
        "robt".to_string()
    }
}

/// Index type, immutable, durable, fully-packed and lockless reads.
pub struct Robt<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    B: Bloom,
{
    inner: sync::Mutex<InnerRobt<K, V, B>>,
    purger: Option<rt::Thread<ffi::OsString, (), ()>>,
}

enum InnerRobt<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    Build {
        dir: ffi::OsString,
        name: Name,
        config: Config,

        _phantom_key: marker::PhantomData<K>,
        _phantom_val: marker::PhantomData<V>,
    },
    Snapshot {
        dir: ffi::OsString,
        name: Name,
        footprint: isize,
        meta: Vec<MetaItem>,
        config: Config,
        stats: Stats,
        bitmap: Arc<B>,
    },
}

impl<K, V, B> Clone for InnerRobt<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    fn clone(&self) -> Self {
        match self {
            InnerRobt::Build {
                dir, name, config, ..
            } => InnerRobt::Build {
                dir: dir.clone(),
                name: name.clone(),
                config: config.clone(),

                _phantom_key: marker::PhantomData,
                _phantom_val: marker::PhantomData,
            },
            InnerRobt::Snapshot {
                dir,
                name,
                footprint,
                meta,
                config,
                stats,
                bitmap,
            } => InnerRobt::Snapshot {
                dir: dir.clone(),
                name: name.clone(),
                footprint: footprint.clone(),
                meta: meta.clone(),
                config: config.clone(),
                stats: stats.clone(),
                bitmap: Arc::clone(bitmap),
            },
        }
    }
}

impl<K, V, B> Clone for Robt<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    B: Bloom,
{
    fn clone(&self) -> Self {
        let inner = self.as_inner().unwrap();
        let name = match inner.deref() {
            InnerRobt::Build { name, .. } => name.clone(),
            InnerRobt::Snapshot { name, .. } => name.clone(),
        };
        let purger = {
            let name = name.to_string();
            rt::Thread::new(format!("robt-purger-{}", name), move |rx| {
                move || thread_purger(name, rx)
            })
        };
        Robt {
            inner: sync::Mutex::new(inner.clone()),
            purger: Some(purger),
        }
    }
}

impl<K, V, B> Drop for Robt<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    B: Bloom,
{
    fn drop(&mut self) {
        let (name, dir) = loop {
            match self.inner.get_mut() {
                Ok(inner) => match inner {
                    InnerRobt::Build { name, dir, .. } => break (name.clone(), dir.clone()),
                    InnerRobt::Snapshot { name, dir, .. } => break (name.clone(), dir.clone()),
                },
                Err(err) => error!(target: "robt  ", "drop {}", err),
            }
        };

        // wait till purger routine exit.
        match self.purger.take() {
            Some(purger) => match purger.close_wait() {
                Err(err) => error!(
                    target: "robt  ", "{}, purger failed {:?}", name, err
                ),
                Ok(_) => (),
            },
            None => (),
        }

        debug!(target: "robt  ", "{:?}/{}, dropped", dir, name);
    }
}

impl<K, V, B> Robt<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    B: Bloom,
{
    pub fn new(dir: &ffi::OsStr, name: &str, mut config: Config) -> Result<Robt<K, V, B>> {
        config.name = name.to_string();

        let inner = InnerRobt::Build {
            dir: dir.to_os_string(),
            name: (name.to_string(), 0).into(),
            config: config.clone(),

            _phantom_key: marker::PhantomData,
            _phantom_val: marker::PhantomData,
        };

        let purger = {
            let name = name.to_string();
            rt::Thread::new(format!("robt-purger-{}", name), move |rx| {
                move || thread_purger(name, rx)
            })
        };

        debug!(
            target: "robt  ", "{:?}/{}, new instance with config ...\n{}",
            dir, name, config
        );

        Ok(Robt {
            inner: sync::Mutex::new(inner),
            purger: Some(purger),
        })
    }

    pub fn open(dir: &ffi::OsStr, name: &str) -> Result<Robt<K, V, B>> {
        let index_file = Self::find_index_file(dir, name)?;
        let name: Name = TryFrom::try_from(IndexFileName(index_file))?;

        let snapshot = Snapshot::<K, V, B>::open(dir, &name.0)?;

        let inner = InnerRobt::Snapshot {
            dir: dir.to_os_string(),
            name: name.clone(),
            footprint: snapshot.footprint()?,
            meta: snapshot.meta.clone(),
            config: snapshot.config.clone(),
            stats: snapshot.to_stats()?,
            bitmap: Arc::clone(&snapshot.bitmap),
        };

        let purger = {
            let name = name.0.clone();
            rt::Thread::new(format!("robt-purger-{}", name), move |rx| {
                move || thread_purger(name, rx)
            })
        };

        debug!(
            target: "robt  ", "{:?}/{}, open instance with config ...\n{}",
            dir, name, snapshot.config
        );

        Ok(Robt {
            inner: sync::Mutex::new(inner),
            purger: Some(purger),
        })
    }

    pub(crate) fn to_clone(&self) -> Result<Self> {
        let inner = self.as_inner()?;
        let name = match inner.deref() {
            InnerRobt::Build { name, .. } => name.clone(),
            InnerRobt::Snapshot { name, .. } => name.clone(),
        };
        let purger = rt::Thread::new(format!("robt-purger-{}", name), move |rx| {
            move || thread_purger(name.0, rx)
        });
        Ok(Robt {
            inner: sync::Mutex::new(inner.deref().clone()),
            purger: Some(purger),
        })
    }

    pub fn purge_files(&self, files: Vec<ffi::OsString>) -> Result<()> {
        for file in files.into_iter() {
            // verify that requested files to be purged belong to this Robt
            // instance and also assert that it belongs to an older snapshot.
            let file_name = match path::PathBuf::from(file.clone()).file_name() {
                Some(file_name) => Ok(file_name.to_os_string()),
                None => err_at!(InvalidFile, msg: format!("purge_files() {:?}", file)),
            }?;
            let nm: Name = match IndexFileName(file_name.clone()).try_into() {
                Ok(nm) => nm,
                Err(_) => VlogFileName(file_name).try_into()?,
            };
            let (nm, ver): (String, usize) = nm.clone().try_into()?;

            let (name, version) = match self.as_inner()?.deref() {
                InnerRobt::Build { name, .. } => {
                    let (name, ver): (String, usize) = name.clone().try_into()?;
                    (name, ver)
                }
                InnerRobt::Snapshot { name, .. } => {
                    let (name, ver): (String, usize) = name.clone().try_into()?;
                    (name, ver)
                }
            };

            if nm == name && ver < version {
                self.purger.as_ref().unwrap().post(file)?;
                Ok(())
            } else {
                err_at!(
                    InvalidFile,
                    msg: format!(
                        "purge_files() {:?} {} {} {} {}",
                        file, nm, name, ver, version
                    )
                )
            }?;
        }

        Ok(())
    }

    fn find_index_file(dir: &ffi::OsStr, name: &str) -> Result<ffi::OsString> {
        let mut versions = vec![];
        for item in err_at!(IoError, fs::read_dir(dir))? {
            match item {
                Ok(item) => {
                    let index_file = IndexFileName(item.file_name());
                    let nm: Result<Name> = index_file.try_into();
                    match nm {
                        Ok(nm) => match nm.try_into() {
                            Ok((nm, ver)) if nm == name => versions.push(ver),
                            _ => continue,
                        },
                        _ => continue,
                    }
                }
                _ => continue,
            }
        }

        let version = match versions.into_iter().max() {
            Some(version) => Ok(version),
            None => err_at!(InvalidInput, msg: format!("invalid file")),
        }?;

        let nm: Name = (name.to_string(), version).into();
        let index_file: IndexFileName = nm.into();

        Ok(index_file.into())
    }
}

impl<K, V, B> Robt<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    B: Bloom,
{
    /// Return Index's version number, every commit and compact shall increment
    /// the version number.
    pub fn to_version(&self) -> Result<usize> {
        let name = match self.as_inner()?.deref() {
            InnerRobt::Build { name, .. } => name.clone(),
            InnerRobt::Snapshot { name, .. } => name.clone(),
        };
        let parts: (String, usize) = TryFrom::try_from(name)?;
        Ok(parts.1) // version
    }

    pub fn to_next_version(&mut self) -> Result<Vec<ffi::OsString>> {
        let mut inner = self.as_inner()?;
        let (new_inner, purge_files) = match inner.deref() {
            InnerRobt::Build { .. } => err_at!(Fatal, msg: format!("unreachable")),
            InnerRobt::Snapshot {
                dir, name, config, ..
            } => {
                let mut purge_files = vec![];
                let old = Snapshot::<K, V, B>::open(dir, &name.0)?;

                // purge old snapshots file(s).
                purge_files.push(old.index_fd.to_file());
                if let Some((file, _)) = &old.valog_fd {
                    purge_files.push(file.clone());
                }

                let mut config = config.clone();
                config.vlog_file = None; // ignore the old file.

                Ok((
                    InnerRobt::Build {
                        dir: dir.clone(),
                        name: name.clone().next(),
                        config,

                        _phantom_key: marker::PhantomData,
                        _phantom_val: marker::PhantomData,
                    },
                    purge_files,
                ))
            }
        }?;
        *inner = new_inner;
        Ok(purge_files)
    }

    fn as_inner(&self) -> Result<MutexGuard<InnerRobt<K, V, B>>> {
        match self.inner.lock() {
            Ok(value) => Ok(value),
            Err(err) => err_at!(Fatal, msg: format!("poisened lock {}", err)),
        }
    }

    fn do_close(&mut self) -> Result<()> {
        match self.purger.take() {
            Some(purger) => purger.close_wait()?,
            None => (),
        };
        Ok(())
    }

    pub fn to_partitions(&mut self) -> Result<Vec<(Bound<K>, Bound<K>)>>
    where
        K: Default + Hash + Footprint,
        V: Default + Footprint,
        <V as Diff>::D: Default,
    {
        self.to_reader()?.to_partitions()
    }

    pub fn len(&self) -> Result<usize> {
        match self.as_inner()?.deref() {
            InnerRobt::Snapshot { stats, .. } => Ok(convert_at!(stats.n_count)?),
            InnerRobt::Build { .. } => err_at!(UnInitialized, msg: format!("Robt.len()")),
        }
    }

    #[allow(dead_code)] // TODO: remove if not required.
    fn is_vlog(&self) -> Result<bool> {
        match self.as_inner()?.deref() {
            InnerRobt::Build { config, .. } => {
                //
                Ok(config.delta_ok || config.value_in_vlog)
            }
            InnerRobt::Snapshot { config, .. } => {
                //
                Ok(config.delta_ok || config.value_in_vlog)
            }
        }
    }
}

impl<K, V, B> Validate<Stats> for Robt<K, V, B>
where
    K: Default + Clone + Ord + Serialize + fmt::Debug,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
    B: Bloom,
{
    fn validate(&mut self) -> Result<Stats> {
        let inner = self.as_inner()?;
        match inner.deref() {
            InnerRobt::Snapshot { dir, name, .. } => {
                let mut snapshot = Snapshot::<K, V, B>::open(dir, &name.0)?;
                snapshot.validate()
            }
            InnerRobt::Build { .. } => err_at!(Fatal, msg: format!("unreachable")),
        }
    }
}

impl<K, V, B> Index<K, V> for Robt<K, V, B>
where
    K: Default + Clone + Ord + Hash + Footprint + Serialize,
    V: Default + Clone + Diff + Footprint + Serialize,
    <V as Diff>::D: Default + Serialize,
    B: Bloom,
{
    type R = Snapshot<K, V, B>;
    type W = Panic;

    fn to_name(&self) -> Result<String> {
        let name = match self.as_inner()?.deref() {
            InnerRobt::Build { name, .. } => name.clone(),
            InnerRobt::Snapshot { name, .. } => name.clone(),
        };
        let parts: (String, usize) = TryFrom::try_from(name)?;
        Ok(parts.0) // just the name as passed to new().
    }

    fn to_metadata(&self) -> Result<Vec<u8>> {
        match self.as_inner()?.deref() {
            InnerRobt::Snapshot { meta, .. } => {
                if let MetaItem::AppMetadata(data) = &meta[2] {
                    Ok(data.clone())
                } else {
                    err_at!(Fatal, msg: format!("unreachable"))
                }
            }
            InnerRobt::Build { .. } => err_at!(UnInitialized, msg: format!("Robt.to_metadata()")),
        }
    }

    /// Return the current seqno tracked by this index.
    fn to_seqno(&self) -> Result<u64> {
        match self.as_inner()?.deref() {
            InnerRobt::Snapshot { stats, .. } => Ok(stats.seqno),
            InnerRobt::Build { .. } => err_at!(UnInitialized, msg: format!("Robt.to_seqno()")),
        }
    }

    /// Application can set the start sequence number for this index.
    fn set_seqno(&mut self, _seqno: u64) -> Result<()> {
        Ok(())
    }

    fn to_reader(&mut self) -> Result<Self::R> {
        match self.as_inner()?.deref() {
            InnerRobt::Snapshot {
                dir, name, bitmap, ..
            } => {
                //println!("Robt.to_reader() {:?} {}", dir, name);
                let mut snapshot = Snapshot::open(dir, &name.0)?;
                snapshot.set_bitmap(Arc::clone(bitmap));
                Ok(snapshot)
            }
            InnerRobt::Build { .. } => err_at!(UnInitialized, msg: format!("Robt.to_reader()")),
        }
    }

    fn to_writer(&mut self) -> Result<Self::W> {
        Ok(Panic::new("robt"))
    }

    fn commit<C, F>(&mut self, mut scanner: core::CommitIter<K, V, C>, metacb: F) -> Result<()>
    where
        C: CommitIterator<K, V>,
        F: Fn(Vec<u8>) -> Vec<u8>,
    {
        let mut inner = self.as_inner()?;
        let new_inner = match inner.deref() {
            InnerRobt::Build {
                dir, name, config, ..
            } => {
                let (snapshot, meta_block_bytes) = {
                    let config = config.clone();
                    let b = Builder::<K, V, B>::initial(dir, &name.0, config)?;
                    let meta_block_bytes = b.build(scanner.scan()?, metacb(vec![]))?;

                    let snapshot = Snapshot::<K, V, B>::open(dir, &name.0)?;
                    (snapshot, meta_block_bytes)
                };

                let stats = snapshot.to_stats()?;
                let footprint = snapshot.footprint()?;

                let index_file = snapshot.index_fd.to_file();
                let vlog_file = snapshot
                    .valog_fd
                    .as_ref()
                    .map(|(vf, _)| vf.clone())
                    .unwrap_or(ffi::OsString::new());
                debug!(
                    target: "robt  ",
                    "{:?}/{}, flush commit to index_file:{:?}, vlog_file:{:?}  footprint:{} wrote:{}",
                    dir, name, index_file, vlog_file, footprint,
                    stats.z_bytes + stats.m_bytes + stats.v_bytes + meta_block_bytes
                );

                InnerRobt::Snapshot {
                    dir: dir.clone(),
                    name: name.clone(),
                    footprint,
                    meta: snapshot.meta.clone(),
                    config: snapshot.config.clone(),
                    stats,
                    bitmap: Arc::clone(&snapshot.bitmap),
                }
            }
            InnerRobt::Snapshot {
                dir, name, config, ..
            } => {
                let mut old = Snapshot::<K, V, B>::open(dir, &name.0)?;
                let old_seqno = old.to_seqno()?;
                let old_bitmap = Arc::clone(&old.bitmap);

                let (name, snapshot, meta_block_bytes) = {
                    let bitmap_iter = scans::BitmappedScan::new(scanner.scan()?);
                    let commit_iter = {
                        let mut mzs = vec![];
                        match old.to_root() {
                            Ok(root) => Ok(old.build_fwd(root, &mut mzs)?),
                            Err(Error::EmptyIndex) => Ok(()),
                            Err(err) => Err(err),
                        }?;
                        let old_iter = Iter::new_shallow(&mut old, mzs);
                        CommitScan::new(bitmap_iter, old_iter)
                    };
                    let mut build_iter = BuildScan::new(commit_iter, old_seqno);

                    let (name, mut b) = {
                        let name = name.clone().next();
                        let b = Builder::<K, V, B>::incremental(
                            //
                            dir,
                            &name.0,
                            config.clone(),
                        )?;
                        (name, b)
                    };

                    let root = b.build_tree(&mut build_iter)?;

                    let commit_iter = build_iter.update_stats(&mut b.stats)?;
                    let (bitmap_iter, _) = commit_iter.close()?;
                    let (_, new_bitmap): (_, B) = bitmap_iter.close()?;

                    let bitmap = old_bitmap.or(&new_bitmap)?;

                    debug!(
                        target: "robt  ",
                        "{:?}/{}, incremental commit old_bitmap({}) + new_bitmap({}) = {}",
                        dir, name, old_bitmap.len()?, new_bitmap.len()?, bitmap.len()?
                    );

                    let meta_block_bytes = {
                        let meta = metacb(old.to_app_meta()?);
                        b.build_finish(meta, bitmap, root)?
                    };

                    let snapshot = Snapshot::<K, V, B>::open(dir, &name.0)?;

                    // purge old snapshot's index file.
                    self.purger.as_ref().unwrap().post(old.index_fd.to_file())?;

                    (name, snapshot, meta_block_bytes)
                };

                let stats = snapshot.to_stats()?;
                let footprint = snapshot.footprint()?;

                let index_file = snapshot.index_fd.to_file();
                let vlog_file = snapshot
                    .valog_fd
                    .as_ref()
                    .map(|(vf, _)| vf.clone())
                    .unwrap_or(ffi::OsString::new());
                debug!(
                    target: "robt  ",
                    "{:?}/{}, incremental commit to index_file:{:?}, vlog_file:{:?}  footprint:{} wrote:{}",
                    dir, name, index_file, vlog_file, footprint,
                    stats.z_bytes + stats.m_bytes + stats.v_bytes + meta_block_bytes
                );

                InnerRobt::Snapshot {
                    dir: dir.clone(),
                    name: name.clone(),
                    footprint,
                    meta: snapshot.meta.clone(),
                    config: snapshot.config.clone(),
                    stats,
                    bitmap: Arc::clone(&snapshot.bitmap),
                }
            }
        };
        *inner = new_inner;
        Ok(())
    }

    fn compact(&mut self, cutoff: Cutoff) -> Result<usize> {
        let mut inner = self.as_inner()?;
        let (new_inner, count) = match inner.deref() {
            InnerRobt::Build {
                dir, name, config, ..
            } => {
                error!(
                    target: "robt  ",
                    "{}, cannot compact in build state ...", name
                );

                (
                    InnerRobt::Build {
                        dir: dir.clone(),
                        name: name.clone(),
                        config: config.clone(),
                        _phantom_key: marker::PhantomData,
                        _phantom_val: marker::PhantomData,
                    },
                    0,
                )
            }
            InnerRobt::Snapshot {
                dir,
                name,
                config,
                meta,
                ..
            } => {
                {
                    // skip compaction if cutoff is empty and the previous
                    // build started from a clean vlog file.
                    let old = Snapshot::<K, V, B>::open(dir, &name.0)?;
                    let stats = old.to_stats()?;
                    if cutoff.is_empty() && stats.n_abytes == 0 {
                        return Ok(0);
                    }
                }

                let (name, snapshot, meta_block_bytes) = {
                    let mut old = Snapshot::<K, V, B>::open(dir, &name.0)?;
                    let old_seqno: u64 = old.to_seqno()?;

                    let comp_iter = {
                        let iter = old.iter_with_versions()?;
                        scans::CompactScan::new(iter, cutoff)
                    };

                    let name = name.clone().next();
                    let (meta_block_bytes, snapshot) = {
                        let conf = {
                            let mut conf = config.clone();
                            conf.vlog_file = None; // use a new vlog file.
                            conf
                        };
                        let meta = match &meta[2] {
                            MetaItem::AppMetadata(data) => data.clone(),
                            _ => err_at!(Fatal, msg: format!("unreachable"))?,
                        };
                        let mut b = Builder::<K, V, B>::initial(dir, &name.0, conf)?;
                        // let mbbytes = b.build(comp_iter, meta)?;

                        let (root, bitmap): (u64, B) = {
                            let mut bditer = {
                                let btiter = scans::BitmappedScan::new(comp_iter);
                                BuildScan::new(btiter, old_seqno)
                            };
                            let root = b.build_tree(&mut bditer)?;
                            let btiter = bditer.update_stats(&mut b.stats)?;
                            let (_, bitmap) = btiter.close()?;
                            (root, bitmap)
                        };
                        let mbbytes = b.build_finish(meta, bitmap, root)?;

                        (mbbytes, Snapshot::<K, V, B>::open(dir, &name.0)?)
                    };

                    // purge old snapshots file(s).
                    self.purger.as_ref().unwrap().post(old.index_fd.to_file())?;
                    if let Some((file, _)) = &old.valog_fd {
                        self.purger.as_ref().unwrap().post(file.clone())?;
                    }

                    (name, snapshot, meta_block_bytes)
                };

                let stats = snapshot.to_stats()?;
                let footprint = snapshot.footprint()?;

                let index_file = snapshot.index_fd.to_file();
                let vlog_file = snapshot
                    .valog_fd
                    .as_ref()
                    .map(|(vf, _)| vf.clone())
                    .unwrap_or(ffi::OsString::new());
                debug!(
                    target: "robt  ",
                    "{:?}/{}, compacted to index_file:{:?} vlog_file:{:?} footprint:{} wrote:{}",
                    dir, name, index_file, vlog_file, footprint,
                    stats.z_bytes + stats.m_bytes + stats.v_bytes + meta_block_bytes
                );

                (
                    InnerRobt::Snapshot {
                        dir: dir.clone(),
                        name: name.clone(),
                        footprint,
                        meta: snapshot.meta.clone(),
                        config: snapshot.config.clone(),
                        stats: stats.clone(),
                        bitmap: Arc::clone(&snapshot.bitmap),
                    },
                    stats.n_count,
                )
            }
        };
        *inner = new_inner;
        Ok(convert_at!(count)?)
    }

    fn close(mut self) -> Result<()> {
        let (dir, name) = match self.as_inner()?.deref() {
            InnerRobt::Snapshot { dir, name, .. } => (dir.clone(), name.clone()),
            InnerRobt::Build { dir, name, .. } => (dir.clone(), name.clone()),
        };

        self.do_close()?;

        debug!(target: "robt  ", "{:?}/{} closed", dir, name);

        Ok(())
    }

    fn purge(mut self) -> Result<()> {
        self.do_close()?;
        let (res, dir, name) = match self.as_inner()?.deref() {
            InnerRobt::Snapshot { dir, name, .. } => {
                let snapshot = Snapshot::<K, V, B>::open(&dir, &name.0)?;
                (snapshot.purge(), dir.clone(), name.clone())
            }
            InnerRobt::Build { dir, name, .. } => {
                let res = err_at!(UnInitialized, msg: format!("Robt.purge()"));
                (res, dir.clone(), name.clone())
            }
        };

        debug!(target: "robt  ", "{:?}/{} purged", dir, name);

        res
    }
}

impl<K, V, B> CommitIterator<K, V> for Robt<K, V, B>
where
    K: Default + Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Serialize,
    B: Bloom,
{
    fn scan<G>(&mut self, within: G) -> Result<IndexIter<K, V>>
    where
        G: Clone + RangeBounds<u64>,
    {
        match self.as_inner()?.deref() {
            InnerRobt::Snapshot { dir, name, .. } => {
                let snap = Snapshot::<K, V, B>::open(dir, &name.0)?;
                let iter = {
                    let iter = snap.into_scan()?;
                    scans::FilterScans::new(vec![iter], within)
                };
                Ok(Box::new(iter))
            }
            InnerRobt::Build { .. } => err_at!(UnInitialized, msg: format!("Robt.scan()")),
        }
    }

    fn scans<G>(&mut self, n_shards: usize, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let inner = self.as_inner()?;
        match inner.deref() {
            InnerRobt::Snapshot { dir, name, .. } => {
                let ranges = {
                    let mut snap = Snapshot::<K, V, B>::open(dir, &name.0)?;
                    snap.to_shards(n_shards)?
                };

                let mut iters = vec![];
                for range in ranges.into_iter() {
                    let snap = Snapshot::<K, V, B>::open(dir, &name.0)?;
                    let iter: IndexIter<K, V> = Box::new(scans::FilterScans::new(
                        vec![snap.into_range_scan(range)?],
                        within.clone(),
                    ));
                    iters.push(iter)
                }

                // If there are not enough shards push empty iterators.
                for _ in iters.len()..n_shards {
                    let ss = vec![];
                    iters.push(Box::new(ss.into_iter()));
                }

                assert_eq!(iters.len(), n_shards);

                Ok(iters)
            }
            InnerRobt::Build { .. } => err_at!(UnInitialized, msg: format!("Robt.scans()")),
        }
    }

    fn range_scans<N, G>(&mut self, ranges: Vec<N>, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
        N: Clone + RangeBounds<K>,
    {
        let inner = self.as_inner()?;
        match inner.deref() {
            InnerRobt::Snapshot { dir, name, .. } => {
                let mut iters = vec![];
                for range in ranges.into_iter() {
                    let snap = Snapshot::<K, V, B>::open(dir, &name.0)?;
                    let iter: IndexIter<K, V> = Box::new(scans::FilterScans::new(
                        vec![snap.into_range_scan(util::to_start_end(range))?],
                        within.clone(),
                    ));

                    iters.push(iter)
                }

                Ok(iters)
            }
            InnerRobt::Build { .. } => err_at!(UnInitialized, msg: format!("Robt.range_scans()")),
        }
    }
}

impl<K, V, B> Footprint for Robt<K, V, B>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize + Footprint,
    B: Bloom,
{
    fn footprint(&self) -> Result<isize> {
        let inner = self.as_inner()?;
        match inner.deref() {
            InnerRobt::Snapshot { footprint, .. } => Ok(*footprint),
            InnerRobt::Build { .. } => err_at!(Fatal, msg: format!("unreachable")),
        }
    }
}

/// Configuration type, for Read Only BTree.
#[derive(Clone)]
pub struct Config {
    /// location path where index files are created.
    pub(crate) dir: ffi::OsString,
    /// name of the index.
    pub(crate) name: String,
    /// Leaf block size in btree index.
    /// Default: Config::ZBLOCKSIZE
    pub(crate) z_blocksize: usize,
    /// Intermediate block size in btree index.
    /// Default: Config::MBLOCKSIZE
    pub(crate) m_blocksize: usize,
    /// If deltas are indexed and/or value to be stored in separate log file.
    /// Default: Config::VBLOCKSIZE
    pub(crate) v_blocksize: usize,
    /// Include delta as part of entry. Note that delta values are always
    /// stored in separate value-log file.
    /// Default: true
    pub(crate) delta_ok: bool,
    /// Optional name for value log file. If not supplied, but `delta_ok` or
    /// `value_in_vlog` is true, then value log file name will be computed
    /// based on configuration`name` and `dir`. Default: None
    pub(crate) vlog_file: Option<ffi::OsString>,
    /// If true, then value shall be persisted in value log file. Otherwise
    /// value shall be saved in the index' leaf node. Default: false
    pub(crate) value_in_vlog: bool,
    /// Flush queue size. Default: Config::FLUSH_QUEUE_SIZE
    pub(crate) flush_queue_size: usize,
}

impl Default for Config {
    /// New configuration with default parameters:
    ///
    /// * With ZBLOCKSIZE, MBLOCKSIZE, VBLOCKSIZE.
    /// * Values are stored in the leaf node.
    /// * LSM entries are preserved.
    /// * Deltas are persisted in default value-log-file.
    /// * Main index is persisted in default index-file.
    fn default() -> Config {
        let dir: &ffi::OsStr = "not/a/path".as_ref();
        Config {
            name: "-not-a-name-".to_string(),
            dir: dir.to_os_string(),
            z_blocksize: Self::ZBLOCKSIZE,
            v_blocksize: Self::VBLOCKSIZE,
            m_blocksize: Self::MBLOCKSIZE,
            delta_ok: true,
            vlog_file: Default::default(),
            value_in_vlog: false,
            flush_queue_size: Self::FLUSH_QUEUE_SIZE,
        }
    }
}

impl Config {
    /// Default value for z-block-size, 4 * 1024 bytes.
    pub const ZBLOCKSIZE: usize = 4 * 1024; // 4KB leaf node
    /// Default value for m-block-size, 4 * 1024 bytes.
    pub const MBLOCKSIZE: usize = 4 * 1024; // 4KB intermediate node
    /// Default value for v-block-size, 4 * 1024 bytes.
    pub const VBLOCKSIZE: usize = 4 * 1024; // 4KB of blobs.
    /// Marker block size, not to be tampered with.
    const MARKER_BLOCK_SIZE: usize = 1024 * 4;
    /// Default Flush queue size, channel queue size, holding index blocks.
    const FLUSH_QUEUE_SIZE: usize = 64;

    /// Configure differt set of block size for leaf-node, intermediate-node.
    pub fn set_blocksize(&mut self, z: usize, v: usize, m: usize) -> Result<&mut Self> {
        self.z_blocksize = z;
        self.v_blocksize = v;
        self.m_blocksize = m;
        Ok(self)
    }

    /// Enable delta persistence, and configure value-log-file. To disable
    /// delta persistance, pass `vlog_file` as None.
    pub fn set_delta(&mut self, vlog_file: Option<ffi::OsString>, ok: bool) -> Result<&mut Self> {
        match vlog_file {
            Some(vlog_file) => {
                self.delta_ok = true;
                self.vlog_file = Some(vlog_file);
            }
            None if ok => self.delta_ok = true,
            None => self.delta_ok = false,
        }
        Ok(self)
    }

    /// Persist values in a separate file, called value-log file. To persist
    /// values along with leaf node, pass `ok` as false.
    pub fn set_value_log(&mut self, file: Option<ffi::OsString>, ok: bool) -> Result<&mut Self> {
        match file {
            Some(vlog_file) => {
                self.value_in_vlog = true;
                self.vlog_file = Some(vlog_file);
            }
            None if ok => self.value_in_vlog = true,
            None => self.value_in_vlog = false,
        }
        Ok(self)
    }

    /// Set flush queue size, increasing the queue size will improve batch
    /// flushing.
    pub fn set_flush_queue_size(&mut self, size: usize) -> Result<&mut Self> {
        self.flush_queue_size = size;
        Ok(self)
    }
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        let vlog_file = self
            .vlog_file
            .as_ref()
            .map_or(r#""""#.to_string(), |f| format!("{:?}, ", f));

        let (z, m, v) = (self.z_blocksize, self.m_blocksize, self.v_blocksize);
        let dok = self.delta_ok;
        let fqs = self.flush_queue_size;

        write!(
            f,
            concat!(
                "robt.name = {}\n",
                "robt.config.blocksize = {{ z={}, m={}, v={} }}\n",
                "robt.config = {{ delta_ok={}, value_in_vlog={} vlog_file={} }}\n",
                "robt.config = {{ flush_queue_size={} }}",
            ),
            self.name, z, m, v, dok, self.value_in_vlog, vlog_file, fqs,
        )
    }
}

impl ToJson for Config {
    fn to_json(&self) -> String {
        let (z, m, v) = (self.z_blocksize, self.m_blocksize, self.v_blocksize);
        let null: ffi::OsString = From::from("null");
        let props = [
            format!(r#""blocksize":  {{ "z": {}, "m": {}, "v": {} }}"#, z, m, v),
            format!(r#""delta_ok": {}"#, self.delta_ok),
            format!(r#""value_in_vlog": {}"#, self.value_in_vlog),
            format!(
                r#""vlog_file": "{:?}""#,
                self.vlog_file.as_ref().map_or(null, |f| f.clone()),
            ),
            format!(r#""flush_queue_size": {}"#, self.flush_queue_size,),
        ];
        format!(
            r#"{{ "robt": {{ "name": "{}", "config": {{ {} }} }}"#,
            self.name,
            props.join(", ")
        )
    }
}

impl From<Stats> for Config {
    fn from(stats: Stats) -> Config {
        let dir: &ffi::OsStr = "not/a/path".as_ref();
        Config {
            name: stats.name,
            dir: dir.to_os_string(),
            z_blocksize: stats.z_blocksize,
            m_blocksize: stats.m_blocksize,
            v_blocksize: stats.v_blocksize,
            delta_ok: stats.delta_ok,
            vlog_file: stats.vlog_file,
            value_in_vlog: stats.value_in_vlog,
            flush_queue_size: stats.flush_queue_size,
        }
    }
}

impl Config {
    fn stitch_index_file(dir: &ffi::OsStr, name: &str) -> ffi::OsString {
        let index_file: IndexFileName = Name(name.to_string()).into();

        let mut index_path = path::PathBuf::from(dir);
        index_path.push(index_file.to_string());
        index_path.into_os_string()
    }

    fn stitch_vlog_file(dir: &ffi::OsStr, name: &str) -> ffi::OsString {
        let vlog_file: VlogFileName = Name(name.to_string()).into();

        let mut vlog_path = path::PathBuf::from(dir);
        vlog_path.push(vlog_file.to_string());
        vlog_path.into_os_string()
    }

    fn compute_root_block(n: usize) -> usize {
        if (n % Config::MARKER_BLOCK_SIZE) == 0 {
            n
        } else {
            ((n / Config::MARKER_BLOCK_SIZE) + 1) * Config::MARKER_BLOCK_SIZE
        }
    }
}

/// Enumeration of meta items stored in [Robt] index.
///
/// [Robt] index is a fully packed immutable [Btree] index. To interpret
/// the index a list of meta items are appended to the tip
/// of index-file.
///
/// [Btree]: https://en.wikipedia.org/wiki/B-tree
#[derive(Clone)]
pub enum MetaItem {
    /// A Unique marker that confirms that index file is valid.
    Marker(Vec<u8>), // tip of the file.
    /// Contains index-statistics along with configuration values.
    Stats(String),
    /// Application supplied metadata, typically serialized and opaque
    /// to [Rdms].
    AppMetadata(Vec<u8>),
    /// Probability data structure, only valid from read_meta_items().
    Bitmap(Vec<u8>),
    /// File-position where the root block for the Btree starts.
    Root(u64),
}

// returns bytes appended to file.
pub(crate) fn write_meta_items(
    file: ffi::OsString,
    items: Vec<MetaItem>, // list of meta items, starting from Marker
) -> Result<u64> {
    let mut fd = {
        let p = path::Path::new(&file);
        let mut opts = fs::OpenOptions::new();
        err_at!(IoError, opts.append(true).open(p))?
    };

    let (mut hdr, mut block) = (vec![], vec![]);
    hdr.resize(40, 0);

    // (fpos, bitmap-len, app-meta-len, stats-len)
    let mut debug_args: (u64, u64, u64, u64) = Default::default();
    for (i, item) in items.into_iter().enumerate() {
        match (i, item) {
            (0, MetaItem::Root(fpos)) => {
                hdr[0..8].copy_from_slice(&fpos.to_be_bytes());
                debug_args.0 = fpos;
            }
            (1, MetaItem::Bitmap(bitmap)) => {
                let ln: u64 = convert_at!(bitmap.len())?;
                hdr[8..16].copy_from_slice(&ln.to_be_bytes());
                block.extend_from_slice(&bitmap);
                debug_args.1 = ln;
            }
            (2, MetaItem::AppMetadata(md)) => {
                let ln: u64 = convert_at!(md.len())?;
                hdr[16..24].copy_from_slice(&ln.to_be_bytes());
                block.extend_from_slice(&md);
                debug_args.2 = ln;
            }
            (3, MetaItem::Stats(s)) => {
                let ln: u64 = convert_at!(s.len())?;
                hdr[24..32].copy_from_slice(&ln.to_be_bytes());
                block.extend_from_slice(s.as_bytes());
                debug_args.3 = ln;
            }
            (4, MetaItem::Marker(data)) => {
                let ln: u64 = convert_at!(data.len())?;
                hdr[32..40].copy_from_slice(&ln.to_be_bytes());
                block.extend_from_slice(&data);
            }
            (i, m) => return err_at!(Fatal, msg: format!("meta-item {},{}", i, m)),
        }
    }
    debug!(
        target: "robt  ",
        "{:?}, writing root:{} bitmap_len:{} meta_len:{}  stats_len:{}",
        file, debug_args.0, debug_args.1, debug_args.2, debug_args.3,
    );
    block.extend_from_slice(&hdr[..]);

    // flush / append into file.
    let n = Config::compute_root_block(block.len());
    let (shift, m) = (n - block.len(), block.len());
    block.resize(n, 0);
    block.copy_within(0..m, shift);
    let n = write_file!(fd, &block, file.clone(), "robt-write_meta_items")?;
    err_at!(IoError, fd.sync_all())?;

    Ok(convert_at!(n)?)
}

/// Read meta items from [Robt] index file.
///
/// Meta-items is stored at the tip of the index file. If successful,
/// a vector of meta items is returned. Along with it, number of
/// meta-block bytes is return. To learn more about the meta items
/// refer to [MetaItem] type.
pub fn read_meta_items(
    dir: &ffi::OsStr, // directory of index, can be os-native string
    name: &str,
) -> Result<(Vec<MetaItem>, usize)> {
    use std::str::from_utf8;

    let index_file = Config::stitch_index_file(dir, name);
    let m = err_at!(IoError, fs::metadata(&index_file))?.len();
    let mut fd = util::open_file_r(index_file.as_ref())?;

    // read header
    let hdr = read_file!(&mut fd, m - 40, 40, "read root-block header")?;
    let root = u64::from_be_bytes(array_at!(hdr[..8])?);
    let n_bmap: usize = convert_at!(u64::from_be_bytes(array_at!(hdr[8..16])?))?;
    let n_md: usize = convert_at!(u64::from_be_bytes(array_at!(hdr[16..24])?))?;
    let n_stats: usize = convert_at!(u64::from_be_bytes(array_at!(hdr[24..32])?))?;
    let n_marker: usize = convert_at!(u64::from_be_bytes(array_at!(hdr[32..40])?))?;
    // read block
    let meta_block_bytes: u64 = {
        let n_total = n_bmap + n_md + n_stats + n_marker + 40;
        convert_at!(Config::compute_root_block(n_total))?
    };
    let block: Vec<u8> = read_file!(
        &mut fd,
        m - meta_block_bytes,
        meta_block_bytes,
        "read root-block"
    )?
    .into_iter()
    .collect();

    let mut meta_items: Vec<MetaItem> = vec![];
    let z = {
        let z: usize = convert_at!(meta_block_bytes)?;
        z - 40
    };

    let (x, y) = (z - n_marker, z);
    let marker = block[x..y].to_vec();
    if marker.ne(&ROOT_MARKER.as_slice()) {
        return err_at!(InvalidFile, msg: format!("marker {:?}", marker));
    }

    let (x, y) = (z - n_marker - n_stats, z - n_marker);
    let stats = err_at!(InvalidInput, from_utf8(&block[x..y]))?.to_string();

    let (x, y) = (z - n_marker - n_stats - n_md, z - n_marker - n_stats);
    let app_data = block[x..y].to_vec();

    let (x, y) = (
        z - n_marker - n_stats - n_md - n_bmap,
        z - n_marker - n_stats - n_md,
    );
    let bitmap = block[x..y].to_vec();

    meta_items.push(MetaItem::Root(root));
    meta_items.push(MetaItem::Bitmap(bitmap));
    meta_items.push(MetaItem::AppMetadata(app_data));
    meta_items.push(MetaItem::Stats(stats.clone()));
    meta_items.push(MetaItem::Marker(marker.clone()));

    // validate and return
    let stats: Stats = stats.parse()?;
    if root == std::u64::MAX {
        Ok((meta_items, convert_at!(meta_block_bytes)?))
    } else {
        let at: u64 = convert_at!(stats.m_blocksize)?;
        let at = m - meta_block_bytes - at;
        if at == root {
            Ok((meta_items, convert_at!(meta_block_bytes)?))
        } else {
            err_at!(InvalidFile, msg: format!("root:{}, found:{}", at, root))
        }
    }
}

impl fmt::Display for MetaItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            MetaItem::Marker(_) => write!(f, "MetaItem::Marker"),
            MetaItem::Stats(_) => write!(f, "MetaItem::Stats"),
            MetaItem::AppMetadata(_) => write!(f, "MetaItem::AppMetadata"),
            MetaItem::Bitmap(_) => write!(f, "MetaItem::Bitmap"),
            MetaItem::Root(_) => write!(f, "MetaItem::Root"),
        }
    }
}

/// Statistic type, for [Robt].
///
/// Note that build-only [configuration][Config] options like:
/// * _`flush_queue_size`_,  configuration option.
///
/// are not persisted as part of statistics.
///
/// Meanwhile, for `vlog_file` configuration option, only file-name is
/// relevant, directory-path shall be ignored.
///
#[derive(Clone, Default, PartialEq)]
pub struct Stats {
    pub name: String,
    /// Part of _build-configuration_, specifies the leaf block size
    /// in btree index.
    pub z_blocksize: usize,
    /// Part of _build-configuration_, specifies the intemediate block
    /// size in btree index.
    pub m_blocksize: usize,
    /// Part of _build-configuration_, specifies block size of value
    /// blocks flushed to _vlog-file_.
    pub v_blocksize: usize,
    /// Part of _build-configuration_, specifies whether delta was
    /// included as part of the entry.
    pub delta_ok: bool,
    /// Part of _build-configuration_, specifies the log file for deltas
    /// and value, if `value_in_vlog` is true. Note that only file-name
    /// is relevant, directory-path shall be ignored.
    pub vlog_file: Option<ffi::OsString>,
    /// Part of _build-configuration_, specifies whether value was
    /// persisted in value log file.
    pub value_in_vlog: bool,
    /// Flush queue size. Default: Config::FLUSH_QUEUE_SIZE
    pub flush_queue_size: usize,

    /// Number of entries indexed.
    pub n_count: u64,
    /// Number of entries that are marked as deleted.
    pub n_deleted: usize,
    /// Sequence number for the latest entry.
    pub seqno: u64,
    /// Total disk footprint for all keys.
    pub key_mem: usize,
    /// Total disk footprint for all deltas.
    pub diff_mem: usize,
    /// Total disk footprint for all values.
    pub val_mem: usize,
    /// Total disk footprint for all leaf-nodes.
    pub z_bytes: usize,
    /// Total disk footprint for all intermediate-nodes.
    pub m_bytes: usize,
    /// Total disk footprint for values and deltas.
    pub v_bytes: usize,
    /// Total disk size wasted in padding leaf-nodes and intermediate-nodes.
    pub padding: usize,
    /// Older size of value-log file, applicable only in compact build.
    pub n_abytes: usize,
    /// Size of serialized bitmap bytes.
    pub mem_bitmap: usize,
    /// Number of entries in bitmap.
    pub n_bitmap: usize,

    /// Time take to build this btree.
    pub build_time: u64,
    /// Timestamp for this index.
    pub epoch: i128,
}

impl Stats {
    pub fn merge(self, other: Stats) -> Stats {
        Stats {
            name: other.name.clone(),
            z_blocksize: other.z_blocksize,
            m_blocksize: other.m_blocksize,
            v_blocksize: other.v_blocksize,
            delta_ok: other.delta_ok,
            vlog_file: None,
            value_in_vlog: other.value_in_vlog,
            flush_queue_size: other.flush_queue_size,

            n_count: self.n_count + other.n_count,
            n_deleted: self.n_deleted + other.n_deleted,
            seqno: cmp::max(self.seqno, other.seqno),
            key_mem: self.key_mem + other.key_mem,
            diff_mem: self.diff_mem + other.diff_mem,
            val_mem: self.val_mem + other.val_mem,
            z_bytes: self.z_bytes + other.z_bytes,
            m_bytes: self.m_bytes + other.m_bytes,
            v_bytes: self.v_bytes + other.v_bytes,
            padding: self.padding + other.padding,
            n_abytes: self.n_abytes + other.n_abytes,
            mem_bitmap: self.mem_bitmap + other.mem_bitmap,
            n_bitmap: self.n_bitmap + other.n_bitmap,

            build_time: other.build_time,
            epoch: other.epoch,
        }
    }
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "robt.name = {}\n", self.name)?;
        write!(
            f,
            "robt.stats = {{ seqno={}, n_count={}, n_deleted={} }}\n",
            self.seqno, self.n_count, self.n_deleted,
        )?;
        write!(
            f,
            "robt.stats = {{ key_mem={}, val_mem={}, diff_mem={} }}\n",
            self.key_mem, self.val_mem, self.diff_mem,
        )?;
        write!(
            f,
            "robt.stats = {{ z_bytes={}, m_bytes={}, v_bytes={} }}\n",
            self.z_bytes, self.m_bytes, self.v_bytes,
        )?;
        write!(
            f,
            "robt.stats = {{ mem_bitmap={}, n_bitmap={}, }}\n",
            self.mem_bitmap, self.n_bitmap,
        )?;
        let bt = time::Duration::from_nanos(self.build_time);
        write!(
            f,
            "robt.stats = {{ padding={}, n_abytes={}, took=\"{:?}\" }}",
            self.padding, self.n_abytes, bt
        )
    }
}

impl ToJson for Stats {
    fn to_json(&self) -> String {
        let vlog_file = match &self.vlog_file {
            Some(vlog_file) => format!("{:?}", vlog_file),
            None => "null".to_string(),
        };
        let props = [
            format!(r#""name": "{}""#, self.name),
            format!(r#""z_blocksize": {}"#, self.z_blocksize),
            format!(r#""m_blocksize": {}"#, self.m_blocksize),
            format!(r#""v_blocksize": {}"#, self.v_blocksize),
            format!(r#""delta_ok": {}"#, self.delta_ok),
            format!(r#""vlog_file": {}"#, vlog_file),
            format!(r#""value_in_vlog": {}"#, self.value_in_vlog),
            format!(r#""flush_queue_size": {}"#, self.flush_queue_size),
            format!(r#""seqno": {}"#, self.seqno),
            format!(r#""n_count": {}"#, self.n_count),
            format!(r#""n_deleted": {}"#, self.n_deleted),
            format!(r#""key_mem": {}"#, self.key_mem),
            format!(r#""val_mem": {}"#, self.val_mem),
            format!(r#""diff_mem": {}"#, self.diff_mem),
            format!(r#""z_bytes": {}"#, self.z_bytes),
            format!(r#""m_bytes": {}"#, self.m_bytes),
            format!(r#""v_bytes": {}"#, self.v_bytes),
            format!(r#""mem_bitmap": {}"#, self.mem_bitmap),
            format!(r#""n_bitmap": {}"#, self.n_bitmap),
            format!(r#""padding": {}"#, self.padding),
            format!(r#""n_abytes": {}"#, self.n_abytes),
            format!(r#""build_time": {}"#, self.build_time),
            format!(r#""epoch": {}"#, self.epoch),
        ];
        format!(r#"{{ {} }}"#, props.join(", "))
    }
}

impl From<Config> for Stats {
    fn from(config: Config) -> Stats {
        Stats {
            name: config.name,
            z_blocksize: config.z_blocksize,
            m_blocksize: config.m_blocksize,
            v_blocksize: config.v_blocksize,
            delta_ok: config.delta_ok,
            vlog_file: config.vlog_file,
            value_in_vlog: config.value_in_vlog,
            flush_queue_size: config.flush_queue_size,

            n_count: Default::default(),
            n_deleted: Default::default(),
            seqno: Default::default(),
            key_mem: Default::default(),
            diff_mem: Default::default(),
            val_mem: Default::default(),
            z_bytes: Default::default(),
            v_bytes: Default::default(),
            m_bytes: Default::default(),
            mem_bitmap: Default::default(),
            n_bitmap: Default::default(),
            padding: Default::default(),
            n_abytes: Default::default(),

            build_time: Default::default(),
            epoch: Default::default(),
        }
    }
}

impl FromStr for Stats {
    type Err = Error;

    fn from_str(s: &str) -> Result<Stats> {
        use jsondata::Json;

        let js: Json = err_at!(InvalidInput, s.parse())?;

        let to_usize = |key: &str| -> Result<usize> {
            match err_at!(InvalidInput, js.get(key))?.to_integer() {
                Some(n) => convert_at!(n),
                None => err_at!(InvalidInput, msg: format!("key:{}", key)),
            }
        };
        let to_u64 = |key: &str| -> Result<u64> {
            match err_at!(InvalidInput, js.get(key))?.to_integer() {
                Some(n) => convert_at!(n),
                None => err_at!(InvalidInput, msg: format!("key:{}", key)),
            }
        };
        let to_i128 = |key: &str| -> Result<i128> {
            match err_at!(InvalidInput, js.get(key))?.to_integer() {
                Some(n) => convert_at!(n),
                None => err_at!(InvalidInput, msg: format!("key:{}", key)),
            }
        };
        let to_bool = |key: &str| -> Result<bool> {
            match err_at!(InvalidInput, js.get(key))?.to_bool() {
                Some(val) => Ok(val),
                None => err_at!(InvalidInput, msg: format!("key:{}", key)),
            }
        };
        let to_string = |key: &str| -> Result<String> {
            match err_at!(InvalidInput, js.get(key))?.as_str() {
                Some(val) => Ok(val.to_string()),
                None => err_at!(InvalidInput, msg: format!("key:{}", key)),
            }
        };
        let vlog_file = {
            match err_at!(InvalidInput, js.get("/vlog_file"))?.as_str() {
                Some(s) if s.len() == 0 => None,
                None => None,
                Some(s) => {
                    let vlog_file: ffi::OsString = s.to_string().into();
                    Some(vlog_file)
                }
            }
        };

        Ok(Stats {
            name: to_string("/name")?,
            // config fields.
            z_blocksize: to_usize("/z_blocksize")?,
            m_blocksize: to_usize("/m_blocksize")?,
            v_blocksize: to_usize("/v_blocksize")?,
            delta_ok: to_bool("/delta_ok")?,
            vlog_file: vlog_file,
            value_in_vlog: to_bool("/value_in_vlog")?,
            flush_queue_size: to_usize("/flush_queue_size")?,
            // statitics fields.
            n_count: to_u64("/n_count")?,
            n_deleted: to_usize("/n_deleted")?,
            seqno: to_u64("/seqno")?,
            key_mem: to_usize("/key_mem")?,
            diff_mem: to_usize("/diff_mem")?,
            val_mem: to_usize("/val_mem")?,
            z_bytes: to_usize("/z_bytes")?,
            v_bytes: to_usize("/v_bytes")?,
            m_bytes: to_usize("/m_bytes")?,
            mem_bitmap: to_usize("/mem_bitmap")?,
            n_bitmap: to_usize("/n_bitmap")?,
            padding: to_usize("/padding")?,
            n_abytes: to_usize("/n_abytes")?,

            build_time: to_u64("/build_time")?,
            epoch: to_i128("/epoch")?,
        })
    }
}

/// Builder type, for constructing Read-Only-BTree index from an iterator.
///
/// Index can be built in [initial][Builder::initial] mode or
/// [incremental][Builder::incremental] mode. Refer to corresponding methods
/// for more information.
pub struct Builder<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    config: Config,
    iflusher: Option<rt::Thread<Vec<u8>, (), (ffi::OsString, u64)>>,
    vflusher: Option<rt::Thread<Vec<u8>, (), (ffi::OsString, u64)>>,
    stats: Stats,

    _phantom_key: marker::PhantomData<K>,
    _phantom_val: marker::PhantomData<V>,
    _phantom_bitmap: marker::PhantomData<B>,
}

impl<K, V, B> Builder<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    B: Bloom,
{
    /// For commit builds, index file and value-log-file, if configured,
    /// shall be created new.
    pub fn initial(
        dir: &ffi::OsStr, // directory path where index file(s) are stored
        name: &str,
        mut config: Config, //  TODO: Bit of ugliness here
    ) -> Result<Builder<K, V, B>> {
        let create = true;

        let iflusher = {
            let ifile = Config::stitch_index_file(dir, name);
            rt::Thread::new_sync(
                format!("robt-index-flusher-{}", name),
                move |rx| move || thread_flush(ifile, create, rx),
                config.flush_queue_size,
            )
        };

        let is_vlog = config.delta_ok || config.value_in_vlog;
        config.vlog_file = match &config.vlog_file {
            Some(vlog_file) if is_vlog => Some(vlog_file.clone()),
            None if is_vlog => Some(Config::stitch_vlog_file(dir, name)),
            _ => None,
        };

        let vflusher = match &config.vlog_file {
            Some(vfile) => {
                let vfile = vfile.clone();
                Some(rt::Thread::new_sync(
                    format!("robt-vlog-flusher-{}", name),
                    move |rx| move || thread_flush(vfile, create, rx),
                    config.flush_queue_size,
                ))
            }
            None => None,
        };

        Ok(Builder {
            config: config.clone(),
            iflusher: Some(iflusher),
            vflusher,
            stats: From::from(config),

            _phantom_key: marker::PhantomData,
            _phantom_val: marker::PhantomData,
            _phantom_bitmap: marker::PhantomData,
        })
    }

    /// For compact build, index file is created new, while
    /// value-log-file, if configured, shall be appended to older version.
    pub fn incremental(
        dir: &ffi::OsStr, // directory path where index files are stored
        name: &str,
        mut config: Config,
    ) -> Result<Builder<K, V, B>> {
        let iflusher = {
            let ifile = Config::stitch_index_file(dir, name);
            rt::Thread::new_sync(
                format!("robt-index-flusher-{}", name),
                move |rx| move || thread_flush(ifile, true /*create*/, rx),
                config.flush_queue_size,
            )
        };

        let is_vlog = config.delta_ok || config.value_in_vlog;
        config.vlog_file = match &config.vlog_file {
            Some(vlog_file) if is_vlog => Some(vlog_file.clone()),
            None if is_vlog => Some(Config::stitch_vlog_file(dir, name)),
            _ => None,
        };

        let create = false;

        let (vflusher, vf_fpos): (_, usize) = match &config.vlog_file {
            Some(vfile) => {
                let vfile = vfile.clone();
                let vf_fpos = err_at!(IoError, fs::metadata(&vfile))?.len();

                let t = rt::Thread::new_sync(
                    format!("robt-vlog-flusher-{}", name),
                    move |rx| move || thread_flush(vfile, create, rx),
                    config.flush_queue_size,
                );

                (Some(t), convert_at!(vf_fpos)?)
            }
            None => (None, Default::default()),
        };

        let mut stats: Stats = From::from(config.clone());
        stats.n_abytes += vf_fpos;

        Ok(Builder {
            config: config.clone(),
            iflusher: Some(iflusher),
            vflusher,
            stats,

            _phantom_key: marker::PhantomData,
            _phantom_val: marker::PhantomData,
            _phantom_bitmap: marker::PhantomData,
        })
    }

    /// Build a new index from the supplied iterator. The iterator shall
    /// return an index entry for each iteration, and the entries are
    /// expected in sort order.
    pub fn build<I>(mut self, iter: I, app_meta: Vec<u8>) -> Result<usize>
    where
        K: Hash,
        I: Iterator<Item = Result<Entry<K, V>>>,
    {
        let (root, bitmap): (u64, B) = {
            let mut bscanner = {
                let seqno: u64 = Default::default();
                BuildScan::new(scans::BitmappedScan::new(iter), seqno)
            };
            let root = self.build_tree(&mut bscanner)?;
            let (_, bitmap) = bscanner.update_stats(&mut self.stats)?.close()?;
            (root, bitmap)
        };

        self.build_finish(app_meta, bitmap, root)
    }

    /// Start building the index, this API should be used along with
    /// [build_finish][Builder::build_finish] to have more fine grained
    /// control, compared to [build][Builder::build], over the index build
    /// process.
    pub fn build_start<I>(mut self, iter: I) -> Result<u64>
    where
        I: Iterator<Item = Result<Entry<K, V>>>,
    {
        let mut build_scanner = {
            let seqno: u64 = Default::default();
            BuildScan::new(iter, seqno)
        };
        let root = self.build_tree(&mut build_scanner)?;
        build_scanner.update_stats(&mut self.stats)?;
        Ok(root)
    }

    /// Completes the build process, refer to
    /// [build_start][Builder::build_start] for details.
    pub fn build_finish(mut self, app_meta: Vec<u8>, bitmap: B, root: u64) -> Result<usize> {
        let (n_bitmap, bitmap) = (bitmap.len()?, bitmap.to_vec());
        let stats: String = {
            self.stats.n_bitmap = n_bitmap;
            self.stats.mem_bitmap = bitmap.len();
            self.stats.to_json()
        };

        // start building metadata items for index files
        let meta_items: Vec<MetaItem> = vec![
            MetaItem::Root(root),
            MetaItem::Bitmap(bitmap),
            MetaItem::AppMetadata(app_meta),
            MetaItem::Stats(stats),
            MetaItem::Marker(ROOT_MARKER.clone()), // tip of the index.
        ];

        // flush blocks and close
        let (index_file, _) = match self.iflusher.take() {
            Some(iflusher) => iflusher.close_wait()?,
            None => err_at!(Fatal, msg: format!("unreachable"))?,
        };
        match self.vflusher.take() {
            Some(vflusher) => {
                vflusher.close_wait()?;
            }
            None => (),
        };
        // flush meta items to disk and close
        let meta_block_bytes = write_meta_items(index_file, meta_items)?;
        Ok(convert_at!(meta_block_bytes)?)
    }

    // return root, iter
    fn build_tree(&mut self, iter: &mut dyn Iterator<Item = Result<Entry<K, V>>>) -> Result<u64> {
        struct Context<K, V>
        where
            K: Clone + Ord + Serialize,
            V: Clone + Diff + Serialize,
            <V as Diff>::D: Serialize,
        {
            fpos: u64,
            zfpos: u64,
            vfpos: u64,
            z: ZBlock<K, V>,
            ms: Vec<MBlock<K, V>>,
        };
        let mut c = {
            let vfpos: u64 = convert_at!(self.stats.n_abytes)?;
            Context {
                fpos: 0,
                zfpos: 0,
                vfpos,
                z: ZBlock::new_encode(vfpos, self.config.clone()),
                ms: vec![MBlock::new_encode(self.config.clone())],
            }
        };

        for entry in iter {
            let entry = entry?;
            // println!("build key: {:?}", entry.to_key());
            // println!("build entry: {}", entry.to_seqno());
            match c.z.insert(&entry, &mut self.stats) {
                Ok(_) => (),
                Err(Error::__ZBlockOverflow(_)) => {
                    // zbytes is z_blocksize
                    let (zbytes, vbytes) = c.z.finalize(&mut self.stats)?;
                    c.z.flush(self.iflusher.as_ref(), self.vflusher.as_ref())?;
                    c.fpos += zbytes;
                    c.vfpos += vbytes;

                    let mut m = c.ms.pop().unwrap();
                    match m.insertz(c.z.as_first_key()?, c.zfpos) {
                        Ok(_) => c.ms.push(m),
                        Err(Error::__MBlockOverflow(_)) => {
                            // x is m_blocksize
                            let x = m.finalize(&mut self.stats)?;
                            m.flush(self.iflusher.as_ref())?;
                            let k = m.as_first_key()?;
                            let r = self.insertms(c.ms, c.fpos + x, k, c.fpos)?;
                            c.ms = r.0;
                            c.fpos = r.1;

                            m.reset()?;
                            m.insertz(c.z.as_first_key()?, c.zfpos)?;
                            c.ms.push(m)
                        }
                        Err(err) => return Err(err),
                    }

                    c.zfpos = c.fpos;
                    c.z.reset(c.vfpos)?;

                    c.z.insert(&entry, &mut self.stats)?;
                }
                Err(err) => return Err(err),
            };
        }

        if c.z.has_first_key()? == false && c.fpos == 0 {
            // empty iterator.
            return Ok(std::u64::MAX);
        }

        // println!(" number of mblocks: {}", c.ms.len());

        // flush final z-block
        if c.z.has_first_key()? {
            // println!(" flush final zblock: {:?}", c.z.as_first_key());
            let (zbytes, vbytes) = c.z.finalize(&mut self.stats)?;
            c.z.flush(self.iflusher.as_ref(), self.vflusher.as_ref())?;
            c.fpos += zbytes;
            c.vfpos += vbytes;

            let mut m = c.ms.pop().unwrap();
            match m.insertz(c.z.as_first_key()?, c.zfpos) {
                Ok(_) => c.ms.push(m),
                Err(Error::__MBlockOverflow(_)) => {
                    let x = m.finalize(&mut self.stats)?;
                    m.flush(self.iflusher.as_ref())?;
                    let mkey = m.as_first_key()?;
                    let res = self.insertms(c.ms, c.fpos + x, mkey, c.fpos)?;
                    c.ms = res.0;
                    c.fpos = res.1;

                    m.reset()?;
                    m.insertz(c.z.as_first_key()?, c.zfpos)?;
                    c.ms.push(m);
                }
                Err(err) => return Err(err),
            }
        } else {
            err_at!(Fatal, msg: format!("unreachable"))?
        }

        // flush final set of m-blocks
        while let Some(mut m) = c.ms.pop() {
            let is_root = m.has_first_key()? && c.ms.len() == 0;
            if is_root {
                let x = m.finalize(&mut self.stats)?;
                m.flush(self.iflusher.as_ref())?;
                c.fpos += x;
            } else if m.has_first_key()? {
                // x is m_blocksize
                let x = m.finalize(&mut self.stats)?;
                m.flush(self.iflusher.as_ref())?;
                let mkey = m.as_first_key()?;
                let res = self.insertms(c.ms, c.fpos + x, mkey, c.fpos)?;
                c.ms = res.0;
                c.fpos = res.1
            }
        }
        let n: u64 = convert_at!(self.config.m_blocksize)?;
        Ok(c.fpos - n)
    }

    fn insertms(
        &mut self,
        mut ms: Vec<MBlock<K, V>>,
        mut fpos: u64,
        key: &K,
        mfpos: u64,
    ) -> Result<(Vec<MBlock<K, V>>, u64)> {
        // println!("insertms key:{:?} {}", key, mfpos);
        let m0 = ms.pop();
        let m0 = match m0 {
            None => {
                // println!("new mblock for {:?} {}", key, mfpos);
                let mut m0 = MBlock::new_encode(self.config.clone());
                m0.insertm(key, mfpos)?;
                m0
            }
            Some(mut m0) => match m0.insertm(key, mfpos) {
                Ok(_) => m0,
                Err(Error::__MBlockOverflow(_)) => {
                    // println!("overflow for {:?} {}", key, mfpos);
                    // x is m_blocksize
                    let x = m0.finalize(&mut self.stats)?;
                    m0.flush(self.iflusher.as_ref())?;
                    let mkey = m0.as_first_key()?;
                    let res = self.insertms(ms, fpos + x, mkey, fpos)?;
                    ms = res.0;
                    fpos = res.1;

                    m0.reset()?;
                    m0.insertm(key, mfpos)?;
                    m0
                }
                Err(err) => return Err(err),
            },
        };
        ms.push(m0);
        Ok((ms, fpos))
    }
}

struct BuildScan<K, V, I>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    iter: I,

    start: time::SystemTime,
    seqno: u64,
    n_count: u64,
    n_deleted: usize,
}

impl<K, V, I> BuildScan<K, V, I>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    fn new(iter: I, seqno: u64) -> BuildScan<K, V, I> {
        BuildScan {
            iter,

            start: time::SystemTime::now(),
            seqno,
            n_count: Default::default(),
            n_deleted: Default::default(),
        }
    }

    fn update_stats(self, stats: &mut Stats) -> Result<I> {
        stats.build_time = {
            let elapsed = err_at!(TimeFail, self.start.elapsed())?;
            convert_at!(elapsed.as_nanos())?
        };
        stats.seqno = self.seqno;
        stats.n_count = self.n_count;
        stats.n_deleted = self.n_deleted;
        stats.epoch = {
            let elapsed = err_at!(TimeFail, time::UNIX_EPOCH.elapsed())?;
            convert_at!(elapsed.as_nanos())?
        };
        Ok(self.iter)
    }
}

impl<K, V, I> Iterator for BuildScan<K, V, I>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    type Item = Result<Entry<K, V>>;

    #[inline]
    fn next(&mut self) -> Option<Result<Entry<K, V>>> {
        match self.iter.next() {
            Some(Ok(entry)) => {
                self.seqno = cmp::max(self.seqno, entry.to_seqno());
                self.n_count += 1;
                if entry.is_deleted() {
                    self.n_deleted += 1;
                }
                Some(Ok(entry))
            }
            Some(Err(err)) => Some(Err(err)),
            None => None,
        }
    }
}

struct CommitScan<'a, K, V, I, B>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    x_iter: I,                      // new iterator
    y_iter: Box<Iter<'a, K, V, B>>, // old iterator
    x_entry: Option<Result<Entry<K, V>>>,
    y_entry: Option<Result<Entry<K, V>>>,
}

impl<'a, K, V, I, B> CommitScan<'a, K, V, I, B>
where
    K: Default + Clone + Ord + Serialize + Footprint,
    V: Default + Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Default + Serialize,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    fn new(mut x_iter: I, mut y_iter: Box<Iter<'a, K, V, B>>) -> CommitScan<'a, K, V, I, B> {
        let x_entry = x_iter.next();
        let y_entry = y_iter.next();
        CommitScan {
            x_iter,
            y_iter,
            x_entry,
            y_entry,
        }
    }

    fn close(self) -> Result<(I, Box<Iter<'a, K, V, B>>)> {
        Ok((self.x_iter, self.y_iter))
    }
}

impl<'a, K, V, I, B> Iterator for CommitScan<'a, K, V, I, B>
where
    K: Default + Clone + Ord + Serialize + Footprint,
    V: Default + Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Default + Serialize,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.x_entry.take(), self.y_entry.take()) {
            (Some(Ok(xe)), Some(Ok(mut ye))) => match xe.as_key().cmp(ye.as_key()) {
                cmp::Ordering::Less => {
                    self.x_entry = self.x_iter.next();
                    self.y_entry = Some(Ok(ye));
                    // println!("commitscan Less {}", xe.to_seqno());
                    Some(Ok(xe))
                }
                cmp::Ordering::Greater => {
                    self.y_entry = self.y_iter.next();
                    self.x_entry = Some(Ok(xe));
                    // println!("commitscan Greater {}", ye.to_seqno());
                    Some(Ok(ye))
                }
                cmp::Ordering::Equal => {
                    self.x_entry = self.x_iter.next();
                    self.y_entry = self.y_iter.next();
                    // println!("commitscan Equal {} {}", xe.to_seqno(), ye.to_seqno(),);
                    // fetch the value from old snapshot, only value.
                    match self.y_iter.snap.fetch(&mut ye, false, false) {
                        Ok(()) => Some(xe.xmerge(ye)),
                        Err(err) => Some(Err(err)),
                    }
                }
            },
            (Some(Ok(xe)), None) => {
                self.x_entry = self.x_iter.next();
                Some(Ok(xe))
            }
            (None, Some(Ok(ye))) => {
                self.y_entry = self.y_iter.next();
                Some(Ok(ye))
            }
            (Some(Ok(_xe)), Some(Err(err))) => Some(Err(err)),
            (Some(Err(err)), Some(Ok(_ye))) => Some(Err(err)),
            _ => None,
        }
    }
}

fn thread_flush(
    file: ffi::OsString, // for debuging purpose
    create: bool,        // if true create a new file
    rx: rt::Rx<Vec<u8>, ()>,
) -> Result<(ffi::OsString, u64)> {
    let (mut fd, fpos) = if create {
        (util::create_file_a(file.clone())?, Default::default())
    } else {
        (
            util::open_file_w(&file)?,
            err_at!(IoError, fs::metadata(&file))?.len(),
        )
    };

    err_at!(IoError, fd.lock_shared())?; // <---- read lock

    for (data, _) in rx {
        // println!("flusher {:?} {} {}", file, fpos, data.len());
        // fpos += data.len();
        let n = write_file!(fd, &data, file.clone(), "robt-thread-flush")?;
        if n != data.len() {
            err_at!(IoError, fd.unlock())?; // <----- read un-lock
        }
    }

    err_at!(IoError, fd.sync_all())?;

    // file descriptor and receiver channel shall be dropped.
    err_at!(IoError, fd.unlock())?; // <----- read un-lock
    Ok((file, fpos))
}

// enumerated index file, that can use file-access or mmap-access based
// on configured variant.
enum IndexFile {
    Block {
        fd: fs::File,
        file: ffi::OsString,
    },
    Mmap {
        fd: fs::File,
        mmap: memmap::Mmap,
        file: ffi::OsString,
    },
}

impl IndexFile {
    // always created for file access.
    fn new_block(file: ffi::OsString) -> Result<IndexFile> {
        Ok(IndexFile::Block {
            fd: util::open_file_r(&file)?,
            file,
        })
    }

    // and later on converted to mmap access, if configured,
    unsafe fn set_mmap(&mut self, ok: bool) -> Result<()> {
        match self {
            IndexFile::Block { file, .. } if ok => {
                let file = file.clone();
                let fd = util::open_file_r(&file)?;
                match memmap::Mmap::map(&fd) {
                    Ok(mmap) => {
                        *self = IndexFile::Mmap { fd, file, mmap };
                        Ok(())
                    }
                    Err(err) => {
                        let msg = format!("file:{:?} err:{}", file, err);
                        err_at!(SystemFail, msg: msg)
                    }
                }
            }
            IndexFile::Mmap { file, .. } if !ok => {
                let file = file.clone();
                let fd = util::open_file_r(&file)?;
                *self = IndexFile::Block { file, fd };
                Ok(())
            }
            IndexFile::Block { .. } => Ok(()),
            IndexFile::Mmap { .. } => Ok(()),
        }
    }

    fn read_buffer(&mut self, fpos: u64, n: usize, msg: &str) -> Result<Vec<u8>> {
        Ok(match self {
            IndexFile::Block { fd, .. } => {
                let n: u64 = convert_at!(n)?;
                read_file!(fd, fpos, n, msg)?
            }
            IndexFile::Mmap { mmap, .. } => {
                let start: usize = convert_at!(fpos)?;
                mmap[start..(start + n)].to_vec()
            }
        })
    }

    fn to_file(&self) -> ffi::OsString {
        match self {
            IndexFile::Block { file, .. } => file.clone(),
            IndexFile::Mmap { file, .. } => file.clone(),
        }
    }

    fn as_fd(&self) -> &fs::File {
        match self {
            IndexFile::Block { fd, .. } => fd,
            IndexFile::Mmap { fd, .. } => fd,
        }
    }

    fn footprint(&self) -> Result<isize> {
        let file = match self {
            IndexFile::Block { file, .. } => file,
            IndexFile::Mmap { file, .. } => file,
        };
        Ok(convert_at!(err_at!(IoError, fs::metadata(file))?.len())?)
    }
}

/// Read handle into [Robt] indexes.
///
/// Every open snapshot will hold an open file-descriptors, and two
/// open file-descritors when configured with value-log file.
pub struct Snapshot<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    pub(crate) dir: ffi::OsString,
    pub(crate) name: String,
    meta: Vec<MetaItem>,
    config: Config,
    bitmap: Arc<B>,

    // working fields
    index_fd: IndexFile,
    valog_fd: Option<(ffi::OsString, fs::File)>,

    _phantom_key: marker::PhantomData<K>,
    _phantom_val: marker::PhantomData<V>,
}

impl<K, V, B> Drop for Snapshot<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    fn drop(&mut self) {
        match self.index_fd.as_fd().unlock() {
            Ok(_) => (),
            Err(err) => error!(
                target: "robtr ", "{}, unlock index file {}", self.name, err
            ),
        }
        if let Some((_, fd)) = &self.valog_fd {
            match fd.unlock() {
                Ok(_) => (),
                Err(err) => error!(
                    target: "robtr ", "{}, unlock vlog file {}", self.name, err
                ),
            }
        }

        debug!(target: "robtr ", "{:?}/{}, snapshot dropped", self.dir, self.name);
    }
}

// Construction methods.
impl<K, V, B> Snapshot<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
    B: Bloom,
{
    /// Open BTree snapshot from file that can be constructed from ``dir``
    /// and ``name``.
    pub fn open(
        dir: &ffi::OsStr,
        name: &str, // index file name.
    ) -> Result<Snapshot<K, V, B>> {
        // println!("Snapshot.open() {:?} {}", dir, name);
        let (mut meta_items, _) = read_meta_items(dir, name)?;
        let stats: Stats = if let MetaItem::Stats(stats) = &meta_items[3] {
            Ok(stats.parse()?)
        } else {
            err_at!(InvalidFile, msg: format!("{:?}/{}", dir, name))
        }?;
        let bitmap: Arc<B> = if let MetaItem::Bitmap(data) = &mut meta_items[1] {
            let bitmap = <B as Bloom>::from_vec(&data)?;
            data.drain(..);
            Ok(Arc::new(bitmap))
        } else {
            err_at!(InvalidFile, msg: format!("{:?}/{}", dir, name))
        }?;

        let config: Config = stats.into();

        // open index file.
        let index_fd = IndexFile::new_block(Config::stitch_index_file(dir, name))?;
        err_at!(IoError, index_fd.as_fd().lock_shared())?;
        // open optional value log file.
        let valog_fd = match config.vlog_file {
            Some(vfile) => {
                // stem the file name.
                let mut vpath = path::PathBuf::new();
                vpath.push(path::Path::new(dir));
                vpath.push(match path::Path::new(&vfile).file_name() {
                    Some(vfile) => Ok(vfile),
                    None => err_at!(InvalidFile, msg: format!("{:?}", vfile)),
                }?);
                let vlog_file = vpath.as_os_str().to_os_string();
                let fd = util::open_file_r(&vlog_file)?;
                err_at!(IoError, fd.lock_shared())?;
                Some((vlog_file, fd))
            }
            None => None,
        };

        let mut snap = Snapshot {
            dir: dir.to_os_string(),
            name: name.to_string(),
            meta: meta_items,
            config: Default::default(),
            bitmap,

            index_fd,
            valog_fd,

            _phantom_key: marker::PhantomData,
            _phantom_val: marker::PhantomData,
        };
        snap.config = snap.to_stats()?.into();

        debug!(target: "robtr ", "{:?}/{}, open snapshot", snap.dir, snap.name);

        Ok(snap) // Okey dockey
    }

    pub fn set_mmap(&mut self, ok: bool) -> Result<()> {
        unsafe { self.index_fd.set_mmap(ok) }
    }

    pub fn set_bitmap(&mut self, bitmap: Arc<B>) {
        if cfg!(debug_assertions) {
            assert_eq!(bitmap.to_vec().len(), self.bitmap.to_vec().len());
            assert_eq!(bitmap.to_vec(), self.bitmap.to_vec());
        };

        self.bitmap = bitmap;
    }

    pub fn is_snapshot(file_name: &ffi::OsStr) -> bool {
        let file_name = file_name.to_os_string();
        let name: Result<Name> = TryFrom::try_from(IndexFileName(file_name));
        name.is_ok()
    }

    pub fn purge(self) -> Result<()> {
        let index_file = self.index_fd.to_file();
        let vlog_file = self.valog_fd.as_ref().map(|x| x.0.clone());
        let (dir, name) = (self.dir.clone(), self.name.clone());

        mem::drop(self); // IMPORTANT: Close this snapshot first.

        match purge_file(index_file.clone(), &mut vec![], &mut vec![]) {
            "ok" => Ok(()),
            "locked" => err_at!(InvalidFile, msg: format!("{:?} locked", index_file)),
            "error" => err_at!(Fatal, msg: format!("error unlocking {:?}", index_file)),
            _ => err_at!(Fatal, msg: format!("unreachable")),
        }?;

        let res = if let Some(vlog_file) = vlog_file {
            match purge_file(vlog_file.clone(), &mut vec![], &mut vec![]) {
                "ok" => Ok(()),
                "locked" => err_at!(InvalidFile, msg: format!("{:?} locked", vlog_file)),
                "error" => err_at!(Fatal, msg: format!("error unlocking {:?}", vlog_file)),
                _ => err_at!(Fatal, msg: format!("unreachable")),
            }
        } else {
            Ok(())
        };

        debug!(target: "robtr ", "{:?}/{}, snapshot purged", dir, name);

        res
    }

    pub fn log(&self) -> Result<()> {
        info!(
            target: "robtr ",
            "{:?}/{}, opening snapshot config ...\n{}",
            self.dir, self.name, self.config
        );
        for item in self.meta.iter().enumerate() {
            match item {
                (0, MetaItem::Root(fpos)) => info!(
                    target: "robt  ", "{}, meta-item root at {}",
                    self.name, fpos
                ),
                (1, MetaItem::Bitmap(_)) => info!(
                    target: "robt  ", "{}, meta-item bit-map", self.name
                ),
                (2, MetaItem::AppMetadata(data)) => info!(
                    target: "robt  ", "{}, meta-item app-meta-data {} bytes",
                    self.name, data.len()
                ),
                (3, MetaItem::Stats(_)) => info!(
                    target: "robt  ", "{}, meta-item stats\n{}",
                    self.name, self.to_stats()?
                ),
                (4, MetaItem::Marker(data)) => info!(
                    target: "robt  ", "{}, meta-item marker {} bytes",
                    self.name, data.len()
                ),
                _ => err_at!(Fatal, msg: format!("unreachable"))?,
            }
        }
        Ok(())
    }
}

// maintanence methods.
impl<K, V, B> Snapshot<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    /// Return number of entries in the snapshot.
    pub fn len(&self) -> Result<usize> {
        Ok(convert_at!(self.to_stats()?.n_count)?)
    }

    /// Return the last seqno found in this snapshot.
    pub fn to_seqno(&self) -> Result<u64> {
        Ok(self.to_stats()?.seqno)
    }

    /// Return the file-position for Btree's root node.
    pub fn to_root(&self) -> Result<u64> {
        if let MetaItem::Root(root) = self.meta[0] {
            if root == std::u64::MAX {
                Err(Error::EmptyIndex)
            } else {
                Ok(root)
            }
        } else {
            err_at!(Fatal, msg: format!("{}", self.meta[0]))
        }
    }

    /// Return the application metadata.
    pub fn to_app_meta(&self) -> Result<Vec<u8>> {
        if let MetaItem::AppMetadata(data) = &self.meta[2] {
            Ok(data.clone())
        } else {
            err_at!(Fatal, msg: format!("{}", self.meta[2]))
        }
    }

    /// Return Btree statistics.
    pub fn to_stats(&self) -> Result<Stats> {
        if let MetaItem::Stats(stats) = &self.meta[3] {
            Ok(stats.parse()?)
        } else {
            err_at!(Fatal, msg: format!("{}", self.meta[3]))
        }
    }

    pub fn to_vlog_path_file(&self) -> Result<Option<String>> {
        let stats: Stats = match &self.meta[3] {
            MetaItem::Stats(stats) => stats.parse()?,
            _ => err_at!(Fatal, msg: format!("unreachable"))?,
        };
        match stats.vlog_file {
            Some(vlog_file) => match path::Path::new(&vlog_file).file_name() {
                Some(vf) => match vf.to_str() {
                    Some(vf) => Ok(Some(vf.to_string())),
                    None => Ok(None),
                },
                None => Ok(None),
            },
            None => Ok(None),
        }
    }
}

impl<K, V, B> Snapshot<K, V, B>
where
    K: Default + Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    pub(crate) fn into_scan(mut self) -> Result<Scan<K, V, B>> {
        let mut mzs = vec![];
        match self.to_root() {
            Ok(root) => Ok(self.build_fwd(root, &mut mzs)?),
            Err(Error::EmptyIndex) => Ok(()),
            Err(err) => Err(err),
        }?;
        Ok(Scan::new(self, mzs))
    }

    pub(crate) fn into_range_scan<R>(mut self, range: R) -> Result<ScanRange<K, V, B, R>>
    where
        R: RangeBounds<K>,
    {
        let mut mzs = vec![];
        let skip_one = match range.start_bound() {
            Bound::Unbounded => {
                match self.to_root() {
                    Ok(root) => Ok(self.build_fwd(root, &mut mzs)?),
                    Err(Error::EmptyIndex) => Ok(()),
                    Err(err) => Err(err),
                }?;
                false
            }
            Bound::Included(key) => match self.build(key, &mut mzs) {
                Ok(entry) => match key.cmp(entry.as_key().borrow()) {
                    cmp::Ordering::Greater => Ok(true),
                    _ => Ok(false),
                },
                Err(Error::EmptyIndex) => Ok(false),
                Err(err) => Err(err),
            }?,
            Bound::Excluded(key) => match self.build(key, &mut mzs) {
                Ok(entry) => match key.cmp(entry.as_key().borrow()) {
                    cmp::Ordering::Equal | cmp::Ordering::Greater => Ok(true),
                    _ => Ok(false),
                },
                Err(Error::EmptyIndex) => Ok(false),
                Err(err) => Err(err),
            }?,
        };

        let mut r = ScanRange::new(self, mzs, range);
        if skip_one {
            r.next();
        }
        Ok(r)
    }
}

impl<K, V, B> Snapshot<K, V, B>
where
    K: Default + Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
    B: Bloom,
{
    /// partition robt indexes into ~ equally sized data-set and return their
    /// range.
    fn to_partitions(&mut self) -> Result<Vec<(Bound<K>, Bound<K>)>> {
        let m_blocksize = self.config.m_blocksize;
        let mut partitions = vec![];
        let mut lk = Bound::<K>::Unbounded;

        // first level
        let fpos = match self.to_root() {
            Ok(root) => Ok(root),
            Err(Error::EmptyIndex) => return Ok(vec![]),
            Err(err) => Err(err),
        }?;
        let mblock1 = MBlock::<K, V>::new_decode(self.index_fd.read_buffer(
            fpos,
            m_blocksize,
            "partitions, reading root",
        )?)?;
        // println!("robt to_partitions root len {}", mblock1.len());
        for index in 0..mblock1.len() {
            let mentry = mblock1.to_entry(index)?;

            if mentry.is_zblock() {
                let hk = mblock1.to_key(index)?;
                let range = (lk.clone(), Bound::Excluded(hk.clone()));
                if self.range(range.clone())?.next().is_none() {
                    continue;
                }

                partitions.push(range);
                lk = Bound::Included(hk);
            } else {
                let mblock2 = MBlock::<K, V>::new_decode(self.index_fd.read_buffer(
                    mentry.to_fpos(),
                    m_blocksize,
                    "partitions, reading mblock1",
                )?)?;
                // println!("to_partitions level-1 len {}", mblock2.len());
                for index in 0..mblock2.len() {
                    let hk = mblock2.to_key(index)?;
                    let range = (lk.clone(), Bound::Excluded(hk.clone()));
                    if self.range(range.clone())?.next().is_none() {
                        continue;
                    }

                    partitions.push(range);
                    lk = Bound::Included(hk);
                }
            }
        }

        partitions.push((lk, Bound::<K>::Unbounded));

        Ok(partitions)
    }

    fn to_shards(&mut self, n_shards: usize) -> Result<Vec<(Bound<K>, Bound<K>)>> {
        let mut shards = vec![];

        let partitions = {
            let mut ps = self.to_partitions()?;
            if ps.len() > 1 {
                ps.pop(); // last partition might have less items.
                let (lk, _) = ps.pop().unwrap();
                ps.push((lk, Bound::<K>::Unbounded));
            }
            ps
        };

        for part in util::as_sharded_array(&partitions, n_shards) {
            if part.len() == 0 {
                continue;
            }
            let (lk, _) = part.first().unwrap();
            let (_, hk) = part.last().unwrap();
            shards.push((lk.clone(), hk.clone()));
        }

        Ok(shards)
    }
}

impl<K, V, B> Footprint for Snapshot<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    fn footprint(&self) -> Result<isize> {
        let i_footprint: isize = self.index_fd.footprint()?;
        let v_footprint: isize = match &self.valog_fd {
            Some((vlog_file, _)) => {
                let md = err_at!(IoError, fs::metadata(vlog_file))?;
                convert_at!(md.len())?
            }
            None => 0,
        };
        Ok(i_footprint + v_footprint)
    }
}

impl<K, V, B> Validate<Stats> for Snapshot<K, V, B>
where
    K: Default + Clone + Ord + Serialize + fmt::Debug,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
    B: Bloom,
{
    fn validate(&mut self) -> Result<Stats> {
        // validate config and stats.
        let c = self.config.clone();
        let s = self.to_stats()?;
        if c.name != s.name {
            let msg = format!("validate, name {} != {}", c.name, s.name);
            return err_at!(Fatal, msg: msg);
        } else if c.z_blocksize != s.z_blocksize {
            let (x, y) = (c.z_blocksize, s.z_blocksize);
            let msg = format!("validate, z_blocksize {} != {}", x, y);
            return err_at!(Fatal, msg: msg);
        } else if c.m_blocksize != s.m_blocksize {
            let (x, y) = (c.m_blocksize, s.m_blocksize);
            let msg = format!("validate, m_blocksize {} != {}", x, y);
            return err_at!(Fatal, msg: msg);
        } else if c.v_blocksize != s.v_blocksize {
            let (x, y) = (c.v_blocksize, s.v_blocksize);
            let msg = format!("validate, v_blocksize {} != {}", x, y);
            return err_at!(Fatal, msg: msg);
        } else if c.delta_ok != s.delta_ok {
            let msg = format!("validate, delta_ok {} != {}", c.delta_ok, s.delta_ok);
            return err_at!(Fatal, msg: msg);
        } else if c.value_in_vlog != s.value_in_vlog {
            let msg = format!(
                "validate, value_in_vlog {} != {}",
                c.value_in_vlog, s.value_in_vlog
            );
            return err_at!(Fatal, msg: msg);
        }

        let mut footprint: isize = {
            let total = s.m_bytes + s.z_bytes + s.v_bytes + s.n_abytes;
            convert_at!(total)?
        };
        let (_, meta_block_bytes) = read_meta_items(&self.dir, &self.name)?;
        footprint += {
            let n: isize = convert_at!(meta_block_bytes)?;
            n
        };
        assert_eq!(footprint, self.footprint()?);

        let iter = self.iter()?;
        let mut prev_key: Option<K> = None;
        let (mut n_count, mut n_deleted, mut seqno) = (0, 0, 0);
        for entry in iter {
            let entry = entry?;
            if entry.is_deleted() {
                n_deleted += 1;
            }
            n_count += 1;
            seqno = cmp::max(seqno, entry.to_seqno());
            //println!(
            //    "validate iter seqno:{} m_seqno:{} s_seqno:{}",
            //    entry.to_seqno(),
            //    seqno,
            //    s.seqno
            //);
            prev_key = match prev_key {
                Some(prev_key) if prev_key.ge(entry.as_key()) => {
                    let msg = format!(
                        "validate, sort error {:?} >= {:?}",
                        prev_key,
                        entry.as_key()
                    );
                    return err_at!(Fatal, msg: msg);
                }
                _ => Some(entry.to_key()),
            }
        }

        if n_count != s.n_count {
            let msg = format!("validate, n_count {} > {}", n_count, s.n_count);
            err_at!(Fatal, msg: msg)
        } else if n_deleted != s.n_deleted {
            let msg = format!("validate, n_deleted {} > {}", n_deleted, s.n_deleted);
            err_at!(Fatal, msg: msg)
        } else if seqno > 0 && seqno > s.seqno {
            let msg = format!("validate, seqno {} > {}", seqno, s.seqno);
            err_at!(Fatal, msg: msg)
        } else {
            Ok(s)
        }
    }
}

// Read methods
impl<K, V, B> Reader<K, V> for Snapshot<K, V, B>
where
    K: Default + Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
    B: Bloom,
{
    fn get<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + Hash,
    {
        // check in the bitmap if key is present, there can be false
        // positive, but can't be a false negative.
        if self.bitmap.contains(key) == false {
            return Err(Error::KeyNotFound);
        }
        // println!("robt get ..");
        let versions = false;
        self.do_get(key, versions)
    }

    fn iter(&mut self) -> Result<IndexIter<K, V>> {
        let mut mzs = vec![];
        match self.to_root() {
            Ok(root) => Ok(self.build_fwd(root, &mut mzs)?),
            Err(Error::EmptyIndex) => Ok(()),
            Err(err) => Err(err),
        }?;
        Ok(Iter::new(self, mzs))
    }

    fn range<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let versions = false;
        self.do_range(range, versions)
    }

    fn reverse<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let versions = false;
        self.do_reverse(range, versions)
    }

    fn get_with_versions<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + Hash,
    {
        // check in the bitmap if key is present, there can be false
        // positive, but can't be a false negative.
        if self.bitmap.contains(key) == false {
            return Err(Error::KeyNotFound);
        }

        let versions = true;
        self.do_get(key, versions)
    }

    /// Iterate over all entries in this index. Returned entry shall
    /// have all its previous versions, can be a costly call.
    fn iter_with_versions(&mut self) -> Result<IndexIter<K, V>> {
        let mut mzs = vec![];
        match self.to_root() {
            Ok(root) => Ok(self.build_fwd(root, &mut mzs)?),
            Err(Error::EmptyIndex) => Ok(()),
            Err(err) => Err(err),
        }?;
        Ok(Iter::new_versions(self, mzs))
    }

    /// Iterate from lower bound to upper bound. Returned entry shall
    /// have all its previous versions, can be a costly call.
    fn range_with_versions<'a, R, Q>(
        &'a mut self,
        range: R, // range bound
    ) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let versions = true;
        self.do_range(range, versions)
    }

    /// Iterate from upper bound to lower bound. Returned entry shall
    /// have all its previous versions, can be a costly call.
    fn reverse_with_versions<'a, R, Q>(
        &'a mut self,
        range: R, // reverse range bound
    ) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let versions = true;
        self.do_reverse(range, versions)
    }
}

impl<K, V, B> CommitIterator<K, V> for Snapshot<K, V, B>
where
    K: Default + Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
    B: Bloom,
{
    fn scan<G>(&mut self, within: G) -> Result<IndexIter<K, V>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let iter = {
            let snap = Snapshot::<K, V, B>::open(&self.dir, &self.name)?;
            scans::FilterScans::new(vec![snap.into_scan()?], within)
        };
        Ok(Box::new(iter))
    }

    fn scans<G>(&mut self, n_shards: usize, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let ranges = {
            let mut snap = Snapshot::<K, V, B>::open(&self.dir, &self.name)?;
            snap.to_shards(n_shards)?
        };

        let mut iters = vec![];
        for range in ranges.into_iter() {
            let snap = Snapshot::<K, V, B>::open(&self.dir, &self.name)?;
            let iter: IndexIter<K, V> = Box::new(scans::FilterScans::new(
                vec![snap.into_range_scan(range)?],
                within.clone(),
            ));
            iters.push(iter)
        }

        // If there are not enough shards push empty iterators.
        for _ in iters.len()..n_shards {
            let ss = vec![];
            iters.push(Box::new(ss.into_iter()));
        }

        assert_eq!(iters.len(), n_shards);

        Ok(iters)
    }

    fn range_scans<N, G>(&mut self, ranges: Vec<N>, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
        N: Clone + RangeBounds<K>,
    {
        let mut iters = vec![];
        for range in ranges.into_iter() {
            let snap = Snapshot::<K, V, B>::open(&self.dir, &self.name)?;
            let iter: IndexIter<K, V> = Box::new(scans::FilterScans::new(
                vec![snap.into_range_scan(util::to_start_end(range))?],
                within.clone(),
            ));

            iters.push(iter)
        }

        Ok(iters)
    }
}

impl<K, V, B> Snapshot<K, V, B>
where
    K: Default + Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    /// Return the first entry in index, with only latest value.
    pub fn first(&mut self) -> Result<Entry<K, V>> {
        let zfpos = self.first_zpos(self.to_root()?)?;

        let z_blocksize = self.config.z_blocksize;
        let zblock = ZBlock::<K, V>::new_decode(self.index_fd.read_buffer(
            zfpos,
            z_blocksize,
            "first(), reading zblock",
        )?)?;

        let mut entry = zblock.to_entry(0)?.1;
        self.fetch(&mut entry, false, false)?;
        Ok(entry)
    }

    /// Return the first entry in index, with all versions.
    pub fn first_with_versions(&mut self) -> Result<Entry<K, V>> {
        let zfpos = self.first_zpos(self.to_root()?)?;

        let z_blocksize = self.config.z_blocksize;
        let zblock = ZBlock::<K, V>::new_decode(self.index_fd.read_buffer(
            zfpos,
            z_blocksize,
            "first(), reading zblock",
        )?)?;

        let mut entry = zblock.to_entry(0)?.1;
        self.fetch(&mut entry, false, true)?;
        Ok(entry)
    }

    /// Return the last entry in index, with only latest value.
    pub fn last(&mut self) -> Result<Entry<K, V>> {
        let zfpos = self.last_zfpos(self.to_root()?)?;

        let z_blocksize = self.config.z_blocksize;
        let zblock = ZBlock::<K, V>::new_decode(self.index_fd.read_buffer(
            zfpos,
            z_blocksize,
            "last(), reading zblock",
        )?)?;

        let mut entry = zblock.last()?.1;
        self.fetch(&mut entry, false, false)?;
        Ok(entry)
    }

    /// Return the last entry in index, with all versions.
    pub fn last_with_versions(&mut self) -> Result<Entry<K, V>> {
        let zfpos = self.last_zfpos(self.to_root()?)?;

        let z_blocksize = self.config.z_blocksize;
        let zblock = ZBlock::<K, V>::new_decode(self.index_fd.read_buffer(
            zfpos,
            z_blocksize,
            "last(), reading zblock",
        )?)?;

        let mut entry = zblock.last()?.1;
        self.fetch(&mut entry, false, true)?;
        Ok(entry)
    }

    fn first_zpos(&mut self, fpos: u64) -> Result<u64> {
        let m_blocksize = self.config.m_blocksize;
        let mblock = MBlock::<K, V>::new_decode(self.index_fd.read_buffer(
            fpos,
            m_blocksize,
            "first_zpos, reading mblock",
        )?)?;

        let mentry = mblock.to_entry(0)?;
        if mentry.is_zblock() {
            Ok(mentry.to_fpos())
        } else {
            self.first_zpos(mentry.to_fpos())
        }
    }

    fn last_zfpos(&mut self, fpos: u64) -> Result<u64> {
        let m_blocksize = self.config.m_blocksize;
        let mblock = MBlock::<K, V>::new_decode(self.index_fd.read_buffer(
            fpos,
            m_blocksize,
            "last_zpos, reading mblock",
        )?)?;

        let mentry = mblock.last()?;
        if mentry.is_zblock() {
            Ok(mentry.to_fpos())
        } else {
            self.last_zfpos(mentry.to_fpos())
        }
    }

    fn get_zpos<Q>(&mut self, key: &Q, fpos: u64) -> Result<u64>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mblock = MBlock::<K, V>::new_decode(self.index_fd.read_buffer(
            fpos,
            self.config.m_blocksize,
            "get_zpos(), reading mblock",
        )?)?;
        match mblock.get(key, Bound::Unbounded, Bound::Unbounded) {
            Err(Error::__LessThan) => Err(Error::KeyNotFound),
            Ok(mentry) if mentry.is_zblock() => Ok(mentry.to_fpos()),
            Ok(mentry) => self.get_zpos(key, mentry.to_fpos()),
            Err(err) => Err(err),
        }
    }

    fn do_get<Q>(&mut self, key: &Q, versions: bool) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let zfpos = self.get_zpos(key, self.to_root()?)?;

        // println!("do_get {}", zfpos);
        let zblock: ZBlock<K, V> = ZBlock::new_decode(self.index_fd.read_buffer(
            zfpos,
            self.config.z_blocksize,
            "do_get(), reading zblock",
        )?)?;
        match zblock.find(key, Bound::Unbounded, Bound::Unbounded) {
            Ok((_, mut entry)) => {
                if entry.as_key().borrow().eq(key) {
                    self.fetch(&mut entry, false /*shallow*/, versions)?;
                    Ok(entry)
                } else {
                    Err(Error::KeyNotFound)
                }
            }
            Err(Error::__LessThan) => Err(Error::KeyNotFound),
            Err(Error::__ZBlockExhausted(_)) => Err(Error::KeyNotFound),
            Err(err) => Err(err),
        }
    }

    fn do_range<'a, R, Q>(
        &'a mut self,
        range: R,
        versions: bool, // if true include older versions.
    ) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let mut mzs = vec![];
        let skip_one = match range.start_bound() {
            Bound::Unbounded => {
                match self.to_root() {
                    Ok(root) => Ok(self.build_fwd(root, &mut mzs)?),
                    Err(Error::EmptyIndex) => Ok(()),
                    Err(err) => Err(err),
                }?;
                false
            }
            Bound::Included(key) => match self.build(key, &mut mzs) {
                Ok(entry) => match key.cmp(entry.as_key().borrow()) {
                    cmp::Ordering::Greater => Ok(true),
                    _ => Ok(false),
                },
                Err(Error::EmptyIndex) => Ok(false),
                Err(err) => Err(err),
            }?,
            Bound::Excluded(key) => match self.build(key, &mut mzs) {
                Ok(entry) => match key.cmp(entry.as_key().borrow()) {
                    cmp::Ordering::Equal | cmp::Ordering::Greater => Ok(true),
                    _ => Ok(false),
                },
                Err(Error::EmptyIndex) => Ok(false),
                Err(err) => Err(err),
            }?,
        };
        let mut r = Range::new(self, mzs, range, versions);
        if skip_one {
            r.next();
        }
        Ok(r)
    }

    fn do_reverse<'a, R, Q>(
        &'a mut self,
        range: R, // reverse range bound
        versions: bool,
    ) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let mut mzs = vec![];
        let skip_one = match range.end_bound() {
            Bound::Unbounded => {
                match self.to_root() {
                    Ok(root) => Ok(self.build_rev(root, &mut mzs)?),
                    Err(Error::EmptyIndex) => Ok(()),
                    Err(err) => Err(err),
                }?;
                false
            }
            Bound::Included(key) => match self.build(&key, &mut mzs) {
                Ok(entry) => match key.cmp(entry.as_key().borrow()) {
                    cmp::Ordering::Less => Ok(true),
                    _ => Ok(false),
                },
                Err(Error::EmptyIndex) => Ok(false),
                Err(err) => Err(err),
            }?,
            Bound::Excluded(key) => match self.build(&key, &mut mzs) {
                Ok(entry) => match key.cmp(entry.as_key().borrow()) {
                    cmp::Ordering::Less | cmp::Ordering::Equal => Ok(true),
                    _ => Ok(false),
                },
                Err(Error::EmptyIndex) => Ok(false),
                Err(err) => Err(err),
            }?,
        };
        let mut rr = Reverse::new(self, mzs, range, versions);
        if skip_one {
            rr.next();
        }
        Ok(rr)
    }

    fn build_fwd(
        &mut self,
        mut fpos: u64,           // from node
        mzs: &mut Vec<MZ<K, V>>, // output
    ) -> Result<()> {
        let config = &self.config;

        // println!("build_fwd {} {}", mzs.len(), fpos);
        let zfpos = loop {
            let mblock = MBlock::<K, V>::new_decode(self.index_fd.read_buffer(
                fpos,
                config.m_blocksize,
                "build_fwd(), reading mblock",
            )?)?;
            mzs.push(MZ::M { fpos, index: 0 });

            let mentry = mblock.to_entry(0)?;
            if mentry.is_zblock() {
                break mentry.to_fpos();
            }
            fpos = mentry.to_fpos();
        };
        // println!("build_fwd {}", mzs.len());

        let zblock = ZBlock::new_decode(self.index_fd.read_buffer(
            zfpos,
            config.z_blocksize,
            "build_fwd(), reading zblock",
        )?)?;
        mzs.push(MZ::Z { zblock, index: 0 });
        Ok(())
    }

    fn rebuild_fwd(&mut self, mzs: &mut Vec<MZ<K, V>>) -> Result<()> {
        let config = &self.config;

        match mzs.pop() {
            None => Ok(()),
            Some(MZ::M { fpos, mut index }) => {
                let mblock = MBlock::<K, V>::new_decode(self.index_fd.read_buffer(
                    fpos,
                    config.m_blocksize,
                    "rebuild_fwd(), reading mblock",
                )?)?;
                index += 1;
                match mblock.to_entry(index) {
                    Ok(MEntry::DecZ { fpos: zfpos, .. }) => {
                        mzs.push(MZ::M { fpos, index });

                        let zblock = ZBlock::new_decode(self.index_fd.read_buffer(
                            zfpos,
                            config.z_blocksize,
                            "rebuild_fwd(), reading zblock",
                        )?)?;
                        mzs.push(MZ::Z { zblock, index: 0 });
                        Ok(())
                    }
                    Ok(MEntry::DecM { fpos: mfpos, .. }) => {
                        mzs.push(MZ::M { fpos, index });
                        self.build_fwd(mfpos, mzs)?;
                        Ok(())
                    }
                    Err(Error::__MBlockExhausted(_)) => self.rebuild_fwd(mzs),
                    _ => err_at!(Fatal, msg: format!("unreachable")),
                }
            }
            Some(MZ::Z { .. }) => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    fn build_rev(
        &mut self,
        mut fpos: u64,           // from node
        mzs: &mut Vec<MZ<K, V>>, // output
    ) -> Result<()> {
        let config = &self.config;

        let zfpos = loop {
            let mblock = MBlock::<K, V>::new_decode(self.index_fd.read_buffer(
                fpos,
                config.m_blocksize,
                "build_rev(), reading mblock",
            )?)?;
            let index = mblock.len() - 1;
            mzs.push(MZ::M { fpos, index });

            let mentry = mblock.to_entry(index)?;
            if mentry.is_zblock() {
                break mentry.to_fpos();
            }
            fpos = mentry.to_fpos();
        };

        let zblock = ZBlock::new_decode(self.index_fd.read_buffer(
            zfpos,
            config.z_blocksize,
            "build_rev(), reading zblock",
        )?)?;
        let index: isize = convert_at!((zblock.len()? - 1))?;
        mzs.push(MZ::Z { zblock, index });
        Ok(())
    }

    fn rebuild_rev(&mut self, mzs: &mut Vec<MZ<K, V>>) -> Result<()> {
        let config = &self.config;

        match mzs.pop() {
            None => Ok(()),
            Some(MZ::M { index: 0, .. }) => self.rebuild_rev(mzs),
            Some(MZ::M { fpos, mut index }) => {
                let mblock = MBlock::<K, V>::new_decode(self.index_fd.read_buffer(
                    fpos,
                    config.m_blocksize,
                    "rebuild_rev(), reading mblock",
                )?)?;
                index -= 1;
                match mblock.to_entry(index) {
                    Ok(MEntry::DecZ { fpos: zfpos, .. }) => {
                        mzs.push(MZ::M { fpos, index });

                        let zblock = ZBlock::new_decode(self.index_fd.read_buffer(
                            zfpos,
                            config.z_blocksize,
                            "rebuild_rev(), reading zblock",
                        )?)?;
                        let idx: isize = convert_at!((zblock.len()? - 1))?;
                        mzs.push(MZ::Z { zblock, index: idx });
                        Ok(())
                    }
                    Ok(MEntry::DecM { fpos: mfpos, .. }) => {
                        mzs.push(MZ::M { fpos, index });
                        self.build_rev(mfpos, mzs)?;
                        Ok(())
                    }
                    _ => err_at!(Fatal, msg: format!("unreachable")),
                }
            }
            Some(MZ::Z { .. }) => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    fn build<Q>(
        &mut self,
        key: &Q,
        mzs: &mut Vec<MZ<K, V>>, // output
    ) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut fpos = self.to_root()?;
        let config = &self.config;
        let (from_min, to_max) = (Bound::Unbounded, Bound::Unbounded);

        let zfpos = loop {
            let mblock = MBlock::<K, V>::new_decode(self.index_fd.read_buffer(
                fpos,
                config.m_blocksize,
                "build(), reading mblock",
            )?)?;
            let mentry = match mblock.find(key, from_min, to_max) {
                Ok(mentry) => Ok(mentry),
                Err(Error::__LessThan) => mblock.to_entry(0),
                Err(err) => Err(err),
            }?;
            let index = mentry.to_index()?;
            mzs.push(MZ::M { fpos, index });
            if mentry.is_zblock() {
                break mentry.to_fpos();
            }
            fpos = mentry.to_fpos();
        };

        let zblock = ZBlock::new_decode(self.index_fd.read_buffer(
            zfpos,
            config.z_blocksize,
            "build(), reading zblock",
        )?)?;
        let (index, entry) = match zblock.find(key, from_min, to_max) {
            Ok((index, entry)) => Ok((index, entry)),
            Err(Error::__LessThan) => zblock.to_entry(0),
            Err(Error::__ZBlockExhausted(index)) => {
                let (_, entry) = zblock.to_entry(index)?;
                Ok((index, entry))
            }
            Err(err) => Err(err),
        }?;
        mzs.push(MZ::Z {
            zblock,
            index: convert_at!(index)?,
        });
        Ok(entry)
    }
}

impl<K, V, B> Snapshot<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    fn fetch(
        &mut self,
        entry: &mut Entry<K, V>,
        shallow: bool,  // fetch neither value nor deltas.
        versions: bool, // fetch deltas as well
    ) -> Result<()> {
        if !shallow {
            match &mut self.valog_fd {
                Some((_, fd)) => entry.fetch_value(fd)?,
                _ => (),
            }
        }
        if versions {
            match &mut self.valog_fd {
                Some((_, fd)) => entry.fetch_deltas(fd)?,
                _ => (),
            }
        }
        Ok(())
    }
}

pub(crate) struct Scan<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    snap: Snapshot<K, V, B>,
    mzs: Vec<MZ<K, V>>,
}

impl<K, V, B> Scan<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    fn new(snap: Snapshot<K, V, B>, mzs: Vec<MZ<K, V>>) -> Self {
        Scan { snap, mzs }
    }
}

impl<K, V, B> Iterator for Scan<K, V, B>
where
    K: Default + Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Result<Entry<K, V>>> {
        match self.mzs.pop() {
            None => None,
            Some(mut z) => match z.next() {
                Some(Ok(mut entry)) => {
                    // println!("one {}", entry.to_seqno());
                    self.mzs.push(z);
                    let (shallow, versions) = (false, true);
                    match self.snap.fetch(&mut entry, shallow, versions) {
                        Ok(()) => Some(Ok(entry)),
                        Err(err) => Some(Err(err)),
                    }
                }
                Some(Err(err)) => {
                    // println!("two {}", err);
                    self.mzs.truncate(0);
                    Some(Err(err))
                }
                None => {
                    // println!("three none");
                    match self.snap.rebuild_fwd(&mut self.mzs) {
                        Err(err) => Some(Err(err)),
                        Ok(_) => self.next(),
                    }
                }
            },
        }
    }
}

pub(crate) struct ScanRange<K, V, B, R>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
    R: RangeBounds<K>,
{
    snap: Snapshot<K, V, B>,
    mzs: Vec<MZ<K, V>>,
    range: R,
}

impl<K, V, B, R> ScanRange<K, V, B, R>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
    R: RangeBounds<K>,
{
    fn new(snap: Snapshot<K, V, B>, mzs: Vec<MZ<K, V>>, range: R) -> Self {
        ScanRange { snap, mzs, range }
    }

    fn till_ok(&self, entry: &Entry<K, V>) -> bool {
        match self.range.end_bound() {
            Bound::Unbounded => true,
            Bound::Included(key) => entry.as_key().borrow().le(key),
            Bound::Excluded(key) => entry.as_key().borrow().lt(key),
        }
    }
}

impl<K, V, B, R> Iterator for ScanRange<K, V, B, R>
where
    K: Default + Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
    R: RangeBounds<K>,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Result<Entry<K, V>>> {
        match self.mzs.pop() {
            None => None,
            Some(mut z) => match z.next() {
                Some(Ok(mut entry)) => {
                    if self.till_ok(&entry) {
                        self.mzs.push(z);
                        let (shallow, versions) = (false, true);
                        match self.snap.fetch(&mut entry, shallow, versions) {
                            Ok(()) => Some(Ok(entry)),
                            Err(err) => Some(Err(err)),
                        }
                    } else {
                        self.mzs.truncate(0);
                        None
                    }
                }
                Some(Err(err)) => {
                    self.mzs.truncate(0);
                    Some(Err(err))
                }
                None => match self.snap.rebuild_fwd(&mut self.mzs) {
                    Err(err) => Some(Err(err)),
                    Ok(_) => self.next(),
                },
            },
        }
    }
}

/// Iterate type, to do full-table scan over [Robt] index.
pub struct Iter<'a, K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    snap: &'a mut Snapshot<K, V, B>,
    mzs: Vec<MZ<K, V>>,
    shallow: bool,
    versions: bool,
}

impl<'a, K, V, B> Iter<'a, K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    fn new(snap: &'a mut Snapshot<K, V, B>, mzs: Vec<MZ<K, V>>) -> Box<Self> {
        Box::new(Iter {
            snap,
            mzs,
            shallow: false,
            versions: false,
        })
    }

    fn new_versions(snap: &'a mut Snapshot<K, V, B>, mzs: Vec<MZ<K, V>>) -> Box<Self> {
        Box::new(Iter {
            snap,
            mzs,
            shallow: false,
            versions: true,
        })
    }

    fn new_shallow(snap: &'a mut Snapshot<K, V, B>, mzs: Vec<MZ<K, V>>) -> Box<Self> {
        Box::new(Iter {
            snap,
            mzs,
            shallow: true,
            versions: false,
        })
    }
}

impl<'a, K, V, B> Iterator for Iter<'a, K, V, B>
where
    K: Default + Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Result<Entry<K, V>>> {
        match self.mzs.pop() {
            None => None,
            Some(mut z) => match z.next() {
                Some(Ok(mut entry)) => {
                    // println!("one {}", entry.to_seqno());
                    self.mzs.push(z);
                    match self.snap.fetch(&mut entry, self.shallow, self.versions) {
                        Ok(()) => Some(Ok(entry)),
                        Err(err) => Some(Err(err)),
                    }
                }
                Some(Err(err)) => {
                    // println!("two {}", err);
                    self.mzs.truncate(0);
                    Some(Err(err))
                }
                None => {
                    // println!("three none");
                    match self.snap.rebuild_fwd(&mut self.mzs) {
                        Err(err) => Some(Err(err)),
                        Ok(_) => self.next(),
                    }
                }
            },
        }
    }
}

/// Iterate type, to range over [Robt] index, from a _lower bound_ to
/// _upper bound_.
pub struct Range<'a, K, V, B, R, Q>
where
    K: Clone + Ord + Borrow<Q> + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    snap: &'a mut Snapshot<K, V, B>,
    mzs: Vec<MZ<K, V>>,
    range: R,
    high: marker::PhantomData<Q>,
    versions: bool,
}

impl<'a, K, V, B, R, Q> Range<'a, K, V, B, R, Q>
where
    K: Clone + Ord + Borrow<Q> + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    fn new(
        snap: &'a mut Snapshot<K, V, B>,
        mzs: Vec<MZ<K, V>>,
        range: R, // range bound
        versions: bool,
    ) -> Box<Self> {
        Box::new(Range {
            snap,
            mzs,
            range,
            high: marker::PhantomData,
            versions,
        })
    }

    fn till_ok(&self, entry: &Entry<K, V>) -> bool {
        match self.range.end_bound() {
            Bound::Unbounded => true,
            Bound::Included(key) => entry.as_key().borrow().le(key),
            Bound::Excluded(key) => entry.as_key().borrow().lt(key),
        }
    }
}

impl<'a, K, V, B, R, Q> Iterator for Range<'a, K, V, B, R, Q>
where
    K: Default + Clone + Ord + Borrow<Q> + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Result<Entry<K, V>>> {
        match self.mzs.pop() {
            None => None,
            Some(mut z) => match z.next() {
                Some(Ok(mut entry)) if self.till_ok(&entry) => {
                    self.mzs.push(z);
                    let shallow = false;
                    match self.snap.fetch(&mut entry, shallow, self.versions) {
                        Ok(()) => Some(Ok(entry)),
                        Err(err) => Some(Err(err)),
                    }
                }
                Some(Ok(_)) => {
                    self.mzs.truncate(0);
                    None
                }
                Some(Err(err)) => {
                    self.mzs.truncate(0);
                    Some(Err(err))
                }
                None => match self.snap.rebuild_fwd(&mut self.mzs) {
                    Err(err) => Some(Err(err)),
                    Ok(_) => self.next(),
                },
            },
        }
    }
}

/// Iterate type, to range over [Robt] index, from an _upper bound_ to
/// _lower bound_.
pub struct Reverse<'a, K, V, B, R, Q>
where
    K: Clone + Ord + Borrow<Q> + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    snap: &'a mut Snapshot<K, V, B>,
    mzs: Vec<MZ<K, V>>,
    range: R,
    low: marker::PhantomData<Q>,
    versions: bool,
}

impl<'a, K, V, B, R, Q> Reverse<'a, K, V, B, R, Q>
where
    K: Clone + Ord + Borrow<Q> + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    fn new(
        snap: &'a mut Snapshot<K, V, B>,
        mzs: Vec<MZ<K, V>>,
        range: R, // reverse range bound
        versions: bool,
    ) -> Box<Self> {
        Box::new(Reverse {
            snap,
            mzs,
            range,
            low: marker::PhantomData,
            versions,
        })
    }

    fn till_ok(&self, entry: &Entry<K, V>) -> bool {
        match self.range.start_bound() {
            Bound::Unbounded => true,
            Bound::Included(key) => entry.as_key().borrow().ge(key),
            Bound::Excluded(key) => entry.as_key().borrow().gt(key),
        }
    }
}

impl<'a, K, V, B, R, Q> Iterator for Reverse<'a, K, V, B, R, Q>
where
    K: Default + Clone + Ord + Borrow<Q> + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Result<Entry<K, V>>> {
        let shallow = false;
        match self.mzs.pop() {
            None => None,
            Some(mut z) => match z.next_back() {
                Some(Err(err)) => {
                    self.mzs.truncate(0);
                    Some(Err(err))
                }
                Some(Ok(mut entry)) if self.till_ok(&entry) => {
                    self.mzs.push(z);
                    match self.snap.fetch(&mut entry, shallow, self.versions) {
                        Ok(()) => Some(Ok(entry)),
                        Err(err) => Some(Err(err)),
                    }
                }
                Some(Ok(_)) => {
                    self.mzs.truncate(0);
                    None
                }
                None => match self.snap.rebuild_rev(&mut self.mzs) {
                    Err(err) => Some(Err(err)),
                    Ok(_) => self.next(),
                },
            },
        }
    }
}

enum MZ<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    M { fpos: u64, index: usize },
    Z { zblock: ZBlock<K, V>, index: isize },
}

impl<K, V> Iterator for MZ<K, V>
where
    K: Default + Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Result<Entry<K, V>>> {
        match self {
            MZ::Z { zblock, index } => {
                let undex: usize = (*index).try_into().unwrap();
                match zblock.to_entry(undex) {
                    Ok((_, entry)) => {
                        *index += 1;
                        Some(Ok(entry))
                    }
                    Err(Error::__ZBlockExhausted(_)) => None,
                    Err(err) => Some(Err(err)),
                }
            }
            MZ::M { .. } => Some(err_at!(Fatal, msg: format!("unreachable"))),
        }
    }
}

impl<K, V> DoubleEndedIterator for MZ<K, V>
where
    K: Default + Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    fn next_back(&mut self) -> Option<Result<Entry<K, V>>> {
        match self {
            MZ::Z { zblock, index } if *index >= 0 => {
                let undex: usize = (*index).try_into().unwrap();
                match zblock.to_entry(undex) {
                    Ok((_, entry)) => {
                        *index -= 1;
                        Some(Ok(entry))
                    }
                    Err(Error::__ZBlockExhausted(_)) => None,
                    Err(err) => Some(Err(err)),
                }
            }
            MZ::Z { .. } => None,
            MZ::M { .. } => Some(err_at!(Fatal, msg: format!("unreachable"))),
        }
    }
}

fn purge_file(
    file: ffi::OsString,
    locked_files: &mut Vec<ffi::OsString>,
    err_files: &mut Vec<ffi::OsString>,
) -> &'static str {
    let res = match util::open_file_r(&file) {
        Ok(fd) => match fd.try_lock_exclusive() {
            Ok(_) => {
                let res = match fs::remove_file(&file) {
                    Err(err) => (
                        "error", // return error
                        format!("remove_file {:?} {}", file, err),
                    ),
                    Ok(_) => ("ok", format!("purged file {:?}", file)),
                };
                fd.unlock().ok();
                res
            }
            Err(_) => ("locked", format!("locked file {:?}", file)),
        },
        Err(err) => ("error", format!("open_file_r {:?} {:?}", file, err)),
    };
    match res {
        ("ok", msg) => {
            debug!(target: "robtpr", "{}", msg);
            "ok"
        }
        ("locked", msg) => {
            debug!(target: "robtpr", "{}", msg);
            locked_files.push(file);
            "locked"
        }
        ("error", msg) => {
            error!(target: "robtpr", "{}", msg);
            err_files.push(file);
            "error"
        }
        _ => unreachable!(),
    }
}

fn thread_purger(name: String, rx: rt::Rx<ffi::OsString, ()>) -> Result<()> {
    let mut locked_files = vec![];
    let mut err_files = vec![];

    loop {
        match rx.try_recv() {
            Ok((file, None)) => {
                purge_file(file.clone(), &mut locked_files, &mut err_files);
            }
            Ok((_, Some(_))) => err_at!(Fatal, msg: format!("unreachable"))?,
            Err(mpsc::TryRecvError::Empty) => (),
            Err(mpsc::TryRecvError::Disconnected) => break,
        }
        for file in locked_files.drain(..).collect::<Vec<ffi::OsString>>() {
            purge_file(file.clone(), &mut locked_files, &mut err_files);
        }
        if err_files.len() > 0 {
            error!(
                target: "robtpr", "{}, failed purging {} files",
                name, err_files.len()
            );
        }
        thread::sleep(time::Duration::from_secs(1));
    }

    for file in locked_files.drain(..).collect::<Vec<ffi::OsString>>() {
        purge_file(file, &mut locked_files, &mut err_files);
    }
    for file in err_files.clone().into_iter() {
        error!(target: "robtpr", "{}, error purging file {:?}", name, file);
    }

    let files = {
        let mut files = vec![];
        files.extend_from_slice(&locked_files);
        files.extend_from_slice(&err_files);
        files
    };
    if files.len() == 0 {
        Ok(())
    } else {
        Err(Error::PurgeFiles(files))
    }
}

#[cfg(test)]
#[path = "fs2_test.rs"]
mod fs2_test;
#[cfg(test)]
#[path = "robt_test.rs"]
mod robt_test;
