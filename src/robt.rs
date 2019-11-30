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
//! Tip of the index file contain 32-byte header providing
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
use jsondata::Json;
use lazy_static::lazy_static;
use log::{debug, error, info};

use std::{
    borrow::Borrow,
    cmp,
    convert::TryInto,
    ffi, fmt, fs,
    hash::Hash,
    io::Write,
    marker, mem,
    ops::{Bound, Deref, RangeBounds},
    path, result,
    str::FromStr,
    sync::{self, mpsc, Arc},
    thread, time,
};

use crate::{
    core::{Bloom, CommitIterator, Index, Serialize, ToJson, Validate},
    core::{Diff, DiskIndexFactory, Entry, Footprint, IndexIter, Reader, Result},
    error::Error,
    panic::Panic,
    robt_entry::MEntry,
    robt_index::{MBlock, ZBlock},
    scans::{BitmappedScan, CompactScan},
    util,
};

include!("robt_marker.rs");

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
    fn from((s, n): (String, usize)) -> Name {
        Name(format!("{}-robt-{}", s, n))
    }
}

impl From<Name> for Option<(String, usize)> {
    fn from(name: Name) -> Option<(String, usize)> {
        let parts: Vec<&str> = name.0.split('-').collect();
        if parts.len() < 3 {
            None
        } else if parts[parts.len() - 2] != "robt" {
            None
        } else {
            let n = parts[parts.len() - 1].parse::<usize>().ok()?;
            let s = parts[..(parts.len() - 2)].join("-");
            Some((s, n))
        }
    }
}

impl From<Name> for ffi::OsString {
    fn from(name: Name) -> ffi::OsString {
        let file_name = format!("{}.indx", name.0);
        let file_name: &ffi::OsStr = file_name.as_ref();
        file_name.to_os_string()
    }
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for Name {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{:?}", self.0)
    }
}

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

impl<K, V, B> RobtFactory<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    // file name should match the following criteria.
    // a. must have a `.indx` suffix.
    // b. must have the robt naming convention, refer `Name` type for details.
    fn to_name(file_name: &ffi::OsStr) -> Option<Name> {
        let stem = match path::Path::new(file_name).extension() {
            Some(ext) if ext.to_str() == Some("indx") => path::Path::new(file_name).file_stem(),
            Some(_) | None => None,
        }?;
        let parts: Option<(String, usize)> = Name(stem.to_str()?.to_string()).into();
        Some(parts?.into())
    }
}

impl<K, V, B> DiskIndexFactory<K, V> for RobtFactory<K, V, B>
where
    K: Clone + Ord + Hash + Footprint + Serialize,
    V: Clone + Diff + Footprint + Serialize,
    <V as Diff>::D: Serialize,
    B: Bloom,
{
    type I = Robt<K, V, B>;

    fn new(&self, dir: &ffi::OsStr, name: &str) -> Result<Robt<K, V, B>> {
        let mut config = self.config.clone();
        config.name = name.to_string();
        info!(
            target: "robtfc",
            "{:?}, new index at {:?} with config ...\n{}", name, dir, config
        );

        let (tx, rx) = mpsc::channel();
        let inner = InnerRobt::Build {
            dir: dir.to_os_string(),
            name: (name.to_string(), 0).into(),
            purge_tx: Some(tx),
            config,

            _phantom_key: marker::PhantomData,
            _phantom_val: marker::PhantomData,
        };
        let name = name.to_string();
        let handle = thread::spawn(|| purger(name, rx));
        Ok(Robt::new(inner, handle))
    }

    fn open(&self, dir: &ffi::OsStr, root: ffi::OsString) -> Result<Robt<K, V, B>> {
        let name = Self::to_name(&root).ok_or(Error::InvalidFile(format!(
            "open robt {:?}/{:?}",
            dir, root
        )))?;

        info!(target: "robtfc", "{:?}, open from {:?}/{} with config ...", name, dir, name);

        let snapshot = Snapshot::<K, V, B>::open(dir, &name.0)?;
        snapshot.log()?;

        let (tx, rx) = mpsc::channel();
        let inner = InnerRobt::Snapshot {
            dir: dir.to_os_string(),
            name: name.clone(),
            footprint: snapshot.footprint()?,
            meta: snapshot.meta.clone(),
            config: snapshot.config.clone(),
            stats: snapshot.to_stats()?,
            bitmap: Arc::new(snapshot.to_bitmap()?),
            purge_tx: Some(tx),
        };
        let name = name.0.clone();
        let handle = thread::spawn(|| purger(name, rx));
        Ok(Robt::new(inner, handle))
    }

    fn to_type(&self) -> String {
        "robt".to_string()
    }
}

/// Read only btree. Immutable, fully-packed and lockless sharing.
pub struct Robt<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    inner: sync::Mutex<InnerRobt<K, V, B>>,
    purger: Option<thread::JoinHandle<()>>,
}

impl<K, V, B> Drop for Robt<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    fn drop(&mut self) {
        {
            let inner = self.inner.get_mut().unwrap();
            let _purge_tx = match inner {
                InnerRobt::Build { purge_tx, .. } => purge_tx.take().unwrap(),
                InnerRobt::Snapshot { purge_tx, .. } => purge_tx.take().unwrap(),
            };
        }
        self.purger.take().unwrap().join().ok(); // TODO: log message
    }
}

impl<K, V, B> Robt<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    fn new(inner: InnerRobt<K, V, B>, purger: thread::JoinHandle<()>) -> Robt<K, V, B> {
        Robt {
            inner: sync::Mutex::new(inner),
            purger: Some(purger),
        }
    }
}

#[derive(Clone)]
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
        purge_tx: Option<mpsc::Sender<ffi::OsString>>,

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
        purge_tx: Option<mpsc::Sender<ffi::OsString>>,
    },
}

impl<K, V, B> Index<K, V> for Robt<K, V, B>
where
    K: Clone + Ord + Hash + Footprint + Serialize,
    V: Clone + Diff + Footprint + Serialize,
    <V as Diff>::D: Serialize,
    B: Bloom,
{
    type R = Snapshot<K, V, B>;
    type W = Panic;
    type O = ffi::OsString;

    fn to_name(&self) -> String {
        let inner = self.inner.lock().unwrap();
        let name = match inner.deref() {
            InnerRobt::Build { name, .. } => name.clone(),
            InnerRobt::Snapshot { name, .. } => name.clone(),
        };
        let parts: Option<(String, usize)> = name.into();
        parts.unwrap().0 // just the name as passed to new().
    }

    fn to_root(&self) -> ffi::OsString {
        let inner = self.inner.lock().unwrap();
        match inner.deref() {
            InnerRobt::Build { name, .. } => name.clone().into(),
            InnerRobt::Snapshot { name, .. } => name.clone().into(),
        }
    }

    fn to_metadata(&self) -> Result<Vec<u8>> {
        let inner = self.inner.lock().unwrap();
        match inner.deref() {
            InnerRobt::Snapshot { meta, .. } => {
                if let MetaItem::AppMetadata(data) = &meta[2] {
                    Ok(data.clone())
                } else {
                    panic!("not reachable")
                }
            }
            InnerRobt::Build { .. } => panic!("not reachable"),
        }
    }

    /// Return the current seqno tracked by this index.
    fn to_seqno(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        match inner.deref() {
            InnerRobt::Build { .. } => 0,
            InnerRobt::Snapshot { stats, .. } => stats.seqno,
        }
    }

    /// Application can set the start sequence number for this index.
    fn set_seqno(&mut self, _seqno: u64) {
        // noop
    }

    fn to_reader(&mut self) -> Result<Self::R> {
        let inner = self.inner.lock().unwrap();
        match inner.deref() {
            InnerRobt::Snapshot {
                dir, name, bitmap, ..
            } => {
                info!(target: "robt  ", "{:?}, new reader ...", name);
                let mut snapshot = Snapshot::open(dir, &name.0)?;
                assert!(snapshot.set_bitmap(Arc::clone(bitmap)));
                snapshot.log()?;
                Ok(snapshot)
            }
            InnerRobt::Build { .. } => panic!("cannot create a reader"),
        }
    }

    fn to_writer(&mut self) -> Result<Self::W> {
        Ok(Panic::new("robt"))
    }

    fn commit<C, F>(&mut self, mut scanner: C, metacb: F) -> Result<()>
    where
        C: CommitIterator<K, V>,
        F: Fn(Vec<u8>) -> Vec<u8>,
    {
        let mut inner = self.inner.lock().unwrap();
        let new_inner = match inner.deref() {
            InnerRobt::Build {
                dir,
                name,
                config,
                purge_tx,
                ..
            } => {
                info!(target: "robt  ", "{:?}, flush commit ...", name);

                let snapshot = {
                    let b = Builder::<K, V, B>::initial(dir, &name.0, config.clone())?;
                    b.build(scanner.scan(Bound::Unbounded)?, metacb(vec![]))?;
                    let snapshot = Snapshot::<K, V, B>::open(dir, &name.0)?;
                    snapshot.log()?;
                    snapshot
                };
                let stats = snapshot.to_stats()?;
                let footprint = snapshot.footprint()?;
                info!(
                    target: "robt  ",
                    "{:?}, flushed to index file {:?}", name, snapshot.index_fd.to_file()
                );
                if let Some((vlog_file, _)) = &snapshot.valog_fd {
                    info!(target: "robt  ", "{:?}, flushed to valog file {:?}", name, vlog_file);
                }
                info!(
                    target: "robt  ",
                    "{:?}, footprint {}, wrote {} bytes", name, footprint, footprint
                );

                InnerRobt::Snapshot {
                    dir: dir.clone(),
                    name: name.clone(),
                    footprint,
                    meta: snapshot.meta.clone(),
                    config: snapshot.config.clone(),
                    stats,
                    bitmap: Arc::new(snapshot.to_bitmap()?),
                    purge_tx: purge_tx.clone(),
                }
            }
            InnerRobt::Snapshot {
                dir,
                name,
                config,
                purge_tx,
                ..
            } => {
                info!(target: "robt  ", "{:?}, incremental commit ...", name);

                let (name, snapshot, meta_block_bytes) = {
                    let scanner = scanner.scan(Bound::Unbounded)?;
                    let bitmap_iter = BitmappedScan::new(scanner);

                    let mut old_snapshot = Snapshot::<K, V, B>::open(dir, &name.0)?;
                    let index_file = old_snapshot.index_fd.to_file();
                    let old_meta = old_snapshot.to_app_meta()?;
                    let (name, b, root, bitmap_iter) = {
                        let commit_scanner = {
                            let mut mzs = vec![];
                            old_snapshot.build_fwd(old_snapshot.to_root().unwrap(), &mut mzs)?;
                            let old_iter = Iter::new_shallow(&mut old_snapshot, mzs);
                            CommitScan::new(bitmap_iter, old_iter)
                        };
                        let mut build_scanner = BuildScan::new(commit_scanner);
                        let name = name.clone().next();
                        let mut b = Builder::<K, V, B>::incremental(dir, &name.0, config.clone())?;
                        let root = b.build_tree(&mut build_scanner)?;
                        let commit_scanner = build_scanner.update_stats(&mut b.stats)?;
                        let (bitmap_iter, _) = commit_scanner.close()?;
                        (name, b, root, bitmap_iter)
                    };

                    let old_bitmap = old_snapshot.to_bitmap()?;
                    let m = old_bitmap.len();
                    let (_, new_bitmap): (_, B) = bitmap_iter.close()?;
                    let n = new_bitmap.len();
                    let bitmap = old_bitmap.or(&new_bitmap)?;
                    let x = bitmap.len();
                    info!(target: "robt  ", "{:?}, old_bitmap({}) + new_bitmap({}) = {}", name, m, n, x);
                    let meta_block_bytes = b.build_finish(metacb(old_meta), bitmap, root)?;

                    let snapshot = Snapshot::<K, V, B>::open(dir, &name.0)?;
                    snapshot.log()?;

                    // purge old snapshots file(s).
                    purge_tx.as_ref().unwrap().send(index_file)?;

                    (name, snapshot, meta_block_bytes)
                };
                let stats = snapshot.to_stats()?;
                let footprint = snapshot.footprint()?;
                info!(
                    target: "robt  ",
                    "{:?}, commited to index file {:?}", name, snapshot.index_fd.to_file()
                );
                if let Some((vlog_file, _)) = &snapshot.valog_fd {
                    info!(target: "robt  ", "{:?}, commited to valog file {:?}", name, vlog_file);
                }
                info!(
                    target: "robt  ",
                    "{:?}, footprint {}, wrote {} bytes",
                    name, footprint, stats.z_bytes + stats.m_bytes + stats.v_bytes + meta_block_bytes
                );

                InnerRobt::Snapshot {
                    dir: dir.clone(),
                    name: name.clone(),
                    footprint,
                    meta: snapshot.meta.clone(),
                    config: snapshot.config.clone(),
                    stats,
                    bitmap: Arc::new(snapshot.to_bitmap()?),
                    purge_tx: purge_tx.clone(),
                }
            }
        };
        *inner = new_inner;
        Ok(())
    }

    fn compact<F>(&mut self, cutoff: Bound<u64>, _metacb: F) -> Result<()>
    where
        F: Fn(Vec<Vec<u8>>) -> Vec<u8>,
    {
        let mut inner = self.inner.lock().unwrap();
        let new_inner = match inner.deref() {
            InnerRobt::Build {
                dir,
                name,
                config,
                purge_tx,
                ..
            } => InnerRobt::Build {
                dir: dir.clone(),
                name: name.clone(),
                config: config.clone(),
                purge_tx: purge_tx.clone(),
                _phantom_key: marker::PhantomData,
                _phantom_val: marker::PhantomData,
            },
            InnerRobt::Snapshot {
                dir,
                name,
                config,
                meta,
                purge_tx,
                ..
            } => {
                let (name, snapshot, meta_block_bytes) = {
                    let mut old_snapshot = Snapshot::<K, V, B>::open(dir, &name.0)?;
                    let iter = CompactScan::new(old_snapshot.iter_with_versions()?, cutoff);

                    info!(target: "robt  ", "{:?}, compact ...", name);
                    let name = name.clone().next();
                    let mut conf = config.clone();
                    conf.vlog_file = None; // use a new vlog file as the target.
                    let meta = match &meta[2] {
                        MetaItem::AppMetadata(data) => data.clone(),
                        _ => unreachable!(),
                    };
                    let b = Builder::<K, V, B>::initial(dir, &name.0, conf)?;
                    let meta_block_bytes = b.build(iter, meta)?;
                    let snapshot = Snapshot::<K, V, B>::open(dir, &name.0)?;
                    snapshot.log()?;

                    // purge old snapshots file(s).
                    purge_tx
                        .as_ref()
                        .unwrap()
                        .send(old_snapshot.index_fd.to_file())?;
                    match &old_snapshot.valog_fd {
                        Some((file, _)) => purge_tx.as_ref().unwrap().send(file.clone())?,
                        None => (),
                    }

                    (name, snapshot, meta_block_bytes)
                };
                let stats = snapshot.to_stats()?;
                let footprint = snapshot.footprint()?;
                info!(
                    target: "robt  ",
                    "{:?}, compacted to index file {:?}", name, snapshot.index_fd.to_file()
                );
                if let Some((vlog_file, _)) = &snapshot.valog_fd {
                    info!(target: "robt  ", "{:?}, compacted to valog file {:?}", name, vlog_file);
                }
                info!(
                    target: "robt  ",
                    "{:?}, footprint {}, wrote {} bytes",
                    name, footprint, stats.z_bytes + stats.m_bytes + stats.v_bytes + meta_block_bytes
                );

                InnerRobt::Snapshot {
                    dir: dir.clone(),
                    name: name.clone(),
                    footprint: snapshot.footprint()?,
                    meta: snapshot.meta.clone(),
                    config: snapshot.config.clone(),
                    stats,
                    bitmap: Arc::new(snapshot.to_bitmap()?),
                    purge_tx: purge_tx.clone(),
                }
            }
        };
        *inner = new_inner;
        Ok(())
    }
}

impl<K, V, B> Footprint for Robt<K, V, B>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize + Footprint,
{
    fn footprint(&self) -> Result<isize> {
        let inner = self.inner.lock().unwrap();
        match inner.deref() {
            InnerRobt::Snapshot { footprint, .. } => Ok(*footprint),
            InnerRobt::Build { .. } => unreachable!(),
        }
    }
}

/// Configuration options for Read Only BTree.
#[derive(Clone)]
pub struct Config {
    /// location path where index files are created.
    pub(crate) dir: ffi::OsString,
    /// name of the index.
    pub(crate) name: String,
    /// Leaf block size in btree index.
    /// Default: Config::ZBLOCKSIZE
    pub(crate) z_blocksize: usize,
    /// Intemediate block size in btree index.
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
    /// Flush queue size, channel queue size, holding index blocks.
    const FLUSH_QUEUE_SIZE: usize = 64;

    /// Configure differt set of block size for leaf-node, intermediate-node.
    pub fn set_blocksize(&mut self, z: usize, v: usize, m: usize) -> &mut Self {
        self.z_blocksize = z;
        self.v_blocksize = v;
        self.m_blocksize = m;
        self
    }

    /// Enable delta persistence, and configure value-log-file. To disable
    /// delta persistance, pass `vlog_file` as None.
    pub fn set_delta(&mut self, vlog_file: Option<ffi::OsString>, ok: bool) -> &mut Self {
        match vlog_file {
            Some(vlog_file) => {
                self.delta_ok = true;
                self.vlog_file = Some(vlog_file);
            }
            None if ok => self.delta_ok = true,
            None => self.delta_ok = false,
        }
        self
    }

    /// Persist values in a separate file, called value-log file. To persist
    /// values along with leaf node, pass `ok` as false.
    pub fn set_value_log(&mut self, file: Option<ffi::OsString>, ok: bool) -> &mut Self {
        match file {
            Some(vlog_file) => {
                self.value_in_vlog = true;
                self.vlog_file = Some(vlog_file);
            }
            None if ok => self.value_in_vlog = true,
            None => self.value_in_vlog = false,
        }
        self
    }

    /// Set flush queue size, increasing the queue size will improve batch
    /// flushing.
    pub fn set_flush_queue_size(&mut self, size: usize) -> &mut Self {
        self.flush_queue_size = size;
        self
    }
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        let (z, m, v) = (self.z_blocksize, self.m_blocksize, self.v_blocksize);
        let vlog_file = self
            .vlog_file
            .as_ref()
            .map_or(r#""""#.to_string(), |f| format!("{:?}, ", f));
        write!(
            f,
            concat!(
                "robt.name = {}\n",
                "robt.config.blocksize = {{ z={}, m={}, v={} }}\n",
                "robt.config = {{ delta_ok={}, value_in_vlog={} }}\n",
                "robt.config = {{ vlog_file={}, flush_queue_size={} }}",
            ),
            self.name, z, m, v, self.delta_ok, self.value_in_vlog, vlog_file, self.flush_queue_size,
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
            flush_queue_size: Self::FLUSH_QUEUE_SIZE,
        }
    }
}

impl Config {
    fn make_index_file(name: &str) -> String {
        format!("{}.indx", name)
    }

    fn make_vlog_file(name: &str) -> String {
        format!("{}.vlog", name)
    }

    fn stitch_index_file(
        dir: &ffi::OsStr, // directory can be os-native string
        name: &str,       // but name must be a valid utf8 string
    ) -> ffi::OsString {
        let mut index_file = path::PathBuf::from(dir);
        index_file.push(Self::make_index_file(name));
        let index_file: &ffi::OsStr = index_file.as_ref();
        index_file.to_os_string()
    }

    fn stitch_vlog_file(
        dir: &ffi::OsStr, // directory can be os-native string
        name: &str,       // but name must be a valid utf8 string
    ) -> ffi::OsString {
        let mut vlog_file = path::PathBuf::from(dir);
        vlog_file.push(Self::make_vlog_file(name));
        let vlog_file: &ffi::OsStr = vlog_file.as_ref();
        vlog_file.to_os_string()
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
    ///
    /// [Rdms]: crate::Rdms
    AppMetadata(Vec<u8>),
    /// Probability data structure,
    Bitmap(Vec<u8>),
    /// File-position where the root block for the Btree starts.
    Root(u64),
}

// returns bytes appended to file.
pub(crate) fn write_meta_items(
    file: ffi::OsString,
    items: Vec<MetaItem>, // list of meta items, starting from Marker
) -> Result<u64> {
    let p = path::Path::new(&file);
    let mut opts = fs::OpenOptions::new();
    let mut fd = opts.append(true).open(p)?;

    let (mut hdr, mut block) = (vec![], vec![]);
    hdr.resize(40, 0);

    for (i, item) in items.into_iter().enumerate() {
        match (i, item) {
            (0, MetaItem::Root(fpos)) => {
                debug!(target: "robt  ", "{:?}, writing root at {} ...", file, fpos);
                hdr[0..8].copy_from_slice(&fpos.to_be_bytes());
            }
            (1, MetaItem::Bitmap(bitmap)) => {
                let ln = bitmap.len() as u64;
                debug!(target: "robt  ", "{:?}, writing bitmap {} bytes ...", file, ln);
                hdr[8..16].copy_from_slice(&ln.to_be_bytes());
                block.extend_from_slice(&bitmap);
            }
            (2, MetaItem::AppMetadata(md)) => {
                let ln = md.len() as u64;
                debug!(target: "robt  ", "{:?}, writing app-meta {} bytes ...", file, ln);
                hdr[16..24].copy_from_slice(&ln.to_be_bytes());
                block.extend_from_slice(&md);
            }
            (3, MetaItem::Stats(s)) => {
                let ln = s.len() as u64;
                debug!(target: "robt  ", "{:?}, writing stats {} bytes ...", file, ln);
                hdr[24..32].copy_from_slice(&ln.to_be_bytes());
                block.extend_from_slice(s.as_bytes());
            }
            (4, MetaItem::Marker(data)) => {
                hdr[32..40].copy_from_slice(&(data.len() as u64).to_be_bytes());
                block.extend_from_slice(&data);
            }
            (i, m) => panic!("unreachable arm at {} {}", i, m),
        }
    }
    block.extend_from_slice(&hdr[..]);

    // flush / append into file.
    let n = Config::compute_root_block(block.len());
    let (shift, m) = (n - block.len(), block.len());
    block.resize(n, 0);
    block.copy_within(0..m, shift);
    let ln = block.len();
    let n = fd.write(&block)?;
    fd.sync_all()?;
    if n == ln {
        Ok(n.try_into().unwrap())
    } else {
        let msg = format!("robt write_meta_items: {:?} {}/{}...", &file, ln, n);
        Err(Error::PartialWrite(msg))
    }
}

/// Read meta items from [Robt] index file.
///
/// Meta-items is stored at the tip of the index file. If successful,
/// a vector of meta items is returned. Along with it, number of
/// meta-block bytes is return. To learn more about the meta items
/// refer to [MetaItem] type.
///
/// [Robt]: crate::robt::Robt
pub fn read_meta_items(
    dir: &ffi::OsStr, // directory of index, can be os-native string
    name: &str,
) -> Result<(Vec<MetaItem>, usize)> {
    let index_file = Config::stitch_index_file(dir, name);
    let m = fs::metadata(&index_file)?.len();
    let mut fd = util::open_file_r(index_file.as_ref())?;

    // read header
    let hdr = util::read_buffer(&mut fd, m - 40, 40, "read root-block header")?;
    let root = u64::from_be_bytes(hdr[..8].try_into().unwrap());
    let n_bmap = u64::from_be_bytes(hdr[8..16].try_into().unwrap()) as usize;
    let n_md = u64::from_be_bytes(hdr[16..24].try_into().unwrap()) as usize;
    let n_stats = u64::from_be_bytes(hdr[24..32].try_into().unwrap()) as usize;
    let n_marker = u64::from_be_bytes(hdr[32..40].try_into().unwrap()) as usize;
    // read block
    let meta_block_bytes = Config::compute_root_block(n_bmap + n_md + n_stats + n_marker + 40)
        .try_into()
        .unwrap();
    let block: Vec<u8> = util::read_buffer(
        &mut fd,
        m - meta_block_bytes,
        meta_block_bytes,
        "read root-block",
    )?
    .into_iter()
    .collect();

    let mut meta_items: Vec<MetaItem> = vec![];
    let z = (meta_block_bytes as usize) - 40;

    let (x, y) = (z - n_marker, z);
    let marker = block[x..y].to_vec();
    if marker.ne(&ROOT_MARKER.as_slice()) {
        let msg = format!("robt unexpected marker {:?}", marker);
        return Err(Error::InvalidSnapshot(msg));
    }

    let (x, y) = (z - n_marker - n_stats, z - n_marker);
    let stats = std::str::from_utf8(&block[x..y])?.to_string();

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
    let at = m - meta_block_bytes - (stats.m_blocksize as u64);
    if at != root {
        let msg = format!("robt expected root at {}, found {}", at, root);
        Err(Error::InvalidSnapshot(msg))
    } else {
        Ok((meta_items, meta_block_bytes as usize))
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

/// Btree configuration and statistics persisted along with index file.
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
        let js: Json = s.parse()?;
        let to_usize = |key: &str| -> Result<usize> {
            let n: usize = js.get(key)?.integer().unwrap().try_into().unwrap();
            Ok(n)
        };
        let to_u64 = |key: &str| -> Result<u64> {
            let n: u64 = js.get(key)?.integer().unwrap().try_into().unwrap();
            Ok(n)
        };
        let vlog_file = match js.get("/vlog_file")?.string() {
            Some(s) if s.len() == 0 => None,
            None => None,
            Some(s) => {
                let vlog_file: ffi::OsString = s.into();
                Some(vlog_file)
            }
        };

        Ok(Stats {
            name: js.get("/name")?.string().unwrap(),
            // config fields.
            z_blocksize: to_usize("/z_blocksize")?,
            m_blocksize: to_usize("/m_blocksize")?,
            v_blocksize: to_usize("/v_blocksize")?,
            delta_ok: js.get("/delta_ok")?.boolean().unwrap(),
            vlog_file: vlog_file,
            value_in_vlog: js.get("/value_in_vlog")?.boolean().unwrap(),
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
            epoch: js.get("/epoch")?.integer().unwrap(),
        })
    }
}

/// Builder type for constructing Read-Only-BTree index from an iterator.
///
/// Index can be built in _initial_ mode or _incremental_ mode. Refer
/// to corresponding methods for more information.
pub struct Builder<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    config: Config,
    iflusher: Flusher,
    vflusher: Option<Flusher>,
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
            let file = Config::stitch_index_file(dir, name);
            Flusher::new(file, config.clone(), create)?
        };
        let is_vlog = config.delta_ok || config.value_in_vlog;
        config.vlog_file = match &config.vlog_file {
            Some(vlog_file) if is_vlog => Some(vlog_file.clone()),
            None if is_vlog => Some(Config::stitch_vlog_file(dir, name)),
            _ => None,
        };
        let vflusher = config
            .vlog_file
            .as_ref()
            .map(|file| Flusher::new(file.clone(), config.clone(), create))
            .transpose()?;

        Ok(Builder {
            config: config.clone(),
            iflusher,
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
        mut config: Config, //  TODO: Bit of ugliness here
    ) -> Result<Builder<K, V, B>> {
        let iflusher = {
            let file = Config::stitch_index_file(dir, name);
            Flusher::new(file, config.clone(), true /*create*/)?
        };
        let is_vlog = config.delta_ok || config.value_in_vlog;
        config.vlog_file = match &config.vlog_file {
            Some(vlog_file) if is_vlog => Some(vlog_file.clone()),
            None if is_vlog => Some(Config::stitch_vlog_file(dir, name)),
            _ => None,
        };
        let create = false;
        let vflusher = config
            .vlog_file
            .as_ref()
            .map(|file| Flusher::new(file.clone(), config.clone(), create))
            .transpose()?;

        let mut stats: Stats = From::from(config.clone());
        stats.n_abytes += vflusher.as_ref().map_or(0, |vf| vf.fpos) as usize;

        Ok(Builder {
            config: config.clone(),
            iflusher,
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
            let mut build_scanner = BuildScan::new(BitmappedScan::new(iter));
            let root = self.build_tree(&mut build_scanner)?;
            let (_, bitmap) = build_scanner.update_stats(&mut self.stats)?.close()?;
            (root, bitmap)
        };

        self.build_finish(app_meta, bitmap, root)
    }

    /// Start building the index, this API should be used along with
    /// build_finish() to have more fine grained control, compared to
    /// build(), over the index build process.
    pub fn build_start<I>(mut self, iter: I) -> Result<u64>
    where
        I: Iterator<Item = Result<Entry<K, V>>>,
    {
        let mut build_scanner = BuildScan::new(iter);
        let root = self.build_tree(&mut build_scanner)?;
        build_scanner.update_stats(&mut self.stats)?;
        Ok(root)
    }

    pub fn build_finish(mut self, app_meta: Vec<u8>, bitmap: B, root: u64) -> Result<usize> {
        let (n_bitmap, bitmap) = (bitmap.len(), bitmap.to_vec());
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

        let index_file: ffi::OsString = self.iflusher.file.clone();
        // flush blocks and close
        self.iflusher.close_wait()?;
        self.vflusher.take().map(|x| x.close_wait()).transpose()?;
        // flush meta items to disk and close
        let meta_block_bytes = write_meta_items(index_file, meta_items)?;
        Ok(meta_block_bytes.try_into().unwrap())
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
            let vfpos = self.stats.n_abytes.try_into().unwrap();
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
                    let (zbytes, vbytes) = c.z.finalize(&mut self.stats);
                    c.z.flush(&mut self.iflusher, self.vflusher.as_mut())?;
                    c.fpos += zbytes;
                    c.vfpos += vbytes;

                    let mut m = c.ms.pop().unwrap();
                    match m.insertz(c.z.as_first_key(), c.zfpos) {
                        Ok(_) => c.ms.push(m),
                        Err(Error::__MBlockOverflow(_)) => {
                            // x is m_blocksize
                            let x = m.finalize(&mut self.stats);
                            m.flush(&mut self.iflusher)?;
                            let k = m.as_first_key();
                            let r = self.insertms(c.ms, c.fpos + x, k, c.fpos)?;
                            c.ms = r.0;
                            c.fpos = r.1;

                            m.reset();
                            m.insertz(c.z.as_first_key(), c.zfpos).unwrap();
                            c.ms.push(m)
                        }
                        Err(err) => return Err(err),
                    }

                    c.zfpos = c.fpos;
                    c.z.reset(c.vfpos);

                    c.z.insert(&entry, &mut self.stats).unwrap();
                }
                Err(err) => return Err(err),
            };
        }
        // println!(" number of mblocks: {}", c.ms.len());

        // flush final z-block
        if c.z.has_first_key() {
            // println!(" flush final zblock: {:?}", c.z.as_first_key());
            let (zbytes, vbytes) = c.z.finalize(&mut self.stats);
            c.z.flush(&mut self.iflusher, self.vflusher.as_mut())?;
            c.fpos += zbytes;
            c.vfpos += vbytes;

            let mut m = c.ms.pop().unwrap();
            match m.insertz(c.z.as_first_key(), c.zfpos) {
                Ok(_) => c.ms.push(m),
                Err(Error::__MBlockOverflow(_)) => {
                    let x = m.finalize(&mut self.stats);
                    m.flush(&mut self.iflusher)?;
                    let mkey = m.as_first_key();
                    let res = self.insertms(c.ms, c.fpos + x, mkey, c.fpos)?;
                    c.ms = res.0;
                    c.fpos = res.1;

                    m.reset();
                    m.insertz(c.z.as_first_key(), c.zfpos)?;
                    c.ms.push(m);
                }
                Err(err) => return Err(err),
            }
        } else {
            let msg = "robt build with empty iteratory".to_string();
            return Err(Error::DiskIndexFail(msg));
        }

        // flush final set of m-blocks
        while let Some(mut m) = c.ms.pop() {
            let is_root = m.has_first_key() && c.ms.len() == 0;
            if is_root {
                let x = m.finalize(&mut self.stats);
                m.flush(&mut self.iflusher)?;
                c.fpos += x;
            } else if m.has_first_key() {
                // x is m_blocksize
                let x = m.finalize(&mut self.stats);
                m.flush(&mut self.iflusher)?;
                let mkey = m.as_first_key();
                let res = self.insertms(c.ms, c.fpos + x, mkey, c.fpos)?;
                c.ms = res.0;
                c.fpos = res.1
            }
        }
        let n: u64 = self.config.m_blocksize.try_into().unwrap();
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
                m0.insertm(key, mfpos).unwrap();
                m0
            }
            Some(mut m0) => match m0.insertm(key, mfpos) {
                Ok(_) => m0,
                Err(Error::__MBlockOverflow(_)) => {
                    // println!("overflow for {:?} {}", key, mfpos);
                    // x is m_blocksize
                    let x = m0.finalize(&mut self.stats);
                    m0.flush(&mut self.iflusher)?;
                    let mkey = m0.as_first_key();
                    let res = self.insertms(ms, fpos + x, mkey, fpos)?;
                    ms = res.0;
                    fpos = res.1;

                    m0.reset();
                    m0.insertm(key, mfpos).unwrap();
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
    fn new(iter: I) -> BuildScan<K, V, I> {
        BuildScan {
            iter,

            start: time::SystemTime::now(),
            seqno: Default::default(),
            n_count: Default::default(),
            n_deleted: Default::default(),
        }
    }

    fn update_stats(self, stats: &mut Stats) -> Result<I> {
        stats.build_time = self.start.elapsed().unwrap().as_nanos().try_into().unwrap();
        stats.seqno = self.seqno;
        stats.n_count = self.n_count;
        stats.n_deleted = self.n_deleted;
        stats.epoch = time::UNIX_EPOCH
            .elapsed()
            .unwrap()
            .as_nanos()
            .try_into()
            .unwrap();
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
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
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
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.x_entry.take(), self.y_entry.take()) {
            (Some(Ok(xe)), Some(Ok(ye))) => match xe.as_key().cmp(ye.as_key()) {
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
                    match self.y_iter.snap.fetch(ye, false, false) {
                        Ok(ye) => Some(Ok(xe.xmerge(ye))),
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

#[allow(dead_code)] // TODO: remove this.
fn bitmap_index<B>(bitmap_rx: mpsc::Receiver<u32>) -> B
where
    B: Bloom,
{
    let mut bitmap = <B as Bloom>::create();
    for digest in bitmap_rx {
        bitmap.add_digest32(digest)
    }
    bitmap
}

pub(crate) struct Flusher {
    file: ffi::OsString,
    fpos: u64,
    t_handle: thread::JoinHandle<Result<()>>,
    tx: mpsc::SyncSender<Vec<u8>>,
}

impl Flusher {
    fn new(
        file: ffi::OsString,
        config: Config,
        create: bool, // if true create a new file
    ) -> Result<Flusher> {
        let (fd, fpos) = if create {
            (util::open_file_cw(file.clone())?, Default::default())
        } else {
            (util::open_file_w(&file)?, fs::metadata(&file)?.len())
        };

        let (tx, rx) = mpsc::sync_channel(config.flush_queue_size);
        let file1 = file.clone();
        let t_handle = thread::spawn(move || thread_flush(file1, fd, rx));

        Ok(Flusher {
            file,
            fpos,
            t_handle,
            tx,
        })
    }

    // return error if flush thread has exited/paniced.
    pub(crate) fn send(&mut self, block: Vec<u8>) -> Result<()> {
        self.tx.send(block)?;
        Ok(())
    }

    // return the cause for thread failure, if there is a failure, or return
    // a known error like io::Error or PartialWrite.
    fn close_wait(self) -> Result<()> {
        mem::drop(self.tx);
        match self.t_handle.join() {
            Ok(Ok(())) => Ok(()),
            Ok(Err(Error::PartialWrite(err))) => Err(Error::PartialWrite(err)),
            Err(err) => match err.downcast_ref::<String>() {
                Some(msg) => {
                    let msg = format!("robt flush err: {}", msg);
                    Err(Error::ThreadFail(msg))
                }
                None => {
                    let msg = format!("robt flush unknown error");
                    Err(Error::ThreadFail(msg))
                }
            },
            Ok(Err(err)) => panic!("unreachable arm with err : {:?}", err),
        }
    }
}

fn thread_flush(
    file: ffi::OsString, // for debuging purpose
    mut fd: fs::File,
    rx: mpsc::Receiver<Vec<u8>>,
) -> Result<()> {
    fd.lock_shared()?; // <---- read lock
    for data in rx {
        // println!("flusher {:?} {} {}", file, fpos, data.len());
        // fpos += data.len();
        let n = fd.write(&data)?;
        let m = data.len();
        if n != m {
            let msg = format!("robt flusher: {:?} {}/{}...", &file, m, n);
            fd.unlock()?; // <----- read un-lock
            return Err(Error::PartialWrite(msg));
        }
    }
    fd.sync_all()?;
    // file descriptor and receiver channel shall be dropped.
    fd.unlock()?; // <----- read un-lock
    Ok(())
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
                    Err(err) => Err(Error::InvalidSnapshot(format!(
                        "opening {:?} in mmap mode failed, {:?}",
                        file, err
                    ))),
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
                let n: u64 = n.try_into().unwrap();
                util::read_buffer(fd, fpos, n, msg)?
            }
            IndexFile::Mmap { mmap, .. } => {
                let start: usize = fpos.try_into().unwrap();
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
        Ok(fs::metadata(file)?.len().try_into().unwrap())
    }
}

/// A read only snapshot of BTree built using [robt] index.
///
/// [robt]: crate::robt
pub struct Snapshot<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    dir: ffi::OsString,
    name: String,
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
        self.index_fd.as_fd().unlock().ok();
        if let Some((_, fd)) = &self.valog_fd {
            fd.unlock().ok();
        }
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
        let (mut meta_items, _) = read_meta_items(dir, name)?;
        let stats: Stats = if let MetaItem::Stats(stats) = &meta_items[3] {
            Ok(stats.parse()?)
        } else {
            let msg = "robt snapshot statistics missing".to_string();
            Err(Error::InvalidSnapshot(msg))
        }?;
        let bitmap: Arc<B> = if let MetaItem::Bitmap(data) = &mut meta_items[1] {
            let bitmap = <B as Bloom>::from_vec(&data)?;
            data.drain(..);
            Ok(Arc::new(bitmap))
        } else {
            let msg = "robt snapshot bitmap missing".to_string();
            Err(Error::InvalidSnapshot(msg))
        }?;

        let config: Config = stats.into();

        // open index file.
        let index_fd = IndexFile::new_block(Config::stitch_index_file(dir, name))?;
        index_fd.as_fd().lock_shared()?;
        // open optional value log file.
        let valog_fd = match config.vlog_file {
            Some(vfile) => {
                // stem the file name.
                let mut vpath = path::PathBuf::new();
                vpath.push(path::Path::new(dir));
                vpath.push(path::Path::new(&vfile).file_name().unwrap());
                let vlog_file = vpath.as_os_str().to_os_string();
                let fd = util::open_file_r(&vlog_file)?;
                fd.lock_shared()?;
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

        Ok(snap) // Okey dockey
    }

    pub fn set_mmap(&mut self, ok: bool) -> Result<()> {
        unsafe { self.index_fd.set_mmap(ok) }
    }

    pub fn set_bitmap(&mut self, bitmap: Arc<B>) -> bool {
        let res = bitmap.to_vec() == self.bitmap.to_vec();
        self.bitmap = bitmap;
        res
    }

    pub fn is_snapshot(file_name: &ffi::OsStr) -> bool {
        RobtFactory::<K, V, B>::to_name(&file_name).is_some()
    }

    fn log(&self) -> Result<()> {
        info!(
            target: "robt  ",
            "{:?}, opening snapshot in dir {:?} config ...\n{}", self.name, self.dir, self.config
        );
        for item in self.meta.iter().enumerate() {
            match item {
                (0, MetaItem::Root(fpos)) => info!(
                    target: "robt  ", "{:?}, meta-item root at {}", self.name, fpos
                ),
                (1, MetaItem::Bitmap(_)) => info!(
                    target: "robt  ", "{:?}, meta-item bit-map", self.name
                ),
                (2, MetaItem::AppMetadata(data)) => info!(
                    target: "robt  ", "{:?}, meta-item app-meta-data {} bytes", self.name, data.len()
                ),
                (3, MetaItem::Stats(_)) => info!(
                    target: "robt  ", "{:?}, meta-item stats\n{}", self.name, self.to_stats()?
                ),
                (4, MetaItem::Marker(data)) => info!(
                    target: "robt  ", "{:?}, meta-item marker {} bytes", self.name, data.len()
                ),
                _ => unreachable!(),
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
    pub fn len(&self) -> usize {
        self.to_stats().unwrap().n_count.try_into().unwrap()
    }

    /// Return the last seqno found in this snapshot.
    pub fn to_seqno(&self) -> u64 {
        self.to_stats().unwrap().seqno
    }

    /// Return the file-position for Btree's root node.
    pub fn to_root(&self) -> Result<u64> {
        if let MetaItem::Root(root) = self.meta[0] {
            Ok(root)
        } else {
            Err(Error::InvalidSnapshot(
                "robt snapshot root missing".to_string(),
            ))
        }
    }

    /// Return the application metadata.
    pub fn to_app_meta(&self) -> Result<Vec<u8>> {
        if let MetaItem::AppMetadata(data) = &self.meta[2] {
            Ok(data.clone())
        } else {
            let msg = "robt snapshot app-metadata missing".to_string();
            Err(Error::InvalidSnapshot(msg))
        }
    }

    /// Return Btree statistics.
    pub fn to_stats(&self) -> Result<Stats> {
        if let MetaItem::Stats(stats) = &self.meta[3] {
            Ok(stats.parse()?)
        } else {
            let msg = "robt snapshot statistics missing".to_string();
            Err(Error::InvalidSnapshot(msg))
        }
    }

    pub fn to_vlog_path_file(&self) -> Option<String> {
        let stats: Stats = match &self.meta[3] {
            MetaItem::Stats(stats) => stats.parse().ok()?,
            _ => unreachable!(),
        };
        match stats.vlog_file {
            Some(vlog_file) => {
                let vf = path::Path::new(&vlog_file).file_name()?;
                Some(vf.to_str()?.to_string())
            }
            None => None,
        }
    }
}

impl<K, V, B> Snapshot<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    B: Bloom,
{
    fn to_bitmap(&self) -> Result<B> {
        if let MetaItem::Bitmap(data) = &self.meta[1] {
            <B as Bloom>::from_vec(&data)
        } else {
            let msg = "robt snapshot app-bitmap missing".to_string();
            Err(Error::InvalidSnapshot(msg))
        }
    }
}

impl<K, V, B> Footprint for Snapshot<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    fn footprint(&self) -> Result<isize> {
        let i_footprint: isize = self.index_fd.footprint()?.try_into().unwrap();
        let v_footprint: isize = match &self.valog_fd {
            Some((vlog_file, _)) => fs::metadata(vlog_file)?.len().try_into().unwrap(),
            None => 0,
        };
        Ok(i_footprint + v_footprint)
    }
}

impl<K, V, B> Validate<Stats> for Snapshot<K, V, B>
where
    K: Clone + Ord + Serialize + fmt::Debug,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
    B: Bloom,
{
    fn validate(&mut self) -> Result<Stats> {
        // validate config and stats.
        let c = self.config.clone();
        let s = self.to_stats()?;
        if c.name != s.name {
            let msg = format!("robt name {} != {}", c.name, s.name);
            return Err(Error::ValidationFail(msg));
        } else if c.z_blocksize != s.z_blocksize {
            let msg = format!("robt z_blocksize {} != {}", c.z_blocksize, s.z_blocksize);
            return Err(Error::ValidationFail(msg));
        } else if c.m_blocksize != s.m_blocksize {
            let msg = format!("robt m_blocksize {} != {}", c.m_blocksize, s.m_blocksize);
            return Err(Error::ValidationFail(msg));
        } else if c.v_blocksize != s.v_blocksize {
            let msg = format!("robt v_blocksize {} != {}", c.v_blocksize, s.v_blocksize);
            return Err(Error::ValidationFail(msg));
        } else if c.delta_ok != s.delta_ok {
            let msg = format!("robt delta_ok {} != {}", c.delta_ok, s.delta_ok);
            return Err(Error::ValidationFail(msg));
        } else if c.value_in_vlog != s.value_in_vlog {
            let msg = format!(
                "robt value_in_vlog {} != {}",
                c.value_in_vlog, s.value_in_vlog
            );
            return Err(Error::ValidationFail(msg));
        }

        let mut footprint: isize = (s.m_bytes + s.z_bytes + s.v_bytes + s.n_abytes)
            .try_into()
            .unwrap();
        let (_, meta_block_bytes) = read_meta_items(&self.dir, &self.name)?;
        footprint += meta_block_bytes as isize;
        assert_eq!(footprint, self.footprint().unwrap());

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
            prev_key = match prev_key {
                Some(prev_key) if prev_key.ge(entry.as_key()) => {
                    let msg = format!("robt sort error {:?} >= {:?}", prev_key, entry.as_key());
                    return Err(Error::ValidationFail(msg));
                }
                _ => Some(entry.to_key()),
            }
        }

        if n_count != s.n_count {
            let msg = format!("robt n_count {} > {}", n_count, s.n_count);
            Err(Error::ValidationFail(msg))
        } else if n_deleted != s.n_deleted {
            let msg = format!("robt n_deleted {} > {}", n_deleted, s.n_deleted);
            Err(Error::ValidationFail(msg))
        } else if seqno != s.seqno {
            let msg = format!("robt seqno {} > {}", seqno, s.seqno);
            Err(Error::ValidationFail(msg))
        } else {
            Ok(s)
        }
    }
}

// Read methods
impl<K, V, B> Reader<K, V> for Snapshot<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
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
        let snap = unsafe { (self as *mut Snapshot<K, V, B>).as_mut().unwrap() };
        let versions = false;
        snap.do_get(key, versions)
    }

    fn iter(&mut self) -> Result<IndexIter<K, V>> {
        let snap = unsafe { (self as *mut Snapshot<K, V, B>).as_mut().unwrap() };
        let mut mzs = vec![];
        snap.build_fwd(snap.to_root().unwrap(), &mut mzs)?;
        Ok(Iter::new(snap, mzs))
    }

    fn range<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let snap = unsafe { (self as *mut Snapshot<K, V, B>).as_mut().unwrap() };
        let versions = false;
        snap.do_range(range, versions)
    }

    fn reverse<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let snap = unsafe { (self as *mut Snapshot<K, V, B>).as_mut().unwrap() };
        let versions = false;
        snap.do_reverse(range, versions)
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

        let snap = unsafe { (self as *mut Snapshot<K, V, B>).as_mut().unwrap() };
        let versions = true;
        snap.do_get(key, versions)
    }

    /// Iterate over all entries in this index. Returned entry shall
    /// have all its previous versions, can be a costly call.
    fn iter_with_versions(&mut self) -> Result<IndexIter<K, V>> {
        let snap = unsafe { (self as *mut Snapshot<K, V, B>).as_mut().unwrap() };
        let mut mzs = vec![];
        snap.build_fwd(snap.to_root().unwrap(), &mut mzs)?;
        Ok(Iter::new_versions(snap, mzs))
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
        let snap = unsafe { (self as *mut Snapshot<K, V, B>).as_mut().unwrap() };
        let versions = true;
        snap.do_range(range, versions)
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
        let snap = unsafe { (self as *mut Snapshot<K, V, B>).as_mut().unwrap() };
        let versions = true;
        snap.do_reverse(range, versions)
    }
}

impl<K, V, B> Snapshot<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
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
        let zfpos = self.get_zpos(key, self.to_root().unwrap())?;

        // println!("do_get {}", zfpos);
        let zblock: ZBlock<K, V> = ZBlock::new_decode(self.index_fd.read_buffer(
            zfpos,
            self.config.z_blocksize,
            "do_get(), reading zblock",
        )?)?;
        match zblock.find(key, Bound::Unbounded, Bound::Unbounded) {
            Ok((_, entry)) => {
                if entry.as_key().borrow().eq(key) {
                    self.fetch(entry, false /*shallow*/, versions)
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
                self.build_fwd(self.to_root().unwrap(), &mut mzs)?;
                false
            }
            Bound::Included(key) => {
                let entry = self.build(key, &mut mzs)?;
                match key.cmp(entry.as_key().borrow()) {
                    cmp::Ordering::Greater => true,
                    _ => false,
                }
            }
            Bound::Excluded(key) => {
                let entry = self.build(key, &mut mzs)?;
                match key.cmp(entry.as_key().borrow()) {
                    cmp::Ordering::Equal | cmp::Ordering::Greater => true,
                    _ => false,
                }
            }
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
                self.build_rev(self.to_root().unwrap(), &mut mzs)?;
                false
            }
            Bound::Included(key) => {
                let entry = self.build(&key, &mut mzs)?;
                match key.cmp(entry.as_key().borrow()) {
                    cmp::Ordering::Less => true,
                    _ => false,
                }
            }
            Bound::Excluded(key) => {
                let entry = self.build(&key, &mut mzs)?;
                match key.cmp(entry.as_key().borrow()) {
                    cmp::Ordering::Less | cmp::Ordering::Equal => true,
                    _ => false,
                }
            }
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
                    _ => unreachable!(),
                }
            }
            Some(MZ::Z { .. }) => unreachable!(),
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
        let index: isize = (zblock.len() - 1).try_into().unwrap();
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
                        let idx: isize = (zblock.len() - 1).try_into().unwrap();
                        mzs.push(MZ::Z { zblock, index: idx });
                        Ok(())
                    }
                    Ok(MEntry::DecM { fpos: mfpos, .. }) => {
                        mzs.push(MZ::M { fpos, index });
                        self.build_rev(mfpos, mzs)?;
                        Ok(())
                    }
                    _ => unreachable!(),
                }
            }
            Some(MZ::Z { .. }) => unreachable!(),
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
        let mut fpos = self.to_root().unwrap();
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
            let index = mentry.to_index();
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
            index: index.try_into().unwrap(),
        });
        Ok(entry)
    }
}

impl<K, V, B> Snapshot<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    fn fetch(
        &mut self,
        mut entry: Entry<K, V>,
        shallow: bool,  // fetch neither value nor deltas.
        versions: bool, // fetch deltas as well
    ) -> Result<Entry<K, V>> {
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
        Ok(entry)
    }
}

/// Iterate over [Robt] index, from beginning to end.
///
/// [Robt]: crate::robt::Robt
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
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Result<Entry<K, V>>> {
        match self.mzs.pop() {
            None => None,
            Some(mut z) => match z.next() {
                Some(Ok(entry)) => {
                    self.mzs.push(z);
                    Some(self.snap.fetch(entry, self.shallow, self.versions))
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

/// Iterate over [Robt] index, from a _lower bound_ to _upper bound_.
///
/// [Robt]: crate::robt::Robt
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
    K: Clone + Ord + Borrow<Q> + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Result<Entry<K, V>>> {
        match self.mzs.pop() {
            None => None,
            Some(mut z) => match z.next() {
                Some(Ok(entry)) => {
                    if self.till_ok(&entry) {
                        self.mzs.push(z);
                        Some(self.snap.fetch(entry, false /*shallow*/, self.versions))
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

/// Reverse iterate over [Robt] index, from an _upper bound_
/// to _lower bound_.
///
/// [Robt]: crate::robt::Robt
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
    K: Clone + Ord + Borrow<Q> + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Result<Entry<K, V>>> {
        match self.mzs.pop() {
            None => None,
            Some(mut z) => match z.next_back() {
                Some(Err(err)) => {
                    self.mzs.truncate(0);
                    Some(Err(err))
                }
                Some(Ok(entry)) => {
                    if self.till_ok(&entry) {
                        self.mzs.push(z);
                        Some(self.snap.fetch(entry, false /*shallow*/, self.versions))
                    } else {
                        self.mzs.truncate(0);
                        None
                    }
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
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
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
            MZ::M { .. } => unreachable!(),
        }
    }
}

impl<K, V> DoubleEndedIterator for MZ<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
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
            MZ::M { .. } => unreachable!(),
        }
    }
}

fn purge_file(
    file: ffi::OsString,
    files: &mut Vec<ffi::OsString>,
    efiles: &mut Vec<ffi::OsString>,
) -> &'static str {
    let res = match util::open_file_r(&file) {
        Ok(fd) => match fd.try_lock_exclusive() {
            Ok(_) => {
                let res = match fs::remove_file(&file) {
                    Err(err) => ("error", format!("remove_file {:?} {:?}", file, err)),
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
            info!(target: "robtpr", "{}", msg);
            "ok"
        }
        ("locked", msg) => {
            info!(target: "robtpr", "{}", msg);
            files.push(file);
            "locked"
        }
        ("error", msg) => {
            error!(target: "robtpr", "{}", msg);
            efiles.push(file);
            "error"
        }
        _ => unreachable!(),
    }
}

fn purger(name: String, rx: mpsc::Receiver<ffi::OsString>) {
    info!(target: "robtpr", "{:?}, starting purger ...", name);

    let mut files = vec![];
    let mut efiles = vec![];

    loop {
        match rx.try_recv() {
            Err(mpsc::TryRecvError::Empty) => (),
            Err(mpsc::TryRecvError::Disconnected) => break,
            Ok(file) => {
                purge_file(file.clone(), &mut files, &mut efiles);
            }
        }
        for file in files.drain(..).collect::<Vec<ffi::OsString>>() {
            purge_file(file.clone(), &mut files, &mut efiles);
        }
        if efiles.len() > 0 {
            error!(target: "robtpr", "{:?}, failed purging {} files", name, efiles.len());
        }
        thread::sleep(time::Duration::from_secs(1));
    }

    for file in files.drain(..).collect::<Vec<ffi::OsString>>() {
        purge_file(file.clone(), &mut files, &mut efiles);
    }
    for file in efiles.into_iter() {
        error!(target: "robtpr", "{:?}, error purging file {:?}", name, file);
    }
    info!(target: "robtpr", "{:?}, stopping purger ...", name);
}

#[cfg(test)]
#[path = "fs2_test.rs"]
mod fs2_test;
#[cfg(test)]
#[path = "robt_test.rs"]
mod robt_test;
