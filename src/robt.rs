//! Read Only BTree for disk based indexes.
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
//! * File-position for btree's root-block.
//!
//! Total length of `metadata-blocks` can be computed based on
//! `marker-length`, `stats-length`, `app-metadata-length`.
//!
//! [Config]: crate::robt::Config
//!

use fs2::FileExt;
use jsondata::{Json, Property};
use lazy_static::lazy_static;

use std::{
    borrow::Borrow,
    cmp,
    convert::TryInto,
    ffi,
    fmt::{self, Display},
    fs,
    io::Write,
    marker, mem,
    ops::{Bound, RangeBounds},
    path, result,
    str::FromStr,
    sync::mpsc,
    thread, time,
};

use crate::core::{Diff, Entry, Footprint, Result, Serialize};
use crate::core::{DiskIndexFactory, DurableIndex, IndexIter, Reader};
use crate::error::Error;
use crate::robt_entry::MEntry;
use crate::robt_index::{MBlock, ZBlock};
use crate::util;

include!("robt_marker.rs");

pub struct RobtFactory {
    config: Config,
}

pub fn robt_factory(config: Config) -> RobtFactory {
    RobtFactory { config }
}

impl<K, V> DiskIndexFactory<K, V> for RobtFactory
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    type I = Robt<K, V>;

    fn name(&self) -> String {
        "robt".to_string()
    }

    fn new(&self, dir: &ffi::OsStr, name: &str) -> Robt<K, V> {
        Robt::Build {
            dir: dir.to_os_string(),
            name: name.to_string(),
            config: self.config.clone(),

            _phantom_key: marker::PhantomData,
            _phantom_val: marker::PhantomData,
        }
    }

    fn open(
        &self,
        dir: &ffi::OsStr,
        dir_entry: fs::DirEntry, // returned by read_dir()
    ) -> Result<Robt<K, V>> {
        let file_name = dir_entry.file_name();
        let name = match Config::to_name(&file_name) {
            Some(name) => name,
            None => {
                let msg = format!("file name {:?} is not for robt", file_name);
                return Err(Error::InvalidFile(msg));
            }
        };
        let snapshot = Snapshot::<K, V>::open(dir, &name)?;
        Ok(Robt::Snapshot {
            dir: dir.to_os_string(),
            name: name.to_string(),
            footprint: snapshot.footprint()?,
            meta: snapshot.meta.clone(),
            config: snapshot.config.clone(),
        })
    }
}

pub enum Robt<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    Build {
        dir: ffi::OsString,
        name: String,
        config: Config,

        _phantom_key: marker::PhantomData<K>,
        _phantom_val: marker::PhantomData<V>,
    },
    Snapshot {
        dir: ffi::OsString,
        name: String,
        footprint: isize,
        meta: Vec<MetaItem>,
        config: Config,
    },
}

impl<K, V> DurableIndex<K, V> for Robt<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    type R = Snapshot<K, V>;
    type C = PrepareCompact;

    fn to_name(&self) -> String {
        match self {
            Robt::Snapshot { name, .. } => name.clone(),
            _ => unreachable!(),
        }
    }

    fn commit(&mut self, iter: IndexIter<K, V>, meta: Vec<u8>) -> Result<()> {
        match self {
            Robt::Build {
                dir, name, config, ..
            } => {
                let b = Builder::<K, V>::commit(dir, name, config.clone())?;
                b.build(iter, meta)?;
                let snapshot = Snapshot::<K, V>::open(dir, &name)?;
                *self = Robt::Snapshot {
                    dir: dir.clone(),
                    name: name.clone(),
                    footprint: snapshot.footprint()?,
                    meta: snapshot.meta.clone(),
                    config: snapshot.config.clone(),
                };
                Ok(())
            }
            Robt::Snapshot { .. } => panic!("cannot commit into open snapshot"),
        }
    }

    fn prepare_compact(&self) -> Self::C {
        match self {
            Robt::Snapshot {
                dir,
                name,
                meta,
                config,
                ..
            } => PrepareCompact {
                dir: dir.clone(),
                name: name.clone(),
                meta: meta.clone(),
                config: config.clone(),
            },
            Robt::Build { .. } => panic!("cannot prepare commit on build robt"),
        }
    }

    fn compact(
        &mut self,
        iter: IndexIter<K, V>,
        meta: Vec<u8>,
        prepare: Self::C, // obtained from the snapshot wishing to be compacted.
    ) -> Result<()> {
        match self {
            Robt::Build {
                dir, name, config, ..
            } => {
                let config = match prepare.config.vlog_file {
                    Some(vlog_file) => {
                        config.set_value_log(Some(vlog_file));
                        config
                    }
                    None => config,
                };
                let b = Builder::compact(dir, name, config.clone())?;
                b.build(iter, meta)?;
                let snapshot = Snapshot::<K, V>::open(dir, &name)?;
                *self = Robt::Snapshot {
                    dir: dir.clone(),
                    name: name.clone(),
                    footprint: snapshot.footprint()?,
                    meta: snapshot.meta.clone(),
                    config: snapshot.config.clone(),
                };
                Ok(())
            }
            Robt::Snapshot { .. } => panic!("cannot compact an open snapshot"),
        }
    }

    fn to_reader(&mut self) -> Result<Self::R> {
        match self {
            Robt::Snapshot { dir, name, .. } => Snapshot::open(dir, &name),
            Robt::Build { .. } => panic!("cannot create a reader"),
        }
    }
}

impl<K, V> Footprint for Robt<K, V>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize + Footprint,
{
    fn footprint(&self) -> Result<isize> {
        match self {
            Robt::Snapshot { footprint, .. } => Ok(*footprint),
            _ => unreachable!(),
        }
    }
}

pub struct PrepareCompact {
    dir: ffi::OsString,
    name: String,
    meta: Vec<MetaItem>,
    config: Config,
}

/// Configuration options for Read Only BTree.
#[derive(Clone)]
pub struct Config {
    /// Leaf block size in btree index.
    /// Default: Config::ZBLOCKSIZE
    pub z_blocksize: usize,
    /// Intemediate block size in btree index.
    /// Default: Config::MBLOCKSIZE
    pub m_blocksize: usize,
    /// If deltas are indexed and/or value to be stored in separate log file.
    /// Default: Config::VBLOCKSIZE
    pub v_blocksize: usize,
    /// Include delta as part of entry. Note that delta values are always
    /// stored in separate value-log file.
    /// Default: true
    pub delta_ok: bool,
    /// Optional name for value log file. If not supplied, but `delta_ok` or
    /// `value_in_vlog` is true, then value log file name will be computed
    /// based on configuration`name` and `dir`. Default: None
    pub vlog_file: Option<ffi::OsString>,
    /// If true, then value shall be persisted in value log file. Otherwise
    /// value shall be saved in the index' leaf node. Default: false
    pub value_in_vlog: bool,
    /// Flush queue size. Default: Config::FLUSH_QUEUE_SIZE
    pub flush_queue_size: usize,
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
        Config {
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

impl From<Stats> for Config {
    fn from(stats: Stats) -> Config {
        Config {
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
    /// Default value for z-block-size, 4 * 1024 bytes.
    pub const ZBLOCKSIZE: usize = 4 * 1024; // 4KB leaf node
    /// Default value for m-block-size, 4 * 1024 bytes.
    pub const MBLOCKSIZE: usize = 4 * 1024; // 4KB intermediate node
    /// Default value for v-block-size, 4 * 1024 bytes.
    pub const VBLOCKSIZE: usize = 4 * 1024; // 4KB of blobs.
    const MARKER_BLOCK_SIZE: usize = 1024 * 4;
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
    pub fn set_delta(&mut self, vlog_file: Option<ffi::OsString>) -> &mut Self {
        match vlog_file {
            Some(vlog_file) => {
                self.delta_ok = true;
                self.vlog_file = Some(vlog_file);
            }
            None => {
                self.delta_ok = false;
            }
        }
        self
    }

    /// Persist values in a separate file, called value-log file. To persist
    /// values along with leaf node, pass `vlog_file` as None.
    pub fn set_value_log(&mut self, file: Option<ffi::OsString>) -> &mut Self {
        match file {
            Some(vlog_file) => {
                self.value_in_vlog = true;
                self.vlog_file = Some(vlog_file);
            }
            None => {
                self.value_in_vlog = false;
            }
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

impl Config {
    fn make_index_file(name: &str) -> String {
        format!("robt-{}.indx", name)
    }

    fn make_vlog_file(name: &str) -> String {
        format!("robt-{}.vlog", name)
    }

    fn to_name(file_name: &ffi::OsStr) -> Option<String> {
        let stem = match path::Path::new(file_name).extension() {
            Some(ext) if ext.to_str() == Some("indx") => {
                // ignore the dir-path and the extension, just the file-stem
                path::Path::new(file_name).file_stem()
            }
            Some(_) | None => None,
        }?;

        let stem = stem.to_str().unwrap();
        if &stem[..5] == "robt-" {
            Some(stem[5..].to_string())
        } else {
            None
        }
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

/// Enumerated meta types stored in [Robt] index.
///
/// [Robt] index is a fully packed immutable [Btree] index. To interpret
/// the index a list of meta items are appended to the tip
/// of index-file.
///
/// [Robt]: crate::robt::Robt
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
    hdr.resize(32, 0);

    for (i, item) in items.into_iter().enumerate() {
        match (i, item) {
            (0, MetaItem::Root(fpos)) => {
                hdr[0..8].copy_from_slice(&fpos.to_be_bytes());
            }
            (1, MetaItem::AppMetadata(md)) => {
                hdr[8..16].copy_from_slice(&(md.len() as u64).to_be_bytes());
                block.extend_from_slice(&md);
            }
            (2, MetaItem::Stats(s)) => {
                hdr[16..24].copy_from_slice(&(s.len() as u64).to_be_bytes());
                block.extend_from_slice(s.as_bytes());
            }
            (3, MetaItem::Marker(data)) => {
                hdr[24..32].copy_from_slice(&(data.len() as u64).to_be_bytes());
                block.extend_from_slice(&data);
            }
            (i, _) => panic!("unreachable arm at {}", i),
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
        let msg = format!("write_meta_items: {:?} {}/{}...", &file, ln, n);
        Err(Error::PartialWrite(msg))
    }
}

/// Read meta items from [Robt] index file.
///
/// Meta-items is stored at the tip of the index file. If successful,
/// a vector of meta items is returned. To learn more about the meta items
/// refer to [MetaItem] type.
///
/// [Robt]: crate::robt::Robt
pub fn read_meta_items(
    dir: &ffi::OsStr, // directory of index, can be os-native string
    name: &str,       // name of index, must be utf8 string
) -> Result<Vec<MetaItem>> {
    let index_file = Config::stitch_index_file(dir, name);
    let m = fs::metadata(&index_file)?.len();
    let mut fd = util::open_file_r(index_file.as_ref())?;

    // read header
    let hdr = util::read_buffer(&mut fd, m - 32, 32, "read root-block header")?;
    let root = u64::from_be_bytes(hdr[..8].try_into().unwrap());
    let n_md = u64::from_be_bytes(hdr[8..16].try_into().unwrap()) as usize;
    let n_stats = u64::from_be_bytes(hdr[16..24].try_into().unwrap()) as usize;
    let n_marker = u64::from_be_bytes(hdr[24..32].try_into().unwrap()) as usize;
    // read block
    let n = Config::compute_root_block(n_stats + n_md + n_marker + 32)
        .try_into()
        .unwrap();
    let block: Vec<u8> = util::read_buffer(&mut fd, m - n, n, "read root-block")?
        .into_iter()
        .collect();

    let mut meta_items: Vec<MetaItem> = vec![];
    let z = (n as usize) - 32;

    let (x, y) = (z - n_marker, z);
    let marker = block[x..y].to_vec();
    if marker.ne(&ROOT_MARKER.as_slice()) {
        let msg = format!("unexpected marker {:?}", marker);
        return Err(Error::InvalidSnapshot(msg));
    }

    let (x, y) = (z - n_marker - n_stats, z - n_marker);
    let stats = std::str::from_utf8(&block[x..y])?.to_string();

    let (x, y) = (z - n_marker - n_stats - n_md, z - n_marker - n_stats);
    let app_data = block[x..y].to_vec();

    meta_items.push(MetaItem::Root(root));
    meta_items.push(MetaItem::AppMetadata(app_data));
    meta_items.push(MetaItem::Stats(stats.clone()));
    meta_items.push(MetaItem::Marker(marker.clone()));

    // validate and return
    let stats: Stats = stats.parse()?;
    let at = m - n - (stats.m_blocksize as u64);
    if at != root {
        let msg = format!("expected root at {}, found {}", at, root);
        Err(Error::InvalidSnapshot(msg))
    } else {
        Ok(meta_items)
    }
}

impl fmt::Display for MetaItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            MetaItem::Marker(_) => write!(f, "MetaItem::Marker"),
            MetaItem::AppMetadata(_) => write!(f, "MetaItem::AppMetadata"),
            MetaItem::Stats(_) => write!(f, "MetaItem::Stats"),
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

    /// Time take to build this btree.
    pub build_time: u64,
    /// Timestamp for this index.
    pub epoch: i128,
}

impl From<Config> for Stats {
    fn from(config: Config) -> Stats {
        Stats {
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
        let s = js.get("/vlog_file")?.string().unwrap();
        let vlog_file: Option<ffi::OsString> = match s {
            s if s.len() == 0 => None,
            s => Some(s.into()),
        };

        Ok(Stats {
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
            padding: to_usize("/padding")?,
            n_abytes: to_usize("/n_abytes")?,

            build_time: to_u64("/build_time")?,
            epoch: js.get("/epoch")?.integer().unwrap(),
        })
    }
}

impl Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        let mut js = Json::new::<Vec<Property>>(vec![]);

        let vlog_file = self.vlog_file.clone().unwrap_or(Default::default());
        let vlog_file = match vlog_file.into_string() {
            Ok(vlog_file) => vlog_file,
            Err(err) => panic!(err), // TODO: will is explode in production ??
        };

        js.set("/z_blocksize", Json::new(self.z_blocksize)).ok();
        js.set("/m_blocksize", Json::new(self.m_blocksize)).ok();
        js.set("/v_blocksize", Json::new(self.v_blocksize)).ok();
        js.set("/delta_ok", Json::new(self.delta_ok)).ok();
        js.set("/vlog_file", Json::new(vlog_file)).ok();
        js.set("/value_in_vlog", Json::new(self.value_in_vlog)).ok();

        js.set("/n_count", Json::new(self.n_count)).ok();
        js.set("/n_deleted", Json::new(self.n_deleted)).ok();
        js.set("/seqno", Json::new(self.seqno)).ok();
        js.set("/key_mem", Json::new(self.key_mem)).ok();
        js.set("/diff_mem", Json::new(self.diff_mem)).ok();
        js.set("/val_mem", Json::new(self.val_mem)).ok();
        js.set("/z_bytes", Json::new(self.z_bytes)).ok();
        js.set("/v_bytes", Json::new(self.v_bytes)).ok();
        js.set("/m_bytes", Json::new(self.m_bytes)).ok();
        js.set("/padding", Json::new(self.padding)).ok();
        js.set("/n_abytes", Json::new(self.n_abytes)).ok();

        js.set("/build_time", Json::new(self.build_time)).ok();
        js.set("/epoch", Json::new(self.epoch)).ok();

        write!(f, "{}", js.to_string())
    }
}

/// Builder type for constructing Read-Only-BTree index from an iterator.
///
/// Index can be built in _initial_ mode or _incremental_ mode. Refer
/// to corresponding methods for more information.
pub struct Builder<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    config: Config,
    iflusher: Flusher,
    vflusher: Option<Flusher>,
    stats: Stats,

    phantom_key: marker::PhantomData<K>,
    phantom_val: marker::PhantomData<V>,
}

impl<K, V> Builder<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    /// For commit builds, index file and value-log-file, if configured,
    /// shall be created new.
    pub fn commit(
        dir: &ffi::OsStr, // directory path where index file(s) are stored
        name: &str,
        mut config: Config, //  TODO: Bit of ugliness here
    ) -> Result<Builder<K, V>> {
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
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        })
    }

    /// For compact build, index file is created new, while
    /// value-log-file, if configured, shall be appended to older version.
    pub fn compact(
        dir: &ffi::OsStr, // directory path where index files are stored
        name: &str,
        mut config: Config, //  TODO: Bit of ugliness here
    ) -> Result<Builder<K, V>> {
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
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        })
    }

    /// Build a new index from the supplied iterator. The iterator shall
    /// return an index entry for each iteration, and the entries are
    /// expected in sort order.
    pub fn build<I>(mut self, iter: I, app_meta: Vec<u8>) -> Result<()>
    where
        I: Iterator<Item = Result<Entry<K, V>>>,
    {
        let (took, root): (u64, u64) = {
            let start = time::SystemTime::now();
            let root = self.build_tree(iter)?;
            (
                start.elapsed().unwrap().as_nanos().try_into().unwrap(),
                root,
            )
        };

        // meta-stats
        let stats: String = {
            self.stats.build_time = took;
            let epoch: i128 = time::UNIX_EPOCH
                .elapsed()
                .unwrap()
                .as_nanos()
                .try_into()
                .unwrap();
            self.stats.epoch = epoch;
            self.stats.to_string()
        };

        // start building metadata items for index files
        let meta_items: Vec<MetaItem> = vec![
            MetaItem::Root(root),
            MetaItem::AppMetadata(app_meta),
            MetaItem::Stats(stats),
            MetaItem::Marker(ROOT_MARKER.clone()), // tip of the index.
        ];

        let index_file: ffi::OsString = self.iflusher.file.clone();

        // flush blocks and close
        self.iflusher.close_wait()?;
        self.vflusher.take().map(|x| x.close_wait()).transpose()?;

        // flush meta items to disk and close
        write_meta_items(index_file, meta_items)?;

        Ok(())
    }

    fn build_tree<I>(&mut self, iter: I) -> Result<u64>
    where
        I: Iterator<Item = Result<Entry<K, V>>>,
    {
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
            let mut entry = match self.preprocess(entry?) {
                Some(entry) => entry,
                None => continue,
            };

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

            self.postprocess(&mut entry);
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
            return Err(Error::EmptyIterator);
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

    fn preprocess(&mut self, entry: Entry<K, V>) -> Option<Entry<K, V>> {
        self.stats.seqno = cmp::max(self.stats.seqno, entry.to_seqno());
        Some(entry)
    }

    fn postprocess(&mut self, entry: &mut Entry<K, V>) {
        self.stats.n_count += 1;
        if entry.is_deleted() {
            self.stats.n_deleted += 1;
        }
    }
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
            Ok(Err(_)) => unreachable!(),
            Err(err) => match err.downcast_ref::<String>() {
                Some(msg) => Err(Error::ThreadFail(msg.to_string())),
                None => Err(Error::ThreadFail("unknown error".to_string())),
            },
        }
    }
}

fn thread_flush(
    file: ffi::OsString, // for debuging purpose
    mut fd: fs::File,
    rx: mpsc::Receiver<Vec<u8>>,
) -> Result<()> {
    fd.lock_exclusive(); // <----- write lock
                         // let mut fpos = 0;
    for data in rx {
        // println!("flusher {:?} {} {}", file, fpos, data.len());
        // fpos += data.len();
        let n = fd.write(&data)?;
        if n != data.len() {
            let msg = format!("flusher: {:?} {}/{}...", &file, data.len(), n);
            fd.unlock(); // <----- write un-lock
            return Err(Error::PartialWrite(msg));
        }
    }
    fd.sync_all()?;
    // file descriptor and receiver channel shall be dropped.
    fd.unlock(); // <----- write un-lock
    Ok(())
}

/// A read only snapshot of BTree built using [robt] index.
///
/// [robt]: crate::robt
pub struct Snapshot<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    dir: ffi::OsString,
    name: String,
    meta: Vec<MetaItem>,
    config: Config,
    // working fields
    fd: (fs::File, Option<fs::File>),

    phantom_key: marker::PhantomData<K>,
    phantom_val: marker::PhantomData<V>,
}

impl<K, V> Drop for Snapshot<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    fn drop(&mut self) {
        self.fd.0.unlock();
        self.fd.1.as_ref().map(|fd| fd.unlock());
    }
}

// Construction methods.
impl<K, V> Snapshot<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    /// Open BTree snapshot from file that can be constructed from ``dir``
    /// and ``name``.
    pub fn open(dir: &ffi::OsStr, name: &str) -> Result<Snapshot<K, V>> {
        let meta_items = read_meta_items(dir, name)?;
        let stats: Stats = if let MetaItem::Stats(stats) = &meta_items[2] {
            Ok(stats.parse()?)
        } else {
            let msg = "snapshot statistics missing".to_string();
            Err(Error::InvalidSnapshot(msg))
        }?;
        let config: Config = stats.into();

        let vlog_file = config.vlog_file.map(|vfile| {
            // stem the file name.
            let vfile = path::Path::new(&vfile).file_name().unwrap();
            let ipath = Config::stitch_index_file(&dir, &name);

            let mut vpath = path::PathBuf::new();
            vpath.push(path::Path::new(&ipath).parent().unwrap());
            vpath.push(vfile);
            vpath.as_os_str().to_os_string()
        });
        let index_fd = {
            let index_file = Config::stitch_index_file(dir, name);
            util::open_file_r(&index_file.as_ref())?
        };
        let vlog_fd = vlog_file
            .as_ref()
            .map(|s| util::open_file_r(s.as_ref()))
            .transpose()?;

        index_fd.lock_shared();
        vlog_fd.as_ref().map(|fd| fd.lock_shared());

        let mut snap = Snapshot {
            dir: dir.to_os_string(),
            name: name.to_string(),
            meta: meta_items,
            config: Default::default(),
            fd: (index_fd, vlog_fd),

            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        };
        snap.config = snap.to_stats()?.into();

        Ok(snap) // Okey dockey
    }
}

// maintanence methods.
impl<K, V> Snapshot<K, V>
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

    /// Return the application metadata.
    pub fn to_app_meta(&self) -> Result<Vec<u8>> {
        if let MetaItem::AppMetadata(data) = &self.meta[1] {
            Ok(data.clone())
        } else {
            let msg = "snapshot app-metadata missing".to_string();
            Err(Error::InvalidSnapshot(msg))
        }
    }

    /// Return Btree statistics.
    pub fn to_stats(&self) -> Result<Stats> {
        if let MetaItem::Stats(stats) = &self.meta[2] {
            Ok(stats.parse()?)
        } else {
            let msg = "snapshot statistics missing".to_string();
            Err(Error::InvalidSnapshot(msg))
        }
    }

    /// Return the file-position for Btree's root node.
    pub fn to_root(&self) -> Result<u64> {
        if let MetaItem::Root(root) = self.meta[0] {
            Ok(root)
        } else {
            Err(Error::InvalidSnapshot("snapshot root missing".to_string()))
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

impl<K, V> Footprint for Snapshot<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    fn footprint(&self) -> Result<isize> {
        let (dir, name) = (self.dir.as_os_str(), self.name.as_str());

        let mut footprint = {
            let filen = Config::stitch_index_file(dir, name);
            fs::metadata(filen)?.len()
        };
        let vlog_file = self
            .fd
            .1
            .as_ref()
            .map(|_| Config::stitch_vlog_file(dir, name));
        footprint += match vlog_file {
            Some(vlog_file) => fs::metadata(vlog_file)?.len(),
            None => 0,
        };
        Ok(footprint.try_into().unwrap())
    }
}

// Read methods
impl<K, V> Reader<K, V> for Snapshot<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    fn get<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        // println!("robt get ..");
        let snap = unsafe { (self as *mut Snapshot<K, V>).as_mut().unwrap() };
        let versions = false;
        snap.do_get(key, versions)
    }

    fn iter(&mut self) -> Result<IndexIter<K, V>> {
        let snap = unsafe { (self as *mut Snapshot<K, V>).as_mut().unwrap() };
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
        let snap = unsafe { (self as *mut Snapshot<K, V>).as_mut().unwrap() };
        let versions = false;
        snap.do_range(range, versions)
    }

    fn reverse<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let snap = unsafe { (self as *mut Snapshot<K, V>).as_mut().unwrap() };
        let versions = false;
        snap.do_reverse(range, versions)
    }

    fn get_with_versions<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let snap = unsafe { (self as *mut Snapshot<K, V>).as_mut().unwrap() };
        let versions = true;
        snap.do_get(key, versions)
    }

    /// Iterate over all entries in this index. Returned entry shall
    /// have all its previous versions, can be a costly call.
    fn iter_with_versions(&mut self) -> Result<IndexIter<K, V>> {
        let snap = unsafe { (self as *mut Snapshot<K, V>).as_mut().unwrap() };
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
        let snap = unsafe { (self as *mut Snapshot<K, V>).as_mut().unwrap() };
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
        let snap = unsafe { (self as *mut Snapshot<K, V>).as_mut().unwrap() };
        let versions = true;
        snap.do_reverse(range, versions)
    }
}

impl<K, V> Snapshot<K, V>
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
        let mblock = {
            let fd = &mut self.fd.0;
            MBlock::<K, V>::new_decode(fd, fpos, &self.config)?
        };
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
        let zblock: ZBlock<K, V> = {
            let fd = &mut self.fd.0;
            ZBlock::new_decode(fd, zfpos, &self.config)?
        };
        match zblock.find(key, Bound::Unbounded, Bound::Unbounded) {
            Ok((_, entry)) => {
                if entry.as_key().borrow().eq(key) {
                    self.fetch(entry, versions)
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
        let fd = &mut self.fd.0;
        let config = &self.config;

        // println!("build_fwd {} {}", mzs.len(), fpos);
        let zfpos = loop {
            let mblock = MBlock::<K, V>::new_decode(fd, fpos, config)?;
            mzs.push(MZ::M { fpos, index: 0 });

            let mentry = mblock.to_entry(0)?;
            if mentry.is_zblock() {
                break mentry.to_fpos();
            }
            fpos = mentry.to_fpos();
        };
        // println!("build_fwd {}", mzs.len());

        let zblock = ZBlock::new_decode(fd, zfpos, config)?;
        mzs.push(MZ::Z { zblock, index: 0 });
        Ok(())
    }

    fn rebuild_fwd(&mut self, mzs: &mut Vec<MZ<K, V>>) -> Result<()> {
        let config = &self.config;

        match mzs.pop() {
            None => Ok(()),
            Some(MZ::M { fpos, mut index }) => {
                let mblock = {
                    let fd = &mut self.fd.0;
                    MBlock::<K, V>::new_decode(fd, fpos, config)?
                };
                index += 1;
                match mblock.to_entry(index) {
                    Ok(MEntry::DecZ { fpos: zfpos, .. }) => {
                        mzs.push(MZ::M { fpos, index });

                        let zblock = {
                            let fd = &mut self.fd.0;
                            ZBlock::new_decode(fd, zfpos, config)?
                        };
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
            let mblock = {
                let fd = &mut self.fd.0;
                MBlock::<K, V>::new_decode(fd, fpos, config)?
            };
            let index = mblock.len() - 1;
            mzs.push(MZ::M { fpos, index });

            let mentry = mblock.to_entry(index)?;
            if mentry.is_zblock() {
                break mentry.to_fpos();
            }
            fpos = mentry.to_fpos();
        };

        let zblock = {
            let fd = &mut self.fd.0;
            ZBlock::new_decode(fd, zfpos, config)?
        };
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
                let mblock = {
                    let fd = &mut self.fd.0;
                    MBlock::<K, V>::new_decode(fd, fpos, config)?
                };
                index -= 1;
                match mblock.to_entry(index) {
                    Ok(MEntry::DecZ { fpos: zfpos, .. }) => {
                        mzs.push(MZ::M { fpos, index });

                        let zblock = {
                            let fd = &mut self.fd.0;
                            ZBlock::new_decode(fd, zfpos, config)?
                        };
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
        let fd = &mut self.fd.0;
        let config = &self.config;
        let (from_min, to_max) = (Bound::Unbounded, Bound::Unbounded);

        let zfpos = loop {
            let mblock = MBlock::<K, V>::new_decode(fd, fpos, config)?;
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

        let zblock = ZBlock::new_decode(fd, zfpos, config)?;
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

    fn fetch(
        &mut self,
        mut entry: Entry<K, V>,
        versions: bool, // fetch deltas as well
    ) -> Result<Entry<K, V>> {
        match &mut self.fd.1 {
            Some(fd) => entry.fetch_value(fd)?,
            _ => (),
        }
        if versions {
            match &mut self.fd.1 {
                Some(fd) => entry.fetch_deltas(fd)?,
                _ => (),
            }
        }
        Ok(entry)
    }
}

/// Iterate over [Robt] index, from beginning to end.
///
/// [Robt]: crate::robt::Robt
pub struct Iter<'a, K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    snap: &'a mut Snapshot<K, V>,
    mzs: Vec<MZ<K, V>>,
    versions: bool,
}

impl<'a, K, V> Iter<'a, K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    fn new(snap: &'a mut Snapshot<K, V>, mzs: Vec<MZ<K, V>>) -> Box<Self> {
        Box::new(Iter {
            snap,
            mzs,
            versions: false,
        })
    }

    fn new_versions(
        snap: &'a mut Snapshot<K, V>, // reference to snapshot
        mzs: Vec<MZ<K, V>>,
    ) -> Box<Self> {
        Box::new(Iter {
            snap,
            mzs,
            versions: true,
        })
    }
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
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
                    Some(self.snap.fetch(entry, self.versions))
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
pub struct Range<'a, K, V, R, Q>
where
    K: Clone + Ord + Borrow<Q> + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    snap: &'a mut Snapshot<K, V>,
    mzs: Vec<MZ<K, V>>,
    range: R,
    high: marker::PhantomData<Q>,
    versions: bool,
}

impl<'a, K, V, R, Q> Range<'a, K, V, R, Q>
where
    K: Clone + Ord + Borrow<Q> + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    fn new(
        snap: &'a mut Snapshot<K, V>,
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

impl<'a, K, V, R, Q> Iterator for Range<'a, K, V, R, Q>
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
                        Some(self.snap.fetch(entry, self.versions))
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
pub struct Reverse<'a, K, V, R, Q>
where
    K: Clone + Ord + Borrow<Q> + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    snap: &'a mut Snapshot<K, V>,
    mzs: Vec<MZ<K, V>>,
    range: R,
    low: marker::PhantomData<Q>,
    versions: bool,
}

impl<'a, K, V, R, Q> Reverse<'a, K, V, R, Q>
where
    K: Clone + Ord + Borrow<Q> + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    fn new(
        snap: &'a mut Snapshot<K, V>,
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

impl<'a, K, V, R, Q> Iterator for Reverse<'a, K, V, R, Q>
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
                        Some(self.snap.fetch(entry, self.versions))
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

#[cfg(test)]
#[path = "fs2_test.rs"]
mod fs2_test;
#[cfg(test)]
#[path = "robt_test.rs"]
mod robt_test;
