/// Read Only BTree for disk based indexes.
///
/// ROBT instances shall have an index file and an optional value-log-file,
/// refer to [Config] for more information.
///
/// **Index-file format**:
///
/// *------------------------------------------* SeekFrom::End(0)
/// |                marker-length             |
/// *------------------------------------------* SeekFrom::End(-8)
/// |                stats-length              |
/// *------------------------------------------* SeekFrom::End(-16)
/// |               metadata-length            |
/// *------------------------------------------* SeekFrom::End(-24)
/// |                  root-fpos               |
/// *------------------------------------------* SeekFrom::MetaBlock
/// *                btree-blocks              *
/// *                    ...                   *
/// *                    ...                   *
/// *------------------------------------------* 0
///
///
/// [Config]: crate::robt_config::Config
///
use lazy_static::lazy_static;

use std::{
    convert::TryInto,
    ffi, fmt,
    fmt::Display,
    fs,
    io::Write,
    marker, mem, path, result,
    str::FromStr,
    sync::{atomic::AtomicPtr, atomic::Ordering, mpsc, Arc},
    thread,
};

pub use crate::robt_build::Builder;
pub use crate::robt_snap::Snapshot;

use crate::core::{Diff, Footprint, Index, Result, Serialize};
use crate::error::Error;
use crate::jsondata::{Json, Property};
use crate::util;

// TODO: make dir, file, path into OsString and OsStr.

include!("robt_marker.rs");

struct Levels<K, V>(AtomicPtr<Arc<Vec<Snapshot<K, V>>>>)
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize;

impl<K, V> Levels<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    fn new() -> Levels<K, V> {
        Levels(AtomicPtr::new(Box::leak(Box::new(Arc::new(vec![])))))
    }

    fn get_snapshots(&self) -> Arc<Vec<Snapshot<K, V>>> {
        unsafe { Arc::clone(self.0.load(Ordering::Relaxed).as_ref().unwrap()) }
    }

    fn compare_swap_snapshots(&self, new_snapshots: Vec<Snapshot<K, V>>) {
        let _olds = unsafe { Box::from_raw(self.0.load(Ordering::Relaxed)) };
        let new_snapshots = Box::leak(Box::new(Arc::new(new_snapshots)));
        self.0.store(new_snapshots, Ordering::Relaxed);
    }
}

pub(crate) struct Robt<K, V, M>
where
    K: 'static + Sync + Send + Clone + Ord + Serialize + Footprint,
    V: 'static + Sync + Send + Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: 'static + Sync + Send + Index<K, V>,
{
    config: Config,
    mem_ratio: f64,
    disk_ratio: f64,
    levels: Levels<K, V>,
    todisk: MemToDisk<K, V, M>,      // encapsulates a thread
    tocompact: DiskCompact<K, V, M>, // encapsulates a thread
}

// new instance of multi-level Robt indexes.
impl<K, V, M> Robt<K, V, M>
where
    K: 'static + Sync + Send + Clone + Ord + Serialize + Footprint,
    V: 'static + Sync + Send + Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: 'static + Sync + Send + Index<K, V>,
{
    const MEM_RATIO: f64 = 0.2;
    const DISK_RATIO: f64 = 0.5;

    pub(crate) fn new(config: Config) -> Robt<K, V, M> {
        Robt {
            config: config.clone(),
            mem_ratio: Self::MEM_RATIO,
            disk_ratio: Self::DISK_RATIO,
            levels: Levels::new(),
            todisk: MemToDisk::new(config.clone()),
            tocompact: DiskCompact::new(config.clone()),
        }
    }

    pub(crate) fn set_mem_ratio(mut self, ratio: f64) -> Robt<K, V, M> {
        self.mem_ratio = ratio;
        self
    }

    pub(crate) fn set_disk_ratio(mut self, ratio: f64) -> Robt<K, V, M> {
        self.disk_ratio = ratio;
        self
    }
}

// add new levels.
impl<K, V, M> Robt<K, V, M>
where
    K: 'static + Sync + Send + Clone + Ord + Serialize + Footprint,
    V: 'static + Sync + Send + Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: 'static + Sync + Send + Index<K, V>,
{
    pub(crate) fn flush_to_disk(
        &mut self,
        index: Arc<M>, // full table scan over mem-index
        metadata: Vec<u8>,
    ) -> Result<()> {
        let _resp = self.todisk.send(Request::MemFlush {
            index,
            metadata,
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        })?;
        Ok(())
    }
}

enum Request<K, V, M>
where
    K: 'static + Sync + Send + Clone + Ord + Serialize + Footprint,
    V: 'static + Sync + Send + Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: 'static + Sync + Send + Index<K, V>,
{
    MemFlush {
        index: Arc<M>,
        metadata: Vec<u8>,
        phantom_key: marker::PhantomData<K>,
        phantom_val: marker::PhantomData<V>,
    },
}

enum Response {
    Ok,
}

struct MemToDisk<K, V, M>
where
    K: 'static + Sync + Send + Clone + Ord + Serialize + Footprint,
    V: 'static + Sync + Send + Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: 'static + Sync + Send + Index<K, V>,
{
    config: Config,
    thread: thread::JoinHandle<Result<()>>,
    tx: mpsc::SyncSender<(Request<K, V, M>, mpsc::SyncSender<Response>)>,
}

impl<K, V, M> MemToDisk<K, V, M>
where
    K: 'static + Sync + Send + Clone + Ord + Serialize + Footprint,
    V: 'static + Sync + Send + Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: 'static + Sync + Send + Index<K, V>,
{
    fn new(config: Config) -> MemToDisk<K, V, M> {
        let (tx, rx) = mpsc::sync_channel(1);
        let conf = config.clone();
        let thread = thread::spawn(move || thread_mem_to_disk(conf, rx));
        MemToDisk { config, thread, tx }
    }

    fn send(&mut self, req: Request<K, V, M>) -> Result<Response> {
        let (tx, rx) = mpsc::sync_channel(0);
        self.tx.send((req, tx))?;
        Ok(rx.recv()?)
    }

    fn close_wait(self) -> Result<()> {
        mem::drop(self.tx);
        match self.thread.join() {
            Ok(res) => res,
            Err(err) => match err.downcast_ref::<String>() {
                Some(msg) => Err(Error::ThreadFail(msg.to_string())),
                None => Err(Error::ThreadFail("unknown error".to_string())),
            },
        }
    }
}

fn thread_mem_to_disk<K, V, M>(
    _config: Config,
    _rx: mpsc::Receiver<(Request<K, V, M>, mpsc::SyncSender<Response>)>,
) -> Result<()>
where
    K: 'static + Sync + Send + Clone + Ord + Serialize + Footprint,
    V: 'static + Sync + Send + Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: 'static + Sync + Send + Index<K, V>,
{
    // TBD
    Ok(())
}

struct DiskCompact<K, V, M>
where
    K: 'static + Sync + Send + Clone + Ord + Serialize + Footprint,
    V: 'static + Sync + Send + Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: 'static + Sync + Send + Index<K, V>,
{
    config: Config,
    thread: thread::JoinHandle<Result<()>>,
    tx: mpsc::SyncSender<(Request<K, V, M>, mpsc::SyncSender<Response>)>,
}

impl<K, V, M> DiskCompact<K, V, M>
where
    K: 'static + Sync + Send + Clone + Ord + Serialize + Footprint,
    V: 'static + Sync + Send + Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: 'static + Sync + Send + Index<K, V>,
{
    fn new(config: Config) -> DiskCompact<K, V, M> {
        let (tx, rx) = mpsc::sync_channel(1);
        let conf = config.clone();
        let thread = thread::spawn(move || thread_disk_compact(conf, rx));
        DiskCompact { config, thread, tx }
    }

    fn send(&mut self, req: Request<K, V, M>) -> Result<Response> {
        let (tx, rx) = mpsc::sync_channel(0);
        self.tx.send((req, tx))?;
        Ok(rx.recv()?)
    }

    fn close_wait(self) -> Result<()> {
        mem::drop(self.tx);
        match self.thread.join() {
            Ok(res) => res,
            Err(err) => match err.downcast_ref::<String>() {
                Some(msg) => Err(Error::ThreadFail(msg.to_string())),
                None => Err(Error::ThreadFail("unknown error".to_string())),
            },
        }
    }
}

fn thread_disk_compact<K, V, M>(
    _config: Config,
    _rx: mpsc::Receiver<(Request<K, V, M>, mpsc::SyncSender<Response>)>,
) -> Result<()>
where
    K: 'static + Sync + Send + Clone + Ord + Serialize + Footprint,
    V: 'static + Sync + Send + Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: 'static + Sync + Send + Index<K, V>,
{
    // TBD
    Ok(())
}

/// Configuration options for Read Only BTree.
#[derive(Clone)]
pub struct Config {
    pub z_blocksize: usize,
    /// Intemediate block size in btree index.
    pub m_blocksize: usize,
    /// If deltas are indexed and/or value to be stored in separate log file.
    pub v_blocksize: usize,
    /// Tombstone purge. For LSM based index older entries can quickly bloat
    /// system. To avoid this, it is a good idea to purge older versions of
    /// an entry that are seen by all participating entities. When configured
    /// with `Some(seqno)`, all iterated entries/versions whose seqno is ``<=``
    /// purge seqno shall be removed totally from the index.
    pub tomb_purge: Option<u64>,
    /// Include delta as part of entry. Note that delta values are always
    /// stored in separate value-log file.
    pub delta_ok: bool,
    /// Optional name for value log file. If not supplied, but `delta_ok` or
    /// `value_in_vlog` is true, then value log file name will be computed
    /// based on configuration`name` and `dir`.
    pub vlog_file: Option<ffi::OsString>,
    /// If true, then value shall be persisted in value log file. Otherwise
    /// value shall be saved in the index' leaf node.
    pub value_in_vlog: bool,
    /// Flush queue size.
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
            tomb_purge: Default::default(),
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
            tomb_purge: Default::default(),
            delta_ok: stats.delta_ok,
            vlog_file: stats.vlog_file,
            value_in_vlog: stats.value_in_vlog,
            flush_queue_size: Self::FLUSH_QUEUE_SIZE,
        }
    }
}

impl Config {
    pub const ZBLOCKSIZE: usize = 4 * 1024; // 4KB leaf node
    pub const VBLOCKSIZE: usize = 4 * 1024; // ~ 4KB of blobs.
    pub const MBLOCKSIZE: usize = 4 * 1024; // 4KB intermediate node
    const MARKER_BLOCK_SIZE: usize = 1024 * 4;
    const FLUSH_QUEUE_SIZE: usize = 64;

    /// Configure differt set of block size for leaf-node, intermediate-node.
    pub fn set_blocksize(&mut self, z: usize, v: usize, m: usize) -> &mut Self {
        self.z_blocksize = z;
        self.v_blocksize = v;
        self.m_blocksize = m;
        self
    }

    /// Enable tombstone purge. Deltas and values with sequence number less
    /// than `before` shall be purged.
    pub fn set_tombstone_purge(&mut self, before: u64) -> &mut Self {
        self.tomb_purge = Some(before);
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
    pub(crate) fn stitch_index_file(dir: &str, name: &str) -> ffi::OsString {
        let mut index_file = path::PathBuf::from(dir);
        index_file.push(format!("robt-{}.indx", name));
        let index_file: &ffi::OsStr = index_file.as_ref();
        index_file.to_os_string()
    }

    pub(crate) fn stitch_vlog_file(dir: &str, name: &str) -> ffi::OsString {
        let mut vlog_file = path::PathBuf::from(dir);
        vlog_file.push(format!("robt-{}.vlog", name));
        let vlog_file: &ffi::OsStr = vlog_file.as_ref();
        vlog_file.to_os_string()
    }

    pub(crate) fn compute_root_block(n: usize) -> usize {
        if (n % Config::MARKER_BLOCK_SIZE) == 0 {
            n
        } else {
            ((n / Config::MARKER_BLOCK_SIZE) + 1) * Config::MARKER_BLOCK_SIZE
        }
    }

    /// Return the index file under configured directory.
    pub fn to_index_file(&self, dir: &str, name: &str) -> ffi::OsString {
        Self::stitch_index_file(&dir, &name)
    }

    /// Return the value-log file, if enabled, under configured directory.
    pub fn to_value_log(&self, dir: &str, name: &str) -> Option<ffi::OsString> {
        match &self.vlog_file {
            Some(file) => Some(file.clone()),
            None => Some(Self::stitch_vlog_file(&dir, &name)),
        }
    }
}

/// Enumerated variants of meta-data items stored in [Robt] index.
///
/// [Robt] index is a full-packed immutable [Btree] index. To interpret
/// the index a list of metadata items are appended to the tip
/// of index-file.
///
/// [Robt]: crate::robt::Robt
/// [Btree]: https://en.wikipedia.org/wiki/B-tree
pub enum MetaItem {
    /// A Unique marker that confirms that index file is valid.
    Marker(Vec<u8>), // tip of the file.
    /// Contains index-statistics along with configuration values.
    Stats(String),
    /// Application supplied metadata, typically serialized and opaque
    /// to [Bogn].
    ///
    /// [Bogn]: crate::Bogn
    Metadata(Vec<u8>),
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
            (1, MetaItem::Metadata(md)) => {
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
    if n == ln {
        Ok(n.try_into().unwrap())
    } else {
        let msg = format!("write_meta_items: {:?} {}/{}...", &file, ln, n);
        Err(Error::PartialWrite(msg))
    }
}

/// Read meta data from [Robt] index file.
///
/// Metadata is stored at the tip of the index file. If successful,
/// a vector of metadata components. To learn more about the metadata
/// components refer to [MetaItem] type.
///
/// [Robt]: crate::robt::Robt
pub fn read_meta_items(
    dir: &str,  // directory of index
    name: &str, // name of index
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

    let mut metaitems: Vec<MetaItem> = vec![];
    let z = (n as usize) - 32;

    let (x, y) = (z - n_marker, z);
    let marker = block[x..y].to_vec();
    if marker.ne(&ROOT_MARKER.as_slice()) {
        let msg = format!("unexpected marker at {:?}", marker);
        return Err(Error::InvalidSnapshot(msg));
    }

    let (x, y) = (z - n_marker - n_stats, z - n_marker);
    let stats = std::str::from_utf8(&block[x..y])?.to_string();

    let (x, y) = (z - n_marker - n_stats - n_md, z - n_marker - n_stats);
    let meta_data = block[x..y].to_vec();

    metaitems.push(MetaItem::Root(root));
    metaitems.push(MetaItem::Metadata(meta_data));
    metaitems.push(MetaItem::Stats(stats));
    metaitems.push(MetaItem::Marker(marker.clone()));

    // validate and return
    if (m - n) != root {
        let msg = format!("expected root at {}, found {}", root, (m - n));
        Err(Error::InvalidSnapshot(msg))
    } else {
        Ok(metaitems)
    }
}

impl fmt::Display for MetaItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            MetaItem::Marker(_) => write!(f, "MetaItem::Marker"),
            MetaItem::Metadata(_) => write!(f, "MetaItem::Metadata"),
            MetaItem::Stats(_) => write!(f, "MetaItem::Stats"),
            MetaItem::Root(_) => write!(f, "MetaItem::Root"),
        }
    }
}

#[derive(Clone, Default, PartialEq)]
pub struct Stats {
    pub z_blocksize: usize,
    pub m_blocksize: usize,
    pub v_blocksize: usize,
    pub delta_ok: bool,
    pub vlog_file: Option<ffi::OsString>,
    pub value_in_vlog: bool,

    pub n_count: u64,
    pub n_deleted: usize,
    pub seqno: u64,
    pub key_mem: usize,
    pub diff_mem: usize,
    pub val_mem: usize,
    pub z_bytes: usize,
    pub v_bytes: usize,
    pub m_bytes: usize,
    pub padding: usize,
    pub n_abytes: usize,

    pub build_time: u64,
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

#[cfg(test)]
#[path = "robt_test.rs"]
mod robt_test;
