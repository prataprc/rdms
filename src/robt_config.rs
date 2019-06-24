use std::{convert::TryInto, fmt, fs, path};

use lazy_static::lazy_static;

use crate::error::Error;
use crate::robt_build::Flusher;
use crate::robt_stats::Stats;
use crate::util;

lazy_static! {
    pub static ref MARKER_BLOCK: Vec<u8> = {
        let mut block: Vec<u8> = Vec::with_capacity(Config::MARKER_BLOCK_SIZE);
        block.resize(Config::MARKER_BLOCK_SIZE, Config::MARKER_BYTE);
        block
    };
}

/// Configuration to build read-only btree.
#[derive(Clone)]
pub struct Config {
    /// Directory where index file(s) shall be stored.
    pub dir: String,
    /// Name of the index file(s) under `dir`.
    pub name: String,
    /// Leaf block size in btree index.
    pub z_blocksize: usize,
    /// Intemediate block size in btree index.
    pub m_blocksize: usize,
    /// If deltas are indexed and/or value to be stored in separate log file.
    pub v_blocksize: usize,
    /// Tombstone purge. For LSM based index older entries can quickly bloat
    /// system. To avoid this, it is a good idea to purge older versions of
    /// an entry which doesn't matter any more. When configured with
    /// `Some(seqno)`, all iterated entries, whose seqno is older than
    /// configured seqno, shall be ignored.
    pub tomb_purge: Option<u64>,
    /// Include delta as part of entry. Note that delta values are always
    /// stored in separate value-log file.
    pub delta_ok: bool,
    /// Optional name for value log file. If not supplied, but `delta_ok` or
    /// `value_in_vlog` is true, then value log file name will be computed
    /// based on configuration`name` and `dir`.
    pub vlog_file: Option<String>,
    /// If true, then value shall be persisted in value log file. Otherwise
    /// value shall be saved in the index' leaf node.
    pub value_in_vlog: bool,
}

impl From<Stats> for Config {
    fn from(stats: Stats) -> Config {
        Config {
            dir: Default::default(),
            name: stats.name,
            z_blocksize: stats.zblocksize,
            m_blocksize: stats.mblocksize,
            v_blocksize: stats.vblocksize,
            tomb_purge: Default::default(),
            delta_ok: stats.delta_ok,
            vlog_file: stats.vlog_file,
            value_in_vlog: stats.value_in_vlog,
        }
    }
}

impl Config {
    pub const ZBLOCKSIZE: usize = 4 * 1024; // 4KB leaf node
    pub const MBLOCKSIZE: usize = 4 * 1024; // 4KB intermediate node
    pub const VBLOCKSIZE: usize = 4 * 1024; // ~ 4KB of blobs.
    const MARKER_BLOCK_SIZE: usize = 1024 * 4;
    const MARKER_BYTE: u8 = 0xAB;

    pub(crate) fn stitch_index_file(dir: &str, name: &str) -> String {
        let mut index_file = path::PathBuf::from(dir);
        index_file.push(format!("robt-{}-shard1.indx", name));
        index_file.to_str().unwrap().to_string()
    }

    pub(crate) fn stitch_vlog_file(dir: &str, name: &str) -> String {
        let mut vlog_file = path::PathBuf::from(dir);
        vlog_file.push(format!("robt-{}-shard1.vlog", name));
        vlog_file.to_str().unwrap().to_string()
    }

    pub(crate) fn compute_metadata_len(n: usize) -> usize {
        let n_blocks = ((n + 8) / Config::MARKER_BLOCK_SIZE) + 1;
        n_blocks * Config::MARKER_BLOCK_SIZE
    }

    /// Return the index file under configured directory.
    pub fn to_index_file(&self) -> String {
        Self::stitch_index_file(&self.dir, &self.name)
    }

    /// Return the value-log file, if enabled, under configured directory.
    pub fn to_value_log(&self) -> Option<String> {
        match &self.vlog_file {
            Some(file) => Some(file.clone()),
            None => Some(Self::stitch_vlog_file(&self.dir, &self.name)),
        }
    }

    /// New configuration with default parameters:
    ///
    /// * With ZBLOCKSIZE, MBLOCKSIZE, VBLOCKSIZE.
    /// * Values are stored in the leaf node.
    /// * LSM entries are preserved.
    /// * Deltas are persisted in default value-log-file.
    /// * Main index is persisted in default index-file.
    pub fn new(dir: &str, name: &str) -> Config {
        Config {
            dir: dir.to_string(),
            name: name.to_string(),
            z_blocksize: Self::ZBLOCKSIZE,
            v_blocksize: Self::VBLOCKSIZE,
            m_blocksize: Self::MBLOCKSIZE,
            tomb_purge: Default::default(),
            delta_ok: true,
            vlog_file: Default::default(),
            value_in_vlog: false,
        }
    }

    /// Configure differt set of block size for leaf-node, intermediate-node.
    pub fn set_blocksize(mut self, m: usize, z: usize, v: usize) -> Config {
        self.m_blocksize = m;
        self.z_blocksize = z;
        self.v_blocksize = v;
        self
    }

    /// Enable tombstone purge. Deltas and values with sequence number less
    /// than `before` shall be purged.
    pub fn set_tombstone_purge(mut self, before: u64) -> Config {
        self.tomb_purge = Some(before);
        self
    }

    /// Enable delta persistence, and configure value-log-file. To disable
    /// delta persistance, pass `vlog_file` as None.
    pub fn set_delta(mut self, vlog_file: Option<String>) -> Config {
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
    pub fn set_value_log(mut self, vlog_file: Option<String>) -> Config {
        match vlog_file {
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
}

pub(crate) enum MetaItem {
    Marker(Vec<u8>),
    Metadata(Vec<u8>),
    Stats(String),
    Root(u64),
}

impl fmt::Display for MetaItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            MetaItem::Marker(_) => write!(f, "MetaItem::Marker"),
            MetaItem::Metadata(_) => write!(f, "MetaItem::Metadata"),
            MetaItem::Stats(_) => write!(f, "MetaItem::Stats"),
            MetaItem::Root(_) => write!(f, "MetaItem::Root"),
        }
    }
}

pub(crate) fn write_meta_items(items: Vec<MetaItem>, flusher: &mut Flusher) {
    for (i, item) in items.into_iter().enumerate() {
        match (i, item) {
            (0, MetaItem::Stats(stats)) => {
                let n = Config::MARKER_BLOCK_SIZE;
                let mut block: Vec<u8> = Vec::with_capacity(n);
                let m: u64 = stats.len().try_into().unwrap();
                block.extend_from_slice(&m.to_be_bytes());
                block.extend_from_slice(stats.as_bytes());
                flusher.send(block);
            }
            (1, MetaItem::Metadata(metadata)) => {
                let n = Config::compute_metadata_len(metadata.len());

                let mut blocks: Vec<u8> = Vec::with_capacity(n);
                blocks.extend_from_slice(&metadata);
                blocks.resize(blocks.capacity(), 0);
                let m: u64 = metadata.len().try_into().unwrap();
                blocks[n - 8..].copy_from_slice(&m.to_be_bytes());
                flusher.send(blocks);
            }
            (2, MetaItem::Marker(block)) => {
                flusher.send(block);
            }
            _ => unreachable!(),
        }
    }
}

pub(crate) fn read_meta_items(
    dir: &str,  // directory of index
    name: &str, // name of index
) -> Result<Vec<MetaItem>, Error> {
    let index_file = Config::stitch_index_file(dir, name);
    let mut fd = util::open_file_r(&index_file)?;
    let mut fpos = fs::metadata(&index_file)?.len();

    let mut metaitems: Vec<MetaItem> = vec![];

    // read marker block
    fpos -= Config::MARKER_BLOCK_SIZE as u64;
    metaitems.push(MetaItem::Marker(util::read_buffer(
        &mut fd,
        fpos,
        Config::MARKER_BLOCK_SIZE as u64,
        "reading marker block",
    )?));

    // read metadata blocks
    let buf = util::read_buffer(&mut fd, fpos - 8, 8, "reading metablock len")?;
    let m: usize = u64::from_be_bytes(buf.as_slice().try_into().unwrap())
        .try_into()
        .unwrap();
    let n: u64 = Config::compute_metadata_len(m).try_into().unwrap();
    fpos -= n;

    let mut blocks = util::read_buffer(&mut fd, fpos, n, "reading metablocks")?;
    blocks.resize(m, 0);
    metaitems.push(MetaItem::Metadata(blocks));

    // read stats block
    let n = Config::MARKER_BLOCK_SIZE.try_into().unwrap();
    fpos -= n;
    let block = util::read_buffer(&mut fd, fpos, n, "reading stats")?;
    let m: usize = u64::from_be_bytes(block[..8].try_into().unwrap())
        .try_into()
        .unwrap();
    let block = &block[8..8 + m];
    let s = std::str::from_utf8(block)?.to_string();
    let stats: Stats = s.parse()?;
    metaitems.push(MetaItem::Stats(s));

    // root item
    let m: u64 = stats.mblocksize.try_into().unwrap();
    metaitems.push(MetaItem::Root(fpos - m));

    Ok(metaitems)
}
