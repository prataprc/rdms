// TODO: make dir, file, path into OsString and OsStr.

use std::{convert::TryInto, fmt, fs, path, result};

use lazy_static::lazy_static;

use crate::core::Result;
use crate::error::Error;
use crate::robt_build::Flusher;
use crate::robt_stats::Stats;
use crate::util;

include!("robt_marker.rs");

/// Configuration to build read-only btree.
#[derive(Clone)]
pub struct Config {
    /// Name of the index.
    pub name: String,
    /// Directory where index file(s) shall be stored.
    pub dir: String,
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
    /// Flush queue size.
    pub flush_queue_size: usize,
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
            flush_queue_size: Self::FLUSH_QUEUE_SIZE,
        }
    }
}

impl Config {
    pub const ZBLOCKSIZE: usize = 4 * 1024; // 4KB leaf node
    pub const MBLOCKSIZE: usize = 4 * 1024; // 4KB intermediate node
    pub const VBLOCKSIZE: usize = 4 * 1024; // ~ 4KB of blobs.
    const MARKER_BLOCK_SIZE: usize = 1024 * 4;
    const FLUSH_QUEUE_SIZE: usize = 16;

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
            flush_queue_size: Self::FLUSH_QUEUE_SIZE,
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

    /// Set flush queue size, increasing the queue size will improve batch
    /// flushing.
    pub fn set_flush_queue_size(mut self, size: usize) -> Config {
        self.flush_queue_size = size;
        self
    }
}

impl Config {
    pub(crate) fn stitch_index_file(dir: &str, name: &str) -> String {
        let mut index_file = path::PathBuf::from(dir);
        index_file.push(format!("robt-{}.indx", name));
        index_file.to_str().unwrap().to_string()
    }

    pub(crate) fn stitch_vlog_file(dir: &str, name: &str) -> String {
        let mut vlog_file = path::PathBuf::from(dir);
        vlog_file.push(format!("robt-{}.vlog", name));
        vlog_file.to_str().unwrap().to_string()
    }

    pub(crate) fn compute_root_block(n: usize) -> usize {
        if (n % Config::MARKER_BLOCK_SIZE) == 0 {
            n
        } else {
            ((n / Config::MARKER_BLOCK_SIZE) + 1) * Config::MARKER_BLOCK_SIZE
        }
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
}

// Disk-Format:
//
// *------------------------------------------* SeekFrom::End(0)
// |                marker-length             |
// *------------------------------------------* SeekFrom::End(-8)
// |               metadata-length            |
// *------------------------------------------* SeekFrom::End(-16)
// |                stats-length              |
// *------------------------------------------* SeekFrom::End(-24)
// |                  root-fpos               |
// *------------------------------------------* SeekFrom::MetaBlock
//
pub(crate) enum MetaItem {
    Marker(Vec<u8>),
    Metadata(Vec<u8>),
    Stats(String),
    Root(u64),
}

pub(crate) fn write_meta_items(
    items: Vec<MetaItem>,
    flusher: &mut Flusher, // index file
) -> Result<u64> {
    let mut block = vec![];
    block.resize(32, 0);

    for (i, item) in items.into_iter().enumerate() {
        match (i, item) {
            (0, MetaItem::Marker(data)) => {
                block[..8].copy_from_slice(&(data.len() as u64).to_be_bytes());
                block.extend_from_slice(&data);
            }
            (1, MetaItem::Metadata(md)) => {
                block[8..16].copy_from_slice(&(md.len() as u64).to_be_bytes());
                block.extend_from_slice(&md);
            }
            (2, MetaItem::Stats(s)) => {
                block[16..24].copy_from_slice(&(s.len() as u64).to_be_bytes());
                block.extend_from_slice(s.as_bytes());
            }
            (3, MetaItem::Root(fpos)) => {
                block[24..32].copy_from_slice(&fpos.to_be_bytes());
            }
            _ => unreachable!(),
        }
    }

    Ok({
        let n = Config::compute_root_block(block.len());
        block.resize(n, 0);
        let block: Vec<u8> = block.into_iter().rev().collect();
        flusher.send(block)?;
        n.try_into().unwrap()
    })
}

pub(crate) fn read_meta_items(
    dir: &str,  // directory of index
    name: &str, // name of index
) -> Result<Vec<MetaItem>> {
    let index_file = Config::stitch_index_file(dir, name);
    let m = fs::metadata(&index_file)?.len();
    let mut fd = util::open_file_r(index_file.as_ref())?;

    // read header
    let hdr = util::read_buffer(&mut fd, m - 32, 32, "read root-block header")?;
    let root = u64::from_be_bytes(hdr[..8].try_into().unwrap());
    let n_stats = u64::from_be_bytes(hdr[8..16].try_into().unwrap()) as usize;
    let n_md = u64::from_be_bytes(hdr[16..24].try_into().unwrap()) as usize;
    let n_marker = u64::from_be_bytes(hdr[24..32].try_into().unwrap()) as usize;

    // read block
    let n = Config::compute_root_block(n_stats + n_md + n_marker + 32)
        .try_into()
        .unwrap();
    let block: Vec<u8> = util::read_buffer(&mut fd, m - n, n, "read root-block")?
        .into_iter()
        .rev()
        .collect();

    let mut metaitems: Vec<MetaItem> = vec![];
    let mut off = 32;
    metaitems.push(MetaItem::Marker(block[off..off + n_marker].to_vec()));
    off += n_marker;
    metaitems.push(MetaItem::Metadata(block[off..off + n_md].to_vec()));
    off += n_md;
    metaitems.push(MetaItem::Stats(
        std::str::from_utf8(&block[off..off + n_stats])?.to_string(),
    ));
    metaitems.push(MetaItem::Root(root));

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
