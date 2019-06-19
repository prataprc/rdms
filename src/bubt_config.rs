// TODO: Review all error messages. Sometimes better to consolidate
// error variants and describe the different error-out with messages.

use std::{convert::TryInto, fmt, fs, path};

use lazy_static::lazy_static;

//use crate::bubt_build::FlushClient;
use crate::bubt_stats::Stats;
use crate::error::Error;
use crate::util;

lazy_static! {
    pub static ref MARKER_BLOCK: Vec<u8> = {
        let mut block: Vec<u8> = Vec::with_capacity(Config::MARKER_BLOCK_SIZE);
        block.resize(Config::MARKER_BLOCK_SIZE, Config::MARKER_BYTE);
        block
    };
}

/// Configuration to build bottoms up btree.
#[derive(Default, Clone)]
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

impl Config {
    const ZBLOCKSIZE: usize = 4 * 1024;
    const MBLOCKSIZE: usize = 4 * 1024;
    const VBLOCKSIZE: usize = 4 * 1024;
    const MARKER_BLOCK_SIZE: usize = 1024 * 4;
    const MARKER_BYTE: u8 = 0xAB;

    pub(crate) fn to_index_file(&self) -> String {
        Self::stitch_index_file(&self.dir, &self.name)
    }

    pub(crate) fn to_value_log(&self) -> Option<String> {
        match &self.vlog_file {
            Some(file) => Some(file.clone()),
            None => Some(Self::stitch_vlog_file(&self.dir, &self.name)),
        }
    }

    fn stitch_index_file(dir: &str, name: &str) -> String {
        let mut index_file = path::PathBuf::from(dir);
        index_file.push(format!("bubt-{}-shard1.indx", name));
        index_file.to_str().unwrap().to_string()
    }

    fn stitch_vlog_file(dir: &str, name: &str) -> String {
        let mut vlog_file = path::PathBuf::from(dir);
        vlog_file.push(format!("bubt-{}-shard1.vlog", name));
        vlog_file.to_str().unwrap().to_string()
    }

    // New default configuration:
    // * With ZBLOCKSIZE, MBLOCKSIZE, VBLOCKSIZE.
    // * Without a separate vlog-file for value.
    // * Without tombstone purge for deleted values.
    pub fn new(dir: &str, name: &str) -> Config {
        Config {
            dir: dir.to_string(),
            name: name.to_string(),
            z_blocksize: Self::ZBLOCKSIZE,
            v_blocksize: Self::VBLOCKSIZE,
            m_blocksize: Self::MBLOCKSIZE,
            tomb_purge: Default::default(),
            delta_ok: Default::default(),
            vlog_file: Default::default(),
            value_in_vlog: Default::default(),
        }
    }

    pub fn set_blocksize(mut self, m: usize, z: usize, v: usize) -> Config {
        self.m_blocksize = m;
        self.z_blocksize = z;
        self.v_blocksize = v;
        self
    }

    pub fn set_tombstone_purge(mut self, before: u64) -> Config {
        self.tomb_purge = Some(before);
        self
    }

    pub fn set_delta(mut self, vlog_file: Option<String>) -> Config {
        self.delta_ok = true;
        self.vlog_file = self.vlog_file.take().or(vlog_file);
        self
    }

    pub fn set_value_log(mut self, vlog_file: Option<String>) -> Config {
        self.value_in_vlog = true;
        self.vlog_file = self.vlog_file.take().or(vlog_file);
        self
    }
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

//pub(crate) fn write_meta_items(items: Vec<MetaItem>, flusher: &mut FlushClient) {
//    let mut iter = items.into_iter();
//    // metaitem - stats
//    if let Some(MetaItem::Stats(stats)) = iter.next() {
//        let mut block: Vec<u8> = Vec::with_capacity(Config::MARKER_BLOCK_SIZE);
//        let scratch = (stats.len() as u64).to_be_bytes();
//        block.extend_from_slice(&scratch);
//        block.extend_from_slice(stats.as_bytes());
//        flusher.send(block);
//    } else {
//        unreachable!()
//    }
//    // metaitem - metadata
//    if let Some(MetaItem::Metadata(metadata)) = iter.next() {
//        let n = ((metadata.len() + 8) / Config::MARKER_BLOCK_SIZE) + 1;
//        let n = n * Config::MARKER_BLOCK_SIZE;
//        let mut blocks: Vec<u8> = Vec::with_capacity(n);
//        blocks.extend_from_slice(&metadata);
//
//        blocks.resize(blocks.capacity(), 0);
//
//        let loc = blocks.len() - 8;
//        let scratch = (metadata.len() as u64).to_be_bytes();
//        blocks[loc..].copy_from_slice(&scratch);
//        flusher.send(blocks);
//    } else {
//        unreachable!();
//    }
//    // metaitem -  marker
//    if let Some(MetaItem::Marker(block)) = iter.next() {
//        flusher.send(block);
//    }
//
//    if iter.next().is_some() {
//        unreachable!();
//    }
//}

pub(crate) fn read_meta_items(dir: &str, name: &str) -> Result<Vec<MetaItem>, Error> {
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
    let mdlen: usize = u64::from_be_bytes(buf.as_slice().try_into().unwrap())
        .try_into()
        .unwrap();

    let n_blocks = ((mdlen + 8) / Config::MARKER_BLOCK_SIZE) + 1;
    let n: u64 = (n_blocks * Config::MARKER_BLOCK_SIZE).try_into().unwrap();
    fpos -= n;

    let mut blocks = util::read_buffer(&mut fd, fpos, n, "reading metablocks")?;
    blocks.resize(mdlen, 0);
    metaitems.push(MetaItem::Metadata(blocks));

    // read stats block
    let n = Config::MARKER_BLOCK_SIZE.try_into().unwrap();
    fpos -= n;
    let block = util::read_buffer(&mut fd, fpos, n, "reading stats")?;
    let m = u64::from_be_bytes(block[..8].try_into().unwrap()) as usize;
    let block = &block[8..8 + m];
    metaitems.push(MetaItem::Stats(std::str::from_utf8(block)?.to_string()));

    // root item
    metaitems.push(MetaItem::Root(fpos - Config::MARKER_BLOCK_SIZE as u64));

    Ok(metaitems)
}
