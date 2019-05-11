use std::{
    convert::TryInto,
    fs,
    io::{self, Read, Seek},
    path,
};

use lazy_static::lazy_static;

use crate::bubt_build::FlushClient;
use crate::bubt_stats::Stats;
use crate::core::Result;
use crate::error::BognError;

lazy_static! {
    pub static ref MARKER_BLOCK: Vec<u8> = {
        let mut block: Vec<u8> = Vec::with_capacity(Config::MARKER_BLOCK_SIZE);
        block.resize(Config::MARKER_BLOCK_SIZE, Config::MARKER_BYTE);
        block
    };
}

#[derive(Default, Clone)]
pub struct Config {
    pub dir: String,
    pub z_blocksize: usize,
    pub m_blocksize: usize,
    pub v_blocksize: usize,
    pub tomb_purge: Option<u64>,
    pub vlog_ok: bool,
    pub vlog_file: Option<String>,
    pub value_in_vlog: bool,
}

impl Config {
    const ZBLOCKSIZE: usize = 4 * 1024;
    const MBLOCKSIZE: usize = 4 * 1024;
    const VBLOCKSIZE: usize = 4 * 1024;
    const MARKER_BLOCK_SIZE: usize = 1024 * 4;
    const MARKER_BYTE: u8 = 0xAB;

    // New default configuration:
    // * With ZBLOCKSIZE, MBLOCKSIZE, VBLOCKSIZE.
    // * Without a separate vlog-file for value.
    // * Without tombstone purge for deleted values.
    pub fn new(dir: &str) -> Config {
        Config {
            dir: dir.to_string(),
            z_blocksize: Self::ZBLOCKSIZE,
            v_blocksize: Self::VBLOCKSIZE,
            m_blocksize: Self::MBLOCKSIZE,
            tomb_purge: Default::default(),
            vlog_ok: Default::default(),
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

    pub fn set_vlog(
        mut self,
        vlog_file: Option<String>, /* if None, generate vlog file */
        value_in_vlog: bool,
    ) -> Config {
        self.vlog_ok = true;
        self.vlog_file = vlog_file;
        self.value_in_vlog = value_in_vlog;
        self
    }

    pub(crate) fn index_file(dir: &str, name: &str) -> String {
        let mut index_file = path::PathBuf::from(dir);
        index_file.push(format!("bubt-{}.indx", name));
        index_file.to_str().unwrap().to_string()
    }

    pub(crate) fn vlog_file(dir: &str, name: &str) -> String {
        let mut vlog_file = path::PathBuf::from(dir);
        vlog_file.push(format!("bubt-{}.vlog", name));
        vlog_file.to_str().unwrap().to_string()
    }

    pub(crate) fn open_file(file: &str, write: bool, append: bool) -> Result<fs::File> {
        let p = path::Path::new(&file);
        let parent = p.parent().ok_or(BognError::InvalidFile(file.to_string()))?;
        if write {
            fs::create_dir_all(parent)?;

            let mut opts = fs::OpenOptions::new();
            Ok(match append {
                false => opts.append(true).create_new(true).open(p)?,
                true => opts.append(true).open(p)?,
            })
        } else {
            let mut opts = fs::OpenOptions::new();
            Ok(opts.read(true).create_new(true).open(p)?)
        }
    }

    pub(crate) fn vlog_file_w(&self, dir: &str, name: &str) -> String {
        match &self.vlog_file {
            Some(vlog_file) => vlog_file.clone(),
            None => Config::vlog_file(dir, name),
        }
    }

    pub(crate) fn write_meta_items(&self, meta_items: Vec<MetaItem>, i_flusher: &mut FlushClient) {
        let mut iter = meta_items.into_iter();
        // metaitem - stats
        if let Some(MetaItem::Stats(stats)) = iter.next() {
            let mut block: Vec<u8> = Vec::with_capacity(Self::MARKER_BLOCK_SIZE);
            block.resize(0, 0);
            let scratch = (stats.len() as u64).to_be_bytes();
            block.extend_from_slice(&scratch);
            block.extend_from_slice(stats.as_bytes());
            i_flusher.send(block);
        } else {
            unreachable!()
        }
        // metaitem - metadata
        if let Some(MetaItem::Metadata(metadata)) = iter.next() {
            let n = ((metadata.len() + 8) / Self::MARKER_BLOCK_SIZE) + 1;
            let mut blocks: Vec<u8> = Vec::with_capacity(n * Self::MARKER_BLOCK_SIZE);
            blocks.extend_from_slice(&metadata);

            blocks.resize(blocks.capacity(), 0);

            let loc = blocks.len() - 8;
            let scratch = (metadata.len() as u64).to_be_bytes();
            blocks[loc..].copy_from_slice(&scratch);
            i_flusher.send(blocks);
        } else {
            unreachable!();
        }
        // metaitem -  marker
        if let Some(MetaItem::Marker(block)) = iter.next() {
            i_flusher.send(block);
        }

        if iter.next().is_some() {
            unreachable!();
        }
    }

    pub(crate) fn open_index(dir: &str, name: &str) -> Result<Vec<MetaItem>> {
        let index_file = Config::index_file(dir, name);
        let mut fd = Self::open_file(&index_file, false /*write*/, false /*append*/)?;

        let mut fpos = fs::metadata(index_file)?.len();
        let mut metaitems: Vec<MetaItem> = vec![];

        // read marker block
        fpos -= Self::MARKER_BLOCK_SIZE as u64;
        fd.seek(io::SeekFrom::Start(fpos))?;

        let mut block = Vec::with_capacity(Self::MARKER_BLOCK_SIZE);
        block.resize(block.capacity(), 0);
        let n = fd.read(&mut block)?;
        let marker = if n != block.len() {
            Err(BognError::PartialRead(block.len(), n))
        } else {
            Ok(MetaItem::Marker(block))
        }?;
        metaitems.push(marker);

        // read metadata blocks
        fd.seek(io::SeekFrom::Start(fpos - 8))?;

        let mut scratch = [0_u8; 8];
        let n = fd.read(&mut scratch)?;
        let metadata = if n != scratch.len() {
            Err(BognError::PartialRead(scratch.len(), n))
        } else {
            let mdlen = u64::from_be_bytes(scratch) as usize;
            let n_blocks = ((mdlen + 8) / Self::MARKER_BLOCK_SIZE) + 1;
            let n = n_blocks * Self::MARKER_BLOCK_SIZE;
            fpos -= n as u64;
            fd.seek(io::SeekFrom::Start(fpos))?;

            let mut blocks: Vec<u8> = Vec::with_capacity(n);
            blocks.resize(blocks.capacity(), 0);
            let n = fd.read(&mut blocks)?;
            if n != blocks.len() {
                Err(BognError::PartialRead(scratch.len(), n))
            } else {
                blocks.resize(mdlen, 0);
                Ok(MetaItem::Metadata(blocks))
            }
        }?;
        metaitems.push(metadata);

        // read stats block
        fpos -= Self::MARKER_BLOCK_SIZE as u64;
        fd.seek(io::SeekFrom::Start(fpos))?;

        let mut block: Vec<u8> = Vec::with_capacity(Self::MARKER_BLOCK_SIZE);
        block.resize(block.capacity(), 0);
        let n = fd.read(&mut block)?;
        let stats = if n != block.len() {
            Err(BognError::PartialRead(scratch.len(), n))
        } else {
            let ln = u64::from_be_bytes(block[..8].try_into().unwrap()) as usize;
            Ok(MetaItem::Stats(
                std::str::from_utf8(&block[8..8 + ln])?.to_string(),
            ))
        }?;
        metaitems.push(stats);

        // root item
        fpos -= Self::MARKER_BLOCK_SIZE as u64;
        metaitems.push(MetaItem::Root(fpos));

        Ok(metaitems)
    }
}

impl From<Stats> for Config {
    fn from(stats: Stats) -> Config {
        Config {
            dir: Default::default(),
            z_blocksize: stats.zblocksize,
            m_blocksize: stats.mblocksize,
            v_blocksize: stats.vblocksize,
            tomb_purge: Default::default(),
            vlog_ok: stats.vlog_ok,
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
