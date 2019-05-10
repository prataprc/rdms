use std::{ffi, path};

use crate::bubt_build::FlushClient;

#[derive(Default, Clone)]
pub struct Config {
    pub dir: String,
    pub z_blocksize: usize,
    pub m_blocksize: usize,
    pub v_blocksize: usize,
    pub tomb_purge: Option<u64>,
    pub vlog_ok: bool,
    pub vlog_file: Option<ffi::OsString>,
    pub value_in_vlog: bool,
}

impl Config {
    const ZBLOCKSIZE: usize = 4 * 1024;
    const MBLOCKSIZE: usize = 4 * 1024;
    const VBLOCKSIZE: usize = 4 * 1024;

    // New default configuration:
    // * With ZBLOCKSIZE, MBLOCKSIZE, VBLOCKSIZE.
    // * Without a separate vlog-file for value.
    // * Without tombstone purge for deleted values.
    pub fn new(dir: String) -> Config {
        Config {
            dir,
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
        vlog_file: Option<ffi::OsString>, /* if None, generate vlog file */
        value_in_vlog: bool,
    ) -> Config {
        self.vlog_ok = true;
        self.vlog_file = vlog_file;
        self.value_in_vlog = value_in_vlog;
        self
    }

    pub(crate) fn index_file(&self, name: &str) -> ffi::OsString {
        let mut index_file = path::PathBuf::from(&self.dir);
        index_file.push(format!("bubt-{}.indx", name));
        index_file.into_os_string()
    }

    pub(crate) fn vlog_file(&self, name: &str) -> ffi::OsString {
        match &self.vlog_file {
            Some(vlog_file) => vlog_file.clone(),
            None => {
                let mut vlog_file = path::PathBuf::from(&self.dir);
                vlog_file.push(format!("bubt-{}.vlog", name));
                vlog_file.into_os_string()
            }
        }
    }

    pub(crate) fn write_meta_items(&self, meta_items: Vec<MetaItem>, i_flusher: &mut FlushClient) {
        let mut iter = meta_items.into_iter();

        if let Some(MetaItem::Stats(stats)) = iter.next() {
            let mut block: Vec<u8> = Vec::with_capacity(self.m_blocksize);
            block.resize(0, 0);
            let scratch = (stats.len() as u64).to_be_bytes();
            block.extend_from_slice(&scratch);
            block.extend_from_slice(stats.as_bytes());
            i_flusher.send(block);
        } else {
            unreachable!()
        }

        if let Some(MetaItem::Metadata(metadata)) = iter.next() {
            let n = ((metadata.len() + 8) / self.m_blocksize) + 1;
            let mut blocks: Vec<u8> = Vec::with_capacity(n * self.m_blocksize);
            blocks.extend_from_slice(&metadata);

            blocks.resize(blocks.capacity(), 0);

            let loc = blocks.len() - 8;
            let scratch = (metadata.len() as u64).to_be_bytes();
            blocks[loc..].copy_from_slice(&scratch);
            i_flusher.send(blocks);
        } else {
            unreachable!();
        }

        if iter.next().is_some() {
            unreachable!();
        }
    }
}

pub(crate) enum MetaItem {
    Stats(String),
    Metadata(Vec<u8>),
}
