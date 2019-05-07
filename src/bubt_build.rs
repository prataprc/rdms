use lazy_static::lazy_static;
use std::sync::mpsc::{Receiver, SyncSender};
use std::{ffi, cmp, fs, io::Write, path, sync::mpsc, thread};

use crate::bubt_indx::{MBlock, ZBlock};
use crate::core::{self, Diff, Serialize};
use crate::error::BognError;

#[derive(Default, Clone)]
pub struct Config {
    pub dir: String,
    pub m_blocksize: usize,
    pub z_blocksize: usize,
    pub v_blocksize: usize,
    pub tomb_purge: Option<u64>,
    pub vlog_ok: bool,
    pub vlog_file: Option<ffi::OsString>,
    pub value_in_vlog: bool,
}

impl Config {
    pub fn new(dir: String) -> Config {
        let mut config: Config = Default::default();
        config.dir = dir;
        config
    }

    pub fn set_blocksize(&mut self, m: usize, z: usize, v: usize) -> &mut Config {
        self.m_blocksize = m;
        self.z_blocksize = z;
        self.v_blocksize = v;
        self
    }

    pub fn set_tombstone_purge(&mut self, before: u64) -> &mut Config {
        self.tomb_purge = Some(before);
        self
    }

    pub fn set_vlog(
        &mut self,
        vlog_file: Option<ffi::OsString>, /* if file is None, generate vlog file */
        value_in_vlog: bool,
    ) -> &mut Config {
        self.vlog_ok = true;
        self.vlog_file = vlog_file;
        self.value_in_vlog = value_in_vlog;
        self
    }

    fn index_file(&self, name: &str) -> ffi::OsString {
        let mut index_file = path::PathBuf::from(&self.dir);
        index_file.push(format!("bubt-{}.indx", name));
        index_file.into_os_string()
    }

    fn vlog_file(&self, name: &str) -> ffi::OsString {
        let mut vlog_file = path::PathBuf::from(&self.dir);
        vlog_file.push(format!("bubt-{}.vlog", name));
        vlog_file.into_os_string()
    }
}

pub struct Builder {
    name: String,
    config: Config,
    indx_tx: mpsc::SyncSender<Vec<u8>>,
    vlog_tx: Option<mpsc::SyncSender<Vec<u8>>>,
    stats: Stats,
}

// TODO: Let us try not send K or V, just binary blocks

impl Builder {
    fn initial(name: String, config: Config) -> Result<Builder, BognError> {
        let file = config.index_file(&name);
        let (indx_tx, _n_abytes) = Self::start_flusher(file, false /*append*/)?;

        let vlog_tx = if config.vlog_ok {
            let file = match &config.vlog_file {
                Some(file) => file.clone(),
                None => config.vlog_file(&name),
            };
            let (vlog_tx, _n_abytes) = Self::start_flusher(file, false)?;
            Some(vlog_tx)
        } else {
            None
        };

        Ok(Builder {
            name,
            config,
            indx_tx,
            vlog_tx,
            stats: Default::default(),
        })
    }

    fn incremental(name: String, config: Config) -> Result<Builder, BognError> {
        let file = config.index_file(&name);
        let (indx_tx, _n_abytes) = Self::start_flusher(file, false /*append*/)?;

        let (vlog_tx, n_abytes) = if config.vlog_ok {
            let file = match &config.vlog_file {
                Some(file) => file.clone(),
                None => config.vlog_file(&name),
            };
            let (vlog_tx, n_abytes) = Self::start_flusher(file, true)?;
            (Some(vlog_tx), n_abytes)
        } else {
            (None, Default::default())
        };

        let mut builder = Builder {
            name,
            config,
            indx_tx,
            vlog_tx,
            stats: Default::default(),
        };
        builder.stats.n_abytes = n_abytes as usize;
        Ok(builder)
    }

    fn start_flusher(
        file: ffi::OsString,
        append: bool,
    ) -> Result<(mpsc::SyncSender<Vec<u8>>, u64), BognError> {
        let fd = Self::open_file(file.clone(), append)?;
        let (flusher, tx, rx) = Flusher::new(file.clone(), fd);
        let size = if append {
            fs::metadata(file)?.len()
        } else {
            Default::default()
        };
        thread::spawn(move || flusher.run(rx));
        Ok((tx, size))
    }

    fn open_file(
        file: ffi::OsString, /* path, if not exist, shall be created */
        append: bool,
    ) -> Result<fs::File, BognError> {
        let p = path::Path::new(&file);
        let parent = p.parent().ok_or(BognError::InvalidFile(file.clone()))?;
        fs::create_dir_all(parent)?;

        let mut opts = fs::OpenOptions::new();
        Ok(match append {
            false => opts.append(true).create_new(true).open(p)?,
            true => opts.append(true).open(p)?,
        })
    }

    pub fn build<I, K, V>(
        &mut self,
        mut iter: I,
        metadata: Vec<u8>, /* appended to index file */
    ) -> Result<(), BognError>
    where
        I: Iterator<Item = Result<core::Entry<K, V>, BognError>>,
        K: Ord + Clone + Serialize,
        V: Default + Clone + Diff + Serialize,
    {
        let mut vlog_fpos = self.stats.n_abytes as u64;
        let mut z = ZBlock::new_encode(vlog_fpos, self.config.clone());

        let mut mstack: Vec<MBlock<K>> = vec![];
        mstack.push(MBlock::new_encode(self.config.clone()));

        let mut indx_fpos = 0;

        while let Some(entry) = iter.next() {
            let mut entry = entry?;
            if self.preprocess_entry(&mut entry) {
                continue; // if true, this entry and all its versions purged
            }
            match z.insert(&entry, &mut self.stats) {
                Ok(_) => (),
                Err(BognError::ZBlockOverflow(_)) => {
                    let first_key = z.first_key();
                    let vlog_tx = self.vlog_tx.as_ref().unwrap();
                    let (zbytes, vbytes) = z.flush(&self.indx_tx, vlog_tx, &mut self.stats);
                    vlog_fpos += vbytes as u64;
                    let mut m = mstack.pop().unwrap();
                    if let Err(_) = m.insertz(&first_key, indx_fpos) {
                        let mbytes = m.flush(&self.indx_tx, &mut self.stats);
                        let rc = self.insertms(
                            mstack,
                            &m,
                            indx_fpos + (zbytes as u64),
                            indx_fpos + (zbytes as u64) + (mbytes as u64),
                        );
                        mstack = rc.0;
                        indx_fpos = rc.1;
                        m.reset();
                        m.insertz(&first_key, indx_fpos).unwrap();
                    }
                    mstack.push(m);
                    z.reset(vlog_fpos);
                    z.insert(&entry, &mut self.stats).unwrap();
                },
                Err(_) => unreachable!(),
            };
            self.postprocess_entry(&mut entry);
        }
        Ok(())
    }

    fn insertms<K>(
        &mut self,
        mut mstack: Vec<MBlock<K>>,
        m1: &MBlock<K>,
        child_fpos: u64,
        mut fpos: u64,
    ) -> (Vec<MBlock<K>>, u64)
    where
        K: Ord + Clone + Serialize,
    {
        let first_key = m1.first_key();
        let (mut m0, overflow) = if mstack.len() == 0 {
            let mut m0 = MBlock::new_encode(self.config.clone());
            if m0.insertm(&first_key, child_fpos) == false {
                panic!("impossible situation");
            }
            (m0, false)
        } else {
            let mut m0 = mstack.pop().unwrap();
            let overflow = m0.insertm(&first_key, child_fpos) == false;
            (m0, overflow)
        };
        if overflow {
            let mbytes = m0.flush(&self.indx_tx, &mut self.stats) as u64;
            let rc = self.insertms(mstack, &m0, fpos, fpos + mbytes);
            mstack = rc.0;
            fpos = rc.1;
            m0.reset();
            if m0.insertm(&first_key, child_fpos) == false {
                panic!("impossible situation");
            }
        }
        mstack.push(m0);
        (mstack, fpos)
    }

    fn preprocess_entry<K,V>(&mut self, entry: &mut core::Entry<K,V>) -> bool
    where
        K: Ord + Clone + Serialize,
        V: Default + Clone + Diff + Serialize,
    {
        self.stats.maxseqno = cmp::max(self.stats.maxseqno, entry.seqno());
        self.purge_values(entry)
    }

    fn postprocess_entry<K,V>(&mut self, entry: &mut core::Entry<K,V>)
    where
        K: Ord + Clone + Serialize,
        V: Default + Clone + Diff + Serialize,
    {
        self.stats.n_count += 1;
        if entry.is_deleted() {
            self.stats.n_deleted += 1;
        }
    }

    fn purge_values<K, V>(&self, entry: &mut core::Entry<K, V>) -> bool
    where
        K: Ord + Clone + Serialize,
        V: Default + Clone + Diff + Serialize,
    {
        match self.config.tomb_purge {
            Some(before) => entry.purge(before),
            _ => false,
        }
    }
}

lazy_static! {
    pub static ref MARKER_BLOCK: Vec<u8> = {
        let mut block: Vec<u8> = Vec::with_capacity(Flusher::MARKER_BLOCK_SIZE);
        block.resize(Flusher::MARKER_BLOCK_SIZE, Flusher::MARKER_BYTE);
        block
    };
}

struct Flusher {
    file: ffi::OsString,
    fd: fs::File,
}

impl Flusher {
    const MARKER_BLOCK_SIZE: usize = 1024 * 4;
    const MARKER_BYTE: u8 = 0xAB;

    fn new(
        file: ffi::OsString, /* for logging purpose */
        fd: fs::File,
    ) -> (Flusher, SyncSender<Vec<u8>>, Receiver<Vec<u8>>) {
        let (tx, rx) = mpsc::sync_channel(16); // TODO: No magic number
        (Flusher { file, fd }, tx, rx)
    }

    fn run(mut self, rx: mpsc::Receiver<Vec<u8>>) {
        for data in rx.iter() {
            if !self.write_data(&data) {
                // file descriptor and receiver channel shall be dropped.
                return;
            }
        }
        self.write_data(&MARKER_BLOCK);
        // file descriptor and receiver channel shall be dropped.
    }

    fn write_data(&mut self, data: &[u8]) -> bool {
        match self.fd.write(data) {
            Err(err) => {
                panic!("flusher: {:?} error {}...", self.file, err);
            }
            Ok(n) if n != data.len() => {
                panic!(
                    "flusher: {:?} partial write {}/{}...",
                    self.file,
                    n,
                    data.len()
                );
            }
            Ok(_) => true,
        }
    }
}

#[derive(Default)]
pub struct Stats {
    pub(crate) n_count: usize,
    pub(crate) n_deleted: usize,
    pub(crate) maxseqno: u64,
    pub(crate) n_abytes: usize,
    pub(crate) keymem: usize,
    pub(crate) valmem: usize,
    pub(crate) z_bytes: usize,
    pub(crate) v_bytes: usize,
    pub(crate) m_bytes: usize,
}
