use lazy_static::lazy_static;
use std::sync::mpsc::{Receiver, SyncSender};
use std::{cmp, ffi, fs, io::Write, marker, path, sync::mpsc, thread};

use crate::bubt;
use crate::core::{AsDelta, AsEntry, Diff, Serialize};
use crate::error::BognError;

#[derive(Default)]
pub struct Stats {
    n_count: usize,
    n_deleted: usize,
    paddingmem: usize,
    n_zbytes: usize,
    n_mbytes: usize,
    n_vbytes: usize,
    n_abytes: usize,
    maxseqno: u64,
    keymem: usize,
    valmem: usize,
}

#[derive(Default)]
pub struct Config {
    pub dir: String,
    pub m_blocksize: usize,
    pub z_blocksize: usize,
    pub v_blocksize: usize,
    pub tomb_purge: Option<u64>,
    pub value_log: bool
    pub value_file: Option<ffi::OsString>,
}

impl Config {
    pub fn new(dir: String) -> Config {
        let config = Default::default();
        config.dir = dir
        config
    }

    pub fn set_blocksize(&mut self, m: usize, z: usize, v: usize) -> &mut Config {
        self.m_blocksize = m;
        self.z_blocksize = z;
        self.v_blocksize = v;
        self
    }

    pub fn set_value_log(&mut self, value_file: ffi::OsString) -> &mut Config {
        self.value_file = Some(value_file);
        self
    }

    pub fn set_tombstone_purge(&mut self, before: u64) -> &mut Config {
        self.tomb_purge = Some(before);
        self
    }

    fn index_file(&self, name: &str) -> ffi::OsString {
        let mut index_file = path::PathBuf::from(self.dir);
        index_file.push(format!("bubt-{}.indx", name));
        index_file.into_os_string()
    }

    fn vlog_file(&self, name: &str) -> ffi::OsString {
        let mut vlog_file = path::PathBuf::from(self.dir);
        vlog_file.push(format!("bubt-{}.vlog", name));
        vlog_file.into_os_string()
    }

}

pub struct Builder<K, V>
where
    K: marker::Send,
{
    name: String,
    config: Config,

    md_ok: bool,
    indx_tx: mpsc::SyncSender<Vec<u8>>,
    vlog_tx: Option<mpsc::SyncSender<Vec<u8>>>,

    stats: Stats,

    phantom_key: marker::PhantomData<K>,
    phantom_val: marker::PhantomData<V>,
}

// TODO: Let us try not send K or V, just binary blocks

impl<K, V> Builder<K, V>
where
    K: Clone + Ord + marker::Send,
    V: Default + Clone + Diff + Serialize,
{
    fn initial(name: String, config: Config) -> Result<Builder<K, V>, BognError> {
        let file = conf.index_file(&name);
        let (indx_tx, _n_abytes) = Self::start_flusher(file, false /*append*/)?;

        let vlog_tx = if conf.value_log {
            let file = conf.vlog_file(&name);
            let (vlog_tx, _n_abytes) = Self::start_flusher(file, false)?;
            Some(vlog_tx)
        } else {
            None
        }

        Ok(Builder{
            name,
            config,
            incremental: false,
            md_ok: false,
            indx_tx,
            vlog_tx,
            stats: Default::default(),
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        })
    }

    fn incremental(name: String, c: Config) -> Result<Builder<K, V>, BognError> {
        // create flushers
        let file = c.index_file(&name);
        let (indx_tx, _n_abytes) = Self::start_flusher(file, false /*append*/)?;
        let (vlog_tx, n_abytes) = match c.value_file {
            Some(value_file) => Self::start_flusher(value_file, true)?,
            None => panic!("set value_file for incremental build"),
        }

        Ok(Builder{
            name,
            config,
            incremental: true,
            md_ok: false,
            indx_tx,
            vlog_tx: Some(vlog_tx),
            stats: Default::default(),
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        })
    }

    fn start_flusher(
        file: ffi::OsString,
        append: bool,
    ) -> Result<(mpsc::SyncSender<Vec<u8>>, u64), BognError> {
        let (flusher, tx, rx) = Flusher::new(file.clone(), append)?;

        let size = if append { fs::metadata(file)?.len() } else { 0 };

        thread::spawn(move || flusher.run(rx));
        Ok((tx, size))
    }

    //pub fn build<I,E>(&mut self, iter: I, metadata: Vec<u8> /* appended to indx file */) -> Result<(), BognError> {
    //where
    //    I: Iterator<Item=Result<E, BognError>>,
    //    E: AsEntry<K, V>,
    //{
    //    self.build_blocks(iter, metadata)
    //}

    //pub fn build_blocks<I,E>(&mut self, iter: I, metadata: Vec<u8>) -> Result<(), BognError> {
    //where
    //    I: Iterator<Item=Result<E, BognError>>,
    //    E: AsEntry<K, V>,
    //{
    //    let iter = BuildIter::new(iter);
    //    let mut mstack: Vec<MBlock> = vec![];
    //    mstack.push(MBlock::new());
    //    let mut z = ZBlock::new();

    //    while let Some(entry) = match iter.next() {
    //        let entry = entry?;
    //        if self.skip_entry(&entry) {
    //            continue
    //        }
    //        if z.insert(&entry)? == false { // overflow
    //            z.flush()?;
    //            let mut m = mstack.pop();
    //            if m.insertz(&z)? == false {// overflow
    //                m.flush()?;
    //                mstack = Self::m_insertm(mstack, &m)?;
    //                m.reset();
    //                m.insertz(&z)?;
    //            }
    //            mstack.push(m)
    //            z.reset();
    //        }
    //    }
    //    Ok(())
    //}

    //fn m_insertm(mut mstack: Vec<MBlock>, m1: &MBlock /* next block */) -> Result<Vec<Block>, BognError> {
    //    if mstack.len() == 0 {
    //        let mut m0 = MBlock::new();
    //        m0.insertm(&m1)?;
    //        mstack.push(m0);
    //    } else {
    //        let mut m0 = mstack.pop();
    //        if m0.insertm(&m1)? == false { // overflow
    //            m0.flush()?;
    //            mstack = Self::m_insertm(mstack, &m0)?;
    //            m0.reset();
    //            m0.insertm(&m1)?;
    //        }
    //        mstack.push(m0)
    //    }
    //    mstack
    //}

    fn skip_entry<D>(&self, entry: &bubt::Entry<K, V, D>) -> bool
    where
        D: Clone + AsDelta<V>,
    {
        entry.is_deleted && self.tomb_purge
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
        file: ffi::OsString,
        append: bool,
    ) -> Result<(Flusher, SyncSender<Vec<u8>>, Receiver<Vec<u8>>), BognError> {
        let p = path::Path::new(&file);
        let parent = p.parent().ok_or(BognError::InvalidFile(file.clone()))?;
        fs::create_dir_all(parent)?;

        let mut opts = fs::OpenOptions::new();
        let fd = match append {
            false => opts.append(true).create_new(true).open(p)?,
            true => opts.append(true).open(p)?,
        };

        let (tx, rx) = mpsc::sync_channel(16); // TODO: No magic number
        Ok((Flusher { file, fd }, tx, rx))
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

struct BuildIter<I, E, K, V>
where
    I: Iterator<Item = Result<E, BognError>>,
    E: AsEntry<K, V>,
    K: Clone + Ord,
    V: Default + Clone + Diff + Serialize,
{
    iter: I,

    phantom_key: marker::PhantomData<K>,
    phantom_val: marker::PhantomData<V>,
}

impl<I, E, K, V> BuildIter<I, E, K, V>
where
    I: Iterator<Item = Result<E, BognError>>,
    E: AsEntry<K, V>,
    K: Clone + Ord,
    V: Default + Clone + Diff + Serialize,
{
    fn new(iter: I) -> BuildIter<I, E, K, V> {
        BuildIter {
            iter,
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        }
    }
}

impl<I, E, K, V> Iterator for BuildIter<I, E, K, V>
where
    I: Iterator<Item = Result<E, BognError>>,
    E: AsEntry<K, V>,
    K: Clone + Ord,
    V: Default + Clone + Diff + Serialize,
{
    type Item = Result<bubt::Entry<K, V, <E as AsEntry<K, V>>::Delta>, BognError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok(entry)) => Some(Ok(bubt::Entry::new(entry))),
            Some(Err(err)) => Some(Err(err)),
            None => None,
        }
    }
}
