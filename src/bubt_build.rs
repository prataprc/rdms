use lazy_static::lazy_static;
use std::sync::mpsc::{Receiver, SyncSender};
use std::{cmp, ffi, fs, io::Write, marker, path, sync::mpsc, thread};

use crate::bubt;
use crate::error::BognError;
use crate::traits::{AsDelta, AsEntry, Diff, Serialize};

pub struct Builder<K, V>
where
    K: marker::Send,
{
    name: String,
    dir: String,
    m_blocksize: usize,
    z_blocksize: usize,
    v_blocksize: usize,

    incremental: bool,
    tomb_purge: bool,
    md_ok: bool,
    indx_tx: mpsc::SyncSender<Vec<u8>>,
    vlog_tx: Option<mpsc::SyncSender<Vec<u8>>>,

    // stats
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

    phantom_key: marker::PhantomData<K>,
    phantom_val: marker::PhantomData<V>,
}

// TODO: Let us try not send K or V, just binary blocks

impl<K, V> Builder<K, V>
where
    K: Clone + Ord + marker::Send,
    V: Default + Clone + Diff + Serialize,
{
    fn default(indx_tx: mpsc::SyncSender<Vec<u8>>) -> Builder<K, V> {
        Builder {
            name: Default::default(),
            dir: Default::default(),
            m_blocksize: Default::default(),
            z_blocksize: Default::default(),
            v_blocksize: Default::default(),

            incremental: Default::default(),
            tomb_purge: Default::default(),
            md_ok: Default::default(),
            indx_tx,
            vlog_tx: None,

            n_count: Default::default(),
            n_deleted: Default::default(),
            paddingmem: Default::default(),
            n_zbytes: Default::default(),
            n_mbytes: Default::default(),
            n_vbytes: Default::default(),
            n_abytes: Default::default(),
            maxseqno: Default::default(),
            keymem: Default::default(),
            valmem: Default::default(),

            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        }
    }

    fn initial(
        name: String,
        dir: String,
        mblock: usize,
        zblock: usize,
        vblock: Option<usize>,
    ) -> Result<Builder<K, V>, BognError> {
        // create flushers
        let file = Self::index_file(dir.clone(), name.clone());
        let (indx_tx, _) = Self::start_flusher(file, false /*append*/)?;

        let (vlog_tx, v_blocksize) = match vblock {
            None => (None, 0),
            Some(vblock) => {
                let file = Self::vlog_file(dir.clone(), name.clone());
                let (vlog_tx, _) = Self::start_flusher(file, false /*append*/)?;
                (Some(vlog_tx), vblock)
            }
        };

        let z_blocksize = cmp::max(zblock, mblock);
        let v_blocksize = cmp::max(v_blocksize, zblock);

        let mut builder = Builder::default(indx_tx);
        builder.name = name;
        builder.dir = dir;
        builder.m_blocksize = mblock;
        builder.z_blocksize = z_blocksize;
        builder.v_blocksize = v_blocksize;
        builder.vlog_tx = vlog_tx;
        Ok(builder)
    }

    fn incremental(
        name: String,
        dir: String,
        mblock: usize,
        zblock: usize,
        vblock: usize,
        value_file: ffi::OsString,
    ) -> Result<Builder<K, V>, BognError> {
        // create flushers
        let file = Self::index_file(dir.clone(), name.clone());
        let (indx_tx, _) = Self::start_flusher(file, false /*append*/)?;
        let (vlog_tx, n_abytes) = Self::start_flusher(value_file, true)?;

        let z_blocksize = cmp::max(zblock, mblock);
        let v_blocksize = cmp::max(vblock, zblock);

        let mut builder = Builder::default(indx_tx);
        builder.name = name;
        builder.dir = dir;
        builder.m_blocksize = mblock;
        builder.z_blocksize = z_blocksize;
        builder.v_blocksize = v_blocksize;
        builder.vlog_tx = Some(vlog_tx);
        builder.n_abytes = n_abytes as usize;
        Ok(builder)
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

    fn index_file(dir: String, name: String) -> ffi::OsString {
        let mut index_file = path::PathBuf::from(dir);
        index_file.push(format!("bubt-{}.indx", name));
        index_file.into_os_string()
    }

    fn vlog_file(dir: String, name: String) -> ffi::OsString {
        let mut vlog_file = path::PathBuf::from(dir);
        vlog_file.push(format!("bubt-{}.vlog", name));
        vlog_file.into_os_string()
    }

    fn set_tombstone_purge(&mut self, purge: bool) {
        self.tomb_purge = purge;
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
