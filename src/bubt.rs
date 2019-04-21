use lazy_static::lazy_static;
use std::sync::mpsc::{Receiver, SyncSender};
use std::{cmp, ffi, fs, io::Write, marker, path, sync::mpsc, thread};

use crate::error::BognError;

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

impl<K, V> Builder<K, V>
where
    K: marker::Send,
{
    fn initial(
        name: String,
        dir: String,
        mblock: usize,
        zblock: usize,
        vblock: usize,
    ) -> Result<Builder<K, V>, BognError> {
        // create flushers
        let file = Self::index_file(dir.clone(), name.clone());
        let indx_tx = Self::start_flusher(file)?;

        let vlog_tx = if vblock == 0 {
            let file = Self::vlog_file(dir.clone(), name.clone());
            Some(Self::start_flusher(file)?)
        } else {
            None
        };

        let z_blocksize = cmp::max(zblock, mblock);
        let v_blocksize = cmp::max(vblock, zblock);

        Ok(Builder {
            name,
            dir,
            m_blocksize: mblock,
            z_blocksize,
            v_blocksize,

            incremental: false,
            tomb_purge: false,
            md_ok: false,
            indx_tx,
            vlog_tx,

            n_count: 0,
            n_deleted: 0,
            paddingmem: 0,
            n_zbytes: 0,
            n_mbytes: 0,
            n_vbytes: 0,
            n_abytes: 0,
            maxseqno: 0,
            keymem: 0,
            valmem: 0,

            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        })
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
        let indx_tx = Self::start_flusher(file)?;

        let vlog_tx = Some(Self::start_flusher(value_file)?);

        let z_blocksize = cmp::max(zblock, mblock);
        let v_blocksize = cmp::max(vblock, zblock);

        Ok(Builder {
            name,
            dir,
            m_blocksize: mblock,
            z_blocksize,
            v_blocksize,

            incremental: true,
            tomb_purge: false,
            md_ok: false,
            indx_tx,
            vlog_tx,

            n_count: 0,
            n_deleted: 0,
            paddingmem: 0,
            n_zbytes: 0,
            n_mbytes: 0,
            n_vbytes: 0,
            n_abytes: 0,
            maxseqno: 0,
            keymem: 0,
            valmem: 0,

            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        })
    }

    fn start_flusher(
        file_name: ffi::OsString /*file to flush*/
    ) -> Result<mpsc::SyncSender<Vec<u8>>, BognError> {
        // create flushers
        let (flusher, tx, rx) = Flusher::new(file_name)?;
        thread::spawn(move || flusher.run(rx));
        Ok(tx)
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

    //pub fn build(iter: impl Iterator<Item = E>, metadata: Vec<u8>)
    //where
    //    E: AsEntry<K, V>,
    //{
    //}
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
    ) -> Result<(Flusher, SyncSender<Vec<u8>>, Receiver<Vec<u8>>), BognError> {
        let p = path::Path::new(&file);
        let parent = p.parent().ok_or(BognError::InvalidFile(file.clone()))?;
        fs::create_dir_all(parent)?;
        let fd = fs::File::create(p)?;

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

//struct Iter<K, V, E>
//where
//    E: AsEntry<K, V>,
//{
//    iter: impl Iterator<Item = E>,
//}
