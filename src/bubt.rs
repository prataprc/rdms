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

    tomb_purge: bool,
    md_ok: bool,
    indx_tx: mpsc::SyncSender<Vec<u8>>,
    vlog_tx: Option<mpsc::SyncSender<Vec<u8>>>,

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
    ) -> Result<Builder<K, V>, BognError<K>> {
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

            tomb_purge: false,
            md_ok: false,
            indx_tx,
            vlog_tx,
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
    ) -> Result<Builder<K, V>, BognError<K>> {
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

            tomb_purge: false,
            md_ok: false,
            indx_tx,
            vlog_tx,
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        })
    }

    fn start_flusher(
        file_name: ffi::OsString /*file to flush*/
    ) -> Result<mpsc::SyncSender<Vec<u8>>, BognError<K>> {
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
}

//    name       String
//    tomb_purge  bool
//    mflusher   *bubtflusher
//    zflushers  []*bubtflusher
//    vflushers  []*bubtflusher
//    headmblock *mblock
//    vlinks     []string
//    vfiles     []string
//    vmode      string
//    appendid   string
//    mdok       bool
//
//    // settings, will be flushed to the tip of indexfile.
//    m_blocksize int64
//    z_blocksize int64
//    v_blocksize int64
//    zeromblock *mblock
//    logprefix  string
//}

lazy_static! {
    pub static ref MARKER_BLOCK: Vec<u8> = {
        let mut block: Vec<u8> = Vec::with_capacity(Flusher::MARKER_BLOCK_SIZE);
        block.resize(Flusher::MARKER_BLOCK_SIZE, Flusher::MARKER_BYTE);
        block
    };
}

// TODO: remove the pub
pub struct Flusher {
    file: ffi::OsString,
    fd: fs::File,
}

impl Flusher {
    const MARKER_BLOCK_SIZE: usize = 1024 * 4;
    const MARKER_BYTE: u8 = 0xAB;

    pub fn new<K>(
        file: ffi::OsString,
    ) -> Result<(Flusher, SyncSender<Vec<u8>>, Receiver<Vec<u8>>), BognError<K>> {
        let p = path::Path::new(&file);
        let parent = p.parent().ok_or(BognError::InvalidFile(file.clone()))?;
        fs::create_dir_all(parent)?;
        let fd = fs::File::create(p)?;

        let (tx, rx) = mpsc::sync_channel(16); // TODO: No magic number
        Ok((Flusher { file, fd }, tx, rx))
    }

    pub fn run(mut self, rx: mpsc::Receiver<Vec<u8>>) {
        for data in rx.iter() {
            if !self.write_data(&data) {
                // file descriptor and receiver channel shall be dropped.
                return;
            }
        }
        self.write_data(&MARKER_BLOCK);
        // file descriptor and receiver channel shall be dropped.
    }

    pub fn write_data(&mut self, data: &[u8]) -> bool {
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
