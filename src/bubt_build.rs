// TODO: flush put blocks into tx channel. Right now we simply unwrap()

use lazy_static::lazy_static;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::{cmp, ffi, fs, io::Write, marker, mem, path, thread, time};

use crate::core::{self, Diff, Serialize};
use crate::error::BognError;
use crate::vlog;

include!("./bubt_indx.rs");

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
        vlog_file: Option<ffi::OsString>, /* if None, generate vlog file */
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

pub struct Builder<K, V>
where
    K: Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
{
    name: String,
    config: Config,
    indx_tx: FlushClient,
    vlog_tx: Option<FlushClient>,
    stats: Stats,

    phantom_key: marker::PhantomData<K>,
    phantom_val: marker::PhantomData<V>,
}

// TODO: Let us try not send K or V, just binary blocks

impl<K, V> Builder<K, V>
where
    K: Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
{
    pub fn initial(name: String, config: Config) -> Result<Builder<K, V>, BognError> {
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
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        })
    }

    pub fn incremental(name: String, config: Config) -> Result<Builder<K, V>, BognError> {
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
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        };
        builder.stats.n_abytes = n_abytes as usize;
        Ok(builder)
    }

    fn start_flusher(file: ffi::OsString, append: bool) -> Result<(FlushClient, u64), BognError> {
        let fd = Self::open_file(file.clone(), append)?;
        let (flusher, tx, rx) = Flusher::new(file.clone(), fd);
        let size = if append {
            fs::metadata(file)?.len()
        } else {
            Default::default()
        };
        let handle = thread::spawn(move || flusher.run(rx));
        Ok((FlushClient { tx, handle }, size))
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

    pub fn build<I>(
        mut self,
        mut iter: I,
        metadata: Vec<u8>, /* appended to index file */
    ) -> Result<(), BognError>
    where
        I: Iterator<Item = Result<core::Entry<K, V>, BognError>>,
    {
        let start = time::SystemTime::now();

        let mut b = BuildData::new(self.stats.n_abytes, self.config.clone());
        while let Some(entry) = iter.next() {
            let mut entry = entry?;
            if self.preprocess_entry(&mut entry) {
                continue; // if true, this entry and all its versions purged
            }

            match b.z.insert(&entry, &mut self.stats) {
                Ok(_) => (),
                Err(BognError::ZBlockOverflow(_)) => {
                    let first_key = b.z.first_key();
                    let (zbytes, vbytes) = b.z.finalize(&mut self.stats);
                    b.z.flush(&mut self.indx_tx, self.vlog_tx.as_mut());
                    b.update_z_flush(zbytes, vbytes);
                    let mut m = b.mstack.pop().unwrap();
                    if let Err(_) = m.insertz(&first_key, b.z_fpos) {
                        let mbytes = m.finalize(&mut self.stats);
                        m.flush(&mut self.indx_tx);
                        b.update_m_flush(mbytes);
                        self.insertms(m.first_key(), &mut b);
                        m.reset();
                        m.insertz(&first_key, b.z_fpos).unwrap();
                    }
                    b.mstack.push(m);
                    b.reset();
                    b.z.insert(&entry, &mut self.stats).unwrap();
                }
                Err(_) => unreachable!(),
            };
            self.postprocess_entry(&mut entry);
        }

        // flush final set partial blocks
        if self.stats.n_count > 0 {
            let first_key = b.z.first_key();
            let (zbytes, vbytes) = b.z.finalize(&mut self.stats);
            b.z.flush(&mut self.indx_tx, self.vlog_tx.as_mut());
            b.update_z_flush(zbytes, vbytes);

            self.finalize1(&mut b, first_key); // flush zblock and its parents
            self.finalize2(&mut b); // flush mblocks
        }

        let elapsed = start.elapsed();
        let epoch = time::UNIX_EPOCH.elapsed().unwrap().as_nanos();
        let attributes: Vec<String> = vec![
            format!(r#""name": {}"#, self.name),
            format!(r#""zblocksize": {}"#, self.config.z_blocksize),
            format!(r#""mblocksize": {}"#, self.config.m_blocksize),
            format!(r#""vblocksize": {}"#, self.config.v_blocksize),
            format!(r#""buildtime":  "{}""#, elapsed.unwrap().as_nanos()),
            format!(r#""epoch": {}"#, epoch),
            format!(r#""seqno": {}"#, self.stats.maxseqno),
            format!(r#""keymem": {}"#, self.stats.keymem),
            format!(r#""valmem": {}"#, self.stats.valmem),
            format!(r#""paddingmem": {}"#, self.stats.padding),
            format!(r#""z_bytes": {}"#, self.stats.z_bytes),
            format!(r#""m_bytes": {}"#, self.stats.m_bytes),
            format!(r#""v_bytes": {}"#, self.stats.v_bytes),
            format!(r#""n_abytes": {}"#, self.stats.n_abytes),
            format!(r#""n_count": {}"#, self.stats.n_count),
            format!(r#""n_deleted": {}"#, self.stats.n_deleted),
        ];
        let stats = "{ ".to_owned() + &attributes.join(",") + " }";
        if (stats.len() + 8) > self.config.m_blocksize {
            panic!("impossible case");
        }
        let mut block: Vec<u8> = Vec::with_capacity(self.config.m_blocksize);
        block.extend_from_slice(&(stats.len() as u64).to_be_bytes());
        block.extend_from_slice(stats.as_bytes());
        self.indx_tx.send(block);

        // flush metadata
        let blocks = ((metadata.len() + 8) / self.config.m_blocksize) + 1;
        let mut data: Vec<u8> = Vec::with_capacity(blocks * self.config.m_blocksize);
        let scratch = (metadata.len() as u64).to_be_bytes();
        data.extend_from_slice(&metadata);
        data.resize(blocks * self.config.m_blocksize, 0);
        let loc = data.len() - 8;
        data[loc..].copy_from_slice(&scratch);
        self.indx_tx.send(data);

        // flush marker block and close
        self.indx_tx.close_wait();
        self.vlog_tx.take().map(|x| x.close_wait());

        Ok(())
    }

    fn finalize1(&mut self, b: &mut BuildData<K, V>, first_key: Option<K>) {
        let mut m = b.mstack.pop().unwrap();
        if let Err(_) = m.insertz(&first_key, b.z_fpos) {
            let mbytes = m.finalize(&mut self.stats);
            m.flush(&mut self.indx_tx);
            b.update_m_flush(mbytes);
            self.insertms(m.first_key(), b);
            m.reset();
            m.insertz(&first_key, b.z_fpos).unwrap();
            b.mstack.push(m);
            b.reset();
        }
    }

    fn finalize2(&mut self, b: &mut BuildData<K, V>) {
        while let Some(mut m) = b.mstack.pop() {
            let mbytes = m.finalize(&mut self.stats);
            m.flush(&mut self.indx_tx);
            b.update_m_flush(mbytes);
            self.insertms(m.first_key(), b);
            b.reset();
        }
    }

    fn insertms(&mut self, first_key: Option<K>, b: &mut BuildData<K, V>) {
        let (mut m0, overflow, m_fpos) = match b.mstack.pop() {
            Some(mut m0) => {
                let overflow = m0.insertm(&first_key, b.m_fpos) == false;
                (m0, overflow, b.m_fpos)
            }
            None => {
                let mut m0 = MBlock::new_encode(self.config.clone());
                if m0.insertm(&first_key, b.m_fpos) == false {
                    panic!("impossible situation");
                }
                (m0, false, b.m_fpos)
            }
        };
        if overflow {
            let mbytes = m0.finalize(&mut self.stats);
            m0.flush(&mut self.indx_tx);
            b.update_m_flush(mbytes);
            self.insertms(m0.first_key(), b);
            m0.reset();
            if m0.insertm(&first_key, m_fpos) == false {
                panic!("impossible situation");
            }
        }
        b.mstack.push(m0);
    }

    fn preprocess_entry(&mut self, entry: &mut core::Entry<K, V>) -> bool {
        self.stats.maxseqno = cmp::max(self.stats.maxseqno, entry.seqno());
        self.purge_values(entry)
    }

    fn postprocess_entry(&mut self, entry: &mut core::Entry<K, V>) {
        self.stats.n_count += 1;
        if entry.is_deleted() {
            self.stats.n_deleted += 1;
        }
    }

    fn purge_values(&self, entry: &mut core::Entry<K, V>) -> bool {
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

struct FlushClient {
    tx: mpsc::SyncSender<Vec<u8>>,
    handle: thread::JoinHandle<()>,
}

impl FlushClient {
    fn send(&mut self, block: Vec<u8>) {
        self.tx.send(block).unwrap();
    }

    fn close_wait(self) {
        mem::drop(self.tx);
        self.handle.join().unwrap();
    }
}

#[derive(Default)]
pub struct Stats {
    n_count: usize,
    n_deleted: usize,
    maxseqno: u64,
    n_abytes: usize,
    keymem: usize,
    valmem: usize,
    z_bytes: usize,
    v_bytes: usize,
    m_bytes: usize,
    padding: usize,
}

pub struct BuildData<K, V>
where
    K: Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
{
    z: ZBlock<K, V>,
    mstack: Vec<MBlock<K>>,
    fpos: u64,
    z_fpos: u64,
    m_fpos: u64,
    vlog_fpos: u64,
}

impl<K, V> BuildData<K, V>
where
    K: Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
{
    fn new(n_abytes: usize, config: Config) -> BuildData<K, V> {
        let vlog_fpos = n_abytes as u64;
        let mut obj = BuildData {
            z: ZBlock::new_encode(vlog_fpos, config.clone()),
            mstack: vec![],
            z_fpos: 0,
            m_fpos: 0,
            fpos: 0,
            vlog_fpos,
        };
        obj.mstack.push(MBlock::new_encode(config));
        obj
    }

    fn update_z_flush(&mut self, zbytes: usize, vbytes: usize) {
        self.fpos += zbytes as u64;
        self.vlog_fpos += vbytes as u64;
    }

    fn update_m_flush(&mut self, mbytes: usize) {
        self.m_fpos = self.fpos;
        self.fpos += mbytes as u64;
    }

    fn reset(&mut self) {
        self.z_fpos = self.fpos;
        self.m_fpos = self.fpos;
        self.z.reset(self.vlog_fpos);
    }
}
