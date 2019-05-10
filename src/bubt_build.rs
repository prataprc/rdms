// TODO: flush put blocks into tx channel. Right now we simply unwrap()

use lazy_static::lazy_static;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::{cmp, ffi, fs, io::Write, marker, mem, path, thread, time};

use crate::bubt_config::{Config, MetaItem};
use crate::bubt_stats::Stats;
use crate::core::{Diff, Entry, Result, Serialize};
use crate::error::BognError;
use crate::vlog;

include!("./bubt_indx.rs");

pub struct Builder<K, V>
where
    K: Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
{
    name: String,
    config: Config,
    i_flusher: FlushClient,
    v_flusher: Option<FlushClient>,
    stats: Stats,

    phantom_key: marker::PhantomData<K>,
    phantom_val: marker::PhantomData<V>,
}

impl<K, V> Builder<K, V>
where
    K: Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
{
    pub fn initial(name: String, config: Config) -> Result<Builder<K, V>> {
        let i_flusher = FlushClient::new(config.index_file(&name), false)?;
        let v_flusher = if config.vlog_ok {
            Some(FlushClient::new(config.vlog_file(&name), false)?)
        } else {
            None
        };

        let mut stats: Stats = From::from(config.clone());
        stats.name = name.clone();

        Ok(Builder {
            name,
            config,
            i_flusher,
            v_flusher,
            stats,
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        })
    }

    pub fn incremental(name: String, config: Config) -> Result<Builder<K, V>> {
        let i_flusher = FlushClient::new(config.index_file(&name), false)?;
        let v_flusher = if config.vlog_ok {
            Some(FlushClient::new(config.vlog_file(&name), true)?)
        } else {
            None
        };

        let mut stats: Stats = From::from(config.clone());
        stats.name = name.clone();
        stats.n_abytes = v_flusher
            .as_ref()
            .map_or(Default::default(), |x| x.fpos as usize);

        Ok(Builder {
            name,
            config: config.clone(),
            i_flusher,
            v_flusher,
            stats: From::from(config),
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        })
    }

    pub fn build<I>(mut self, mut iter: I, metadata: Vec<u8>) -> Result<()>
    where
        I: Iterator<Item = Result<Entry<K, V>>>,
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
                    b.z.flush(&mut self.i_flusher, self.v_flusher.as_mut());
                    b.update_z_flush(zbytes, vbytes);
                    let mut m = b.mstack.pop().unwrap();
                    if let Err(_) = m.insertz(first_key.as_ref().unwrap(), b.z_fpos) {
                        let mbytes = m.finalize(&mut self.stats);
                        m.flush(&mut self.i_flusher);
                        b.update_m_flush(mbytes);
                        self.insertms(m.first_key(), &mut b)?;
                        m.reset();
                        m.insertz(first_key.as_ref().unwrap(), b.z_fpos)?;
                    }
                    b.mstack.push(m);
                    b.reset();
                    b.z.insert(&entry, &mut self.stats)?;
                }
                Err(_) => unreachable!(),
            };
            self.postprocess_entry(&mut entry);
        }

        // flush final set partial blocks
        if self.stats.n_count > 0 {
            self.finalize1(&mut b); // flush zblock
            self.finalize2(&mut b); // flush mblocks
        }

        // start building metadata items for index files
        let mut meta_items: Vec<MetaItem> = vec![];

        // meta-stats
        self.stats.buildtime = start.elapsed().unwrap().as_nanos() as u64;
        self.stats.epoch = time::UNIX_EPOCH.elapsed().unwrap().as_nanos() as i128;
        let stats = self.stats.to_string();
        if (stats.len() + 8) > self.config.m_blocksize {
            panic!("impossible case");
        }
        meta_items.push(MetaItem::Stats(stats));
        // metadata
        meta_items.push(MetaItem::Metadata(metadata));
        // flush them down
        self.config
            .write_meta_items(meta_items, &mut self.i_flusher);

        // flush marker block and close
        self.i_flusher.close_wait();
        self.v_flusher.take().map(|x| x.close_wait());

        Ok(())
    }

    fn insertms(&mut self, first_key: Option<K>, b: &mut BuildData<K, V>) -> Result<()> {
        let first_key = first_key.as_ref().unwrap();
        let (mut m0, overflow, m_fpos) = match b.mstack.pop() {
            Some(mut m0) => match m0.insertm(first_key, b.m_fpos) {
                Ok(_) => (m0, false, b.m_fpos),
                Err(_) => (m0, true, b.m_fpos),
            },
            None => {
                let mut m0 = MBlock::new_encode(self.config.clone());
                m0.insertm(first_key, b.m_fpos)?;
                (m0, false, b.m_fpos)
            }
        };
        if overflow {
            let mbytes = m0.finalize(&mut self.stats);
            m0.flush(&mut self.i_flusher);
            b.update_m_flush(mbytes);
            self.insertms(m0.first_key(), b)?;
            m0.reset();
            m0.insertm(first_key, m_fpos)?;
        }
        b.mstack.push(m0);
        Ok(())
    }

    fn finalize1(&mut self, b: &mut BuildData<K, V>) -> Result<()> {
        if let Some(first_key) = b.z.first_key() {
            let (zbytes, vbytes) = b.z.finalize(&mut self.stats);
            b.z.flush(&mut self.i_flusher, self.v_flusher.as_mut());
            b.update_z_flush(zbytes, vbytes);
            let mut m = b.mstack.pop().unwrap();
            if let Err(_) = m.insertz(&first_key, b.z_fpos) {
                let mbytes = m.finalize(&mut self.stats);
                m.flush(&mut self.i_flusher);
                b.update_m_flush(mbytes);
                self.insertms(m.first_key(), b)?;

                m.reset();
                m.insertz(&first_key, b.z_fpos)?;
                b.reset();
            }
            b.mstack.push(m);
        };
        Ok(())
    }

    fn finalize2(&mut self, b: &mut BuildData<K, V>) -> Result<()> {
        while let Some(mut m) = b.mstack.pop() {
            if let Some(first_key) = m.first_key() {
                let mbytes = m.finalize(&mut self.stats);
                m.flush(&mut self.i_flusher);
                b.update_m_flush(mbytes);
                self.insertms(Some(first_key), b)?;

                b.reset();
            }
        }
        Ok(())
    }

    fn preprocess_entry(&mut self, entry: &mut Entry<K, V>) -> bool {
        self.stats.maxseqno = cmp::max(self.stats.maxseqno, entry.seqno());
        self.purge_values(entry)
    }

    fn postprocess_entry(&mut self, entry: &mut Entry<K, V>) {
        self.stats.n_count += 1;
        if entry.is_deleted() {
            self.stats.n_deleted += 1;
        }
    }

    fn purge_values(&self, entry: &mut Entry<K, V>) -> bool {
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

pub(crate) struct FlushClient {
    tx: mpsc::SyncSender<Vec<u8>>,
    handle: thread::JoinHandle<()>,
    fpos: u64,
}

impl FlushClient {
    fn new(file: ffi::OsString, append: bool) -> Result<FlushClient> {
        let fd = Self::open_file(file.clone(), append)?;
        let (flusher, tx, rx) = Flusher::new(file.clone(), fd);
        let fpos = if append {
            fs::metadata(file)?.len()
        } else {
            Default::default()
        };
        let handle = thread::spawn(move || flusher.run(rx));
        Ok(FlushClient { tx, handle, fpos })
    }

    pub(crate) fn send(&mut self, block: Vec<u8>) {
        self.tx.send(block).unwrap();
    }

    fn close_wait(self) {
        mem::drop(self.tx);
        self.handle.join().unwrap();
    }

    fn open_file(file: ffi::OsString, append: bool) -> Result<fs::File> {
        let p = path::Path::new(&file);
        let parent = p.parent().ok_or(BognError::InvalidFile(file.clone()))?;
        fs::create_dir_all(parent)?;

        let mut opts = fs::OpenOptions::new();
        Ok(match append {
            false => opts.append(true).create_new(true).open(p)?,
            true => opts.append(true).open(p)?,
        })
    }
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
