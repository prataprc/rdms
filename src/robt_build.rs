// TODO: flush put blocks into tx channel. Right now we simply unwrap()

use std::sync::mpsc;
use std::{cmp, convert::TryInto, fs, io::Write, marker, mem, thread, time};

use crate::core::{Diff, Entry, Result, Serialize};
use crate::error::Error;
use crate::robt_config::{self, Config, MetaItem, MARKER_BLOCK};
use crate::robt_indx::{MBlock, ZBlock};
use crate::robt_stats::Stats;
use crate::util;

/// Build a new instance of Read-Only-BTree. ROBT instances shall have
/// an index file and an optional value-log-file, refer to [``Config``]
/// for more information.
pub struct Builder<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    config: Config,
    iflusher: Flusher,
    vflusher: Option<Flusher>,
    stats: Stats,

    phantom_key: marker::PhantomData<K>,
    phantom_val: marker::PhantomData<V>,
}

impl<K, V> Builder<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    /// For initial builds, index file and value-log-file, if any,
    /// are always created new.
    pub fn initial(config: Config) -> Result<Builder<K, V>> {
        let iflusher = {
            let file = config.to_index_file();
            Flusher::new(file, config.clone(), false /*reuse*/)?
        };
        let vflusher = config
            .to_value_log()
            .map(|file| Flusher::new(file, config.clone(), false /*reuse*/))
            .transpose()?;

        Ok(Builder {
            config: config.clone(),
            iflusher,
            vflusher,
            stats: From::from(config),
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        })
    }

    /// For incremental build, index file is created new, while
    /// value-log-file, if any, is appended to older version.
    pub fn incremental(config: Config) -> Result<Builder<K, V>> {
        let iflusher = {
            let file = config.to_index_file();
            Flusher::new(file, config.clone(), false /*reuse*/)?
        };
        let vflusher = config
            .to_value_log()
            .map(|file| Flusher::new(file, config.clone(), true /*reuse*/))
            .transpose()?;

        let mut stats: Stats = From::from(config.clone());
        stats.n_abytes += vflusher.as_ref().map_or(0, |vf| vf.fpos) as usize;

        Ok(Builder {
            config: config.clone(),
            iflusher,
            vflusher,
            stats,
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        })
    }

    /// Build a new index.
    pub fn build<I>(mut self, iter: I, metadata: Vec<u8>) -> Result<()>
    where
        I: Iterator<Item = Result<Entry<K, V>>>,
    {
        let start = time::SystemTime::now();
        self.build_tree(iter)?;
        let took: u64 = start.elapsed().unwrap().as_nanos().try_into().unwrap();

        // start building metadata items for index files
        let mut meta_items: Vec<MetaItem> = vec![];

        // meta-stats
        self.stats.buildtime = took;
        let epoch: i128 = time::UNIX_EPOCH
            .elapsed()
            .unwrap()
            .as_nanos()
            .try_into()
            .unwrap();
        self.stats.epoch = epoch;
        let stats = self.stats.to_string();
        if (stats.len() + 8) > self.config.m_blocksize {
            panic!("stats({}) > {}", stats.len(), self.config.m_blocksize);
        }
        meta_items.push(MetaItem::Stats(stats));

        // metadata
        meta_items.push(MetaItem::Metadata(metadata));

        // marker
        meta_items.push(MetaItem::Marker(MARKER_BLOCK.clone()));

        // flush them down
        robt_config::write_meta_items(meta_items, &mut self.iflusher)?;

        // flush marker block and close
        self.iflusher.close_wait()?;
        self.vflusher.take().map(|x| x.close_wait()).transpose()?;

        Ok(())
    }

    fn build_tree<I>(&mut self, mut iter: I) -> Result<()>
    where
        I: Iterator<Item = Result<Entry<K, V>>>,
    {
        let mut vfpos: u64 = self.stats.n_abytes.try_into().unwrap();
        let (mut fpos, mut zfpos) = (0_u64, 0_u64);
        let mut ms: Vec<MBlock<K, V>> = vec![
            // If there is atleat one z-block, there will be atleat one m-block
            MBlock::new_encode(self.config.clone()),
        ];
        let mut z = ZBlock::new_encode(vfpos, self.config.clone());

        for entry in iter.next() {
            let mut entry = entry?;
            if self.preprocess_entry(&mut entry) {
                continue;
            }

            match z.insert(&entry, &mut self.stats) {
                Ok(_) => (),
                Err(Error::__ZBlockOverflow(_)) => {
                    let (zbytes, vbytes) = z.finalize(&mut self.stats);
                    z.flush(&mut self.iflusher, self.vflusher.as_mut())?;
                    fpos += zbytes;
                    vfpos += vbytes;

                    let mut m = ms.pop().unwrap();
                    match m.insertz(z.as_first_key(), zfpos) {
                        Ok(_) => (),
                        Err(Error::__MBlockOverflow(_)) => {
                            let x = m.finalize(&mut self.stats);
                            m.flush(&mut self.iflusher)?;
                            let mkey = m.as_first_key();
                            let res = self.insertms(ms, fpos + x, mkey, fpos)?;
                            ms = res.0;
                            fpos = res.1;

                            m.reset();
                            m.insertz(z.as_first_key(), zfpos).unwrap();
                        }
                        _ => unreachable!(),
                    }
                    ms.push(m);

                    zfpos = fpos;
                    z.reset(vfpos);

                    z.insert(&entry, &mut self.stats).unwrap();
                }
                _ => unreachable!(),
            };

            self.postprocess_entry(&mut entry);
        }

        // flush final z-block
        if z.has_first_key() {
            let (zbytes, _vbytes) = z.finalize(&mut self.stats);
            z.flush(&mut self.iflusher, self.vflusher.as_mut())?;
            fpos += zbytes;
            // vfpos += vbytes; TODO: is this required ?

            let mut m = ms.pop().unwrap();
            match m.insertz(z.as_first_key(), zfpos) {
                Ok(_) => (),
                Err(Error::__MBlockOverflow(_)) => {
                    let x = m.finalize(&mut self.stats);
                    m.flush(&mut self.iflusher)?;
                    let mkey = m.as_first_key();
                    let res = self.insertms(ms, fpos + x, mkey, fpos)?;
                    ms = res.0;
                    fpos = res.1;

                    m.reset();
                    m.insertz(z.as_first_key(), zfpos)?;
                }
                _ => unreachable!(),
            }
            ms.push(m);
        }
        // flush final set of m-blocks
        if ms.len() > 0 {
            while let Some(mut m) = ms.pop() {
                if m.has_first_key() {
                    let x = m.finalize(&mut self.stats);
                    m.flush(&mut self.iflusher)?;
                    let mkey = m.as_first_key();
                    let res = self.insertms(ms, fpos + x, mkey, fpos)?;
                    ms = res.0;
                    fpos = res.1
                }
            }
        }
        Ok(())
    }

    fn insertms(
        &mut self,
        mut ms: Vec<MBlock<K, V>>,
        mut fpos: u64,
        key: &K,
        mfpos: u64,
    ) -> Result<(Vec<MBlock<K, V>>, u64)> {
        let m0 = ms.pop();
        let m0 = match m0 {
            None => {
                let mut m0 = MBlock::new_encode(self.config.clone());
                m0.insertm(key, mfpos).unwrap();
                m0
            }
            Some(mut m0) => match m0.insertm(key, mfpos) {
                Ok(_) => m0,
                Err(Error::__MBlockOverflow(_)) => {
                    let x = m0.finalize(&mut self.stats);
                    m0.flush(&mut self.iflusher)?;
                    let mkey = m0.as_first_key();
                    let res = self.insertms(ms, fpos + x, mkey, fpos)?;
                    ms = res.0;
                    fpos = res.1;

                    m0.reset();
                    m0.insertm(key, mfpos).unwrap();
                    m0
                }
                _ => unreachable!(),
            },
        };
        ms.push(m0);
        Ok((ms, fpos))
    }

    // return whether this entry can be skipped.
    fn preprocess_entry(&mut self, entry: &mut Entry<K, V>) -> bool {
        self.stats.seqno = cmp::max(self.stats.seqno, entry.to_seqno());

        // if tombstone purge is configured, then purge.
        match self.config.tomb_purge {
            Some(before) => entry.purge(before),
            _ => false,
        }
    }

    fn postprocess_entry(&mut self, entry: &mut Entry<K, V>) {
        self.stats.n_count += 1;
        if entry.is_deleted() {
            self.stats.n_deleted += 1;
        }
    }
}

pub(crate) struct Flusher {
    tx: mpsc::SyncSender<(Vec<u8>, mpsc::SyncSender<Result<()>>)>,
    handle: thread::JoinHandle<Result<()>>,
    fpos: u64,
}

impl Flusher {
    fn new(file: String, config: Config, reuse: bool) -> Result<Flusher> {
        let fd = util::open_file_w(&file, reuse)?;
        let fpos = if reuse {
            fs::metadata(&file)?.len()
        } else {
            Default::default()
        };

        let (tx, rx) = mpsc::sync_channel(config.flush_queue_size);
        let handle = thread::spawn(move || flush_thread(file, fd, rx));

        Ok(Flusher { tx, handle, fpos })
    }

    // return the cause thread failure if there is a failure, or return
    // a known error like io::Error or PartialWrite.
    fn close_wait(self) -> Result<()> {
        mem::drop(self.tx);
        match self.handle.join() {
            Ok(res) => res,
            Err(err) => match err.downcast_ref::<String>() {
                Some(msg) => Err(Error::ThreadFail(msg.to_string())),
                None => Err(Error::ThreadFail("unknown error".to_string())),
            },
        }
    }

    // return error if flush thread has exited/paniced.
    pub(crate) fn send(&mut self, block: Vec<u8>) -> Result<()> {
        let (tx, rx) = mpsc::sync_channel(0);
        self.tx.send((block, tx))?;
        rx.recv()?
    }
}

fn flush_thread(
    file: String, // for debuging purpose
    mut fd: fs::File,
    rx: mpsc::Receiver<(Vec<u8>, mpsc::SyncSender<Result<()>>)>,
) -> Result<()> {
    let mut write_data = |data: &[u8]| -> Result<()> {
        let n = fd.write(data)?;
        if n == data.len() {
            Ok(())
        } else {
            let msg = format!("flusher: {:?} {}/{}...", &file, data.len(), n);
            Err(Error::PartialWrite(msg))
        }
    };

    for (data, tx) in rx.iter() {
        write_data(&data)?;
        tx.send(Ok(()))?;
    }
    // file descriptor and receiver channel shall be dropped.
    Ok(())
}
