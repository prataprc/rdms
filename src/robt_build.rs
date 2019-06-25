// TODO: flush put blocks into tx channel. Right now we simply unwrap()

use std::sync::mpsc;
use std::{cmp, convert::TryInto, fs, io::Write, marker, mem, thread, time};

use crate::core::{Diff, Entry, Serialize};
use crate::error::Error;
use crate::robt_config::{self, Config, MetaItem, MARKER_BLOCK};
use crate::robt_indx::{MBlock, ZBlock};
use crate::robt_stats::Stats;
use crate::util;

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
    pub fn initial(config: Config) -> Result<Builder<K, V>, Error> {
        let (index_reuse, vlog_reuse) = (false, false);
        let iflusher = Flusher::new(config.to_index_file(), index_reuse)?;
        let vflusher = match config.to_value_log() {
            Some(vlog_file) => Some(Flusher::new(vlog_file, vlog_reuse)?),
            None => None,
        };

        Ok(Builder {
            config: config.clone(),
            iflusher,
            vflusher,
            stats: From::from(config),
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        })
    }

    pub fn incremental(config: Config) -> Result<Builder<K, V>, Error> {
        let (index_reuse, vlog_reuse) = (false, true);
        let iflusher = Flusher::new(config.to_index_file(), index_reuse)?;
        let (vflusher, n_abytes) = match config.to_value_log() {
            Some(vlog_file) => {
                let vf = Flusher::new(vlog_file, vlog_reuse)?;
                let fpos: usize = vf.fpos.try_into().unwrap();
                (Some(vf), fpos)
            }
            None => (None, usize::default()),
        };
        let mut stats: Stats = From::from(config.clone());
        stats.n_abytes += n_abytes;

        Ok(Builder {
            config: config.clone(),
            iflusher,
            vflusher,
            stats,
            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        })
    }

    pub fn build<I>(mut self, iter: I, metadata: Vec<u8>) -> Result<(), Error>
    where
        I: Iterator<Item = Result<Entry<K, V>, Error>>,
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
        robt_config::write_meta_items(meta_items, &mut self.iflusher);

        // flush marker block and close
        self.iflusher.close_wait();
        self.vflusher.take().map(|x| x.close_wait());

        Ok(())
    }

    fn build_tree<I>(&mut self, mut iter: I) -> Result<(), Error>
    where
        I: Iterator<Item = Result<Entry<K, V>, Error>>,
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
                    z.flush(&mut self.iflusher, self.vflusher.as_mut());
                    fpos += zbytes;
                    vfpos += vbytes;

                    let mut m = ms.pop().unwrap();
                    if let Err(_) = m.insertz(z.as_first_key(), zfpos) {
                        let x = m.finalize(&mut self.stats);
                        m.flush(&mut self.iflusher);
                        let mkey = m.as_first_key();
                        let res = self.insertms(ms, fpos + x, mkey, fpos)?;
                        ms = res.0;
                        fpos = res.1;

                        m.reset();
                        m.insertz(z.as_first_key(), zfpos).unwrap();
                    }
                    ms.push(m);

                    zfpos = fpos;
                    z.reset(vfpos);

                    z.insert(&entry, &mut self.stats).unwrap();
                }
                Err(_) => unreachable!(),
            };

            self.postprocess_entry(&mut entry);
        }

        // flush final z-block
        if z.has_first_key() {
            let (zbytes, _vbytes) = z.finalize(&mut self.stats);
            z.flush(&mut self.iflusher, self.vflusher.as_mut());
            fpos += zbytes;
            // vfpos += vbytes; TODO: is this required ?

            let mut m = ms.pop().unwrap();
            if let Err(_) = m.insertz(z.as_first_key(), zfpos) {
                let x = m.finalize(&mut self.stats);
                m.flush(&mut self.iflusher);
                let mkey = m.as_first_key();
                let res = self.insertms(ms, fpos + x, mkey, fpos)?;
                ms = res.0;
                fpos = res.1;

                m.reset();
                m.insertz(z.as_first_key(), zfpos)?;
            }
            ms.push(m);
        }
        // flush final set of m-blocks
        if ms.len() > 0 {
            while let Some(mut m) = ms.pop() {
                if m.has_first_key() {
                    let x = m.finalize(&mut self.stats);
                    m.flush(&mut self.iflusher);
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
    ) -> Result<(Vec<MBlock<K, V>>, u64), Error> {
        let m0 = ms.pop();
        let m0 = match m0 {
            None => {
                let mut m0 = MBlock::new_encode(self.config.clone());
                m0.insertm(key, mfpos).unwrap();
                m0
            }
            Some(mut m0) => match m0.insertm(key, mfpos) {
                Ok(_) => m0,
                Err(_) => {
                    let x = m0.finalize(&mut self.stats);
                    m0.flush(&mut self.iflusher);
                    let mkey = m0.as_first_key();
                    let res = self.insertms(ms, fpos + x, mkey, fpos)?;
                    ms = res.0;
                    fpos = res.1;

                    m0.reset();
                    m0.insertm(key, mfpos).unwrap();
                    m0
                }
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
    tx: mpsc::SyncSender<Vec<u8>>,
    handle: thread::JoinHandle<()>,
    fpos: u64,
}

impl Flusher {
    fn new(file: String, reuse: bool) -> Result<Flusher, Error> {
        let fpos = if reuse {
            fs::metadata(&file)?.len()
        } else {
            Default::default()
        };
        let fd = util::open_file_w(&file, reuse)?;

        let (tx, rx) = mpsc::sync_channel(16); // TODO: No magic number
        let handle = thread::spawn(move || flush_thread(file, fd, rx));

        Ok(Flusher { tx, handle, fpos })
    }

    fn close_wait(self) {
        mem::drop(self.tx);
        self.handle.join().unwrap();
    }

    pub(crate) fn send(&mut self, block: Vec<u8>) {
        self.tx.send(block).unwrap();
    }
}

fn flush_thread(file: String, mut fd: fs::File, rx: mpsc::Receiver<Vec<u8>>) {
    let write_data = |file: &str, fd: &mut fs::File, data: &[u8]| -> bool {
        let res = match fd.write(data) {
            Err(err) => {
                panic!("flusher: {:?} error {}...", file, err);
            }
            Ok(n) if n != data.len() => {
                panic!("flusher: {:?} write {}/{}...", file, n, data.len());
            }
            Ok(_) => true,
        };
        res
    };

    for data in rx.iter() {
        if !write_data(&file, &mut fd, &data) {
            break;
        }
    }
    // file descriptor and receiver channel shall be dropped.
}
