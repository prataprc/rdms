// TODO: flush put blocks into tx channel. Right now we simply unwrap()

use std::ops::Bound;
use std::sync::mpsc;
use std::{cmp, convert::TryInto, ffi, fs, io::Write, marker, mem, thread, time};

use crate::core::{Diff, Entry, Result, Serialize};
use crate::error::Error;
use crate::robt::Stats;
use crate::robt::{self, Config, MetaItem, ROOT_MARKER};
use crate::robt_index::{MBlock, ZBlock};
use crate::util;

/// Builder type for Read-Only-BTree.
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
    pub fn initial(
        dir: &str, // directory path where index file(s) are stored
        name: &str,
        config: Config,
    ) -> Result<Builder<K, V>> {
        let create = true;
        let iflusher = {
            let file = config.to_index_file(dir, name);
            Flusher::new(file, config.clone(), create)?
        };
        let vflusher = config
            .to_value_log(dir, name)
            .map(|file| Flusher::new(file, config.clone(), create))
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
    pub fn incremental(config: Config, dir: &str, name: &str) -> Result<Builder<K, V>> {
        let iflusher = {
            let file = config.to_index_file(dir, name);
            Flusher::new(file, config.clone(), true /*create*/)?
        };
        let vflusher = config
            .to_value_log(dir, name)
            .map(|file| Flusher::new(file, config.clone(), false /*create*/))
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
        let (took, root): (u64, u64) = {
            let start = time::SystemTime::now();
            let root = self.build_tree(iter)?;
            (
                start.elapsed().unwrap().as_nanos().try_into().unwrap(),
                root,
            )
        };

        // meta-stats
        let stats: String = {
            self.stats.build_time = took;
            let epoch: i128 = time::UNIX_EPOCH
                .elapsed()
                .unwrap()
                .as_nanos()
                .try_into()
                .unwrap();
            self.stats.epoch = epoch;
            self.stats.to_string()
        };

        // start building metadata items for index files
        let meta_items: Vec<MetaItem> = vec![
            MetaItem::Root(root),
            MetaItem::Metadata(metadata),
            MetaItem::Stats(stats),
            MetaItem::Marker(ROOT_MARKER.clone()), // tip of the index.
        ];
        // flush them to disk
        robt::write_meta_items(self.iflusher.file.clone(), meta_items)?;

        // flush marker block and close
        self.iflusher.close_wait()?;
        self.vflusher.take().map(|x| x.close_wait()).transpose()?;

        Ok(())
    }

    fn build_tree<I>(&mut self, mut iter: I) -> Result<u64>
    where
        I: Iterator<Item = Result<Entry<K, V>>>,
    {
        struct Context<K, V>
        where
            K: Clone + Ord + Serialize,
            V: Clone + Diff + Serialize,
            <V as Diff>::D: Serialize,
        {
            fpos: u64,
            zfpos: u64,
            vfpos: u64,
            z: ZBlock<K, V>,
            ms: Vec<MBlock<K, V>>,
        };
        let mut c = {
            let vfpos = self.stats.n_abytes.try_into().unwrap();
            Context {
                fpos: 0,
                zfpos: 0,
                vfpos,
                z: ZBlock::new_encode(vfpos, self.config.clone()),
                ms: vec![MBlock::new_encode(self.config.clone())],
            }
        };

        for entry in iter.next() {
            let mut entry = match self.preprocess(entry?) {
                Some(entry) => entry,
                None => continue,
            };

            match c.z.insert(&entry, &mut self.stats) {
                Ok(_) => (),
                Err(Error::__ZBlockOverflow(_)) => {
                    let (zbytes, vbytes) = c.z.finalize(&mut self.stats);
                    c.z.flush(&mut self.iflusher, self.vflusher.as_mut())?;
                    c.fpos += zbytes;
                    c.vfpos += vbytes;

                    let mut m = c.ms.pop().unwrap();
                    match m.insertz(c.z.as_first_key(), c.zfpos) {
                        Ok(_) => (),
                        Err(Error::__MBlockOverflow(_)) => {
                            let x = m.finalize(&mut self.stats);
                            m.flush(&mut self.iflusher)?;
                            let k = m.as_first_key();
                            let r = self.insertms(c.ms, c.fpos + x, k, c.fpos)?;
                            c.ms = r.0;
                            c.fpos = r.1;

                            m.reset();
                            m.insertz(c.z.as_first_key(), c.zfpos).unwrap();
                        }
                        Err(err) => return Err(err),
                    }
                    c.ms.push(m);

                    c.zfpos = c.fpos;
                    c.z.reset(c.vfpos);

                    c.z.insert(&entry, &mut self.stats).unwrap();
                }
                Err(err) => return Err(err),
            };

            self.postprocess(&mut entry);
        }

        // flush final z-block
        if c.z.has_first_key() {
            let (zbytes, _vbytes) = c.z.finalize(&mut self.stats);
            c.z.flush(&mut self.iflusher, self.vflusher.as_mut())?;
            c.fpos += zbytes;
            // vfpos += vbytes; TODO: is this required ?

            let mut m = c.ms.pop().unwrap();
            match m.insertz(c.z.as_first_key(), c.zfpos) {
                Ok(_) => (),
                Err(Error::__MBlockOverflow(_)) => {
                    let x = m.finalize(&mut self.stats);
                    m.flush(&mut self.iflusher)?;
                    let mkey = m.as_first_key();
                    let res = self.insertms(c.ms, c.fpos + x, mkey, c.fpos)?;
                    c.ms = res.0;
                    c.fpos = res.1;

                    m.reset();
                    m.insertz(c.z.as_first_key(), c.zfpos)?;
                }
                Err(err) => return Err(err),
            }
            c.ms.push(m);
        }
        // flush final set of m-blocks
        if c.ms.len() > 0 {
            while let Some(mut m) = c.ms.pop() {
                if m.has_first_key() {
                    let x = m.finalize(&mut self.stats);
                    m.flush(&mut self.iflusher)?;
                    let mkey = m.as_first_key();
                    let res = self.insertms(c.ms, c.fpos + x, mkey, c.fpos)?;
                    c.ms = res.0;
                    c.fpos = res.1
                }
            }
        }
        Ok(c.fpos)
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
                Err(err) => return Err(err),
            },
        };
        ms.push(m0);
        Ok((ms, fpos))
    }

    // return whether this entry can be skipped.
    fn preprocess(&mut self, entry: Entry<K, V>) -> Option<Entry<K, V>> {
        self.stats.seqno = cmp::max(self.stats.seqno, entry.to_seqno());

        // if tombstone purge is configured, then purge all versions on or
        // before the purge-seqno.
        match self.config.tomb_purge {
            Some(before) => entry.purge(Bound::Excluded(before)),
            _ => Some(entry),
        }
    }

    fn postprocess(&mut self, entry: &mut Entry<K, V>) {
        self.stats.n_count += 1;
        if entry.is_deleted() {
            self.stats.n_deleted += 1;
        }
    }
}

pub(crate) struct Flusher {
    file: ffi::OsString,
    fpos: u64,
    thread: thread::JoinHandle<Result<()>>,
    tx: mpsc::SyncSender<Vec<u8>>,
}

impl Flusher {
    fn new(
        file: ffi::OsString,
        config: Config,
        create: bool, // if true create a new file
    ) -> Result<Flusher> {
        let (fd, fpos) = if create {
            (util::open_file_cw(file.clone())?, Default::default())
        } else {
            (util::open_file_w(&file)?, fs::metadata(&file)?.len())
        };

        let (tx, rx) = mpsc::sync_channel(config.flush_queue_size);
        let file1 = file.clone();
        let thread = thread::spawn(move || thread_flush(file1, fd, rx));

        Ok(Flusher {
            file,
            fpos,
            thread,
            tx,
        })
    }

    // return error if flush thread has exited/paniced.
    pub(crate) fn send(&mut self, block: Vec<u8>) -> Result<()> {
        self.tx.send(block)?;
        Ok(())
    }

    // return the cause thread failure if there is a failure, or return
    // a known error like io::Error or PartialWrite.
    fn close_wait(self) -> Result<()> {
        mem::drop(self.tx);
        match self.thread.join() {
            Ok(res) => res,
            Err(err) => match err.downcast_ref::<String>() {
                Some(msg) => Err(Error::ThreadFail(msg.to_string())),
                None => Err(Error::ThreadFail("unknown error".to_string())),
            },
        }
    }
}

fn thread_flush(
    file: ffi::OsString, // for debuging purpose
    mut fd: fs::File,
    rx: mpsc::Receiver<Vec<u8>>,
) -> Result<()> {
    for data in rx.iter() {
        let n = fd.write(&data)?;
        if n != data.len() {
            let msg = format!("flusher: {:?} {}/{}...", &file, data.len(), n);
            return Err(Error::PartialWrite(msg));
        }
    }
    // file descriptor and receiver channel shall be dropped.
    Ok(())
}
