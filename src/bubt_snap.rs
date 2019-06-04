// TODO: review resize() calls
// TODO: review "as type conversions" for llrb-index jsondata
// TODO: implement log() and validate() API.
// TODO: cache root block.

use std::borrow::Borrow;
use std::{fs, marker, ops::Bound, path};

use crate::bubt_config::{self, Config, MetaItem};
use crate::bubt_indx::{MBlock, ZBlock};
use crate::bubt_stats::Stats;
use crate::core::{Diff, Entry, Result, Serialize};
use crate::error::Error;
use crate::util;
use crate::vlog;

pub struct Snapshot<K, V>
where
    K: Default + Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    config: Config,
    stats: Stats,
    metadata: Vec<u8>,
    root: u64,
    index_fd: fs::File,
    vlog_fd: Option<fs::File>,

    phantom_key: marker::PhantomData<K>,
    phantom_val: marker::PhantomData<V>,
}

// Construction methods.
impl<K, V> Snapshot<K, V>
where
    K: Default + Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    pub fn open(dir: &str, name: &str) -> Result<Snapshot<K, V>> {
        let index_fd = util::open_file_r(&Config::index_file(dir, name))?;

        let mut snap = Snapshot {
            metadata: Default::default(),
            stats: Default::default(),
            config: Default::default(),
            root: Default::default(),
            index_fd,
            vlog_fd: Default::default(),

            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        };

        let mut iter = bubt_config::read_meta_items(dir, name)?.into_iter();
        // read and discard marker
        match iter.next() {
            Some(MetaItem::Marker(_)) => (),
            Some(item) => {
                let err = format!("expected marker, found {}", item);
                return Err(Error::InvalidSnapshot(err));
            }
            None => {
                let err = "expected marker, eof".to_string();
                return Err(Error::InvalidSnapshot(err));
            }
        }
        // read metadata
        snap.metadata = match iter.next() {
            Some(MetaItem::Metadata(metadata)) => metadata,
            Some(item) => {
                let err = format!("expected metadata, found {}", item);
                return Err(Error::InvalidSnapshot(err));
            }
            None => {
                let err = "expected metadata, eof".to_string();
                return Err(Error::InvalidSnapshot(err));
            }
        };
        // read the statistics and information for this snapshot.
        snap.stats = match iter.next() {
            Some(MetaItem::Stats(stats)) => stats.parse()?,
            Some(item) => {
                let err = format!("expected metadata, found {}", item);
                return Err(Error::InvalidSnapshot(err));
            }
            None => {
                let err = "expected statistics".to_string();
                return Err(Error::InvalidSnapshot(err));
            }
        };
        snap.config = snap.stats.clone().into();
        snap.config.dir = dir.to_string();
        snap.config.vlog_file = match snap.config.vlog_file.clone() {
            None => None,
            Some(vlog_file_1) => {
                let f = path::Path::new(&vlog_file_1).file_name().unwrap();
                let ifile = Config::index_file(&dir, &name);
                let mut file = path::PathBuf::new();
                file.push(path::Path::new(&ifile).parent().unwrap());
                file.push(f);
                let vlog_file_2 = file.to_str().unwrap().to_string();
                // TODO: verify whether both the file names are equal.
                Some(vlog_file_2)
            }
        };
        snap.vlog_fd = if snap.config.vlog_ok {
            Some(util::open_file_r(&Config::vlog_file(dir, name))?)
        } else {
            None
        };

        // read root
        snap.root = match iter.next() {
            Some(MetaItem::Root(root)) => root,
            Some(item) => {
                let err = format!("expected metadata, found {}", item);
                return Err(Error::InvalidSnapshot(err));
            }
            None => {
                let err = "expected statistics".to_string();
                return Err(Error::InvalidSnapshot(err));
            }
        };
        // make sure nothing is left !!
        if let Some(item) = iter.next() {
            let err = format!("expected eof, found {}", item);
            return Err(Error::InvalidSnapshot(err));
        }
        // validate snapshot
        if snap.stats.name != name {
            let err = format!("name mistmatch {} != {}", snap.stats.name, name);
            return Err(Error::InvalidSnapshot(err));
        }
        // Okey dockey
        Ok(snap)
    }
}

// maintanence methods.
impl<K, V> Snapshot<K, V>
where
    K: Default + Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    pub fn purge(&mut self) {
        panic!("to-be-implemented")
    }

    pub fn count(&self) -> u64 {
        self.stats.n_count
    }

    pub fn footprint(&self) -> u64 {
        let index_file = Config::index_file(&self.config.dir, &self.config.name);
        let mut footprint = fs::metadata(index_file).unwrap().len();

        let vlog_file = Config::vlog_file(&self.config.dir, &self.config.name);
        footprint += fs::metadata(vlog_file).unwrap().len();
        footprint
    }

    pub fn get_seqno(&self) -> u64 {
        self.stats.seqno
    }

    pub fn metadata(&self) -> Vec<u8> {
        self.metadata.clone()
    }

    pub fn stats(&self) -> Stats {
        self.stats.clone()
    }

    pub fn close(self) {
        // TODO: can be implemented via Drop.
        panic!("to-be-implemented")
    }
}

impl<K, V> Snapshot<K, V>
where
    K: Default + Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    pub fn get(&mut self, key: &K) -> Result<Entry<K, V>> {
        let mut fpos = self.root;
        let fd = &mut self.index_fd;
        let config = &self.config;
        let (from, to) = (Bound::Unbounded, Bound::Unbounded);

        let zblock_fpos = loop {
            let mblock: MBlock<K, V> = MBlock::new_decode(fd, fpos, &config)?;
            let (_, is_z, entry) = mblock.find(key, from, to)?;
            fpos = match entry.vlog_value_ref() {
                vlog::Value::Reference { fpos, .. } => *fpos,
                _ => unreachable!(),
            };
            if is_z {
                break fpos;
            }
        };

        let zblock: ZBlock<K, V> = ZBlock::new_decode(fd, zblock_fpos, &config)?;
        let (_, entry) = zblock.find(key, from, to)?;
        if entry.key_ref().borrow().eq(key) {
            Ok(entry)
        } else {
            Err(Error::KeyNotFound)
        }
    }

    pub fn iter<'a>(&'a mut self) -> Result<Iter<'a, K, V>> {
        let mut mzs = vec![];
        self.build_fwd(&mut mzs, self.root)?;
        Ok(Iter {
            snap: self,
            mzs: mzs,
        })
    }

    pub fn range<'a>(
        &'a mut self,
        low: Bound<K>,  // upper bound
        high: Bound<K>, // lower bound
    ) -> Result<Range<'a, K, V>> {
        Ok(match low {
            Bound::Unbounded => {
                let mut mzs = vec![];
                self.build_fwd(&mut mzs, self.root)?;
                Range {
                    snap: self,
                    mzs,
                    high,
                }
            }
            Bound::Included(key) => {
                let (mzs, _entry) = self.build(&key)?;
                Range {
                    snap: self,
                    mzs,
                    high,
                }
            }
            Bound::Excluded(key) => {
                let (mzs, entry) = self.build(&key)?;
                let mut r = Range {
                    snap: self,
                    mzs,
                    high,
                };
                if entry.key_ref().eq(&key) {
                    r.next();
                }
                r
            }
        })
    }

    pub fn reverse<'a>(
        &'a mut self,
        high: Bound<K>, // upper bound
        low: Bound<K>,  // lower bound
    ) -> Result<Reverse<'a, K, V>> {
        Ok(match high {
            Bound::Unbounded => {
                let mut mzs = vec![];
                self.build_rev(&mut mzs, self.root)?;
                Reverse {
                    snap: self,
                    mzs,
                    low,
                }
            }
            Bound::Included(key) => {
                let (mzs, _entry) = self.build(&key)?;
                Reverse {
                    snap: self,
                    mzs,
                    low,
                }
            }
            Bound::Excluded(key) => {
                let (mzs, entry) = self.build(&key)?;
                let mut r = Reverse {
                    snap: self,
                    mzs,
                    low,
                };
                if entry.key_ref().eq(&key) {
                    r.next();
                }
                r
            }
        })
    }

    fn build_fwd(
        &mut self,
        mzs: &mut Vec<MZ<K, V>>,
        mut fpos: u64, // from node
    ) -> Result<Entry<K, V>> {
        let fd = &mut self.index_fd;
        let config = &self.config;

        let zblock_fpos = loop {
            let mblock: MBlock<K, V> = MBlock::new_decode(fd, fpos, config)?;
            let (index, is_z, entry) = mblock.entry_at(0)?;
            fpos = match (is_z, entry.vlog_value_ref()) {
                (false, vlog::Value::Reference { fpos, .. }) => {
                    mzs.push(MZ::M { fpos: *fpos, index });
                    *fpos
                }
                (true, vlog::Value::Reference { fpos, .. }) => {
                    break *fpos;
                }
                _ => unreachable!(),
            };
        };

        let zblock = ZBlock::new_decode(fd, zblock_fpos, config)?;
        let (index, entry) = zblock.entry_at(0)?;
        mzs.push(MZ::Z { zblock, index });
        Ok(entry)
    }

    fn build_rev(
        &mut self,
        mzs: &mut Vec<MZ<K, V>>,
        mut fpos: u64, // from node
    ) -> Result<Entry<K, V>> {
        let fd = &mut self.index_fd;
        let config = &self.config;

        let zblock_fpos = loop {
            let mblock: MBlock<K, V> = MBlock::new_decode(fd, fpos, config)?;
            let (index, is_z, entry) = mblock.entry_at(mblock.len() - 1)?;
            fpos = match (is_z, entry.vlog_value_ref()) {
                (false, vlog::Value::Reference { fpos, .. }) => {
                    mzs.push(MZ::M { fpos: *fpos, index });
                    *fpos
                }
                (true, vlog::Value::Reference { fpos, .. }) => {
                    break *fpos;
                }
                _ => unreachable!(),
            };
        };

        let zblock = ZBlock::new_decode(fd, zblock_fpos, config)?;
        let (index, entry) = zblock.entry_at(zblock.len() - 1)?;
        mzs.push(MZ::Z { zblock, index });
        Ok(entry)
    }

    fn build(&mut self, key: &K) -> Result<(Vec<MZ<K, V>>, Entry<K, V>)> {
        let mut mzs = vec![];
        let mut fpos = self.root;
        let fd = &mut self.index_fd;
        let config = &self.config;
        let (from, to) = (Bound::Unbounded, Bound::Unbounded);

        let zblock_fpos = loop {
            let mblock: MBlock<K, V> = MBlock::new_decode(fd, fpos, config)?;
            let (index, is_z, entry) = mblock.find(key, from, to)?;
            fpos = match (is_z, entry.vlog_value_ref()) {
                (false, vlog::Value::Reference { fpos, .. }) => {
                    mzs.push(MZ::M { fpos: *fpos, index });
                    *fpos
                }
                (true, vlog::Value::Reference { fpos, .. }) => {
                    break *fpos;
                }
                _ => unreachable!(),
            };
        };

        let zblock = ZBlock::new_decode(fd, zblock_fpos, config)?;
        let (index, entry) = zblock.find(key, from, to)?;
        mzs.push(MZ::Z { zblock, index });
        Ok((mzs, entry))
    }

    fn rebuild_fwd(&mut self, mzs: &mut Vec<MZ<K, V>>) -> Result<()> {
        let fd = &mut self.index_fd;
        let config = &self.config;

        match mzs.pop() {
            None => Ok(()),
            Some(MZ::Z { .. }) => unreachable!(),
            Some(MZ::M { fpos, index }) => {
                let mblock: MBlock<K, V> = MBlock::new_decode(fd, fpos, config)?;
                match mblock.entry_at(index + 1) {
                    Ok((_index, true /*is_z*/, entry)) => {
                        mzs.push(MZ::M {
                            fpos,
                            index: index + 1,
                        });

                        let zblock_fpos = match entry.vlog_value_ref() {
                            vlog::Value::Reference { fpos, .. } => *fpos,
                            _ => unreachable!(),
                        };
                        let zblock = ZBlock::new_decode(fd, zblock_fpos, config)?;
                        zblock.entry_at(0)?;
                        mzs.push(MZ::Z { zblock, index: 0 });
                        Ok(())
                    }
                    Ok((index, false /*is_z*/, entry)) => {
                        mzs.push(MZ::M {
                            fpos,
                            index: index + 1,
                        });

                        let fpos = match entry.vlog_value_ref() {
                            vlog::Value::Reference { fpos, .. } => *fpos,
                            _ => unreachable!(),
                        };
                        self.build_fwd(mzs, fpos)?;
                        Ok(())
                    }
                    Err(Error::ZBlockExhausted) => self.rebuild_fwd(mzs),
                    _ => unreachable!(),
                }
            }
        }
    }

    fn rebuild_rev(&mut self, mzs: &mut Vec<MZ<K, V>>) -> Result<()> {
        let fd = &mut self.index_fd;
        let config = &self.config;

        match mzs.pop() {
            None => Ok(()),
            Some(MZ::Z { .. }) => unreachable!(),
            Some(MZ::M { index: 0, .. }) => self.rebuild_rev(mzs),
            Some(MZ::M { fpos, index }) => {
                let mblock: MBlock<K, V> = MBlock::new_decode(fd, fpos, config)?;
                match mblock.entry_at(index - 1) {
                    Ok((_index, true /*is_z*/, entry)) => {
                        mzs.push(MZ::M {
                            fpos,
                            index: index - 1,
                        });

                        let zblock_fpos = match entry.vlog_value_ref() {
                            vlog::Value::Reference { fpos, .. } => *fpos,
                            _ => unreachable!(),
                        };
                        let zblock = ZBlock::new_decode(fd, zblock_fpos, config)?;
                        let (index, _) = zblock.entry_at(zblock.len() - 1)?;
                        mzs.push(MZ::Z { zblock, index });
                        Ok(())
                    }
                    Ok((index, false /*is_z*/, entry)) => {
                        mzs.push(MZ::M {
                            fpos,
                            index: index - 1,
                        });

                        let fpos = match entry.vlog_value_ref() {
                            vlog::Value::Reference { fpos, .. } => *fpos,
                            _ => unreachable!(),
                        };
                        self.build_rev(mzs, fpos)?;
                        Ok(())
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
}

pub struct Iter<'a, K, V>
where
    K: Default + Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    snap: &'a mut Snapshot<K, V>,
    mzs: Vec<MZ<K, V>>,
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
    K: Default + Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Result<Entry<K, V>>> {
        match self.mzs.pop() {
            None => None,
            Some(mut z) => match z.next() {
                Some(entry) => {
                    self.mzs.push(z);
                    Some(Ok(entry))
                }
                None => match self.snap.rebuild_fwd(&mut self.mzs) {
                    Err(err) => Some(Err(err)),
                    Ok(_) => self.next(),
                },
            },
        }
    }
}

pub struct Range<'a, K, V>
where
    K: Default + Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    snap: &'a mut Snapshot<K, V>,
    mzs: Vec<MZ<K, V>>,
    high: Bound<K>,
}

impl<'a, K, V> Range<'a, K, V>
where
    K: Default + Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    fn till_ok(&self, entry: &Entry<K, V>) -> bool {
        match &self.high {
            Bound::Unbounded => true,
            Bound::Included(key) => entry.key_ref().le(key),
            Bound::Excluded(key) => entry.key_ref().lt(key),
        }
    }
}

impl<'a, K, V> Iterator for Range<'a, K, V>
where
    K: Default + Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Result<Entry<K, V>>> {
        match self.mzs.pop() {
            None => None,
            Some(mut z) => match z.next() {
                Some(entry) => {
                    if self.till_ok(&entry) {
                        self.mzs.push(z);
                        Some(Ok(entry))
                    } else {
                        self.mzs.truncate(0);
                        None
                    }
                }
                None => match self.snap.rebuild_fwd(&mut self.mzs) {
                    Err(err) => Some(Err(err)),
                    Ok(_) => self.next(),
                },
            },
        }
    }
}

pub struct Reverse<'a, K, V>
where
    K: Default + Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    snap: &'a mut Snapshot<K, V>,
    mzs: Vec<MZ<K, V>>,
    low: Bound<K>,
}

impl<'a, K, V> Reverse<'a, K, V>
where
    K: Default + Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    fn till_ok(&self, entry: &Entry<K, V>) -> bool {
        match &self.low {
            Bound::Unbounded => true,
            Bound::Included(key) => entry.key_ref().ge(key),
            Bound::Excluded(key) => entry.key_ref().gt(key),
        }
    }
}

impl<'a, K, V> Iterator for Reverse<'a, K, V>
where
    K: Default + Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Result<Entry<K, V>>> {
        match self.mzs.pop() {
            None => None,
            Some(mut z) => match z.next_back() {
                Some(entry) => {
                    if self.till_ok(&entry) {
                        self.mzs.push(z);
                        Some(Ok(entry))
                    } else {
                        self.mzs.truncate(0);
                        None
                    }
                }
                None => match self.snap.rebuild_rev(&mut self.mzs) {
                    Err(err) => Some(Err(err)),
                    Ok(_) => self.next(),
                },
            },
        }
    }
}

enum MZ<K, V>
where
    K: Default + Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    M { fpos: u64, index: usize },
    Z { zblock: ZBlock<K, V>, index: usize },
}

impl<K, V> Iterator for MZ<K, V>
where
    K: Default + Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    type Item = Entry<K, V>;

    fn next(&mut self) -> Option<Entry<K, V>> {
        match self {
            MZ::Z { zblock, index } => match zblock.entry_at(*index) {
                Err(Error::ZBlockExhausted) => None,
                Ok((_, entry)) => {
                    *index += 1;
                    Some(entry)
                }
                _ => unreachable!(),
            },
            MZ::M { .. } => unreachable!(),
        }
    }
}

impl<K, V> DoubleEndedIterator for MZ<K, V>
where
    K: Default + Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    fn next_back(&mut self) -> Option<Entry<K, V>> {
        match self {
            MZ::Z { index: 0, .. } => None,
            MZ::Z { zblock, index } => match zblock.entry_at(*index) {
                Ok((_, entry)) => {
                    *index -= 1;
                    Some(entry)
                }
                _ => unreachable!(),
            },
            MZ::M { .. } => unreachable!(),
        }
    }
}
