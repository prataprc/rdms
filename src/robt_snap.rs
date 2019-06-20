// TODO: review resize() calls
// TODO: review "as type conversions" for llrb-index jsondata
// TODO: implement log() and validate() API.
// TODO: cache root block.

use std::borrow::Borrow;
use std::{fs, marker, ops::Bound, path};

use crate::core::{Diff, Entry, Serialize};
use crate::error::Error;
use crate::robt_config::{self, Config, MetaItem};
use crate::robt_entry::DiskEntryM;
use crate::robt_indx::{MBlock, ZBlock};
use crate::robt_stats::Stats;
use crate::util;

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
    pub fn open(dir: &str, name: &str) -> Result<Snapshot<K, V>, Error> {
        let index_fd = util::open_file_r(&Config::stitch_index_file(dir, name))?;

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

        let mut iter = robt_config::read_meta_items(dir, name)?.into_iter();
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
                let ifile = Config::stitch_index_file(&dir, &name);
                let mut file = path::PathBuf::new();
                file.push(path::Path::new(&ifile).parent().unwrap());
                file.push(f);
                let vlog_file_2 = file.to_str().unwrap().to_string();
                // TODO: verify whether both the file names are equal.
                Some(vlog_file_2)
            }
        };
        snap.vlog_fd = snap
            .config
            .to_value_log()
            .as_ref()
            .map(|s| util::open_file_r(s.as_str()))
            .transpose()?;

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
        let index_file = self.config.to_index_file();
        let mut footprint = fs::metadata(index_file).unwrap().len();

        footprint += match self.config.to_value_log() {
            Some(vlog_file) => fs::metadata(vlog_file).unwrap().len(),
            None => 0,
        };
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
    pub fn get(&mut self, key: &K) -> Result<Entry<K, V>, Error> {
        let fpos = self.root;
        let fd = &mut self.index_fd;
        let config = &self.config;
        let (from, to) = (Bound::Unbounded, Bound::Unbounded);

        let zblock_fpos = loop {
            let mblock: MBlock<K, V> = MBlock::new_decode(fd, fpos, &config)?;
            let mentry = mblock.find(key, from, to)?;
            if mentry.is_zblock() {
                break mentry.to_fpos();
            }
        };

        let zblock: ZBlock<K, V> = ZBlock::new_decode(fd, zblock_fpos, &config)?;
        let (_index, entry) = zblock.find(key, from, to)?;
        if entry.as_key().borrow().eq(key) {
            Ok(entry)
        } else {
            Err(Error::KeyNotFound)
        }
    }

    pub fn iter<'a>(&'a mut self) -> Result<Iter<'a, K, V>, Error> {
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
    ) -> Result<Range<'a, K, V>, Error> {
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
                if entry.as_key().eq(&key) {
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
    ) -> Result<Reverse<'a, K, V>, Error> {
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
                if entry.as_key().eq(&key) {
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
    ) -> Result<Entry<K, V>, Error> {
        let fd = &mut self.index_fd;
        let config = &self.config;

        let zblock_fpos = loop {
            let mblock: MBlock<K, V> = MBlock::new_decode(fd, fpos, config)?;
            let mentry = mblock.to_entry(0)?;
            fpos = match (mentry.is_zblock(), mentry.to_fpos()) {
                (false, fpos) => {
                    mzs.push(MZ::M { fpos, index: 0 });
                    fpos
                }
                (true, fpos) => {
                    break fpos;
                }
            };
        };

        let zblock = ZBlock::new_decode(fd, zblock_fpos, config)?;
        let (_index, entry) = zblock.to_entry(0)?;
        mzs.push(MZ::Z { zblock, index: 0 });
        Ok(entry)
    }

    fn build_rev(
        &mut self,
        mzs: &mut Vec<MZ<K, V>>,
        mut fpos: u64, // from node
    ) -> Result<Entry<K, V>, Error> {
        let fd = &mut self.index_fd;
        let config = &self.config;

        let zblock_fpos = loop {
            let mblock: MBlock<K, V> = MBlock::new_decode(fd, fpos, config)?;
            let mentry = mblock.to_entry(mblock.len() - 1)?;
            fpos = match (mentry.is_zblock(), mentry.to_fpos()) {
                (false, fpos) => {
                    mzs.push(MZ::M {
                        fpos,
                        index: mblock.len() - 1,
                    });
                    fpos
                }
                (true, fpos) => {
                    break fpos;
                }
            };
        };

        let zblock = ZBlock::new_decode(fd, zblock_fpos, config)?;
        let (index, entry) = zblock.to_entry(zblock.len() - 1)?;
        mzs.push(MZ::Z { zblock, index });
        Ok(entry)
    }

    fn build(&mut self, key: &K) -> Result<(Vec<MZ<K, V>>, Entry<K, V>), Error> {
        let mut mzs = vec![];
        let mut fpos = self.root;
        let fd = &mut self.index_fd;
        let config = &self.config;
        let (from, to) = (Bound::Unbounded, Bound::Unbounded);

        let zblock_fpos = loop {
            let mblock: MBlock<K, V> = MBlock::new_decode(fd, fpos, config)?;
            let mentry = mblock.find(key, from, to)?;
            fpos = match (mentry.is_zblock(), mentry.to_fpos()) {
                (false, fpos) => {
                    mzs.push(MZ::M {
                        fpos: fpos,
                        index: mentry.to_index(),
                    });
                    fpos
                }
                (true, fpos) => {
                    break fpos;
                }
            };
        };

        let zblock = ZBlock::new_decode(fd, zblock_fpos, config)?;
        let (index, entry) = zblock.find(key, from, to)?;
        mzs.push(MZ::Z { zblock, index });
        Ok((mzs, entry))
    }

    fn rebuild_fwd(&mut self, mzs: &mut Vec<MZ<K, V>>) -> Result<(), Error> {
        let fd = &mut self.index_fd;
        let config = &self.config;

        match mzs.pop() {
            None => Ok(()),
            Some(MZ::Z { .. }) => unreachable!(),
            Some(MZ::M { fpos, index }) => {
                let mblock: MBlock<K, V> = MBlock::new_decode(fd, fpos, config)?;
                match mblock.to_entry(index + 1) {
                    Ok(DiskEntryM::Entry {
                        z: true,
                        fpos: zblock_fpos,
                        ..
                    }) => {
                        mzs.push(MZ::M {
                            fpos,
                            index: index + 1,
                        });

                        let zblock = ZBlock::new_decode(fd, zblock_fpos, config)?;
                        zblock.to_entry(0)?;
                        mzs.push(MZ::Z { zblock, index: 0 });
                        Ok(())
                    }
                    Ok(DiskEntryM::Entry {
                        z: false,
                        fpos: mblock_fpos,
                        ..
                    }) => {
                        mzs.push(MZ::M {
                            fpos,
                            index: index + 1,
                        });

                        self.build_fwd(mzs, mblock_fpos)?;
                        Ok(())
                    }
                    Err(Error::ZBlockExhausted) => self.rebuild_fwd(mzs),
                    _ => unreachable!(),
                }
            }
        }
    }

    fn rebuild_rev(&mut self, mzs: &mut Vec<MZ<K, V>>) -> Result<(), Error> {
        let fd = &mut self.index_fd;
        let config = &self.config;

        match mzs.pop() {
            None => Ok(()),
            Some(MZ::Z { .. }) => unreachable!(),
            Some(MZ::M { index: 0, .. }) => self.rebuild_rev(mzs),
            Some(MZ::M { fpos, index }) => {
                let mblock: MBlock<K, V> = MBlock::new_decode(fd, fpos, config)?;
                match mblock.to_entry(index - 1) {
                    Ok(DiskEntryM::Entry {
                        z: true,
                        fpos: zblock_fpos,
                        ..
                    }) => {
                        mzs.push(MZ::M {
                            fpos,
                            index: index - 1,
                        });

                        let zblock = ZBlock::new_decode(fd, zblock_fpos, config)?;
                        let (index, _) = zblock.to_entry(zblock.len() - 1)?;
                        mzs.push(MZ::Z { zblock, index });
                        Ok(())
                    }
                    Ok(DiskEntryM::Entry {
                        z: false,
                        fpos: mblock_fpos,
                        ..
                    }) => {
                        mzs.push(MZ::M {
                            fpos,
                            index: index - 1,
                        });

                        self.build_rev(mzs, mblock_fpos)?;
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
    type Item = Result<Entry<K, V>, Error>;

    fn next(&mut self) -> Option<Result<Entry<K, V>, Error>> {
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
            Bound::Included(key) => entry.as_key().le(key),
            Bound::Excluded(key) => entry.as_key().lt(key),
        }
    }
}

impl<'a, K, V> Iterator for Range<'a, K, V>
where
    K: Default + Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    type Item = Result<Entry<K, V>, Error>;

    fn next(&mut self) -> Option<Result<Entry<K, V>, Error>> {
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
            Bound::Included(key) => entry.as_key().ge(key),
            Bound::Excluded(key) => entry.as_key().gt(key),
        }
    }
}

impl<'a, K, V> Iterator for Reverse<'a, K, V>
where
    K: Default + Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
{
    type Item = Result<Entry<K, V>, Error>;

    fn next(&mut self) -> Option<Result<Entry<K, V>, Error>> {
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
            MZ::Z { zblock, index } => match zblock.to_entry(*index) {
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
            MZ::Z { zblock, index } => match zblock.to_entry(*index) {
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
