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
use crate::error::BognError;
use crate::util;
use crate::vlog;

pub struct Snapshot<K, V>
where
    K: Default + Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
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
                return Err(BognError::InvalidSnapshot(err));
            }
            None => {
                let err = "expected marker, eof".to_string();
                return Err(BognError::InvalidSnapshot(err));
            }
        }
        // read metadata
        snap.metadata = match iter.next() {
            Some(MetaItem::Metadata(metadata)) => metadata,
            Some(item) => {
                let err = format!("expected metadata, found {}", item);
                return Err(BognError::InvalidSnapshot(err));
            }
            None => {
                let err = "expected metadata, eof".to_string();
                return Err(BognError::InvalidSnapshot(err));
            }
        };
        // read the statistics and information for this snapshot.
        snap.stats = match iter.next() {
            Some(MetaItem::Stats(stats)) => stats.parse()?,
            Some(item) => {
                let err = format!("expected metadata, found {}", item);
                return Err(BognError::InvalidSnapshot(err));
            }
            None => {
                let err = "expected statistics".to_string();
                return Err(BognError::InvalidSnapshot(err));
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
                return Err(BognError::InvalidSnapshot(err));
            }
            None => {
                let err = "expected statistics".to_string();
                return Err(BognError::InvalidSnapshot(err));
            }
        };
        // make sure nothing is left !!
        if let Some(item) = iter.next() {
            let err = format!("expected eof, found {}", item);
            return Err(BognError::InvalidSnapshot(err));
        }
        // validate snapshot
        if snap.stats.name != name {
            let err = format!("name mistmatch {} != {}", snap.stats.name, name);
            return Err(BognError::InvalidSnapshot(err));
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
{
    pub fn get<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
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
            Err(BognError::KeyNotFound)
        }
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

    fn build<Q>(&mut self, key: &Q) -> Result<(Vec<MZ<K, V>>, Entry<K, V>)>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
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

    fn rebuild_fwd(&mut self, mut mzs: Vec<MZ<K, V>>) -> Result<Vec<MZ<K, V>>> {
        let fd = &mut self.index_fd;
        let config = &self.config;

        match mzs.pop() {
            None => Ok(mzs),
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
                        Ok(mzs)
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
                        self.build_fwd(&mut mzs, fpos)?;
                        Ok(mzs)
                    }
                    Err(BognError::ZBlockExhausted) => self.rebuild_fwd(mzs),
                    _ => unreachable!(),
                }
            }
        }
    }

    fn rebuild_rev(&mut self, mut mzs: Vec<MZ<K, V>>) -> Result<Vec<MZ<K, V>>> {
        let fd = &mut self.index_fd;
        let config = &self.config;

        match mzs.pop() {
            None => Ok(mzs),
            Some(MZ::Z { .. }) => unreachable!(),
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
                        Ok(mzs)
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
                        self.build_rev(&mut mzs, fpos)?;
                        Ok(mzs)
                    }
                    Err(BognError::ZBlockExhausted) => self.rebuild_rev(mzs),
                    _ => unreachable!(),
                }
            }
        }
    }

    fn inclusive(k: &Option<Bound<K>>, e: Entry<K, V>) -> Option<Entry<K, V>> {
        match k {
            None => Some(e),
            Some(k) => match k {
                Bound::Unbounded => Some(e),
                Bound::Included(k) if e.key_ref().eq(&k) => Some(e),
                Bound::Included(_) | Bound::Excluded(_) => None,
            },
        }
    }
}

pub struct Iter<'a, K, V>
where
    K: Default + Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
{
    mzs: Vec<MZ<K, V>>,
    snap: &'a mut Snapshot<K, V>,
    iter: std::vec::IntoIter<Entry<K, V>>,
    after_key: Option<Bound<K>>,
    limit: usize,
}

enum MZ<K, V>
where
    K: Default + Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
{
    M { fpos: u64, index: usize },
    Z { zblock: ZBlock<K, V>, index: usize },
}

impl<K, V> Iterator for MZ<K, V>
where
    K: Default + Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
{
    type Item = Entry<K, V>;

    fn next(&mut self) -> Option<Entry<K, V>> {
        match self {
            MZ::Z { zblock, index } => match zblock.entry_at(*index) {
                Err(BognError::ZBlockExhausted) => None,
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
{
    fn next_back(&mut self) -> Option<Entry<K, V>> {
        match self {
            MZ::Z { zblock, index } => match zblock.entry_at(*index) {
                Err(BognError::ZBlockExhausted) => None,
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
