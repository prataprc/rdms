// TODO: review resize() calls
// TODO: review "as type conversions" for llrb-index jsondata
// TODO: implement log() and validate() API.
// TODO: cache root block.

use std::borrow::Borrow;
use std::{
    cmp, fs, marker,
    ops::{Bound, RangeBounds},
    path,
};

use crate::core::{Diff, Entry, Serialize};
use crate::error::Error;
use crate::robt_config::{self, Config, MetaItem};
use crate::robt_entry::DiskEntryM;
use crate::robt_indx::{MBlock, ZBlock};
use crate::robt_stats::Stats;
use crate::util;

pub struct Snapshot<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
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
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    pub fn open(dir: &str, name: &str) -> Result<Snapshot<K, V>, Error> {
        let index_file = Config::stitch_index_file(dir, name);
        let index_fd = util::open_file_r(&index_file.as_ref())?;

        let mut snap = Snapshot {
            config: Config::new(dir, name),
            stats: Default::default(),
            metadata: Default::default(),
            root: Default::default(),
            index_fd,
            vlog_fd: Default::default(),

            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        };

        let items = robt_config::read_meta_items(dir, name)?;

        for (i, item) in items.into_iter().enumerate() {
            match (i, item) {
                (0, MetaItem::Marker(_)) => (),
                (1, MetaItem::Metadata(metadata)) => {
                    snap.metadata = metadata;
                }
                (2, MetaItem::Stats(stats)) => {
                    snap.stats = stats.parse()?;
                }
                (3, MetaItem::Root(root)) => {
                    snap.root = root;
                }
                (i, item) => {
                    let err = format!("found {} at {}", item, i);
                    return Err(Error::InvalidSnapshot(err));
                }
            }
        }

        snap.config = snap.stats.clone().into();
        snap.config.dir = dir.to_string();

        snap.config.vlog_file = match snap.config.vlog_file.clone() {
            None => None,
            Some(vfile) => {
                // stem the file name.
                let vfile = path::Path::new(&vfile).file_name().unwrap();
                let ipath = Config::stitch_index_file(&dir, &name);
                let mut vpath = path::PathBuf::new();
                vpath.push(path::Path::new(&ipath).parent().unwrap());
                vpath.push(vfile);
                Some(vpath.to_str().unwrap().to_string())
            }
        };
        snap.vlog_fd = snap
            .config
            .to_value_log()
            .as_ref()
            .map(|s| util::open_file_r(s.as_ref()))
            .transpose()?;

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
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
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
}

// Read methods
impl<K, V> Snapshot<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    fn get_zpos<Q>(&mut self, key: &Q, fpos: u64) -> Result<u64, Error>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let fd = &mut self.index_fd;
        let mblock = MBlock::<K, V>::new_decode(fd, fpos, &self.config)?;
        match mblock.get(key, Bound::Unbounded, Bound::Unbounded) {
            Err(Error::__LessThan) => Err(Error::KeyNotFound),
            Err(Error::__MBlockExhausted(_)) => unreachable!(),
            Ok(mentry) if mentry.is_zblock() => Ok(mentry.to_fpos()),
            Ok(mentry) => self.get_zpos(key, mentry.to_fpos()),
            Err(err) => Err(err),
        }
    }

    pub fn get<Q>(&mut self, key: &Q) -> Result<Entry<K, V>, Error>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let zfpos = self.get_zpos(key, self.root)?;

        let fd = &mut self.index_fd;
        let zblock: ZBlock<K, V> = ZBlock::new_decode(fd, zfpos, &self.config)?;
        match zblock.find(key, Bound::Unbounded, Bound::Unbounded) {
            Ok((_, entry)) => {
                if entry.as_key().borrow().eq(key) {
                    Ok(entry)
                } else {
                    Err(Error::KeyNotFound)
                }
            }
            Err(Error::__ZBlockExhausted(_)) => unreachable!(),
            Err(err) => Err(err),
        }
    }

    pub fn iter(&mut self) -> Result<Iter<K, V>, Error> {
        let mut mzs = vec![];
        self.build_fwd(self.root, &mut mzs)?;
        Ok(Iter {
            snap: self,
            mzs: mzs,
        })
    }

    pub fn range<R, Q>(&mut self, range: R) -> Result<Range<K, V, R, Q>, Error>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q>,
        Q: Ord + ?Sized,
    {
        let mut mzs = vec![];
        let skip_one = match range.start_bound() {
            Bound::Unbounded => {
                self.build_fwd(self.root, &mut mzs)?;
                false
            }
            Bound::Included(key) => {
                let entry = self.build(key, &mut mzs)?;
                match key.cmp(entry.as_key().borrow()) {
                    cmp::Ordering::Greater => true,
                    _ => false,
                }
            }
            Bound::Excluded(key) => {
                let entry = self.build(key, &mut mzs)?;
                match key.cmp(entry.as_key().borrow()) {
                    cmp::Ordering::Equal | cmp::Ordering::Greater => true,
                    _ => false,
                }
            }
        };
        let mut r = Range {
            snap: self,
            mzs,
            range,
            high: marker::PhantomData,
        };
        if skip_one {
            r.next();
        }
        Ok(r)
    }

    pub fn reverse<R, Q>(&mut self, r: R) -> Result<Reverse<K, V, R, Q>, Error>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q>,
        Q: Ord + ?Sized,
    {
        let mut mzs = vec![];
        let skip_one = match r.end_bound() {
            Bound::Unbounded => {
                self.build_rev(self.root, &mut mzs)?;
                false
            }
            Bound::Included(key) => {
                self.build(&key, &mut mzs)?;
                false
            }
            Bound::Excluded(key) => {
                let entry = self.build(&key, &mut mzs)?;
                key.eq(entry.as_key().borrow())
            }
        };
        let mut rr = Reverse {
            snap: self,
            mzs,
            range: r,
            low: marker::PhantomData,
        };
        if skip_one {
            rr.next();
        }
        Ok(rr)
    }

    fn build_fwd(
        &mut self,
        mut fpos: u64,           // from node
        mzs: &mut Vec<MZ<K, V>>, // output
    ) -> Result<(), Error> {
        let fd = &mut self.index_fd;
        let config = &self.config;

        let zfpos = loop {
            let mblock = MBlock::<K, V>::new_decode(fd, fpos, config)?;
            let mentry = mblock.to_entry(0)?;
            if mentry.is_zblock() {
                break mentry.to_fpos();
            }
            mzs.push(MZ::M { fpos, index: 0 });
            fpos = mentry.to_fpos();
        };

        let zblock = ZBlock::new_decode(fd, zfpos, config)?;
        mzs.push(MZ::Z { zblock, index: 0 });
        Ok(())
    }

    fn rebuild_fwd(&mut self, mzs: &mut Vec<MZ<K, V>>) -> Result<(), Error> {
        let fd = &mut self.index_fd;
        let config = &self.config;

        match mzs.pop() {
            None => Ok(()),
            Some(MZ::Z { .. }) => unreachable!(),
            Some(MZ::M { fpos, mut index }) => {
                let mblock = MBlock::<K, V>::new_decode(fd, fpos, config)?;
                index += 1;
                match mblock.to_entry(index) {
                    Ok(DiskEntryM::Entry {
                        z: true,
                        fpos: zfpos,
                        ..
                    }) => {
                        mzs.push(MZ::M { fpos, index });

                        let zblock = ZBlock::new_decode(fd, zfpos, config)?;
                        mzs.push(MZ::Z { zblock, index: 0 });
                        Ok(())
                    }
                    Ok(DiskEntryM::Entry {
                        z: false,
                        fpos: mfpos,
                        ..
                    }) => {
                        mzs.push(MZ::M { fpos, index });
                        self.build_fwd(mfpos, mzs)?;
                        Ok(())
                    }
                    Err(Error::__ZBlockExhausted(_)) => self.rebuild_fwd(mzs),
                    _ => unreachable!(),
                }
            }
        }
    }

    fn build_rev(
        &mut self,
        mut fpos: u64,           // from node
        mzs: &mut Vec<MZ<K, V>>, // output
    ) -> Result<(), Error> {
        let fd = &mut self.index_fd;
        let config = &self.config;

        let zfpos = loop {
            let mblock = MBlock::<K, V>::new_decode(fd, fpos, config)?;
            let index = mblock.len() - 1;
            let mentry = mblock.to_entry(index)?;
            if mentry.is_zblock() {
                break mentry.to_fpos();
            }
            mzs.push(MZ::M { fpos, index });
            fpos = mentry.to_fpos();
        };

        let zblock = ZBlock::new_decode(fd, zfpos, config)?;
        let index = zblock.len() - 1;
        mzs.push(MZ::Z { zblock, index });
        Ok(())
    }

    fn rebuild_rev(&mut self, mzs: &mut Vec<MZ<K, V>>) -> Result<(), Error> {
        let fd = &mut self.index_fd;
        let config = &self.config;

        match mzs.pop() {
            None => Ok(()),
            Some(MZ::Z { .. }) => unreachable!(),
            Some(MZ::M { index: 0, .. }) => self.rebuild_rev(mzs),
            Some(MZ::M { fpos, mut index }) => {
                let mblock = MBlock::<K, V>::new_decode(fd, fpos, config)?;
                index -= 1;
                match mblock.to_entry(index) {
                    Ok(DiskEntryM::Entry {
                        z: true,
                        fpos: zfpos,
                        ..
                    }) => {
                        mzs.push(MZ::M { fpos, index });

                        let zblock = ZBlock::new_decode(fd, zfpos, config)?;
                        let index = zblock.len() - 1;
                        mzs.push(MZ::Z { zblock, index });
                        Ok(())
                    }
                    Ok(DiskEntryM::Entry {
                        z: false,
                        fpos: mfpos,
                        ..
                    }) => {
                        mzs.push(MZ::M { fpos, index });
                        self.build_rev(mfpos, mzs)?;
                        Ok(())
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    fn build<Q>(
        &mut self,
        key: &Q,
        mzs: &mut Vec<MZ<K, V>>, // output
    ) -> Result<Entry<K, V>, Error>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut fpos = self.root;
        let fd = &mut self.index_fd;
        let config = &self.config;
        let (from, to) = (Bound::Unbounded, Bound::Unbounded);

        let zfpos = loop {
            let mblock = MBlock::<K, V>::new_decode(fd, fpos, config)?;
            match mblock.find(key, from, to) {
                Ok(mentry) => {
                    if mentry.is_zblock() {
                        break mentry.to_fpos();
                    }
                    let index = mentry.to_index();
                    mzs.push(MZ::M { fpos, index });
                    fpos = mentry.to_fpos();
                }
                Err(Error::__LessThan) => unreachable!(),
                Err(err) => return Err(err),
            }
        };

        let zblock = ZBlock::new_decode(fd, zfpos, config)?;
        let (index, entry) = zblock.find(key, from, to)?;
        mzs.push(MZ::Z { zblock, index });
        Ok(entry)
    }
}

pub struct Iter<'a, K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    snap: &'a mut Snapshot<K, V>,
    mzs: Vec<MZ<K, V>>,
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    type Item = Result<Entry<K, V>, Error>;

    fn next(&mut self) -> Option<Result<Entry<K, V>, Error>> {
        match self.mzs.pop() {
            None => None,
            Some(mut z) => match z.next() {
                Some(Err(err)) => {
                    self.mzs.truncate(0);
                    Some(Err(err))
                }
                Some(Ok(entry)) => {
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

pub struct Range<'a, K, V, R, Q>
where
    Q: Ord + ?Sized,
    R: RangeBounds<Q>,
    K: Clone + Ord + Borrow<Q> + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    snap: &'a mut Snapshot<K, V>,
    mzs: Vec<MZ<K, V>>,
    range: R,
    high: marker::PhantomData<Q>,
}

impl<'a, K, V, R, Q> Range<'a, K, V, R, Q>
where
    Q: Ord + ?Sized,
    R: RangeBounds<Q>,
    K: Clone + Ord + Borrow<Q> + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    fn till_ok(&self, entry: &Entry<K, V>) -> bool {
        match self.range.end_bound() {
            Bound::Unbounded => true,
            Bound::Included(key) => entry.as_key().borrow().le(key),
            Bound::Excluded(key) => entry.as_key().borrow().lt(key),
        }
    }
}

impl<'a, K, V, R, Q> Iterator for Range<'a, K, V, R, Q>
where
    Q: Ord + ?Sized,
    R: RangeBounds<Q>,
    K: Clone + Ord + Borrow<Q> + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    type Item = Result<Entry<K, V>, Error>;

    fn next(&mut self) -> Option<Result<Entry<K, V>, Error>> {
        match self.mzs.pop() {
            None => None,
            Some(mut z) => match z.next() {
                Some(Err(err)) => {
                    self.mzs.truncate(0);
                    Some(Err(err))
                }
                Some(Ok(entry)) => {
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

pub struct Reverse<'a, K, V, R, Q>
where
    Q: Ord + ?Sized,
    R: RangeBounds<Q>,
    K: Clone + Ord + Borrow<Q> + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    snap: &'a mut Snapshot<K, V>,
    mzs: Vec<MZ<K, V>>,
    range: R,
    low: marker::PhantomData<Q>,
}

impl<'a, K, V, R, Q> Reverse<'a, K, V, R, Q>
where
    Q: Ord + ?Sized,
    R: RangeBounds<Q>,
    K: Clone + Ord + Borrow<Q> + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    fn till_ok(&self, entry: &Entry<K, V>) -> bool {
        match self.range.start_bound() {
            Bound::Unbounded => true,
            Bound::Included(key) => entry.as_key().borrow().ge(key),
            Bound::Excluded(key) => entry.as_key().borrow().gt(key),
        }
    }
}

impl<'a, K, V, R, Q> Iterator for Reverse<'a, K, V, R, Q>
where
    Q: Ord + ?Sized,
    R: RangeBounds<Q>,
    K: Clone + Ord + Borrow<Q> + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    type Item = Result<Entry<K, V>, Error>;

    fn next(&mut self) -> Option<Result<Entry<K, V>, Error>> {
        match self.mzs.pop() {
            None => None,
            Some(mut z) => match z.next_back() {
                Some(Err(err)) => {
                    self.mzs.truncate(0);
                    Some(Err(err))
                }
                Some(Ok(entry)) => {
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
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    M { fpos: u64, index: usize },
    Z { zblock: ZBlock<K, V>, index: usize },
}

impl<K, V> Iterator for MZ<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    type Item = Result<Entry<K, V>, Error>;

    fn next(&mut self) -> Option<Result<Entry<K, V>, Error>> {
        match self {
            MZ::Z { zblock, index } => match zblock.to_entry(*index) {
                Ok((_, entry)) => {
                    *index += 1;
                    Some(Ok(entry))
                }
                Err(Error::__ZBlockExhausted(_)) => None,
                Err(err) => Some(Err(err)),
            },
            MZ::M { .. } => unreachable!(),
        }
    }
}

impl<K, V> DoubleEndedIterator for MZ<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    fn next_back(&mut self) -> Option<Result<Entry<K, V>, Error>> {
        match self {
            MZ::Z { zblock, index } => match zblock.to_entry(*index) {
                Ok((_, entry)) => {
                    *index -= 1;
                    Some(Ok(entry))
                }
                Err(Error::__ZBlockExhausted(_)) => None,
                Err(err) => Some(Err(err)),
            },
            MZ::M { .. } => unreachable!(),
        }
    }
}
