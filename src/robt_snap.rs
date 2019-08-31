// TODO: review resize() calls
// TODO: review "as type conversions" for llrb-index jsondata
// TODO: implement log() and validate() API.
// TODO: cache root block.

use std::borrow::Borrow;
use std::{
    cmp,
    convert::TryInto,
    fs, marker, mem,
    ops::{Bound, RangeBounds},
    path,
    sync::{self},
};

use crate::core::{Diff, Entry, Footprint, Result, Serialize};
use crate::core::{Index, IndexIter, Reader, Writer};
use crate::error::Error;
use crate::robt::Stats;
use crate::robt::{self, Config, MetaItem};
use crate::robt_entry::MEntry;
use crate::robt_indx::{MBlock, ZBlock};
use crate::util;

/// A read only snapshot of BTree built using [robt] index.
///
/// [robt]: crate::robt
pub struct Snapshot<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    dir: String,
    name: String,
    meta: Vec<MetaItem>,
    // working fields
    config: Config,
    index_fd: fs::File,
    vlog_fd: Option<fs::File>,
    mutex: sync::Mutex<i32>,

    phantom_key: marker::PhantomData<K>,
    phantom_val: marker::PhantomData<V>,
}

// Construction methods.
impl<K, V> Snapshot<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    /// Open BTree snapshot from file that can be constructed from ``dir``
    /// and ``name``.
    pub fn open(dir: &str, name: &str) -> Result<Snapshot<K, V>> {
        let meta_items = robt::read_meta_items(dir, name)?;
        let mut snap = Snapshot {
            dir: dir.to_string(),
            name: name.to_string(),
            meta: meta_items,
            config: Config::new(),
            index_fd: {
                let index_file = Config::stitch_index_file(dir, name);
                util::open_file_r(&index_file.as_ref())?
            },
            vlog_fd: Default::default(),
            mutex: sync::Mutex::new(0),

            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        };
        snap.config = snap.to_stats()?.into();
        snap.config.vlog_file = snap.config.vlog_file.map(|vfile| {
            // stem the file name.
            let vfile = path::Path::new(&vfile).file_name().unwrap();
            let ipath = Config::stitch_index_file(&dir, &name);
            let mut vpath = path::PathBuf::new();
            vpath.push(path::Path::new(&ipath).parent().unwrap());
            vpath.push(vfile);
            vpath.as_os_str().to_os_string()
        });
        snap.vlog_fd = snap
            .config
            .to_value_log(dir, name)
            .as_ref()
            .map(|s| util::open_file_r(s.as_ref()))
            .transpose()?;

        Ok(snap) // Okey dockey
    }
}

// maintanence methods.
impl<K, V> Snapshot<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    /// Return number of entries in the snapshot.
    pub fn len(&self) -> u64 {
        self.to_stats().unwrap().n_count
    }

    /// Return the last seqno found in this snapshot.
    pub fn to_seqno(&self) -> u64 {
        self.to_stats().unwrap().seqno
    }

    /// Return the application metadata.
    pub fn to_metadata(&self) -> Result<Vec<u8>> {
        if let MetaItem::Metadata(data) = &self.meta[1] {
            Ok(data.clone())
        } else {
            let msg = "snapshot metadata missing".to_string();
            Err(Error::InvalidSnapshot(msg))
        }
    }

    /// Return Btree statistics.
    pub fn to_stats(&self) -> Result<Stats> {
        if let MetaItem::Stats(stats) = &self.meta[2] {
            Ok(stats.parse()?)
        } else {
            let msg = "snapshot statistics missing".to_string();
            Err(Error::InvalidSnapshot(msg))
        }
    }

    /// Return the file-position for Btree's root node.
    pub fn to_root(&self) -> Result<u64> {
        if let MetaItem::Root(root) = self.meta[3] {
            Ok(root)
        } else {
            Err(Error::InvalidSnapshot("snapshot root missing".to_string()))
        }
    }
}

impl<K, V> Index<K, V> for Snapshot<K, V>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
{
    type W = RobtWriter;

    /// Make a new empty index of this type, with same configuration.
    fn make_new(&self) -> Result<Box<Self>> {
        Ok(Box::new(Snapshot::open(
            self.name.as_str(),
            self.dir.as_str(),
        )?))
    }

    /// Create a new writer handle. Note that, not all indexes allow
    /// concurrent writers, and not all indexes support concurrent
    /// read/write.
    fn to_writer(&mut self) -> Self::W {
        panic!("Read-only-btree don't support write operations")
    }
}

impl<K, V> Footprint for Snapshot<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    fn footprint(&self) -> isize {
        let (dir, name) = (self.dir.as_str(), self.name.as_str());
        let mut footprint = fs::metadata(self.config.to_index_file(dir, name))
            .unwrap()
            .len();
        footprint += match self.config.to_value_log(dir, name) {
            Some(vlog_file) => fs::metadata(vlog_file).unwrap().len(),
            None => 0,
        };
        footprint.try_into().unwrap()
    }
}

// Read methods
impl<K, V> Reader<K, V> for Snapshot<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    fn get<Q>(&self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let _lock = self.mutex.lock();
        let snap = unsafe {
            let snap = self as *const Snapshot<K, V> as *mut Snapshot<K, V>;
            snap.as_mut().unwrap()
        };

        snap.do_get(key, false /*versions*/)
    }

    fn iter(&self) -> Result<IndexIter<K, V>> {
        let _lock = self.mutex.lock();
        let snap = unsafe {
            let snap = self as *const Snapshot<K, V> as *mut Snapshot<K, V>;
            snap.as_mut().unwrap()
        };

        let mut mzs = vec![];
        snap.build_fwd(snap.to_root().unwrap(), &mut mzs)?;
        Ok(Box::new(Iter {
            snap,
            mzs: mzs,
            versions: false,
        }))
    }

    fn range<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let _lock = self.mutex.lock();
        let snap = unsafe {
            let snap = self as *const Snapshot<K, V> as *mut Snapshot<K, V>;
            snap.as_mut().unwrap()
        };

        snap.do_range(range, false /*versions*/)
    }

    fn reverse<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let _lock = self.mutex.lock();
        let snap = unsafe {
            let snap = self as *const Snapshot<K, V> as *mut Snapshot<K, V>;
            snap.as_mut().unwrap()
        };

        snap.do_reverse(range, false /*versions*/)
    }

    fn get_with_versions<Q>(&self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let _lock = self.mutex.lock();
        let snap = unsafe {
            let snap = self as *const Snapshot<K, V> as *mut Snapshot<K, V>;
            snap.as_mut().unwrap()
        };

        snap.do_get(key, true /*versions*/)
    }

    /// Iterate over all entries in this index. Returned entry shall
    /// have all its previous versions, can be a costly call.
    fn iter_with_versions(&self) -> Result<IndexIter<K, V>> {
        let _lock = self.mutex.lock();
        let snap = unsafe {
            let snap = self as *const Snapshot<K, V> as *mut Snapshot<K, V>;
            snap.as_mut().unwrap()
        };

        let mut mzs = vec![];
        snap.build_fwd(snap.to_root().unwrap(), &mut mzs)?;
        Ok(Box::new(Iter {
            snap,
            mzs: mzs,
            versions: true,
        }))
    }

    /// Iterate from lower bound to upper bound. Returned entry shall
    /// have all its previous versions, can be a costly call.
    fn range_with_versions<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let _lock = self.mutex.lock();
        let snap = unsafe {
            let snap = self as *const Snapshot<K, V> as *mut Snapshot<K, V>;
            snap.as_mut().unwrap()
        };

        snap.do_range(range, true /*versions*/)
    }

    /// Iterate from upper bound to lower bound. Returned entry shall
    /// have all its previous versions, can be a costly call.
    fn reverse_with_versions<'a, R, Q>(&'a self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let _lock = self.mutex.lock();
        let snap = unsafe {
            let snap = self as *const Snapshot<K, V> as *mut Snapshot<K, V>;
            snap.as_mut().unwrap()
        };

        snap.do_reverse(range, true /*versions*/)
    }
}

impl<K, V> Snapshot<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    fn get_zpos<Q>(&mut self, key: &Q, fpos: u64) -> Result<u64>
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

    fn do_get<Q>(&mut self, key: &Q, versions: bool) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let zfpos = self.get_zpos(key, self.to_root().unwrap())?;

        let fd = &mut self.index_fd;
        let zblock: ZBlock<K, V> = ZBlock::new_decode(fd, zfpos, &self.config)?;
        match zblock.find(key, Bound::Unbounded, Bound::Unbounded) {
            Ok((_, entry)) => {
                if entry.as_key().borrow().eq(key) {
                    self.fetch(entry, versions)
                } else {
                    Err(Error::KeyNotFound)
                }
            }
            Err(Error::__ZBlockExhausted(_)) => unreachable!(),
            Err(err) => Err(err),
        }
    }

    fn do_range<'a, R, Q>(
        &'a mut self,
        range: R,
        versions: bool, // if true include older versions.
    ) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let mut mzs = vec![];
        let skip_one = match range.start_bound() {
            Bound::Unbounded => {
                self.build_fwd(self.to_root().unwrap(), &mut mzs)?;
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
        let mut r = Box::new(Range {
            snap: self,
            mzs,
            range,
            high: marker::PhantomData,
            versions,
        });
        if skip_one {
            r.next();
        }
        Ok(r)
    }

    fn do_reverse<'a, R, Q>(&'a mut self, range: R, versions: bool) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let mut mzs = vec![];
        let skip_one = match range.end_bound() {
            Bound::Unbounded => {
                self.build_rev(self.to_root().unwrap(), &mut mzs)?;
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
        let mut rr = Box::new(Reverse {
            snap: self,
            mzs,
            range: range,
            low: marker::PhantomData,
            versions,
        });
        if skip_one {
            rr.next();
        }
        Ok(rr)
    }

    fn build_fwd(
        &mut self,
        mut fpos: u64,           // from node
        mzs: &mut Vec<MZ<K, V>>, // output
    ) -> Result<()> {
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

    fn rebuild_fwd(&mut self, mzs: &mut Vec<MZ<K, V>>) -> Result<()> {
        let fd = &mut self.index_fd;
        let config = &self.config;

        match mzs.pop() {
            None => Ok(()),
            Some(MZ::Z { .. }) => unreachable!(),
            Some(MZ::M { fpos, mut index }) => {
                let mblock = MBlock::<K, V>::new_decode(fd, fpos, config)?;
                index += 1;
                match mblock.to_entry(index) {
                    Ok(MEntry::DecZ { fpos: zfpos, .. }) => {
                        mzs.push(MZ::M { fpos, index });

                        let zblock = ZBlock::new_decode(fd, zfpos, config)?;
                        mzs.push(MZ::Z { zblock, index: 0 });
                        Ok(())
                    }
                    Ok(MEntry::DecM { fpos: mfpos, .. }) => {
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
    ) -> Result<()> {
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

    fn rebuild_rev(&mut self, mzs: &mut Vec<MZ<K, V>>) -> Result<()> {
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
                    Ok(MEntry::DecZ { fpos: zfpos, .. }) => {
                        mzs.push(MZ::M { fpos, index });

                        let zblock = ZBlock::new_decode(fd, zfpos, config)?;
                        let index = zblock.len() - 1;
                        mzs.push(MZ::Z { zblock, index });
                        Ok(())
                    }
                    Ok(MEntry::DecM { fpos: mfpos, .. }) => {
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
    ) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut fpos = self.to_root().unwrap();
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

    fn fetch(
        &mut self,
        mut entry: Entry<K, V>,
        versions: bool, // fetch deltas as well
    ) -> Result<Entry<K, V>> {
        match &mut self.vlog_fd {
            Some(fd) => entry.fetch_value(fd)?,
            _ => (),
        }
        if versions {
            match &mut self.vlog_fd {
                Some(fd) => entry.fetch_deltas(fd)?,
                _ => (),
            }
        }
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
    versions: bool,
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Result<Entry<K, V>>> {
        match self.mzs.pop() {
            None => None,
            Some(mut z) => match z.next() {
                Some(Err(err)) => {
                    self.mzs.truncate(0);
                    Some(Err(err))
                }
                Some(Ok(entry)) => {
                    self.mzs.push(z);
                    Some(self.snap.fetch(entry, self.versions))
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
    K: Clone + Ord + Borrow<Q> + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    snap: &'a mut Snapshot<K, V>,
    mzs: Vec<MZ<K, V>>,
    range: R,
    high: marker::PhantomData<Q>,
    versions: bool,
}

impl<'a, K, V, R, Q> Range<'a, K, V, R, Q>
where
    K: Clone + Ord + Borrow<Q> + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
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
    K: Clone + Ord + Borrow<Q> + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Result<Entry<K, V>>> {
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
                        Some(self.snap.fetch(entry, self.versions))
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
    K: Clone + Ord + Borrow<Q> + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    snap: &'a mut Snapshot<K, V>,
    mzs: Vec<MZ<K, V>>,
    range: R,
    low: marker::PhantomData<Q>,
    versions: bool,
}

impl<'a, K, V, R, Q> Reverse<'a, K, V, R, Q>
where
    K: Clone + Ord + Borrow<Q> + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
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
    K: Clone + Ord + Borrow<Q> + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Clone + Serialize,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Result<Entry<K, V>>> {
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
                        Some(self.snap.fetch(entry, self.versions))
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
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Result<Entry<K, V>>> {
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
    fn next_back(&mut self) -> Option<Result<Entry<K, V>>> {
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

pub struct RobtWriter;

impl<K, V> Writer<K, V> for RobtWriter
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn set_index(
        &mut self,
        key: K,
        value: V,
        seqno: u64, // seqno for this mutation
    ) -> (Option<u64>, Result<Option<Entry<K, V>>>) {
        panic!(
            "{} {} {}",
            mem::size_of_val(&key),
            mem::size_of_val(&value),
            seqno
        )
    }

    fn set_cas_index(
        &mut self,
        key: K,
        value: V,
        cas: u64,
        seqno: u64, // seqno for this mutation
    ) -> (Option<u64>, Result<Option<Entry<K, V>>>) {
        panic!(
            "{} {} {} {}",
            mem::size_of_val(&key),
            mem::size_of_val(&value),
            seqno,
            cas
        )
    }

    fn delete_index<Q>(
        &mut self,
        key: &Q,
        seqno: u64, // seqno for this mutation
    ) -> (Option<u64>, Result<Option<Entry<K, V>>>)
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Ord + ?Sized,
    {
        panic!("{} {}", mem::size_of_val(key), seqno)
    }
}
