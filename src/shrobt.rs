//! Module `shrobt` implement an ordered set of index using Robt shards.

use crate::{
    core::{
        self, Bloom, CommitIter, CommitIterator, Diff, DiskIndexFactory, Entry, Footprint, Index,
        IndexIter, Reader, Result, Serialize, ThreadSafe, Validate,
    },
    error::Error,
    lsm,
    panic::Panic,
    robt::{self, Robt},
    scans, util,
};
use log::error;
use toml;

use std::{
    borrow::Borrow,
    cmp, convert,
    convert::{TryFrom, TryInto},
    ffi, fmt, fs,
    hash::Hash,
    io::{Read, Write},
    marker,
    ops::{Bound, RangeBounds},
    path, result,
    sync::{Arc, Mutex},
    thread,
};

#[derive(Clone)]
struct RootFileName(ffi::OsString);

impl From<String> for RootFileName {
    fn from(s: String) -> RootFileName {
        let file_name = format!("{}-shrobt.root", s);
        let name: &ffi::OsStr = file_name.as_ref();
        RootFileName(name.to_os_string())
    }
}

impl TryFrom<RootFileName> for String {
    type Error = crate::error::Error;

    fn try_from(name: RootFileName) -> Result<String> {
        use crate::error::Error::InvalidFile;
        let err = format!("not shrobt root file");

        let rootp = path::Path::new(&name.0);
        let ext = rootp
            .extension()
            .ok_or(InvalidFile(err.clone()))?
            .to_str()
            .ok_or(InvalidFile(err.clone()))?;

        if ext == "root" {
            let stem = rootp
                .file_stem()
                .ok_or(InvalidFile(err.clone()))?
                .to_str()
                .ok_or(InvalidFile(err.clone()))?
                .to_string();

            let parts: Vec<&str> = stem.split('-').collect();

            if parts.len() < 2 {
                Err(InvalidFile(err.clone()))
            } else if parts[parts.len() - 1] != "shrobt" {
                Err(InvalidFile(err.clone()))
            } else {
                Ok(parts[..(parts.len() - 1)].join("-"))
            }
        } else {
            Err(InvalidFile(err.clone()))
        }
    }
}

impl From<RootFileName> for ffi::OsString {
    fn from(s: RootFileName) -> ffi::OsString {
        s.0
    }
}

impl fmt::Display for RootFileName {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{}", self.0.to_str().unwrap())
    }
}

impl fmt::Debug for RootFileName {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{:?}", self.0)
    }
}

#[derive(Clone)]
struct ShardName(String);

impl From<(String, usize)> for ShardName {
    fn from((s, shard_i): (String, usize)) -> ShardName {
        ShardName(format!("{}-shrobt-shard-{:03}", s, shard_i))
    }
}

impl TryFrom<ShardName> for (String, usize) {
    type Error = crate::error::Error;

    fn try_from(name: ShardName) -> Result<(String, usize)> {
        use crate::error::Error::InvalidFile;

        let parts: Vec<&str> = name.0.split('-').collect();

        if parts.len() < 4 {
            Err(InvalidFile(format!("not shrobt index")))
        } else if parts[parts.len() - 2] != "shard" {
            Err(InvalidFile(format!("not shrobt index")))
        } else if parts[parts.len() - 3] != "shrobt" {
            Err(InvalidFile(format!("not shrobt index")))
        } else {
            let shard_i: usize = parts[parts.len() - 1]
                .parse()
                .map_err(|_| InvalidFile(format!("not shrobt index")))?;
            let s = parts[..(parts.len() - 3)].join("-");
            Ok((s, shard_i))
        }
    }
}

impl fmt::Display for ShardName {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for ShardName {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{:?}", self.0)
    }
}

/// Create a new factory with initial set of configuration.
///
/// To know more about other configurations supported by the ShrobtFactory.
///
/// * *num_shards*, number of shards to be used while building an index.
pub fn shrobt_factory<K, V, B>(config: robt::Config, num_shards: usize) -> ShrobtFactory<K, V, B>
where
    K: Clone + Ord + Serialize + ThreadSafe,
    V: Clone + Diff + Serialize + ThreadSafe,
    <V as Diff>::D: Serialize,
{
    ShrobtFactory {
        config,
        num_shards,

        _phantom_key: marker::PhantomData,
        _phantom_val: marker::PhantomData,
        _phantom_bmap: marker::PhantomData,
    }
}

/// ShrobtFactory captures a set of configuration for creating new ShRobt
/// instances.
///
/// By implementing `WriteIndexFactory` trait this can be
/// used with other, more sophisticated, index implementations.
pub struct ShrobtFactory<K, V, B>
where
    K: Clone + Ord + Serialize + ThreadSafe,
    V: Clone + Diff + Serialize + ThreadSafe,
    <V as Diff>::D: Serialize,
{
    config: robt::Config,
    num_shards: usize,

    _phantom_key: marker::PhantomData<K>,
    _phantom_val: marker::PhantomData<V>,
    _phantom_bmap: marker::PhantomData<B>,
}

impl<K, V, B> ShrobtFactory<K, V, B>
where
    K: Clone + Ord + Serialize + ThreadSafe,
    V: Clone + Diff + Serialize + ThreadSafe,
    <V as Diff>::D: Serialize,
{
    fn new_root_file(dir: &ffi::OsStr, name: &str, num_shards: usize) -> Result<ffi::OsString> {
        use toml::Value;

        let root_file: ffi::OsString = {
            let rootf: RootFileName = name.to_string().into();
            let mut rootp = path::PathBuf::from(dir);
            rootp.push(&rootf.0);
            rootp.into_os_string()
        };

        let text = {
            let num_shards: i64 = num_shards.try_into()?;
            let mut dict = toml::map::Map::new();
            dict.insert("num_shards".to_string(), Value::Integer(num_shards));
            Value::Table(dict).to_string()
        };

        let mut fd = util::create_file_a(root_file.clone())?;
        fd.write(text.as_bytes())?;
        Ok(root_file.into())
    }

    fn open_root_file(dir: &ffi::OsStr, root: &ffi::OsStr) -> Result<toml::Value> {
        use crate::error::Error::InvalidFile;

        let _: String = TryFrom::try_from(RootFileName(root.to_os_string()))?;
        let root_file = {
            let mut rootp = path::PathBuf::from(dir);
            rootp.push(root);
            rootp.into_os_string()
        };

        let text = {
            let mut fd = util::open_file_r(&root_file)?;
            let mut bytes = vec![];
            fd.read_to_end(&mut bytes)?;
            std::str::from_utf8(&bytes)?.to_string()
        };
        let value: toml::Value = text
            .parse()
            .map_err(|_| InvalidFile(format!("shrobt, invalid root file")))?;
        Ok(value)
    }
}

impl<K, V, B> DiskIndexFactory<K, V> for ShrobtFactory<K, V, B>
where
    K: Default + Clone + Ord + Hash + Footprint + Serialize + ThreadSafe,
    V: Default + Clone + Diff + Footprint + Serialize + ThreadSafe,
    <V as Diff>::D: Default + Serialize,
    B: Sync + Bloom + ThreadSafe,
{
    type I = ShRobt<K, V, B>;

    fn new(&self, dir: &ffi::OsStr, name: &str) -> Result<ShRobt<K, V, B>> {
        ShRobt::new(dir, name, self.config.clone(), self.num_shards)
    }

    fn open(&self, dir: &ffi::OsStr, root: ffi::OsString) -> Result<ShRobt<K, V, B>> {
        ShRobt::open(dir, root)
    }

    fn to_type(&self) -> String {
        "shrobt".to_string()
    }
}

/// Range partitioned index using [Robt] shards.
pub struct ShRobt<K, V, B>
where
    K: Clone + Ord + Hash + Footprint + Serialize + ThreadSafe,
    V: Clone + Diff + Footprint + Serialize + ThreadSafe,
    <V as Diff>::D: Serialize,
    B: Bloom + ThreadSafe,
{
    name: String,
    root: ffi::OsString,

    seqno: u64,
    count: usize,
    metadata: Vec<u8>,
    shards: Arc<Mutex<Vec<Shard<K, V, B>>>>,
}

/// Create and configure a range partitioned index.
impl<K, V, B> ShRobt<K, V, B>
where
    K: Default + Clone + Ord + Hash + Footprint + Serialize + ThreadSafe,
    V: Default + Clone + Diff + Footprint + Serialize + ThreadSafe,
    <V as Diff>::D: Default + Clone + Serialize,
    B: Bloom + ThreadSafe,
{
    /// Create a new instance of range-partitioned index using Llrb tree.
    pub fn new(
        dir: &ffi::OsStr,
        name: &str,
        config: robt::Config,
        num_shards: usize,
    ) -> Result<ShRobt<K, V, B>> {
        let root = ShrobtFactory::<K, V, B>::new_root_file(
            // create a new root file
            dir, name, num_shards,
        )?;

        let mut shards = vec![];
        for shard_i in 0..num_shards {
            let (name, _): (String, usize) = {
                let name: ShardName = (name.to_string(), shard_i).into();
                name.try_into()?
            };
            let index = Robt::new(dir, &name, config.clone())?;
            shards.push(Shard::new_build(index));
        }

        let name: &str = name.as_ref();
        Ok(ShRobt {
            name: name.to_string(),
            root,
            seqno: Default::default(),
            count: Default::default(),
            metadata: Default::default(),
            shards: Arc::new(Mutex::new(shards)),
        })
    }

    pub fn open(dir: &ffi::OsStr, root: ffi::OsString) -> Result<ShRobt<K, V, B>> {
        use crate::error::Error::InvalidFile;

        let name: String = TryFrom::try_from(RootFileName(root.clone()))?;

        let num_shards: usize = {
            let err1 = InvalidFile(format!("shrobt, not a table"));
            let err2 = InvalidFile(format!("shrobt, missing num_shards"));
            let err3 = InvalidFile(format!("shrobt, num_shards not int"));

            let root = root.as_os_str();
            let value = ShrobtFactory::<K, V, B>::open_root_file(dir, root)?;

            let num_shards = value
                .as_table()
                .ok_or(err1)?
                .get("num_shards")
                .ok_or(err2)?
                .as_integer()
                .ok_or(err3)?
                .try_into()?;

            if num_shards > 0 {
                Ok(num_shards)
            } else {
                Err(InvalidFile(format!("shrobt, num_shards == {}", num_shards)))
            }
        }?;

        let mut indexes: Vec<Option<Robt<K, V, B>>> = vec![];
        (0..num_shards).for_each(|_| indexes.push(None));

        let items: Vec<(usize, Robt<K, V, B>)> = fs::read_dir(dir)?
            .filter_map(|item| item.ok())
            .filter_map(|item| Robt::open(dir, item.file_name()).ok())
            .filter_map(|index| match index.to_name().ok() {
                Some(nm) => Some((nm, index)),
                None => None,
            })
            .filter_map(|(nm, index)| {
                let nm = ShardName(nm);
                let (nm, index_i): (String, usize) = TryFrom::try_from(nm).ok()?;
                Some((nm, index_i, index))
            })
            .filter(|(nm, _, _)| &name == nm)
            .filter_map(|(_, index_i, index)| Some((index_i, index)))
            .collect();

        for (index_i, index) in items.into_iter() {
            indexes[index_i] = match indexes[index_i].take() {
                None => Some(index),
                Some(old_index) => {
                    if old_index.to_version()? < index.to_version()? {
                        old_index.purge()?;
                        Some(index)
                    } else {
                        index.purge()?;
                        Some(old_index)
                    }
                }
            };
        }

        for (i, index) in indexes.iter().enumerate() {
            if index.is_none() {
                return Err(InvalidFile(format!("shrobt, missing index {}", i)));
            }
        }

        let mut indexes: Vec<Robt<K, V, B>> =
            indexes.into_iter().filter_map(convert::identity).collect();

        let (seqno, count, metadata) = {
            let mut seqno = std::u64::MIN;
            let mut count = 0;
            let mut metadata: Option<Vec<u8>> = None;
            for index in indexes.iter_mut() {
                seqno = cmp::max(seqno, index.to_seqno()?);
                count += index.to_reader()?.len()?;
                metadata.get_or_insert(index.to_metadata()?);
                assert_eq!(metadata.clone().unwrap(), index.to_metadata()?);
            }
            (seqno, count, metadata)
        };

        let shards = robts_to_shards(indexes)?;

        Ok(ShRobt {
            name: name.clone(),
            root,

            seqno,
            count,
            metadata: metadata.unwrap(),
            shards: Arc::new(Mutex::new(shards)),
        })
    }

    fn to_state(&self) -> Result<&str> {
        let shards = self.shards.lock().unwrap();

        let is_build = shards.iter().all(|s| s.is_build());
        let is_snapshot = shards.iter().all(|s| s.is_snapshot());

        if is_build {
            Ok("build")
        } else if is_snapshot {
            Ok("snapshot")
        } else {
            Err(Error::UnexpectedFail(format!("shrobt, mixed shard state")))
        }
    }
}

fn robts_to_shards<K, V, B>(mut indexes: Vec<Robt<K, V, B>>) -> Result<Vec<Shard<K, V, B>>>
where
    K: Default + Clone + Ord + Hash + Footprint + Serialize,
    V: Default + Clone + Diff + Footprint + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
    B: Bloom,
{
    indexes.reverse();

    let mut high_key = Bound::Unbounded;
    let mut shards = vec![];
    for mut index in indexes.into_iter() {
        let mut snapshot = index.to_reader()?;
        shards.push(Shard::new_snapshot(high_key, index));
        high_key = Bound::Excluded(snapshot.first()?.to_key());
    }

    shards.reverse();

    Ok(shards)
}

/// Maintenance API.
impl<K, V, B> ShRobt<K, V, B>
where
    K: Default + Clone + Ord + Hash + Footprint + Serialize + ThreadSafe,
    V: Default + Clone + Diff + Footprint + Serialize + ThreadSafe,
    B: Bloom + ThreadSafe,
    <V as Diff>::D: Default + Clone + Serialize,
{
    #[inline]
    pub fn len(&self) -> usize {
        self.count
    }

    #[inline]
    pub fn to_name(&self) -> String {
        self.name.clone()
    }

    pub fn to_stats(&mut self) -> Result<robt::Stats> {
        let mut shards = self.shards.lock().unwrap();

        let mut stats: robt::Stats = Default::default();

        for shard in shards.iter_mut() {
            stats = stats.merge(shard.to_snapshot()?.to_stats()?)
        }

        assert_eq!(stats.seqno, self.seqno);

        stats.name = self.name.clone();

        Ok(stats)
    }

    fn to_ranges(&self) -> Vec<(Bound<K>, Bound<K>)> {
        let mut shards = self.shards.lock().unwrap();

        let high_keys: Vec<Bound<K>> = shards
            .iter_mut()
            .filter_map(|shard| shard.to_high_key())
            .collect();
        util::high_keys_to_ranges(high_keys)
    }

    fn to_footprints(&self) -> Result<Vec<isize>> {
        let mut shards = self.shards.lock().unwrap();

        let mut footprints = vec![];
        for shard in shards.iter_mut() {
            footprints.push(shard.as_robt().footprint()?);
        }

        Ok(footprints)
    }

    fn to_partitions(&self) -> Result<Vec<(isize, Bound<K>, Bound<K>)>> {
        let mut shards = self.shards.lock().unwrap();

        let mut partitions: Vec<(isize, Bound<K>, Bound<K>)> = vec![];
        for shard in shards.iter_mut() {
            let robt_index = shard.as_mut_robt();

            let mut ps: Vec<(isize, Bound<K>, Bound<K>)> = {
                let ps = robt_index.to_partitions()?;
                let q = robt_index.footprint()? / (ps.len() as isize);
                ps.into_iter().map(|(lk, hk)| (q, lk, hk)).collect()
            };

            let stitch_item = {
                ps.reverse();
                let item = match (partitions.pop(), ps.pop()) {
                    (None, Some(_)) => None,
                    (Some((zf, zl, zh)), Some((af, al, ah))) => {
                        assert!(zh == Bound::Unbounded);
                        assert!(al == Bound::Unbounded);
                        Some((zf + af, zl, ah))
                    }
                    (Some(a), None) => Some(a),
                    (None, None) => None,
                };
                ps.reverse();
                item
            };

            if let Some(item) = stitch_item {
                partitions.push(item);
            }
            partitions.extend_from_slice(&ps);
        }

        Ok(partitions)
    }

    fn rebalance(&self) -> Result<Option<Vec<(Bound<K>, Bound<K>)>>> {
        let footprints = self.to_footprints()?;
        let num_shards = footprints.len();

        let footprint: usize = self.footprint()?.try_into().unwrap();
        let avg = footprint / num_shards;

        let ok = footprints
            .into_iter()
            .map(|footprint| (footprint as f64) / (avg as f64))
            .any(|ratio| (ratio < 0.5) || (ratio > 1.5)); // TODO: no magic
        if ok {
            let partitions = self.to_partitions()?;
            let ranges = util::as_sharded_array(&partitions, num_shards);
            let ranges = ranges
                .into_iter()
                .map(|rs| match (rs.first(), rs.last()) {
                    (Some((_, l, _)), Some((_, _, h))) => (l.clone(), h.clone()),
                    _ => unreachable!(),
                })
                .collect();
            Ok(Some(ranges))
        } else {
            Ok(None)
        }
    }

    fn to_range_scans<'a>(
        &mut self,
        re_ranges: Vec<(Bound<K>, Bound<K>)>,
    ) -> Result<Vec<IndexIter<'a, K, V>>> {
        let mut shards = self.shards.lock().unwrap();

        let mut outer_iters = vec![];
        for re_range in re_ranges.into_iter() {
            let mut iters = vec![];
            for shard in shards.iter_mut() {
                let snap = shard.as_mut_robt().to_reader()?;
                let iter = snap.into_range_scan(re_range.clone())?;
                iters.push(Box::new(iter) as IndexIter<K, V>);
            }
            outer_iters.push(Box::new(Iter::new(iters)) as IndexIter<K, V>)
        }

        Ok(outer_iters)
    }

    fn transform_metadatas<F>(&self, metacb: F) -> Result<Vec<Vec<u8>>>
    where
        F: Fn(Vec<u8>) -> Vec<u8>,
    {
        let shards = self.shards.lock().unwrap();

        let mut metas = vec![];
        for shard in shards.iter() {
            metas.push(metacb(shard.as_robt().to_metadata()?))
        }

        Ok(metas)
    }
}

impl<K, V, B> Index<K, V> for ShRobt<K, V, B>
where
    K: Default + Clone + Ord + Hash + Footprint + Serialize + ThreadSafe,
    V: Default + Clone + Diff + Footprint + Serialize + ThreadSafe,
    <V as Diff>::D: Default + Clone + Serialize,
    B: Sync + Bloom + ThreadSafe,
{
    type R = ShrobtReader<K, V, B>;
    type W = Panic;
    type O = ffi::OsString;

    #[inline]
    fn to_name(&self) -> Result<String> {
        Ok(self.name.clone())
    }

    #[inline]
    fn to_root(&self) -> Result<Self::O> {
        Ok(self.root.clone())
    }

    #[inline]
    fn to_metadata(&self) -> Result<Vec<u8>> {
        Ok(self.metadata.clone())
    }

    #[inline]
    fn to_seqno(&self) -> Result<u64> {
        Ok(self.seqno)
    }

    #[inline]
    fn set_seqno(&mut self, _seqno: u64) -> Result<()> {
        // no-op
        Ok(())
    }

    fn to_reader(&mut self) -> Result<Self::R> {
        let mut shards = self.shards.lock().unwrap();

        let mut readers = vec![];
        for shard in shards.iter_mut().rev() {
            let high_key = shard.to_high_key().unwrap();
            let snapshot = shard.as_mut_robt().to_reader()?;
            readers.push(ShardReader::new(high_key, snapshot));
        }

        ShrobtReader::new(self.name.clone(), readers)
    }

    fn to_writer(&mut self) -> Result<Self::W> {
        Ok(Panic::new("shrobt"))
    }

    // holds global lock. no other operations are allowed.
    fn commit<C, F>(&mut self, mut scanner: core::CommitIter<K, V, C>, metacb: F) -> Result<()>
    where
        C: CommitIterator<K, V>,
        F: Fn(Vec<u8>) -> Vec<u8>,
    {
        let within = scanner.to_within();
        let re_ranges = self.rebalance()?;
        let re_iters = match re_ranges.clone() {
            Some(re_ranges) => Some(self.to_range_scans(re_ranges)?),
            None => None,
        };
        let metas: Vec<Vec<u8>> = self.transform_metadatas(metacb)?;

        let mut shards = self.shards.lock().unwrap();

        let iters = match (self.to_state()?, re_ranges, re_iters) {
            ("build", _, _) => {
                let iters = scanner.scans(shards.len())?;
                if shards.len() == iters.len() {
                    Ok(iters)
                } else {
                    Err(Error::UnexpectedFail(format!(
                        "shrobt.commit(), {}/{} shards",
                        shards.len(),
                        iters.len()
                    )))
                }
            }
            ("snapshot", Some(re_ranges), Some(re_iters)) => {
                let src_iters = scanner.range_scans(re_ranges.clone())?;

                let reverse = false;
                let iters = src_iters
                    .into_iter()
                    .zip(re_iters.into_iter())
                    .into_iter()
                    .map(|(i1, i2)| lsm::y_iter(i1, i2, reverse))
                    .collect();

                for shard in shards.iter_mut() {
                    shard.as_mut_robt().to_next_build()?;
                }

                Ok(iters)
            }
            ("snapshot", None, _) => Ok(scanner.range_scans(self.to_ranges())?),
            _ => unreachable!(),
        }?;

        let indexes: Vec<Robt<K, V, B>> = {
            let iter = shards.drain(..).map(|shard| shard.into_robt());
            iter.collect()
        };

        // scatter
        let mut threads = vec![];
        let iter = indexes
            .into_iter()
            .zip(iters.into_iter())
            .into_iter()
            .zip(metas.into_iter())
            .enumerate();
        for (off, ((index, iter), meta)) in iter {
            let iter = unsafe {
                let iter = Box::leak(iter);
                let iter = iter as *mut dyn Iterator<Item = Result<Entry<K, V>>>;
                Box::from_raw(iter as *mut ffi::c_void)
            };

            threads.push(thread::spawn(move || {
                do_commit(off, index, iter, meta, within.clone())
            }));
        }

        // gather
        let mut indexes = vec![];
        let mut errs = vec![];
        for t in threads.into_iter() {
            match t.join() {
                Ok(Ok((off, index))) => indexes.push((off, index)),
                Ok(Err(err)) => {
                    error!(target: "shrobt", "commit: {:?}", err);
                    errs.push(err);
                }
                Err(err) => {
                    error!(target: "shrobt", "commit-thread: {:?}", err);
                    errs.push(Error::ThreadFail(format!("{:?}", err)));
                }
            }
        }

        let indexes: Vec<Robt<K, V, B>> = if errs.len() == 0 {
            indexes.sort_by(|x, y| x.0.cmp(&y.0));
            Ok(indexes.into_iter().map(|x| x.1).collect())
        } else {
            Err(Error::DiskIndexFail(
                errs.into_iter()
                    .map(|e| format!("{:?}", e))
                    .collect::<Vec<String>>()
                    .join("; "),
            ))
        }?;

        robts_to_shards(indexes)?
            .drain(..)
            .for_each(|shard| shards.push(shard));

        Ok(())
    }

    fn compact<F>(&mut self, cutoff: Bound<u64>, metacb: F) -> Result<usize>
    where
        F: Fn(Vec<u8>) -> Vec<u8>,
    {
        let metas: Vec<Vec<u8>> = self.transform_metadatas(metacb)?;

        let mut shards = self.shards.lock().unwrap();

        let indexes: Vec<Robt<K, V, B>> = {
            let iter = shards.drain(..).map(|shard| shard.into_robt());
            iter.collect()
        };

        // scatter
        let iter = indexes.into_iter().zip(metas.into_iter()).enumerate();
        let mut threads = vec![];
        for (off, (index, meta)) in iter {
            threads.push(thread::spawn(move || {
                do_compact(off, index, cutoff.clone(), meta)
            }));
        }

        // gather
        let (mut indexes, mut errs, mut count) = (vec![], vec![], 0);
        for t in threads.into_iter() {
            match t.join() {
                Ok(Ok((off, cnt, index))) => {
                    count += cnt;
                    indexes.push((off, index));
                }
                Ok(Err(err)) => {
                    error!(target: "shrobt", "compact: {:?}", err);
                    errs.push(err);
                }
                Err(err) => {
                    error!(target: "shrobt", "compact-thread: {:?}", err);
                    errs.push(Error::ThreadFail(format!("{:?}", err)));
                }
            }
        }

        let indexes: Vec<Robt<K, V, B>> = if errs.len() == 0 {
            indexes.sort_by(|x, y| x.0.cmp(&y.0));
            Ok(indexes.into_iter().map(|x| x.1).collect())
        } else {
            Err(Error::DiskIndexFail(
                errs.into_iter()
                    .map(|e| format!("{:?}", e))
                    .collect::<Vec<String>>()
                    .join("; "),
            ))
        }?;

        robts_to_shards(indexes)?
            .drain(..)
            .for_each(|shard| shards.push(shard));

        Ok(count)
    }

    fn close(self) -> Result<()> {
        let mut shards = self.shards.lock().unwrap();

        for shard in shards.drain(..) {
            shard.into_robt().close()?
        }

        Ok(())
    }

    fn purge(self) -> Result<()> {
        let mut shards = self.shards.lock().unwrap();

        for shard in shards.drain(..) {
            shard.into_robt().purge()?
        }

        Ok(())
    }
}

fn do_commit<K, V, B>(
    off: usize,
    mut index: Robt<K, V, B>,
    iter: Box<ffi::c_void>,
    meta: Vec<u8>,
    within: (Bound<u64>, Bound<u64>),
) -> Result<(usize, Robt<K, V, B>)>
where
    K: Default + Clone + Ord + Hash + Footprint + Serialize + ThreadSafe,
    V: Default + Clone + Diff + Footprint + Serialize + ThreadSafe,
    B: Bloom + ThreadSafe,
    <V as Diff>::D: Default + Clone + Serialize,
{
    let iter = {
        let iter = unsafe {
            let iter = Box::leak(iter);
            Box::from_raw(iter as *mut ffi::c_void as *mut IndexIter<K, V>)
        };
        CommitIter::new(scans::CommitWrapper::new(iter), within)
    };

    index.commit(iter, |_| meta.clone())?;
    Ok((off, index))
}

fn do_compact<K, V, B>(
    off: usize,
    mut index: Robt<K, V, B>,
    cutoff: Bound<u64>,
    meta: Vec<u8>,
) -> Result<(usize, usize, Robt<K, V, B>)>
where
    K: Default + Clone + Ord + Hash + Footprint + Serialize + ThreadSafe,
    V: Default + Clone + Diff + Footprint + Serialize + ThreadSafe,
    B: Bloom + ThreadSafe,
    <V as Diff>::D: Default + Clone + Serialize,
{
    let count = index.compact(cutoff, |_| meta.clone())?;
    Ok((off, count, index))
}

impl<K, V, B> Footprint for ShRobt<K, V, B>
where
    K: Default + Clone + Ord + Hash + Footprint + Serialize + ThreadSafe,
    V: Default + Clone + Diff + Footprint + Serialize + ThreadSafe,
    <V as Diff>::D: Default + Clone + Serialize + Footprint,
    B: Bloom + ThreadSafe,
{
    fn footprint(&self) -> Result<isize> {
        let shards = self.shards.lock().unwrap();

        let mut footprint = 0;
        for shard in shards.iter() {
            footprint += shard.as_robt().footprint()?
        }
        Ok(footprint)
    }
}

impl<K, V, B> CommitIterator<K, V> for ShRobt<K, V, B>
where
    K: Default + Clone + Ord + Hash + Footprint + Serialize + ThreadSafe,
    V: Default + Clone + Diff + Footprint + Serialize + ThreadSafe,
    <V as Diff>::D: Default + Clone + Serialize + Footprint,
    B: Bloom + ThreadSafe,
{
    fn scan<G>(&mut self, within: G) -> Result<IndexIter<K, V>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let mut shards = self.shards.lock().unwrap();

        let mut iters = vec![];
        for shard in shards.iter_mut() {
            let snap = shard.as_mut_robt().to_reader()?;
            iters.push(snap.into_scan()?);
        }

        Ok(Box::new(scans::FilterScans::new(iters, within)))
    }

    fn scans<G>(&mut self, shards: usize, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let partitions: Vec<(isize, Bound<K>, Bound<K>)> = self.to_partitions()?;
        assert!(partitions.len() > shards);

        let ranges = util::as_sharded_array(&partitions, shards);
        let ranges: Vec<(Bound<K>, Bound<K>)> = ranges
            .into_iter()
            .map(|rs| match (rs.first(), rs.last()) {
                (Some((_, l, _)), Some((_, _, h))) => (l.clone(), h.clone()),
                _ => unreachable!(),
            })
            .collect();

        let mut shards = self.shards.lock().unwrap();

        let mut iters = vec![];
        for range in ranges.into_iter() {
            let mut scans = vec![];
            for shard in shards.iter_mut() {
                let snap = shard.as_mut_robt().to_reader()?;
                scans.push(snap.into_range_scan(range.clone())?)
            }
            let iter = scans::FilterScans::new(scans, within.clone());
            iters.push(Box::new(iter) as IndexIter<K, V>);
        }

        Ok(iters)
    }

    fn range_scans<N, G>(&mut self, ranges: Vec<N>, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        N: Clone + RangeBounds<K>,
        G: Clone + RangeBounds<u64>,
    {
        let mut shards = self.shards.lock().unwrap();

        let mut iters = vec![];
        for range in ranges.into_iter() {
            let mut scans = vec![];
            for shard in shards.iter_mut() {
                let snap = shard.as_mut_robt().to_reader()?;
                let range = util::to_start_end(range.clone());
                scans.push(snap.into_range_scan(range)?)
            }
            let iter = scans::FilterScans::new(scans, within.clone());
            iters.push(Box::new(iter) as IndexIter<K, V>);
        }

        Ok(iters)
    }
}

impl<K, V, B> Validate<robt::Stats> for ShRobt<K, V, B>
where
    K: Default + Clone + Ord + fmt::Debug + Hash + Footprint + Serialize + ThreadSafe,
    V: Default + Clone + Diff + Footprint + Serialize + ThreadSafe,
    <V as Diff>::D: Default + Clone + Serialize,
    B: Bloom + ThreadSafe,
{
    fn validate(&mut self) -> Result<robt::Stats> {
        let mut shards = self.shards.lock().unwrap();

        let mut stats: robt::Stats = Default::default();

        for shard in shards.iter_mut() {
            stats = stats.merge(shard.to_snapshot()?.validate()?)
        }

        Ok(stats)
    }
}

/// Read handle into [ShRobt] index.
pub struct ShrobtReader<K, V, B>
where
    K: Ord + Clone + Serialize,
    V: Clone + Diff + Serialize,
{
    name: String,
    readers: Vec<ShardReader<K, V, B>>,
}

impl<K, V, B> ShrobtReader<K, V, B>
where
    K: Ord + Clone + Serialize,
    V: Clone + Diff + Serialize,
{
    fn new(name: String, readers: Vec<ShardReader<K, V, B>>) -> Result<ShrobtReader<K, V, B>> {
        Ok(ShrobtReader { name, readers })
    }

    fn find<'a, Q>(
        key: &Q,
        rs: &'a mut [ShardReader<K, V, B>],
    ) -> (usize, &'a mut ShardReader<K, V, B>)
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match rs.len() {
            0 => unreachable!(),
            1 => (0, &mut rs[0]),
            2 => {
                if ShardReader::less(key, &rs[0]) {
                    (0, &mut rs[0])
                } else {
                    (1, &mut rs[1])
                }
            }
            n => {
                let pivot = n / 2;
                if ShardReader::less(key, &rs[pivot]) {
                    Self::find(key, &mut rs[..pivot + 1])
                } else {
                    let (off, sr) = Self::find(key, &mut rs[pivot + 1..]);
                    (pivot + 1 + off, sr)
                }
            }
        }
    }
}

impl<K, V, B> Reader<K, V> for ShrobtReader<K, V, B>
where
    K: Default + Clone + Ord + Serialize,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
    B: Bloom,
{
    fn get<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + Hash,
    {
        let (_, reader) = Self::find(key, self.readers.as_mut_slice());
        reader.snapshot.get(key)
    }

    fn iter(&mut self) -> Result<IndexIter<K, V>> {
        let mut iters = vec![];
        for reader in self.readers.iter_mut() {
            iters.push(reader.snapshot.iter()?);
        }
        Ok(Box::new(Iter::new(iters)))
    }

    fn range<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let start = match range.start_bound() {
            Bound::Excluded(lr) | Bound::Included(lr) => {
                Self::find(lr, self.readers.as_mut_slice()).0
            }
            Bound::Unbounded => 0,
        };

        let mut iters = vec![];
        for reader in self.readers[start..].iter_mut() {
            iters.push(reader.snapshot.range(range.clone())?);

            let ok = match (range.end_bound(), reader.high_key.clone()) {
                (Bound::Unbounded, _) => true,
                (_, Bound::Unbounded) => false, // last shard.
                (Bound::Included(hr), Bound::Excluded(hk)) => hr.ge(hk.borrow()),
                (Bound::Excluded(hr), Bound::Excluded(hk)) => hr.gt(hk.borrow()),
                _ => unreachable!(),
            };
            if !ok {
                break;
            }
        }
        Ok(Box::new(Iter::new(iters)))
    }

    fn reverse<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let start = match range.start_bound() {
            Bound::Excluded(lr) | Bound::Included(lr) => {
                Self::find(lr, self.readers.as_mut_slice()).0
            }
            Bound::Unbounded => 0,
        };

        let mut iters = vec![];
        for reader in self.readers[start..].iter_mut() {
            iters.push(reader.snapshot.reverse(range.clone())?);

            let ok = match (range.end_bound(), reader.high_key.clone()) {
                (Bound::Unbounded, _) => true,
                (_, Bound::Unbounded) => false, // last shard.
                (Bound::Included(hr), Bound::Excluded(hk)) => hr.ge(hk.borrow()),
                (Bound::Excluded(hr), Bound::Excluded(hk)) => hr.ge(hk.borrow()),
                _ => unreachable!(),
            };
            if !ok {
                break;
            }
        }

        iters.reverse();
        Ok(Box::new(Iter::new(iters)))
    }

    fn get_with_versions<Q>(&mut self, key: &Q) -> Result<Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + Hash,
    {
        let (_, reader) = Self::find(key, self.readers.as_mut_slice());
        reader.snapshot.get_with_versions(key)
    }

    fn iter_with_versions(&mut self) -> Result<IndexIter<K, V>> {
        let mut iters = vec![];
        for reader in self.readers.iter_mut() {
            iters.push(reader.snapshot.iter_with_versions()?);
        }
        Ok(Box::new(Iter::new(iters)))
    }

    fn range_with_versions<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let start = match range.start_bound() {
            Bound::Excluded(lr) | Bound::Included(lr) => {
                Self::find(lr, self.readers.as_mut_slice()).0
            }
            Bound::Unbounded => 0,
        };

        let mut iters = vec![];
        for reader in self.readers[start..].iter_mut() {
            iters.push(reader.snapshot.range_with_versions(range.clone())?);

            let ok = match (range.end_bound(), reader.high_key.clone()) {
                (Bound::Unbounded, _) => true,
                (_, Bound::Unbounded) => false, // last shard.
                (Bound::Included(hr), Bound::Excluded(hk)) => hr.ge(hk.borrow()),
                (Bound::Excluded(hr), Bound::Excluded(hk)) => hr.gt(hk.borrow()),
                _ => unreachable!(),
            };
            if !ok {
                break;
            }
        }
        Ok(Box::new(Iter::new(iters)))
    }

    fn reverse_with_versions<'a, R, Q>(&'a mut self, range: R) -> Result<IndexIter<K, V>>
    where
        K: Borrow<Q>,
        R: 'a + Clone + RangeBounds<Q>,
        Q: 'a + Ord + ?Sized,
    {
        let start = match range.start_bound() {
            Bound::Excluded(lr) | Bound::Included(lr) => {
                Self::find(lr, self.readers.as_mut_slice()).0
            }
            Bound::Unbounded => 0,
        };

        let mut iters = vec![];
        for reader in self.readers[start..].iter_mut() {
            iters.push(reader.snapshot.reverse_with_versions(range.clone())?);

            let ok = match (range.end_bound(), reader.high_key.clone()) {
                (Bound::Unbounded, _) => true,
                (_, Bound::Unbounded) => false, // last shard.
                (Bound::Included(hr), Bound::Excluded(hk)) => hr.ge(hk.borrow()),
                (Bound::Excluded(hr), Bound::Excluded(hk)) => hr.ge(hk.borrow()),
                _ => unreachable!(),
            };
            if !ok {
                break;
            }
        }

        iters.reverse();
        Ok(Box::new(Iter::new(iters)))
    }
}

pub enum Shard<K, V, B>
where
    K: Clone + Ord + Hash + Footprint + Serialize,
    V: Clone + Diff + Footprint + Serialize,
    <V as Diff>::D: Serialize,
    B: Bloom,
{
    Build {
        inner: Robt<K, V, B>,
    },
    Snapshot {
        high_key: Bound<K>,
        inner: Robt<K, V, B>,
    },
}

impl<K, V, B> Shard<K, V, B>
where
    K: Clone + Ord + Hash + Footprint + Serialize,
    V: Clone + Diff + Footprint + Serialize,
    <V as Diff>::D: Serialize,
    B: Bloom,
{
    fn new_build(inner: Robt<K, V, B>) -> Shard<K, V, B> {
        Shard::Build { inner }
    }

    fn new_snapshot(high_key: Bound<K>, inner: Robt<K, V, B>) -> Shard<K, V, B> {
        Shard::Snapshot { high_key, inner }
    }
}

impl<K, V, B> Shard<K, V, B>
where
    K: Default + Clone + Ord + Hash + Footprint + Serialize,
    V: Default + Clone + Diff + Footprint + Serialize,
    <V as Diff>::D: Default + Serialize,
    B: Bloom,
{
    fn is_build(&self) -> bool {
        match self {
            Shard::Build { .. } => true,
            Shard::Snapshot { .. } => false,
        }
    }

    fn is_snapshot(&self) -> bool {
        match self {
            Shard::Build { .. } => false,
            Shard::Snapshot { .. } => true,
        }
    }

    fn into_robt(self) -> Robt<K, V, B> {
        match self {
            Shard::Build { inner } => inner,
            Shard::Snapshot { inner, .. } => inner,
        }
    }

    fn as_robt(&self) -> &Robt<K, V, B> {
        match self {
            Shard::Build { inner } => inner,
            Shard::Snapshot { inner, .. } => inner,
        }
    }

    fn as_mut_robt(&mut self) -> &mut Robt<K, V, B> {
        match self {
            Shard::Build { inner } => inner,
            Shard::Snapshot { inner, .. } => inner,
        }
    }

    fn to_snapshot(&mut self) -> Result<robt::Snapshot<K, V, B>> {
        match self {
            Shard::Snapshot { inner, .. } => inner.to_reader(),
            Shard::Build { .. } => unreachable!(),
        }
    }

    fn to_high_key(&mut self) -> Option<Bound<K>> {
        match self {
            Shard::Snapshot { high_key, .. } => Some(high_key.clone()),
            Shard::Build { .. } => None,
        }
    }
}

struct ShardReader<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    high_key: Bound<K>,
    snapshot: robt::Snapshot<K, V, B>,
}

impl<K, V, B> ShardReader<K, V, B>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
{
    fn new(high_key: Bound<K>, snapshot: robt::Snapshot<K, V, B>) -> ShardReader<K, V, B> {
        ShardReader { high_key, snapshot }
    }

    fn less<Q>(key: &Q, s: &ShardReader<K, V, B>) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match &s.high_key {
            Bound::Excluded(high_key) => key.lt(high_key.borrow()),
            Bound::Unbounded => true,
            _ => unreachable!(),
        }
    }
}

struct Iter<'a, K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    iter: Option<IndexIter<'a, K, V>>,
    iters: Vec<IndexIter<'a, K, V>>,
}

impl<'a, K, V> Iter<'a, K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    pub fn new(iters: Vec<IndexIter<'a, K, V>>) -> Iter<'a, K, V> {
        Iter { iter: None, iters }
    }
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.iter {
            Some(iter) => match iter.next() {
                Some(item) => Some(item),
                None => {
                    self.iter = None;
                    self.next()
                }
            },
            None if self.iters.len() == 0 => None,
            None => {
                self.iter = Some(self.iters.remove(0));
                self.iter.as_mut().unwrap().next()
            }
        }
    }
}
