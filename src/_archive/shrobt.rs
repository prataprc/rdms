//! Module `shrobt` implement an ordered set of index using Robt shards.

use log::{debug, error};
use toml;

use std::{
    borrow::Borrow,
    cmp,
    convert::{self, TryFrom, TryInto},
    ffi, fmt, fs,
    hash::Hash,
    io::{Read, Write},
    marker, mem,
    ops::{Bound, RangeBounds},
    path, result,
    sync::{Arc, Mutex, MutexGuard},
    thread,
};

use crate::{
    core::{self, Bloom, CommitIter, CommitIterator, Diff, DiskIndexFactory},
    core::{Cutoff, Validate},
    core::{Entry, Footprint, Index, IndexIter, Reader, Result, Serialize},
    error::Error,
    lsm,
    panic::Panic,
    robt::{self, Robt},
    scans, util,
};

#[derive(Clone)]
struct Root {
    num_shards: usize,
}

impl TryFrom<Root> for Vec<u8> {
    type Error = crate::error::Error;

    fn try_from(root: Root) -> Result<Vec<u8>> {
        use toml::Value;

        let text = {
            let mut dict = toml::map::Map::new();

            dict.insert(
                "num_shards".to_string(),
                Value::Integer(convert_at!(root.num_shards)?),
            );

            Value::Table(dict).to_string()
        };

        Ok(text.as_bytes().to_vec())
    }
}

impl TryFrom<Vec<u8>> for Root {
    type Error = crate::error::Error;

    fn try_from(bytes: Vec<u8>) -> Result<Root> {
        use std::str::from_utf8;

        let text = err_at!(InvalidFile, from_utf8(&bytes))?.to_string();

        let value = parse_at!(text, toml::Value)?;
        match value.as_table() {
            Some(table) => match table.get("num_shards") {
                Some(value) => match value.as_integer() {
                    Some(num_shards) => Ok(Root {
                        num_shards: convert_at!(num_shards)?,
                    }),
                    None => err_at!(InvalidFile, msg: format!("not integer")),
                },
                None => err_at!(InvalidFile, msg: format!("no num_shards")),
            },
            None => err_at!(InvalidFile, msg: format!("no table")),
        }
    }
}

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
        let rootp = path::Path::new(&name.0);
        let ext = match rootp.extension() {
            Some(ext) => match ext.to_str() {
                Some(ext) => Ok(ext),
                None => err_at!(InvalidFile, msg: format!("not str")),
            },
            None => err_at!(InvalidFile, msg: format!("no extension")),
        }?;
        let stem = match rootp.file_stem() {
            Some(stem) => match stem.to_str() {
                Some(stem) => Ok(stem.to_string()),
                None => err_at!(InvalidFile, msg: format!("not str")),
            },
            None => err_at!(InvalidFile, msg: format!("no stem")),
        }?;

        if ext == "root" {
            let parts: Vec<&str> = stem.split('-').collect();

            if parts.len() < 2 {
                err_at!(InvalidFile, msg: format!("not shrot root file"))
            } else if parts[parts.len() - 1] != "shrobt" {
                err_at!(InvalidFile, msg: format!("not shrot root file"))
            } else {
                Ok(parts[..(parts.len() - 1)].join("-"))
            }
        } else {
            err_at!(InvalidFile, msg: format!("not shrot root file"))
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
        let parts: Vec<&str> = name.0.split('-').collect();

        if parts.len() < 4 {
            err_at!(InvalidFile, msg: format!("not shrobt shard"))
        } else if parts[parts.len() - 2] != "shard" {
            err_at!(InvalidFile, msg: format!("shard not shrobt index"))
        } else if parts[parts.len() - 3] != "shrobt" {
            err_at!(InvalidFile, msg: format!("shard not shrobt index"))
        } else {
            let shard_i = parse_at!(parts[parts.len() - 1], usize)?;
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
/// * *mmap*, if true enable mmap for snapshots.
pub fn shrobt_factory<K, V, B>(
    config: robt::Config,
    num_shards: usize,
    mmap: bool,
) -> ShrobtFactory<K, V, B>
where
    K: 'static + Send + Clone + Ord + Serialize,
    V: 'static + Send + Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    ShrobtFactory {
        config,
        num_shards,
        mmap,

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
    K: 'static + Send + Clone + Ord + Serialize,
    V: 'static + Send + Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    config: robt::Config,
    num_shards: usize,
    mmap: bool,

    _phantom_key: marker::PhantomData<K>,
    _phantom_val: marker::PhantomData<V>,
    _phantom_bmap: marker::PhantomData<B>,
}

impl<K, V, B> DiskIndexFactory<K, V> for ShrobtFactory<K, V, B>
where
    K: 'static + Send + Default + Clone + Ord + Hash + Footprint + Serialize,
    V: 'static + Send + Default + Clone + Diff + Footprint + Serialize,
    <V as Diff>::D: Default + Serialize,
    B: 'static + Send + Sync + Bloom,
{
    type I = ShRobt<K, V, B>;

    fn to_type(&self) -> String {
        "shrobt".to_string()
    }

    fn new(&self, dir: &ffi::OsStr, name: &str) -> Result<ShRobt<K, V, B>> {
        ShRobt::new(dir, name, self.config.clone(), self.num_shards, self.mmap)
    }

    fn open(&self, dir: &ffi::OsStr, name: &str) -> Result<ShRobt<K, V, B>> {
        ShRobt::open(dir, name, self.mmap)
    }
}

/// Range partitioned index using [Robt] shards.
pub struct ShRobt<K, V, B>
where
    K: 'static + Send + Clone + Ord + Hash + Footprint + Serialize,
    V: 'static + Send + Clone + Diff + Footprint + Serialize,
    <V as Diff>::D: Serialize,
    B: 'static + Send + Bloom,
{
    dir: ffi::OsString,
    name: String,
    mmap: bool,

    seqno: u64,
    count: usize,
    metadata: Vec<u8>,
    build_time: u64,
    epoch: i128,

    shards: Arc<Mutex<Vec<Shard<K, V, B>>>>,
}

impl<K, V, B> Clone for ShRobt<K, V, B>
where
    K: 'static + Send + Default + Clone + Ord + Hash + Footprint + Serialize,
    V: 'static + Send + Default + Clone + Diff + Footprint + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
    B: 'static + Send + Bloom,
{
    fn clone(&self) -> Self {
        let shards: Vec<Shard<K, V, B>> = {
            let shards = self.as_shards().unwrap();
            shards.iter().map(|shard| shard.clone()).collect()
        };

        ShRobt {
            dir: self.dir.clone(),
            name: self.name.clone(),
            mmap: self.mmap.clone(),
            seqno: self.seqno.clone(),
            count: self.count.clone(),
            metadata: self.metadata.clone(),
            build_time: self.build_time.clone(),
            epoch: self.epoch.clone(),
            shards: Arc::new(Mutex::new(shards)),
        }
    }
}

/// Create and configure a range partitioned index.
impl<K, V, B> ShRobt<K, V, B>
where
    K: 'static + Send + Default + Clone + Ord + Hash + Footprint + Serialize,
    V: 'static + Send + Default + Clone + Diff + Footprint + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
    B: 'static + Send + Bloom,
{
    /// Create a new instance of range-partitioned index using Llrb tree.
    pub fn new(
        dir: &ffi::OsStr,
        name: &str,
        config: robt::Config,
        num_shards: usize,
        mmap: bool,
    ) -> Result<ShRobt<K, V, B>> {
        Self::new_root_file(dir, name, Root { num_shards })?;

        let mut shards = vec![];
        for shard_i in 0..num_shards {
            let name = {
                let name: ShardName = (name.to_string(), shard_i).into();
                name.0
            };
            let index = Robt::new(dir, &name, config.clone())?;
            shards.push(Shard::new_build(index));
        }

        debug!(target: "shrobt", "{:?}/{} new instance", dir, name);

        Ok(ShRobt {
            dir: dir.to_os_string(),
            name: name.to_string(),
            mmap,

            seqno: std::u64::MIN,
            count: std::usize::MIN,
            metadata: Default::default(),
            build_time: std::u64::MIN,
            epoch: std::i128::MAX,

            shards: Arc::new(Mutex::new(shards)),
        })
    }

    pub fn open(dir: &ffi::OsStr, name: &str, mmap: bool) -> Result<ShRobt<K, V, B>> {
        let root = Self::find_root_file(dir, name)?;

        let num_shards: usize = {
            let root = root.as_os_str();
            let root = Self::open_root_file(dir, root)?;
            if root.num_shards > 0 {
                Ok(root.num_shards)
            } else {
                err_at!(
                    InvalidFile,
                    msg: format!("unexpected num_shards:{}", root.num_shards)
                )
            }
        }?;

        let mut indexes = vec![];
        for shard_id in 0..num_shards {
            let sname: ShardName = (name.to_string(), shard_id).into();
            let sname = sname.to_string();
            indexes.push(Robt::open(&dir, &sname)?);
        }

        let (seqno, count, metadata, build_time, epoch) =
            // get metadata
            Self::get_metadata(&mut indexes)?;
        let shards = Arc::new(Mutex::new(robts_to_shards(indexes)?));

        debug!(target: "shrobt", "{:?}/{} open instance", dir, name);

        let index = ShRobt {
            dir: dir.to_os_string(),
            name: name.to_string(),
            mmap,

            seqno,
            count,
            metadata,
            build_time,
            epoch,

            shards,
        };
        Ok(index)
    }

    fn to_state(&self) -> Result<(String, usize)> {
        let shards = self.as_shards()?;
        let num_shards = shards.len();

        let is_build = shards.iter().all(|s| s.is_build());
        let is_snapshot = shards.iter().all(|s| s.is_snapshot());

        if is_build {
            Ok(("build".to_string(), num_shards))
        } else if is_snapshot {
            Ok(("snapshot".to_string(), num_shards))
        } else {
            err_at!(Fatal, msg: format!("mixed shard state"))
        }
    }

    fn get_metadata(
        //
        ris: &mut Vec<Robt<K, V, B>>,
    ) -> Result<(u64, usize, Vec<u8>, u64, i128)> {
        let mut seqno = std::u64::MIN;
        let mut count = std::usize::MIN;
        let mut metadata: Option<Vec<u8>> = None;
        let mut build_time = std::u64::MIN;
        let mut epoch = std::i128::MAX;

        for ri in ris.iter_mut() {
            let snap = ri.to_reader()?;
            let stats = snap.to_stats()?;

            seqno = cmp::max(seqno, ri.to_seqno()?);
            count += snap.len()?;
            metadata.get_or_insert(ri.to_metadata()?);
            build_time = cmp::max(build_time, stats.build_time);
            epoch = cmp::min(epoch, stats.epoch);

            assert_eq!(metadata.clone().unwrap(), ri.to_metadata()?);
        }
        let metadata = metadata.unwrap_or(vec![]);

        Ok((seqno, count, metadata, build_time, epoch))
    }

    fn new_root_file(dir: &ffi::OsStr, name: &str, root: Root) -> Result<ffi::OsString> {
        let root_file: ffi::OsString = {
            let rootf: RootFileName = name.to_string().into();
            let mut rootp = path::PathBuf::from(dir);
            rootp.push(&rootf.0);
            rootp.into_os_string()
        };

        let data: Vec<u8> = root.try_into()?;

        let mut fd = util::create_file_a(root_file.clone())?;
        write_file!(fd, &data, root_file.clone(), "shrobt-root-file")?;
        Ok(root_file.into())
    }

    fn open_root_file(dir: &ffi::OsStr, root: &ffi::OsStr) -> Result<Root> {
        let _: String = TryFrom::try_from(RootFileName(root.to_os_string()))?;
        let root_file = {
            let mut rootp = path::PathBuf::from(dir);
            rootp.push(root);
            rootp.into_os_string()
        };

        let mut fd = util::open_file_r(&root_file)?;
        let mut bytes = vec![];
        err_at!(IoError, fd.read_to_end(&mut bytes))?;

        Ok(bytes.try_into()?)
    }

    fn find_root_file(dir: &ffi::OsStr, name: &str) -> Result<ffi::OsString> {
        for item in err_at!(IoError, fs::read_dir(dir))? {
            match item {
                Ok(item) => {
                    let root_file = RootFileName(item.file_name());
                    let nm: Result<String> = root_file.clone().try_into();
                    match nm {
                        Ok(nm) if nm == name => return Ok(root_file.into()),
                        _ => continue,
                    }
                }
                _ => continue,
            }
        }

        err_at!(InvalidFile, msg: format!("missing root file"))
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
        shards.push(Shard::new_snapshot(high_key.clone(), index));
        match snapshot.first() {
            Ok(entry) => high_key = Bound::Excluded(entry.to_key()),
            Err(Error::EmptyIndex) => (),
            Err(err) => return Err(err),
        }
    }

    shards.reverse();

    Ok(shards)
}

/// Maintenance API.
impl<K, V, B> ShRobt<K, V, B>
where
    K: 'static + Send + Default + Clone + Ord + Hash + Footprint + Serialize,
    V: 'static + Send + Default + Clone + Diff + Footprint + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
    B: 'static + Send + Bloom,
{
    #[inline]
    pub fn len(&self) -> Result<usize> {
        Ok(self.count)
    }

    fn to_num_shards(&self) -> Result<usize> {
        Ok(self.as_shards()?.len())
    }

    fn to_shard_files(&self) -> Result<Vec<Vec<ffi::OsString>>> {
        let mut shards = self.as_shards()?;
        let mut purge_files = vec![];
        for shard in shards.iter_mut() {
            let xs = shard.as_mut_robt().to_next_version()?;
            purge_files.push(xs);
        }
        Ok(purge_files)
    }

    fn as_shards(&self) -> Result<MutexGuard<Vec<Shard<K, V, B>>>> {
        match self.shards.lock() {
            Ok(value) => Ok(value),
            Err(err) => err_at!(Fatal, msg: format!("poisened lock {}", err)),
        }
    }

    #[inline]
    pub fn to_name(&self) -> String {
        self.name.clone()
    }

    pub fn to_stats(&mut self) -> Result<robt::Stats> {
        let mut shards = self.as_shards()?;

        let mut stats: robt::Stats = Default::default();
        for shard in shards.iter_mut() {
            stats = stats.merge(shard.to_snapshot()?.to_stats()?);
        }
        assert_eq!(stats.seqno, self.seqno);
        stats.name = self.name.clone();

        Ok(stats)
    }

    fn to_ranges(&self) -> Result<Vec<(Bound<K>, Bound<K>)>> {
        let mut shards = self.as_shards()?;

        let mut high_keys: Vec<Bound<K>> = vec![];
        let iter = shards.iter_mut().filter_map(|shard| shard.to_high_key());
        for high_key in iter {
            match high_key {
                Bound::Unbounded => break,
                Bound::Excluded(_) => high_keys.push(high_key),
                Bound::Included(_) => err_at!(Fatal, msg: format!("unreachable"))?,
            }
        }

        assert!(
            high_keys.len() < shards.len(),
            "{}/{}",
            high_keys.len(),
            shards.len()
        );

        high_keys.push(Bound::Unbounded);
        Ok(util::high_keys_to_ranges(high_keys))
    }

    fn to_footprints(&self) -> Result<Vec<isize>> {
        let mut shards = self.as_shards()?;

        let mut footprints = vec![];
        for shard in shards.iter_mut() {
            footprints.push(shard.as_robt().footprint()?);
        }

        Ok(footprints)
    }

    fn to_partitions(&self) -> Result<Vec<(isize, Bound<K>, Bound<K>)>> {
        let mut shards = self.as_shards()?;

        let mut partitions: Vec<(isize, Bound<K>, Bound<K>)> = vec![];
        for (_i, shard) in shards.iter_mut().enumerate() {
            let robt_index = shard.as_mut_robt();

            let mut ps: Vec<(isize, Bound<K>, Bound<K>)> = {
                let ps = robt_index.to_partitions()?;
                // println!("shard {} partitions {}", _i, ps.len());
                let q = if ps.len() == 0 {
                    0
                } else {
                    robt_index.footprint()? / (ps.len() as isize)
                };
                ps.into_iter().map(|(lk, hk)| (q, lk, hk)).collect()
            };

            let stitch_item = {
                ps.reverse();
                let item = match (partitions.pop(), ps.pop()) {
                    (None, Some(item)) => Some(item),
                    (Some((zf, zl, zh)), Some((af, al, ah))) => {
                        assert!(zh == Bound::Unbounded);
                        assert!(al == Bound::Unbounded);
                        Some((zf + af, zl, ah))
                    }
                    (Some(item), None) => Some(item),
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

    fn rebalance(&self) -> Result<Option<Vec<Option<(Bound<K>, Bound<K>)>>>> {
        let footprints = self.to_footprints()?;
        let num_shards = footprints.len();

        let footprint: usize = convert_at!(self.footprint()?)?;
        let avg = footprint / num_shards;

        let do_rebalance = footprints
            .into_iter()
            .map(|footprint| (footprint as f64) / (avg as f64))
            .any(|ratio| (ratio < 0.5) || (ratio > 1.5)); // TODO: no magic

        let mut partitions = self.to_partitions()?;
        debug!(
            target: "shrobt",
            "rebalance partitions {} {} {}",
            do_rebalance, partitions.len(), num_shards
        );

        let mut ranges = match (do_rebalance, partitions.len()) {
            (false, _) | (true, 0) => return Ok(None),
            (true, 1) => {
                let item = partitions.remove(0);
                vec![Some((item.1, item.2))]
            }
            (true, n) if n < (num_shards * 2) => {
                let (f, l) = (partitions.remove(0), partitions.remove(n - 2));
                vec![Some((f.1, l.2))]
            }
            (true, _) => {
                let mut ranges: Vec<Option<(Bound<K>, Bound<K>)>> = vec![];
                for rs in util::as_sharded_array(&partitions, num_shards).into_iter() {
                    match (rs.first(), rs.last()) {
                        (Some((_, l, _)), Some((_, _, h))) => {
                            let (l, h) = (l.clone(), h.clone());
                            ranges.push(Some((l, h)))
                        }
                        _ => err_at!(Fatal, msg: format!("unreachable"))?,
                    }
                }
                ranges
            }
        };

        // If there are not enough shards push empty iterators.
        (ranges.len()..num_shards).for_each(|_| ranges.push(None));
        assert_eq!(ranges.len(), num_shards);

        Ok(Some(ranges))
    }

    fn to_range_scans<'a>(
        &mut self,
        re_ranges: Vec<Option<(Bound<K>, Bound<K>)>>,
    ) -> Result<Vec<IndexIter<'a, K, V>>> {
        let mut shards = self.as_shards()?;

        let mut outer_iters = vec![];
        for re_range in re_ranges.into_iter() {
            let mut iters = vec![];
            for shard in shards.iter_mut() {
                let snap = shard.as_mut_robt().to_reader()?;
                let iter: IndexIter<K, V> = match re_range.clone() {
                    Some(re_range) => {
                        let iter = snap.into_range_scan(re_range)?;
                        Box::new(iter)
                    }
                    None => Box::new((vec![]).into_iter()),
                };
                iters.push(iter);
            }
            outer_iters.push(Box::new(Iter::new(iters)) as IndexIter<K, V>)
        }

        Ok(outer_iters)
    }

    fn transform_metadatas<F>(&self, metacb: F, state: &str) -> Result<Vec<Vec<u8>>>
    where
        F: Fn(Vec<u8>) -> Vec<u8>,
    {
        let shards = self.as_shards()?;

        let mut metas = vec![];
        for shard in shards.iter() {
            metas.push(match state {
                "build" => metacb(vec![]),
                "snapshot" => {
                    let meta = shard.as_robt().to_metadata()?;
                    metacb(meta)
                }
                _ => err_at!(Fatal, msg: format!("unreachable"))?,
            });
        }

        Ok(metas)
    }
}

impl<K, V, B> Index<K, V> for ShRobt<K, V, B>
where
    K: 'static + Send + Default + Clone + Ord + Hash + Footprint + Serialize,
    V: 'static + Send + Default + Clone + Diff + Footprint + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
    B: 'static + Send + Sync + Bloom,
{
    type R = ShrobtReader<K, V, B>;
    type W = Panic;

    #[inline]
    fn to_name(&self) -> Result<String> {
        Ok(self.name.clone())
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
        let mut shards = self.as_shards()?;

        let mut readers = vec![];
        for shard in shards.iter_mut().rev() {
            let high_key = match shard.to_high_key() {
                Some(high_key) => high_key,
                None => err_at!(UnInitialized, msg: format!("shrobt.to_reader()"))?,
            };
            let mut snapshot = shard.as_mut_robt().to_reader()?;
            snapshot.set_mmap(self.mmap)?;
            readers.push(ShardReader::new(high_key, snapshot));
        }

        readers.reverse();
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
        let (state, num_shards) = self.to_state()?;
        let re_ranges = match state.as_str() {
            "build" => None,
            "snapshot" => self.rebalance()?,
            _ => err_at!(Fatal, msg: format!("unreachable"))?,
        };
        let re_iters = match re_ranges.clone() {
            None => None,
            Some(re_ranges) => Some(self.to_range_scans(re_ranges)?),
        };
        let metas = self.transform_metadatas(metacb, state.as_str())?;

        let (iters, pfs, r) = match (state.as_str(), re_ranges, re_iters) {
            ("build", _, _) => {
                debug!(
                    target: "shrobt", "{:?}/{}, initial commit",
                    self.dir, self.name
                );
                // println!("{}, initial shrobt-commit", self.name);

                let iters = scanner.scans(self.to_num_shards()?)?;
                (iters, vec![], None)
            }
            ("snapshot", Some(re_ranges), Some(re_iters)) => {
                debug!(
                    target: "shrobt",
                    "{:?}/{}, commit with rebalance {}",
                    self.dir, self.name, re_ranges.len()
                );

                let purge_files = self.to_shard_files()?;

                let commit_iters = {
                    let ranges: Vec<(Bound<K>, Bound<K>)> = re_ranges
                        .clone()
                        .into_iter()
                        .filter_map(convert::identity)
                        .collect();
                    let mut commit_iters = scanner.range_scans(ranges.clone())?;
                    for _ in commit_iters.len()..re_ranges.len() {
                        let ss = vec![];
                        commit_iters.push(Box::new(ss.into_iter()));
                    }
                    commit_iters
                };

                assert_eq!(commit_iters.len(), re_iters.len());

                let reverse = false;
                let iters = commit_iters
                    .into_iter()
                    .zip(re_iters.into_iter())
                    .into_iter()
                    .map(|(i1, i2)| lsm::y_iter_versions(i1, i2, reverse))
                    .collect();

                (iters, purge_files, None)
            }
            ("snapshot", None, _) => {
                debug!(
                    target: "shrobt", "{:?}/{}, commit without rebalance",
                    self.dir, self.name
                );
                // println!("{}, shrobt-commit without rebalance", self.name);

                let r = self.to_reader()?;

                let iters = {
                    let ranges = self.to_ranges()?;
                    let mut iters = scanner.range_scans(ranges)?;
                    (iters.len()..num_shards)
                        .for_each(|_| iters.push(Box::new(vec![].into_iter())));
                    iters
                };
                (iters, vec![], Some(r))
            }
            _ => err_at!(Fatal, msg: format!("unreachable"))?,
        };

        let mut shards = match self.shards.lock() {
            Ok(value) => Ok(value),
            Err(err) => err_at!(Fatal, msg: format!("poisened lock {}", err)),
        }?;

        if shards.len() != iters.len() {
            let msg = format!("{}/{} iters/shards", iters.len(), shards.len());
            err_at!(Fatal, msg: msg)?
        }

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
            let iter: Box<ffi::c_void> = unsafe {
                let iter = scans::CommitWrapper::new(vec![iter]);
                let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
                let iter = Box::leak(Box::new(CommitIter::new(iter, within)));
                let iter = iter as *mut CommitIter<K, V, _>;
                Box::from_raw(iter as *mut ffi::c_void)
            };
            threads.push(thread::spawn(move || {
                //
                thread_commit(off, index, iter, meta)
            }));
        }

        // gather
        let mut indexes = vec![];
        let mut errs = vec![];
        for t in threads.into_iter() {
            match t.join().unwrap() {
                Ok((off, index)) => indexes.push((off, index)),
                Err(err) => {
                    error!(target: "shrobt", "commit: {:?}", err);
                    errs.push(err);
                }
            }
        }

        let mut indexes: Vec<Robt<K, V, B>> = if errs.len() == 0 {
            indexes.sort_by(|x, y| x.0.cmp(&y.0));
            Ok(indexes.into_iter().map(|x| x.1).collect())
        } else {
            let msg = errs
                .into_iter()
                .map(|e| format!("commit-err:{:?}", e))
                .collect::<Vec<String>>()
                .join("; ");
            err_at!(Fatal, msg: msg)
        }?;

        {
            let (seqno, count, metadata, build_time, epoch) =
                // get metadata
                Self::get_metadata(&mut indexes)?;
            self.seqno = seqno;
            self.count = count;
            self.metadata = metadata;
            self.build_time = build_time;
            self.epoch = epoch;
        }

        // now finally clean up the purged files due to rebalance.
        for (index, files) in indexes.iter_mut().zip(pfs.into_iter()) {
            index.purge_files(files)?;
        }

        robts_to_shards(indexes)?
            .drain(..)
            .for_each(|shard| shards.push(shard));

        // In one scenario it is important to hold on to a reader snapshot,
        // of older version. This is to make sure that older snapshot is
        // purged only after new version for all shards are persisted. So
        // that recovery is possible.
        mem::drop(r);

        Ok(())
    }

    fn compact(&mut self, cutoff: Cutoff) -> Result<usize> {
        // let (state, _num_shards) = self.to_state()?;
        // let metas = self.transform_metadatas(metacb, state.as_str())?;

        let r = {
            let (state, _) = self.to_state()?;
            match state.as_str() {
                "build" => None,
                "snapshot" => Some(self.to_reader()?),
                _ => err_at!(Fatal, msg: format!("unreachable"))?,
            };
        };

        let mut shards = self.as_shards()?;

        let indexes: Vec<Robt<K, V, B>> = {
            let iter = shards.drain(..).map(|shard| shard.into_robt());
            iter.collect()
        };

        // scatter
        let mut threads = vec![];
        for (off, index) in indexes.into_iter().enumerate() {
            threads.push(thread::spawn(move || {
                thread_compact(off, index, cutoff.clone())
            }));
        }

        // gather
        let (mut indexes, mut errs, mut count) = (vec![], vec![], 0);
        for t in threads.into_iter() {
            match t.join().unwrap() {
                Ok((off, cnt, index)) => {
                    count += cnt;
                    indexes.push((off, index));
                }
                Err(err) => {
                    error!(target: "shrobt", "compact: {:?}", err);
                    errs.push(err);
                }
            }
        }

        let indexes: Vec<Robt<K, V, B>> = if errs.len() == 0 {
            indexes.sort_by(|x, y| x.0.cmp(&y.0));
            Ok(indexes.into_iter().map(|x| x.1).collect())
        } else {
            let msg = errs
                .into_iter()
                .map(|e| format!("compact-err:{:?}", e))
                .collect::<Vec<String>>()
                .join("; ");
            err_at!(Fatal, msg: msg)
        }?;

        robts_to_shards(indexes)?
            .drain(..)
            .for_each(|shard| shards.push(shard));

        // In one scenario it is important to hold on to a reader snapshot,
        // of older version. This is to make sure that older snapshot is
        // purged only after new version for all shards are persisted. So
        // that recovery is possible.
        mem::drop(r);

        debug!(
            target: "shrobt", "{:?}/{}, compact items {}",
            self.dir, self.name, count
        );

        Ok(count)
    }

    fn close(self) -> Result<()> {
        let mut shards = self.as_shards()?;

        for shard in shards.drain(..) {
            shard.into_robt().close()?
        }

        Ok(())
    }

    fn purge(self) -> Result<()> {
        let mut shards = self.as_shards()?;

        for shard in shards.drain(..) {
            shard.into_robt().purge()?
        }

        Ok(())
    }
}

fn thread_commit<K, V, B>(
    off: usize,
    mut index: Robt<K, V, B>,
    iter: Box<ffi::c_void>,
    meta: Vec<u8>,
) -> Result<(usize, Robt<K, V, B>)>
where
    K: 'static + Send + Default + Clone + Ord + Hash + Footprint + Serialize,
    V: 'static + Send + Default + Clone + Diff + Footprint + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
    B: 'static + Send + Bloom,
{
    let iter = unsafe {
        Box::from_raw(Box::leak(iter) as *mut ffi::c_void
            as *mut CommitIter<K, V, scans::CommitWrapper<'static, K, V>>)
    };
    index.commit(*iter, |_| meta.clone())?;
    Ok((off, index))
}

fn thread_compact<K, V, B>(
    off: usize,
    mut index: Robt<K, V, B>,
    cutoff: Cutoff,
) -> Result<(usize, usize, Robt<K, V, B>)>
where
    K: 'static + Send + Default + Clone + Ord + Hash + Footprint + Serialize,
    V: 'static + Send + Default + Clone + Diff + Footprint + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
    B: 'static + Send + Bloom,
{
    let count = index.compact(cutoff)?;
    Ok((off, count, index))
}

impl<K, V, B> Footprint for ShRobt<K, V, B>
where
    K: 'static + Send + Default + Clone + Ord + Hash + Footprint + Serialize,
    V: 'static + Send + Default + Clone + Diff + Footprint + Serialize,
    <V as Diff>::D: Default + Clone + Serialize + Footprint,
    B: 'static + Send + Bloom,
{
    fn footprint(&self) -> Result<isize> {
        let shards = self.as_shards()?;

        let mut footprint = 0;
        for shard in shards.iter() {
            footprint += shard.as_robt().footprint()?
        }
        Ok(footprint)
    }
}

impl<K, V, B> CommitIterator<K, V> for ShRobt<K, V, B>
where
    K: 'static + Send + Default + Clone + Ord + Hash + Footprint + Serialize,
    V: 'static + Send + Default + Clone + Diff + Footprint + Serialize,
    <V as Diff>::D: Default + Clone + Serialize + Footprint,
    B: 'static + Send + Bloom,
{
    fn scan<G>(&mut self, within: G) -> Result<IndexIter<K, V>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let mut shards = self.as_shards()?;

        let mut iters = vec![];
        for shard in shards.iter_mut() {
            let snap = shard.as_mut_robt().to_reader()?;
            iters.push(snap.into_scan()?);
        }
        iters.reverse();

        Ok(Box::new(scans::FilterScans::new(iters, within)))
    }

    fn scans<G>(&mut self, n_shards: usize, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let mut shards = self.as_shards()?;

        let mut iters = vec![];
        for shard in shards.iter_mut() {
            let snap = shard.as_mut_robt().to_reader()?;
            let iter = {
                let iter = snap.into_scan()?;
                Box::new(scans::FilterScans::new(vec![iter], within.clone()))
            };
            iters.push(iter as IndexIter<K, V>)
        }

        // If there are not enough shards push empty iterators.
        for _ in iters.len()..n_shards {
            let ss = vec![];
            iters.push(Box::new(ss.into_iter()));
        }

        assert_eq!(iters.len(), n_shards);

        Ok(iters)
    }

    fn range_scans<N, G>(&mut self, ranges: Vec<N>, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        N: Clone + RangeBounds<K>,
        G: Clone + RangeBounds<u64>,
    {
        let mut shards = self.as_shards()?;

        let mut iters = vec![];
        for range in ranges.into_iter() {
            let mut scans = vec![];
            for shard in shards.iter_mut() {
                let snap = shard.as_mut_robt().to_reader()?;
                let range = util::to_start_end(range.clone());
                scans.push(snap.into_range_scan(range)?)
            }
            scans.reverse();

            let iter = scans::FilterScans::new(scans, within.clone());
            iters.push(Box::new(iter) as IndexIter<K, V>);
        }

        Ok(iters)
    }
}

impl<K, V, B> Validate<robt::Stats> for ShRobt<K, V, B>
where
    K: 'static + Send + Default + Clone + Ord + fmt::Debug + Hash + Footprint + Serialize,
    V: 'static + Send + Default + Clone + Diff + Footprint + Serialize,
    <V as Diff>::D: Default + Clone + Serialize,
    B: 'static + Send + Bloom,
{
    fn validate(&mut self) -> Result<robt::Stats> {
        let mut shards = self.as_shards()?;

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
    _name: String,
    readers: Vec<ShardReader<K, V, B>>,
}

impl<K, V, B> ShrobtReader<K, V, B>
where
    K: Ord + Clone + Serialize,
    V: Clone + Diff + Serialize,
{
    fn new(_name: String, readers: Vec<ShardReader<K, V, B>>) -> Result<ShrobtReader<K, V, B>> {
        Ok(ShrobtReader { _name, readers })
    }

    fn find<'a, Q>(
        key: &Q,
        rs: &'a mut [ShardReader<K, V, B>],
    ) -> Result<(usize, &'a mut ShardReader<K, V, B>)>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match rs.len() {
            0 => err_at!(Fatal, msg: format!("unreachable")),
            1 => Ok((0, &mut rs[0])),
            2 => {
                if ShardReader::less(key, &rs[0]) {
                    Ok((0, &mut rs[0]))
                } else {
                    Ok((1, &mut rs[1]))
                }
            }
            n => {
                let pivot = n / 2;
                if ShardReader::less(key, &rs[pivot]) {
                    Self::find(key, &mut rs[..pivot + 1])
                } else {
                    let (off, sr) = Self::find(key, &mut rs[pivot + 1..])?;
                    Ok((pivot + 1 + off, sr))
                }
            }
        }
    }

    /// Return the first entry in index, with only latest value.
    pub fn first(&mut self) -> Result<Entry<K, V>>
    where
        K: Default,
        V: Default,
        <V as Diff>::D: Default + Serialize,
    {
        match self.readers.first_mut() {
            Some(reader) => reader.snapshot.first(),
            None => Err(Error::EmptyIndex),
        }
    }

    /// Return the first entry in index, with all versions.
    pub fn first_with_versions(&mut self) -> Result<Entry<K, V>>
    where
        K: Default,
        V: Default,
        <V as Diff>::D: Default + Serialize,
    {
        match self.readers.first_mut() {
            Some(reader) => reader.snapshot.first_with_versions(),
            None => Err(Error::EmptyIndex),
        }
    }

    /// Return the last entry in index, with only latest value.
    pub fn last(&mut self) -> Result<Entry<K, V>>
    where
        K: Default,
        V: Default,
        <V as Diff>::D: Default + Serialize,
    {
        match self.readers.last_mut() {
            Some(reader) => reader.snapshot.last(),
            None => Err(Error::EmptyIndex),
        }
    }

    /// Return the last entry in index, with all versions.
    pub fn last_with_versions(&mut self) -> Result<Entry<K, V>>
    where
        K: Default,
        V: Default,
        <V as Diff>::D: Default + Serialize,
    {
        match self.readers.last_mut() {
            Some(reader) => reader.snapshot.last_with_versions(),
            None => Err(Error::EmptyIndex),
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
        let (_, reader) = Self::find(key, self.readers.as_mut_slice())?;
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
                Self::find(lr, self.readers.as_mut_slice())?.0
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
                _ => err_at!(Fatal, msg: format!("unreachable"))?,
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
                Self::find(lr, self.readers.as_mut_slice())?.0
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
                _ => err_at!(Fatal, msg: format!("unreachable"))?,
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
        let (_, reader) = Self::find(key, self.readers.as_mut_slice())?;
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
                Self::find(lr, self.readers.as_mut_slice())?.0
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
                _ => err_at!(Fatal, msg: format!("unreachable"))?,
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
                Self::find(lr, self.readers.as_mut_slice())?.0
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
                _ => err_at!(Fatal, msg: format!("unreachable"))?,
            };
            if !ok {
                break;
            }
        }

        iters.reverse();
        Ok(Box::new(Iter::new(iters)))
    }
}

impl<K, V, B> CommitIterator<K, V> for ShrobtReader<K, V, B>
where
    K: 'static + Send + Default + Clone + Ord + Hash + Footprint + Serialize,
    V: 'static + Send + Default + Clone + Diff + Footprint + Serialize,
    <V as Diff>::D: Default + Clone + Serialize + Footprint,
    B: 'static + Send + Bloom,
{
    fn scan<G>(&mut self, within: G) -> Result<IndexIter<K, V>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let mut iters = vec![];
        for shard_r in self.readers.iter_mut() {
            let snap = robt::Snapshot::<K, V, B>::open(
                //
                &shard_r.snapshot.dir,
                &shard_r.snapshot.name,
            )?;
            iters.push(snap.into_scan()?);
        }
        iters.reverse();
        Ok(Box::new(scans::FilterScans::new(iters, within)))
    }

    fn scans<G>(&mut self, n_shards: usize, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        G: Clone + RangeBounds<u64>,
    {
        let mut iters = vec![];
        for shard_r in self.readers.iter_mut() {
            let iter = {
                let snap = robt::Snapshot::<K, V, B>::open(
                    //
                    &shard_r.snapshot.dir,
                    &shard_r.snapshot.name,
                )?;
                let iter = snap.into_scan()?;
                Box::new(scans::FilterScans::new(vec![iter], within.clone()))
            };
            iters.push(iter as IndexIter<K, V>)
        }

        // If there are not enough shards push empty iterators.
        for _ in iters.len()..n_shards {
            let ss = vec![];
            iters.push(Box::new(ss.into_iter()));
        }

        assert_eq!(iters.len(), n_shards);

        Ok(iters)
    }

    fn range_scans<N, G>(&mut self, ranges: Vec<N>, within: G) -> Result<Vec<IndexIter<K, V>>>
    where
        N: Clone + RangeBounds<K>,
        G: Clone + RangeBounds<u64>,
    {
        let mut iters = vec![];
        for range in ranges.into_iter() {
            let mut scans = vec![];
            for shard_r in self.readers.iter_mut() {
                let snap = robt::Snapshot::<K, V, B>::open(
                    //
                    &shard_r.snapshot.dir,
                    &shard_r.snapshot.name,
                )?;
                let range = util::to_start_end(range.clone());
                scans.push(snap.into_range_scan(range)?)
            }
            scans.reverse();

            let iter = scans::FilterScans::new(scans, within.clone());
            iters.push(Box::new(iter) as IndexIter<K, V>);
        }

        Ok(iters)
    }
}

enum Shard<K, V, B>
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

impl<K, V, B> Clone for Shard<K, V, B>
where
    K: Clone + Ord + Hash + Footprint + Serialize,
    V: Clone + Diff + Footprint + Serialize,
    <V as Diff>::D: Serialize,
    B: Bloom,
{
    fn clone(&self) -> Self {
        match self {
            Shard::Build { inner } => Shard::Build {
                inner: inner.to_clone().unwrap(),
            },
            Shard::Snapshot { high_key, inner } => Shard::Snapshot {
                high_key: high_key.clone(),
                inner: inner.to_clone().unwrap(),
            },
        }
    }
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
            Shard::Build { .. } => err_at!(Fatal, msg: format!("unreachable")),
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
        loop {
            match &mut self.iter {
                Some(iter) => match iter.next() {
                    Some(item) => break Some(item),
                    None => self.iter = None,
                },
                None if self.iters.len() == 0 => break None,
                None => self.iter = Some(self.iters.remove(0)),
            }
        }
    }
}

#[cfg(test)]
#[path = "shrobt_test.rs"]
mod shrobt_test;
