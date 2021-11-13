use cbordata::{Cborize, FromCbor, IntoCbor};

use std::{
    borrow::Borrow,
    cell::RefCell,
    cmp,
    convert::{TryFrom, TryInto},
    ffi, fmt, fs,
    hash::Hash,
    io::{self, Read, Seek},
    marker, mem,
    ops::{Bound, RangeBounds},
    path,
    rc::Rc,
    sync::Arc,
};

use crate::{
    dbs, read_file,
    robt::{
        build,
        lsm::YIter,
        reader::{Iter, IterLsm, Reader},
        scans::{BitmappedScan, BuildScan, CompactScan},
        to_index_location, to_vlog_location, Config, Entry, Flusher, IndexFileName,
        Stats, VlogFileName, ROOT_MARKER,
    },
    util, Error, Result,
};

/// Marker block size, not to be tampered with.
const MARKER_BLOCK_SIZE: usize = 1024 * 4;

/// Maximum supported depth of the tree.
const MAX_DEPTH: usize = 11;

/// Build an immutable read-only btree index from an iterator.
///
/// Refer to package documentation for typical work-flow.
pub struct Builder<K, V>
where
    K: IntoCbor,
    V: dbs::Diff + IntoCbor,
    <V as dbs::Diff>::Delta: IntoCbor,
{
    // configuration
    config: Config,
    // active values
    iflush: Rc<RefCell<Flusher>>,
    vflush: Rc<RefCell<Flusher>>,
    // final result to be persisted
    app_meta: Vec<u8>,
    stats: Stats,
    root: Option<u64>,

    _key: marker::PhantomData<K>,
    _val: marker::PhantomData<V>,
}

impl<K, V> Builder<K, V>
where
    K: IntoCbor,
    V: dbs::Diff + IntoCbor,
    <V as dbs::Diff>::Delta: IntoCbor,
{
    /// Build a fresh index, using configuration and snapshot specific meta-data.
    ///
    /// Subsequently call [Builder::build_index] to start building the index.
    pub fn initial(mut config: Config, meta: Vec<u8>) -> Result<Self> {
        // set to fresh vlog location, don't carry forward.
        config.set_vlog_location(None);

        let queue_size = config.flush_queue_size;
        let iflush = {
            let loc = to_index_location(&config.dir, &config.name);
            Rc::new(RefCell::new(Flusher::new(&loc, true, queue_size)?))
        };
        let vflush = if config.value_in_vlog || config.delta_ok {
            let loc = to_vlog_location(&config.dir, &config.name);
            Rc::new(RefCell::new(Flusher::new(&loc, true, queue_size)?))
        } else {
            Rc::new(RefCell::new(Flusher::empty()))
        };

        let mut stats: Stats = config.clone().into();
        stats.vlog_location = vflush.as_ref().borrow().to_location();

        let val = Builder {
            config,
            iflush,
            vflush,

            app_meta: meta,
            stats,
            root: None,

            _key: marker::PhantomData,
            _val: marker::PhantomData,
        };

        Ok(val)
    }

    /// Build an incremental index on top of an existing index. Note
    /// that the entire btree along with root-node, intermediate-nodes
    /// and leaf-nodes shall be built fresh from the iterator, but entries
    /// form the iterator can hold reference, as `{fpos, length}` to values
    /// and deltas within a value-log file. Instead of creating a fresh
    /// value-log file, incremental build will serialize values and deltas
    /// into supplied `vlog` file in append only fashion.
    ///
    /// Subsequently call [Builder::build_index] to start building the index.
    fn incremental(config: Config, meta: Vec<u8>) -> Result<Self> {
        let queue_size = config.flush_queue_size;
        let iflush = {
            let loc = to_index_location(&config.dir, &config.name);
            Rc::new(RefCell::new(Flusher::new(&loc, true, queue_size)?))
        };
        let vflush = match config.to_vlog_location() {
            Some(vlog) => Rc::new(RefCell::new(Flusher::new(&vlog, true, queue_size)?)),
            None => Rc::new(RefCell::new(Flusher::empty())),
        };

        let mut stats: Stats = config.clone().into();
        stats.vlog_location = vflush.as_ref().borrow().to_location();

        let val = Builder {
            config,
            iflush,
            vflush,

            app_meta: meta,
            stats,
            root: None,

            _key: marker::PhantomData,
            _val: marker::PhantomData,
        };

        Ok(val)
    }
}

impl<K, V> Builder<K, V>
where
    K: Clone + Hash + IntoCbor + FromCbor,
    V: dbs::Diff + IntoCbor + FromCbor,
    <V as dbs::Diff>::Delta: IntoCbor + FromCbor,
{
    pub fn build_index<B, I, E>(
        &mut self,
        iter: I,
        bitmap: B,
        seqno: Option<u64>,
    ) -> Result<Index<K, V, B>>
    where
        B: dbs::Bloom,
        I: Iterator<Item = Result<E>>,
        E: TryInto<Entry<K, V>>,
        <E as TryInto<Entry<K, V>>>::Error: fmt::Display,
    {
        let build_iter = BuildScan::new(iter, 0 /*seqno*/);
        let bitmap_iter = BitmappedScan::<K, V, B, _>::new(build_iter, bitmap);

        self.stats.n_abytes = self.vflush.as_ref().borrow().to_fpos().unwrap_or(0);

        let (bitmap_iter, root) = self.build_tree(bitmap_iter)?;
        let (bitmap, build_iter) = bitmap_iter.unwrap()?;

        let (build_time, build_seqno, n_count, n_deleted, epoch, _iter) =
            build_iter.unwrap()?;

        self.root = root;
        self.stats.build_time = build_time;
        self.stats.seqno = seqno
            .map(|seqno| cmp::max(seqno, build_seqno))
            .unwrap_or(build_seqno);
        self.stats.n_count = n_count;
        self.stats.n_deleted = n_deleted.try_into().unwrap();
        self.stats.epoch = epoch;

        self.build_flush(err_at!(Fatal, bitmap.to_bytes())?)?;

        Index::open(&self.config.dir, &self.config.name)
    }
}

impl<K, V> Builder<K, V>
where
    K: Clone + IntoCbor,
    V: dbs::Diff + IntoCbor,
    <V as dbs::Diff>::Delta: IntoCbor,
{
    fn build_tree<I>(&self, iter: I) -> Result<(I, Option<u64>)>
    where
        I: Iterator<Item = Result<Entry<K, V>>>,
    {
        let iter = Rc::new(RefCell::new(iter));

        let zz = build::BuildZZ::new(
            &self.config,
            Rc::clone(&self.iflush),
            Rc::clone(&self.vflush),
            Rc::clone(&iter),
        );
        let mz = build::BuildMZ::new(&self.config, Rc::clone(&self.iflush), zz);
        let mut build = (0..MAX_DEPTH).fold(build::BuildIter::from(mz), |build, _| {
            build::BuildMM::new(&self.config, Rc::clone(&self.iflush), build).into()
        });

        let root = match build.next() {
            Some(Ok((_, root))) => Some(root),
            Some(Err(err)) => return Err(err),
            None => None,
        };
        mem::drop(build);

        Ok((Rc::try_unwrap(iter).ok().unwrap().into_inner(), root))
    }

    fn build_flush(&mut self, bitmap: Vec<u8>) -> Result<(u64, u64)> {
        let block = self.meta_blocks(bitmap)?;

        self.iflush.borrow_mut().flush(block)?;

        let len1 = self.iflush.borrow_mut().close()?;
        let len2 = self.vflush.borrow_mut().close()?;

        Ok((len1, len2))
    }

    fn meta_blocks(&mut self, bitmap: Vec<u8>) -> Result<Vec<u8>> {
        let stats = util::into_cbor_bytes(self.stats.clone())?;

        let metas = vec![
            MetaItem::AppMetadata(self.app_meta.clone()),
            MetaItem::Stats(stats),
            MetaItem::Bitmap(bitmap),
            MetaItem::Root(self.root),
            MetaItem::Marker(ROOT_MARKER.clone()),
        ];

        let mut block = util::into_cbor_bytes(metas)?;
        let len = err_at!(Fatal, u64::try_from(block.len()))?;
        let m = Self::compute_root_block(block.len() + 16);
        block.resize(m, 0);
        let off = err_at!(Fatal, u64::try_from(m))?;

        // 8-byte length-prefixed-message, message is the meta-block.
        block[m - 16..m - 8].copy_from_slice(&off.to_be_bytes());
        block[m - 8..m].copy_from_slice(&len.to_be_bytes());

        Ok(block)
    }

    fn compute_root_block(n: usize) -> usize {
        match n % MARKER_BLOCK_SIZE {
            0 => n,
            _ => ((n / MARKER_BLOCK_SIZE) + 1) * MARKER_BLOCK_SIZE,
        }
    }
}

/// Enumeration of meta items stored in [Robt] index.
///
/// [Robt] index is a fully packed immutable [Btree] index. To interpret
/// the index a list of meta items are appended to the tip of index-file.
///
/// [Btree]: https://en.wikipedia.org/wiki/B-tree
#[derive(Clone, Debug, Cborize)]
pub enum MetaItem {
    /// Application supplied metadata, typically serialized and opaque to `robt`.
    AppMetadata(Vec<u8>),
    /// Contains index-statistics along with configuration values.
    Stats(Vec<u8>),
    /// Bloom-filter.
    Bitmap(Vec<u8>),
    /// File-position where the root block for the Btree starts. For empty index
    /// root shall be None.
    Root(Option<u64>),
    /// Finger print for robt.
    Marker(Vec<u8>),
}

impl MetaItem {
    const ID: &'static str = "robt/metaitem/0.0.1";
}

/// Index type, immutable, durable, fully-packed and lockless reads.
pub struct Index<K, V, B>
where
    K: FromCbor,
    V: dbs::Diff + FromCbor,
    <V as dbs::Diff>::Delta: FromCbor,
    B: dbs::Bloom,
{
    dir: ffi::OsString,
    name: String,

    reader: Reader<K, V>,
    metas: Arc<Vec<MetaItem>>,
    stats: Stats,
    bitmap: Arc<B>,
}

impl<K, V, B> Index<K, V, B>
where
    K: FromCbor,
    V: dbs::Diff + FromCbor,
    <V as dbs::Diff>::Delta: FromCbor,
    B: dbs::Bloom,
{
    /// Open an existing index for read-only.
    pub fn open(dir: &ffi::OsStr, name: &str) -> Result<Index<K, V, B>> {
        match find_index_file(dir, name) {
            Some(file) => Self::open_file(&file),
            None => err_at!(InvalidInput, msg: "no index file {:?}/{}", dir, name)?,
        }
    }

    /// Open an existing index for read-only, from index file. file must be supplied
    /// along with full-path.
    pub fn open_file(file: &ffi::OsStr) -> Result<Index<K, V, B>> {
        let dir = match path::Path::new(file).parent() {
            Some(dir) => dir.as_os_str().to_os_string(),
            None => err_at!(IOError, msg: "file {:?} does not have parent dir", file)?,
        };
        let name = String::try_from(IndexFileName(file.to_os_string()))?;

        let mut index = err_at!(IOError, fs::OpenOptions::new().read(true).open(&file))?;

        let metas: Vec<MetaItem> = {
            let off = {
                let seek = io::SeekFrom::End(-16);
                let data = read_file!(index, seek, 8, "reading meta-off from index")?;
                i64::from_be_bytes(data.try_into().unwrap())
            };
            let len = {
                let seek = io::SeekFrom::End(-8);
                let data = read_file!(index, seek, 8, "reading meta-len from index")?;
                u64::from_be_bytes(data.try_into().unwrap())
            };
            let seek = io::SeekFrom::End(-off);
            let block = read_file!(index, seek, len, "reading meta-data from index")?;
            util::from_cbor_bytes(&block)?.0
        };

        let stats: Stats = match &metas[1] {
            MetaItem::Stats(stats) => util::from_cbor_bytes(stats)?.0,
            _ => unreachable!(),
        };

        let bitmap = match &metas[2] {
            MetaItem::Bitmap(data) => err_at!(Fatal, B::from_bytes(data))?.0,
            _ => unreachable!(),
        };

        let root = match &metas[3] {
            MetaItem::Root(root) => *root,
            _ => unreachable!(),
        };

        if let MetaItem::Marker(mrkr) = &metas[4] {
            if mrkr.ne(ROOT_MARKER.as_slice()) {
                err_at!(InvalidFile, msg: "invalid marker {:?}", mrkr)?
            }
        }

        let vlog = match stats.value_in_vlog || stats.delta_ok {
            true => {
                let vloc = stats.vlog_location.as_ref();
                let file_name = match vloc.map(|f| path::Path::new(f).file_name()) {
                    Some(Some(file_name)) => file_name.to_os_string(),
                    _ => ffi::OsString::from(VlogFileName::from(name.to_string())),
                };
                let vp: path::PathBuf = [dir.clone(), file_name].iter().collect();
                let vlog = err_at!(IOError, fs::OpenOptions::new().read(true).open(&vp))?;
                Some(vlog)
            }
            false => None,
        };

        let reader = Reader::from_root(root, &stats, index, vlog)?;

        let val = Index {
            dir,
            name,

            reader,
            metas: Arc::new(metas),
            stats,
            bitmap: Arc::new(bitmap),
        };

        Ok(val)
    }

    /// Optionally set a different bitmap over this index. Know what you are doing
    /// before calling this API.
    pub fn set_bitmap(&mut self, bitmap: B) {
        self.bitmap = Arc::new(bitmap)
    }

    /// Clone this index instance, with its underlying meta-data `shared` across index
    /// instances. Note that file-descriptors are not `shared`.
    pub fn try_clone(&self) -> Result<Self> {
        let index = match find_index_file(&self.dir, &self.name) {
            Some(ip) => err_at!(IOError, fs::OpenOptions::new().read(true).open(&ip))?,
            None => err_at!(InvalidFile, msg: "bad file {:?}/{}", &self.dir, &self.name)?,
        };

        let vlog = match self.stats.value_in_vlog || self.stats.delta_ok {
            true => {
                let vloc = self.stats.vlog_location.as_ref();
                let fnm = match vloc.map(|f| path::Path::new(f).file_name()) {
                    Some(Some(fnm)) => fnm.to_os_string(),
                    _ => ffi::OsString::from(VlogFileName::from(self.name.to_string())),
                };
                let vp: path::PathBuf = [self.dir.to_os_string(), fnm].iter().collect();
                Some(err_at!(
                    IOError,
                    fs::OpenOptions::new().read(true).open(&vp)
                )?)
            }
            false => None,
        };

        let root = match &self.metas[3] {
            MetaItem::Root(root) => *root,
            _ => unreachable!(),
        };

        let reader = Reader::from_root(root, &self.stats, index, vlog)?;

        let val = Index {
            dir: self.dir.clone(),
            name: self.name.clone(),

            reader,
            metas: Arc::clone(&self.metas),
            stats: self.stats.clone(),
            bitmap: Arc::clone(&self.bitmap),
        };

        Ok(val)
    }

    /// Consume this index and return a builder that can be used to incrementally
    /// build a new snapshot ontop of this index-snapthos.
    pub fn incremental(
        self,
        dir: &ffi::OsStr,
        name: &str,
        meta: Vec<u8>,
    ) -> Result<Builder<K, V>>
    where
        K: IntoCbor,
        V: IntoCbor,
        <V as dbs::Diff>::Delta: IntoCbor,
    {
        let mut config: Config = self.stats.into();
        config.dir = dir.to_os_string();
        config.name = name.to_string();
        Builder::incremental(config, meta)
    }

    /// Compact this index into a new index specified by [Config].
    /// The `bitmap` argument carry same meaning as that of `build_index`
    /// method. Refer to package documentation to know more about `Cutoff`.
    pub fn compact(
        mut self,
        mut config: Config,
        bitmap: B,
        cutoff: dbs::Cutoff,
    ) -> Result<Self>
    where
        K: Clone + Ord + Hash + IntoCbor,
        V: IntoCbor,
        <V as dbs::Diff>::Delta: IntoCbor,
    {
        // set to fresh vlog location, don't carry forward.
        config.set_vlog_location(None);

        let mut builder = {
            let app_meta = self.to_app_metadata();
            Builder::<K, V>::initial(config.clone(), app_meta)?
        };
        let r = (Bound::<K>::Unbounded, Bound::<K>::Unbounded);
        let iter = CompactScan::new(self.iter_versions(r)?, cutoff);

        builder.build_index(iter, bitmap, None)?;

        Index::open(&config.dir, &config.name)
    }

    /// Close this index, releasing OS resources. To purge, call `purge()` method.
    pub fn close(self) -> Result<()> {
        Ok(())
    }

    /// Purge this index from disk.
    pub fn purge(self) -> Result<()> {
        let is_vlog = self.stats.value_in_vlog || self.stats.delta_ok;
        let index_loc = to_index_location(&self.dir, &self.name);
        let vlog_loc = to_vlog_location(&self.dir, &self.name);

        mem::drop(self);

        purge_file(index_loc)?;
        if is_vlog {
            purge_file(vlog_loc)?;
        }

        Ok(())
    }
}

impl<K, V, B> Index<K, V, B>
where
    K: FromCbor,
    V: dbs::Diff + FromCbor,
    <V as dbs::Diff>::Delta: FromCbor,
    B: dbs::Bloom,
{
    pub fn to_name(&self) -> String {
        self.name.clone()
    }

    pub fn to_app_metadata(&self) -> Vec<u8> {
        match &self.metas[0] {
            MetaItem::AppMetadata(data) => data.clone(),
            _ => unreachable!(),
        }
    }

    pub fn to_stats(&self) -> Stats {
        self.stats.clone()
    }

    pub fn as_bitmap(&self) -> &B {
        self.bitmap.as_ref()
    }

    pub fn to_bitmap(&self) -> B
    where
        B: Clone,
    {
        self.bitmap.as_ref().clone()
    }

    pub fn to_root(&self) -> Option<u64> {
        match &self.metas[3] {
            MetaItem::Root(root) => *root,
            _ => unreachable!(),
        }
    }

    pub fn to_seqno(&self) -> u64 {
        self.stats.seqno
    }

    pub fn is_compacted(&self) -> bool {
        self.stats.n_abytes == 0
    }

    pub fn len(&self) -> usize {
        usize::try_from(self.stats.n_count).unwrap()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn to_index_location(&self) -> ffi::OsString {
        let mut config: Config = self.stats.clone().into();
        config.dir = self.dir.clone();
        config.to_index_location()
    }

    pub fn to_vlog_location(&self) -> Option<ffi::OsString> {
        match &self.stats.vlog_location {
            Some(loc) => {
                let loc: path::PathBuf =
                    [self.dir.clone(), path::Path::new(loc).file_name()?.into()]
                        .iter()
                        .collect();
                Some(loc.into())
            }
            None => None,
        }
    }

    pub fn footprint(&self) -> Result<usize> {
        self.reader.footprint()
    }
}

impl<K, V, B> Index<K, V, B>
where
    K: FromCbor,
    V: dbs::Diff + FromCbor,
    <V as dbs::Diff>::Delta: FromCbor,
    B: dbs::Bloom,
{
    pub fn get<Q>(&mut self, key: &Q) -> Result<dbs::Entry<K, V>>
    where
        K: Clone + Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let versions = false;
        dbs::Entry::try_from(self.reader.get(key, versions)?)
    }

    pub fn get_versions<Q>(&mut self, key: &Q) -> Result<dbs::Entry<K, V>>
    where
        K: Clone + Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let versions = true;
        dbs::Entry::try_from(self.reader.get(key, versions)?)
    }

    pub fn iter<R, Q>(&mut self, range: R) -> Result<Iter<K, V>>
    where
        K: Clone + Ord + Borrow<Q>,
        Q: ?Sized + Ord + ToOwned<Owned = K>,
        R: RangeBounds<Q>,
    {
        let (reverse, versions) = (false, false);
        self.reader.iter(range, reverse, versions)
    }

    pub fn iter_versions<R, Q>(&mut self, range: R) -> Result<Iter<K, V>>
    where
        K: Clone + Ord + Borrow<Q>,
        Q: ?Sized + Ord + ToOwned<Owned = K>,
        R: RangeBounds<Q>,
    {
        let (reverse, versions) = (false, true);
        self.reader.iter(range, reverse, versions)
    }

    pub fn reverse<R, Q>(&mut self, range: R) -> Result<Iter<K, V>>
    where
        K: Clone + Ord + Borrow<Q>,
        Q: ?Sized + Ord + ToOwned<Owned = K>,
        R: RangeBounds<Q>,
    {
        let (reverse, versions) = (true, false);
        self.reader.iter(range, reverse, versions)
    }

    pub fn reverse_versions<R, Q>(&mut self, range: R) -> Result<Iter<K, V>>
    where
        K: Clone + Ord + Borrow<Q>,
        Q: ?Sized + Ord + ToOwned<Owned = K>,
        R: RangeBounds<Q>,
    {
        let (reverse, versions) = (true, true);
        self.reader.iter(range, reverse, versions)
    }

    pub fn lsm_merge<I, E>(
        &mut self,
        snapshot: I,
        versions: bool,
    ) -> Result<YIter<K, V, I, E>>
    where
        K: Clone + Ord + FromCbor,
        V: dbs::Diff + FromCbor,
        <V as dbs::Diff>::Delta: FromCbor,
        I: Iterator<Item = Result<E>>,
        E: Into<Entry<K, V>>,
    {
        let start_bound = Bound::<&K>::Unbounded;
        let stack = self.reader.fwd_stack(start_bound, self.reader.as_root())?;
        let iter = YIter::new(snapshot, IterLsm::new(&mut self.reader, stack, versions));

        Ok(iter)
    }

    pub fn validate(&mut self) -> Result<Stats>
    where
        K: Clone + PartialOrd + Ord + fmt::Debug,
    {
        let iter = self.iter((Bound::<K>::Unbounded, Bound::<K>::Unbounded))?;

        let mut prev_key: Option<K> = None;
        let (mut n_count, mut n_deleted, mut seqno) = (0, 0, 0);

        for entry in iter {
            let entry = entry?;
            n_count += 1;

            if entry.is_deleted() {
                n_deleted += 1;
            }

            seqno = cmp::max(seqno, entry.to_seqno());

            match prev_key.as_ref().map(|pk| pk.lt(&entry.key)) {
                Some(true) | None => (),
                Some(false) => err_at!(Fatal, msg: "{:?} >= {:?}", prev_key, entry.key)?,
            }

            for d in entry.deltas.iter() {
                if d.to_seqno() >= seqno {
                    err_at!(Fatal, msg: "delta is newer {} {}", d.to_seqno(), seqno)?;
                }
            }

            prev_key = Some(entry.key.clone());
        }

        let s = self.to_stats();
        if n_count != s.n_count {
            err_at!(Fatal, msg: "validate, n_count {} > {}", n_count, s.n_count)
        } else if n_deleted != s.n_deleted {
            err_at!(Fatal, msg: "validate, n_deleted {} > {}", n_deleted, s.n_deleted)
        } else if seqno > 0 && seqno > s.seqno {
            err_at!(Fatal, msg: "validate, seqno {} > {}", seqno, s.seqno)
        } else {
            Ok(s)
        }
    }

    // TODO: use this with traversal arguments.
    pub fn print(&mut self) -> Result<()>
    where
        K: Clone + fmt::Debug,
        V: fmt::Debug,
        <V as dbs::Diff>::Delta: fmt::Debug,
    {
        println!("name              : {}", self.to_name());
        println!("app_meta_data     : {}", self.to_app_metadata().len());
        println!("root block at     : {:?}", self.to_root());
        println!("sequence num. at  : {}", self.to_seqno());
        let stats = self.to_stats();
        println!("stats         :");
        println!("  z_blocksize  : {}", stats.z_blocksize);
        println!("  m_blocksize  : {}", stats.m_blocksize);
        println!("  v_blocksize  : {}", stats.v_blocksize);
        println!("  delta_ok     : {}", stats.delta_ok);
        println!("  vlog_location: {:?}", stats.vlog_location);
        println!("  value_in_vlog: {}", stats.value_in_vlog);
        println!("  n_count      : {}", stats.n_count);
        println!("  n_deleted    : {}", stats.n_deleted);
        println!("  seqno        : {}", stats.seqno);
        println!("  n_abytes     : {}", stats.n_abytes);
        println!("  build_time   : {}", stats.build_time);
        println!("  epoch        : {}", stats.epoch);
        println!();
        self.reader.print()
    }
}

fn find_index_file(dir: &ffi::OsStr, name: &str) -> Option<ffi::OsString> {
    let iter = fs::read_dir(dir).ok()?;
    let entry = iter.filter_map(|entry| entry.ok()).find(|entry| {
        let filen = IndexFileName(entry.file_name());
        matches!(String::try_from(filen), Ok(nm) if nm == name)
    });

    entry.map(|entry| {
        let file_path: path::PathBuf =
            [dir.to_os_string(), IndexFileName(entry.file_name()).into()]
                .iter()
                .collect();
        file_path.as_os_str().to_os_string()
    })
}

fn purge_file(file: ffi::OsString) -> Result<()> {
    use fs2::FileExt;

    let fd = util::files::open_file_r(&file)?;
    // println!("purge file try_lock_exclusive <");
    match fd.try_lock_exclusive() {
        Ok(_) => {
            err_at!(IOError, fs::remove_file(&file), "remove file {:?}", file)?;
            err_at!(
                PurgeFile,
                fd.unlock(),
                "fail unlock for exclusive lock {:?}",
                file
            )
        }
        Err(_) => {
            err_at!(PurgeFile, msg: "file {:?} locked", file)
        }
    }
}

#[cfg(test)]
#[path = "index_test.rs"]
mod index_test;
