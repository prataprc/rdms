use binread::{BinRead, BinReaderExt};

use std::{
    collections::BTreeMap,
    convert::{TryFrom, TryInto},
    ffi, fmt, fs,
    io::{self, Read, Seek},
    path, result,
    str::FromStr,
    sync::{mpsc, Arc},
};

use crate::{util, zimf::workers, Error, Result};

// TODO: Metadata

const MAX_ENTRY_SIZE: usize = 1024;
const MAX_CLUSTER_SIZE: usize = 10 * 1024 * 1024;

/// Compression types allowed in zim archive.
#[derive(Clone)]
pub enum Compression {
    /// Legacy compression
    Uncompress1 = 0,
    /// Content is uncompressed.
    Uncompress2,
    /// Legacy, deprecated, Zlib compression
    Zlib,
    /// Legacy, deprecated, Bzip2 compression
    Bzip2,
    /// XZ2 compression
    Lzma2,
    /// Zstd compression
    Zstd,
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            Compression::Uncompress1 => write!(f, "Uncompress1"),
            Compression::Uncompress2 => write!(f, "Uncompress2"),
            Compression::Zlib => write!(f, "Zlib"),
            Compression::Bzip2 => write!(f, "Bzip2"),
            Compression::Lzma2 => write!(f, "Lzma2"),
            Compression::Zstd => write!(f, "Zstd"),
        }
    }
}

impl fmt::Debug for Compression {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{}", self)
    }
}

/// Namespaces supported by zim archive.
#[derive(Copy, Clone, PartialEq)]
pub enum Namespace {
    I,
    A,
    /// User content entries [Article Format](https://openzim.org/wiki/Article_Format)
    C,
    /// ZIM metadata - see [Metadata](https://openzim.org/wiki/Metadata)
    M,
    /// Well know entries (MainPage, Favicon) - see
    /// [Well known entries](https://openzim.org/wiki/Well_known_entries)
    W,
    /// search indexes - see [Search indexes](https://openzim.org/wiki/Search_indexes)
    X,
    /// No namepsace
    None,
}

impl FromStr for Namespace {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "I" | "i" => Ok(Namespace::I),
            "A" | "a" => Ok(Namespace::A),
            "C" | "c" => Ok(Namespace::C),
            "M" | "m" => Ok(Namespace::M),
            "W" | "w" => Ok(Namespace::W),
            "X" | "x" => Ok(Namespace::X),
            "-" => Ok(Namespace::None),
            _ => err_at!(InvalidInput, msg: "invalid namespace {:?}", s),
        }
    }
}

impl TryFrom<char> for Namespace {
    type Error = Error;

    fn try_from(ch: char) -> Result<Self> {
        match ch {
            'I' | 'i' => Ok(Namespace::I),
            'A' | 'a' => Ok(Namespace::A),
            'C' | 'c' => Ok(Namespace::C),
            'M' | 'm' => Ok(Namespace::M),
            'W' | 'w' => Ok(Namespace::W),
            'X' | 'x' => Ok(Namespace::X),
            '-' => Ok(Namespace::None),
            _ => err_at!(InvalidInput, msg: "invalid namespace {:?}", ch),
        }
    }
}

impl fmt::Display for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            Namespace::I => write!(f, "I??"),
            Namespace::A => write!(f, "A??"),
            Namespace::C => write!(f, "Content"),
            Namespace::M => write!(f, "Metadata"),
            Namespace::W => write!(f, "Wellknown"),
            Namespace::X => write!(f, "indeX"),
            Namespace::None => write!(f, "None"),
        }
    }
}

impl fmt::Debug for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{}", self)
    }
}

/// Handle to Zim archive. Maintains a thread pool locally can be cloned and used
/// by concurrent threads.
pub struct Zimf {
    inner: Inner,
}

impl Zimf {
    /// Open a zim-archive file in `loc`. `pool_size` thread pool size to be used
    /// for processing zim-archive content.
    pub fn open<P>(loc: P) -> Result<Zimf>
    where
        P: AsRef<path::Path>,
    {
        let inner = Inner::open(loc)?;
        Ok(Zimf { inner })
    }

    pub fn set_pool_size(&mut self, pool_size: usize) -> Result<&mut Self> {
        self.inner.set_pool_size(pool_size)?;
        Ok(self)
    }
}

impl Zimf {
    /// Return the zim-header
    pub fn to_location(&self) -> ffi::OsString {
        self.inner.loc.clone()
    }

    /// Return the zim-header
    pub fn as_header(&self) -> &Header {
        &self.inner.header
    }

    /// Return the full list of mime-types found in this zim file.
    pub fn as_mimes(&self) -> &[Mime] {
        &self.inner.mimes
    }

    /// Return list of all the entry details found in this zim file, sorted by url.
    pub fn as_entries(&self) -> &[Arc<Entry>] {
        &self.inner.entries
    }

    /// Return list of all the entry details found in this zim file, sorted by title.
    pub fn as_title_list(&self) -> &[Arc<Entry>] {
        &self.inner.title_list
    }

    /// Return list of all the cluster details
    pub fn as_clusters(&self) -> &[Cluster] {
        &self.inner.clusters
    }

    /// Return the Entry at `index`, follows the redirection link.
    pub fn get_entry(&self, index: usize) -> &Arc<Entry> {
        self.inner.get_entry(index)
    }

    /// Fetch entry corresponding to `cluster_num` and `blob_num`.
    pub fn index_to_entry(&self, cluster_num: usize, blob_num: usize) -> &Arc<Entry> {
        self.inner.index_to_entry(cluster_num, blob_num)
    }

    /// Return the Entry at `index` along with its content as a blob, follows the
    /// redirection link.
    pub fn get_entry_content(&self, index: usize) -> Result<(Entry, Vec<u8>)> {
        self.inner.get_entry_content(index)
    }

    /// Get all blobs archived in cluster identified by `cluster_num`.
    pub fn get_blobs(&self, cluster_num: usize) -> Result<Vec<Vec<u8>>> {
        self.inner.get_blobs(cluster_num)
    }
}

impl Zimf {
    /// Return the zimf information in json formatted string.
    pub fn to_json(&self) -> String {
        format!(
            concat!(
                "{{ ",
                r#""file_loc": {:?}, "#,
                r#""header": {}, "#,
                r#""mimes": {:?}, "#,
                r#""entries_count": {}, "#,
                r#""title_list_count": {} "#,
                "}} "
            ),
            self.inner.loc,
            self.inner.header.to_json(),
            self.inner.mimes,
            self.inner.entries.len(),
            self.inner.title_list.len(),
        )
    }
}

struct Inner {
    /// Location of the zim archive file.
    pub loc: ffi::OsString,
    /// Zim archive header.
    pub header: Header,
    /// Contents in zim-archive fall under one of the listed MIME type.
    pub mimes: Vec<Mime>,
    /// Full list of all entries zim-archive, each entry correspond to a url and
    /// the list is sorted by `url`.
    pub entries: Vec<Arc<Entry>>,
    /// Full list of all entries in zim-archive, sorted by title.
    pub title_list: Vec<Arc<Entry>>,
    /// One are more entries are compressed into a cluster. List of all clusters
    /// in the zim-archive.
    pub clusters: Vec<Cluster>,

    index_cluster: BTreeMap<u32, Vec<Arc<Entry>>>,
    pool: Option<util::thread::Pool<workers::Req, workers::Res, Result<()>>>,
}

impl Inner {
    fn open<P>(loc: P) -> Result<Inner>
    where
        P: AsRef<path::Path>,
    {
        use std::mem;

        let mut fd = err_at!(IOError, fs::OpenOptions::new().read(true).open(&loc))?;
        let loc = {
            let loc: &path::Path = loc.as_ref();
            loc.as_os_str().to_os_string()
        };

        let mut pool = util::thread::Pool::new_sync("zimf-parser", 1024);
        let zim_loc = loc.clone();
        pool.spawn(|rx: util::thread::Rx<workers::Req, workers::Res>| {
            || workers::worker(zim_loc, rx)
        });

        let header: Header = {
            let mut buf: Vec<u8> = vec![0; 80];
            err_at!(IOError, fd.read(&mut buf))?;
            let mut br = binread::io::Cursor::new(&buf);
            err_at!(InvalidFormat, br.read_le())?
        };

        let (mimes, _n) = Mime::from_file(&loc, header.mime_list_pos)?;

        let entry_offsets: Vec<u64> = {
            let mut buf: Vec<u8> = vec![0; (header.entry_count * 8) as usize];
            err_at!(IOError, fd.seek(io::SeekFrom::Start(header.url_ptr_pos)))?;
            err_at!(IOError, fd.read(&mut buf))?;
            buf.chunks(8)
                .map(|bs| u64::from_le_bytes(bs.try_into().unwrap()))
                .collect()
        };
        {
            let mut xs = entry_offsets.clone();
            xs.sort_unstable();
            xs.dedup();
            assert!(xs == entry_offsets);
        }
        let mut entries: Vec<Arc<Entry>> = {
            let (start, len) = match (entry_offsets.first(), entry_offsets.last()) {
                (Some(first), Some(last)) => {
                    let len = (last - first) + (MAX_ENTRY_SIZE as u64);
                    (*first, len)
                }
                (_, _) => err_at!(InvalidFormat, msg: "fail loading entry_offsets")?,
            };
            let mut buf: Vec<u8> = vec![0; len as usize];
            err_at!(IOError, fd.seek(io::SeekFrom::Start(start)))?;
            err_at!(IOError, fd.read(&mut buf))?;

            let mut entries = vec![];
            for off in entry_offsets.into_iter() {
                let start = (off - start) as usize;
                entries.push(Arc::new(Entry::from_slice(&buf[start..])?))
            }
            entries
        };
        entries.sort_by(|e1, e2| e1.url.cmp(&e2.url));

        let title_list: Vec<Arc<Entry>> = {
            let mut buf: Vec<u8> = vec![0; (header.entry_count * 4) as usize];
            err_at!(IOError, fd.seek(io::SeekFrom::Start(header.title_ptr_pos)))?;
            err_at!(IOError, fd.read(&mut buf))?;
            buf.chunks(4)
                .map(|bs| u32::from_le_bytes(bs.try_into().unwrap()) as usize)
                .map(|off| Arc::clone(&entries[off]))
                .collect()
        };

        let cluster_offsets: Vec<u64> = {
            let mut buf: Vec<u8> = vec![0; (header.cluster_count * 8) as usize];
            err_at!(
                IOError,
                fd.seek(io::SeekFrom::Start(header.cluster_ptr_pos))
            )?;
            err_at!(IOError, fd.read(&mut buf))?;
            buf.chunks(8)
                .map(|bs| u64::from_le_bytes(bs.try_into().unwrap()))
                .collect()
        };
        {
            let mut xs = cluster_offsets.clone();
            xs.sort_unstable();
            xs.dedup();
            assert!(xs == cluster_offsets);
        }
        let clusters: Vec<Cluster> = {
            // println!("cluster_offsets:{}", cluster_offsets.len());
            let (tx, rx) = mpsc::channel();
            for (_num, off) in cluster_offsets.into_iter().enumerate() {
                // println!("cluster num:{} off:{}", _num, off);
                workers::read_cluster_header(&pool, off, tx.clone())?;
            }
            mem::drop(tx);

            let mut clusters = vec![];
            for res in rx {
                // println!("clusters:{}", clusters.len());
                clusters.push(res?.try_into()?);
            }
            clusters.sort_by_key(|c: &Cluster| c.off);

            let nclust = clusters[1..].to_vec();
            for (a, b) in clusters[..].iter_mut().zip(nclust.iter()) {
                a.size = Some((b.off - a.off) as usize);
            }
            // println!("last cluster {:?}", clusters.last());
            clusters
        };

        let mut inner = Inner {
            loc,
            header,
            mimes,
            entries,
            title_list,
            clusters,

            index_cluster: BTreeMap::new(),
            pool: Some(pool),
        };

        {
            (0..inner.header.cluster_count).for_each(|cn| {
                inner.index_cluster.insert(cn, vec![]);
            });
            let n = inner.entries.len();
            for index in 0..n {
                let entry = Arc::clone(inner.get_entry(index));
                if let Some(value) = inner
                    .index_cluster
                    .get_mut(&entry.to_cluster_num().unwrap())
                {
                    value.push(entry)
                }
            }
        }

        Ok(inner)
    }

    fn set_pool_size(&mut self, pool_size: usize) -> Result<()> {
        self.pool.take().unwrap().close_wait()?;
        let mut pool = util::thread::Pool::new_sync("zimf-parser", 1024);
        pool.set_pool_size(pool_size);
        let zim_loc = self.loc.clone();
        pool.spawn(|rx: util::thread::Rx<workers::Req, workers::Res>| {
            || workers::worker(zim_loc, rx)
        });
        self.pool = Some(pool);

        Ok(())
    }

    fn get_entry(&self, index: usize) -> &Arc<Entry> {
        // println!("get_entry index:{}", index);
        match self.entries[index].ee.clone() {
            EE::D { .. } => &self.entries[index],
            EE::R { redirect_index } => self.get_entry(redirect_index as usize),
        }
    }

    fn index_to_entry(&self, cluster_num: usize, blob_num: usize) -> &Arc<Entry> {
        for (i, e) in self.entries.iter().enumerate() {
            if let Some((c, b)) = e.to_blob_num() {
                if (c as usize) == cluster_num && (b as usize) == blob_num {
                    return &self.entries[i];
                }
            }
        }
        unreachable!()
    }

    fn get_entry_content(&self, index: usize) -> Result<(Entry, Vec<u8>)> {
        // println!("get_entry index:{}", index);
        let entry = self.entries[index].clone();
        match entry.ee.clone() {
            EE::D {
                cluster_num,
                blob_num,
            } => {
                let (tx, rx) = mpsc::channel();
                let cluster = self.clusters[cluster_num as usize].clone();
                //println!(
                //    "get_entry cluster_num:{} blob_num:{} cluster_off:{}",
                //    cluster_num, blob_num, cluster.off
                //);
                workers::read_cluster_blobs(self.pool.as_ref().unwrap(), cluster, tx)?;

                let blob = match err_at!(IPCFail, rx.recv())?? {
                    workers::Res::Blocks { blobs } => blobs[blob_num as usize].to_vec(),
                    _ => unreachable!(),
                };

                Ok((entry.as_ref().clone(), blob))
            }
            EE::R { redirect_index } => self.get_entry_content(redirect_index as usize),
        }
    }

    pub fn get_blobs(&self, cluster_num: usize) -> Result<Vec<Vec<u8>>> {
        let cluster = self.clusters[cluster_num].clone();
        let (tx, rx) = mpsc::channel();

        workers::read_cluster_blobs(self.pool.as_ref().unwrap(), cluster, tx)?;

        let blobs = match err_at!(IPCFail, rx.recv())?? {
            workers::Res::Blocks { blobs } => blobs,
            _ => unreachable!(),
        };
        Ok(blobs)
    }
}

/// Zim archive file's [Header](https://openzim.org/wiki/ZIM_file_format#Header)
#[derive(Clone, BinRead)]
pub struct Header {
    pub magic_number: u32,
    pub major_version: u16,
    pub minor_version: u16,
    pub uuid: [u8; 16],
    pub entry_count: u32,
    pub cluster_count: u32,
    pub url_ptr_pos: u64,
    pub title_ptr_pos: u64,
    pub cluster_ptr_pos: u64,
    pub mime_list_pos: u64,
    pub main_page: u32,
    pub layout_page: u32,
    pub checksum_pos: u64, // TODO
}

impl Header {
    /// Return the zim header information in json formatted string.
    pub fn to_json(&self) -> String {
        let uuid = uuid::Uuid::from_slice(&self.uuid).unwrap();
        format!(
            concat!(
                "{{ ",
                r#""magic_number": "{:x}", "#,
                r#""major_version": {}, "#,
                r#""minor_version": {}, "#,
                r#""uuid": "{}", "#,
                r#""entry_count": {}, "#,
                r#""cluster_count": {}, "#,
                r#""url_ptr_pos": {}, "#,
                r#""title_ptr_pos": {}, "#,
                r#""cluster_ptr_pos": {}, "#,
                r#""mime_list_pos": {}, "#,
                r#""main_page": {}, "#,
                r#""layout_page": "{:x}", "#,
                r#""checksum_pos": {} "#,
                "}}"
            ),
            self.magic_number,
            self.major_version,
            self.minor_version,
            uuid.to_hyphenated().to_string(),
            self.entry_count,
            self.cluster_count,
            self.url_ptr_pos,
            self.title_ptr_pos,
            self.cluster_ptr_pos,
            self.mime_list_pos,
            self.main_page,
            self.layout_page,
            self.checksum_pos,
        )
    }
}

/// Mime string.
#[derive(Clone)]
pub struct Mime(String);

impl fmt::Display for Mime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> result::Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for Mime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> result::Result<(), fmt::Error> {
        write!(f, "{:?}", self.0)
    }
}

impl Mime {
    fn from_file(loc: &ffi::OsStr, pos: u64) -> Result<(Vec<Mime>, u64)> {
        use std::str::from_utf8;

        let mut fd = err_at!(IOError, fs::OpenOptions::new().read(true).open(&loc))?;
        err_at!(IOError, fd.seek(io::SeekFrom::Start(pos)))?;

        let mut mimes = vec![];
        let mut bs = vec![];
        let mut iter = fd.bytes().enumerate();
        loop {
            match iter.next() {
                Some((n, Ok(0))) if bs.is_empty() => break Ok((mimes, (n + 1) as u64)),
                Some((_, Ok(0))) => {
                    let s: &str = err_at!(InvalidFormat, from_utf8(&bs), "bad-mime")?;
                    mimes.push(Mime(s.to_string()));
                    bs.truncate(0);
                }
                Some((_, Ok(b))) => {
                    bs.push(b);
                }
                Some((_, Err(err))) => err_at!(IOError, msg: "{}", err)?,
                None => err_at!(InvalidFormat, msg: "End-of-file while reading Mimes")?,
            }
        }
    }
}

/// Entry corresponds to a single url and its content.
/// Refer [here](https://openzim.org/wiki/ZIM_file_format#Directory_Entries)
/// for details.
#[derive(Clone, Debug, BinRead)]
pub struct Entry {
    pub mime_type: u16,
    pub param_len: u8,
    pub namespace: u8,
    pub revision: u32,
    #[br(ignore)]
    pub ee: EE,
    #[br(ignore)]
    pub url: String,
    #[br(ignore)]
    pub title: String,
    #[br(ignore)]
    pub param: Vec<u8>,
}

/// Entry can directly point to the content or redirect to another entry.
#[derive(Clone, Debug)]
pub enum EE {
    D { cluster_num: u32, blob_num: u32 },
    R { redirect_index: u32 },
}

impl Default for EE {
    fn default() -> EE {
        EE::D {
            cluster_num: 0,
            blob_num: 0,
        }
    }
}

impl Entry {
    fn from_slice(buf: &[u8]) -> Result<Entry> {
        use std::str::from_utf8;

        let mut entry: Entry = {
            let mut br = binread::io::Cursor::new(&buf);
            err_at!(InvalidFormat, br.read_le())?
        };

        let (ee, mut n) = match entry.mime_type {
            0xFFFF => {
                let redirect_index = u32::from_le_bytes(buf[8..12].try_into().unwrap());
                (EE::R { redirect_index }, 12)
            }
            _ => {
                let cluster_num = u32::from_le_bytes(buf[8..12].try_into().unwrap());
                let blob_num = u32::from_le_bytes(buf[12..16].try_into().unwrap());
                let ee = EE::D {
                    cluster_num,
                    blob_num,
                };
                (ee, 16)
            }
        };

        let iter = &mut buf[n..].iter().enumerate();
        let (m, url) = loop {
            match iter.next() {
                Some((m, 0)) => {
                    let res = from_utf8(&buf[n..(n + m)]);
                    break (m, err_at!(InvalidFormat, res, "bad_url {}", m)?);
                }
                Some((_, _)) => continue,
                None => err_at!(InvalidFormat, msg: "bad entry")?,
            }
        };
        n += m;

        let iter = &mut buf[n..].iter().enumerate();
        let (m, title) = loop {
            match iter.next() {
                Some((m, 0)) => {
                    let res = from_utf8(&buf[n..(n + m)]);
                    break (m, err_at!(InvalidFormat, res, "bad_url {}", m)?);
                }
                Some((_, _)) => continue,
                None => err_at!(InvalidFormat, msg: "bad entry")?,
            }
        };
        n += m;

        let param = buf[n..(n + entry.param_len as usize)].to_vec();

        entry.ee = ee;
        entry.url = url.to_string();
        entry.title = title.to_string();
        entry.param = param;

        Ok(entry)
    }

    /// Return the namespace, the entry belongs to
    pub fn to_namespace(&self) -> Result<Namespace> {
        (self.namespace as char).try_into()
    }

    /// Return the cluster number in which the entry is stored.
    pub fn to_cluster_num(&self) -> Option<u32> {
        match self.ee.clone() {
            EE::D { cluster_num, .. } => Some(cluster_num),
            EE::R { .. } => None,
        }
    }

    /// Return the (cluster-number, blob-number) in which the entry is stored.
    pub fn to_blob_num(&self) -> Option<(u32, u32)> {
        match self.ee.clone() {
            EE::D {
                cluster_num,
                blob_num,
            } => Some((cluster_num, blob_num)),
            EE::R { .. } => None,
        }
    }

    pub fn is_redirect(&self) -> bool {
        match &self.ee {
            EE::D { .. } => false,
            EE::R { .. } => true,
        }
    }
}

/// Cluster location in file, its compression and other details.
/// Refer [here](https://openzim.org/wiki/ZIM_file_format#Clusters) for details.
#[derive(Clone, Debug)]
pub struct Cluster {
    pub off: u64,
    pub size: Option<usize>,
    pub compression: Compression,
    pub boff_size: usize,
}

impl Cluster {
    /// Load cluster from given offset in file `fd`.
    pub fn from_offset(off: u64, fd: &mut fs::File) -> Result<Cluster> {
        let mut buf = [0_u8; 1];
        err_at!(IOError, fd.seek(io::SeekFrom::Start(off)))?;
        err_at!(IOError, fd.read(&mut buf))?;

        // println!("cluster-byte:0x{:x}", buf[0]);
        let compression = match buf[0] & 0xf {
            0 => Compression::Uncompress1,
            1 => Compression::Uncompress1,
            2 => Compression::Zlib,
            3 => Compression::Bzip2,
            4 => Compression::Lzma2,
            5 => Compression::Zstd,
            c => err_at!(InvalidFormat, msg: "invalid compression byte {}", c)?,
        };
        let boff_size = if buf[0] & 0x10 == 0 { 4 } else { 8 };

        //println!("cluster-compression:{:?}", compression);
        let val = Cluster {
            off,
            size: None,
            compression,
            boff_size,
        };
        Ok(val)
    }

    /// Return all the blobs in this cluster.
    pub fn to_blobs(&self, fd: &mut fs::File) -> Result<Vec<Vec<u8>>> {
        // println!("to_blobs off:{}", self.off);

        err_at!(IOError, fd.seek(io::SeekFrom::Start(self.off + 1)))?;

        let data = match self.compression {
            Compression::Lzma2 => {
                use xz2::read::XzDecoder;

                let size = self.size.unwrap_or(MAX_CLUSTER_SIZE) - 1;
                // println!("lzma2 off:{} size:{}", self.off, size);
                let mut indata = vec![0; size];
                let n = err_at!(IOError, fd.read(&mut indata))?;
                indata.truncate(n);

                let mut data = vec![];
                let mut dec = XzDecoder::new(fd);
                let _n = err_at!(IOError, dec.read_to_end(&mut data))?;
                Ok(data)
            }
            Compression::Zstd => {
                use zstd::stream::read::Decoder;

                let size = self.size.unwrap_or(MAX_CLUSTER_SIZE) - 1;
                // println!("zstd off:{} size:{}", self.off, size);
                let mut indata = vec![0; size];
                let n = err_at!(IOError, fd.read(&mut indata))?;
                indata.truncate(n);

                let mut outdata = vec![];
                let mut dec = err_at!(IOError, Decoder::new(indata.as_slice()))?;
                let _n = err_at!(IOError, dec.read_to_end(&mut outdata))?;
                Ok(outdata)
            }
            Compression::Uncompress1 | Compression::Uncompress2 => {
                let mut buf = vec![0_u8; self.boff_size];
                err_at!(IOError, fd.read(&mut buf))?;

                let first_blob_off: u64 = match self.boff_size {
                    4 => u32::from_le_bytes(buf[..4].try_into().unwrap()) as u64,
                    8 => u64::from_le_bytes(buf[..8].try_into().unwrap()),
                    _ => unreachable!(),
                };
                let n_blobs = (first_blob_off / (self.boff_size as u64)) - 1;
                err_at!(
                    IOError,
                    fd.seek(io::SeekFrom::Start(
                        self.off + 1 + (n_blobs * (self.boff_size as u64))
                    ))
                )?;
                err_at!(IOError, fd.read(&mut buf))?;

                let len: u64 = match self.boff_size {
                    4 => u32::from_le_bytes(buf[..4].try_into().unwrap()) as u64,
                    8 => u64::from_le_bytes(buf[..8].try_into().unwrap()),
                    _ => unreachable!(),
                };

                //println!(
                //    "uncompress off:{} first_blob_off:{} n_blobs:{} len:{}",
                //    self.off, first_blob_off, n_blobs, len
                //);

                let mut data = vec![0; len as usize];
                err_at!(IOError, fd.seek(io::SeekFrom::Start(self.off + 1)))?;
                err_at!(IOError, fd.read(&mut data))?;
                Ok(data)
            }
            Compression::Zlib => err_at!(InvalidFormat, msg: "invalid compression Zlib"),
            Compression::Bzip2 => err_at!(InvalidFormat, msg: "invalid compression Zlib"),
        }?;

        // println!("data_len:{}", data.len());
        let mut blob_offsets: Vec<usize> = data
            .chunks(self.boff_size)
            .map(|bs| match self.boff_size {
                4 => u32::from_le_bytes(bs[..4].try_into().unwrap()) as usize,
                8 => u64::from_le_bytes(bs[..8].try_into().unwrap()) as usize,
                _ => unreachable!(),
            })
            .take_while(|off| off < &data.len())
            .collect();
        blob_offsets.push(data.len());
        // println!("{:?}", blob_offsets);
        // println!("blob_offsets len:{}", blob_offsets.len());

        Ok(blob_offsets[..]
            .iter()
            .zip(blob_offsets[1..].iter())
            .map(|(s, e)| data[*s..*e].to_vec())
            .collect())
    }
}
