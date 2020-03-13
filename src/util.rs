use std::{
    borrow::Borrow,
    convert::TryInto,
    ffi, fs,
    io::{self, Read, Seek},
    ops::{Bound, RangeBounds},
    path,
};

use crate::{
    core::{Footprint, Result},
    error::Error,
};

// create a file in append mode for writing.
pub(crate) fn create_file_a(file: ffi::OsString) -> Result<fs::File> {
    let os_file = {
        let os_file = path::Path::new(&file);
        fs::remove_file(os_file).ok(); // NOTE: ignore remove errors.
        os_file
    };

    {
        let parent = os_file
            .parent()
            .ok_or(Error::InvalidFile(format!("{:?}", file)))?;
        err_at!(IoError, fs::create_dir_all(parent))?;
    };

    let mut opts = fs::OpenOptions::new();
    Ok(err_at!(
        IoError,
        opts.append(true).create_new(true).open(os_file)
    )?)
}

// open existing file in append mode for writing.
pub(crate) fn open_file_w(file: &ffi::OsString) -> Result<fs::File> {
    let os_file = path::Path::new(file);
    let mut opts = fs::OpenOptions::new();
    Ok(err_at!(IoError, opts.append(true).open(os_file))?)
}

// open file for reading.
pub(crate) fn open_file_r(file: &ffi::OsStr) -> Result<fs::File> {
    let os_file = path::Path::new(file);
    Ok(err_at!(
        IoError,
        fs::OpenOptions::new().read(true).open(os_file)
    )?)
}

pub(crate) fn read_buffer(fd: &mut fs::File, fpos: u64, n: u64, msg: &str) -> Result<Vec<u8>> {
    err_at!(IoError, fd.seek(io::SeekFrom::Start(fpos)))?;

    let mut buf = {
        let mut buf = Vec::with_capacity(convert_at!(n)?);
        buf.resize(buf.capacity(), 0);
        buf
    };

    let n = err_at!(IoError, fd.read(&mut buf))?;
    if buf.len() == n {
        Ok(buf)
    } else {
        let msg = format!("{} partial read {}/{} at {}", msg, buf.len(), n, fpos);
        Err(Error::PartialRead(msg))
    }
}

#[inline]
pub(crate) fn check_remaining(buf: &[u8], want: usize, msg: &str) -> Result<()> {
    if buf.len() < want {
        let msg = format!("{} unexpected buf size {} {}", msg, buf.len(), want);
        Err(Error::DecodeFail(msg))
    } else {
        Ok(())
    }
}

pub(crate) fn to_start_end<G, K>(within: G) -> (Bound<K>, Bound<K>)
where
    K: Clone,
    G: RangeBounds<K>,
{
    let start = match within.start_bound() {
        Bound::Included(val) => Bound::Included(val.clone()),
        Bound::Excluded(val) => Bound::Excluded(val.clone()),
        Bound::Unbounded => Bound::Unbounded,
    };
    let end = match within.end_bound() {
        Bound::Included(val) => Bound::Included(val.clone()),
        Bound::Excluded(val) => Bound::Excluded(val.clone()),
        Bound::Unbounded => Bound::Unbounded,
    };
    (start, end)
}

pub(crate) fn key_footprint<K>(key: &K) -> Result<isize>
where
    K: Footprint,
{
    use std::mem::size_of;
    let footprint: isize = convert_at!(size_of::<K>())?;
    Ok(footprint + key.footprint()?)
}

pub(crate) fn as_sharded_array<T>(array: &Vec<T>, mut shards: usize) -> Vec<&[T]>
where
    T: Clone,
{
    let mut n = array.len();
    let mut begin = 0;
    let mut acc = vec![];
    while (begin < array.len()) && (shards > 0) {
        let m: usize = ((n as f64) / (shards as f64)).ceil() as usize;
        acc.push(&array[begin..(begin + m)]);
        begin = begin + m;
        n -= m;
        shards -= 1;
    }

    (0..shards).for_each(|_| acc.push(&array[..0]));

    acc
}

pub(crate) fn as_part_array<T, K, N>(array: &Vec<T>, ranges: Vec<N>) -> Vec<Vec<T>>
where
    T: Clone + Borrow<K>,
    K: Clone + PartialOrd,
    N: Clone + RangeBounds<K>,
{
    let mut partitions: Vec<Vec<T>> = vec![vec![]; ranges.len()];
    for item in array.iter() {
        for (i, r) in ranges.iter().enumerate() {
            if r.contains(item.borrow()) {
                partitions[i].push(item.clone());
                break;
            }
        }
    }
    partitions
}

pub(crate) fn high_keys_to_ranges<K>(high_keys: Vec<Bound<K>>) -> Vec<(Bound<K>, Bound<K>)>
where
    K: Clone + Ord,
{
    let mut ranges = vec![];
    let mut low_key = Bound::<K>::Unbounded;
    for high_key in high_keys.into_iter() {
        let lk = high_key_to_low_key(&high_key);
        ranges.push((low_key, high_key));
        low_key = lk;
    }

    assert!(low_key == Bound::Unbounded);

    ranges
}

pub(crate) fn high_key_to_low_key<K>(hk: &Bound<K>) -> Bound<K>
where
    K: Clone,
{
    match hk {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Excluded(hk) => Bound::Included(hk.clone()),
        _ => unreachable!(),
    }
}

#[cfg(test)]
#[path = "util_test.rs"]
mod util_test;
