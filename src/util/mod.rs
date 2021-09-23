//! Utility functions.

use std::{
    borrow::Borrow,
    convert::TryFrom,
    ffi, fs,
    ops::{Bound, RangeBounds},
    path,
};

use crate::{db, Error, Result};

pub mod spinlock;

pub use spinlock::Spinlock;

#[macro_export]
macro_rules! check_remaining {
    ($buf:expr, $want:expr, $msg:expr) => {
        if $buf.len() < $want {
            err_at!(
                DecodeFail, msg: "insufficient input {}/{} ({})", $msg, $buf.len(), $want
            );
        } else {
            Ok(())
        }
    };
}

#[macro_export]
macro_rules! write_file {
    ($fd:expr, $buffer:expr, $file:expr, $msg:expr) => {{
        let n = err_at!(IoError, $fd.write($buffer))?;
        if $buffer.len() == n {
            Ok(n)
        } else {
            err_at!(Fatal, msg: "{}, {:?}, {}/{}", $msg, $file, $buffer.len(), n)
        }
    }};
}

#[macro_export]
macro_rules! read_file {
    ($fd:expr, $fpos:expr, $n:expr, $msg:expr) => {
        match $fd.seek(io::SeekFrom::Start($fpos)) {
            Ok(_) => {
                let mut buf = {
                    let mut buf = Vec::with_capacity($n as usize);
                    buf.resize(buf.capacity(), 0);
                    buf
                };
                match $fd.read(&mut buf) {
                    Ok(n) if buf.len() == n => Ok(buf),
                    Ok(n) => {
                        let m = buf.len();
                        err_at!(Fatal, msg: "{}, {}/{} at {}", $msg, m, n, $fpos)
                    }
                    Err(err) => err_at!(IoError, Err(err)),
                }
            }
            Err(err) => err_at!(IoError, Err(err)),
        }
    };
}

// create a file in append mode for writing.
pub fn create_file_a(file: ffi::OsString) -> Result<fs::File> {
    let os_file = {
        let os_file = path::Path::new(&file);
        fs::remove_file(os_file).ok(); // NOTE: ignore remove errors.
        os_file
    };

    {
        let parent = match os_file.parent() {
            Some(parent) => Ok(parent),
            None => err_at!(InvalidFile, msg: "{:?}", file),
        }?;
        err_at!(IoError, fs::create_dir_all(parent))?;
    };

    let mut opts = fs::OpenOptions::new();
    Ok(err_at!(
        IoError,
        opts.append(true).create_new(true).open(os_file)
    )?)
}

// open existing file in append mode for writing.
pub fn open_file_w(file: &ffi::OsString) -> Result<fs::File> {
    let os_file = path::Path::new(file);
    let mut opts = fs::OpenOptions::new();
    Ok(err_at!(IoError, opts.append(true).open(os_file))?)
}

// open file for reading.
pub fn open_file_r(file: &ffi::OsStr) -> Result<fs::File> {
    let os_file = path::Path::new(file);
    Ok(err_at!(
        IoError,
        fs::OpenOptions::new().read(true).open(os_file)
    )?)
}

pub fn to_start_end<G, K>(within: G) -> (Bound<K>, Bound<K>)
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

pub fn key_footprint<K>(key: &K) -> Result<isize>
where
    K: db::Footprint,
{
    use std::mem::size_of;
    let footprint = err_at!(FailConvert, isize::try_from(size_of::<K>()))?;
    Ok(footprint + key.footprint()?)
}

pub fn as_sharded_array<T>(array: &Vec<T>, mut shards: usize) -> Vec<&[T]>
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

pub fn as_part_array<T, K, N>(array: &Vec<T>, ranges: Vec<N>) -> Vec<Vec<T>>
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

pub fn high_keys_to_ranges<K>(high_keys: Vec<Bound<K>>) -> Vec<(Bound<K>, Bound<K>)>
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

pub fn high_key_to_low_key<K>(hk: &Bound<K>) -> Bound<K>
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
#[path = "mod_test.rs"]
mod mod_test;
