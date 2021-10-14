//! Module implement common utility functions and types.

use cbordata::{Cbor, FromCbor, IntoCbor};

use std::{
    borrow::Borrow,
    ffi, fs,
    ops::{Bound, RangeBounds},
    path,
};

use crate::{db, Error, Result};

pub mod spinlock;
pub mod thread;

pub use spinlock::Spinlock;
pub use thread::Thread;

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
macro_rules! read_file {
    ($fd:expr, $seek:expr, $n:expr, $msg:expr) => {{
        use std::convert::TryFrom;

        match $fd.seek($seek) {
            Ok(_) => {
                let mut buf = vec![0; usize::try_from($n).unwrap()];
                match $fd.read(&mut buf) {
                    Ok(n) if buf.len() == n => Ok(buf),
                    Ok(n) => {
                        let m = buf.len();
                        err_at!(Fatal, msg: concat!($msg, " {}/{} at {:?}"), m, n, $seek)
                    }
                    Err(err) => err_at!(IOError, Err(err)),
                }
            }
            Err(err) => err_at!(IOError, Err(err)),
        }
    }};
}

#[macro_export]
macro_rules! write_file {
    ($fd:expr, $buffer:expr, $file:expr, $msg:expr) => {{
        use std::io::Write;

        match err_at!(IOError, $fd.write($buffer))? {
            n if $buffer.len() == n => Ok(n),
            n => err_at!(
                Fatal, msg: "partial-wr {}, {:?}, {}/{}", $msg, $file, $buffer.len(), n
            ),
        }
    }};
}

/// Helper function to serialize value `T` implementing IntoCbor, into byte-string.
pub fn into_cbor_bytes<T>(val: T) -> Result<Vec<u8>>
where
    T: IntoCbor,
{
    let mut data: Vec<u8> = vec![];
    let n = err_at!(
        FailCbor,
        err_at!(FailCbor, val.into_cbor())?.encode(&mut data)
    )?;
    if n != data.len() {
        err_at!(Fatal, msg: "cbor encoding len mistmatch {} {}", n, data.len())
    } else {
        Ok(data)
    }
}

/// Helper function to deserialize value `T` implementing FromCbor, from byte-string.
/// Return (value, bytes-consumed)
pub fn from_cbor_bytes<T>(mut data: &[u8]) -> Result<(T, usize)>
where
    T: FromCbor,
{
    let (val, n) = err_at!(FailCbor, Cbor::decode(&mut data))?;
    Ok((err_at!(FailCbor, T::from_cbor(val))?, n))
}

// create a file in append mode for writing.
pub fn create_file_a(file: &ffi::OsStr) -> Result<fs::File> {
    let os_file = {
        let os_file = path::Path::new(file);
        fs::remove_file(os_file).ok(); // NOTE: ignore remove errors.
        os_file
    };

    {
        let parent = match os_file.parent() {
            Some(parent) => Ok(parent),
            None => err_at!(InvalidFile, msg: "{:?}", file),
        }?;
        err_at!(IOError, fs::create_dir_all(parent))?;
    };

    let mut opts = fs::OpenOptions::new();
    Ok(err_at!(
        IOError,
        opts.append(true).create_new(true).open(os_file)
    )?)
}

// open existing file in append mode for writing.
pub fn open_file_a(file: &ffi::OsStr) -> Result<fs::File> {
    let os_file = path::Path::new(file);
    let mut opts = fs::OpenOptions::new();
    Ok(err_at!(IOError, opts.append(true).open(os_file))?)
}

// open file for reading.
pub fn open_file_r(file: &ffi::OsStr) -> Result<fs::File> {
    let os_file = path::Path::new(file);
    Ok(err_at!(
        IOError,
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
    use std::convert::TryFrom;

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

pub fn sync_write(file: &mut fs::File, data: &[u8]) -> Result<usize> {
    use std::io::Write;

    let n = err_at!(IOError, file.write(data))?;
    if n != data.len() {
        err_at!(IOError, msg: "partial write to file {} {}", n, data.len())?
    }
    err_at!(IOError, file.sync_all())?;
    Ok(n)
}

#[cfg(test)]
#[path = "mod_test.rs"]
mod mod_test;
