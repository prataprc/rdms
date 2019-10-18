use std::{
    convert::TryInto,
    ffi, fs,
    io::{self, Read, Seek},
    path,
};

use crate::{core::Result, error::Error};

// create a file in append mode for writing.
pub(crate) fn open_file_cw(file: ffi::OsString) -> Result<fs::File> {
    let err_file = file.clone();
    let p = path::Path::new(&file);
    let mut opts = fs::OpenOptions::new();
    let parent = p
        .parent()
        .ok_or(Error::InvalidFile(err_file.into_string()?))?;
    fs::create_dir_all(parent)?;
    fs::remove_file(p).ok(); // NOTE: ignore remove errors.
    Ok(opts.append(true).create_new(true).open(p)?)
}

// open existing file in append mode for writing.
pub(crate) fn open_file_w(file: &ffi::OsString) -> Result<fs::File> {
    let p = path::Path::new(file);
    let mut opts = fs::OpenOptions::new();
    Ok(opts.append(true).open(p)?)
}

// open file for reading.
pub(crate) fn open_file_r(file: &ffi::OsStr) -> Result<fs::File> {
    let p = path::Path::new(file);
    Ok(fs::OpenOptions::new().read(true).open(p)?)
}

pub(crate) fn read_buffer(
    fd: &mut fs::File,
    fpos: u64, // position to read from.
    n: u64,    // bytes to read.
    msg: &str, // failure message.
) -> Result<Vec<u8>> {
    fd.seek(io::SeekFrom::Start(fpos))?;
    let mut buf = Vec::with_capacity(n.try_into().unwrap());
    buf.resize(buf.capacity(), 0);
    let n = fd.read(&mut buf)?;
    if buf.len() == n {
        Ok(buf)
    } else {
        let msg = format!("{} partial read {} at {}", msg, fpos, n);
        Err(Error::PartialRead(msg))
    }
}

// TODO: can this be replaced as Macros.
#[inline]
pub(crate) fn check_remaining(buf: &[u8], want: usize, msg: &str) -> Result<()> {
    if buf.len() < want {
        let msg = format!("{} unexpected buf size {} {}", msg, buf.len(), want);
        Err(Error::DecodeFail(msg))
    } else {
        Ok(())
    }
}

#[cfg(test)]
#[path = "util_test.rs"]
mod util_test;
