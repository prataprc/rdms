use std::{
    convert::TryInto,
    ffi, fs,
    io::{self, Read, Seek},
    path,
};

use crate::{core::Result, error::Error};

// create a file in append mode for writing.
pub(crate) fn open_file_cw(file: ffi::OsString) -> Result<fs::File> {
    let os_file = {
        let os_file = path::Path::new(&file);
        fs::remove_file(os_file).ok(); // NOTE: ignore remove errors.
        os_file
    };

    {
        let parent = os_file
            .parent()
            .ok_or(Error::InvalidFile(file.clone().into_string()?))?;
        fs::create_dir_all(parent)?;
    };

    let mut opts = fs::OpenOptions::new();
    Ok(opts.append(true).create_new(true).open(os_file)?)
}

// open existing file in append mode for writing.
pub(crate) fn open_file_w(file: &ffi::OsString) -> Result<fs::File> {
    let os_file = path::Path::new(file);
    let mut opts = fs::OpenOptions::new();
    Ok(opts.append(true).open(os_file)?)
}

// open file for reading.
pub(crate) fn open_file_r(file: &ffi::OsStr) -> Result<fs::File> {
    let os_file = path::Path::new(file);
    Ok(fs::OpenOptions::new().read(true).open(os_file)?)
}

// TODO: can we convert this into a macro ???
pub(crate) fn read_buffer(
    fd: &mut fs::File,
    fpos: u64, // position to read from.
    n: u64,    // bytes to read.
    msg: &str, // failure message.
) -> Result<Vec<u8>> {
    fd.seek(io::SeekFrom::Start(fpos))?;

    let mut buf = {
        let mut buf = Vec::with_capacity(n.try_into().unwrap());
        buf.resize(buf.capacity(), 0);
        buf
    };

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
