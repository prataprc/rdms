use std::{
    convert::TryInto,
    ffi, fs,
    io::{self, Read, Seek},
    path,
};

use crate::error::Error;

// open file for writing, if reuse is false, create file.
pub(crate) fn open_file_w(file: &str, reuse: bool) -> Result<fs::File, Error> {
    let p = path::Path::new(file);

    Ok(match reuse {
        false => {
            let mut opts = fs::OpenOptions::new();
            let parent = p.parent().ok_or(Error::InvalidFile(file.to_string()))?;
            fs::create_dir_all(parent)?;
            fs::remove_file(p).ok(); // NOTE: ignore remove errors.
            opts.append(true).create_new(true).open(p)?
        }
        true => {
            let mut opts = fs::OpenOptions::new();
            opts.append(true).open(p)?
        }
    })
}

// open file for reading.
pub(crate) fn open_file_r(file: &ffi::OsStr) -> Result<fs::File, Error> {
    let p = path::Path::new(file);
    Ok(fs::OpenOptions::new().read(true).open(p)?)
}

pub(crate) fn read_buffer(
    fd: &mut fs::File,
    fpos: u64, // position to read from.
    n: u64,    // bytes to read.
    msg: &str, // failure message.
) -> Result<Vec<u8>, Error> {
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
pub(crate) fn check_remaining(buf: &[u8], want: usize, msg: &str) -> Result<(), Error> {
    if buf.len() < want {
        let msg = format!("{} unexpected buf size {} {}", msg, buf.len(), want);
        Err(Error::DecodeFail(msg))
    } else {
        Ok(())
    }
}
