use std::convert::TryInto;
use std::fmt::Display;
use std::{
    fs,
    io::{self, Read, Seek},
    path,
};

use crate::error::Error;

// open file for writing, if reuse is false, create file.
#[allow(dead_code)] // TODO: remove this once bogn is weaved-up.
pub(crate) fn open_file_w(file: &str, reuse: bool) -> Result<fs::File, Error> {
    let p = path::Path::new(file);

    let mut opts = fs::OpenOptions::new();
    Ok(match reuse {
        false => {
            let err = Error::InvalidFile(file.to_string());
            let parent = p.parent().ok_or(err)?;
            fs::create_dir_all(parent)?;
            fs::remove_file(p).ok();
            opts.append(true).create_new(true).open(p)?
        }
        true => opts.append(true).open(p)?,
    })
}

// open file for reading.
#[allow(dead_code)] // TODO: remove this once bogn is weaved-up.
pub(crate) fn open_file_r(file: &str) -> Result<fs::File, Error> {
    let p = path::Path::new(file);
    let mut opts = fs::OpenOptions::new();
    Ok(opts.read(true).open(p)?)
}

pub(crate) fn try_convert_int<T, U>(from: T, msg: &str) -> Result<U, Error>
where
    T: Copy + Display + TryInto<U>,
{
    match from.try_into() {
        Ok(to) => Ok(to),
        Err(_) => Err(Error::FailConversion(format!("{} for {}", msg, from))),
    }
}

pub(crate) fn read_buffer(
    fd: &mut fs::File,
    fpos: u64,
    n: u64,
    msg: &str,
) -> Result<Vec<u8>, Error> {
    fd.seek(io::SeekFrom::Start(fpos))?;
    let mut buf = Vec::with_capacity(n.try_into().unwrap());
    buf.resize(buf.capacity(), 0);
    let n = fd.read(&mut buf)?;
    if buf.len() == n {
        Ok(buf)
    } else {
        Err(Error::PartialRead(msg.to_string(), buf.len(), n))
    }
}
