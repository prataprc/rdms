use std::{fs, path};

use crate::core::Result;
use crate::error::Error;

// open file for writing, if reuse is false, create file.
pub(crate) fn open_file_w(file: &str, reuse: bool) -> Result<fs::File> {
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
pub(crate) fn open_file_r(file: &str) -> Result<fs::File> {
    let p = path::Path::new(file);
    let mut opts = fs::OpenOptions::new();
    Ok(opts.read(true).open(p)?)
}
