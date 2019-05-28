use std::{fs, path};

use crate::core::Result;
use crate::error::BognError;

// open file for writing, if append is false, create file.
pub(crate) fn open_file_w(file: &str, append: bool) -> Result<fs::File> {
    let p = path::Path::new(file);

    let mut opts = fs::OpenOptions::new();
    Ok(match append {
        false => {
            let err = BognError::InvalidFile(file.to_string());
            let parent = p.parent().ok_or(err)?;
            fs::create_dir_all(parent)?;
            opts.append(true).create_new(true).open(p)?
        }
        true => opts.append(true).open(p)?,
    })
}

// open file for reading.
pub(crate) fn open_file_r(file: &str) -> Result<fs::File> {
    let p = path::Path::new(file);
    let mut opts = fs::OpenOptions::new();
    Ok(opts.read(true).create_new(true).open(p)?)
}
