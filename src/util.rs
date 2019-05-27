use std::{fs, path};

use crate::core::Result;
use crate::error::BognError;

pub(crate) fn open_file_w(file: &str, append: bool) -> Result<fs::File> {
    let p = path::Path::new(&file);

    let parent = p.parent().ok_or(BognError::InvalidFile(file.to_string()))?;
    fs::create_dir_all(parent)?;

    let mut opts = fs::OpenOptions::new();
    Ok(match append {
        false => opts.append(true).create_new(true).open(p)?,
        true => opts.append(true).open(p)?,
    })
}

pub(crate) fn open_file_r(file: &str) -> Result<fs::File> {
    let p = path::Path::new(&file);
    let mut opts = fs::OpenOptions::new();
    Ok(opts.read(true).create_new(true).open(p)?)
}
