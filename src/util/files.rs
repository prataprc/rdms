use std::{ffi, fs, path};

use crate::{err_at, Error, Result};

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

/// create a file in append mode for writing.
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

/// open existing file in append mode for writing.
pub fn open_file_a(file: &ffi::OsStr) -> Result<fs::File> {
    let os_file = path::Path::new(file);
    let mut opts = fs::OpenOptions::new();
    Ok(err_at!(IOError, opts.append(true).open(os_file))?)
}

/// open file for reading.
pub fn open_file_r(file: &ffi::OsStr) -> Result<fs::File> {
    let os_file = path::Path::new(file);
    Ok(err_at!(
        IOError,
        fs::OpenOptions::new().read(true).open(os_file)
    )?)
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

/// Breadth first directory walking.
///
/// `callb` arguments:
///
/// * _state_, as mutable reference, user supplied and exist for the duration of walk.
/// * _dir_entry_, for each entry in a sub-directory.
/// * _depth_, depth level at which _dir-entry_ is located, starts with ZERO.
/// * _breath_, index of _dir-entry_ as stored in its parent directory.
pub fn walk<P, S, F>(root: P, state: S, mut callb: F) -> Result<S>
where
    P: AsRef<path::Path>,
    F: FnMut(&mut S, &fs::DirEntry, usize, usize) -> Result<()>,
{
    let depth = 0;
    do_walk(root, state, &mut callb, depth)
}

fn do_walk<P, S, F>(root: P, mut state: S, callb: &mut F, depth: usize) -> Result<S>
where
    P: AsRef<path::Path>,
    F: FnMut(&mut S, &fs::DirEntry, usize, usize) -> Result<()>,
{
    let mut subdirs = vec![];

    for (breath, entry) in err_at!(IOError, fs::read_dir(root))?.enumerate() {
        let entry = err_at!(IOError, entry)?;
        callb(&mut state, &entry, depth, breath)?;
        if err_at!(IOError, entry.file_type())?.is_dir() {
            subdirs.push(entry)
        }
    }

    for subdir in subdirs.into_iter() {
        state = do_walk(subdir.path(), state, callb, depth + 1)?;
    }

    Ok(state)
}

#[cfg(test)]
#[path = "files_test.rs"]
mod files_test;
