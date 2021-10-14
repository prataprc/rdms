use std::{convert::TryFrom, ffi, fmt, path, result};

use crate::{Error, Result};

/// An Index file is uniquely locatable by providing the `dir` and name.
/// where `dir` is the directory in which the index file is located and `name`
/// is the unique name for the index. `format!("{}-robt.indx", name)`
#[derive(Clone)]
pub struct IndexFileName(pub ffi::OsString);

impl From<String> for IndexFileName {
    fn from(name: String) -> IndexFileName {
        let file_name = format!("{}-robt.indx", name);
        IndexFileName(AsRef::<ffi::OsStr>::as_ref(&file_name).to_os_string())
    }
}

impl TryFrom<IndexFileName> for String {
    type Error = Error;

    fn try_from(fname: IndexFileName) -> Result<String> {
        let ffpp = path::Path::new(&fname.0);
        let fname = || -> Option<&str> {
            let fname = ffpp.file_name()?;
            if fname.to_str()?.ends_with("-robt.indx") {
                Some(path::Path::new(fname).file_stem()?.to_str()?)
            } else {
                None
            }
        }();

        match fname {
            Some(fname) => Ok(fname.strip_suffix("-robt").unwrap().to_string()),
            None => err_at!(InvalidFile, msg: "{:?}", ffpp),
        }
    }
}

impl From<IndexFileName> for ffi::OsString {
    fn from(name: IndexFileName) -> ffi::OsString {
        name.0
    }
}

impl fmt::Display for IndexFileName {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self.0.to_str() {
            Some(s) => write!(f, "{}", s),
            None => write!(f, "{:?}", self.0),
        }
    }
}

/// A Value log file is uniquely locatable by providing the `dir` and name.
/// where `dir` is the directory in which the index/vlog file is located and
/// `name` is the unique name for the index/vlog. `format!("{}-robt.vlog", name)`
#[derive(Clone)]
pub struct VlogFileName(pub ffi::OsString);

impl From<String> for VlogFileName {
    fn from(name: String) -> VlogFileName {
        let file_name = format!("{}-robt.vlog", name);
        VlogFileName(AsRef::<ffi::OsStr>::as_ref(&file_name).to_os_string())
    }
}

impl From<VlogFileName> for ffi::OsString {
    fn from(val: VlogFileName) -> ffi::OsString {
        val.0
    }
}

impl TryFrom<VlogFileName> for String {
    type Error = Error;

    fn try_from(fname: VlogFileName) -> Result<String> {
        let ffpp = path::Path::new(&fname.0);

        let fname = || -> Option<&str> {
            let fname = ffpp.file_name()?;
            if fname.to_str()?.ends_with("-robt.vlog") {
                Some(path::Path::new(fname).file_stem()?.to_str()?)
            } else {
                None
            }
        }();

        match fname {
            Some(fname) => Ok(fname.strip_suffix("-robt").unwrap().to_string()),
            None => err_at!(InvalidFile, msg: "{:?}", ffpp),
        }
    }
}

impl fmt::Display for VlogFileName {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self.0.to_str() {
            Some(s) => write!(f, "{}", s),
            None => write!(f, "{:?}", self.0),
        }
    }
}

#[cfg(test)]
#[path = "files_test.rs"]
mod files_test;
