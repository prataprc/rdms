use fs2::FileExt;

use std::{convert::TryFrom, ffi, fs, mem};

use crate::{util, write_file, Error, Result};

pub enum Flusher {
    File {
        loc: ffi::OsString,
        fpos: u64,
        th: Option<util::Thread<Vec<u8>, u64, Result<u64>>>,
        tx: Option<util::thread::Tx<Vec<u8>, u64>>,
    },
    None,
}

impl Drop for Flusher {
    fn drop(&mut self) {
        match self {
            Flusher::None => (),
            Flusher::File { tx, .. } => mem::drop(tx.take()),
        }
    }
}

impl Flusher {
    // Create a new flusher thread, there are two flushers for `robt` index, one
    // for the index-file and the other is for value-file, if enabled.
    pub fn new(loc: &ffi::OsStr, create: bool, chan_size: usize) -> Result<Flusher> {
        let (fd, fpos) = if create {
            (util::create_file_a(loc)?, 0)
        } else {
            let fpos = err_at!(IOError, fs::metadata(loc))?.len().saturating_sub(1);
            (util::open_file_a(loc)?, fpos)
        };

        let ffpp = loc.to_os_string();
        let (th, tx) = util::Thread::new_sync(
            "flusher",
            chan_size,
            move |rx: util::thread::Rx<Vec<u8>, u64>| {
                move || thread_flush(ffpp, fd, rx, fpos)
            },
        );

        let val = Flusher::File {
            loc: loc.to_os_string(),
            fpos,
            th: Some(th),
            tx: Some(tx),
        };

        Ok(val)
    }

    // create an empty flusher.
    pub fn empty() -> Flusher {
        Flusher::None
    }

    pub fn to_location(&self) -> Option<ffi::OsString> {
        match self {
            Flusher::File { loc, .. } => Some(loc.clone()),
            Flusher::None => None,
        }
    }

    // return the latest file position.
    pub fn to_fpos(&self) -> Option<u64> {
        match self {
            Flusher::File { fpos, .. } => Some(*fpos),
            Flusher::None => None,
        }
    }

    // flush data, call to this function only batches data.
    pub fn flush(&mut self, data: Vec<u8>) -> Result<()> {
        match self {
            Flusher::File { fpos, tx, .. } => {
                *fpos = tx.as_ref().unwrap().request(data)?
            }
            Flusher::None => (),
        };
        Ok(())
    }

    // close this flusher and associated thread, after syncing data to disk.
    pub fn close(&mut self) -> Result<u64> {
        match self {
            Flusher::File { tx, th, .. } => {
                mem::drop(tx.take());
                th.take().unwrap().join()?
            }
            Flusher::None => Ok(0),
        }
    }
}

fn thread_flush(
    loc: ffi::OsString,
    mut fd: fs::File,
    rx: util::thread::Rx<Vec<u8>, u64>,
    mut fpos: u64,
) -> Result<u64> {
    // println!("thread_flush lock_shared <");
    err_at!(IOError, fd.lock_shared(), "fail read lock for {:?}", loc)?;

    for (data, res_tx) in rx {
        // println!("flush {:?} fpos:{} len:{}", loc, fpos, data.len());
        write_file!(fd, &data, &loc, "flushing file")?;

        fpos += u64::try_from(data.len()).unwrap();
        res_tx.map(|tx| tx.send(fpos).ok());
    }

    err_at!(IOError, fd.sync_all(), "fail sync_all {:?}", loc)?;
    err_at!(IOError, fd.unlock(), "fail read unlock {:?}", loc)?;
    // println!("thread_flush unlock >");

    Ok(fpos)
}

#[cfg(test)]
#[path = "flush_test.rs"]
mod flush_test;
