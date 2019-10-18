// Derived work from https://github.com/danburkert/fs2-rs

use std::{fs, io};

use crate::{core::Result, error::Error};

struct FileLock;

#[cfg(unix)]
use crate::libc;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;

#[cfg(unix)]
impl FileLock {
    pub fn lock_shared(file: &fs::File) -> Result<()> {
        Self::flock(file, libc::LOCK_SH)
    }

    pub fn lock_exclusive(file: &fs::File) -> Result<()> {
        Self::flock(file, libc::LOCK_EX)
    }

    pub fn try_lock_shared(file: &fs::File) -> Result<()> {
        Self::flock(file, libc::LOCK_SH | libc::LOCK_NB)
    }

    pub fn try_lock_exclusive(file: &fs::File) -> Result<()> {
        Self::flock(file, libc::LOCK_EX | libc::LOCK_NB)
    }

    pub fn unlock(file: &fs::File) -> Result<()> {
        Self::flock(file, libc::LOCK_UN)
    }

    #[cfg(not(target_os = "solaris"))]
    fn flock(file: &fs::File, flag: libc::c_int) -> Result<()> {
        let ret = unsafe { libc::flock(file.as_raw_fd(), flag) };
        if ret < 0 {
            Err(Error::IoError(io::Error::last_os_error()))
        } else {
            Ok(())
        }
    }

    /// Simulate flock() using fcntl(); primarily for Oracle Solaris.
    #[cfg(target_os = "solaris")]
    fn flock(file: &fs::File, flag: libc::c_int) -> Result<()> {
        let mut fl = libc::flock {
            l_whence: 0,
            l_start: 0,
            l_len: 0,
            l_type: 0,
            l_pad: [0; 4],
            l_pid: 0,
            l_sysid: 0,
        };

        // In non-blocking mode, use F_SETLK for cmd,
        // F_SETLKW otherwise, and don't forget to clear LOCK_NB.
        let (cmd, operation) = match flag & libc::LOCK_NB {
            0 => (libc::F_SETLKW, flag),
            _ => (libc::F_SETLK, flag & !libc::LOCK_NB),
        };

        match operation {
            libc::LOCK_SH => fl.l_type |= libc::F_RDLCK,
            libc::LOCK_EX => fl.l_type |= libc::F_WRLCK,
            libc::LOCK_UN => fl.l_type |= libc::F_UNLCK,
            _ => return Err(io::Error::from_raw_os_error(libc::EINVAL)),
        }

        let ret = unsafe { libc::fcntl(file.as_raw_fd(), cmd, &fl) };
        match ret {
            // Translate EACCES to EWOULDBLOCK
            -1 => match io::Error::last_os_error().raw_os_error() {
                Some(libc::EACCES) => return Err(lock_error()),
                _ => return Err(io::Error::last_os_error()),
            },
            _ => Ok(()),
        }
    }
}

#[cfg(windows)]
use crate::winapi::um::{
    fileapi::{LockFileEx, UnlockFile},
    minwinbase::{LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY},
};
#[cfg(windows)]
use std::mem;

#[cfg(windows)]
impl FileLock {
    pub fn lock_shared(file: &fs::File) -> Result<()> {
        lock_file(file, 0)
    }

    pub fn lock_exclusive(file: &fs::File) -> Result<()> {
        lock_file(file, LOCKFILE_EXCLUSIVE_LOCK)
    }

    pub fn try_lock_shared(file: &fs::File) -> Result<()> {
        lock_file(file, LOCKFILE_FAIL_IMMEDIATELY)
    }

    pub fn try_lock_exclusive(file: &fs::File) -> Result<()> {
        lock_file(file, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY)
    }

    pub fn unlock(file: &fs::File) -> Result<()> {
        unsafe {
            let ret = UnlockFile(file.as_raw_handle(), 0, 0, !0, !0);
            if ret == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }
    }

    fn lock_file(file: &fs::File, flags: DWORD) -> Result<()> {
        unsafe {
            let mut overlapped = mem::zeroed();
            let ret = LockFileEx(
                // args
                file.as_raw_handle(),
                flags,
                0,
                !0,
                !0,
                &mut overlapped,
            );
            if ret == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }
    }
}
