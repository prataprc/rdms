use std::{
    ffi, fs,
    io::{self, Read, Seek, Write},
};

use crate::core::{Diff, Result, Serialize};
use crate::error::BognError;

// *-----*------------------------------------*
// |flags|        60-bit length               |
// *-----*------------------------------------*
// |                 payload                  |
// *-------------------*----------------------*
//
// bit 60 shall be set.

#[derive(Clone)]
pub(crate) enum Value<V>
where
    V: Default + Serialize,
{
    Native {
        value: V,
    },
    Reference {
        fpos: u64, // point to value payload in index-file or vlog-file
        length: u64,
    },
    Backup {
        file: ffi::OsString, // must be a vlog or index filename with path
        fpos: u64,           // point to payload in index-file or vlog-file
        length: u64,
    }, // points to entry on disk.
}

impl<V> Value<V>
where
    V: Default + Serialize,
{
    const VALUE_FLAG: u64 = 0x1000000000000000;

    pub(crate) fn new_native(value: V) -> Value<V> {
        Value::Native { value }
    }

    pub(crate) fn new_reference(fpos: u64, length: u64) -> Value<V> {
        Value::Reference { fpos, length }
    }

    pub(crate) fn new_backup(file: ffi::OsString, fpos: u64, length: u64) -> Value<V> {
        Value::Backup { file, fpos, length }
    }

    pub(crate) fn is_reference(&self) -> bool {
        match self {
            Value::Reference { .. } => true,
            Value::Native { .. } | Value::Backup { .. } => false,
        }
    }

    pub(crate) fn to_native(self, fd: Option<&mut fs::File>) -> Result<Value<V>> {
        match (self, fd) {
            (obj @ Value::Native { .. }, _) => Ok(obj),
            (Value::Reference { fpos, length }, Some(fd)) => Self::read(fd, fpos, length as usize),
            (Value::Reference { .. }, None) => panic!("invalid call !!"),
            (Value::Backup { fpos, length, .. }, Some(fd)) => Self::read(fd, fpos, length as usize),
            (Value::Backup { file, fpos, length }, None) => {
                let mut fd = fs::OpenOptions::new().read(true).open(file)?;
                Self::read(&mut fd, fpos, length as usize)
            }
        }
    }

    pub(crate) fn flush(self, fd: &mut fs::File, buf: &mut Vec<u8>) -> Result<Value<V>> {
        match self {
            Value::Native { value } => Self::append(value, fd, buf),
            obj @ Value::Reference { .. } => Ok(obj),
            Value::Backup { .. } => panic!("impossible situation"),
        }
    }

    fn read(fd: &mut fs::File, fpos: u64, ln: usize) -> Result<Value<V>> {
        let mut buf = Vec::with_capacity(ln);
        buf.resize(ln, 0);
        fd.seek(io::SeekFrom::Start(fpos + 8))?;
        let n = fd.read(&mut buf)?;
        if n == ln {
            let mut value: V = Default::default();
            value.decode(&buf)?;
            Ok(Value::Native { value })
        } else {
            Err(BognError::PartialRead(ln, n))
        }
    }

    fn append(value: V, fd: &mut fs::File, buf: &mut Vec<u8>) -> Result<Value<V>> {
        let fpos = fd.metadata()?.len();
        buf.resize(0, 0);
        value.encode(buf);
        let length = buf.len();
        let scratch = ((length as u64) | Self::VALUE_FLAG).to_be_bytes();
        let total_len = length + scratch.len();

        // TODO: can we avoid 2 writes ?
        let mut n = fd.write(&scratch)?;
        n += fd.write(buf)?;
        if n != total_len {
            Err(BognError::PartialWrite(total_len, n))
        } else {
            Ok(Value::Reference {
                fpos,
                length: length as u64,
            })
        }
    }
}

// *-----*------------------------------------*
// |flags|        60-bit length               |
// *-----*------------------------------------*
// |                 payload                  |
// *-------------------*----------------------*
//
// bit 60 shall be clear.

#[derive(Clone)]
pub(crate) enum Delta<V>
where
    V: Default + Diff,
{
    Native {
        delta: <V as Diff>::D,
    },
    Reference {
        fpos: u64, // point to value payload in vlog-file
        length: u64,
    },
    Backup {
        file: ffi::OsString, // must be a vlog file name, with full path
        fpos: u64,           // point to value payload in vlog-file
        length: u64,
    }, // points to entry on disk.
}

impl<V> Delta<V>
where
    V: Default + Diff,
{
    pub(crate) fn new_native(delta: <V as Diff>::D) -> Delta<V> {
        Delta::Native { delta }
    }

    pub(crate) fn new_reference(fpos: u64, length: u64) -> Delta<V> {
        Delta::Reference { fpos, length }
    }

    pub(crate) fn new_backup(file: ffi::OsString, fpos: u64, length: u64) -> Delta<V> {
        Delta::Backup { file, fpos, length }
    }

    pub fn append_to(
        self,
        fd: &mut fs::File,
        buf: &mut Vec<u8>, /* reusable buffer*/
    ) -> Result<Delta<V>> {
        match self {
            Delta::Native { delta } => {
                let fpos = fd.metadata()?.len();
                buf.resize(0, 0);
                delta.encode(buf);
                let length = buf.len() as u64;
                let scratch = length.to_be_bytes();

                let total_len = length + (scratch.len() as u64);
                let mut n = fd.write(&scratch)?;
                n += fd.write(buf)?;
                if (n as u64) != total_len {
                    Err(BognError::PartialWrite(total_len as usize, n))
                } else {
                    Ok(Delta::Reference { fpos, length })
                }
            }
            obj @ Delta::Reference { .. } => Ok(obj),
            Delta::Backup { .. } => panic!("impossible situation"),
        }
    }

    pub(crate) fn to_native(self, fd: Option<&mut fs::File>) -> Result<Delta<V>> {
        match (self, fd) {
            (obj @ Delta::Native { .. }, _) => Ok(obj),
            (Delta::Reference { fpos, length }, Some(fd)) => Self::read(fd, fpos, length as usize),
            (Delta::Reference { .. }, None) => panic!("invalid call !!"),
            (Delta::Backup { fpos, length, .. }, Some(fd)) => Self::read(fd, fpos, length as usize),
            (Delta::Backup { file, fpos, length }, None) => {
                let mut fd = fs::OpenOptions::new().read(true).open(file)?;
                Self::read(&mut fd, fpos, length as usize)
            }
        }
    }

    pub(crate) fn flush(self, fd: &mut fs::File, buf: &mut Vec<u8>) -> Result<Delta<V>> {
        match self {
            Delta::Native { delta } => Self::append(delta, fd, buf),
            obj @ Delta::Reference { .. } => Ok(obj),
            Delta::Backup { .. } => panic!("impossible situation"),
        }
    }

    fn read(fd: &mut fs::File, fpos: u64, ln: usize) -> Result<Delta<V>> {
        let mut buf = Vec::with_capacity(ln);
        buf.resize(ln, 0);
        fd.seek(io::SeekFrom::Start(fpos + 8))?;
        let n = fd.read(&mut buf)?;
        if n == ln {
            let mut delta: <V as Diff>::D = Default::default();
            delta.decode(&buf)?;
            Ok(Delta::Native { delta })
        } else {
            Err(BognError::PartialRead(ln, n))
        }
    }

    fn append(delta: <V as Diff>::D, fd: &mut fs::File, buf: &mut Vec<u8>) -> Result<Delta<V>> {
        let fpos = fd.metadata()?.len();
        buf.resize(0, 0);
        delta.encode(buf);
        let length = buf.len();
        let scratch = (length as u64).to_be_bytes();
        let total_len = length + scratch.len();

        // TODO: can we avoid 2 writes ?
        let mut n = fd.write(&scratch)?;
        n += fd.write(buf)?;
        if n != total_len {
            Err(BognError::PartialWrite(total_len, n))
        } else {
            Ok(Delta::Reference {
                fpos,
                length: length as u64,
            })
        }
    }
}
