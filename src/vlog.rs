use std::{
    fs, io,
    io::{Read, Seek, Write},
};

use crate::error::BognError;
use crate::traits::{Diff, Serialize};

#[derive(Clone)]
pub enum Value<V>
where
    V: Default + Clone + Diff + Serialize,
{
    Native { value: V },
    Disk { fpos: u64, length: usize },
}

impl<V> Value<V>
where
    V: Default + Clone + Diff + Serialize,
{
    pub fn to_native(self, fd: &mut fs::File) -> Result<Value<V>, BognError> {
        match self {
            Value::Disk { fpos, length } => {
                let value = Self::disk_read_value(fd, fpos, length)?;
                Ok(Value::Native { value })
            }
            obj => Ok(obj),
        }
    }

    pub fn into_value(self, fd: &mut fs::File) -> Result<V, BognError> {
        match self {
            Value::Disk {
                fpos: fpos,
                length: length,
            } => Self::disk_read_value(fd, fpos, length),
            Value::Native { value } => Ok(value.clone()),
        }
    }

    pub fn value(self) -> V {
        match self {
            Value::Native { value } => value,
            _ => panic!("convert Value<V> to Native variant"),
        }
    }

    pub fn disk_append(
        &self,
        fd: &mut fs::File, /* file to append */
    ) -> Result<Value<V>, BognError> {
        match self {
            Value::Native { value } => Self::disk_write_value(fd, value),
            obj => Ok(obj.clone()),
        }
    }

    // *-----*------------------------------------*
    // |flags|        60-bit length               |
    // *-----*------------------------------------*
    // |                 payload                  |
    // *-------------------*----------------------*
    fn disk_read_value(
        fd: &mut fs::File,
        fpos: u64,
        length: usize, /* length of serialized value */
    ) -> Result<V, BognError>
    where
        V: Default + Clone + Serialize + Diff,
    {
        let mut buf = Vec::with_capacity(length);
        buf.resize(length, 0);

        fd.seek(io::SeekFrom::Start(fpos));
        let n = fd.read(&mut buf)?;
        if n != length {
            Err(BognError::PartialRead(length, n))
        } else {
            let mut value: V = Default::default();
            value.decode(&buf)?;
            Ok(value)
        }
    }

    fn disk_write_value(
        fd: &mut fs::File, /*file to append*/
        value: &V,
    ) -> Result<Value<V>, BognError>
    where
        V: Default + Clone + Serialize + Diff,
    {
        let fpos = fd.metadata()?.len();

        let buf = value.encode(Vec::with_capacity(1024)); /* TODO: magic num */
        let n = fd.write(&buf)?;
        if n != buf.len() {
            Err(BognError::PartialWrite(buf.len(), n))
        } else {
            Ok(Value::Disk {
                fpos,
                length: buf.len(),
            })
        }
    }
}

#[derive(Clone)]
pub enum Delta<V>
where
    V: Default + Clone + Diff + Serialize,
{
    Native {
        delta: <V as Diff>::D,
    },
    Disk {
        file: String,
        fpos: u64,
        length: usize,
    },
}

impl<V> Delta<V>
where
    V: Default + Clone + Diff + Serialize,
{
    pub fn to_native(
        self,
        fd: Option<&mut fs::File>, /* file is None, then Native::file used */
    ) -> Result<Delta<V>, BognError> {
        match (self, fd) {
            (Delta::Disk { file, fpos, length }, Some(fd)) => {
                let delta = Self::disk_read_delta(fd, fpos, length)?;
                Ok(Delta::Native { delta })
            }
            (Delta::Disk { file, fpos, length }, None) => {
                let mut fd = fs::File::open(file)?;
                let delta = Self::disk_read_delta(&mut fd, fpos, length)?;
                Ok(Delta::Native { delta })
            }
            (obj, _) => Ok(obj),
        }
    }

    pub fn into_delta(
        self,
        fd: Option<&mut fs::File>, /* file is None, then Native::file used */
    ) -> Result<<V as Diff>::D, BognError> {
        match (self, fd) {
            (Delta::Disk { file, fpos, length }, Some(fd)) => {
                let delta = Self::disk_read_delta(fd, fpos, length)?;
                Ok(delta)
            }
            (Delta::Disk { file, fpos, length }, None) => {
                let mut fd = fs::File::open(file)?;
                let delta = Self::disk_read_delta(&mut fd, fpos, length)?;
                Ok(delta)
            }
            (Delta::Native { delta }, _) => Ok(delta),
        }
    }

    pub fn delta(self) -> <V as Diff>::D {
        match self {
            Delta::Native { delta } => delta,
            _ => panic!("convert Delta<V> to Native variant"),
        }
    }

    pub fn disk_append(&self, file: &mut fs::File) -> Result<Self, BognError> {
        match self {
            Delta::Native { delta } => Self::disk_write_delta(file, delta),
            obj => Ok(obj.clone()),
        }
    }

    // *-----*------------------------------------*
    // |flags|        60-bit length               |
    // *-----*------------------------------------*
    // |                 payload                  |
    // *-------------------*----------------------*
    fn disk_read_delta(
        fd: &mut fs::File,
        fpos: u64,
        length: usize, /* length of serialized delta */
    ) -> Result<<V as Diff>::D, BognError>
    where
        V: Default + Clone + Serialize + Diff,
    {
        let mut buf = Vec::with_capacity(length);
        buf.resize(length, 0);

        fd.seek(io::SeekFrom::Start(fpos));
        let n = fd.read(&mut buf)?;
        if n != length {
            Err(BognError::PartialRead(length, n))
        } else {
            let mut delta: <V as Diff>::D = Default::default();
            delta.decode(&buf)?;
            Ok(delta)
        }
    }

    fn disk_write_delta(
        fd: &mut fs::File, /*file to append data*/
        delta: &<V as Diff>::D,
    ) -> Result<Delta<V>, BognError>
    where
        V: Default + Clone + Serialize + Diff,
    {
        let fpos = fd.metadata()?.len();

        let buf = delta.encode(Vec::with_capacity(1024)); /* TODO: magic num */
        let n = fd.write(&buf)?;
        if n != buf.len() {
            Err(BognError::PartialWrite(buf.len(), n))
        } else {
            Ok(Delta::Disk {
                file: "caller-shall-fill".to_string(),
                fpos,
                length: buf.len(),
            })
        }
    }
}
