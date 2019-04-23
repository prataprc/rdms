use std::{
    fs, io,
    io::{Read, Seek, Write},
};

use crate::core::{Diff, Serialize};
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
    Native { value: V },
    Reference { fpos: u64, length: u64 },
}

impl<V> Value<V>
where
    V: Default + Serialize,
{
    const VALUE_FLAG: u64 = 0x1000000000000000;

    pub fn new_native(value: V) -> Value<V> {
        Value::Native { value }
    }

    pub fn new_reference(fpos: u64, length: u64) -> Value<V> {
        Value::Reference { fpos, length }
    }

    pub fn fetch(self, fd: &mut fs::File) -> Result<Value<V>, BognError> {
        match self {
            Value::Reference { fpos, length } => {
                let offset = (length + 8) as usize;
                let mut buf = Vec::with_capacity(offset);
                buf.resize(offset, 0);
                fd.seek(io::SeekFrom::Start(offset as u64))?;
                let n = fd.read(&mut buf)?;
                if (n as u64) == length {
                    let mut value: V = Default::default();
                    value.decode(&buf)?;
                    Ok(Value::Native { value })
                } else {
                    Err(BognError::PartialRead(length as usize, n))
                }
            }
            obj @ Value::Native { value: _ } => Ok(obj),
        }
    }

    pub fn append_to(
        self,
        fd: &mut fs::File,
        buf: &mut Vec<u8>, /* reuse buffer */
    ) -> Result<Value<V>, BognError> {
        match self {
            Value::Native { value } => {
                let fpos = fd.metadata()?.len();
                buf.resize(0, 0);
                value.encode(buf);
                let length = buf.len() as u64;
                let scratch = (length | Self::VALUE_FLAG).to_be_bytes();

                let total_len = length + (scratch.len() as u64);
                let mut n = fd.write(&scratch)?;
                n += fd.write(buf)?;
                if (n as u64) != total_len {
                    Err(BognError::PartialWrite(total_len as usize, n))
                } else {
                    Ok(Value::Reference { fpos, length })
                }
            }
            obj @ Value::Reference { .. } => Ok(obj),
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
pub enum Delta<V>
where
    V: Default + Diff,
{
    Native { delta: <V as Diff>::D },
    Reference { fpos: u64, length: u64 },
}

impl<V> Delta<V>
where
    V: Default + Diff,
{
    pub fn new_native(delta: <V as Diff>::D) -> Delta<V> {
        Delta::Native { delta }
    }

    pub fn new_reference(fpos: u64, length: u64) -> Delta<V> {
        Delta::Reference { fpos, length }
    }

    pub fn fetch(self, fd: &mut fs::File) -> Result<Delta<V>, BognError> {
        match self {
            Delta::Reference { fpos, length } => {
                let offset = (length + 8) as usize;
                let mut buf = Vec::with_capacity(offset);
                buf.resize(offset, 0);
                fd.seek(io::SeekFrom::Start(offset as u64))?;
                let n = fd.read(&mut buf)?;
                if (n as u64) == length {
                    let mut delta: <V as Diff>::D = Default::default();
                    delta.decode(&buf)?;
                    Ok(Delta::Native { delta })
                } else {
                    Err(BognError::PartialRead(length as usize, n))
                }
            }
            obj @ Delta::Native { delta: _ } => Ok(obj),
        }
    }

    pub fn append_to(
        self,
        fd: &mut fs::File,
        buf: &mut Vec<u8>, /* reusable buffer*/
    ) -> Result<Delta<V>, BognError> {
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
        }
    }
}
