use std::{
    fs, io,
    io::{Read, Seek, Write},
};

use crate::error::BognError;
use crate::traits::{Diff, Serialize};

// *-----*------------------------------------*
// |flags|        60-bit length               |
// *-----*------------------------------------*
// |                 payload                  |
// *-------------------*----------------------*
//
// bit 60 shall be set.

#[derive(Clone, Default)]
pub struct Value<V>
where
    V: Default + Clone + Diff + Serialize,
{
    file: String,
    fpos: u64,
    length: usize,
    value: Option<V>,
}

impl<V> Value<V>
where
    V: Default + Clone + Diff + Serialize,
{
    const VALUE_FLAG: u64 = 0x1000000000000000;

    #[inline]
    fn value_offset(&self) -> u64 {
        self.fpos + 8
    }

    pub fn new_value(value: V) -> Value<V> {
        let mut v: Value<V> = Default::default();
        v.value = Some(value);
        v
    }

    pub fn new_ref(file: String, fpos: u64, length: usize) -> Value<V> {
        Value {
            file,
            fpos,
            length,
            value: None,
        }
    }

    pub fn fetch(self, fd: Option<&mut fs::File>) -> Result<Value<V>, BognError> {
        if self.value.is_some() {
            // nothing to do.
            return Ok(self);
        }

        let mut buf = Vec::with_capacity(self.length);
        buf.resize(self.length, 0);

        let n = match fd {
            Some(fd) => {
                fd.seek(io::SeekFrom::Start(self.value_offset()))?;
                fd.read(&mut buf)?
            }
            None => {
                let mut fd = fs::File::open(&self.file)?;
                fd.seek(io::SeekFrom::Start(self.value_offset()))?;
                fd.read(&mut buf)?
            }
        };

        if n == self.length {
            let mut new_self = self.clone();
            new_self.value.as_mut().unwrap().decode(&buf)?;
            Ok(new_self)
        } else {
            Err(BognError::PartialRead(self.length, n))
        }
    }

    pub fn value(&self) -> Result<V, BognError> {
        match &self.value {
            Some(value) => Ok(value.clone()),
            None => panic!("call fetch() before value()"),
        }
    }

    pub fn disk_append(
        self,
        fd: &mut fs::File,
        buf: &mut Vec<u8>, /* reusable buffer */
    ) -> Result<Value<V>, BognError> {
        match self.value {
            Some(value) => {
                buf.resize(0, 0);
                value.encode(buf);
                let len = buf.len() as u64;
                let scratch = (len | Self::VALUE_FLAG).to_be_bytes();

                let mut n = fd.write(&scratch)?;
                n += fd.write(buf)?;
                if n != (buf.len() + scratch.len()) {
                    Err(BognError::PartialWrite(buf.len() + scratch.len(), n))
                } else {
                    Ok(Value {
                        file: Default::default(),
                        fpos: fd.metadata()?.len(),
                        length: buf.len(),
                        value: Some(value),
                    })
                }
            }
            _ => Ok(self),
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

#[derive(Clone, Default)]
pub struct Delta<V>
where
    V: Default + Clone + Diff + Serialize,
{
    file: String,
    fpos: u64,
    length: usize,
    delta: Option<<V as Diff>::D>,
}

impl<V> Delta<V>
where
    V: Default + Clone + Diff + Serialize,
{
    #[inline]
    fn delta_offset(&self) -> u64 {
        self.fpos + 8
    }

    pub fn new_delta(delta: <V as Diff>::D) -> Delta<V> {
        let mut d: Delta<V> = Default::default();
        d.delta = Some(delta);
        d
    }

    pub fn new_ref(file: String, fpos: u64, length: usize) -> Delta<V> {
        Delta {
            file,
            fpos,
            length,
            delta: None,
        }
    }

    pub fn fetch(self, fd: Option<&mut fs::File>) -> Result<Delta<V>, BognError> {
        if self.delta.is_some() {
            // nothing to do.
            return Ok(self);
        }

        let mut buf = Vec::with_capacity(self.length);
        buf.resize(self.length, 0);

        let n = match fd {
            Some(fd) => {
                fd.seek(io::SeekFrom::Start(self.delta_offset()))?;
                fd.read(&mut buf)?
            }
            None => {
                let mut fd = fs::File::open(&self.file)?;
                fd.seek(io::SeekFrom::Start(self.delta_offset()))?;
                fd.read(&mut buf)?
            }
        };

        if n != self.length {
            let mut new_self = self.clone();
            new_self.delta.as_mut().unwrap().decode(&buf)?;
            Ok(new_self)
        } else {
            Err(BognError::PartialRead(self.length, n))
        }
    }

    pub fn delta(&self) -> Result<<V as Diff>::D, BognError> {
        match &self.delta {
            Some(delta) => Ok(delta.clone()),
            None => panic!("call fetch() before delta()"),
        }
    }

    pub fn disk_append(
        self,
        fd: &mut fs::File,
        buf: &mut Vec<u8>, /* reusable buffer*/
    ) -> Result<Delta<V>, BognError> {
        match self.delta {
            Some(delta) => {
                buf.resize(0, 0);
                delta.encode(buf);
                let scratch = (buf.len() as u64).to_be_bytes();

                let mut n = fd.write(&scratch)?;
                n += fd.write(buf)?;
                if n != (buf.len() + scratch.len()) {
                    Err(BognError::PartialWrite(buf.len() + scratch.len(), n))
                } else {
                    Ok(Delta {
                        file: Default::default(),
                        fpos: fd.metadata()?.len(),
                        length: buf.len(),
                        delta: Some(delta),
                    })
                }
            }
            None => Ok(self),
        }
    }
}
