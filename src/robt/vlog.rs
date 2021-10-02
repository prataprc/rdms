use cbordata::{Cborize, FromCbor, IntoCbor};

use std::{convert::TryFrom, io};

use crate::{db, err_at, read_file, util, Error, Result};

const VALUE_VER: u32 = 0x000d0001;
const DELTA_VER: u32 = 0x00110001;

#[derive(Clone, Debug, Eq, PartialEq, Cborize)]
pub enum Value<V> {
    N { value: db::Value<V> },
    R { fpos: u64, length: u64 },
}

impl<V> Value<V> {
    const ID: u32 = VALUE_VER;
}

impl<V> From<db::Value<V>> for Value<V> {
    fn from(value: db::Value<V>) -> Value<V> {
        Value::N { value }
    }
}

impl<V> TryFrom<Value<V>> for db::Value<V> {
    type Error = Error;

    fn try_from(value: Value<V>) -> Result<db::Value<V>> {
        let value = match value {
            Value::N { value } => value,
            Value::R { .. } => err_at!(
                FailConvert, msg: "robt::Value is reference, can't convert to db::Value"
            )?,
        };

        Ok(value)
    }
}

impl<V> Value<V> {
    pub fn into_reference(self, fpos: u64) -> Result<(Self, Vec<u8>)>
    where
        V: IntoCbor,
    {
        let (value, data) = match self {
            Value::N { value } => {
                let data = util::into_cbor_bytes(value)?;
                let length = err_at!(FailConvert, u64::try_from(data.len()))?;
                (Value::R { fpos, length }, data)
            }
            val @ Value::R { .. } => (val, vec![]),
        };

        Ok((value, data))
    }

    pub fn into_native<F>(self, f: &mut F) -> Result<Self>
    where
        F: io::Seek + io::Read,
        V: FromCbor,
    {
        let value = match self {
            Value::N { .. } => self,
            Value::R { fpos, length } => {
                let seek = io::SeekFrom::Start(fpos);
                let block = read_file!(f, seek, length, "reading value from vlog")?;
                let value = util::from_cbor_bytes(&block)?.0;
                Value::N { value }
            }
        };

        Ok(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Cborize)]
pub enum Delta<D> {
    N { delta: db::Delta<D> },
    R { fpos: u64, length: u64 },
}

impl<D> Delta<D> {
    const ID: u32 = DELTA_VER;
}

impl<D> From<db::Delta<D>> for Delta<D> {
    fn from(delta: db::Delta<D>) -> Delta<D> {
        Delta::N { delta }
    }
}

impl<D> TryFrom<Delta<D>> for db::Delta<D> {
    type Error = Error;

    fn try_from(delta: Delta<D>) -> Result<db::Delta<D>> {
        let delta = match delta {
            Delta::N { delta } => delta,
            Delta::R { .. } => err_at!(
                FailConvert, msg: "robt::Delta is reference, can't convert to db::Value"
            )?,
        };

        Ok(delta)
    }
}

impl<D> Delta<D> {
    pub fn into_reference(self, fpos: u64) -> Result<(Self, Vec<u8>)>
    where
        D: IntoCbor,
    {
        match self {
            Delta::N { delta } => {
                let data = util::into_cbor_bytes(delta)?;
                let length = err_at!(FailConvert, u64::try_from(data.len()))?;
                Ok((Delta::R { fpos, length }, data))
            }
            val @ Delta::R { .. } => Ok((val, vec![])),
        }
    }

    pub fn into_native<F>(self, f: &mut F) -> Result<Self>
    where
        F: io::Seek + io::Read,
        D: FromCbor,
    {
        match self {
            Delta::N { .. } => Ok(self),
            Delta::R { fpos, length } => {
                let seek = io::SeekFrom::Start(fpos);
                let block = read_file!(f, seek, length, "reading delta from vlog")?;
                let delta = util::from_cbor_bytes(&block)?.0;
                Ok(Delta::N { delta })
            }
        }
    }
}

#[cfg(test)]
#[path = "vlog_test.rs"]
mod vlog_test;
