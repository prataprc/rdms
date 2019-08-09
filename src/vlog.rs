use std::convert::TryInto;
use std::{fs, mem};

use crate::core::{self, Diff, Footprint, Result, Serialize};
use crate::error::Error;
use crate::util;

// *-----*------------------------------------*
// |flags|        60-bit length               |
// *-----*------------------------------------*
// |                 payload                  |
// *-------------------*----------------------*
//
// Flags:
// * bit 60 shall be set.
// * bit 61 reserved
// * bit 62 reserved
// * bit 63 reserved

#[derive(Clone)]
pub(crate) enum Value<V> {
    // Native value, already de-serialized.
    Native { value: V },
    // Refers to serialized value on disk, either index-file or vlog-file
    Reference { fpos: u64, length: u64, seqno: u64 },
}

impl<V> Value<V> {
    const VALUE_FLAG: u64 = 0x1000000000000000;

    pub(crate) fn new_native(value: V) -> Value<V> {
        Value::Native { value }
    }

    pub(crate) fn new_reference(fpos: u64, length: u64, seqno: u64) -> Value<V> {
        Value::Reference {
            fpos,
            length,
            seqno,
        }
    }
}

impl<V> Value<V>
where
    V: Clone,
{
    pub(crate) fn to_native_value(&self) -> Option<V> {
        match self {
            Value::Native { value } => Some(value.clone()),
            _ => None,
        }
    }
}

impl<V> Value<V>
where
    V: Clone + Footprint,
{
    pub(crate) fn value_footprint(&self) -> isize {
        match self {
            Value::Native { value } => value.footprint(),
            Value::Reference { .. } => 0,
        }
    }
}

impl<V> Value<V>
where
    V: Serialize,
{
    pub(crate) fn encode(&self, buf: &mut Vec<u8>) -> Result<usize>
    where
        V: Serialize,
    {
        match self {
            Value::Native { value } => {
                let m = buf.len();
                buf.resize(m + 8, 0);

                let vlen = {
                    let vlen = value.encode(buf);
                    if vlen > core::Entry::<i32, i32>::VALUE_SIZE_LIMIT {
                        return Err(Error::ValueSizeExceeded(vlen));
                    };
                    vlen
                };

                let mut hdr1: u64 = vlen.try_into().unwrap();
                hdr1 |= Value::<V>::VALUE_FLAG;
                buf[m..m + 8].copy_from_slice(&hdr1.to_be_bytes());

                Ok(vlen + 8)
            }
            _ => Err(Error::NotNativeValue),
        }
    }
}

pub(crate) fn fetch_value<V>(fpos: u64, n: u64, fd: &mut fs::File) -> Result<Value<V>>
where
    V: Serialize,
{
    let block = util::read_buffer(fd, fpos, n, "reading value from vlog")?;
    let mut value: V = unsafe { mem::zeroed() };
    value.decode(&block[8..])?;
    Ok(Value::new_native(value))
}

// *-----*------------------------------------*
// |flags|        60-bit length               |
// *-----*------------------------------------*
// |                 payload                  |
// *-------------------*----------------------*
//
// Flags:
// * bit 60 shall be clear.
// * bit 61 reserved
// * bit 62 reserved
// * bit 63 reserved

#[derive(Clone)]
pub(crate) enum Delta<V>
where
    V: Diff,
{
    // Native diff, already de-serialized.
    Native { diff: <V as Diff>::D },
    // Refers to serialized diff on disk, either index-file or vlog-file
    Reference { fpos: u64, length: u64, seqno: u64 },
}

impl<V> Delta<V>
where
    V: Diff,
{
    pub(crate) fn new_native(diff: <V as Diff>::D) -> Delta<V> {
        Delta::Native { diff }
    }

    pub(crate) fn new_reference(fpos: u64, length: u64, seqno: u64) -> Delta<V> {
        Delta::Reference {
            fpos,
            length,
            seqno,
        }
    }
}

impl<V> Delta<V>
where
    V: Diff,
{
    pub(crate) fn into_native_delta(self) -> Option<<V as Diff>::D> {
        match self {
            Delta::Native { diff } => Some(diff),
            _ => None,
        }
    }

    pub(crate) fn diff_footprint(&self) -> isize {
        match self {
            Delta::Native { diff } => diff.footprint(),
            Delta::Reference { .. } => 0,
        }
    }
}

impl<V> Delta<V>
where
    V: Diff,
{
    pub(crate) fn encode(&self, buf: &mut Vec<u8>) -> Result<usize>
    where
        V: Diff,
        <V as Diff>::D: Serialize,
    {
        match self {
            Delta::Native { diff } => {
                let m = buf.len();
                buf.resize(m + 8, 0);

                let dlen = {
                    let dlen = diff.encode(buf);
                    if dlen > core::Entry::<i32, i32>::DIFF_SIZE_LIMIT {
                        return Err(Error::DiffSizeExceeded(dlen));
                    };
                    dlen
                };

                let hdr1: u64 = dlen.try_into().unwrap();
                buf[m..m + 8].copy_from_slice(&hdr1.to_be_bytes());

                Ok(dlen + 8)
            }
            _ => Err(Error::NotNativeDelta),
        }
    }
}

pub(crate) fn fetch_delta<V>(fpos: u64, n: u64, fd: &mut fs::File) -> Result<Delta<V>>
where
    V: Diff,
    <V as Diff>::D: Serialize,
{
    let block = util::read_buffer(fd, fpos, n, "reading delta from vlog")?;
    let mut delta: <V as Diff>::D = unsafe { mem::zeroed() };
    delta.decode(&block[8..])?;
    Ok(Delta::new_native(delta))
}
