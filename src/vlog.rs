use std::{convert::TryInto, fs, mem};

use crate::{
    core::{self, Diff, Footprint, Result, Serialize},
    error::Error,
    util,
};

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
    // Native value.
    Native { value: V },
    // Refers to serialized value on disk, either index-file or vlog-file
    Reference { fpos: u64, length: u64, seqno: u64 },
}

impl<V> Value<V> {
    const VALUE_FLAG: u64 = 0x1000000000000000;

    pub(crate) fn new_native(value: V) -> Value<V> {
        Value::Native { value }
    }

    pub(crate) fn new_reference(fpos: u64, len: u64, seqno: u64) -> Value<V> {
        Value::Reference {
            fpos,
            length: len,
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

    pub(crate) fn to_reference(&self) -> Option<(u64, u64, u64)> {
        match self {
            Value::Reference {
                fpos,
                length,
                seqno,
            } => Some((*fpos, *length, *seqno)),
            _ => None,
        }
    }

    pub(crate) fn is_reference(&self) -> bool {
        match self {
            Value::Reference { .. } => true,
            _ => false,
        }
    }
}

impl<V> Footprint for Value<V>
where
    V: Footprint,
{
    fn footprint(&self) -> Result<isize> {
        match self {
            Value::Native { value } => value.footprint(),
            Value::Reference { .. } => Ok(0),
        }
    }
}

impl<V> Value<V>
where
    V: Serialize,
{
    // Return the size of header + payload.
    pub(crate) fn encode(&self, buf: &mut Vec<u8>) -> Result<usize> {
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

    // not meant for disk serialization, only value is encoded.
    pub(crate) fn encode_local(&self, buf: &mut Vec<u8>) -> Result<usize> {
        match self {
            Value::Native { value } => {
                let vlen = value.encode(buf);
                if vlen > core::Entry::<i32, i32>::VALUE_SIZE_LIMIT {
                    return Err(Error::ValueSizeExceeded(vlen));
                };
                Ok(vlen)
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
    pub(crate) fn to_native_delta(&self) -> Option<<V as Diff>::D> {
        match self {
            Delta::Native { diff } => Some(diff.clone()),
            _ => None,
        }
    }

    pub(crate) fn into_native_delta(self) -> Option<<V as Diff>::D> {
        match self {
            Delta::Native { diff } => Some(diff),
            _ => None,
        }
    }
}

impl<V> Footprint for Delta<V>
where
    V: Diff,
    <V as Diff>::D: Footprint,
{
    fn footprint(&self) -> Result<isize> {
        match self {
            Delta::Native { diff } => diff.footprint(),
            Delta::Reference { .. } => Ok(0),
        }
    }
}

impl<V> Delta<V>
where
    V: Diff,
    <V as Diff>::D: Serialize,
{
    pub(crate) fn encode(&self, buf: &mut Vec<u8>) -> Result<usize> {
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

#[cfg(test)]
#[path = "vlog_test.rs"]
mod vlog_test;
