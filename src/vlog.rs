use std::convert::TryInto;

use crate::core::{self, Diff, Serialize};
use crate::error::Error;

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

    #[allow(dead_code)] // TODO: remove this after wiring with bogn.
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
    #[allow(dead_code)] // TODO: remove this after wiring with bogn.
    pub(crate) fn into_native_value(self) -> Option<V> {
        match self {
            Value::Native { value } => Some(value),
            _ => None,
        }
    }

    pub(crate) fn to_native_value(&self) -> Option<V> {
        match self {
            Value::Native { value } => Some(value.clone()),
            _ => None,
        }
    }
}

impl<V> Value<V>
where
    V: Serialize,
{
    pub(crate) fn encode(&self, buf: &mut Vec<u8>) -> Result<usize, Error>
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

    #[allow(dead_code)] // TODO: remove this after wiring with bogn.
    pub(crate) fn new_reference(fpos: u64, length: u64, seqno: u64) -> Delta<V> {
        Delta::Reference {
            fpos,
            length,
            seqno,
        }
    }

    pub(crate) fn into_native_delta(self) -> Option<<V as Diff>::D> {
        match self {
            Delta::Native { diff } => Some(diff),
            _ => None,
        }
    }
}

impl<V> Delta<V>
where
    V: Diff,
{
    pub(crate) fn encode(&self, buf: &mut Vec<u8>) -> Result<usize, Error>
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
