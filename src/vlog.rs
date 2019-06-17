// TODO: There are dead code meant for future use case.

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
    Native {
        value: V,
    },
    // Refers to serialized value on disk, either index-file or vlog-file
    Reference {
        fpos: u64,
        length: u64,
    },
    // Refers to serialized value on disk, either index-file or vlog-file.
    Backup {
        file: String,
        fpos: u64,
        length: u64,
    },
}

impl<V> Value<V> {
    const VALUE_FLAG: u64 = 0x1000000000000000;

    pub(crate) fn new_native(value: V) -> Value<V> {
        Value::Native { value }
    }

    #[allow(dead_code)]
    pub(crate) fn new_reference(fpos: u64, length: u64) -> Value<V> {
        Value::Reference { fpos, length }
    }

    #[allow(dead_code)]
    pub(crate) fn new_backup(file: &str, fpos: u64, length: u64) -> Value<V> {
        Value::Backup {
            file: file.to_string(),
            fpos,
            length,
        }
    }

    #[allow(dead_code)] // TODO: remove this once bogn is weaved-up.
    pub(crate) fn into_native(self) -> Option<V> {
        match self {
            Value::Native { value } => Some(value),
            _ => None,
        }
    }
}

pub(crate) fn encode_value<V>(
    value: &Value<V>, // encode if native value
    buf: &mut Vec<u8>,
) -> Result<usize, Error>
where
    V: Serialize,
{
    use crate::util::try_convert_int;

    match value {
        Value::Native { value } => {
            let m = buf.len();
            let n = value.encode(buf);
            buf.resize(m + n + 8, 0);
            buf.copy_within(m..n, m + 8);

            let mut vlen: u64 = try_convert_int(n + 8, "value-size: usize->u64")?;
            vlen |= Value::<V>::VALUE_FLAG;
            (&mut buf[m..m + 8]).copy_from_slice(&(vlen - 8).to_be_bytes());

            if n < core::Entry::<i32, i32>::VALUE_SIZE_LIMIT {
                Ok(n)
            } else {
                Err(Error::ValueSizeExceeded(n))
            }
        }
        _ => Err(Error::NotNativeValue),
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
// TODO: figure out a way to make this crate private
pub enum Delta<V>
where
    V: Diff,
{
    // Native diff, already de-serialized.
    Native {
        diff: <V as Diff>::D,
    },
    // Refers to serialized diff on disk, either index-file or vlog-file
    Reference {
        fpos: u64,
        length: u64,
    },
    // Refers to serialized value on disk, either index-file or vlog-file.
    Backup {
        file: String,
        fpos: u64,
        length: u64,
    },
}

impl<V> Delta<V>
where
    V: Diff,
{
    pub(crate) fn new_native(diff: <V as Diff>::D) -> Delta<V> {
        Delta::Native { diff }
    }

    #[allow(dead_code)]
    pub(crate) fn new_reference(fpos: u64, length: u64) -> Delta<V> {
        Delta::Reference { fpos, length }
    }

    #[allow(dead_code)]
    pub(crate) fn new_backup(file: String, fpos: u64, length: u64) -> Delta<V> {
        Delta::Backup { file, fpos, length }
    }

    pub(crate) fn into_native(self) -> Option<<V as Diff>::D> {
        match self {
            Delta::Native { diff } => Some(diff),
            _ => None,
        }
    }
}

pub(crate) fn encode_delta<V>(
    delta: &Delta<V>, // encode if native diff
    buf: &mut Vec<u8>,
) -> Result<usize, Error>
where
    V: Diff,
    <V as Diff>::D: Serialize,
{
    use crate::util::try_convert_int;

    match delta {
        Delta::Native { diff } => {
            let m = buf.len();
            let n = diff.encode(buf);
            buf.resize(m + n + 8, 0);
            buf.copy_within(m..n, m + 8);

            let dlen: u64 = try_convert_int(n + 8, "diff-size: usize->u64")?;
            (&mut buf[m..m + 8]).copy_from_slice(&(dlen - 8).to_be_bytes());

            if n < core::Entry::<i32, i32>::DIFF_SIZE_LIMIT {
                Ok(n)
            } else {
                Err(Error::DiffSizeExceeded(n))
            }
        }
        _ => Err(Error::NotNativeDelta),
    }
}
