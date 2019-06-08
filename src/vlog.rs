// TODO: There are dead code meant for future use case.

use crate::core::Diff;

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
    pub(crate) const VALUE_FLAG: u64 = 0x1000000000000000;

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
    // Native delta, already de-serialized.
    Native {
        delta: <V as Diff>::D,
    },
    // Refers to serialized delta on disk, either index-file or vlog-file
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
    pub(crate) fn new_native(delta: <V as Diff>::D) -> Delta<V> {
        Delta::Native { delta }
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
            Delta::Native { delta } => Some(delta),
            _ => None,
        }
    }
}
