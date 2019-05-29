use crate::core::{Diff, Serialize};

pub(crate) const VALUE_FLAG: u64 = 0x1000000000000000;

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
pub(crate) enum Value<V>
where
    V: Default + Serialize,
{
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

impl<V> Value<V>
where
    V: Default + Serialize,
{
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
    V: Default + Diff,
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
    V: Default + Diff,
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
}
