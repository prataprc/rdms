use cbordata::Cborize;

/// This value must change only when the shape of Value type changes. High 16-bits
/// identify the type and lower 16-bits identify the version.
pub const VALUE_VER: u32 = 0x00020001;

/// Value type, describe the value part of each entry withing a indexed data-set
#[derive(Clone, Debug, Eq, PartialEq, Cborize)]
pub enum Value<V> {
    U { value: V, seqno: u64 },
    D { seqno: u64 },
}

impl<V> Value<V> {
    pub const ID: u32 = VALUE_VER;

    pub fn set(&mut self, value: V, seqno: u64) {
        *self = Value::U { value, seqno };
    }

    pub fn delete(&mut self, seqno: u64) {
        *self = Value::D { seqno };
    }
}

impl<V> Value<V> {
    pub fn to_seqno(&self) -> u64 {
        match self {
            Value::U { seqno, .. } => *seqno,
            Value::D { seqno } => *seqno,
        }
    }

    pub fn is_deleted(&self) -> bool {
        match self {
            Value::U { .. } => false,
            Value::D { .. } => true,
        }
    }
}
