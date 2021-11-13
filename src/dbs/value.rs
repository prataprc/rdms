use cbordata::Cborize;

use crate::{dbs::Footprint, Error, Result};

/// This value must change only when the shape of Value type changes. High 16-bits
/// identify the type and lower 16-bits identify the version.
pub const VALUE_VER: u32 = 0x00020001;

/// Value type, describe the value part of each entry withing a indexed data-set
#[derive(Clone, Debug, Eq, PartialEq, Cborize)]
pub enum Value<V> {
    U { value: V, seqno: u64 },
    D { seqno: u64 },
}

impl<V> Footprint for Value<V>
where
    V: Footprint,
{
    fn footprint(&self) -> Result<isize> {
        use std::{convert::TryFrom, mem::size_of};

        let mut size = {
            err_at!(FailConvert, isize::try_from(size_of::<Value<V>>()))?
                - err_at!(FailConvert, isize::try_from(size_of::<V>()))?
        };

        size += match self {
            Value::U { value, .. } => value.footprint()?,
            Value::D { .. } => 0,
        };

        Ok(size)
    }
}

impl<V> Value<V> {
    pub const ID: u32 = VALUE_VER;

    #[inline]
    pub fn new_upsert(value: V, seqno: u64) -> Self {
        Value::U { value, seqno }
    }

    #[inline]
    pub fn new_delete(seqno: u64) -> Self {
        Value::D { seqno }
    }
}

impl<V> Value<V> {
    #[inline]
    pub fn to_seqno(&self) -> u64 {
        match self {
            Value::U { seqno, .. } => *seqno,
            Value::D { seqno } => *seqno,
        }
    }

    #[inline]
    pub fn is_deleted(&self) -> bool {
        match self {
            Value::U { .. } => false,
            Value::D { .. } => true,
        }
    }

    #[inline]
    pub fn to_value(&self) -> Option<V>
    where
        V: Clone,
    {
        match self {
            Value::U { value, .. } => Some(value.clone()),
            Value::D { .. } => None,
        }
    }

    #[inline]
    pub fn unpack(&self) -> (u64, Option<V>)
    where
        V: Clone,
    {
        match self {
            Value::U { value, seqno } => (*seqno, Some(value.clone())),
            Value::D { seqno } => (*seqno, None),
        }
    }
}

#[cfg(test)]
#[path = "value_test.rs"]
mod value_test;
