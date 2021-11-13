use cbordata::Cborize;

use crate::{dbs::Footprint, Error, Result};

const DELTA_VER: u32 = 0x00030001;

/// Delta type, describe the older-versions of an indexed entry.
#[derive(Clone, Debug, Eq, PartialEq, Cborize)]
pub enum Delta<D> {
    U { delta: D, seqno: u64 },
    D { seqno: u64 },
}

impl<D> Footprint for Delta<D>
where
    D: Footprint,
{
    /// Return the previous versions of this entry as Deltas.
    fn footprint(&self) -> Result<isize> {
        use std::{convert::TryFrom, mem::size_of};

        let mut size = {
            err_at!(FailConvert, isize::try_from(size_of::<Delta<D>>()))?
                - err_at!(FailConvert, isize::try_from(size_of::<D>()))?
        };

        size += match self {
            Delta::U { delta, .. } => delta.footprint()?,
            Delta::D { .. } => 0,
        };

        Ok(size)
    }
}

impl<D> Delta<D> {
    pub const ID: u32 = DELTA_VER;

    #[inline]
    pub fn new_upsert(delta: D, seqno: u64) -> Delta<D> {
        Delta::U { delta, seqno }
    }

    #[inline]
    pub fn new_delete(seqno: u64) -> Delta<D> {
        Delta::D { seqno }
    }
}

impl<D> Delta<D> {
    #[inline]
    pub fn to_seqno(&self) -> u64 {
        match self {
            Delta::U { seqno, .. } => *seqno,
            Delta::D { seqno } => *seqno,
        }
    }

    #[inline]
    pub fn to_delta(&self) -> Option<D>
    where
        D: Clone,
    {
        match self {
            Delta::U { delta, .. } => Some(delta.clone()),
            Delta::D { .. } => None,
        }
    }

    #[inline]
    pub fn unpack(&self) -> (u64, Option<D>)
    where
        D: Clone,
    {
        match self {
            Delta::U { delta, seqno } => (*seqno, Some(delta.clone())),
            Delta::D { seqno } => (*seqno, None),
        }
    }
}

#[cfg(test)]
#[path = "delta_test.rs"]
mod delta_test;
