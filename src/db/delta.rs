use cbordata::Cborize;

const DELTA_VER: u32 = 0x00030001;

/// Delta type, describe the older-versions of an indexed entry.
#[derive(Clone, Debug, Eq, PartialEq, Cborize)]
pub enum Delta<D> {
    U { delta: D, seqno: u64 },
    D { seqno: u64 },
}

impl<D> Delta<D> {
    pub const ID: u32 = DELTA_VER;

    pub fn to_seqno(&self) -> u64 {
        match self {
            Delta::U { seqno, .. } => *seqno,
            Delta::D { seqno } => *seqno,
        }
    }
}
