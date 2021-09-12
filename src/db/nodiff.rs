use cbordata::Cborize;

// TODO: give a new type number for high 16-bits.
const NDIFF_VER: u32 = 0x0001;

/// Associated type for value-type that don't implement [Diff] trait, i.e
/// whereever applicable, use NoDiff as delta type.
#[derive(Clone, Default, Debug, Cborize)]
pub struct NoDiff;

impl NoDiff {
    pub const ID: u32 = NDIFF_VER;
}
