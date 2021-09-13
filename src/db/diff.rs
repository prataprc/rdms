use cbordata::{Cborize, FromCbor, IntoCbor};

// TODO: give a new type number for high 16-bits.
const NDIFF_VER: u32 = 0x0001;

/// Trait for diff-able values.
///
/// Version control is a necessary feature for non-destructive writes.
/// Using this trait it is possible to generate concise older versions as
/// deltas. Note that this version control follows centralized behavior, as
/// apposed to distributed behavior, for which we need three-way-merge.
///
/// If,
/// ```notest
/// P = old value; C = new value; D = difference between P and C
/// ```
///
/// Then,
/// ```notest
/// D = C - P (diff operation)
/// P = C - D (merge operation, to get old value)
/// ```
pub trait Diff: Sized + From<<Self as Diff>::Delta> {
    type Delta: Clone + From<Self> + FromCbor + IntoCbor;

    /// Return the delta between two consecutive versions of a value.
    /// `Delta = New - Old`.
    fn diff(&self, old: &Self) -> Self::Delta;

    /// Merge delta with newer version to return older version of the value.
    /// `Old = New - Delta`.
    fn merge(&self, delta: &Self::Delta) -> Self;
}

/// Associated type for value-type that don't implement [Diff] trait, i.e
/// whereever applicable, use NoDiff as delta type.
#[derive(Clone, Default, Debug, Cborize)]
pub struct NoDiff;

impl NoDiff {
    pub const ID: u32 = NDIFF_VER;
}

macro_rules! impl_diff_basic_types {
    ($($type:ident),*) => (
        $(
            impl Diff for $type {
                type Delta = $type;

                fn diff(&self, old: &$type) -> Self::Delta {
                    *old
                }

                fn merge(&self, delta: &Self::Delta) -> Self {
                    *delta
                }
            }
        )*
    );
}

// TODO: implement Diff for all Rust native types - char, f32, f64, u128, i128 and others
impl_diff_basic_types![bool, i8, i16, i32, i64, isize, u8, u16, u32, u64, usize];
