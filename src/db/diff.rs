//! Module define [Diff] trait and implement the trait for native rust types.
//!
//! [NoDiff] can be used for implementing [Diff] trait on user-defined value types
//! Refer [NoDiff] for detail.

use cbordata::Cborize;

// TODO: give a new type number for high 16-bits.
const NDIFF_VER: u32 = 0x00070001;

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
pub trait Diff: Sized + Clone + From<<Self as Diff>::Delta> {
    type Delta: Clone + From<Self>;

    /// Return the delta between two consecutive versions of a value.
    /// `Delta = New - Old`.
    fn diff(&self, old: &Self) -> Self::Delta;

    /// Merge delta with newer version to return older version of the value.
    /// `Old = New - Delta`.
    fn merge(&self, delta: &Self::Delta) -> Self;
}

/// Associated type for value-type that don't implement [Diff] trait, i.e
/// whereever applicable, use NoDiff as delta type.
#[derive(Clone, Default, Debug, Eq, PartialEq, Cborize)]
pub struct NoDiff;

impl NoDiff {
    pub const ID: u32 = NDIFF_VER;
}

#[cfg(test)]
#[path = "diff_test.rs"]
mod diff_test;
