//! Implement Diff, Footprint traits for native types and std-types.

use std::convert::TryFrom;

use crate::{
    db::{Diff, Footprint},
    Error, Result,
};

macro_rules! impl_diff_basic_types {
    ($($type:ty),*) => (
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

// TODO: implement Diff for all Rust native types - char, f32, f64, and others
impl_diff_basic_types![
    bool, i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize
];

macro_rules! impl_footprint_basic_types {
    ($($type:ty),*) => (
        $(
            impl Footprint for $type {
                fn footprint(&self) -> Result<isize> {
                    use std::mem::size_of;
                    err_at!(ConversionFail, isize::try_from(size_of::<$type>()))
                }
            }
        )*
    );
}

impl_footprint_basic_types![
    bool, i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize, f32, f64, char
];

impl<T> Footprint for Vec<T> {
    fn footprint(&self) -> Result<isize> {
        use std::mem::size_of;
        Ok(err_at!(
            ConversionFail,
            isize::try_from(size_of::<Vec<T>>() + self.capacity())
        )?)
    }
}

impl Footprint for String {
    fn footprint(&self) -> Result<isize> {
        use std::mem::size_of;
        Ok(err_at!(
            ConversionFail,
            isize::try_from(size_of::<String>() + self.capacity())
        )?)
    }
}

#[cfg(test)]
#[path = "types_test.rs"]
mod types_test;
