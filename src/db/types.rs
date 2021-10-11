//! Implement Diff, Footprint traits for native types and std-types.

use cbordata::Cborize;
use std::{convert::TryFrom, hash::Hash};

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
                    err_at!(FailConvert, isize::try_from(size_of::<$type>()))
                }
            }
        )*
    );
}

impl_footprint_basic_types![
    bool, i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize, f32, f64, char
];

impl<T> Footprint for Vec<T>
where
    T: Footprint,
{
    fn footprint(&self) -> Result<isize> {
        use std::mem::size_of;

        let mut size = err_at!(
            FailConvert,
            isize::try_from(size_of::<Vec<T>>() + self.capacity())
        )?;

        for item in self.iter() {
            size += item.footprint()?
        }

        Ok(size)
    }
}

impl Footprint for String {
    fn footprint(&self) -> Result<isize> {
        use std::mem::size_of;
        Ok(err_at!(
            FailConvert,
            isize::try_from(size_of::<String>() + self.capacity())
        )?)
    }
}

const BINARY_VER: u32 = 0x00170001_u32;

#[derive(Clone, Default, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, Cborize)]
pub struct Binary {
    pub val: Vec<u8>,
}

impl Binary {
    const ID: u32 = BINARY_VER;
}

impl ToString for Binary {
    fn to_string(&self) -> String {
        std::str::from_utf8(&self.val).unwrap().to_string()
    }
}

impl Diff for Binary {
    type Delta = Self;

    fn diff(&self, old: &Self) -> Self::Delta {
        Binary {
            val: old.val.to_vec(),
        }
    }

    fn merge(&self, delta: &Self::Delta) -> Self {
        Binary {
            val: delta.val.to_vec(),
        }
    }
}

impl Footprint for Binary {
    fn footprint(&self) -> Result<isize> {
        use std::mem::size_of;
        let size = size_of::<Binary>() + self.val.capacity();
        err_at!(FailConvert, isize::try_from(size))
    }
}

impl<'a> arbitrary::Arbitrary<'a> for Binary {
    fn arbitrary(u: &mut arbitrary::Unstructured) -> arbitrary::Result<Self> {
        let size = u.arbitrary::<usize>()? % 1024;
        let val = match u.arbitrary::<u64>()? {
            0 => Binary::default(),
            val => Binary {
                val: format!("{:0width$}", val, width = size).as_bytes().to_vec(),
            },
        };
        Ok(val)
    }
}

#[cfg(any(test, feature = "rand"))]
impl rand::distributions::Distribution<Binary> for rand::distributions::Standard {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> Binary {
        let (val, size) = (rng.gen::<u64>(), rng.gen::<usize>() % 1024);
        Binary {
            val: format!("{:0width$}", val, width = size).as_bytes().to_vec(),
        }
    }
}

#[cfg(test)]
#[path = "types_test.rs"]
mod types_test;
