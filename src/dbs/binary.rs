use cbordata::Cborize;

use std::{convert::TryFrom, hash::Hash};

use crate::{dbs, Error, Result};

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

impl dbs::Diff for Binary {
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

impl dbs::Footprint for Binary {
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

impl rand::distributions::Distribution<Binary> for rand::distributions::Standard {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> Binary {
        let (val, size) = (rng.gen::<u64>(), rng.gen::<usize>() % 1024);
        Binary {
            val: format!("{:0width$}", val, width = size).as_bytes().to_vec(),
        }
    }
}
