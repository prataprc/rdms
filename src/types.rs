//! Module `types` implement core key-traits and value-traits for
//! built-in types.

use std::{convert::TryInto, ffi, marker};

use crate::{
    core::{Diff, Entry, Footprint, Result, Serialize},
    error::Error,
};

// TODO: instead of using resize()/copy_from_slice() can we use
// extend_from_slice() ?

/// Empty value, can be used for indexing entries that have a
/// key but no value.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, PartialOrd, Ord)]
pub struct Empty;

//-------------------------------------------------------------------

impl Diff for Empty {
    type D = Empty;

    /// D = C - P
    fn diff(&self, _a: &Self) -> Self::D {
        Empty
    }

    /// P = C - D
    fn merge(&self, _a: &Self::D) -> Self {
        Empty
    }
}

impl Serialize for Empty {
    fn encode(&self, _buf: &mut Vec<u8>) -> usize {
        0
    }

    fn decode(&mut self, _buf: &[u8]) -> Result<usize> {
        Ok(0)
    }
}

impl Footprint for Empty {
    fn footprint(&self) -> Result<isize> {
        Ok(0)
    }
}

impl From<ffi::OsString> for Empty {
    fn from(_: ffi::OsString) -> Empty {
        Empty
    }
}

//-------------------------------------------------------------------

// TODO: implement test case for this.
impl Diff for [u8; 20] {
    type D = [u8; 20];

    /// D = C - P
    fn diff(&self, old: &Self) -> Self::D {
        old.clone()
    }

    /// P = C - D
    fn merge(&self, delta: &Self::D) -> Self {
        delta.clone()
    }
}

impl Serialize for [u8; 20] {
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        let m = buf.len();
        buf.resize(m + 20, 0);
        buf[m..(m + 20)].copy_from_slice(self);
        20
    }

    fn decode(&mut self, buf: &[u8]) -> Result<usize> {
        self.copy_from_slice(buf);
        Ok(20)
    }
}

impl Footprint for [u8; 20] {
    fn footprint(&self) -> Result<isize> {
        Ok(0)
    }
}

//-------------------------------------------------------------------

impl Diff for Vec<u8> {
    type D = Vec<u8>;

    /// D = C - P
    fn diff(&self, old: &Self) -> Self::D {
        old.clone()
    }

    /// P = C - D
    fn merge(&self, delta: &Self::D) -> Self {
        delta.clone()
    }
}

// 4 byte header, encoding the length of payload followed by
// the actual payload.
impl Serialize for Vec<u8> {
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        let hdr1: u32 = self.len().try_into().unwrap();
        let scratch = hdr1.to_be_bytes();

        let mut m = buf.len();
        buf.resize(m + scratch.len() + self.len(), 0);
        buf[m..m + scratch.len()].copy_from_slice(&scratch);
        m += scratch.len();
        buf[m..(m + self.len())].copy_from_slice(self);
        scratch.len() + self.len()
    }

    fn decode(&mut self, buf: &[u8]) -> Result<usize> {
        if buf.len() < 4 {
            let msg = format!("type-bytes decode header {} < 4", buf.len());
            return Err(Error::DecodeFail(msg));
        }
        let len: usize = u32::from_be_bytes(buf[..4].try_into().unwrap())
            .try_into()
            .unwrap();
        if buf.len() < (len + 4) {
            let msg = format!("type-bytes decode payload {} < {}", buf.len(), len);
            return Err(Error::DecodeFail(msg));
        }
        self.resize(len, 0);
        self.copy_from_slice(&buf[4..len + 4]);
        Ok(len + 4)
    }
}

impl Footprint for Vec<u8> {
    fn footprint(&self) -> Result<isize> {
        Ok(self.capacity().try_into().unwrap())
    }
}

//-------------------------------------------------------------------

impl Diff for i32 {
    type D = i32;

    /// D = C - P
    fn diff(&self, old: &Self) -> Self::D {
        old.clone()
    }

    /// P = C - D
    fn merge(&self, delta: &Self::D) -> Self {
        delta.clone()
    }
}

impl Serialize for i32 {
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        let m = buf.len();
        buf.resize(m + 4, 0);
        buf[m..m + 4].copy_from_slice(&self.to_be_bytes());
        4
    }

    fn decode(&mut self, buf: &[u8]) -> Result<usize> {
        if buf.len() >= 4 {
            let mut scratch = [0_u8; 4];
            scratch.copy_from_slice(&buf[..4]);
            *self = i32::from_be_bytes(scratch);
            Ok(4)
        } else {
            let msg = format!("type-i32 encoded len {}", buf.len());
            Err(Error::DecodeFail(msg))
        }
    }
}

impl Footprint for i32 {
    fn footprint(&self) -> Result<isize> {
        Ok(0)
    }
}

//-------------------------------------------------------------------

impl Diff for i64 {
    type D = i64;

    /// D = C - P
    fn diff(&self, old: &Self) -> Self::D {
        old.clone()
    }

    /// P = C - D
    fn merge(&self, delta: &Self::D) -> Self {
        delta.clone()
    }
}

impl Serialize for i64 {
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        let n = buf.len();
        buf.resize(n + 8, 0);
        buf[n..n + 8].copy_from_slice(&self.to_be_bytes());
        8
    }

    fn decode(&mut self, buf: &[u8]) -> Result<usize> {
        if buf.len() >= 8 {
            let mut scratch = [0_u8; 8];
            scratch.copy_from_slice(&buf[..8]);
            *self = i64::from_be_bytes(scratch);
            Ok(8)
        } else {
            let msg = format!("type-i64 encoded len {}", buf.len());
            Err(Error::DecodeFail(msg))
        }
    }
}

impl Footprint for i64 {
    fn footprint(&self) -> Result<isize> {
        Ok(0)
    }
}

//-------------------------------------------------------------------

pub(crate) struct EmptyIter<'a, K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    pub(crate) _phantom_key: &'a marker::PhantomData<K>,
    pub(crate) _phantom_val: &'a marker::PhantomData<V>,
}

impl<'a, K, V> Iterator for EmptyIter<'a, K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

//-------------------------------------------------------------------

#[cfg(test)]
#[path = "types_test.rs"]
mod types_test;
