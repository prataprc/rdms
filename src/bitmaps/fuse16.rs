use cbordata::{Cbor, FromCbor, IntoCbor};
use xorfilter::Fuse16;

use std::hash::{BuildHasher, Hash};

use crate::{db, Error, Result};

impl<H> db::Bloom for Fuse16<H>
where
    H: Clone + BuildHasher + From<Vec<u8>> + Into<Vec<u8>>,
{
    fn len(&self) -> Result<usize> {
        match self.len() {
            Some(n) => Ok(n),
            None => err_at!(NotImplemented, msg: "Fuse16 does not implement length"),
        }
    }

    fn add_key<Q: ?Sized + Hash>(&mut self, key: &Q) {
        self.insert(key)
    }

    fn add_digest32(&mut self, digest: u32) {
        self.populate_keys(&[u64::from(digest)])
    }

    fn build(&mut self) -> Result<()> {
        err_at!(Fatal, self.build())
    }

    fn contains<Q: ?Sized + Hash>(&self, element: &Q) -> bool {
        self.contains(element)
    }

    fn to_bytes(&self) -> Result<Vec<u8>> {
        let cbor_val = err_at!(FailCbor, self.clone().into_cbor())?;
        let mut buf: Vec<u8> = vec![];
        err_at!(FailCbor, cbor_val.encode(&mut buf))?;

        Ok(buf)
    }

    fn from_bytes(mut buf: &[u8]) -> Result<(Self, usize)> {
        let (cbor_val, n) = err_at!(IOError, Cbor::decode(&mut buf))?;
        Ok((err_at!(FailCbor, Fuse16::<H>::from_cbor(cbor_val))?, n))
    }

    fn or(&self, _other: &Self) -> Result<Self> {
        err_at!(NotImplemented, msg: "Fuse16 does not implement or() method")
    }
}

#[cfg(test)]
#[path = "fuse16_test.rs"]
mod fuse16_test;