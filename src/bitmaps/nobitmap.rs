//! Module `nobitmap` define a dummy bitmap index.

use std::hash::Hash;

use crate::{dbs, Result};

#[derive(Default, Clone)]
pub struct NoBitmap;

impl dbs::Bloom for NoBitmap {
    #[inline]
    fn len(&self) -> Result<usize> {
        Ok(0)
    }

    #[inline]
    fn add_key<Q: ?Sized + Hash>(&mut self, _key: &Q) {
        // Do nothing
    }

    #[inline]
    fn add_keys<Q: Hash>(&mut self, _keys: &[Q]) {
        // Do nothing
    }

    #[inline]
    fn add_digest32(&mut self, _digest: u32) {
        // Do nothing
    }

    #[inline]
    fn add_digests32(&mut self, _digests: &[u32]) {
        // Do nothing
    }

    #[inline]
    fn add_digest64(&mut self, _digest: u64) {
        // Do nothing
    }

    #[inline]
    fn add_digests64(&mut self, _digests: &[u64]) {
        // Do nothing
    }

    #[inline]
    fn build(&mut self) -> Result<()> {
        Ok(())
    }

    #[inline]
    fn contains<Q: ?Sized + Hash>(&self, _element: &Q) -> bool {
        true // false positives are okay.
    }

    #[inline]
    fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(vec![])
    }

    #[inline]
    fn from_bytes(_buf: &[u8]) -> Result<(NoBitmap, usize)> {
        Ok((NoBitmap, 0))
    }

    #[inline]
    fn or(&self, _other: &NoBitmap) -> Result<NoBitmap> {
        Ok(NoBitmap)
    }
}
