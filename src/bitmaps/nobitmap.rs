//! Module `nobitmap` define a dummy bitmap index.

use std::hash::Hash;

use crate::{db, Result};

#[derive(Default)]
pub struct NoBitmap;

impl db::Bloom for NoBitmap {
    #[inline]
    fn len(&self) -> Result<usize> {
        Ok(0)
    }

    #[inline]
    fn add_key<Q: ?Sized + Hash>(&mut self, _element: &Q) {
        // Do nothing.
    }

    #[inline]
    fn add_digest32(&mut self, _digest: u32) {
        // Do nothing.
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
