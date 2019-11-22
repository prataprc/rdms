//! Module `nobitmap` define a dummy bitmap index.

use crate::core::{Bloom, Result};

pub struct NoBitmap;

impl Bloom for NoBitmap {
    #[inline]
    fn create() -> Self {
        NoBitmap
    }

    #[inline]
    fn len(&self) -> usize {
        0
    }

    #[inline]
    fn add_key<Q: ?Sized>(&mut self, _element: &Q) {
        // Do nothing.
    }

    #[inline]
    fn add_digest32(&mut self, _digest: u32) {
        // Do nothing.
    }

    #[inline]
    fn contains<Q: ?Sized>(&self, _element: &Q) -> bool {
        true // false positives are okay.
    }

    #[inline]
    fn to_vec(&self) -> Vec<u8> {
        vec![]
    }

    #[inline]
    fn from_vec(_buf: &[u8]) -> Result<NoBitmap> {
        Ok(NoBitmap)
    }
}
