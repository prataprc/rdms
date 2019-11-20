use crate::core::{Bloom, Result};

pub struct NoBitmap;

impl<K> Bloom<K> for NoBitmap {
    #[inline]
    fn create() -> Self {
        NoBitmap
    }

    #[inline]
    fn len(&self) -> usize {
        0
    }

    #[inline]
    fn add(&self, _element: &K) {
        // Do nothing.
    }

    #[inline]
    fn contains(&self, _element: &K) -> bool {
        true // false positives are okay.
    }

    #[inline]
    fn to_vec(&self) -> Vec<u8> {
        vec![]
    }

    #[inline]
    fn from_vec(_buf: &[u8]) -> Result<Self> {
        Ok(NoBitmap)
    }
}
