//! Module `croaring` implement the [Bloom] trait for
//! [roaring bitmap][roaring-bitmap].
//!
//! [Bloom]: crate::core::Bloom
//! [roaring-bitmap]: https://roaringbitmap.org

use crc::crc32::{self, Hasher32};
use croaring::bitmap::Bitmap;

use std::{convert::TryInto, hash::Hash};

use crate::core::{Bloom, Result};

// TODO: right now we are using crc32, make hasher generic.

pub struct CRoaring {
    hasher: crc32::Digest,
    bitmap: Bitmap,
}

impl Bloom for CRoaring {
    #[inline]
    fn create() -> Self {
        CRoaring {
            hasher: crc32::Digest::new(crc32::IEEE),
            bitmap: Bitmap::create(),
        }
    }

    #[inline]
    fn len(&self) -> Result<usize> {
        Ok(self.bitmap.cardinality().try_into()?)
    }

    #[inline]
    fn add_key<Q: ?Sized + Hash>(&mut self, element: &Q) {
        self.hasher.reset();
        element.hash(&mut self.hasher);
        self.add_digest32(self.hasher.sum32());
    }

    #[inline]
    fn add_digest32(&mut self, digest: u32) {
        self.bitmap.add(digest)
    }

    #[inline]
    fn contains<Q: ?Sized + Hash>(&self, element: &Q) -> bool {
        let mut hasher = crc32::Digest::new(crc32::IEEE);
        element.hash(&mut hasher);
        self.bitmap.contains(hasher.sum32())
    }

    #[inline]
    fn to_vec(&self) -> Vec<u8> {
        self.bitmap.serialize()
    }

    #[inline]
    fn from_vec(buf: &[u8]) -> Result<CRoaring> {
        Ok(CRoaring {
            hasher: crc32::Digest::new(crc32::IEEE),
            bitmap: Bitmap::deserialize(buf),
        })
    }

    #[inline]
    fn or(&self, other: &CRoaring) -> Result<CRoaring> {
        Ok(CRoaring {
            hasher: crc32::Digest::new(crc32::IEEE),
            bitmap: self.bitmap.or(&other.bitmap),
        })
    }
}
