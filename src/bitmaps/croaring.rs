//! Module `croaring` implement the [Bloom] trait for [roaring bitmap][roaring-bitmap].
//!
//! [Bloom]: crate::db::Bloom
//! [roaring-bitmap]: https://roaringbitmap.org

use crc::crc32::{self, Hasher32};
use croaring::bitmap::Bitmap;

use std::{convert::TryInto, hash::Hash};

use crate::{db::Bloom, Error, Result};

// TODO: right now we are using crc32, make hasher generic.

pub struct CRoaring {
    hasher: crc32::Digest,
    bitmap: Bitmap,
}

impl CRoaring {
    pub fn new() -> CRoaring {
        CRoaring {
            hasher: crc32::Digest::new(crc32::IEEE),
            bitmap: Bitmap::create(),
        }
    }
}

impl Bloom for CRoaring {
    #[inline]
    fn len(&self) -> Result<usize> {
        err_at!(FailConvert, self.bitmap.cardinality().try_into())
    }

    #[inline]
    fn add_key<Q: ?Sized + Hash>(&mut self, element: &Q) {
        self.hasher.reset();
        element.hash(&mut self.hasher);
        self.add_digest32(self.hasher.sum32());
    }

    #[inline]
    fn add_keys<Q: Hash>(&mut self, keys: &[Q]) {
        for key in keys.iter() {
            self.add_key(key)
        }
    }

    #[inline]
    fn add_digest32(&mut self, digest: u32) {
        self.bitmap.add(digest)
    }

    #[inline]
    fn add_digests32(&mut self, digests: &[u32]) {
        self.bitmap.add_many(digests)
    }

    #[inline]
    fn add_digest64(&mut self, digest: u64) {
        let digest = ((digest >> 32) ^ (digest & 0xFFFFFFFF)) as u32;
        self.bitmap.add(digest)
    }

    #[inline]
    fn add_digests64(&mut self, digests: &[u64]) {
        for digest in digests.iter() {
            self.add_digest64(*digest)
        }
    }

    #[inline]
    fn build(&mut self) -> Result<()> {
        Ok(())
    }

    #[inline]
    fn contains<Q: ?Sized + Hash>(&self, element: &Q) -> bool {
        let mut hasher = crc32::Digest::new(crc32::IEEE);
        element.hash(&mut hasher);
        self.bitmap.contains(hasher.sum32())
    }

    #[inline]
    fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(self.bitmap.serialize())
    }

    #[inline]
    fn from_bytes(buf: &[u8]) -> Result<(CRoaring, usize)> {
        let val = CRoaring {
            hasher: crc32::Digest::new(crc32::IEEE),
            bitmap: Bitmap::deserialize(buf),
        };
        let n = buf.len();
        Ok((val, n))
    }

    #[inline]
    fn or(&self, other: &CRoaring) -> Result<CRoaring> {
        Ok(CRoaring {
            hasher: crc32::Digest::new(crc32::IEEE),
            bitmap: self.bitmap.or(&other.bitmap),
        })
    }
}

#[cfg(test)]
#[path = "croaring_test.rs"]
mod croaring_test;
