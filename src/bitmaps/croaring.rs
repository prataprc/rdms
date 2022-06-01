//! Module `croaring` implement the [Bloom] trait for [roaring bitmap][roaring-bitmap].
//!
//! [Bloom]: crate::dbs::Bloom
//! [roaring-bitmap]: https://roaringbitmap.org

use croaring::bitmap::Bitmap;

use std::{
    convert::TryInto,
    hash::{Hash, Hasher},
};

use crate::{dbs::Bloom, hash::CityHasher, Error, Result};

// TODO: right now we are using cityhash-rs, make hasher generic.

pub struct CRoaring {
    bitmap: Bitmap,
}

impl Default for CRoaring {
    fn default() -> CRoaring {
        CRoaring::new()
    }
}

impl CRoaring {
    pub fn new() -> CRoaring {
        CRoaring { bitmap: Bitmap::create() }
    }
}

impl Bloom for CRoaring {
    #[inline]
    fn len(&self) -> Result<usize> {
        err_at!(FailConvert, self.bitmap.cardinality().try_into())
    }

    #[inline]
    fn add_key<Q: ?Sized + Hash>(&mut self, element: &Q) {
        let mut hasher = CityHasher::default();
        element.hash(&mut hasher);
        let code: u64 = hasher.finish();
        let digest = (((code >> 32) ^ code) & 0xFFFFFFFF) as u32;

        self.add_digest32(digest);
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
        let mut hasher = CityHasher::default();

        element.hash(&mut hasher);
        let code: u64 = hasher.finish();
        let digest = (((code >> 32) ^ code) & 0xFFFFFFFF) as u32;

        self.bitmap.contains(digest)
    }

    #[inline]
    fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(self.bitmap.serialize())
    }

    #[inline]
    fn from_bytes(buf: &[u8]) -> Result<(CRoaring, usize)> {
        let val = CRoaring { bitmap: Bitmap::deserialize(buf) };
        let n = buf.len();
        Ok((val, n))
    }

    #[inline]
    fn or(&self, other: &CRoaring) -> Result<CRoaring> {
        Ok(CRoaring { bitmap: self.bitmap.or(&other.bitmap) })
    }
}

#[cfg(test)]
#[path = "croaring_test.rs"]
mod croaring_test;
