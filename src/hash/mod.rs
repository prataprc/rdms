use std::hash::{BuildHasher, Hasher};

/// Type uses google's city hash to convert [Hash]able key into ``u64``.
/// Refer [cityhash_rs] for details.
#[derive(Clone, Copy, Default)]
pub struct CityHasher {
    digest: u128,
}

impl CityHasher {
    pub fn new() -> CityHasher {
        CityHasher::default()
    }
}

impl BuildHasher for CityHasher {
    type Hasher = Self;

    #[inline]
    fn build_hasher(&self) -> Self {
        self.clone()
    }
}

impl Hasher for CityHasher {
    fn finish(&self) -> u64 {
        ((self.digest >> 64) as u64) ^ ((self.digest & 0xFFFFFFFFFFFFFFFF) as u64)
    }

    fn write(&mut self, bytes: &[u8]) {
        self.digest = cityhash_rs::cityhash_110_128(bytes);
    }
}
