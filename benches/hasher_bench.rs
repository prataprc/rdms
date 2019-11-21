#![feature(test)]
extern crate test;

use std::hash::Hasher;

use test::Bencher;

#[bench]
fn bench_default_hasher(b: &mut Bencher) {
    use std::collections::hash_map::DefaultHasher;

    let mut n: u64 = 1;
    let mut sum = 0;
    b.iter(|| {
        let mut hasher = DefaultHasher::new();
        hasher.write(&n.to_be_bytes());
        sum += hasher.finish();
        n += 1
    });
}

#[bench]
fn bench_crc32_hasher(b: &mut Bencher) {
    use crc::crc32::{self, Hasher32};

    let mut n: u64 = 1;
    let mut sum = 0;
    let mut digest = crc32::Digest::new(crc32::IEEE);
    b.iter(|| {
        Hasher32::write(&mut digest, &n.to_be_bytes());
        sum += digest.sum32();
        n += 1;
        digest.reset();
    });
}
