#![feature(test)]
extern crate test;

use test::Bencher;

#[bench]
fn bench_default_hasher(b: &mut Bencher) {
    use std::{collections::hash_map::DefaultHasher, hash::Hasher};

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
    let mut n: u64 = 1;
    let mut sum = 0;
    let val = crc::Crc::<u32>::new(&crc::CRC_32_CKSUM);
    b.iter(|| {
        sum += val.checksum(&n.to_be_bytes());
        n += 1;
    });
}
