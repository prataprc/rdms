#![feature(test)]
extern crate test;

use croaring::bitmap::Bitmap;
use rand::prelude::random;
use test::Bencher;

use std::time;

#[bench]
fn bench_croaring_add(b: &mut Bencher) {
    let mut bmap = Bitmap::create();
    let mut n = 1;
    b.iter(|| {
        bmap.add(n);
        n += 1
    });
}

#[bench]
fn bench_croaring_contains(b: &mut Bencher) {
    let mut bmap = Bitmap::create();
    let start = time::SystemTime::now();
    let count = 100_000_000;
    for _i in 0..count {
        let n: u32 = random();
        bmap.add(n)
    }
    let elapsed = start.elapsed().unwrap().as_nanos();
    println!(
        "elapsed {} to add {} items, footprint={}",
        elapsed,
        count,
        bmap.cardinality()
    );

    let mut n = 1;
    b.iter(|| {
        bmap.contains(1000);
        n += 1
    });
}
