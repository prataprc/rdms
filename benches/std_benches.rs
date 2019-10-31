#![feature(test)]
extern crate test;

use std::time::SystemTime;
use test::Bencher;

#[bench]
fn bench_systemtime_now(b: &mut Bencher) {
    b.iter(|| SystemTime::now());
}

#[bench]
fn bench_systemtime_elapsed(b: &mut Bencher) {
    let now = SystemTime::now();
    b.iter(|| now.elapsed());
}
