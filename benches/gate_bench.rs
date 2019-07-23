#![feature(test)]
extern crate test;

use test::Bencher;

use bogn::RWSpinlock;

#[bench]
fn bench_acquire_read(b: &mut Bencher) {
    let g = RWSpinlock::new();
    b.iter(|| g.acquire_read(false));
}

#[bench]
fn bench_acquire_write(b: &mut Bencher) {
    let g = RWSpinlock::new();
    b.iter(|| g.acquire_write(false));
}
