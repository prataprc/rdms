#![feature(test)]
extern crate test;

use test::Bencher;

use bogn::Gate;

#[bench]
fn bench_acquire_read(b: &mut Bencher) {
    let g = Gate::new();
    b.iter(|| g.acquire_read(false));
}

#[bench]
fn bench_acquire_write(b: &mut Bencher) {
    let g = Gate::new();
    b.iter(|| g.acquire_write(false));
}
