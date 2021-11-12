#![feature(test)]
extern crate test;

use test::Bencher;

use rdms::util::spinlock::Spinlock;

#[bench]
fn bench_spinlock_read(b: &mut Bencher) {
    let g = Spinlock::new(0);
    b.iter(|| g.read());
}

#[bench]
fn bench_spinlock_write(b: &mut Bencher) {
    let g = Spinlock::new(0);
    b.iter(|| g.write());
}
