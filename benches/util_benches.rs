#![feature(test)]
extern crate test;

use rand::prelude::random;
use test::Bencher;

#[bench]
fn bench_1_kb(b: &mut Bencher) {
    let src: Vec<u8> = (0..1024).map(|x| x as u8).collect();
    let mut dst: Vec<u8> = Vec::with_capacity(src.len());
    dst.resize(dst.capacity(), 0);
    b.iter(|| dst.copy_from_slice(&src));
}

#[bench]
fn bench_1_mb(b: &mut Bencher) {
    let src: Vec<u8> = (0..(1024 * 1024)).map(|x| x as u8).collect();
    let mut dst: Vec<u8> = Vec::with_capacity(src.len());
    dst.resize(dst.capacity(), 0);
    b.iter(|| dst.copy_from_slice(&src));
}

#[bench]
fn bench_random_64(b: &mut Bencher) {
    b.iter(|| busy_loop(1000));
}

#[bench]
fn bench_atomicptr(b: &mut Bencher) {
    let ptr = Box::leak(Box::new(10_u32));
    let val = std::sync::atomic::AtomicPtr::<u32>::new(ptr);
    b.iter(|| {
        val.store(ptr, std::sync::atomic::Ordering::SeqCst);
        val.load(std::sync::atomic::Ordering::SeqCst);
    });
}

fn busy_loop(count: usize) -> u64 {
    let acc: u64 = (0..count).map(|_| random::<u32>() as u64).sum();
    acc
}
