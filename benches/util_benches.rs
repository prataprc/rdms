#![feature(test)]
extern crate test;

use test::Bencher;

use bogn::Gate;

#[bench]
fn bench_1_K(b: &mut Bencher) {
    let src: Vec<u8> = (0..1024).map(|x| x as u8).collect();
    let mut dst: Vec<u8> = Vec::with_capacity(src.len());
    dst.resize(dst.capacity(), 0);
    b.iter(|| dst.copy_from_slice(&src));
}

#[bench]
fn bench_1_M(b: &mut Bencher) {
    let src: Vec<u8> = (0..(1024 * 1024)).map(|x| x as u8).collect();
    let mut dst: Vec<u8> = Vec::with_capacity(src.len());
    dst.resize(dst.capacity(), 0);
    b.iter(|| dst.copy_from_slice(&src));
}
