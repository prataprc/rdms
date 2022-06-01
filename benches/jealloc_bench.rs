#![feature(test)]
extern crate test;

use test::Bencher;

use std::{
    alloc::{GlobalAlloc, Layout},
    mem,
    sync::{mpsc, Arc},
    thread,
    time::SystemTime,
};

struct Node {
    _field1: [u8; 12],
    _field2: [u8; 24],
    _field3: [u8; 48],
    _field4: [u8; 20],
}

#[bench]
fn bench_je_alloc(b: &mut Bencher) {
    let je = jemallocator::Jemalloc;

    let start = SystemTime::now();
    let n = 1_000_000;
    for _i in 0..n {
        unsafe { je.alloc(Layout::new::<Node>()) };
    }
    println!("took {:?} to allocate {} blocks", start.elapsed().unwrap(), n);

    b.iter(|| unsafe { je.alloc(Layout::new::<Node>()) });
}

#[bench]
fn bench_je_alloc_free(b: &mut Bencher) {
    let je = jemallocator::Jemalloc;
    let start = SystemTime::now();
    let n = 1_000_000;
    for _i in 0..n {
        unsafe {
            let lt = Layout::new::<Node>();
            let ptr = je.alloc(lt);
            je.dealloc(ptr, lt);
        }
    }
    println!("took {:?} to allocate/free {} blocks", start.elapsed().unwrap(), n);

    b.iter(|| unsafe {
        let lt = Layout::new::<Node>();
        let ptr = je.alloc(lt);
        je.dealloc(ptr, lt);
    });
}

#[bench]
fn bench_je_alloc_cc2(b: &mut Bencher) {
    let je_tx = Arc::new(jemallocator::Jemalloc);
    let je_rx = Arc::clone(&je_tx);

    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let lt = Layout::new::<Node>();
        for ptr in rx {
            let ptr = Box::leak(ptr);
            unsafe { je_rx.dealloc(ptr, lt) };
        }
    });

    let start = SystemTime::now();
    let n = 1_000_000;
    for _i in 0..n {
        unsafe {
            let lt = Layout::new::<Node>();
            tx.send(Box::from_raw(je_tx.alloc(lt))).unwrap();
        }
    }
    mem::drop(tx);

    println!("took {:?} to allocate {} blocks", start.elapsed().unwrap(), n);
    handle.join().unwrap();
    println!("took {:?} to free {} blocks", start.elapsed().unwrap(), n);

    let je = jemallocator::Jemalloc;
    b.iter(|| unsafe {
        let lt = Layout::new::<Node>();
        let ptr = je.alloc(lt);
        je.dealloc(ptr, lt);
    });
}
