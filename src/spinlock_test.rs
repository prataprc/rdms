use rand::prelude::random;

use std::{
    sync::Arc,
    {thread, time},
};

use super::*;

// TODO: yield_ok == true

#[test]
fn test_rw_spinlock() {
    let g = Arc::new(RWSpinlock::new());
    let c = Context {
        n_readers: 4,
        n_writers: 4,
        size: 1024,
    };

    let writer = |g: Arc<RWSpinlock>, mut data: Box<Data>, idx: usize, c: Context| {
        let mut res = Vec::with_capacity(c.n_writers);
        res.resize(res.capacity(), 0);

        let start = time::SystemTime::now();
        let value: Vec<u8> = ((idx * c.size)..((idx * c.size) + c.size))
            .map(|x| x as u8)
            .collect();
        while start.elapsed().unwrap().as_secs() < 10 {
            {
                let _w = g.acquire_write(false);
                data.idx = idx;
                data.value.copy_from_slice(&value);
                res[idx] += 1;
            }
        }
        Box::leak(data);

        Rc::Ws(res)
    };

    let reader = |g: Arc<RWSpinlock>, data: Box<Data>, c: Context| {
        let mut res = Vec::with_capacity(std::cmp::max(c.n_writers, 1));
        res.resize(res.capacity(), 0);

        let mut values = vec![];
        (0..res.len()).for_each(|idx| {
            let value: Vec<u8> = ((idx * c.size)..((idx * c.size) + c.size))
                .map(|x| x as u8)
                .collect();
            values.push(value);
        });

        let start = time::SystemTime::now();
        while start.elapsed().unwrap().as_secs() < 10 {
            {
                let _r = g.acquire_read(false);
                assert_eq!(values[data.idx], data.value);
                res[data.idx] += 1;
                busy_loop(25);
            }
        }
        Box::leak(data);

        Rc::Rs(res)
    };

    let mut data: Box<Data> = Box::new(Data::new(c.size));
    data.value.resize(c.size, 0);

    let mut writers = vec![];
    for idx in 0..c.n_writers {
        let arg1 = Arc::clone(&g);
        let arg2 = unsafe { Box::from_raw(data.as_mut() as *mut Data) };
        let (arg3, arg4) = (idx, c.clone());
        writers.push(thread::spawn(move || writer(arg1, arg2, arg3, arg4)));
    }

    let mut readers = vec![];
    for _idx in 0..c.n_readers {
        let arg1 = Arc::clone(&g);
        let arg2 = unsafe { Box::from_raw(data.as_mut() as *mut Data) };
        let arg3 = c.clone();
        readers.push(thread::spawn(move || reader(arg1, arg2, arg3)));
    }

    print_w_res(writers.into_iter().map(|w| w.join().unwrap()).collect());
    print_r_res(readers.into_iter().map(|r| r.join().unwrap()).collect());
    println!("RWSpinlock {}", g);
}

struct Data {
    idx: usize,
    value: Vec<u8>,
}

impl Data {
    fn new(size: usize) -> Data {
        let value: Vec<u8> = (0..size).map(|x| x as u8).collect();
        Data { idx: 0, value }
    }
}

enum Rc {
    Ws(Vec<usize>),
    Rs(Vec<usize>),
}

#[derive(Clone)]
struct Context {
    n_readers: usize,
    n_writers: usize,
    size: usize,
}

fn print_w_res(w_res: Vec<Rc>) {
    for res in w_res {
        match res {
            Rc::Ws(res) => res,
            _ => unreachable!(),
        }
        .iter()
        .enumerate()
        .filter_map(|(i, n)| if *n > 0 { Some((i, *n)) } else { None })
        .into_iter()
        .for_each(|(i, n)| println!("writer {} ops {}", i, n));
    }
}

fn print_r_res(r_res: Vec<Rc>) {
    for (r, res) in r_res.into_iter().enumerate() {
        match res {
            Rc::Rs(res) => res,
            _ => unreachable!(),
        }
        .iter()
        .enumerate()
        .filter_map(|(i, n)| if *n > 0 { Some((i, *n)) } else { None })
        .into_iter()
        .for_each(|(i, n)| println!("reader {} data {} ops {}", r, i, n));
    }
}

fn busy_loop(count: usize) -> u64 {
    let acc: u64 = (0..count).map(|_| random::<u32>() as u64).sum();
    acc
}
