use arbitrary::Unstructured;
use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use super::*;

#[test]
fn test_wral_wal() {
    use crate::wral::state;
    use std::env;

    let seed: u64 =
        [4285628235488288451, 4686907263384396610, random()][random::<usize>() % 3];
    // let seed: u64 = 4686907263384396610;

    let mut rng = SmallRng::seed_from_u64(seed);
    println!("test_wral_wal {}", seed);

    let mut config: Config = {
        let bytes = rng.gen::<[u8; 32]>();
        let mut uns = Unstructured::new(&bytes);
        uns.arbitrary().unwrap()
    };
    config.name = "test-wral-wal".to_string();
    config.dir = {
        let dir: path::PathBuf = vec![env::temp_dir(), config.name.clone().into()]
            .into_iter()
            .collect();
        dir.into()
    };

    let n_threads = [1, 2, 4, 8][rng.gen::<usize>() % 4];
    let w_ops = [1, 10, 100, 1_000, 10_000][rng.gen::<usize>() % 5];
    config.journal_limit = std::cmp::max(1000, (n_threads * w_ops) / 1000);
    println!(
        "test_wral_wal config:{:?} n_threads:{} w_ops:{}",
        config, n_threads, w_ops
    );

    let wal = Wal::create(config, state::NoState).unwrap();

    let mut writers = vec![];
    for id in 0..n_threads {
        let wal = wal.clone();
        writers.push(std::thread::spawn(move || {
            writer(id, wal, w_ops, seed + (id as u64 * 100))
        }));
    }

    let mut entries: Vec<Vec<wral::Entry>> = vec![];
    for handle in writers {
        entries.push(handle.join().unwrap());
    }
    let mut entries: Vec<wral::Entry> = entries.into_iter().flatten().collect();
    entries.sort_unstable_by_key(|e| e.seqno);

    wal.commit().unwrap();

    let n = entries.len() as u64;
    let sum = entries.iter().map(|e| e.to_seqno()).sum::<u64>();
    assert_eq!(sum, (n * (n + 1)) / 2);

    let mut readers = vec![];
    for id in 0..n_threads {
        let wal = wal.clone();
        let entries = entries.clone();
        let n_ops = 10;
        readers.push(std::thread::spawn(move || {
            reader(id, wal, n_ops, seed + (id as u64), entries)
        }));
    }

    for handle in readers {
        handle.join().unwrap();
    }

    wal.purge().unwrap();
}

fn writer(_id: usize, wal: Wal, ops: usize, seed: u64) -> Vec<wral::Entry> {
    let mut rng = SmallRng::seed_from_u64(seed);

    let mut entries = vec![];
    for _i in 0..ops {
        let op: Vec<u8> = {
            let bytes = rng.gen::<[u8; 32]>();
            let mut uns = Unstructured::new(&bytes);
            uns.arbitrary().unwrap()
        };
        let seqno = wal.add_op(&op).unwrap();
        entries.push(wral::Entry::new(seqno, op));
    }

    wal.close().unwrap();

    entries
}

fn reader(_id: usize, wal: Wal, ops: usize, seed: u64, entries: Vec<wral::Entry>) {
    let mut rng = SmallRng::seed_from_u64(seed);

    for _i in 0..ops {
        match rng.gen::<u8>() % 2 {
            0 => {
                let items: Vec<wral::Entry> =
                    wal.iter().unwrap().filter_map(|x| x.ok()).collect();
                assert_eq!(items.len(), entries.len());
                assert_eq!(items, entries);
            }
            1 => {
                let start = rng.gen::<usize>() % entries.len();
                let end = start + (rng.gen::<usize>() % (entries.len() - start));
                let (x, y) = (entries[start].to_seqno(), entries[end].to_seqno());
                let items: Vec<wral::Entry> =
                    wal.range(x..y).unwrap().map(|x| x.unwrap()).collect();
                assert_eq!(items, entries[start..end]);
            }
            _ => unreachable!(),
        }
    }

    wal.close().unwrap();
}
