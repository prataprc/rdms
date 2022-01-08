use arbitrary::{self, unstructured::Unstructured, Arbitrary};
use rand::{self, prelude::random, rngs::SmallRng, Rng, SeedableRng};

use super::*;

#[test]
fn test_trie() {
    let seed: u64 = [8941903814573999963, random()][random::<usize>() % 2];
    // let seed: u64 = 8941903814573999963;
    println!("test_trie {}", seed);
    let mut rng = SmallRng::seed_from_u64(seed);

    let n_ops = [0, 1, 2, 3, 5, 7, 10, 100, 10000, 100000][rng.gen::<usize>() % 10];
    let n_ops = 4;

    let mut trie = Trie::<char, u64>::new();
    let mut index = ppom::OMap::<String, u64>::new();

    let mut op_counts = [0_u64; 8];

    for _i in 0..n_ops {
        let op: Op = {
            let bytes = rng.gen::<[u8; 32]>();
            let mut uns = Unstructured::new(&bytes);
            uns.arbitrary().unwrap()
        };

        println!("{:?}", op);
        match op {
            Op::Set(key, value) => {
                op_counts[0] += 1;
                let refv = index.set(key.clone(), value.clone());
                let parts = key.chars().collect::<Vec<char>>();
                let resv = trie.set(&parts, value);
                assert_eq!(refv, resv);
            }
            Op::RandomSet(value) => match index.random(&mut rng) {
                Some((key, _)) => {
                    op_counts[1] += 1;
                    let refv = index.set(key.clone(), value.clone());
                    let parts = key.chars().collect::<Vec<char>>();
                    let resv = trie.set(&parts, value);
                    assert_eq!(refv, resv);
                }
                None => (),
            }, //Op::Remove(key) => {
               //    op_counts[2] += 1;
               //    let refv = index.remove(key);
               //    let parts = key.chars().collect::<Vec<char>>();
               //    let resv = trie.remove(parts);
               //    assert_eq!(refv, resv);
               //}
               //Op::RandomRemove => match index.random(rng) {
               //    Some((key, _)) => {
               //        op_counts[3] += 1;
               //        let refv = index.remove(key);
               //        let parts = key.chars().collect::<Vec<char>>();
               //        let resv = trie.remove(parts);
               //        assert_eq!(refv, resv);
               //    }
               //    None => (),
               //}
               //Op::Len => {
               //    op_counts[4] += 1;
               //    assert_eq!(index.len(), trie.len()),
               //}
               //Op::IsEmpty => {
               //    op_counts[5] += 1;
               //    assert_eq!(index.is_empty(), trie.is_empty()),
               //}
               //Op::Get(key) => {
               //    op_counts[6] += 1;
               //    let refv = index.get(key);
               //    let parts = key.chars().collect::<Vec<char>>();
               //    let resv = trie.get(parts);
               //    assert_eq!(refv, resv);
               //}
               // Op::Walk(start, end) => {
               //    op_counts[7] += 1;
               //}
        }
    }

    println!("test_trie op_counts:{:?}", op_counts);
}

#[derive(Clone, Debug, Arbitrary)]
enum Op {
    Set(String, u64),
    RandomSet(u64),
    //Remove(String),
    //RandomRemove,
    //Len,
    //IsEmpty,
    //Get(String),
    //Walk(Limit, Limit),
}

#[derive(Clone, Debug, Arbitrary, Eq, PartialEq)]
enum Limit {
    Unbounded,
    Included(String),
    Excluded(String),
}
