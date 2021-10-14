use arbitrary::Unstructured;
use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use super::*;

#[test]
fn test_wral_index() {
    let seed: u128 = random();
    println!("test_wral_index {}", seed);
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let index: Index = {
        let bytes = rng.gen::<[u8; 32]>();
        let mut uns = Unstructured::new(&bytes);
        uns.arbitrary().unwrap()
    };
    assert_eq!(index.to_first_seqno(), index.first_seqno);
    assert_eq!(index.to_first_seqno(), index.first_seqno);

    let val = Index::new(
        index.fpos,
        index.length,
        index.first_seqno,
        index.last_seqno,
    );
    assert_eq!(index, val);
}

#[test]
fn test_wral_batch() {
    use cbordata::{Cbor, FromCbor, IntoCbor};

    let seeds: Vec<u128> = vec![
        225569602694000826843969627559726108824,
        214504593551397116282345381712716803483,
        177121329091129258928668221088480874568,
        random(),
        random(),
        random(),
    ];
    let seed = seeds[random::<usize>() % seeds.len()];
    // let seed: u128 = 214504593551397116282345381712716803483;
    println!("test_wral_batch {}", seed);
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let mut batches = vec![];
    for _i in 0..1000 {
        let batch: Batch = {
            let bytes = rng.gen::<[u8; 32]>();
            let mut uns = Unstructured::new(&bytes);
            uns.arbitrary().unwrap()
        };
        batches.push(batch.clone());

        assert_eq!(batch.as_state_bytes().to_vec(), batch.state);
        assert_eq!(batch.to_first_seqno(), batch.first_seqno);
        assert_eq!(batch.to_last_seqno(), batch.last_seqno);
        assert_eq!(
            batch
                .clone()
                .into_iter(0..=u64::MAX)
                .collect::<Vec<wral::Entry>>(),
            batch.entries
        );

        let cbor: Cbor = batch.clone().into_cbor().unwrap();

        let mut buf: Vec<u8> = vec![];
        let n = cbor.encode(&mut buf).unwrap();
        let (val, m) = Cbor::decode(&mut buf.as_slice()).unwrap();
        assert_eq!(n, m);
        assert_eq!(cbor, val);

        let rbatch = Batch::from_cbor(val).unwrap();
        assert_eq!(batch, rbatch);
    }

    let mut batches: Vec<Batch> = batches
        .into_iter()
        .filter(|b| b.entries.is_empty())
        .collect();
    batches.sort();
    batches.dedup_by(|a, b| a.first_seqno == b.first_seqno);

    let mut seqno = 0;
    for batch in batches.into_iter() {
        assert!(
            seqno <= batch.first_seqno,
            "{} {}",
            seqno,
            batch.first_seqno
        );
        assert!(batch.first_seqno <= batch.last_seqno, "{}", batch);
        seqno = batch.first_seqno
    }
}

#[test]
fn test_wral_worker() {
    use std::env;

    let seeds: Vec<u128> = vec![148484157541144179681685363423689665370, random()];
    let seed = seeds[random::<usize>() % seeds.len()];
    // let seed: u128 = 148484157541144179681685363423689665370;
    println!("test_wral_worker {}", seed);
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let mut file = {
        let ntf = env::temp_dir().join("test_wral_worker.data");
        println!("test_wral_worker temporary file created {:?}", ntf);
        let mut opts = fs::OpenOptions::new();
        opts.create(true)
            .append(true)
            .read(true)
            .open(&ntf)
            .unwrap()
    };

    let mut worker = Worker::new(state::NoState);

    let mut index = vec![];
    let mut all_entries = vec![];
    for _i in 0..1000 {
        let mut entries = vec![];
        let n = rng.gen::<u8>();
        for _j in 0..n {
            let entry: wral::Entry = {
                let bytes = rng.gen::<[u8; 32]>();
                let mut uns = Unstructured::new(&bytes);
                uns.arbitrary().unwrap()
            };
            worker.add_entry(entry.clone()).unwrap();
            entries.push(entry.clone());
            all_entries.push(entry);
        }

        assert_eq!(entries, worker.as_entries().to_vec());
        if n > 0 {
            assert_eq!(entries.last().map(|e| e.to_seqno()), worker.to_last_seqno())
        }

        if let Some(x) = worker.flush(&mut file).unwrap() {
            index.push(x)
        };

        if n > 0 {
            assert_eq!(entries.last().map(|e| e.to_seqno()), worker.to_last_seqno())
        }
    }

    assert_eq!(index, worker.as_index().to_vec());
    let entries = index
        .iter()
        .map(|x| {
            Batch::from_index(x.clone(), &mut file)
                .unwrap()
                .into_iter(0..=u64::MAX)
                .collect::<Vec<wral::Entry>>()
        })
        .flatten()
        .collect::<Vec<wral::Entry>>();
    assert_eq!(entries, all_entries)
}
