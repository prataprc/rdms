use arbitrary::Unstructured;
use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use super::*;

#[test]
fn test_wral_journal() {
    use std::env;

    let seed: u64 = random();
    let mut rng = SmallRng::seed_from_u64(seed);
    println!("test_wral_journal {}", seed);

    let name = "test_wral_journal";
    let dir = env::temp_dir().into_os_string();
    println!("test_wral_journal {:?}", dir);
    let mut jn = Journal::start(&dir, name, 0, state::NoState).unwrap();
    assert_eq!(jn.to_journal_number(), 0);
    assert_eq!(jn.len_batches(), 0);
    assert_eq!(jn.as_state().clone(), state::NoState);

    let mut entries: Vec<wral::Entry> = (0..1_000_000)
        .map(|_i| {
            let bytes = rng.gen::<[u8; 32]>();
            let mut uns = Unstructured::new(&bytes);
            uns.arbitrary::<wral::Entry>().unwrap()
        })
        .collect();
    entries.sort();
    entries.dedup_by(|a, b| a.to_seqno() == b.to_seqno());

    let mut n_batches = 0;
    let mut offset = 0;
    for _i in 0..1000 {
        let n = rng.gen::<u8>();
        for _j in 0..n {
            let entry = entries[offset].clone();
            jn.add_entry(entry.clone()).unwrap();
            entries.push(entry);
            offset += 1;
        }

        assert_eq!(jn.to_last_seqno(), Some(entries[offset - 1].to_seqno()));

        jn.flush().unwrap();
        if n > 0 {
            n_batches += 1;
        }

        assert_eq!(jn.to_last_seqno(), Some(entries[offset - 1].to_seqno()));
    }
    assert_eq!(n_batches, jn.len_batches());

    let iter = IterJournal::from_journal(&jn, 0..=u64::MAX).unwrap();
    let jn_entries: Vec<wral::Entry> = iter.map(|x| x.unwrap()).collect();
    let entries = entries[..offset].to_vec();
    assert_eq!(entries.len(), jn_entries.len());
    assert_eq!(entries, jn_entries);

    {
        let (load_jn, _) =
            Journal::<state::NoState>::load(name, &jn.to_location()).unwrap();
        let iter = IterJournal::from_journal(&load_jn, 0..=u64::MAX).unwrap();
        let jn_entries: Vec<wral::Entry> = iter.map(|x| x.unwrap()).collect();
        let entries = entries[..offset].to_vec();
        assert_eq!(entries.len(), jn_entries.len());
        assert_eq!(entries, jn_entries);
    }

    jn.purge().unwrap();
}
