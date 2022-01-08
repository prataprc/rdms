use rand::{prelude::random, rngs::StdRng, Rng, SeedableRng};

use super::*;

#[test]
fn test_entry_values() {
    let mut entry: Entry<u8, u64> = Entry::new(10, 200, 1);
    entry = entry.insert(300, 2);
    entry = entry.insert(400, 3);
    entry = entry.delete(4);
    entry = entry.insert(500, 5);
    entry = entry.delete(6);
    entry = entry.delete(7);
    entry = entry.insert(600, 8);

    let values = entry.to_values();
    let mut refvs = vec![
        Value::U {
            value: 200,
            seqno: 1,
        },
        Value::U {
            value: 300,
            seqno: 2,
        },
        Value::U {
            value: 400,
            seqno: 3,
        },
        Value::D { seqno: 4 },
        Value::U {
            value: 500,
            seqno: 5,
        },
        Value::D { seqno: 6 },
        Value::D { seqno: 7 },
        Value::U {
            value: 600,
            seqno: 8,
        },
    ];
    assert_eq!(values, refvs);
    assert_eq!(Entry::from_values(entry.key, values), Ok(entry.clone()));

    entry = entry.delete(9);
    let values = entry.to_values();
    refvs.push(Value::D { seqno: 9 });
    assert_eq!(values, refvs);
}

#[test]
fn test_entry_contains() {
    let mut one: Entry<u8, u64> = Entry::new(10, 200, 1);
    one = one.insert(300, 3);
    one = one.insert(400, 5);
    one = one.delete(7);
    one = one.insert(500, 9);
    one = one.delete(11);
    one = one.delete(13);
    one = one.insert(600, 15);

    assert!(one.contains(&Entry::new(10, 200, 1)), "{:?}", one);
    assert!(one.contains(&Entry::new_delete(10, 7)), "{:?}", one);
    assert!(!one.contains(&Entry::new(10, 200, 2)), "{:?}", one);

    let mut two: Entry<u8, u64> = Entry::new(10, 200, 1);
    two = two.insert(300, 3);
    two = two.insert(400, 5);
    two = two.delete(7);
    two = two.insert(500, 9);
    two = two.delete(11);
    two = two.delete(13);
    assert!(one.contains(&two), "{:?} {:?}", one, two);
    two = two.insert(600, 15);
    assert!(one.contains(&two), "{:?} {:?}", one, two);
    two = two.insert(600, 16);
    assert!(!one.contains(&two), "{:?} {:?}", one, two);
}

#[test]
fn test_entry_merge() {
    let mut one: Entry<u8, u64> = Entry::new(10, 200, 1);
    one = one.insert(300, 3);
    one = one.insert(400, 5);
    one = one.delete(7);
    one = one.insert(500, 9);
    one = one.delete(11);
    one = one.delete(13);
    one = one.insert(600, 15);

    let mut two: Entry<u8, u64> = Entry::new(10, 1000, 2);
    two = two.insert(2000, 4);
    two = two.delete(6);
    two = two.insert(3000, 8);
    two = two.delete(10);
    two = two.insert(4000, 12);
    two = two.insert(5000, 14);
    two = two.delete(16);

    let mut entry: Entry<u8, u64> = Entry::new(10, 200, 1);
    entry = entry.insert(1000, 2);
    entry = entry.insert(300, 3);
    entry = entry.insert(2000, 4);
    entry = entry.insert(400, 5);
    entry = entry.delete(6);
    entry = entry.delete(7);
    entry = entry.insert(3000, 8);
    entry = entry.insert(500, 9);
    entry = entry.delete(10);
    entry = entry.delete(11);
    entry = entry.insert(4000, 12);
    entry = entry.delete(13);
    entry = entry.insert(5000, 14);
    entry = entry.insert(600, 15);
    entry = entry.delete(16);

    assert_eq!(one.commit(&two).unwrap(), entry);
}

#[test]
fn test_entry_compact_mono() {
    let seed: u64 = random();
    // let seed: u128 = 55460639888202704213451510247183500784;
    println!("test_entry_compact_mono {}", seed);
    let mut rng = StdRng::seed_from_u64(seed);

    for _ in 0..100 {
        let (value, seqno) = (100_u64, 1);
        let mut entry = match rng.gen::<u8>() % 2 {
            0 => Entry::new(10_u64, value, seqno),
            1 => Entry::new_delete(10_u64, seqno),
            _ => unreachable!(),
        };

        for i in 0..1000 {
            entry = match rng.gen::<u8>() % 2 {
                0 => entry.insert(value + i, seqno + i),
                1 => entry.delete(seqno + 1),
                _ => unreachable!(),
            };
        }

        if entry.is_deleted() {
            assert_eq!(entry.compact(Cutoff::Mono), None);
        } else {
            let value = entry.to_value();
            let seqno = entry.to_seqno();

            let entry = entry.compact(Cutoff::Mono).unwrap();
            assert_eq!(entry.deltas.len(), 0);
            assert_eq!(entry.to_value(), value);
            assert_eq!(entry.to_seqno(), seqno);
        }
    }
}

#[test]
fn test_entry_compact_lsm() {
    use std::ops::RangeBounds;

    let seed: u64 = random();
    // let seed: u128 = 97177838929013801741121704795542894024;
    println!("test_entry_compact_lsm {}", seed);
    let mut rng = StdRng::seed_from_u64(seed);

    for _ in 0..100 {
        let (value, seqno) = (100_u64, 1);
        let mut entry = match rng.gen::<u8>() % 2 {
            0 => Entry::new(10_u64, value, seqno),
            1 => Entry::new_delete(10_u64, seqno),
            _ => unreachable!(),
        };

        for i in 1..1000 {
            entry = match rng.gen::<u8>() % 2 {
                0 => entry.insert(value + i, seqno + i),
                1 => entry.delete(seqno + i),
                _ => unreachable!(),
            };
        }

        let curr_seqno = entry.to_seqno();
        let cutoff_seqno = match rng.gen::<u8>() % 4 {
            0 => u64::MIN,
            1 => u64::MAX,
            2 => rng.gen::<u64>() % curr_seqno,
            3 => rng.gen::<u64>() % (curr_seqno + 1),
            _ => unreachable!(),
        };

        let (cutoff, start) = match rng.gen::<u8>() % 3 {
            0 => (Bound::Unbounded, Bound::Excluded(curr_seqno)),
            1 => (Bound::Included(cutoff_seqno), Bound::Excluded(cutoff_seqno)),
            2 => (Bound::Excluded(cutoff_seqno), Bound::Included(cutoff_seqno)),
            _ => unreachable!(),
        };

        let range = (start, Bound::Excluded(curr_seqno + 1));
        match entry.clone().compact(Cutoff::Lsm(cutoff)) {
            None => {
                let b = entry
                    .to_values()
                    .iter()
                    .any(|v| range.contains(&v.to_seqno()));
                assert!(
                    !b,
                    "None ... \n{:?}\n{:?}\n {:?}",
                    cutoff,
                    range,
                    entry.to_values()
                );
            }
            Some(compacted) => {
                let b = compacted
                    .to_values()
                    .iter()
                    .all(|v| range.contains(&v.to_seqno()));
                assert!(
                    b,
                    "Some ...\n{:?}\n{:?}\n{:?}\n{:?}",
                    cutoff,
                    range,
                    entry.to_values(),
                    compacted.to_values()
                );
            }
        }
    }
}

#[test]
fn test_entry_compact_tombstone() {
    let seed: u64 = random();
    // let seed: u128 = 97177838929013801741121704795542894024;
    println!("test_entry_compact_tombstone {}", seed);
    let mut rng = StdRng::seed_from_u64(seed);

    for _ in 0..100 {
        let (value, seqno) = (100_u64, 1);
        let mut entry = match rng.gen::<u8>() % 2 {
            0 => Entry::new(10_u64, value, seqno),
            1 => Entry::new_delete(10_u64, seqno),
            _ => unreachable!(),
        };

        for i in 1..1000 {
            entry = match rng.gen::<u8>() % 2 {
                0 => entry.insert(value + i, seqno + i),
                1 => entry.delete(seqno + i),
                _ => unreachable!(),
            };
        }

        let curr_seqno = entry.to_seqno();

        if entry.is_deleted() {
            assert_eq!(
                entry
                    .clone()
                    .compact(Cutoff::Tombstone(Bound::Included(curr_seqno))),
                None
            );
            assert_eq!(
                entry
                    .clone()
                    .compact(Cutoff::Tombstone(Bound::Excluded(curr_seqno + 1))),
                None
            );
            assert_eq!(
                entry
                    .clone()
                    .compact(Cutoff::Tombstone(Bound::Excluded(curr_seqno)))
                    .unwrap(),
                entry
            );
            assert_eq!(
                entry.clone().compact(Cutoff::Tombstone(Bound::Unbounded)),
                None
            );

            assert_eq!(
                entry
                    .clone()
                    .compact(Cutoff::Tombstone(Bound::Included(curr_seqno - 1)))
                    .unwrap(),
                entry
            );
            assert_eq!(
                entry
                    .clone()
                    .compact(Cutoff::Tombstone(Bound::Excluded(curr_seqno - 1)))
                    .unwrap(),
                entry
            );
        } else {
            assert_eq!(
                entry
                    .clone()
                    .compact(Cutoff::Tombstone(Bound::Included(curr_seqno)))
                    .unwrap(),
                entry
            );
            assert_eq!(
                entry
                    .clone()
                    .compact(Cutoff::Tombstone(Bound::Excluded(curr_seqno)))
                    .unwrap(),
                entry
            );
            assert_eq!(
                entry
                    .clone()
                    .compact(Cutoff::Tombstone(Bound::Unbounded))
                    .unwrap(),
                entry,
                ""
            );
        }
    }
}
