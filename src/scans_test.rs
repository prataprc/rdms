use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use super::*;
use crate::{
    core::{Index, Reader, Writer},
    croaring::CRoaring,
    error::Error,
    llrb::Llrb,
};

#[test]
fn test_into_iter_scan() {
    use std::vec::IntoIter;
    let seed: u128 = random();

    let test_cases = [(6_000_i64, 2_000), (0, 2_000)];
    let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
    for (n_ops, key_max) in test_cases.to_vec().into_iter() {
        let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
        random_llrb(n_ops, key_max, seed, &mut llrb);

        let ref_entries: Vec<Result<Entry<i64, i64>>> = llrb.iter().unwrap().collect();
        let mut into_iter = ref_entries.into_iter();
        let entries: Vec<Entry<i64, i64>> = <IntoIter<Result<Entry<i64, i64>>> as CommitIterator<
            i64,
            i64,
        >>::scan(&mut into_iter, within.clone())
        .unwrap()
        .map(|e| e.unwrap())
        .collect();

        let ref_entries: Vec<Entry<i64, i64>> = llrb.iter().unwrap().map(|e| e.unwrap()).collect();
        assert_eq!(ref_entries.len(), entries.len());
        entries
            .iter()
            .zip(ref_entries.iter())
            .for_each(|(e, re)| check_node(e, re));
    }
}

#[test]
fn test_into_iter_scans() {
    use std::vec::IntoIter;

    let seed: u128 = random();
    println!("seed {}", seed);

    let test_cases = [(6_000_i64, 2_000), (0, 2_000)];
    let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
    for (n_ops, key_max) in test_cases.to_vec().into_iter() {
        let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
        random_llrb(n_ops, key_max, seed, &mut llrb);

        for shards in 1..10 {
            println!("n_ops:{} key_max:{} shards:{}", n_ops, key_max, shards);
            let ref_entries: Vec<Result<Entry<i64, i64>>> = llrb.iter().unwrap().collect();
            let mut into_iter = ref_entries.into_iter();
            let iter = {
                let mut iters =
                    <IntoIter<Result<Entry<i64, i64>>> as CommitIterator<i64, i64>>::scans(
                        &mut into_iter,
                        shards,
                        within.clone(),
                    )
                    .unwrap();
                let w = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
                iters.reverse(); // make this to stack
                FilterScans::new(iters, w)
            };
            let entries: Vec<Entry<i64, i64>> = iter.map(|e| e.unwrap()).collect();

            let ref_entries: Vec<Entry<i64, i64>> =
                llrb.iter().unwrap().map(|e| e.unwrap()).collect();
            assert_eq!(ref_entries.len(), entries.len());
            entries
                .iter()
                .zip(ref_entries.iter())
                .for_each(|(e, re)| check_node(e, re));
        }
    }
}

#[test]
fn test_into_iter_range_scans() {
    use std::ops::Bound::{Excluded, Included, Unbounded};
    use std::vec::IntoIter;

    let seed: u128 = random();
    println!("seed {}", seed);

    let test_cases = [(6_000_i64, 2_000)];
    let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
    for (n_ops, key_max) in test_cases.to_vec().into_iter() {
        let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
        random_llrb(n_ops, key_max, seed, &mut llrb);

        for shards in 1..10 {
            println!("n_ops:{} key_max:{} shards:{}", n_ops, key_max, shards);
            let (last_hk, mut ranges) = llrb
                .scans(shards, within.clone())
                .unwrap()
                .into_iter()
                .fold((Unbounded, vec![]), |(lk, mut acc), mut iter| {
                    let hk = iter.next().unwrap().unwrap().to_key();
                    acc.push((lk, Excluded(hk.clone())));
                    (Included(hk), acc)
                });
            ranges.push((last_hk, Unbounded));

            let ref_entries: Vec<Result<Entry<i64, i64>>> = llrb.iter().unwrap().collect();
            let mut into_iter = ref_entries.into_iter();
            let iter = {
                let mut iters =
                    <IntoIter<Result<Entry<i64, i64>>> as CommitIterator<i64, i64>>::range_scans(
                        &mut into_iter,
                        ranges,
                        within.clone(),
                    )
                    .unwrap();
                iters.reverse(); // make this to stack
                let w = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
                FilterScans::new(iters, w)
            };
            let entries: Vec<Entry<i64, i64>> = iter.map(|e| e.unwrap()).collect();

            let ref_entries: Vec<Entry<i64, i64>> =
                llrb.iter().unwrap().map(|e| e.unwrap()).collect();
            assert_eq!(ref_entries.len(), entries.len());
            entries
                .iter()
                .zip(ref_entries.iter())
                .for_each(|(e, re)| check_node(e, re));
        }
    }
}

#[test]
fn test_skip_scan() {
    use std::ops::Bound;

    let seed: u128 = random();
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let (n_ops, key_max) = (6_000_i64, 2_000);
    let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
    random_llrb(n_ops, key_max, seed, &mut llrb);

    let withins = vec![
        (Bound::Included(0), Bound::Included(5100)),
        (Bound::Included(6000), Bound::Excluded(5100)),
        (Bound::Included(4000), Bound::Unbounded),
        (Bound::Excluded(5000), Bound::Included(5100)),
        (Bound::Excluded(5000), Bound::Excluded(5100)),
        (Bound::Excluded(5000), Bound::Unbounded),
        (Bound::Unbounded, Bound::Excluded(5100)),
        (Bound::Unbounded, Bound::Included(5100)),
        (Bound::Unbounded, Bound::Unbounded),
    ];
    for _i in 0..1000 {
        let mut scanner = SkipScan::new(llrb.to_reader().unwrap());

        let j = rng.gen::<usize>() % (withins.len() * 2);
        let within = if j < withins.len() {
            withins[j].clone()
        } else {
            let start_seqno = match rng.gen::<i64>() {
                n if (n >= 0) && (n % 4 == 0) => Bound::Included((n % n_ops) as u64),
                n if (n >= 0) && (n % 4 == 1) => Bound::Included(0),
                n if (n >= 0) && (n % 4 == 2) => Bound::Included((n % n_ops) as u64),
                n if (n >= 0) && (n % 4 == 3) => Bound::Included(((n % n_ops) + 1) as u64),
                _ => Bound::Unbounded,
            };
            let end_seqno = match rng.gen::<i64>() {
                n if (n >= 0) && (n % 4 == 0) => Bound::Included((n % n_ops) as u64),
                n if (n >= 0) && (n % 4 == 1) => Bound::Included(0),
                n if (n >= 0) && (n % 4 == 2) => Bound::Included((n % n_ops) as u64),
                n if (n >= 0) && (n % 4 == 3) => Bound::Included(((n % n_ops) + 1) as u64),
                _ => Bound::Unbounded,
            };
            (start_seqno, end_seqno)
        };

        scanner.set_seqno_range(within);

        let start_key = match rng.gen::<i64>() {
            n if (n >= 0) && (n % 2 == 0) => Bound::Included(n % key_max),
            n if n >= 0 => Bound::Excluded(n % key_max),
            _ => Bound::Unbounded,
        };
        let end_key = match rng.gen::<i64>() {
            n if n >= 0 && n % 2 == 0 => Bound::Included(n % key_max),
            n if n >= 0 => Bound::Excluded(n % key_max),
            _ => Bound::Unbounded,
        };
        let key_range = (start_key, end_key);

        scanner.set_key_range(key_range);

        let es: Vec<Entry<i64, i64>> = scanner.map(|e| e.unwrap()).collect();
        println!(
            "within:{:?} key:{:?} entries:{}",
            within,
            key_range,
            es.len()
        );
        for e in es.iter() {
            assert!(within.contains(&e.to_seqno()));
            for d in e.as_deltas() {
                assert!(within.contains(&d.to_seqno()));
            }
        }

        let ref_iter = llrb.range(key_range).unwrap();
        let mut iter = es.iter();
        for ref_entry in ref_iter {
            let ref_entry = ref_entry.unwrap();
            let ref_entry = match ref_entry.filter_within(within.0, within.1) {
                Some(ref_entry) => ref_entry,
                None => continue,
            };
            let entry = iter.next().unwrap();
            check_node(entry, &ref_entry);
        }
        match iter.next() {
            Some(entry) => panic!(
                "within {:?} range {:?} {}",
                within,
                key_range,
                entry.to_key()
            ),
            None => (),
        }
    }

    let mut scanner = SkipScan::new(llrb.to_reader().unwrap());
    scanner.set_seqno_range((Bound::Included(5000), Bound::Included(5000)));
    let es: Vec<Entry<i64, i64>> = scanner.map(|e| e.unwrap()).collect();
    assert_eq!(es.len(), 1);
    assert_eq!(es[0].to_seqno(), 5000);

    let mut scanner = SkipScan::new(llrb.to_reader().unwrap());
    scanner.set_seqno_range((Bound::Included(5000), Bound::Excluded(5000)));
    let es: Vec<Entry<i64, i64>> = scanner.map(|e| e.unwrap()).collect();
    assert_eq!(es.len(), 0);

    let mut scanner = SkipScan::new(llrb.to_reader().unwrap());
    scanner.set_seqno_range((Bound::Excluded(5000), Bound::Included(5000)));
    let es: Vec<Entry<i64, i64>> = scanner.map(|e| e.unwrap()).collect();
    assert_eq!(es.len(), 0);

    let mut scanner = SkipScan::new(llrb.to_reader().unwrap());
    scanner.set_seqno_range((Bound::Excluded(5000), Bound::Excluded(5000)));
    let es: Vec<Entry<i64, i64>> = scanner.map(|e| e.unwrap()).collect();
    assert_eq!(es.len(), 0);

    let mut scanner = SkipScan::new(llrb.to_reader().unwrap());
    scanner.set_seqno_range((Bound::Included(5000), Bound::Excluded(5001)));
    let es: Vec<Entry<i64, i64>> = scanner.map(|e| e.unwrap()).collect();
    assert_eq!(es.len(), 1);
    assert_eq!(es[0].to_seqno(), 5000);

    let mut scanner = SkipScan::new(llrb.to_reader().unwrap());
    scanner.set_seqno_range((Bound::Excluded(5000), Bound::Included(5001)));
    let es: Vec<Entry<i64, i64>> = scanner.map(|e| e.unwrap()).collect();
    assert_eq!(es.len(), 1);
    assert_eq!(es[0].to_seqno(), 5001);

    let mut scanner = SkipScan::new(llrb.to_reader().unwrap());
    scanner.set_seqno_range((Bound::Excluded(5000), Bound::Excluded(5001)));
    let es: Vec<Entry<i64, i64>> = scanner.map(|e| e.unwrap()).collect();
    assert_eq!(es.len(), 0);
}

#[test]
fn test_filter_scan() {
    use std::ops::Bound;

    let seed: u128 = random();
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let (n_ops, key_max) = (6_000_i64, 2_000);
    let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
    random_llrb(n_ops, key_max, seed, &mut llrb);

    let withins = vec![
        (Bound::Included(0), Bound::Included(5100)),
        (Bound::Included(6000), Bound::Excluded(5100)),
        (Bound::Included(4000), Bound::Unbounded),
        (Bound::Excluded(5000), Bound::Included(5100)),
        (Bound::Excluded(5000), Bound::Excluded(5100)),
        (Bound::Excluded(5000), Bound::Unbounded),
        (Bound::Unbounded, Bound::Excluded(5100)),
        (Bound::Unbounded, Bound::Included(5100)),
        (Bound::Unbounded, Bound::Unbounded),
    ];
    for _i in 0..1000 {
        let j = rng.gen::<usize>() % (withins.len() * 2);

        let within = if j < withins.len() {
            withins[j].clone()
        } else {
            let start_seqno = match rng.gen::<i64>() {
                n if (n >= 0) && (n % 4 == 0) => Bound::Included((n % n_ops) as u64),
                n if (n >= 0) && (n % 4 == 1) => Bound::Included(0),
                n if (n >= 0) && (n % 4 == 2) => Bound::Included((n % n_ops) as u64),
                n if (n >= 0) && (n % 4 == 3) => Bound::Included(((n % n_ops) + 1) as u64),
                _ => Bound::Unbounded,
            };
            let end_seqno = match rng.gen::<i64>() {
                n if (n >= 0) && (n % 4 == 0) => Bound::Included((n % n_ops) as u64),
                n if (n >= 0) && (n % 4 == 1) => Bound::Included(0),
                n if (n >= 0) && (n % 4 == 2) => Bound::Included((n % n_ops) as u64),
                n if (n >= 0) && (n % 4 == 3) => Bound::Included(((n % n_ops) + 1) as u64),
                _ => Bound::Unbounded,
            };
            (start_seqno, end_seqno)
        };

        let scanner = {
            let iters = vec![llrb.iter().unwrap()];
            FilterScans::new(iters, within)
        };

        let es: Vec<Entry<i64, i64>> = scanner.map(|e| e.unwrap()).collect();
        println!("within:{:?} entries:{}", within, es.len());
        for e in es.iter() {
            assert!(within.contains(&e.to_seqno()));
            for d in e.as_deltas() {
                assert!(within.contains(&d.to_seqno()));
            }
        }

        let (ref_iter, mut iter) = (llrb.iter().unwrap(), es.iter());
        for ref_entry in ref_iter {
            let ref_entry = ref_entry.unwrap();
            let ref_entry = match ref_entry.filter_within(within.0, within.1) {
                Some(ref_entry) => ref_entry,
                None => continue,
            };
            let entry = iter.next().unwrap();
            check_node(entry, &ref_entry);
        }
        match iter.next() {
            Some(entry) => panic!("within:{:?} {}", within, entry.to_key()),
            None => (),
        }
    }

    let within = (Bound::Included(5000), Bound::Included(5000));
    let scanner = FilterScans::new(vec![llrb.iter().unwrap()], within);
    let es: Vec<Entry<i64, i64>> = scanner.map(|e| e.unwrap()).collect();
    assert_eq!(es.len(), 1);
    assert_eq!(es[0].to_seqno(), 5000);

    let within = (Bound::Included(5000), Bound::Excluded(5000));
    let scanner = FilterScans::new(vec![llrb.iter().unwrap()], within);
    let es: Vec<Entry<i64, i64>> = scanner.map(|e| e.unwrap()).collect();
    assert_eq!(es.len(), 0);

    let within = (Bound::Excluded(5000), Bound::Included(5000));
    let scanner = FilterScans::new(vec![llrb.iter().unwrap()], within);
    let es: Vec<Entry<i64, i64>> = scanner.map(|e| e.unwrap()).collect();
    assert_eq!(es.len(), 0);

    let within = (Bound::Excluded(5000), Bound::Excluded(5000));
    let scanner = FilterScans::new(vec![llrb.iter().unwrap()], within);
    let es: Vec<Entry<i64, i64>> = scanner.map(|e| e.unwrap()).collect();
    assert_eq!(es.len(), 0);

    let within = (Bound::Included(5000), Bound::Excluded(5001));
    let scanner = FilterScans::new(vec![llrb.iter().unwrap()], within);
    let es: Vec<Entry<i64, i64>> = scanner.map(|e| e.unwrap()).collect();
    assert_eq!(es.len(), 1);
    assert_eq!(es[0].to_seqno(), 5000);

    let within = (Bound::Excluded(5000), Bound::Included(5001));
    let scanner = FilterScans::new(vec![llrb.iter().unwrap()], within);
    let es: Vec<Entry<i64, i64>> = scanner.map(|e| e.unwrap()).collect();
    assert_eq!(es.len(), 1);
    assert_eq!(es[0].to_seqno(), 5001);

    let within = (Bound::Excluded(5000), Bound::Excluded(5001));
    let scanner = FilterScans::new(vec![llrb.iter().unwrap()], within);
    let es: Vec<Entry<i64, i64>> = scanner.map(|e| e.unwrap()).collect();
    assert_eq!(es.len(), 0);
}

#[test]
fn test_bitmapped_scan() {
    let seed: u128 = random();
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    for i in 0..100 {
        let (n_ops, key_max) = (6_000_i64, 2_000);
        let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
        random_llrb(n_ops, key_max, seed + (i * 10), &mut llrb);

        let (bitmap, es) = {
            let mut scanner = BitmappedScan::<_, _, _, CRoaring>::new(llrb.iter().unwrap());
            let mut es = vec![];
            let (mut iter, bitmap) = loop {
                match scanner.next() {
                    Some(e) => es.push(e.as_ref().unwrap().clone()),
                    None => break scanner.close().unwrap(),
                }
            };
            assert!(iter.next().is_none());
            (bitmap, es)
        };
        println!("entries:{}", es.len());
        assert!(es.len() == llrb.len());
        assert!(
            bitmap.len().unwrap() <= llrb.len(),
            "{}/{}",
            bitmap.len().unwrap(),
            llrb.len()
        );
        for _j in 0..10000 {
            let key = (rng.gen::<i64>() % key_max).abs();
            let false_positve = llrb.get(&key).ok().is_some() == false && bitmap.contains(&key);
            assert!(!false_positve);
        }
    }
}

#[test]
fn test_compact_scan() {
    use std::ops::Bound;

    let seed: u128 = random();
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let (n_ops, key_max) = (6_000_i64, 2_000);
    let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
    random_llrb(n_ops, key_max, seed, &mut llrb);

    let cutoffs = vec![
        Bound::Included(0),
        Bound::Included(6000),
        Bound::Included(4000),
        Bound::Excluded(5000),
        Bound::Excluded(5000),
        Bound::Excluded(5000),
        Bound::Unbounded,
        Bound::Unbounded,
        Bound::Unbounded,
    ];
    for _i in 0..1000 {
        let j = rng.gen::<usize>() % (cutoffs.len() * 2);

        let cutoff = if j < cutoffs.len() {
            cutoffs[j].clone()
        } else {
            match rng.gen::<i64>() {
                n if (n >= 0) && (n % 4 == 0) => Bound::Included((n % n_ops) as u64),
                n if (n >= 0) && (n % 4 == 1) => Bound::Included(0),
                n if (n >= 0) && (n % 4 == 2) => Bound::Included((n % n_ops) as u64),
                n if (n >= 0) && (n % 4 == 3) => Bound::Included(((n % n_ops) + 1) as u64),
                _ => Bound::Unbounded,
            }
        };
        let within = (Bound::Unbounded, cutoff.clone());

        let scanner = CompactScan::new(llrb.iter().unwrap(), cutoff.clone());

        let es: Vec<Entry<i64, i64>> = scanner.map(|e| e.unwrap()).collect();
        println!("cutoff:{:?} entries:{}", cutoff, es.len());
        for e in es.iter() {
            assert!(e.to_seqno() <= n_ops as u64);
            assert!(
                !within.contains(&e.to_seqno()),
                "key:{} seqno:{}",
                e.to_key(),
                e.to_seqno()
            );
            for d in e.as_deltas() {
                assert!(!within.contains(&d.to_seqno()));
            }
        }

        let (ref_iter, mut iter) = (llrb.iter().unwrap(), es.iter());
        for ref_entry in ref_iter {
            let ref_entry = ref_entry.unwrap();
            let ref_entry = match ref_entry.purge(cutoff.clone()) {
                Some(ref_entry) => ref_entry,
                None => continue,
            };
            let entry = iter.next().unwrap();
            check_node(entry, &ref_entry);
        }
        match iter.next() {
            Some(entry) => panic!("cutoff:{:?} {}", cutoff, entry.to_key()),
            None => (),
        }
    }
}

fn check_node(entry: &Entry<i64, i64>, ref_entry: &Entry<i64, i64>) {
    //println!("check_node {} {}", entry.key(), ref_entry.key);
    assert_eq!(entry.to_key(), ref_entry.to_key(), "key");

    let key = entry.to_key();
    //println!("check-node value {:?}", entry.to_native_value());
    assert_eq!(
        entry.to_native_value(),
        ref_entry.to_native_value(),
        "key {}",
        key
    );
    assert_eq!(entry.to_seqno(), ref_entry.to_seqno(), "key {}", key);
    assert_eq!(entry.is_deleted(), ref_entry.is_deleted(), "key {}", key);
    assert_eq!(
        entry.as_deltas().len(),
        ref_entry.as_deltas().len(),
        "key {}",
        key
    );

    //println!("versions {} {}", n_vers, refn_vers);
    let mut vers = entry.versions();
    let mut ref_vers = ref_entry.versions();
    loop {
        match (vers.next(), ref_vers.next()) {
            (Some(e), Some(re)) => {
                assert_eq!(e.to_native_value(), re.to_native_value(), "key {}", key);
                assert_eq!(e.to_seqno(), re.to_seqno(), "key {} ", key);
                assert_eq!(e.is_deleted(), re.is_deleted(), "key {}", key);
            }
            (None, None) => break,
            (Some(e), None) => panic!("invalid entry {} {}", e.to_key(), e.to_seqno()),
            (None, Some(re)) => panic!("invalid entry {} {}", re.to_key(), re.to_seqno()),
        }
    }
}

fn random_llrb(n_ops: i64, key_max: i64, seed: u128, llrb: &mut Llrb<i64, i64>) {
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
    for _i in 0..n_ops {
        let key = (rng.gen::<i64>() % key_max).abs();
        let op = rng.gen::<usize>() % 3;
        //println!("key {} {} {} {}", key, llrb.to_seqno(), op);
        match op {
            0 => {
                let value: i64 = rng.gen();
                llrb.set(key, value).unwrap();
            }
            1 => {
                let value: i64 = rng.gen();
                {
                    let cas = match llrb.get(&key) {
                        Err(Error::KeyNotFound) => 0,
                        Err(_err) => unreachable!(),
                        Ok(e) => e.to_seqno(),
                    };
                    llrb.set_cas(key, value, cas).unwrap();
                }
            }
            2 => {
                llrb.delete(&key).unwrap();
            }
            _ => unreachable!(),
        }
    }
}
