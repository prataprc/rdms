use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use super::*;
use crate::util;

#[test]
fn test_build_scan() {
    use std::time::Duration;

    let seed: u128 = random();
    // let seed: u128 = 284595450980088120127817086088032225381;
    println!("test_build_scan {}", seed);
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let inserts = 1_000_000;
    let mdb = util::load_index(seed, 0, inserts, 0, 1_000, None);

    let start_seqno = rng.gen::<u64>() % ((mdb.len() as u64) * 2);
    let mut iter = BuildScan::new(mdb.iter().unwrap(), start_seqno);
    let mut count = 0;
    while let Some(entry) = iter.next() {
        count += 1;
        if count % 10 == 0 {
            iter.push(entry)
        }
    }

    let (build_time, seqno, count, _deleted, epoch, mut iter) = iter.unwrap().unwrap();
    println!(
        "BuildScan build_time {:?}",
        Duration::from_nanos(build_time)
    );
    println!("BuildScan epoch {:?}", Duration::from_nanos(epoch));
    assert_eq!(seqno, cmp::max(start_seqno, mdb.to_seqno()));
    assert_eq!(count, mdb.len() as u64);
    assert_eq!(iter.next(), None);
}

#[test]
fn test_nobitmap_scan() {
    use mkit::nobitmap::NoBitmap;

    let seed: u128 = random();
    // let seed: u128 = 284595450980088120127817086088032225381;
    println!("test_nobitmap_scan {}", seed);
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let inserts = 1_000_000;
    let mdb = util::load_index(seed, 0, inserts, 0, 1_000, None);

    // with NoBitmap
    let mut iter = BitmappedScan::new(mdb.iter().unwrap(), NoBitmap);
    let len: usize = iter.by_ref().map(|_| 1).sum();
    let (mut bitmap, mut iter) = iter.unwrap().unwrap();
    bitmap.build();
    assert_eq!(len, mdb.len());
    assert_eq!(iter.next(), None);
    assert_eq!(bitmap.to_bytes().unwrap().len(), 0);
    let bitmap = NoBitmap::from_bytes(&bitmap.to_bytes().unwrap()).unwrap().0;
    for _i in 0..1_000_000 {
        let key = rng.gen::<u16>();
        assert!(bitmap.contains(&key), "{}", key);
    }
}

#[test]
fn test_xorfilter_scan() {
    use xorfilter::Xor8;

    let seed: u128 = random();
    // let seed: u128 = 55460639888202704213451510247183500784;
    println!("test_xorfilter_scan {}", seed);
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let inserts = 1_000_000;
    let mdb = util::load_index(seed, 0, inserts, 0, 1_000, None);

    // with xorfilter
    let mut iter = BitmappedScan::new(mdb.iter().unwrap(), Xor8::new());
    let len: usize = iter.by_ref().map(|_| 1).sum();
    let (mut bitmap, mut iter) = iter.unwrap().unwrap();
    bitmap.build();
    assert_eq!(len, mdb.len());
    assert_eq!(iter.next(), None);
    let bitma = {
        let bytes = <Xor8 as Bloom>::to_bytes(&bitmap).unwrap();
        <Xor8 as Bloom>::from_bytes(&bytes).unwrap().0
    };
    let mut found_keys = 0;
    for _i in 0..1_000_000 {
        let key = rng.gen::<u16>();
        if mdb.get(&key).is_ok() {
            found_keys += 1;
            assert!(bitma.contains(&key), "{}", key);
        }
    }
    println!("found keys in xor8 {}", found_keys);
}
