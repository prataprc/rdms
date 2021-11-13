use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use super::*;
use crate::{
    dbs::{self, Bloom},
    llrb,
};

#[test]
fn test_robt_build_scan() {
    use std::time::Duration;

    let seed: u64 = random();
    let mut rng = SmallRng::seed_from_u64(seed);
    println!("test_build_scan {}", seed);

    let inserts = 1_000_000;
    let mdb = llrb::load_index::<u16, u64>(seed, 0, inserts, 0, 1_000, None);

    let start_seqno = rng.gen::<u64>() % ((mdb.len() as u64) * 2);
    let mut iter = BuildScan::new(mdb.iter().unwrap().map(Ok), start_seqno);
    let mut count = 0;
    for _ in &mut iter {
        count += 1;
    }
    assert_eq!(count, mdb.len() as u64, "{} {}", count, mdb.len());

    let (build_time, seqno, count, _deleted, epoch, mut iter) = iter.unwrap().unwrap();
    println!(
        "BuildScan build_time {:?}",
        Duration::from_nanos(build_time)
    );
    println!("BuildScan epoch {:?}", Duration::from_nanos(epoch));
    assert_eq!(seqno, cmp::max(start_seqno, mdb.to_seqno()));
    assert_eq!(count, mdb.len() as u64, "{} {}", count, mdb.len());
    assert_eq!(iter.next(), None);
}

#[test]
fn test_robt_nobitmap_scan() {
    use crate::bitmaps::NoBitmap;

    let seed: u64 = random();
    let mut rng = SmallRng::seed_from_u64(seed);
    println!("test_nobitmap_scan {}", seed);

    let inserts = 1_000_000;
    let mdb = llrb::load_index::<u16, u64>(seed, 0, inserts, 0, 1_000, None);

    // with NoBitmap
    let mut iter =
        BitmappedScan::new(mdb.iter().unwrap().map(|e| Ok(e.into())), NoBitmap);
    let len: usize = iter.by_ref().map(|_| 1).sum();
    let (mut bitmap, mut iter) = iter.unwrap().unwrap();
    bitmap.build().unwrap();
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
fn test_robt_xorfilter_scan() {
    use xorfilter::Xor8;

    let seed: u64 = random();
    let mut rng = SmallRng::seed_from_u64(seed);
    println!("test_xorfilter_scan {}", seed);

    let inserts = 1_000_000;
    let mdb = llrb::load_index::<u16, u64>(seed, 0, inserts, 0, 1_000, None);

    // with xorfilter
    let mut iter =
        BitmappedScan::new(mdb.iter().unwrap().map(|e| Ok(e.into())), Xor8::new());
    let len: usize = iter.by_ref().map(|_| 1).sum();
    let (mut bitmap, mut iter) = iter.unwrap().unwrap();
    bitmap.build().unwrap();
    assert_eq!(len, mdb.len());
    assert_eq!(iter.next(), None);
    let bitma = {
        let bytes = <Xor8 as dbs::Bloom>::to_bytes(&bitmap).unwrap();
        <Xor8 as dbs::Bloom>::from_bytes(&bytes).unwrap().0
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
