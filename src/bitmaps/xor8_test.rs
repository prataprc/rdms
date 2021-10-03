use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};
use xorfilter::BuildHasherDefault;

use crate::db::Bloom;

use super::*;

#[test]
fn test_xor8_bitmap() {
    let seed: u128 = random();
    println!("test_xor8 seed:{}", seed);
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let keys: Vec<u64> = (0..100_000).map(|_| rng.gen::<u64>()).collect();

    let filter = {
        let mut filter = Xor8::<BuildHasherDefault>::new();
        for key in keys.clone().into_iter() {
            filter.add_key(&key);
        }
        filter.build().expect("fail building xor8 filter");
        filter
    };

    for key in keys.iter() {
        assert!(filter.contains(key), "key {} not present", key);
    }

    let filter = {
        let val = <Xor8 as db::Bloom>::to_bytes(&filter).unwrap();
        let (filter, n) =
            <Xor8<BuildHasherDefault> as db::Bloom>::from_bytes(&val).unwrap();
        assert_eq!(n, val.len(), "{} {}", n, val.len());
        filter
    };
    for key in keys.iter() {
        assert!(filter.contains(key), "key {} not present", key);
    }
    assert_eq!(
        filter.len(),
        Some(keys.len()),
        "{:?} {}",
        filter.len(),
        keys.len()
    );
}
