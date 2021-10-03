use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};
use xorfilter::BuildHasherDefault;

use crate::db::Bloom;

use super::*;

#[test]
fn test_fuse16_bitmap() {
    let seed: u128 = random();
    println!("test_fuse16 seed:{}", seed);
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let keys: Vec<u64> = (0..100_000).map(|_| rng.gen::<u64>()).collect();

    let filter = {
        let mut filter = Fuse16::<BuildHasherDefault>::new(keys.len() as u32);
        for key in keys.clone().into_iter() {
            filter.add_key(&key);
        }
        filter.build().expect("fail building fuse16 filter");
        filter
    };

    for key in keys.iter() {
        assert!(filter.contains(key), "key {} not present", key);
    }

    let filter = {
        let val = filter.to_bytes().unwrap();
        let (filter, n) = Fuse16::<BuildHasherDefault>::from_bytes(&val).unwrap();
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
