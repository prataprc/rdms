use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use crate::db::Bloom;

use super::*;

#[test]
fn test_croaring_bitmap() {
    let seed: u128 = random();
    println!("test_croaring seed:{}", seed);
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let mut keys: Vec<u64> = (0..100_000).map(|_| rng.gen::<u64>()).collect();
    keys.sort();
    keys.dedup();

    let filter = {
        let mut filter = CRoaring::new();
        for key in keys.iter() {
            filter.add_key(&key);
        }
        filter.build().expect("fail building croaring filter");
        filter
    };

    assert_eq!(
        filter.len(),
        Ok(keys.len()),
        "{:?} {}",
        filter.len(),
        keys.len()
    );

    for key in keys.iter() {
        assert!(filter.contains(key), "key {} not present", key);
    }

    let filter = {
        let val = filter.to_bytes().unwrap();
        let (filter, n) = CRoaring::from_bytes(&val).unwrap();
        assert_eq!(n, val.len(), "{} {}", n, val.len());
        filter
    };
    for key in keys.iter() {
        assert!(filter.contains(key), "key {} not present", key);
    }
}
