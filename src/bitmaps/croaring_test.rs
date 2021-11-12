use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use crate::db::Bloom;

use super::*;

#[test]
fn test_croaring_bitmap() {
    let seed: u64 = random();
    let mut rng = SmallRng::seed_from_u64(seed);
    println!("test_croaring seed:{}", seed);

    let keys: Vec<u64> = (0..100_000).map(|_| rng.gen::<u64>()).collect();

    let mut digests = vec![];
    let filter = {
        let mut filter = CRoaring::new();
        for key in keys.iter() {
            let digest = {
                let mut hasher = Hash128.build_hasher();
                key.hash(&mut hasher);
                let code: u64 = hasher.finish();
                (((code >> 32) ^ code) & 0xFFFFFFFF) as u32
            };
            digests.push(digest);

            filter.add_key(&key);
        }
        filter.build().expect("fail building croaring filter");
        filter
    };
    digests.sort_unstable();
    digests.dedup();
    println!("digests {}", digests.len());

    assert_eq!(
        filter.len(),
        Ok(digests.len()),
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
