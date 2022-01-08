use rand::{prelude::random, rngs::StdRng, Rng, SeedableRng};

use super::*;

#[test]
fn test_llrb_depth() {
    let seed: u64 = random();
    println!("test_llrb_depth seed:{}", seed);
    let mut rng = StdRng::seed_from_u64(seed);

    let mut depths = [0_usize; 256];
    let (mut val, n_samples) = (Depth::default(), rng.gen::<usize>() % 1_000_000);
    println!("test_llrb_depth n_samples:{}", n_samples);
    for _ in 0..n_samples {
        let d = rng.gen::<u8>();
        depths[d as usize] += 1;
        val.sample(d as usize);
    }

    assert_eq!(val.to_samples(), n_samples);
    {
        let min = depths
            .to_vec()
            .into_iter()
            .enumerate()
            .find(|(_, c)| *c != 0)
            .map(|x| x.0)
            .unwrap_or(usize::MAX);
        assert_eq!(val.to_min(), min);
    }
    {
        let max = depths
            .to_vec()
            .into_iter()
            .enumerate()
            .rev()
            .find(|(_, c)| *c != 0)
            .map(|x| x.0)
            .unwrap_or(usize::MIN);
        assert_eq!(val.to_max(), max);
    }
    {
        let total: usize = depths.iter().enumerate().map(|(d, c)| d * (*c)).sum();
        let count: usize = depths.to_vec().into_iter().sum();
        assert_eq!(val.to_mean(), total / count);
    }
    // TODO: test case for to_percentiles()
}
