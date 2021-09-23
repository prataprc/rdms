use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use super::*;

#[test]
fn test_llrb_depth() {
    let seed: u128 = random();
    println!("test_llrb_depth seed:{}", seed);
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

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
            .skip_while(|(_, c)| *c == 0)
            .next()
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
            .skip_while(|(_, c)| *c == 0)
            .next()
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
