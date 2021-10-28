use super::*;

// TODO: review mod.rs and mod_test.rs after full refactoring.

#[test]
fn test_as_sharded_array() {
    for i in 0..100 {
        let array: Vec<i32> = (0..i).collect();
        for n_shards in 0..100 {
            let acc = as_sharded_array(&array, n_shards);
            assert_eq!(acc.len(), n_shards);
            assert!(acc.len() <= n_shards, "{} {}", acc.len(), n_shards);
            if n_shards > 0 {
                let res: Vec<i32> = {
                    let iter = acc.iter().flat_map(|shard| shard.to_vec());
                    iter.collect()
                };
                assert_eq!(array, res);
            }
        }
    }
}
