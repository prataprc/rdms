use rand::prelude::random;

use super::*;
use crate::core::Reader;
use crate::llrb::Llrb;
use crate::robt;
use crate::scans::SkipScan;

// TODO: with mvcc

#[test]
fn test_robt_build() {
    let lsm: bool = random();
    let mut llrb: Box<Llrb<i64, i64>> = if lsm {
        Llrb::new_lsm("test-llrb")
    } else {
        Llrb::new("test-llrb")
    };
    let n_ops = 6_000;
    let key_max = 2_000;
    for _i in 0..n_ops {
        let key = ((random::<i32>() as u32) % key_max) as i64;
        match random::<usize>() % 3 {
            0 => {
                let value: i64 = random();
                llrb.set(key, value).unwrap();
            }
            1 => {
                let value: i64 = random();
                let cas = match llrb.get(&key) {
                    Err(Error::KeyNotFound) => 0,
                    Err(_err) => unreachable!(),
                    Ok(e) => e.to_seqno(),
                };
                llrb.set_cas(key, value, cas).unwrap();
            }
            2 => {
                llrb.delete(&key).unwrap();
            }
            _ => unreachable!(),
        }
    }

    let dir = {
        let mut dir = std::env::temp_dir();
        dir.push("test-robt-build");
        dir.to_str().unwrap().to_string()
    };
    let mut config: robt::Config = Default::default();
    config.delta_ok = random();
    config.value_in_vlog = random();
    config.tomb_purge = match random::<u64>() % 100 {
        0..=60 => None,
        61..=70 => Some(0),
        71..=80 => Some(1),
        81..=90 => Some(random::<u64>() % n_ops),
        91..=100 => Some(10_000_000),
        _ => unreachable!(),
    };
    println!(
        "lsm:{} delta:{} vlog:{} purge:{:?}",
        lsm, config.delta_ok, config.value_in_vlog, config.tomb_purge
    );

    let iter = SkipScan::new(&llrb, ..);
    let refs: Vec<Entry<i64, i64>> = iter.map(|e| e.unwrap()).collect();

    let iter = SkipScan::new(&llrb, ..);
    let b = Builder::initial(&dir, "test-build", config.clone()).unwrap();
    let metadata = "heloo world".to_string();
    b.build(iter, metadata.as_bytes().to_vec()).unwrap();

    for e in refs.iter() {
        println!("{}", e.to_key());
    }
    println!("total entries {}", refs.len());
}
