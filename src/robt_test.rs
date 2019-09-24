use rand::prelude::random;

use super::*;
use crate::core::Reader;
use crate::llrb::Llrb;
use crate::robt;
use crate::scans::SkipScan;

use rand::{rngs::SmallRng, Rng, SeedableRng};

#[test]
fn test_stats() {
    let vlog_file: &ffi::OsStr = "robt-users-level-1.vlog".as_ref();

    let stats1 = Stats {
        z_blocksize: 16384,
        m_blocksize: 4096,
        v_blocksize: 65536,
        delta_ok: true,
        vlog_file: Some(vlog_file.to_os_string()),
        value_in_vlog: true,

        n_count: 1000000,
        n_deleted: 100,
        seqno: 2000000,
        key_mem: 128000000,
        diff_mem: 512000000,
        val_mem: 1024000000,
        z_bytes: 256000000,
        v_bytes: 2048000000,
        m_bytes: 256000000,
        padding: 100000000,
        n_abytes: 0,

        build_time: 10000000000000,
        epoch: 121345678998765,
    };
    let s = stats1.to_string();
    let stats2: Stats = s.parse().unwrap();
    assert!(stats1 == stats2);

    let vlog_file: &ffi::OsStr = "robt-users-level-1.vlog".as_ref();
    let cnf = Config {
        z_blocksize: 16384,
        m_blocksize: 4096,
        v_blocksize: 65536,
        delta_ok: true,
        vlog_file: Some(vlog_file.to_os_string()),
        value_in_vlog: true,
        tomb_purge: None,
        flush_queue_size: 1024,
    };
    let stats1: Stats = cnf.into();
    let s = stats1.to_string();
    let stats2: Stats = s.parse().unwrap();
    assert!(stats1 == stats2);
}

#[test]
fn test_meta_items() {
    use std::time::SystemTime;

    let dir = std::env::temp_dir().to_str().unwrap().to_string();
    fs::remove_file(dir.clone()).ok();
    let name = "users".to_string();
    let file = Config::stitch_index_file(&dir, &name);
    fs::write(&file, [1, 2, 3, 4, 5]).unwrap();

    let n: u64 = (SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        % (std::u64::MAX as u128))
        .try_into()
        .unwrap();
    let len1 = ROOT_MARKER.len();
    let stats = <Stats as Default>::default().to_string();
    let len2 = (n % 65536) as usize;
    let app_meta: Vec<u8> = (0..len2).map(|x| (x % 256) as u8).collect();
    let len3 = stats.len();

    let meta_items = vec![
        MetaItem::Root(5),
        MetaItem::AppMetadata(app_meta.clone()),
        MetaItem::Stats(stats.clone()),
        MetaItem::Marker(ROOT_MARKER.clone()),
    ];
    let n = write_meta_items(file, meta_items).unwrap();
    let ref_n = Config::compute_root_block(32 + len1 + len2 + len3);
    assert_eq!(n, ref_n as u64);

    let iter = read_meta_items(&dir, &name).unwrap().into_iter();
    for (i, item) in iter.enumerate() {
        match (i, item) {
            (0, MetaItem::Root(value)) => assert_eq!(value, 5),
            (1, MetaItem::AppMetadata(value)) => assert_eq!(value, app_meta),
            (2, MetaItem::Stats(value)) => assert_eq!(value, stats),
            (3, MetaItem::Marker(v)) => assert_eq!(v, ROOT_MARKER.clone()),
            (i, _) => panic!("at {}, failure", i),
        }
    }
}

#[test]
fn test_config() {
    let vlog_file: &ffi::OsStr = "same-file.log".as_ref();
    let mut config1 = Config {
        z_blocksize: 1024 * 4,
        v_blocksize: 1024 * 16,
        m_blocksize: 1024 * 32,
        tomb_purge: Some(543),
        delta_ok: true,
        vlog_file: Some(vlog_file.to_os_string()),
        value_in_vlog: true,
        flush_queue_size: 1234,
    };

    let stats: Stats = config1.clone().into();
    let config2: Config = stats.into();
    assert_eq!(config2.z_blocksize, config1.z_blocksize);
    assert_eq!(config2.v_blocksize, config1.v_blocksize);
    assert_eq!(config2.m_blocksize, config1.m_blocksize);
    assert_eq!(config2.delta_ok, config1.delta_ok);
    assert_eq!(config2.vlog_file, config1.vlog_file);
    assert_eq!(config2.value_in_vlog, config1.value_in_vlog);
    assert_eq!(config2.tomb_purge, None);
    assert_eq!(config2.flush_queue_size, Config::FLUSH_QUEUE_SIZE);

    config1.set_blocksize(1024 * 8, 1024 * 32, 1024 * 64);
    config1.set_tombstone_purge(782);
    config1.set_delta(None);
    config1.set_value_log(None);
    config1.set_flush_queue_size(1023);
    assert_eq!(config1.z_blocksize, 1024 * 8);
    assert_eq!(config1.v_blocksize, 1024 * 32);
    assert_eq!(config1.m_blocksize, 1024 * 64);
    assert_eq!(config1.delta_ok, false);
    assert_eq!(config1.value_in_vlog, false);
    assert_eq!(config1.tomb_purge, Some(782));
    assert_eq!(config1.flush_queue_size, 1023);

    assert_eq!(Config::compute_root_block(4095), 4096);
    assert_eq!(Config::compute_root_block(4096), 4096);
    assert_eq!(Config::compute_root_block(4097), 8192);
    let config: Config = Default::default();
    let dir_path = std::env::temp_dir();
    let dir: &ffi::OsStr = dir_path.as_ref();
    let ref_file: &ffi::OsStr = "/tmp/robt-users.indx".as_ref();
    assert_eq!(
        config.to_index_file(dir.to_str().unwrap(), "users"),
        ref_file.to_os_string()
    );
    let ref_file: &ffi::OsStr = "/tmp/robt-users.vlog".as_ref();
    assert_eq!(
        config.to_value_log(dir.to_str().unwrap(), "users").unwrap(),
        ref_file.to_os_string()
    );
}

#[test]
fn test_robt_llrb1() {
    run_robt_llrb(6_000, 2_000_i64, 10);
    run_robt_llrb(60_000, 20_000_i64, 2);
}

#[test]
#[ignore] // TODO: long running test case
fn test_robt_llrb2() {
    run_robt_llrb(600_000, 200_000_i64, 1);
}

#[test]
#[ignore] // TODO: long running test case
fn test_robt_llrb3() {
    run_robt_llrb(6_000_000, 2_000_000_i64, 1);
}

fn run_robt_llrb(mut n_ops: u64, key_max: i64, repeat: usize) {
    for _ in 0..repeat {
        // let seed: u128 = 267758138677710051843709887156736556713;
        let seed: u128 = random();
        let mut rng = SmallRng::from_seed(seed.to_le_bytes());
        // populate llrb
        let lsm: bool = rng.gen();
        let mut llrb: Box<Llrb<i64, i64>> = if lsm {
            Llrb::new_lsm("test-llrb")
        } else {
            Llrb::new("test-llrb")
        };
        for _i in 0..n_ops {
            let key = (rng.gen::<i64>() % key_max).abs();
            match rng.gen::<usize>() % 3 {
                0 => {
                    let value: i64 = rng.gen();
                    llrb.set(key, value).unwrap();
                }
                1 => {
                    let value: i64 = rng.gen();
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
        // to avoid screwing up the seqno in non-lsm mode, say, what if
        // the last operation was a delete.
        llrb.set(123, 123456789).unwrap();
        n_ops += 1;

        // build ROBT
        let mut config: robt::Config = Default::default();
        config.delta_ok = rng.gen();
        config.value_in_vlog = rng.gen();
        let tomb_purge = match rng.gen::<u64>() % 100 {
            0..=60 => None,
            61..=70 => Some(0),
            71..=80 => Some(1),
            81..=90 => {
                let x = rng.gen::<u64>() % n_ops;
                Some(x)
            }
            91..=100 => Some(n_ops),
            _ => unreachable!(),
        };
        config.tomb_purge = tomb_purge;
        println!(
            "seed:{} n_ops:{} lsm:{} delta:{} vlog:{} tombstone:{:?}",
            seed, n_ops, lsm, config.delta_ok, config.value_in_vlog, tomb_purge
        );
        let (llrb, refs) = llrb_to_refs1(llrb, &config);
        let n_deleted: usize = refs
            .iter()
            .map(|e| if e.is_deleted() { 1 } else { 0 })
            .sum();
        // println!("refs len: {}", refs.len());
        let iter = SkipScan::new(&*llrb, ..);
        let dir = {
            let mut dir = std::env::temp_dir();
            dir.push("test-robt-build");
            dir.to_str().unwrap().to_string()
        };
        let name = "test-build";
        let b = Builder::initial(&dir, name, config.clone()).unwrap();
        let app_meta = "heloo world".to_string();
        match b.build(iter, app_meta.as_bytes().to_vec()) {
            Err(Error::EmptyIterator) if refs.len() == 0 => continue,
            Err(err) => panic!("{:?}", err),
            _ => (),
        }

        let snap = robt::Snapshot::<i64, i64>::open(&dir, name).unwrap();
        assert_eq!(snap.len(), refs.len());
        assert_eq!(snap.to_seqno(), llrb.to_seqno());
        assert_eq!(snap.to_app_meta().unwrap(), app_meta.as_bytes().to_vec());
        let stats = snap.to_stats().unwrap();
        assert_eq!(stats.z_blocksize, config.z_blocksize);
        assert_eq!(stats.m_blocksize, config.m_blocksize);
        assert_eq!(stats.v_blocksize, config.v_blocksize);
        assert_eq!(stats.delta_ok, config.delta_ok);
        assert_eq!(stats.value_in_vlog, config.value_in_vlog);
        if lsm {
            assert_eq!(stats.n_deleted, n_deleted);
        }

        // test get
        for entry in refs.iter() {
            let e = snap.get(entry.as_key()).unwrap();
            check_entry1(&entry, &e);
        }
        // test get_with_versions
        for entry in refs.iter() {
            let e = snap.get_with_versions(entry.as_key()).unwrap();
            check_entry1(&entry, &e);
            check_entry2(&entry, &e)
        }
        // test iter
        let xs = snap.iter().unwrap();
        let xs: Vec<Entry<i64, i64>> = xs.map(|e| e.unwrap()).collect();
        for (x, y) in xs.iter().zip(refs.iter()) {
            check_entry1(&x, &y)
        }
        assert_eq!(xs.len(), refs.len());
        // test iter_with_versions
        let xs = snap.iter_with_versions().unwrap();
        let xs: Vec<Entry<i64, i64>> = xs.map(|e| e.unwrap()).collect();
        assert_eq!(xs.len(), refs.len());
        for (x, y) in xs.iter().zip(refs.iter()) {
            check_entry1(&x, &y);
            check_entry2(&x, &y)
        }
        for j in 0..100 {
            // test range
            let range = random_low_high(key_max, seed + j);
            let refs = llrb_to_refs2(&llrb, range, &config);
            // println!("range bounds {:?} {}", range, refs.len());
            let xs = snap.range(range).unwrap();
            let xs: Vec<Entry<i64, i64>> = xs.map(|e| e.unwrap()).collect();
            assert_eq!(xs.len(), refs.len());
            for (x, y) in xs.iter().zip(refs.iter()) {
                check_entry1(&x, &y);
            }
            // test range_with_versions
            let range = random_low_high(key_max, seed + j);
            let refs = llrb_to_refs2(&llrb, range, &config);
            // println!("range..versions bounds {:?} {}", range, refs.len());
            let xs = snap.range_with_versions(range).unwrap();
            let xs: Vec<Entry<i64, i64>> = xs.map(|e| e.unwrap()).collect();
            assert_eq!(xs.len(), refs.len());
            for (x, y) in xs.iter().zip(refs.iter()) {
                check_entry1(&x, &y);
                check_entry2(&x, &y);
            }
            // test reverse
            let range = random_low_high(key_max, seed + j);
            let refs = llrb_to_refs3(&llrb, range, &config);
            // println!("reverse bounds {:?} {}", range, refs.len());
            let xs = snap.reverse(range).unwrap();
            let xs: Vec<Entry<i64, i64>> = xs.map(|e| e.unwrap()).collect();
            for (x, y) in xs.iter().zip(refs.iter()) {
                check_entry1(&x, &y);
            }
            // test reverse_with_versions
            let range = random_low_high(key_max, seed + j);
            let refs = llrb_to_refs3(&llrb, range, &config);
            // println!("reverse..versions bounds {:?} {}", range, refs.len());
            let xs = snap.reverse_with_versions(range).unwrap();
            let xs: Vec<Entry<i64, i64>> = xs.map(|e| e.unwrap()).collect();
            for (x, y) in xs.iter().zip(refs.iter()) {
                check_entry1(&x, &y);
                check_entry2(&x, &y);
            }
        }
    }
}

fn llrb_to_refs1(
    llrb: Box<Llrb<i64, i64>>, // reference
    config: &Config,
) -> (Box<Llrb<i64, i64>>, Vec<Entry<i64, i64>>) {
    let iter = SkipScan::new(&*llrb, ..);
    let refs = iter
        .filter_map(|e| {
            let mut e = e.unwrap();
            // println!("llrb_to_refs1 {}", e.to_key());
            if !config.delta_ok {
                e.set_deltas(vec![]);
            }
            match &config.tomb_purge {
                None => Some(e),
                Some(cutoff) => e.purge(Bound::Excluded(*cutoff)),
            }
        })
        .collect();
    (llrb, refs)
}

fn llrb_to_refs2<R>(
    llrb: &Llrb<i64, i64>, // reference
    range: R,
    config: &Config,
) -> Vec<Entry<i64, i64>>
where
    R: RangeBounds<i64>,
{
    let iter = llrb.range(range).unwrap();
    iter.filter_map(|e| {
        let mut e = e.unwrap();
        if !config.delta_ok {
            e.set_deltas(vec![]);
        }
        match &config.tomb_purge {
            None => Some(e),
            Some(cutoff) => e.purge(Bound::Excluded(*cutoff)),
        }
    })
    .collect()
}

fn llrb_to_refs3<R>(
    llrb: &Llrb<i64, i64>, // reference
    range: R,
    config: &Config,
) -> Vec<Entry<i64, i64>>
where
    R: RangeBounds<i64>,
{
    let iter = llrb.reverse(range).unwrap();
    iter.filter_map(|e| {
        let mut e = e.unwrap();
        if !config.delta_ok {
            e.set_deltas(vec![]);
        }
        match &config.tomb_purge {
            None => Some(e),
            Some(cutoff) => e.purge(Bound::Excluded(*cutoff)),
        }
    })
    .collect()
}

fn random_low_high(key_max: i64, seed: u128) -> (Bound<i64>, Bound<i64>) {
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
    let (low, high): (i64, i64) = (rng.gen(), rng.gen());
    let low = match rng.gen::<u8>() % 3 {
        0 => Bound::Included(low % key_max),
        1 => Bound::Excluded(low % key_max),
        2 => Bound::Unbounded,
        _ => unreachable!(),
    };
    let high = match rng.gen::<u8>() % 3 {
        0 => Bound::Included(high % key_max),
        1 => Bound::Excluded(high % key_max),
        2 => Bound::Unbounded,
        _ => unreachable!(),
    };
    //println!("low_high {:?} {:?}", low, high);
    (low, high)
}

fn check_entry1(e1: &Entry<i64, i64>, e2: &Entry<i64, i64>) {
    assert_eq!(e1.to_key(), e2.to_key());
    assert_eq!(e1.to_seqno(), e2.to_seqno());
    assert_eq!(e1.to_native_value(), e2.to_native_value());
    assert_eq!(e1.is_deleted(), e2.is_deleted());
    assert_eq!(e1.as_deltas().len(), e2.as_deltas().len());
}

fn check_entry2(e1: &Entry<i64, i64>, e2: &Entry<i64, i64>) {
    for (m, n) in e1.to_deltas().iter().zip(e2.to_deltas().iter()) {
        assert_eq!(m.to_seqno(), n.to_seqno());
        assert_eq!(m.is_deleted(), n.is_deleted());
        // println!("d {} {}", m.is_deleted(), e1.as_deltas().len());
        assert_eq!(m.to_diff(), n.to_diff());
    }
}
