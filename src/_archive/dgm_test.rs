use rand::{prelude::random, rngs::StdRng, Rng, SeedableRng};

use crate::nobitmap::NoBitmap;
use crate::{
    mvcc::{self, MvccFactory},
    robt::{self, RobtFactory},
};

use std::convert;

use super::*;

#[test]
fn test_config_root() {
    let seed: u128 = random();
    let mut rng = StdRng::from_seed(seed.to_le_bytes());

    let m0_limit = if rng.gen::<bool>() { Some(1000) } else { None };

    let ref_root = Root {
        version: 0,
        levels: Config::NLEVELS,
        lsm_cutoff: Default::default(),
        tombstone_cutoff: Default::default(),

        lsm: false,
        m0_limit,
        mem_ratio: 0.25,
        disk_ratio: 0.65,
        commit_interval: Some(time::Duration::from_secs(10)),
        compact_interval: Some(time::Duration::from_secs(10)),
    };

    let ref_config = Config {
        lsm: false,
        m0_limit,
        mem_ratio: 0.25,
        disk_ratio: 0.65,
        commit_interval: Some(time::Duration::from_secs(10)),
        compact_interval: Some(time::Duration::from_secs(10)),
    };
    let root = ref_config.clone().into();
    assert_eq!(ref_root, root);

    let config: Config = root.into();
    assert_eq!(config, ref_config);
}

#[test]
fn test_root1() {
    let seed: u128 = random();
    let mut rng = StdRng::from_seed(seed.to_le_bytes());

    let cutoffs = vec![
        None,
        Some(Bound::Included(101)),
        Some(Bound::Excluded(1001)),
    ];
    let m0_limit = if rng.gen::<bool>() { Some(1000) } else { None };

    for cutoff in cutoffs.into_iter() {
        let ref_root = Root {
            version: 0,
            levels: Config::NLEVELS,
            lsm_cutoff: cutoff,
            tombstone_cutoff: cutoff,

            lsm: true,
            m0_limit,
            mem_ratio: 0.25,
            disk_ratio: 0.65,
            commit_interval: Some(time::Duration::from_secs(10)),
            compact_interval: Some(time::Duration::from_secs(10)),
        };
        let bytes: Vec<u8> = ref_root.clone().try_into().unwrap();
        println!("{:?}", std::str::from_utf8(&bytes));
        let root: Root = bytes.try_into().unwrap();
        assert_eq!(root, ref_root);
    }
}

#[test]
fn test_root2() {
    let root = Root {
        version: 0,
        levels: Config::NLEVELS,
        lsm_cutoff: Default::default(),
        tombstone_cutoff: Default::default(),

        lsm: false,
        m0_limit: None,
        mem_ratio: 0.25,
        disk_ratio: 0.65,
        commit_interval: Some(time::Duration::from_secs(10)),
        compact_interval: Some(time::Duration::from_secs(10)),
    };
    let root = root.to_next();
    let ref_root = Root {
        version: 1,
        levels: Config::NLEVELS,
        lsm_cutoff: Default::default(),
        tombstone_cutoff: Default::default(),

        lsm: false,
        m0_limit: None,
        mem_ratio: 0.25,
        disk_ratio: 0.65,
        commit_interval: Some(time::Duration::from_secs(10)),
        compact_interval: Some(time::Duration::from_secs(10)),
    };
    assert_eq!(root, ref_root);
}

#[test]
fn test_root3() {
    let mut root = Root {
        version: 0,
        levels: Config::NLEVELS,
        lsm_cutoff: Default::default(),
        tombstone_cutoff: Default::default(),

        lsm: true,
        m0_limit: None,
        mem_ratio: 0.25,
        disk_ratio: 0.65,
        commit_interval: Some(time::Duration::from_secs(10)),
        compact_interval: Some(time::Duration::from_secs(10)),
    };

    let cutoffs = vec![
        (
            Cutoff::new_lsm(Bound::Included(101)),
            Some(Bound::Included(101)),
            None,
        ),
        (
            Cutoff::new_lsm(Bound::Included(1)),
            Some(Bound::Included(101)),
            None,
        ),
        (
            Cutoff::new_lsm(Bound::Included(101)),
            Some(Bound::Included(101)),
            None,
        ),
        (
            Cutoff::new_lsm(Bound::Excluded(101)),
            Some(Bound::Included(101)),
            None,
        ),
        (
            Cutoff::new_lsm(Bound::Excluded(102)),
            Some(Bound::Excluded(102)),
            None,
        ),
        (
            Cutoff::new_lsm(Bound::Included(101)),
            Some(Bound::Excluded(102)),
            None,
        ),
        (
            Cutoff::new_lsm(Bound::Included(102)),
            Some(Bound::Included(102)),
            None,
        ),
        (
            Cutoff::new_lsm(Bound::Unbounded),
            Some(Bound::Included(200001)),
            None,
        ),
        (
            Cutoff::new_tombstone(Bound::Included(1001)),
            Some(Bound::Included(200001)),
            Some(Bound::Included(1001)),
        ),
        (
            Cutoff::new_tombstone(Bound::Included(1)),
            Some(Bound::Included(200001)),
            Some(Bound::Included(1001)),
        ),
        (
            Cutoff::new_tombstone(Bound::Included(1001)),
            Some(Bound::Included(200001)),
            Some(Bound::Included(1001)),
        ),
        (
            Cutoff::new_tombstone(Bound::Excluded(1001)),
            Some(Bound::Included(200001)),
            Some(Bound::Included(1001)),
        ),
        (
            Cutoff::new_tombstone(Bound::Excluded(1002)),
            Some(Bound::Included(200001)),
            Some(Bound::Excluded(1002)),
        ),
        (
            Cutoff::new_tombstone(Bound::Included(1001)),
            Some(Bound::Included(200001)),
            Some(Bound::Excluded(1002)),
        ),
        (
            Cutoff::new_tombstone(Bound::Included(1002)),
            Some(Bound::Included(200001)),
            Some(Bound::Included(1002)),
        ),
        (
            Cutoff::new_tombstone(Bound::Unbounded),
            Some(Bound::Included(200001)),
            Some(Bound::Included(200001)),
        ),
    ];
    let seqno = 200001;
    for (cutoff, lref, tref) in cutoffs.into_iter() {
        root.update_cutoff(cutoff, seqno).unwrap();
        assert_eq!(root.lsm_cutoff, lref);
        assert_eq!(root.tombstone_cutoff, tref);
    }
}

#[test]
fn test_root_file_name() {
    let s = "my-index-dgm-000.root".to_string();
    let ss: &ffi::OsStr = s.as_ref();

    let root = RootFileName(ss.to_os_string());
    let (name, ver): (String, usize) = root.try_into().unwrap();
    assert_eq!(name, "my-index".to_string());
    assert_eq!(ver, 0);

    let root: RootFileName = (name, 1).into();
    assert_eq!(root.0, "my-index-dgm-001.root");

    let fname: ffi::OsString = root.clone().into();
    assert_eq!(fname, "my-index-dgm-001.root");

    assert_eq!(root.to_string(), "my-index-dgm-001.root".to_string());
}

#[test]
fn test_level_file_name() {
    let name = "my-index-dgmlevel-000".to_string();

    let level_name = LevelName(name.clone());
    let (name, level): (String, usize) = level_name.try_into().unwrap();
    assert_eq!(name, "my-index".to_string());
    assert_eq!(level, 0);

    let level_name: LevelName = (name, 1).into();
    assert_eq!(level_name.to_string(), "my-index-dgmlevel-001");

    assert_eq!(level_name.to_string(), "my-index-dgmlevel-001".to_string());
    assert_eq!(
        format!("{:?}", level_name),
        format!("{:?}", "my-index-dgmlevel-001"),
    );
}

#[test]
fn test_dgm_crud() {
    let seed: u128 = random();
    // let seed: u128 = 244047294379045884577463665526896848685;
    let mut rng = StdRng::from_seed(seed.to_le_bytes());

    let config = Config {
        lsm: true,
        m0_limit: None,
        mem_ratio: 0.5,
        disk_ratio: 0.5,
        commit_interval: None,
        compact_interval: None,
    };

    println!("seed: {}", seed);

    let mut ref_index = mvcc::Mvcc::new_lsm("dgm-crud");

    let dir = {
        let mut dir = std::env::temp_dir();
        dir.push("test-dgm-crud");
        dir.into_os_string()
    };
    let mem_factory = mvcc::mvcc_factory(true /*lsm*/);
    let disk_factory = {
        let mut config: robt::Config = Default::default();
        config.delta_ok = true;
        config.value_in_vlog = true;
        robt::robt_factory::<i64, i64, NoBitmap>(config)
    };
    let open: bool = rng.gen();

    let mut index = Dgm::new(
        //
        &dir,
        "dgm-crud",
        mem_factory,
        disk_factory,
        config.clone(),
    )
    .unwrap();

    let n_ops = 1_000;
    let key_max = n_ops * 3;
    for _i in 0..20 {
        // println!("loop {}", _i);
        let mut index_w = index.to_writer().unwrap();
        let mut index_r = index.to_reader().unwrap();
        for _ in 0..n_ops {
            let key: i64 = rng.gen::<i64>().abs() % key_max;
            let value: i64 = rng.gen::<i64>().abs();
            let op: i64 = (rng.gen::<u8>() % 3) as i64;
            let _seqno = ref_index.to_seqno().unwrap();
            //println!(
            //    "i:{} key:{} value:{} op:{} seqno:{}",
            //    _i, key, value, op, _seqno
            //);
            match op {
                0 => {
                    let entry = index_w.set(key, value).unwrap();
                    let refn = ref_index.set(key, value).unwrap();
                    match (entry, refn) {
                        (Some(entry), Some(refn)) => {
                            check_entry1(&entry, &refn);
                        }
                        (None, None) => (),
                        _ => (),
                    }
                    false
                }
                1 => {
                    let cas = match index_r.get(&key) {
                        Ok(entry) => entry.to_seqno(),
                        Err(Error::NotFound) => std::u64::MIN,
                        Err(err) => panic!(err),
                    };
                    let entry = {
                        let res = index_w.set_cas(key, value, cas);
                        res.unwrap()
                    };
                    let refn = {
                        let res = ref_index.set_cas(key, value, cas);
                        res.unwrap()
                    };
                    match (entry, refn) {
                        (Some(entry), Some(refn)) => {
                            check_entry1(&entry, &refn);
                        }
                        (None, None) => (),
                        _ => (),
                    }
                    false
                }
                2 => {
                    let entry = index_w.delete(&key).unwrap();
                    let refn = ref_index.delete(&key).unwrap();
                    match (entry, refn) {
                        (Some(entry), Some(refn)) => {
                            check_entry1(&entry, &refn);
                        }
                        (None, None) => (),
                        _ => (),
                    }
                    true
                }
                op => panic!("unreachable {}", op),
            };
        }
        mem::drop(index_w);
        mem::drop(index_r);

        index.validate().unwrap();
        // println!("seqno {}", ref_index.to_seqno().unwrap());

        verify_read(key_max, &mut ref_index, &mut index, &mut rng);

        {
            index
                .commit(CommitIter::new_empty(), convert::identity)
                .unwrap()
        }

        verify_read(key_max, &mut ref_index, &mut index, &mut rng);

        {
            index.compact(Cutoff::new_lsm_empty()).unwrap();
        }

        verify_read(key_max, &mut ref_index, &mut index, &mut rng);

        {
            let mem_factory = mvcc::mvcc_factory(true /*lsm*/);
            let disk_factory = {
                let mut config: robt::Config = Default::default();
                config.delta_ok = true;
                config.value_in_vlog = true;
                robt::robt_factory::<i64, i64, NoBitmap>(config)
            };
            if open {
                index = Dgm::open(
                    //
                    &dir,
                    "dgm-crud",
                    mem_factory,
                    disk_factory,
                )
                .unwrap();
            }
        }

        verify_read(key_max, &mut ref_index, &mut index, &mut rng);
    }

    // try some set_cas invalid cas.
    let mut index_w = index.to_writer().unwrap();
    for key in 0..(key_max * 3) {
        let cas = match ref_index.get(&key) {
            Ok(e) => e.to_seqno(),
            Err(Error::NotFound) => 0,
            Err(err) => panic!("unexpected error: {:?}", err),
        };
        let cas_arg = {
            let cass = vec![cas, 123456789];
            cass[rng.gen::<usize>() % 2]
        };
        match (cas_arg, index_w.set_cas(key, 10000, cas_arg)) {
            (123456789, Err(Error::InvalidCAS(val))) => assert_eq!(val, cas),
            (123456789, Ok(_)) => panic!("expected error"),
            (_, Ok(_)) => (),
            (_, Err(err)) => {
                panic!("unexpected cas:{} cas_arg:{} err:{:?}", cas, cas_arg, err)
            }
        }
    }
}

#[test]
fn test_dgm_non_lsm() {
    let seed: u128 = {
        let ss: Vec<u128> = vec![
            10975319741753784730289078611426332775,
            random(),
            random(),
            random(),
        ];
        ss[random::<usize>() % 2]
    };
    // let seed: u128 = 10975319741753784730289078611426332775;
    let mut rng = StdRng::from_seed(seed.to_le_bytes());

    let config = Config {
        lsm: false, // non-lsm
        m0_limit: None,
        mem_ratio: 0.5,
        disk_ratio: 0.5,
        commit_interval: None,
        compact_interval: None,
    };

    let dir = {
        let mut dir = std::env::temp_dir();
        dir.push("test-dgm-non-lsm");
        dir.into_os_string()
    };
    let mem_factory = mvcc::mvcc_factory(true /*lsm*/);
    let disk_factory = {
        let mut config: robt::Config = Default::default();
        config.delta_ok = true;
        config.value_in_vlog = true;
        robt::robt_factory::<i64, i64, NoBitmap>(config)
    };
    let mut index = Dgm::new(
        //
        &dir,
        "dgm-non-lsm",
        mem_factory,
        disk_factory,
        config.clone(),
    )
    .unwrap();

    let n_ops = 1_000;
    let key_max = n_ops * 3;
    let cycles: usize = rng.gen::<usize>() % 20;
    println!("seed:{} cycles:{}", seed, cycles);
    for _i in 0..cycles {
        // println!("loop {}", _i);
        let mut index_w = index.to_writer().unwrap();
        let mut index_r = index.to_reader().unwrap();
        for _ in 0..n_ops {
            let key: i64 = rng.gen::<i64>().abs() % key_max;
            let value: i64 = rng.gen::<i64>().abs();
            let op: i64 = (rng.gen::<u8>() % 3) as i64;
            let _seqno = index.to_seqno().unwrap();
            //println!(
            //    "i:{} key:{} value:{} op:{} seqno:{}",
            //    _i, key, value, op, _seqno
            //);
            match op {
                0 => index_w.set(key, value).unwrap(),
                1 => {
                    let cas = match index_r.get(&key) {
                        Ok(entry) => entry.to_seqno(),
                        Err(Error::NotFound) => std::u64::MIN,
                        Err(err) => panic!(err),
                    };
                    index_w.set_cas(key, value, cas).unwrap()
                }
                2 => index_w.delete(&key).unwrap(),
                op => panic!("unreachable {}", op),
            };
        }
        mem::drop(index_w);
        mem::drop(index_r);

        {
            let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
            let scanner = CommitIter::new(vec![].into_iter(), within);
            index.commit(scanner, convert::identity).unwrap()
        }

        {
            index.compact(Cutoff::new_lsm_empty()).unwrap();
        }
    }

    index.validate().unwrap();
}

#[test]
fn test_dgm_cutoffs() {
    let seed: u128 = {
        let ss: Vec<u128> = vec![
            28033407443451930364604529838062294466,
            random(),
            random(),
            random(),
        ];
        ss[random::<usize>() % 2]
    };
    // let seed: u128 = 28033407443451930364604529838062294466;
    let mut rng = StdRng::from_seed(seed.to_le_bytes());

    let config = Config {
        lsm: false, // non-lsm
        m0_limit: None,
        mem_ratio: 0.5,
        disk_ratio: 0.5,
        commit_interval: None,
        compact_interval: None,
    };

    let dir = {
        let mut dir = std::env::temp_dir();
        dir.push("test-dgm-cutoffs");
        dir.into_os_string()
    };
    let mem_factory = mvcc::mvcc_factory(true /*lsm*/);
    let disk_factory = {
        let mut config: robt::Config = Default::default();
        config.delta_ok = true;
        config.value_in_vlog = true;
        robt::robt_factory::<i64, i64, NoBitmap>(config)
    };
    let mut index = Dgm::new(
        //
        &dir,
        "dgm-cutoffs",
        mem_factory,
        disk_factory,
        config.clone(),
    )
    .unwrap();

    let n_ops = 1_000;
    let key_max = n_ops * 3;
    let cycles: usize = rng.gen::<usize>() % 20;
    println!("seed:{} cycles:{}", seed, cycles);
    for _i in 0..cycles {
        // println!("loop {}", _i);
        let mut index_w = index.to_writer().unwrap();
        let mut index_r = index.to_reader().unwrap();
        for _ in 0..n_ops {
            let key: i64 = rng.gen::<i64>().abs() % key_max;
            let value: i64 = rng.gen::<i64>().abs();
            let op: i64 = (rng.gen::<u8>() % 3) as i64;
            let _seqno = index.to_seqno().unwrap();
            //println!(
            //    "i:{} key:{} value:{} op:{} seqno:{}",
            //    _i, key, value, op, _seqno
            //);
            match op {
                0 => index_w.set(key, value).unwrap(),
                1 => {
                    let cas = match index_r.get(&key) {
                        Ok(entry) => entry.to_seqno(),
                        Err(Error::NotFound) => std::u64::MIN,
                        Err(err) => panic!(err),
                    };
                    index_w.set_cas(key, value, cas).unwrap()
                }
                2 => index_w.delete(&key).unwrap(),
                op => panic!("unreachable {}", op),
            };
        }
        mem::drop(index_w);
        mem::drop(index_r);

        {
            let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
            let scanner = CommitIter::new(vec![].into_iter(), within);
            index.commit(scanner, convert::identity).unwrap()
        }

        {
            let seqno = random::<u64>() % index.to_seqno().unwrap();
            let cutoff = match random::<usize>() % 6 {
                0 => Cutoff::new_lsm(Bound::Excluded(seqno)),
                1 => Cutoff::new_lsm(Bound::Included(seqno)),
                2 => Cutoff::new_lsm(Bound::Unbounded),
                3 => Cutoff::new_tombstone(Bound::Excluded(seqno)),
                4 => Cutoff::new_tombstone(Bound::Included(seqno)),
                5 => Cutoff::new_tombstone(Bound::Unbounded),
                _ => unreachable!(),
            };
            index.compact(cutoff).unwrap();
        }
    }

    index.validate().unwrap();
}

fn verify_read(
    key_max: i64,
    ref_index: &mut mvcc::Mvcc<i64, i64>,
    index: &mut Dgm<i64, i64, MvccFactory, RobtFactory<i64, i64, NoBitmap>>,
    rng: &mut StdRng,
) {
    let mut index_r = index.to_reader().unwrap();

    assert_eq!(ref_index.to_seqno().unwrap(), index.to_seqno().unwrap());

    {
        for key in 0..key_max {
            let key = key as i64;
            let res = index_r.get(&key);
            let ref_res = ref_index.get(&key);
            match (res, ref_res) {
                (Ok(entry), Ok(ref_entry)) => check_entry1(&entry, &ref_entry),
                (Err(Error::NotFound), Err(Error::NotFound)) => (),
                _ => unreachable!(),
            }

            let res = index_r.get_with_versions(&key);
            let ref_res = ref_index.get_with_versions(&key);
            match (res, ref_res) {
                (Ok(entry), Ok(ref_entry)) => {
                    check_entry1(&entry, &ref_entry);
                    check_entry2(&entry, &ref_entry);
                }
                (Err(Error::NotFound), Err(Error::NotFound)) => (),
                (res, ref_res) => {
                    println!("res:{} ref_res:{}", res.is_ok(), ref_res.is_ok());
                    match res {
                        Err(err) => panic!("{:?} {}", err, key),
                        _ => (),
                    };
                    match ref_res {
                        Err(err) => panic!("{:?} {}", err, key),
                        _ => (),
                    };
                    unreachable!();
                }
            }
        }
    }

    {
        let iter = index_r.iter().unwrap();
        let ref_iter = ref_index.iter().unwrap();
        verify_iter(iter, ref_iter);

        let iter = index_r.iter_with_versions().unwrap();
        let ref_iter = ref_index.iter_with_versions().unwrap();
        verify_iter_vers(iter, ref_iter);
    }

    // ranges and reverses
    for _ in 0..100 {
        let (low, high) = random_low_high(rng);
        // println!("test loop {:?} {:?}", low, high);

        {
            let iter = index_r.range((low, high)).unwrap();
            let ref_iter = ref_index.range((low, high)).unwrap();
            verify_iter(iter, ref_iter);

            let iter = index_r.range_with_versions((low, high)).unwrap();
            let ref_iter = ref_index.range_with_versions((low, high)).unwrap();
            verify_iter_vers(iter, ref_iter);
        }

        {
            let iter = index_r.reverse((low, high)).unwrap();
            let ref_iter = ref_index.reverse((low, high)).unwrap();
            verify_iter(iter, ref_iter);

            let iter = index_r.range_with_versions((low, high)).unwrap();
            let ref_iter = ref_index.range_with_versions((low, high)).unwrap();
            verify_iter_vers(iter, ref_iter);
        }
    }
}

fn verify_iter(mut iter: IndexIter<i64, i64>, mut ref_iter: IndexIter<i64, i64>) {
    loop {
        let refn = ref_iter.next();
        let entry = iter.next();
        match (entry, refn) {
            (Some(Ok(entry)), Some(Ok(refn))) => {
                check_entry1(&entry, &refn);
            }
            (Some(Err(err)), Some(Ok(refn))) => {
                let key = refn.to_key();
                panic!("verify key:{} {:?}", key, err)
            }
            (None, None) => break,
            _ => unreachable!(),
        }
    }
}

fn verify_iter_vers(
    mut iter: IndexIter<i64, i64>,
    mut ref_iter: IndexIter<i64, i64>, // ref
) {
    loop {
        let refn = ref_iter.next();
        let entry = iter.next();
        match (entry, refn) {
            (Some(Ok(entry)), Some(Ok(refn))) => {
                check_entry1(&entry, &refn);
                check_entry2(&entry, &refn);
            }
            (Some(Err(err)), Some(Ok(refn))) => {
                let key = refn.to_key();
                panic!("verify key:{} {:?}", key, err)
            }
            (None, None) => break,
            _ => unreachable!(),
        }
    }
}

fn check_entry1(e1: &Entry<i64, i64>, e2: &Entry<i64, i64>) {
    assert_eq!(e1.to_key(), e2.to_key());
    let key = e1.to_key();
    assert_eq!(e1.to_seqno(), e2.to_seqno(), "key:{}", key);
    assert_eq!(e1.to_native_value(), e2.to_native_value(), "key:{}", key);
    assert_eq!(e1.is_deleted(), e2.is_deleted(), "key:{}", key);
}

fn check_entry2(e1: &Entry<i64, i64>, e2: &Entry<i64, i64>) {
    let key = e1.to_key();
    assert_eq!(e1.as_deltas().len(), e2.as_deltas().len(), "key:{}", key);

    let key = e1.to_key();
    let xs: Vec<core::Delta<i64>> = e1.to_deltas();
    let ys: Vec<core::Delta<i64>> = e2.to_deltas();

    assert_eq!(xs.len(), ys.len(), "for key {}", key);
    for (m, n) in xs.iter().zip(ys.iter()) {
        assert_eq!(m.to_seqno(), n.to_seqno(), "for key {}", key);
        assert_eq!(m.is_deleted(), n.is_deleted(), "for key {}", key);
        // println!("d {} {}", m.is_deleted(), n.is_deleted());
        assert_eq!(m.to_diff(), n.to_diff(), "for key {}", key);
        // println!("key:{} diff {:?} {:?}", key, m.to_diff(), n.to_diff());
    }
}

fn random_low_high(rng: &mut StdRng) -> (Bound<i64>, Bound<i64>) {
    let low: i64 = rng.gen();
    let high: i64 = rng.gen();
    let low = match rng.gen::<u8>() % 3 {
        0 => Bound::Included(low),
        1 => Bound::Excluded(low),
        2 => Bound::Unbounded,
        _ => unreachable!(),
    };
    let high = match rng.gen::<u8>() % 3 {
        0 => Bound::Included(high),
        1 => Bound::Excluded(high),
        2 => Bound::Unbounded,
        _ => unreachable!(),
    };
    //println!("low_high {:?} {:?}", low, high);
    (low, high)
}
