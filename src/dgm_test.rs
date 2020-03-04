use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use crate::nobitmap::NoBitmap;
use crate::{
    mvcc::{self, MvccFactory},
    robt::{self, RobtFactory},
};

use super::*;

#[test]
fn test_config_root() {
    let ref_root = Root {
        version: 0,
        levels: Config::NLEVELS,
        mono_cutoff: Default::default(),
        lsm_cutoff: Default::default(),
        tombstone_cutoff: Default::default(),

        lsm: false,
        mem_ratio: 0.25,
        disk_ratio: 0.65,
        commit_interval: time::Duration::from_secs(10),
        compact_interval: time::Duration::from_secs(10),
    };

    let ref_config = Config {
        lsm: false,
        mem_ratio: 0.25,
        disk_ratio: 0.65,
        commit_interval: time::Duration::from_secs(10),
        compact_interval: time::Duration::from_secs(10),
    };
    let root = ref_config.clone().into();
    assert_eq!(ref_root, root);

    let config: Config = root.into();
    assert_eq!(config, ref_config);
}

#[test]
fn test_root1() {
    let cutoffs = vec![
        None,
        Some(Bound::Included(101)),
        Some(Bound::Excluded(1001)),
    ];

    for cutoff in cutoffs.into_iter() {
        let ref_root = Root {
            version: 0,
            levels: Config::NLEVELS,
            mono_cutoff: cutoff,
            lsm_cutoff: cutoff,
            tombstone_cutoff: cutoff,

            lsm: true,
            mem_ratio: 0.25,
            disk_ratio: 0.65,
            commit_interval: time::Duration::from_secs(10),
            compact_interval: time::Duration::from_secs(10),
        };
        let bytes: Vec<u8> = ref_root.clone().try_into().unwrap();
        let root: Root = bytes.try_into().unwrap();
        assert_eq!(root, ref_root);
    }
}

#[test]
fn test_root2() {
    let root = Root {
        version: 0,
        levels: Config::NLEVELS,
        mono_cutoff: Default::default(),
        lsm_cutoff: Default::default(),
        tombstone_cutoff: Default::default(),

        lsm: false,
        mem_ratio: 0.25,
        disk_ratio: 0.65,
        commit_interval: time::Duration::from_secs(10),
        compact_interval: time::Duration::from_secs(10),
    };
    let root = root.to_next();
    let ref_root = Root {
        version: 1,
        levels: Config::NLEVELS,
        mono_cutoff: Default::default(),
        lsm_cutoff: Default::default(),
        tombstone_cutoff: Default::default(),

        lsm: false,
        mem_ratio: 0.25,
        disk_ratio: 0.65,
        commit_interval: time::Duration::from_secs(10),
        compact_interval: time::Duration::from_secs(10),
    };
    assert_eq!(root, ref_root);
}

#[test]
fn test_root3() {
    let mut root = Root {
        version: 0,
        levels: Config::NLEVELS,
        mono_cutoff: Default::default(),
        lsm_cutoff: Default::default(),
        tombstone_cutoff: Default::default(),

        lsm: true,
        mem_ratio: 0.25,
        disk_ratio: 0.65,
        commit_interval: time::Duration::from_secs(10),
        compact_interval: time::Duration::from_secs(10),
    };

    let cutoffs = vec![
        Cutoff::new_mono(Bound::Unbounded),
        Cutoff::new_mono(Bound::Included(101)),
        Cutoff::new_mono(Bound::Excluded(1001)),
        Cutoff::new_lsm(Bound::Unbounded),
        Cutoff::new_lsm(Bound::Included(101)),
        Cutoff::new_lsm(Bound::Excluded(1001)),
        Cutoff::new_tombstone(Bound::Unbounded),
        Cutoff::new_tombstone(Bound::Included(101)),
        Cutoff::new_tombstone(Bound::Excluded(1001)),
    ];
    let seqno = 200001;
    for cutoff in cutoffs.into_iter() {
        root.update_cutoff(cutoff, seqno);
        match cutoff {
            Cutoff::Mono(Bound::Unbounded) => {
                let cutoff = Bound::Included(seqno);
                assert_eq!(root.mono_cutoff, Some(cutoff))
            }
            Cutoff::Mono(cutoff) => assert_eq!(root.mono_cutoff, Some(cutoff)),
            Cutoff::Lsm(Bound::Unbounded) => {
                let cutoff = Bound::Included(seqno);
                assert_eq!(root.lsm_cutoff, Some(cutoff))
            }
            Cutoff::Lsm(cutoff) => assert_eq!(root.lsm_cutoff, Some(cutoff)),
            Cutoff::Tombstone(Bound::Unbounded) => {
                let cutoff = Bound::Included(seqno);
                assert_eq!(root.tombstone_cutoff, Some(cutoff))
            }
            Cutoff::Tombstone(c) => assert_eq!(root.tombstone_cutoff, Some(c)),
        }

        root.reset_cutoff(cutoff);
        match cutoff {
            Cutoff::Mono(_) => assert!(root.mono_cutoff.is_none()),
            Cutoff::Lsm(_) => assert!(root.lsm_cutoff.is_none()),
            Cutoff::Tombstone(_) => assert!(root.tombstone_cutoff.is_none()),
        }
    }

    root.mono_cutoff = Some(Bound::Included(11));
    root.tombstone_cutoff = Some(Bound::Included(101));
    root.lsm_cutoff = Some(Bound::Included(1001));
    assert_eq!(root.to_cutoff(), Cutoff::new_mono(Bound::Included(11)));

    root.reset_cutoff(Cutoff::Mono(Bound::Included(11)));
    assert_eq!(
        root.to_cutoff(),
        Cutoff::new_tombstone(Bound::Included(101))
    );

    root.reset_cutoff(Cutoff::Tombstone(Bound::Included(101)));
    assert_eq!(root.to_cutoff(), Cutoff::new_lsm(Bound::Included(1001)));

    root.reset_cutoff(Cutoff::Lsm(Bound::Included(1001)));
    assert_eq!(
        root.to_cutoff(),
        Cutoff::Lsm(Bound::Excluded(std::u64::MIN))
    );
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
    let seed: u128 = 103567949789069280795782456171344187045;
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let config = Config {
        lsm: true,
        mem_ratio: 0.5,
        disk_ratio: 0.5,
        commit_interval: time::Duration::from_secs(0),
        compact_interval: time::Duration::from_secs(0),
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
        for _ in 0..n_ops {
            let key: i64 = rng.gen::<i64>().abs() % key_max;
            let value: i64 = rng.gen::<i64>().abs();
            let op: i64 = (rng.gen::<u8>() % 2) as i64;
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
                //1 => { // TODO enable this case once Dgm.set_cas() implemented
                //    let cas = match index_r.get(&key) {
                //        Ok(entry) => entry.to_seqno(),
                //        Err(Error::KeyNotFound) => std::u64::MIN,
                //        Err(err) => panic!(err),
                //    };
                //    let entry = {
                //        let res = index_w.set_cas(key, value, cas);
                //        res.unwrap()
                //    };
                //    let refn = {
                //        let res = ref_index.set_cas(key, value, cas);
                //        res.unwrap()
                //    };
                //    match (entry, refn) {
                //        (Some(entry), Some(refn)) => {
                //            check_entry1(&entry, &refn);
                //            check_entry2(&entry, &refn);
                //        }
                //        (None, None) => (),
                //        _ => (),
                //    }
                //    false
                //}
                1 => {
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

        assert!(index.validate().is_ok());
        // println!("seqno {}", ref_index.to_seqno().unwrap());

        verify_read(key_max, &mut ref_index, &mut index, &mut rng);

        {
            let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
            let scanner = CommitIter::new(vec![].into_iter(), within);
            index.commit(scanner, convert::identity).unwrap()
        }

        verify_read(key_max, &mut ref_index, &mut index, &mut rng);

        {
            index
                .compact(Cutoff::new_lsm_empty(), convert::identity)
                .unwrap();
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
}

fn verify_read(
    key_max: i64,
    ref_index: &mut mvcc::Mvcc<i64, i64>,
    index: &mut Dgm<i64, i64, MvccFactory, RobtFactory<i64, i64, NoBitmap>>,
    rng: &mut SmallRng,
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
                (Err(Error::KeyNotFound), Err(Error::KeyNotFound)) => (),
                _ => unreachable!(),
            }

            let res = index_r.get_with_versions(&key);
            let ref_res = ref_index.get_with_versions(&key);
            match (res, ref_res) {
                (Ok(entry), Ok(ref_entry)) => {
                    check_entry1(&entry, &ref_entry);
                    check_entry2(&entry, &ref_entry);
                }
                (Err(Error::KeyNotFound), Err(Error::KeyNotFound)) => (),
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

fn random_low_high(rng: &mut SmallRng) -> (Bound<i64>, Bound<i64>) {
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
