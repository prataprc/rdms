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
        levels: NLEVELS,
        lsm_cutoff: Default::default(),
        tombstone_cutoff: Default::default(),

        mem_ratio: 0.25,
        disk_ratio: 0.65,
        commit_interval: time::Duration::from_secs(10),
        compact_interval: time::Duration::from_secs(10),
    };

    let ref_config = Config {
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
            levels: NLEVELS,
            lsm_cutoff: cutoff,
            tombstone_cutoff: cutoff,

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
        levels: NLEVELS,
        lsm_cutoff: Default::default(),
        tombstone_cutoff: Default::default(),

        mem_ratio: 0.25,
        disk_ratio: 0.65,
        commit_interval: time::Duration::from_secs(10),
        compact_interval: time::Duration::from_secs(10),
    };
    let root = root.to_next();
    let ref_root = Root {
        version: 1,
        levels: NLEVELS,
        lsm_cutoff: Default::default(),
        tombstone_cutoff: Default::default(),

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
        levels: NLEVELS,
        lsm_cutoff: Default::default(),
        tombstone_cutoff: Default::default(),

        mem_ratio: 0.25,
        disk_ratio: 0.65,
        commit_interval: time::Duration::from_secs(10),
        compact_interval: time::Duration::from_secs(10),
    };

    let cutoffs = vec![
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
            Cutoff::Lsm(Bound::Unbounded) => {
                let cutoff = Bound::Excluded(seqno);
                assert_eq!(root.lsm_cutoff, Some(cutoff))
            }
            Cutoff::Lsm(cutoff) => assert_eq!(root.lsm_cutoff, Some(cutoff)),
            Cutoff::Tombstone(Bound::Unbounded) => {
                let cutoff = Bound::Excluded(seqno);
                assert_eq!(root.tombstone_cutoff, Some(cutoff))
            }
            Cutoff::Tombstone(c) => assert_eq!(root.tombstone_cutoff, Some(c)),
        }

        root.reset_cutoff(cutoff);
        match cutoff {
            Cutoff::Lsm(_) => assert!(root.lsm_cutoff.is_none()),
            Cutoff::Tombstone(_) => assert!(root.tombstone_cutoff.is_none()),
        }
    }

    root.lsm_cutoff = Some(Bound::Included(101));
    root.tombstone_cutoff = Some(Bound::Included(1001));
    assert_eq!(root.as_cutoff(), Cutoff::new_lsm(Bound::Included(101)));

    root.reset_cutoff(Cutoff::Lsm(Bound::Included(101)));
    assert_eq!(
        root.as_cutoff(),
        Cutoff::new_tombstone(Bound::Included(1001))
    );

    root.reset_cutoff(Cutoff::Tombstone(Bound::Included(1001)));
    assert_eq!(
        root.as_cutoff(),
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
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let config = Config {
        mem_ratio: 0.5,
        disk_ratio: 0.5,
        commit_interval: time::Duration::from_secs(0),
        compact_interval: time::Duration::from_secs(0),
    };

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

    let mut index = Dgm::new(
        //
        &dir,
        "dgm-crud",
        mem_factory,
        disk_factory,
        config,
    )
    .unwrap();

    let mut index_w = index.to_writer().unwrap();
    let mut index_r = index.to_reader().unwrap();

    for _ in 0..1 {
        for _ in 0..1_00_000 {
            let key: i64 = rng.gen::<i64>().abs();
            let value: i64 = rng.gen::<i64>().abs();
            let op: i64 = (rng.gen::<u8>() % 3) as i64;
            //println!("key {} value {} op {}", key, value, op);
            match op {
                0 => {
                    let entry = index_w.set(key, value).unwrap();
                    let refn = ref_index.set(key, value).unwrap();
                    match (entry, refn) {
                        (Some(entry), Some(refn)) => {
                            check_entry1(&entry, &refn);
                            check_entry2(&entry, &refn);
                        }
                        (None, None) => break,
                        _ => unreachable!(),
                    }
                    false
                }
                1 => {
                    let cas = match index_r.get(&key) {
                        Ok(entry) => entry.to_seqno(),
                        Err(Error::KeyNotFound) => std::u64::MIN,
                        Err(err) => panic!(err),
                    };
                    let entry = index_w.set_cas(key, value, cas).ok().unwrap();
                    let refn = ref_index.set_cas(key, value, cas).ok().unwrap();
                    match (entry, refn) {
                        (Some(entry), Some(refn)) => {
                            check_entry1(&entry, &refn);
                            check_entry2(&entry, &refn);
                        }
                        (None, None) => break,
                        _ => unreachable!(),
                    }
                    false
                }
                2 => {
                    let entry = index_w.delete(&key).unwrap();
                    let refn = ref_index.delete(&key).unwrap();
                    match (entry, refn) {
                        (Some(entry), Some(refn)) => {
                            check_entry1(&entry, &refn);
                            check_entry2(&entry, &refn);
                        }
                        (None, None) => break,
                        _ => unreachable!(),
                    }
                    true
                }
                op => panic!("unreachable {}", op),
            };
        }

        // assert!(index.validate().is_ok()); TODO
        //println!("len {}", index.len());

        verify_read(&mut ref_index, &mut index, &mut rng);
    }

    mem::drop(index_w);
    mem::drop(index_r);
}

fn verify_read(
    ref_index: &mut mvcc::Mvcc<i64, i64>,
    index: &mut Dgm<i64, i64, MvccFactory, RobtFactory<i64, i64, NoBitmap>>,
    rng: &mut SmallRng,
) {
    let mut index_r = index.to_reader().unwrap();

    assert_eq!(ref_index.to_seqno().unwrap(), index.to_seqno().unwrap());

    {
        // test iter
        let mut iter = index_r.iter().unwrap();
        let mut iter_ref = ref_index.iter().unwrap();
        loop {
            let entry = iter.next().transpose().unwrap();
            let refn = iter_ref.next().transpose().unwrap();
            match (entry, refn) {
                (Some(entry), Some(refn)) => {
                    check_entry1(&entry, &refn);
                    check_entry2(&entry, &refn);
                }
                (None, None) => break,
                _ => unreachable!(),
            }
        }
    }

    // ranges and reverses
    for _ in 0..1000 {
        let (low, high) = random_low_high(rng);
        //println!("test loop {:?} {:?}", low, high);

        {
            let mut iter = index_r.range((low, high)).unwrap();
            let mut iter_ref = ref_index.range((low, high)).unwrap();
            loop {
                let entry = iter.next().transpose().unwrap();
                let refn = iter_ref.next().transpose().unwrap();
                match (entry, refn) {
                    (Some(entry), Some(refn)) => {
                        check_entry1(&entry, &refn);
                        check_entry2(&entry, &refn);
                    }
                    (None, None) => break,
                    _ => unreachable!(),
                }
            }
        }

        {
            let mut iter = index_r.reverse((low, high)).unwrap();
            let mut iter_ref = ref_index.reverse((low, high)).unwrap();
            loop {
                let entry = iter.next().transpose().unwrap();
                let refn = iter_ref.next().transpose().unwrap();
                match (entry, refn) {
                    (Some(entry), Some(refn)) => {
                        check_entry1(&entry, &refn);
                        check_entry2(&entry, &refn);
                    }
                    (None, None) => break,
                    _ => unreachable!(),
                }
            }
        }
    }
}

fn check_entry1(e1: &Entry<i64, i64>, e2: &Entry<i64, i64>) {
    assert_eq!(e1.to_key(), e2.to_key());
    let key = e1.to_key();
    assert_eq!(e1.to_seqno(), e2.to_seqno(), "key:{}", key);
    assert_eq!(e1.to_native_value(), e2.to_native_value(), "key:{}", key);
    assert_eq!(e1.is_deleted(), e2.is_deleted(), "key:{}", key);
    assert_eq!(e1.as_deltas().len(), e2.as_deltas().len(), "key:{}", key);
}

fn check_entry2(e1: &Entry<i64, i64>, e2: &Entry<i64, i64>) {
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
