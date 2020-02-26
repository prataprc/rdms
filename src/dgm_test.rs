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
        compact_interval: time::Duration::from_secs(10),
    };

    let ref_config = Config {
        mem_ratio: 0.25,
        disk_ratio: 0.65,
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
