use fs2::FileExt;
use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use super::*;
use crate::{
    core::{Delta, Index, Reader, Writer},
    croaring::CRoaring,
    llrb::Llrb,
    nobitmap::NoBitmap,
    robt,
    scans::{FilterScan, IterChain, SkipScan},
};

#[test]
fn test_name() {
    let name = Name("somename-0-robt-000".to_string());
    assert_eq!(name.to_string(), "somename-0-robt-000".to_string());

    let (s, n): (String, usize) = TryFrom::try_from(name.clone()).unwrap();
    assert_eq!(s, "somename-0".to_string());
    assert_eq!(n, 0);

    let name1: Name = (s, n).into();
    assert_eq!(name.0, name1.0);

    assert_eq!(name1.next().0, "somename-0-robt-001".to_string());
}

#[test]
fn test_index_file_name() {
    let name = Name("somename-0-robt-000".to_string());
    let iname: IndexFileName = name.clone().into();
    assert_eq!(iname.to_string(), "somename-0-robt-000.indx".to_string());

    let name1: Name = TryFrom::try_from(iname.clone()).unwrap();
    assert_eq!(name.0, name1.0);

    let file_name: ffi::OsString = iname.into();
    let ref_name: &ffi::OsStr = "somename-0-robt-000.indx".as_ref();
    assert_eq!(file_name, ref_name.to_os_string());
}

#[test]
fn test_vlog_file_name() {
    let name = Name("somename-0-robt-000".to_string());
    let iname: VlogFileName = name.into();
    assert_eq!(iname.to_string(), "somename-0-robt-000.vlog".to_string());
    let ref_name: &ffi::OsStr = "somename-0-robt-000.vlog".as_ref();
    assert_eq!(iname.0, ref_name.to_os_string());
}

#[test]
fn test_version() {
    use crate::nobitmap::NoBitmap;

    for i in 0..1000 {
        let name = Name(format!("somename-0-robt-{:03}", i));
        let inner = InnerRobt::<i64, i64, NoBitmap>::Build {
            dir: Default::default(),
            name: name.clone(),
            config: Default::default(),
            purge_tx: Default::default(),
            _phantom_key: marker::PhantomData,
            _phantom_val: marker::PhantomData,
        };
        let index = Robt {
            inner: sync::Mutex::new(inner),
            purger: None,
        };
        assert_eq!(index.version().unwrap(), i);

        let inner = InnerRobt::<i64, i64, NoBitmap>::Snapshot {
            dir: Default::default(),
            name,
            footprint: Default::default(),
            meta: Default::default(),
            config: Default::default(),
            stats: Default::default(),
            bitmap: Arc::new(NoBitmap),
            purge_tx: Default::default(),
        };
        let index = Robt {
            inner: sync::Mutex::new(inner),
            purger: None,
        };
        assert_eq!(index.version().unwrap(), i);
    }
}

#[test]
fn test_stats() {
    let vlog_file: &ffi::OsStr = "robt-users-level-1.vlog".as_ref();

    let stats1 = Stats {
        name: "test_stats".to_string(),
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
        mem_bitmap: 12310000,
        n_bitmap: 1000000,
        padding: 100000000,
        n_abytes: 0,

        build_time: 10000000000000,
        epoch: 121345678998765,
    };
    let s = stats1.to_json();
    let stats2: Stats = s.parse().unwrap();
    assert!(stats1 == stats2);

    let vlog_file: &ffi::OsStr = "robt-users-level-1.vlog".as_ref();
    let dir: &ffi::OsStr = "/path/to/dummy/dir".as_ref();
    let cnf = Config {
        dir: dir.to_os_string(),
        name: "test_stats".to_string(),
        z_blocksize: 16384,
        m_blocksize: 4096,
        v_blocksize: 65536,
        delta_ok: true,
        vlog_file: Some(vlog_file.to_os_string()),
        value_in_vlog: true,
        flush_queue_size: 1024,
    };
    let stats1: Stats = cnf.into();
    let s = stats1.to_json();
    let stats2: Stats = s.parse().unwrap();
    assert!(stats1 == stats2);
}

#[test]
fn test_stats_merge() {
    let stats1 = Stats {
        name: "test_stats".to_string(),
        z_blocksize: 16384,
        m_blocksize: 4096,
        v_blocksize: 65536,
        delta_ok: true,
        vlog_file: None,
        value_in_vlog: true,

        n_count: 1,
        n_deleted: 1,
        seqno: 1,
        key_mem: 1,
        diff_mem: 1,
        val_mem: 1,
        z_bytes: 1,
        v_bytes: 1,
        m_bytes: 1,
        mem_bitmap: 1,
        n_bitmap: 1,
        padding: 1,
        n_abytes: 2,

        build_time: 1,
        epoch: 1,
    };
    let stats2 = Stats {
        name: "test_stats".to_string(),
        z_blocksize: 16384,
        m_blocksize: 4096,
        v_blocksize: 65536,
        delta_ok: true,
        vlog_file: None,
        value_in_vlog: true,

        n_count: 2,
        n_deleted: 2,
        seqno: 2,
        key_mem: 2,
        diff_mem: 2,
        val_mem: 2,
        z_bytes: 2,
        v_bytes: 2,
        m_bytes: 2,
        mem_bitmap: 2,
        n_bitmap: 2,
        padding: 2,
        n_abytes: 2,

        build_time: 2,
        epoch: 2,
    };

    let stats = stats1.merge(stats2);
    assert_eq!(stats.z_blocksize, 16384);
    assert_eq!(stats.m_blocksize, 4096);
    assert_eq!(stats.v_blocksize, 65536);
    assert_eq!(stats.delta_ok, true);
    assert_eq!(stats.vlog_file, None);
    assert_eq!(stats.value_in_vlog, true);

    assert_eq!(stats.n_count, 3);
    assert_eq!(stats.n_deleted, 3);
    assert_eq!(stats.seqno, 2);
    assert_eq!(stats.key_mem, 3);
    assert_eq!(stats.diff_mem, 3);
    assert_eq!(stats.val_mem, 3);
    assert_eq!(stats.z_bytes, 3);
    assert_eq!(stats.v_bytes, 3);
    assert_eq!(stats.m_bytes, 3);
    assert_eq!(stats.mem_bitmap, 3);
    assert_eq!(stats.n_bitmap, 3);
    assert_eq!(stats.padding, 3);
    assert_eq!(stats.n_abytes, 0);
    assert_eq!(stats.build_time, 0);
    assert_eq!(stats.epoch, 0);
}

#[test]
fn test_meta_items() {
    use std::time::SystemTime;

    let dir = std::env::temp_dir().into_os_string();
    fs::remove_file(dir.clone()).ok();
    let name = "test-meta-items-users-robt-0".to_string();
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
    let stats = <Stats as Default>::default().to_json();
    let len2 = (n % 65536) as usize;
    let app_meta: Vec<u8> = (0..len2).map(|x| (x % 256) as u8).collect();
    let len3 = stats.len();

    let meta_items = vec![
        MetaItem::Root(5),
        MetaItem::Bitmap(vec![]),
        MetaItem::AppMetadata(app_meta.clone()),
        MetaItem::Stats(stats.clone()),
        MetaItem::Marker(ROOT_MARKER.clone()),
    ];
    let n = write_meta_items(file, meta_items).unwrap();
    let ref_n = Config::compute_root_block(32 + len1 + len2 + len3);
    assert_eq!(n, ref_n as u64);

    let iter = read_meta_items(&dir, &name).unwrap().0.into_iter();
    for (i, item) in iter.enumerate() {
        match (i, item) {
            (0, MetaItem::Root(value)) => assert_eq!(value, 5),
            (1, MetaItem::Bitmap(value)) => assert_eq!(value, vec![]),
            (2, MetaItem::AppMetadata(value)) => assert_eq!(value, app_meta),
            (3, MetaItem::Stats(value)) => assert_eq!(value, stats),
            (4, MetaItem::Marker(v)) => assert_eq!(v, ROOT_MARKER.clone()),
            (i, _) => panic!("at {}, failure", i),
        }
    }
}

#[test]
fn test_config() {
    let vlog_file: &ffi::OsStr = "same-file.log".as_ref();
    let dir: &ffi::OsStr = "/path/to/dummy/dir".as_ref();
    let mut config1 = Config {
        dir: dir.to_os_string(),
        name: "test_config".to_string(),
        z_blocksize: 1024 * 4,
        v_blocksize: 1024 * 16,
        m_blocksize: 1024 * 32,
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
    assert_eq!(config2.flush_queue_size, Config::FLUSH_QUEUE_SIZE);

    config1.set_blocksize(1024 * 8, 1024 * 32, 1024 * 64);
    config1.set_delta(None, false);
    config1.set_value_log(None, false);
    config1.set_flush_queue_size(1023);
    assert_eq!(config1.z_blocksize, 1024 * 8);
    assert_eq!(config1.v_blocksize, 1024 * 32);
    assert_eq!(config1.m_blocksize, 1024 * 64);
    assert_eq!(config1.delta_ok, false);
    assert_eq!(config1.value_in_vlog, false);
    assert_eq!(config1.flush_queue_size, 1023);

    assert_eq!(Config::compute_root_block(4095), 4096);
    assert_eq!(Config::compute_root_block(4096), 4096);
    assert_eq!(Config::compute_root_block(4097), 8192);

    let dir_path = std::env::temp_dir();
    let dir = dir_path.clone().into_os_string();
    let ref_file = {
        let mut rpath = path::PathBuf::new();
        rpath.push(dir_path.clone());
        rpath.push("users.indx");
        rpath.into_os_string()
    };
    assert_eq!(
        Config::stitch_index_file(&dir, "users"),
        ref_file.to_os_string()
    );
    let ref_file = {
        let mut rpath = path::PathBuf::new();
        rpath.push(dir_path.clone());
        rpath.push("users.vlog");
        rpath.into_os_string()
    };
    assert_eq!(
        Config::stitch_vlog_file(&dir, "users"),
        ref_file.to_os_string()
    );
}

#[test]
fn test_robt_llrb1() {
    let seed: u128 = random();
    // let seed: u128 = 279765853267557126686238657580803488536;
    run_robt_llrb("test-robt-llrb1-1", 60_000, 20_000_i64, 2, seed);
    println!("test_robt_llrb1 first run ...");
    run_robt_llrb("test-robt-llrb1-2", 6_000, 2_000_i64, 10, seed);
    println!("test_robt-llrb1 second run ...");
    run_robt_llrb("test-robt-llrb1-3", 60_000, 20_000_i64, 2, seed);
}

#[test]
#[ignore] // TODO: long running test case
fn test_robt_llrb2() {
    let seed: u128 = random();
    run_robt_llrb("test-robt-llrb2", 600_000, 200_000_i64, 1, seed);
}

#[test]
#[ignore] // TODO: long running test case
fn test_robt_llrb3() {
    let seed: u128 = random();
    run_robt_llrb("test-robt-llrb3", 6_000_000, 2_000_000_i64, 1, seed);
}

#[test]
fn test_purger() {
    let file = {
        let mut dir = std::env::temp_dir();
        dir.push("test-purger-purge-file.data");
        dir.into_os_string()
    };
    let (mut files, mut efiles) = (vec![], vec![]);

    assert_eq!(purge_file(file.clone(), &mut files, &mut efiles), "error");
    assert_eq!(files.len(), 0);
    assert_eq!(efiles.len(), 1);
    assert_eq!(efiles[0], file);
    efiles.remove(0);

    fs::File::create(&file).unwrap();
    assert_eq!(purge_file(file.clone(), &mut files, &mut efiles), "ok");
    assert_eq!(files.len(), 0);
    assert_eq!(efiles.len(), 0);

    let fd = fs::File::create(&file).unwrap();
    fd.lock_shared().unwrap();
    assert_eq!(purge_file(file.clone(), &mut files, &mut efiles), "locked");
    assert_eq!(files.len(), 1);
    assert_eq!(files[0], file);
    assert_eq!(efiles.len(), 0);
    fd.unlock().unwrap();

    fd.lock_exclusive().unwrap();
    assert_eq!(purge_file(file.clone(), &mut files, &mut efiles), "locked");
    assert_eq!(files.len(), 2);
    assert_eq!(files[0], file);
    assert_eq!(files[1], file);
    assert_eq!(efiles.len(), 0);
    fd.unlock().unwrap();
    files.remove(0);
    files.remove(0);

    assert_eq!(purge_file(file.clone(), &mut files, &mut efiles), "ok");
    assert_eq!(files.len(), 0);
    assert_eq!(efiles.len(), 0);
}

#[test]
fn test_build_scan() {
    let seed: u128 = random();

    for _i in 0..100 {
        let (n_ops, key_max) = (6_000_i64, 2_000);
        let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
        random_llrb(n_ops, key_max, seed, &mut llrb);

        let stats = {
            let mut scanner = BuildScan::new(llrb.iter().unwrap());
            loop {
                match scanner.next() {
                    Some(_) => (),
                    None => break,
                }
            }

            let mut stats: Stats = Default::default();
            let mut iter = scanner.update_stats(&mut stats).unwrap();
            assert!(iter.next().is_none());
            stats
        };
        assert_eq!(stats.seqno, llrb.to_seqno());
        assert_eq!(stats.n_count as usize, llrb.to_stats().entries);
        assert_eq!(stats.n_deleted, llrb.to_stats().n_deleted);
    }
}

#[test]
fn test_commit_scan() {
    let seed: u128 = random();
    // let seed: u128 = 329574334243588244341656545742438834233;
    println!("seed:{}", seed);
    let dir = {
        let mut dir = std::env::temp_dir();
        dir.push("test-commit-scan");
        dir.into_os_string()
    };
    let mut config: robt::Config = Default::default();
    config.delta_ok = true;
    config.value_in_vlog = true;
    let app_meta = "heloo world".to_string();

    for i in 0..50 {
        let mut llrb: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
        let mut snap1 = {
            let (n_ops, key_max, config) = (30_000_i64, 20_000, config.clone());
            let mut llrb_snap: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
            random_llrb(n_ops, key_max, seed + (i + 1) * 10, &mut llrb_snap);
            let es: Vec<Result<Entry<i64, i64>>> = llrb_snap
                .iter()
                .unwrap()
                .map(|e| Ok(e.as_ref().unwrap().clone()))
                .collect();
            llrb.commit(
                CommitIter::new(
                    es.into_iter(),
                    (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded),
                ),
                |val| val,
            )
            .unwrap();

            let b = Builder::<i64, i64, NoBitmap>::initial(&dir, "snapshot1", config).unwrap();
            b.build(llrb_snap.iter().unwrap(), app_meta.as_bytes().to_vec())
                .unwrap();
            robt::Snapshot::<i64, i64, NoBitmap>::open(&dir, "snapshot1").unwrap()
        };
        let stats1 = snap1.to_stats().unwrap();
        let fp1 = snap1.footprint().unwrap();
        // println!("flushed snap1");

        let mut snap2 = {
            let (n_ops, key_max, config) = (30_000_i64, 20_000, config.clone());
            let mut llrb_snap: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
            llrb_snap.set_seqno(llrb.to_seqno());
            random_llrb(n_ops, key_max, seed + (i + 1) * 20, &mut llrb_snap);
            let es: Vec<Result<Entry<i64, i64>>> = llrb_snap
                .iter()
                .unwrap()
                .map(|e| Ok(e.as_ref().unwrap().clone()))
                .collect();
            llrb.commit(
                CommitIter::new(
                    es.into_iter(),
                    (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded),
                ),
                |val| val,
            )
            .unwrap();

            let b = Builder::<i64, i64, NoBitmap>::initial(&dir, "snapshot2", config).unwrap();
            b.build(llrb_snap.iter().unwrap(), app_meta.as_bytes().to_vec())
                .unwrap();
            robt::Snapshot::<i64, i64, NoBitmap>::open(&dir, "snapshot2").unwrap()
        };
        let stats2 = snap2.to_stats().unwrap();
        let fp2 = snap2.footprint().unwrap();
        // println!("flushed snap2");

        let config: Config = stats1.clone().into();
        let new_scanner = snap2.iter_with_versions().unwrap();
        let commit_scanner = {
            let mut mzs = vec![];
            snap1.build_fwd(snap1.to_root().unwrap(), &mut mzs).unwrap();
            CommitScan::new(new_scanner, Iter::new_shallow(&mut snap1, mzs))
        };
        let b = Builder::<i64, i64, NoBitmap>::incremental(&dir, "snapshot3", config).unwrap();
        b.build(commit_scanner, app_meta.as_bytes().to_vec())
            .unwrap();
        let mut snap = robt::Snapshot::<i64, i64, NoBitmap>::open(&dir, "snapshot3").unwrap();
        let stats = snap.to_stats().unwrap();
        let fp = snap.footprint().unwrap();

        println!(
            "snap1:{}/{}, snap2:{}/{}, snap:{}/{}",
            stats1.n_count, fp1, stats2.n_count, fp2, stats.n_count, fp,
        );

        let mut iter = snap.iter_with_versions().unwrap();
        for ref_entry in llrb.iter().unwrap() {
            let ref_entry = ref_entry.unwrap();
            match iter.next() {
                Some(Ok(entry)) => {
                    check_entry1(&entry, &ref_entry);
                    check_entry2(&entry, &ref_entry);
                }
                Some(Err(err)) => panic!("{:?}", err),
                None => panic!("entry key:{} not found", ref_entry.to_key()),
            }
        }
        assert!(iter.next().is_none());
    }
}

#[test]
fn test_commit_iterator_scan() {
    let seed: u128 = random();
    // let seed: u128 = 329574334243588244341656545742438834233;
    println!("seed:{}", seed);
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let dir = {
        let mut dir = std::env::temp_dir();
        dir.push("test-commit-iterator-scan");
        println!("temp dir {:?}", dir);
        dir.into_os_string()
    };
    let mut config: robt::Config = Default::default();
    config.delta_ok = true;
    config.value_in_vlog = true;
    let robtf = robt_factory::<i64, i64, NoBitmap>(config);

    for i in 0..50 {
        let (n_ops, key_max) = match rng.gen::<u8>() % 3 {
            1 => (1_i64, 20_000),
            _n => {
                let n_ops = i64::abs(rng.gen::<i64>()) % 30_000;
                let key_max = (i64::abs(rng.gen::<i64>()) % n_ops) + n_ops;
                (n_ops, key_max)
            }
        };

        let mut llrb_snap: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
        random_llrb(n_ops, key_max, seed + (i + 1) * 10, &mut llrb_snap);
        println!("n_ops:{}, key_max:{}", n_ops, key_max);

        let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);

        let mut index = robtf.new(&dir, "snapshot-scan").unwrap();

        let c_entries: Vec<Result<Entry<i64, i64>>> = llrb_snap
            .to_reader()
            .unwrap()
            .iter_with_versions()
            .unwrap()
            .collect();
        index
            .commit(
                CommitIter::new(c_entries.into_iter(), within.clone()),
                std::convert::identity,
            )
            .unwrap();

        let mut iter = index.scan(within).unwrap();

        let mut r = llrb_snap.to_reader().unwrap();
        let ref_iter = r.iter_with_versions().unwrap();
        for ref_entry in ref_iter {
            let ref_entry = ref_entry.unwrap();
            match iter.next() {
                Some(Ok(entry)) => {
                    check_entry1(&entry, &ref_entry);
                    check_entry2(&entry, &ref_entry);
                }
                Some(Err(err)) => panic!("{:?}", err),
                None => panic!("entry key:{} not found", ref_entry.to_key()),
            }
        }
        assert!(iter.next().is_none());
    }
}

#[test]
fn test_commit_iterator_scans() {
    let seed: u128 = random();
    // let seed: u128 = 329574334243588244341656545742438834233;
    println!("seed:{}", seed);
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let dir = {
        let mut dir = std::env::temp_dir();
        dir.push("test-commit-iterator-scans");
        println!("temp dir {:?}", dir);
        dir.into_os_string()
    };

    let mut config: robt::Config = Default::default();
    config.delta_ok = true;
    config.value_in_vlog = true;
    let robtf = robt_factory::<i64, i64, NoBitmap>(config);

    for i in 0..50 {
        let (n_ops, key_max) = match rng.gen::<u8>() % 3 {
            1 => (1_i64, 20_000),
            _n => {
                let n_ops = i64::abs(rng.gen::<i64>()) % 30_000;
                let key_max = (i64::abs(rng.gen::<i64>()) % n_ops) + n_ops;
                (n_ops, key_max)
            }
        };

        let mut llrb_snap: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
        random_llrb(n_ops, key_max, seed + (i + 1) * 10, &mut llrb_snap);
        println!("i:{} n_ops:{}, key_max:{}", i, n_ops, key_max);

        let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);

        let mut index = robtf.new(&dir, "snapshot-scans").unwrap();
        let c_entries: Vec<Result<Entry<i64, i64>>> = llrb_snap
            .to_reader()
            .unwrap()
            .iter_with_versions()
            .unwrap()
            .collect();
        index
            .commit(
                CommitIter::new(c_entries.into_iter(), within.clone()),
                std::convert::identity,
            )
            .unwrap();

        let shards = (i + 1) as usize;
        let iters = index.scans(shards, within).unwrap();
        let mut iter = IterChain::new(iters);

        let mut r = llrb_snap.to_reader().unwrap();
        let ref_iter = r.iter_with_versions().unwrap();
        for ref_entry in ref_iter {
            let ref_entry = ref_entry.unwrap();
            match iter.next() {
                Some(Ok(entry)) => {
                    check_entry1(&entry, &ref_entry);
                    check_entry2(&entry, &ref_entry);
                }
                Some(Err(err)) => panic!("{:?}", err),
                None => panic!("entry key:{} not found", ref_entry.to_key()),
            }
        }
        assert!(iter.next().is_none());
    }
}

#[test]
fn test_commit_iterator_range_scans() {
    let seed: u128 = random();
    // let seed: u128 = 329574334243588244341656545742438834233;
    println!("seed:{}", seed);
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let dir = {
        let mut dir = std::env::temp_dir();
        dir.push("test-commit-iterator-range-scans");
        println!("temp dir {:?}", dir);
        dir.into_os_string()
    };

    let mut config: robt::Config = Default::default();
    config.delta_ok = true;
    config.value_in_vlog = true;
    let robtf = robt_factory::<i64, i64, NoBitmap>(config);

    for i in 0..50 {
        let (n_ops, key_max) = match rng.gen::<u8>() % 3 {
            1 => (1_i64, 20_000),
            _n => {
                let n_ops = i64::abs(rng.gen::<i64>()) % 30_000;
                let key_max = (i64::abs(rng.gen::<i64>()) % n_ops) + n_ops;
                (n_ops, key_max)
            }
        };

        let mut llrb_snap: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
        random_llrb(n_ops, key_max, seed + (i + 1) * 10, &mut llrb_snap);
        println!("i:{} n_ops:{}, key_max:{}", i, n_ops, key_max);

        let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);

        let mut index = robtf.new(&dir, "snapshot-range-scans").unwrap();
        let c_entries: Vec<Result<Entry<i64, i64>>> = llrb_snap
            .to_reader()
            .unwrap()
            .iter_with_versions()
            .unwrap()
            .collect();
        index
            .commit(
                CommitIter::new(c_entries.into_iter(), within.clone()),
                std::convert::identity,
            )
            .unwrap();

        let shards = (i + 1) as usize;
        let (last_hk, mut ranges) = llrb_snap
            .scans(shards, within.clone())
            .unwrap()
            .into_iter()
            .fold((Bound::Unbounded, vec![]), |(lk, mut acc), mut iter| {
                let hk = iter.next().unwrap().unwrap().to_key();
                acc.push((lk, Bound::Excluded(hk.clone())));
                (Bound::Included(hk), acc)
            });
        ranges.push((last_hk, Bound::Unbounded));
        let iters = index.range_scans(ranges, within).unwrap();
        let mut iter = IterChain::new(iters);

        let mut r = llrb_snap.to_reader().unwrap();
        let ref_iter = r.iter_with_versions().unwrap();
        for ref_entry in ref_iter {
            let ref_entry = ref_entry.unwrap();
            match iter.next() {
                Some(Ok(entry)) => {
                    check_entry1(&entry, &ref_entry);
                    check_entry2(&entry, &ref_entry);
                }
                Some(Err(err)) => panic!("{:?}", err),
                None => panic!("entry key:{} not found", ref_entry.to_key()),
            }
        }
        assert!(iter.next().is_none());
    }
}

fn run_robt_llrb(name: &str, n_ops: u64, key_max: i64, repeat: usize, seed: u128) {
    for i in 0..repeat {
        let mut n_ops = n_ops;
        let seed = seed + (i as u128);
        let mut rng = SmallRng::from_seed(seed.to_le_bytes());
        // populate llrb
        let lsm: bool = rng.gen();
        let sticky: bool = rng.gen();
        let mut llrb: Box<Llrb<i64, i64>> = if lsm {
            Llrb::new_lsm("test-llrb")
        } else {
            Llrb::new("test-llrb")
        };
        llrb.set_sticky(sticky);

        random_llrb(n_ops as i64, key_max, seed, &mut llrb);

        // to avoid screwing up the seqno in non-lsm mode, say, what if
        // the last operation was a delete.
        llrb.set(123, 123456789).unwrap();
        n_ops += 1;

        // build ROBT
        let mut config: robt::Config = Default::default();
        config.delta_ok = lsm;
        config.value_in_vlog = rng.gen();
        let within = match rng.gen::<u64>() % 100 {
            0..=60 => (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded),
            61..=70 => (Bound::<u64>::Excluded(1), Bound::<u64>::Unbounded),
            71..=80 => (Bound::<u64>::Included(1), Bound::<u64>::Unbounded),
            81..=90 => {
                let x = rng.gen::<u64>() % n_ops;
                (Bound::<u64>::Excluded(x), Bound::<u64>::Unbounded)
            }
            91..=100 => (Bound::<u64>::Excluded(n_ops), Bound::<u64>::Unbounded),
            _ => unreachable!(),
        };
        let mmap = rng.gen::<bool>();
        println!(
            "seed:{} n_ops:{} lsm:{} sticky:{} delta:{} vlog:{} within:{:?} mmap:{}",
            seed, n_ops, lsm, sticky, config.delta_ok, config.value_in_vlog, within, mmap,
        );
        let (mut llrb, refs) = llrb_to_refs1(llrb, within.clone(), &config);
        let n_deleted: usize = refs
            .iter()
            .map(|e| if e.is_deleted() { 1 } else { 0 })
            .sum();
        // println!("refs len: {}", refs.len());
        let mut iter = SkipScan::new(llrb.to_reader().unwrap());
        iter.set_seqno_range(within);
        let dir = {
            let mut dir = std::env::temp_dir();
            dir.push("test-robt-build");
            dir.into_os_string()
        };
        let b = Builder::<i64, i64, CRoaring>::initial(&dir, name, config.clone()).unwrap();
        let app_meta = "heloo world".to_string();
        match b.build(iter, app_meta.as_bytes().to_vec()) {
            Err(Error::DiskIndexFail(msg)) => {
                if msg.contains("empty iterator") && refs.len() == 0 {
                    continue;
                } else {
                    panic!("{:?}", Error::DiskIndexFail(msg));
                }
            }
            Err(err) => panic!("{:?}", err),
            _ => (),
        }

        let mut snap = robt::Snapshot::<i64, i64, CRoaring>::open(&dir, name).unwrap();
        snap.validate().unwrap();
        snap.set_mmap(mmap).unwrap();
        assert_eq!(snap.len(), refs.len());
        assert_eq!(snap.to_seqno(), llrb.to_seqno());
        assert_eq!(snap.to_app_meta().unwrap(), app_meta.as_bytes().to_vec());
        let stats = snap.to_stats().unwrap();
        assert_eq!(stats.z_blocksize, config.z_blocksize);
        assert_eq!(stats.m_blocksize, config.m_blocksize);
        assert_eq!(stats.v_blocksize, config.v_blocksize);
        assert_eq!(stats.delta_ok, config.delta_ok);
        assert_eq!(stats.value_in_vlog, config.value_in_vlog);
        if lsm || sticky {
            assert_eq!(stats.n_deleted, n_deleted);
        }

        // test get
        for entry in refs.iter() {
            let e = snap.get(entry.as_key()).unwrap();
            check_entry1(&entry, &e);
        }

        // test first entry
        let ref_entry = refs.first().unwrap();
        let ref_entry = ref_entry
            .clone()
            .purge(Bound::Excluded(ref_entry.to_seqno()))
            .unwrap();
        let entry = snap.first().unwrap();
        let entry = entry
            .clone()
            .purge(Bound::Excluded(entry.to_seqno()))
            .unwrap();
        check_entry1(&entry, &ref_entry);
        check_entry1(&snap.first_versions().unwrap(), &refs.first().unwrap());
        // test last entry
        let ref_entry = refs.last().unwrap();
        let ref_entry = ref_entry
            .clone()
            .purge(Bound::Excluded(ref_entry.to_seqno()))
            .unwrap();
        let entry = snap.last().unwrap();
        let entry = entry
            .clone()
            .purge(Bound::Excluded(entry.to_seqno()))
            .unwrap();
        check_entry1(&entry, &ref_entry);
        check_entry1(&snap.last_versions().unwrap(), &refs.last().unwrap());

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
            let refs = llrb_to_refs2(&mut llrb, range, within.clone(), &config);
            // println!("range bounds {:?} {}", range, refs.len());
            let xs = snap.range(range).unwrap();
            let xs: Vec<Entry<i64, i64>> = xs.map(|e| e.unwrap()).collect();
            assert_eq!(xs.len(), refs.len());
            for (x, y) in xs.iter().zip(refs.iter()) {
                check_entry1(&x, &y);
            }
            // test range_with_versions
            let range = random_low_high(key_max, seed + j);
            let refs = llrb_to_refs2(&mut llrb, range, within.clone(), &config);
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
            let refs = llrb_to_refs3(&mut llrb, range, within.clone(), &config);
            // println!("reverse bounds {:?} {}", range, refs.len());
            let xs = snap.reverse(range).unwrap();
            let xs: Vec<Entry<i64, i64>> = xs.map(|e| e.unwrap()).collect();
            assert_eq!(xs.len(), refs.len());
            for (x, y) in xs.iter().zip(refs.iter()) {
                check_entry1(&x, &y);
            }
            // test reverse_with_versions
            let range = random_low_high(key_max, seed + j);
            let refs = llrb_to_refs3(&mut llrb, range, within.clone(), &config);
            // println!("reverse..versions bounds {:?} {}", range, refs.len());
            let xs = snap.reverse_with_versions(range).unwrap();
            let xs: Vec<Entry<i64, i64>> = xs.map(|e| e.unwrap()).collect();
            assert_eq!(xs.len(), refs.len());
            for (x, y) in xs.iter().zip(refs.iter()) {
                check_entry1(&x, &y);
                check_entry2(&x, &y);
            }
        }
    }
}

fn llrb_to_refs1(
    mut llrb: Box<Llrb<i64, i64>>, // reference
    within: (Bound<u64>, Bound<u64>),
    config: &Config,
) -> (Box<Llrb<i64, i64>>, Vec<Entry<i64, i64>>) {
    let mut iter = SkipScan::new(llrb.to_reader().unwrap());
    iter.set_seqno_range(within);
    let refs = iter
        .filter_map(|e| {
            let mut e = e.unwrap();
            // println!("llrb_to_refs1 {}", e.to_key());
            if !config.delta_ok {
                e.set_deltas(vec![]);
            }
            Some(e)
        })
        .collect();
    (llrb, refs)
}

fn llrb_to_refs2<R>(
    llrb: &mut Llrb<i64, i64>, // reference
    range: R,
    within: (Bound<u64>, Bound<u64>),
    config: &Config,
) -> Vec<Entry<i64, i64>>
where
    R: Clone + RangeBounds<i64>,
{
    let iter = FilterScan::new(llrb.range(range).unwrap(), within);
    iter.filter_map(|e| {
        let mut e = e.unwrap();
        if !config.delta_ok {
            e.set_deltas(vec![]);
        }
        Some(e)
    })
    .collect()
}

fn llrb_to_refs3<R>(
    llrb: &mut Llrb<i64, i64>, // reference
    range: R,
    within: (Bound<u64>, Bound<u64>),
    config: &Config,
) -> Vec<Entry<i64, i64>>
where
    R: Clone + RangeBounds<i64>,
{
    let iter = FilterScan::new(llrb.reverse(range).unwrap(), within);
    iter.filter_map(|e| {
        let mut e = e.unwrap();
        if !config.delta_ok {
            e.set_deltas(vec![]);
        }
        Some(e)
    })
    .collect()
}

fn random_llrb(n_ops: i64, key_max: i64, seed: u128, llrb: &mut Llrb<i64, i64>) {
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
    for _i in 0..n_ops {
        let key = (rng.gen::<i64>() % key_max).abs();
        let op = rng.gen::<usize>() % 3;
        match op {
            0 => {
                let value: i64 = rng.gen();
                // println!("key {} {} {} {}", key, llrb.to_seqno(), op, value);
                llrb.set(key, value).unwrap();
            }
            1 => {
                let value: i64 = rng.gen();
                // println!("key {} {} {} {}", key, llrb.to_seqno(), op, value);
                {
                    let cas = match llrb.get(&key) {
                        Err(Error::KeyNotFound) => 0,
                        Err(_err) => unreachable!(),
                        Ok(e) => e.to_seqno(),
                    };
                    llrb.set_cas(key, value, cas).unwrap();
                }
            }
            2 => {
                // println!("key {} {} {}", key, llrb.to_seqno(), op);
                llrb.delete(&key).unwrap();
            }
            _ => unreachable!(),
        }
    }
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
    let key = e1.to_key();
    assert_eq!(e1.to_seqno(), e2.to_seqno(), "key:{}", key);
    assert_eq!(e1.to_native_value(), e2.to_native_value(), "key:{}", key);
    assert_eq!(e1.is_deleted(), e2.is_deleted(), "key:{}", key);
    assert_eq!(e1.as_deltas().len(), e2.as_deltas().len(), "key:{}", key);
}

fn check_entry2(e1: &Entry<i64, i64>, e2: &Entry<i64, i64>) {
    let key = e1.to_key();
    let xs: Vec<Delta<i64>> = e1.to_deltas();
    let ys: Vec<Delta<i64>> = e2.to_deltas();

    assert_eq!(xs.len(), ys.len(), "for key {}", key);
    for (m, n) in xs.iter().zip(ys.iter()) {
        assert_eq!(m.to_seqno(), n.to_seqno(), "for key {}", key);
        assert_eq!(m.is_deleted(), n.is_deleted(), "for key {}", key);
        // println!("d {} {}", m.is_deleted(), n.is_deleted());
        assert_eq!(m.to_diff(), n.to_diff(), "for key {}", key);
        // println!("key:{} diff {:?} {:?}", key, m.to_diff(), n.to_diff());
    }
}
