use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use super::*;
use crate::{
    core::{self, Delta, Index, Reader, Writer},
    croaring::CRoaring,
    llrb::Llrb,
    nobitmap::NoBitmap,
    robt, scans,
};

#[test]
fn test_root_file_name() {
    let s = "my-index-shrobt.root".to_string();
    let ss: &ffi::OsStr = s.as_ref();

    let root = RootFileName(ss.to_os_string());
    let name: String = root.try_into().unwrap();
    assert_eq!(name, "my-index".to_string());

    let root: RootFileName = name.into();
    assert_eq!(root.0, ss.to_os_string());

    let fname: ffi::OsString = root.clone().into();
    let refname: &ffi::OsStr = root.0.as_ref();
    assert_eq!(fname, refname.to_os_string());

    let root = RootFileName(ss.to_os_string());
    assert_eq!(root.to_string(), "my-index-shrobt.root".to_string());
}

#[test]
fn test_shard_name() {
    let sname = ShardName("my-index-shrobt-shard-001".to_string());
    let (name, shard_id) = sname.try_into().unwrap();
    assert_eq!(name, "my-index".to_string());
    assert_eq!(shard_id, 1);

    let sname: ShardName = (name, shard_id).into();
    assert_eq!(sname.0, "my-index-shrobt-shard-001".to_string());

    let sname = ShardName("my-index-shrobt-shard-001".to_string());
    assert_eq!(sname.to_string(), "my-index-shrobt-shard-001".to_string());
}

#[test]
fn test_root_file() {
    let seed: u64 = random();
    let mut rng = SmallRng::seed_from_u64(seed);

    let dir = {
        let mut dir = std::env::temp_dir();
        dir.push("test-shrobt-build");
        dir.into_os_string()
    };

    let num_shards = (rng.gen::<usize>() % 8) + 1;
    let name = "test-root-file";
    let root = ShRobt::<i64, i64, NoBitmap>::new_root_file(
        //
        &dir,
        name,
        Root { num_shards },
    )
    .unwrap();
    assert!(root
        .as_os_str()
        .to_str()
        .unwrap()
        .contains("test-root-file"));

    let root = ShRobt::<i64, i64, NoBitmap>::open_root_file(&dir, &root).unwrap();
    assert_eq!(num_shards, root.num_shards);
}

#[test]
fn test_shrobt_llrb1() {
    let seed: u64 = random();
    println!("seed: {}", seed);
    run_shrobt_llrb("test-shrobt-llrb1-1", 60_000, 20_000_i64, 2, seed);
    println!("test_shrobt_llrb1 first run ...");
    run_shrobt_llrb("test-shrobt-llrb1-2", 6_000, 2_000_i64, 10, seed);
    println!("test_shrobt-llrb1 second run ...");
    run_shrobt_llrb("test-shrobt-llrb1-3", 60_000, 20_000_i64, 2, seed);
}

#[test]
#[ignore]
fn test_shrobt_llrb2() {
    let seed: u64 = random();
    run_shrobt_llrb("test-shrobt-llrb2", 600_000, 200_000_i64, 1, seed);
}

#[test]
#[ignore]
fn test_shrobt_llrb3() {
    let seed: u64 = random();
    run_shrobt_llrb("test-shrobt-llrb3", 6_000_000, 2_000_000_i64, 1, seed);
}

#[test]
fn test_shrobt_commit_compact() {
    let seed: u64 = random();
    let mut rng = SmallRng::seed_from_u64(seed);

    let name = "test-shrobt-commit-compact";
    let num_shards = (rng.gen::<usize>() % 8) + 1;
    let mmap = rng.gen::<bool>();

    let mut mindex: Box<Llrb<i64, i64>> = Llrb::new_lsm(name);

    let dir = {
        let mut dir = std::env::temp_dir();
        dir.push(name);
        dir.into_os_string()
    };
    fs::remove_dir_all(&dir).ok();

    println!("seed:{} dir:{:?}", seed, dir);

    let mut config: robt::Config = Default::default();
    config.delta_ok = true;
    config.value_in_vlog = rng.gen();
    let mut index = ShRobt::<i64, i64, CRoaring>::new(
        //
        &dir,
        name,
        config.clone(),
        num_shards,
        mmap,
    )
    .unwrap();

    // populate mindex
    let cycles = rng.gen::<usize>() % 10 + 1;
    let mut within_low_seqno = Bound::<u64>::Unbounded;
    for i in 0..cycles {
        let (mut n_ops, key_max) = random_ops_keys(
            //
            seed + (i as u128 * 100),
            6_000_000,
            2_000_000_i64,
        );
        random_llrb(n_ops as i64, key_max, seed, &mut mindex);

        // to avoid screwing up the seqno in non-lsm mode, say, what if
        // the last operation was a delete.
        mindex.set(123, 123456789).unwrap();
        n_ops += 1;

        // build ShRobt
        let within = (within_low_seqno, Bound::<u64>::Unbounded);

        {
            print!(
                "n_ops:{} key_max:{} delta:{} ",
                n_ops, key_max, config.delta_ok,
            );
            println!(
                "vlog:{} num_shards:{:?} within:{:?} mmap:{} cycles:{}",
                config.value_in_vlog, num_shards, within, mmap, cycles
            );
        }

        let app_meta = "heloo world".to_string();
        let scanner = core::CommitIter::new(mindex.as_mut(), within.clone());
        index
            .commit(scanner, |_| app_meta.as_bytes().to_vec())
            .unwrap();

        if rng.gen::<bool>() {
            let cutoff = match rng.gen::<u8>() % 3 {
                0 => Bound::Excluded(rng.gen::<u64>() % (n_ops as u64) / 2),
                1 => Bound::Included(rng.gen::<u64>() % (n_ops as u64) / 2),
                2 => Bound::Unbounded,
                _ => unreachable!(),
            };
            let cutoff = match rng.gen::<u8>() % 2 {
                0 => Cutoff::new_tombstone(cutoff),
                1 => Cutoff::new_lsm(cutoff),
                _ => unreachable!(),
            };
            println!("cutoff {:?}", cutoff);
            mindex.compact(cutoff).unwrap();
            index.compact(cutoff).unwrap();
        }

        index = {
            println!("dir:{:?} name:{:?}", dir, name);
            match index.close() {
                Ok(()) => (),
                Err(Error::PurgeFiles(files)) => {
                    println!("purge-files {:?}", files);
                    for file in files {
                        fs::remove_file(&file).unwrap();
                    }
                }
                Err(err) => panic!("{:?}", err),
            }
            ShRobt::<i64, i64, CRoaring>::open(&dir, name, mmap).unwrap()
        };

        within_low_seqno = Bound::Excluded(index.to_seqno().unwrap());
    }

    //// test iter
    let refs: Vec<Entry<i64, i64>> = {
        let mut r = mindex.to_reader().unwrap();
        let iter = r.iter_with_versions().unwrap();
        iter.map(|e| e.unwrap()).collect()
    };
    let es: Vec<Entry<i64, i64>> = {
        let mut r = index.to_reader().unwrap();
        let iter = r.iter_with_versions().unwrap();
        iter.map(|e| e.unwrap()).collect()
    };
    assert_eq!(es.len(), refs.len());
    for (e, re) in es.into_iter().zip(refs.into_iter()) {
        //println!(
        //    "check key {} {} {} {}",
        //    e.to_key(),
        //    re.to_key(),
        //    e.to_seqno(),
        //    e.to_seqno()
        //);
        check_entry1(&e, &re);
        check_entry2(&e, &re);
    }

    match index.validate() {
        Err(Error::EmptyIndex) if mindex.len() == 0 => (),
        Err(err) => panic!("{:?}", err),
        Ok(_) => (),
    }
}

#[test]
fn test_commit_iterator_scan() {
    let seed: u64 = random();
    let mut rng = SmallRng::seed_from_u64(seed);
    println!("seed:{}", seed);

    let dir = {
        let mut dir = std::env::temp_dir();
        dir.push("shrobt-test-commit-iterator-scan");
        println!("temp dir {:?}", dir);
        dir.into_os_string()
    };
    for i in 0..50 {
        let mut config: robt::Config = Default::default();
        config.delta_ok = true;
        config.value_in_vlog = true;
        let num_shards = (rng.gen::<usize>() % 8) + 1;
        let mmap = rng.gen::<bool>();
        let shrobtf = shrobt_factory::<i64, i64, NoBitmap>(
            //
            config, num_shards, mmap,
        );

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

        let mut index = shrobtf.new(&dir, "commit-iterator-scan").unwrap();

        let c_entries: Vec<Result<Entry<i64, i64>>> = llrb_snap
            .to_reader()
            .unwrap()
            .iter_with_versions()
            .unwrap()
            .collect();
        index
            .commit(
                core::CommitIter::new(c_entries.into_iter(), within.clone()),
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
fn test_commit_iterator_scans1() {
    let seed: u64 = random();
    let mut rng = SmallRng::seed_from_u64(seed);
    println!("seed:{}", seed);

    let dir = {
        let mut dir = std::env::temp_dir();
        dir.push("shrobt-test-commit-iterator-scans1");
        println!("temp dir {:?}", dir);
        dir.into_os_string()
    };

    for i in 0..50 {
        let mut config: robt::Config = Default::default();
        config.delta_ok = true;
        config.value_in_vlog = true;

        let num_shards = (rng.gen::<usize>() % 8) + 1;
        let mmap = rng.gen::<bool>();
        let shrobtf = shrobt_factory::<i64, i64, NoBitmap>(
            //
            config, num_shards, mmap,
        );
        let (n_ops, key_max) = random_ops_keys(
            //
            seed + (i * 100),
            100_000,
            300_000,
        );

        let mut mindex: Box<Llrb<i64, i64>> = Llrb::new_lsm("test-llrb");
        random_llrb(n_ops, key_max, seed + (i + 1) * 10, &mut mindex);
        println!(
            "i:{} n_ops:{}, key_max:{} num_shards:{} mmap:{}",
            i, n_ops, key_max, num_shards, mmap
        );

        let within = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);

        let mut index = shrobtf.new(&dir, "snapshot-scans").unwrap();
        let iter = {
            let iters = mindex.scans(num_shards, within.clone()).unwrap();
            core::CommitIter::new(scans::CommitWrapper::new(iters), within)
        };
        index.commit(iter, std::convert::identity).unwrap();

        let iters = index.scans(num_shards, within).unwrap();
        assert_eq!(iters.len(), num_shards);

        let mut counts: Vec<usize> = vec![];
        for iter in iters.into_iter() {
            counts.push(iter.map(|_| 1).collect::<Vec<usize>>().into_iter().sum());
        }
        println!("{} {} {:?}", i, num_shards, counts);

        let avg = mindex.len() / num_shards;
        for (i, count) in counts.into_iter().enumerate() {
            if avg < 100 {
                continue;
            }
            assert!(
                ((count as f64) / (avg as f64)) > 0.1,
                "shard no {}, {} / {}",
                i,
                count,
                avg
            )
        }
    }
}

#[test]
fn test_commit_iterator_scans2() {
    let seed: u64 = random();
    let mut rng = SmallRng::seed_from_u64(seed);
    println!("seed:{}", seed);

    let dir = {
        let mut dir = std::env::temp_dir();
        dir.push("shrobt-test-commit-iterator-scans2");
        println!("temp dir {:?}", dir);
        dir.into_os_string()
    };

    let mut config: robt::Config = Default::default();
    config.delta_ok = true;
    config.value_in_vlog = true;
    let num_shards = (rng.gen::<usize>() % 8) + 1;
    let mmap = rng.gen::<bool>();
    let shrobtf = shrobt_factory::<i64, i64, NoBitmap>(config, num_shards, mmap);

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

        let mut index = shrobtf.new(&dir, "commit-iterator-scans2").unwrap();
        let c_entries: Vec<Result<Entry<i64, i64>>> = llrb_snap
            .to_reader()
            .unwrap()
            .iter_with_versions()
            .unwrap()
            .collect();
        index
            .commit(
                core::CommitIter::new(c_entries.into_iter(), within.clone()),
                std::convert::identity,
            )
            .unwrap();

        let mut iter = {
            let mut iters = index.scans(num_shards, within).unwrap();
            iters.reverse(); // make this to stack
            let w = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
            scans::FilterScans::new(iters, w)
        };

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
    let seed: u64 = random();
    let mut rng = SmallRng::seed_from_u64(seed);
    println!("seed:{}", seed);

    let dir = {
        let mut dir = std::env::temp_dir();
        dir.push("shrobt-test-commit-iterator-range-scans");
        println!("temp dir {:?}", dir);
        dir.into_os_string()
    };

    let mut config: robt::Config = Default::default();
    config.delta_ok = true;
    config.value_in_vlog = true;
    let num_shards = (rng.gen::<usize>() % 8) + 1;
    let mmap = rng.gen::<bool>();
    let shrobtf = shrobt_factory::<i64, i64, NoBitmap>(config, num_shards, mmap);

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

        let mut index = shrobtf.new(&dir, "snapshot-range-scans").unwrap();
        let c_entries: Vec<Result<Entry<i64, i64>>> = llrb_snap
            .to_reader()
            .unwrap()
            .iter_with_versions()
            .unwrap()
            .collect();
        index
            .commit(
                core::CommitIter::new(c_entries.into_iter(), within.clone()),
                std::convert::identity,
            )
            .unwrap();

        let shards = (i + 1) as usize;
        let ranges: Vec<(Bound<i64>, Bound<i64>)> = llrb_snap
            .scans(shards, within.clone())
            .unwrap()
            .into_iter()
            .map(|iter| {
                let entries: Vec<Result<Entry<i64, i64>>> = iter.collect();
                let lk = match entries.first() {
                    Some(Ok(e)) => Bound::Included(e.to_key()),
                    None => Bound::Excluded(std::i64::MAX),
                    _ => unreachable!(),
                };
                let hk = match entries.last() {
                    Some(Ok(e)) => Bound::Included(e.to_key()),
                    None => Bound::Excluded(std::i64::MAX),
                    _ => unreachable!(),
                };
                (lk, hk)
            })
            .collect();
        let mut iter = {
            let mut iters = index.range_scans(ranges, within).unwrap();
            iters.reverse(); // make this to stack
            let w = (Bound::<u64>::Unbounded, Bound::<u64>::Unbounded);
            scans::FilterScans::new(iters, w)
        };

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

fn run_shrobt_llrb(
    //
    name: &str,
    n_ops: u64,
    key_max: i64,
    repeat: usize,
    seed: u64,
) {
    for i in 0..repeat {
        let mut n_ops = n_ops;
        let seed = seed + (i as u64);
        let mut rng = SmallRng::seed_from_u64(seed);

        // populate llrb
        let lsm: bool = rng.gen();
        let sticky: bool = rng.gen();
        let mut llrb: Box<Llrb<i64, i64>> = if lsm {
            Llrb::new_lsm("test-llrb")
        } else {
            Llrb::new("test-llrb")
        };
        llrb.set_sticky(sticky).unwrap();

        random_llrb(n_ops as i64, key_max, seed, &mut llrb);

        // to avoid screwing up the seqno in non-lsm mode, say, what if
        // the last operation was a delete.
        llrb.set(123, 123456789).unwrap();
        n_ops += 1;

        // build ShRobt
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
        let num_shards = (rng.gen::<usize>() % 8) + 1;

        {
            print!(
                "seed:{} n_ops:{} key_max:{} lsm:{} sticky:{} delta:{} ",
                seed, n_ops, key_max, lsm, sticky, config.delta_ok,
            );
            println!(
                " vlog:{} num_shards:{:?} within:{:?} mmap:{}",
                config.value_in_vlog, num_shards, within, mmap
            );
        }

        let (mut llrb, refs) = llrb_to_refs1(llrb, within.clone(), &config);
        let n_deleted: usize = refs
            .iter()
            .map(|e| if e.is_deleted() { 1 } else { 0 })
            .sum();
        // println!("refs len: {}", refs.len());
        let dir = {
            let mut dir = std::env::temp_dir();
            dir.push(name);
            dir.into_os_string()
        };
        fs::remove_dir_all(&dir).ok();

        let mut index = ShRobt::<i64, i64, CRoaring>::new(
            //
            &dir,
            name,
            config.clone(),
            num_shards,
            mmap,
        )
        .unwrap();

        let app_meta = "heloo world".to_string();
        {
            let within = within.clone();
            let scanner = {
                let iters = llrb.scans(num_shards, within.clone()).unwrap();
                core::CommitIter::new(scans::CommitWrapper::new(iters), within)
            };
            index
                .commit(scanner, |_| app_meta.as_bytes().to_vec())
                .unwrap();
        }

        assert_eq!(index.len().unwrap(), refs.len());
        assert_eq!(index.to_name(), name);

        match index.validate() {
            Err(Error::EmptyIndex) if refs.len() == 0 => continue,
            Err(err) => panic!("{:?}", err),
            Ok(_) => (),
        }

        assert_eq!(index.to_metadata().unwrap(), app_meta.as_bytes().to_vec());

        let stats = index.to_stats().unwrap();
        assert_eq!(stats.z_blocksize, config.z_blocksize);
        assert_eq!(stats.m_blocksize, config.m_blocksize);
        assert_eq!(stats.v_blocksize, config.v_blocksize);
        assert_eq!(stats.delta_ok, config.delta_ok);
        assert_eq!(stats.value_in_vlog, config.value_in_vlog);
        if lsm || sticky {
            assert_eq!(stats.n_deleted, n_deleted);
        }

        // test get
        let mut seqno = 0;
        let mut r = index.to_reader().unwrap();
        for entry in refs.iter() {
            // println!("{}", entry.as_key());
            let e = r.get(entry.as_key()).unwrap();
            check_entry1(&entry, &e);
            seqno = std::cmp::max(seqno, e.to_seqno());
        }
        assert_eq!(index.to_seqno().unwrap(), seqno);

        if seqno == 0 {
            continue;
        }

        let mut r = index.to_reader().unwrap();

        // test first entry
        let ref_entry = refs.first().unwrap();
        let ref_entry = ref_entry
            .clone()
            .purge(Cutoff::new_lsm(Bound::Excluded(ref_entry.to_seqno())))
            .unwrap();
        let entry = r.first().unwrap();
        let entry = entry
            .clone()
            .purge(Cutoff::new_lsm(Bound::Excluded(entry.to_seqno())))
            .unwrap();
        check_entry1(&entry, &ref_entry);
        check_entry1(&r.first_with_versions().unwrap(), &refs.first().unwrap());
        // test last entry
        let ref_entry = refs.last().unwrap();
        let ref_entry = ref_entry
            .clone()
            .purge(Cutoff::new_lsm(Bound::Excluded(ref_entry.to_seqno())))
            .unwrap();
        let entry = r.last().unwrap();
        let entry = entry
            .clone()
            .purge(Cutoff::new_lsm(Bound::Excluded(entry.to_seqno())))
            .unwrap();
        check_entry1(&entry, &ref_entry);
        check_entry1(&r.last_with_versions().unwrap(), &refs.last().unwrap());

        // test get_with_versions
        for entry in refs.iter() {
            let e = r.get_with_versions(entry.as_key()).unwrap();
            check_entry1(&entry, &e);
            check_entry2(&entry, &e)
        }
        // test iter
        let xs = r.iter().unwrap();
        let xs: Vec<Entry<i64, i64>> = xs.map(|e| e.unwrap()).collect();
        for (x, y) in xs.iter().zip(refs.iter()) {
            check_entry1(&x, &y)
        }
        assert_eq!(xs.len(), refs.len());
        // test iter_with_versions
        let xs = r.iter_with_versions().unwrap();
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
            let xs = r.range(range).unwrap();
            let xs: Vec<Entry<i64, i64>> = xs.map(|e| e.unwrap()).collect();
            assert_eq!(xs.len(), refs.len());
            for (x, y) in xs.iter().zip(refs.iter()) {
                check_entry1(&x, &y);
            }
            // test range_with_versions
            let range = random_low_high(key_max, seed + j);
            let refs = llrb_to_refs2(&mut llrb, range, within.clone(), &config);
            // println!("range..versions bounds {:?} {}", range, refs.len());
            let xs = r.range_with_versions(range).unwrap();
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
            let xs = r.reverse(range).unwrap();
            let xs: Vec<Entry<i64, i64>> = xs.map(|e| e.unwrap()).collect();
            assert_eq!(xs.len(), refs.len());
            for (x, y) in xs.iter().zip(refs.iter()) {
                check_entry1(&x, &y);
            }
            // test reverse_with_versions
            let range = random_low_high(key_max, seed + j);
            let refs = llrb_to_refs3(&mut llrb, range, within.clone(), &config);
            // println!("reverse..versions bounds {:?} {}", range, refs.len());
            let xs = r.reverse_with_versions(range).unwrap();
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
    config: &robt::Config,
) -> (Box<Llrb<i64, i64>>, Vec<Entry<i64, i64>>) {
    let mut iter = scans::SkipScan::new(llrb.to_reader().unwrap());
    iter.set_seqno_range(within).unwrap();
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
    config: &robt::Config,
) -> Vec<Entry<i64, i64>>
where
    R: Clone + RangeBounds<i64>,
{
    let iter = {
        let iters = vec![llrb.range(range).unwrap()];
        scans::FilterScans::new(iters, within)
    };
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
    config: &robt::Config,
) -> Vec<Entry<i64, i64>>
where
    R: Clone + RangeBounds<i64>,
{
    let iter = {
        let iters = vec![llrb.reverse(range).unwrap()];
        scans::FilterScans::new(iters, within)
    };
    iter.filter_map(|e| {
        let mut e = e.unwrap();
        if !config.delta_ok {
            e.set_deltas(vec![]);
        }
        Some(e)
    })
    .collect()
}

fn random_llrb(n_ops: i64, key_max: i64, seed: u64, mindex: &mut Llrb<i64, i64>) {
    let mut rng = SmallRng::seed_from_u64(seed);

    for _i in 0..n_ops {
        let _seqno = mindex.to_seqno().unwrap();
        let key = (rng.gen::<i64>() % key_max).abs();
        let op = rng.gen::<usize>() % 3;
        match op {
            0 => {
                let value: i64 = rng.gen();
                // println!("key {} {} {} {}", key, _seqno, op, value);
                mindex.set(key, value).unwrap();
            }
            1 => {
                let value: i64 = rng.gen();
                // println!("key {} {} {} {}", key, _seqno, op, value);
                {
                    let cas = match mindex.get(&key) {
                        Err(Error::KeyNotFound) => 0,
                        Err(_err) => unreachable!(),
                        Ok(e) => e.to_seqno(),
                    };
                    mindex.set_cas(key, value, cas).unwrap();
                }
            }
            2 => {
                // println!("key {} {} {}", key, _seqno, op);
                mindex.delete(&key).unwrap();
            }
            _ => unreachable!(),
        }
    }
}

fn random_low_high(key_max: i64, seed: u64) -> (Bound<i64>, Bound<i64>) {
    let mut rng = SmallRng::seed_from_u64(seed);

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
    assert_eq!(e1.is_deleted(), e2.is_deleted(), "key:{}", key);
    assert_eq!(
        e1.to_seqno(),
        e2.to_seqno(),
        "key:{} {}",
        key,
        e1.is_deleted()
    );
    assert_eq!(e1.to_native_value(), e2.to_native_value(), "key:{}", key);
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

fn random_ops_keys(seed: u64, ops_limit: i64, key_limit: i64) -> (i64, i64) {
    let mut rng = SmallRng::seed_from_u64(seed);

    let n_ops_set: Vec<i64> = vec![
        0,
        ops_limit / 10,
        ops_limit / 100,
        ops_limit / 1000,
        ops_limit / 10000,
    ];
    let i = rng.gen::<usize>() % (n_ops_set.len() + 1);
    let n_ops = if i == n_ops_set.len() {
        10000 + (rng.gen::<u64>() % (ops_limit as u64))
    } else {
        n_ops_set[i] as u64
    };
    let n_ops = n_ops as i64;

    let max_key_set: Vec<i64> = vec![
        (key_limit / 10) + 1,
        (key_limit / 100) + 1,
        (key_limit / 1000) + 1,
        (key_limit / 10000) + 1,
    ];
    let i: usize = rng.gen::<usize>() % (max_key_set.len() + 1);
    let max_key = if i == max_key_set.len() {
        10000 + (rng.gen::<i64>() % key_limit)
    } else {
        max_key_set[i]
    };
    (n_ops, i64::max(i64::abs(max_key), n_ops / 10) + 1)
}

#[allow(dead_code)]
fn print_dir(dir: &ffi::OsString) {
    for item in fs::read_dir(dir).unwrap() {
        let item = item.unwrap();
        let len = item.metadata().unwrap().len();
        println!("read_dir:file:{:?} {}", item.file_name(), len);
    }
    println!("");
}
