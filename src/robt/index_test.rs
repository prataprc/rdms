use arbitrary::{self, unstructured::Unstructured, Arbitrary};
use rand::{self, prelude::random, rngs::SmallRng, Rng, SeedableRng};
use xorfilter::{BuildHasherDefault, Xor8};

use std::{fs, mem, thread};

use super::*;
use crate::{bitmaps::NoBitmap, db, llrb};

// open open_file set_bitmap compact print validate try_clone purge close
// get get_versions iter iter_versions reverse reverse_versions len
// is_compacted is_empty
// as_bitmap to_app_metadata to_bitmap to_index_location to_name to_root to_seqno to_stats
// to_vlog_location

#[test]
fn test_robt_build_read() {
    let seed: u128 = [
        random(),
        315408295460649044406651951935429140111,
        254380117901283245685140957742548176144,
        109332097090788254409904627378619335666,
    ][random::<usize>() % 4];
    // let seed: u128 = 315408295460649044406651951935429140111;
    println!("test_robt_read {}", seed);

    do_robt_build_read::<u16, _>("u16,nobitmap", seed, NoBitmap);
    do_robt_build_read::<u64, _>("u64,nobitmap", seed, NoBitmap);
    do_robt_build_read::<u16, _>("u16,xor8", seed, Xor8::<BuildHasherDefault>::new());
    do_robt_build_read::<u64, _>("u64,xor8", seed, Xor8::<BuildHasherDefault>::new());
}

fn do_robt_build_read<K, B>(prefix: &str, seed: u128, bitmap: B)
where
    for<'a> K: 'static
        + Sync
        + Send
        + Clone
        + Ord
        + Hash
        + db::Footprint
        + IntoCbor
        + FromCbor
        + Arbitrary<'a>
        + fmt::Debug,
    B: 'static + Sync + Send + Clone + db::Bloom,
    rand::distributions::Standard: rand::distributions::Distribution<K>,
{
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    // initial build
    let dir = std::env::temp_dir().join("test_robt_read");
    fs::remove_dir(&dir).ok();
    let name = "do-robt-read";
    let mut config = Config {
        dir: dir.as_os_str().to_os_string(),
        name: name.to_string(),
        z_blocksize: [1024, 4096, 8192, 16384, 1048576][rng.gen::<usize>() % 5],
        m_blocksize: [1024, 4096, 8192, 16384, 1048576][rng.gen::<usize>() % 5],
        v_blocksize: [1024, 4096, 8192, 16384, 1048576][rng.gen::<usize>() % 5],
        delta_ok: rng.gen::<bool>(),
        value_in_vlog: rng.gen::<bool>(),
        flush_queue_size: [32, 64, 1024][rng.gen::<usize>() % 3],
        vlog_location: None,
    };
    println!(
        "do_robt_build_read-{} index file {:?}",
        prefix,
        config.to_index_location()
    );
    println!("do_robt_build_read-{} config:{:?}", prefix, config);

    let mdb = do_initial::<K, B>(prefix, seed, bitmap.clone(), &config, None);

    let (mut index, vlog) = {
        let file = config.to_index_location();
        (
            open_index::<K, B>(&config.dir, &config.name, &file, seed),
            config.to_vlog_location(),
        )
    };

    config.name = name.to_owned() + "-incr";
    vlog.clone().map(|f| config.set_vlog_location(Some(f)));
    do_incremental::<K, B>(
        prefix,
        seed,
        bitmap.clone(),
        &mdb,
        &mut index,
        &config,
        vlog,
    );

    index = {
        let file = config.to_index_location();
        open_index::<K, B>(&config.dir, &config.name, &file, seed)
    };

    config.name = name.to_owned() + "-compact1";
    config.set_vlog_location(None);
    let cutoff: db::Cutoff = {
        let bytes = rng.gen::<[u8; 32]>();
        let mut uns = Unstructured::new(&bytes);
        uns.arbitrary().unwrap()
    };
    println!("do_robt_build_read-{}, compact cutoff:{:?}", prefix, cutoff);
    index = index.compact(config.clone(), bitmap, cutoff).unwrap();
    validate_compact(&mut index, cutoff, &mdb);

    let file = config.to_index_location();
    index = open_index::<K, B>(&config.dir, &config.name, &file, seed);
    index.validate().unwrap();
    index.purge().unwrap();
}

fn do_initial<K, B>(
    prefix: &str,
    seed: u128,
    bitmap: B,
    config: &Config,
    seqno: Option<u64>,
) -> llrb::Index<K, u64>
where
    for<'a> K: 'static
        + Sync
        + Send
        + Clone
        + Ord
        + Hash
        + db::Footprint
        + IntoCbor
        + FromCbor
        + Arbitrary<'a>
        + fmt::Debug,
    B: 'static + Sync + Send + Clone + db::Bloom,
    rand::distributions::Standard: rand::distributions::Distribution<K>,
{
    let n_sets = 100_000;
    let n_inserts = 100_000;
    let n_dels = 10_000;
    let n_rems = 10_000;
    let n_readers = 1;

    let appmd = "test_robt_read-metadata".as_bytes().to_vec();
    let mdb = llrb::load_index(seed, n_sets, n_inserts, n_rems, n_dels, seqno);
    let seqno = Some(mdb.to_seqno());

    let mut build = Builder::initial(config.clone(), appmd.to_vec()).unwrap();
    build
        .build_index(mdb.iter().unwrap().map(|e| Ok(e)), bitmap, seqno)
        .unwrap();
    mem::drop(build);

    let mut handles = vec![];
    for i in 0..n_readers {
        let prefix = prefix.to_string();
        let (cnf, mdb, appmd) = (config.clone(), mdb.clone(), appmd.to_vec());
        let seed = seed + ((i as u128) * 10);
        handles.push(thread::spawn(move || {
            read_thread::<K, B>(prefix, i, seed, cnf, mdb, appmd)
        }));
    }

    for handle in handles.into_iter() {
        handle.join().unwrap();
    }

    mdb
}

fn do_incremental<K, B>(
    prefix: &str,
    seed: u128,
    bitmap: B,
    mdb: &llrb::Index<K, u64>,
    index: &mut Index<K, u64, B>,
    config: &Config,
    vlog: Option<ffi::OsString>,
) where
    for<'a> K: 'static
        + Sync
        + Send
        + Clone
        + Ord
        + Hash
        + db::Footprint
        + IntoCbor
        + FromCbor
        + Arbitrary<'a>
        + fmt::Debug,
    B: 'static + Sync + Send + Clone + db::Bloom,
    rand::distributions::Standard: rand::distributions::Distribution<K>,
{
    let n_sets = 100_000;
    let n_inserts = 100_000;
    let n_dels = 10_000;
    let n_rems = 10_000;
    let n_readers = 1;

    let appmd = "test_robt_read-metadata-snap".as_bytes().to_vec();
    let snap = {
        let seqno = Some(mdb.to_seqno());
        llrb::load_index(seed, n_sets, n_inserts, n_rems, n_dels, seqno)
    };

    let mut build = Builder::incremental(config.clone(), vlog, appmd.to_vec()).unwrap();
    build
        .build_index(
            index
                .lsm_merge(snap.iter().unwrap().map(|e| Ok(e)), true /*versions*/)
                .unwrap(),
            bitmap,
            Some(snap.to_seqno()),
        )
        .unwrap();
    mem::drop(build);

    mdb.commit(snap.iter().unwrap()).unwrap();
    mdb.set_seqno(snap.to_seqno());

    let mut handles = vec![];
    for i in 0..n_readers {
        let prefix = prefix.to_string();
        let (cnf, mdb, appmd) = (config.clone(), mdb.clone(), appmd.to_vec());
        let seed = seed + ((i as u128) * 10);
        handles.push(thread::spawn(move || {
            read_thread::<K, B>(prefix, i, seed, cnf, mdb, appmd)
        }));
    }

    for handle in handles.into_iter() {
        handle.join().unwrap();
    }
}

fn read_thread<K, B>(
    prefix: String,
    id: usize,
    seed: u128,
    config: Config,
    mdb: llrb::Index<K, u64>,
    app_meta_data: Vec<u8>,
) where
    for<'a> K: Clone + Ord + FromCbor + Arbitrary<'a> + fmt::Debug,
    B: Clone + db::Bloom,
{
    use Error::KeyNotFound;

    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let mut index = {
        let dir = config.dir.as_os_str();
        let file = config.to_index_location();
        open_index::<K, B>(dir, &config.name, &file, seed)
    };

    let mut counts = [0_usize; 19];
    let ops = 200;
    for _i in 0..ops {
        let bytes = rng.gen::<[u8; 32]>();
        let mut uns = Unstructured::new(&bytes);

        let op: Op<K> = uns.arbitrary().unwrap();
        // println!("{} {}-op {} -- {:?}", prefix, id, _i, op);
        match op.clone() {
            Op::M(meta_op) => {
                use MetaOp::*;

                counts[0] += 1;
                match meta_op {
                    IsCompacted => {
                        counts[1] += 1;
                        assert_eq!(index.is_compacted(), true);
                    }
                    IsEmpty => {
                        counts[2] += 1;
                        assert_eq!(index.is_empty(), false);
                    }
                    AsBitmap => {
                        counts[5] += 1;
                        index.as_bitmap();
                    }
                    ToName => {
                        counts[3] += 1;
                        assert_eq!(index.to_name(), config.name.clone());
                    }
                    ToAppMetadata => {
                        counts[4] += 1;
                        assert_eq!(index.to_app_metadata(), app_meta_data);
                    }
                    ToBitmap => {
                        counts[5] += 1;
                        index.to_bitmap();
                    }
                    ToIndexLocation => {
                        counts[6] += 1;
                        assert_eq!(index.to_index_location(), config.to_index_location());
                    }
                    ToVlogLocation if config.value_in_vlog || config.delta_ok => {
                        counts[7] += 1;
                        assert_eq!(index.to_vlog_location(), config.to_vlog_location());
                    }
                    ToVlogLocation => (),
                    ToRoot => {
                        counts[8] += 1;
                        assert!(index.to_root() > 0, "{}", index.to_root());
                    }
                    ToSeqno => {
                        counts[9] += 1;
                        assert!(index.to_seqno() > 0, "{}", index.to_seqno());
                    }
                    ToStats => {
                        counts[10] += 1;
                        validate_stats(&index.to_stats(), &config, &mdb, 0);
                    }
                    Len => {
                        counts[11] += 1;
                        assert_eq!(
                            index.len(),
                            mdb.len(),
                            "{} {}",
                            index.len(),
                            mdb.len()
                        );
                    }
                }
            }
            Op::Get(key) => {
                counts[13] += 1;
                match (index.get(&key), mdb.get(&key)) {
                    (Ok(e1), Ok(mut e2)) => {
                        e2.deltas = vec![];
                        assert_eq!(e1, e2);
                    }
                    (Err(KeyNotFound(_, _)), Err(Error::KeyNotFound(_, _))) => (),
                    (Err(err1), Err(err2)) => panic!("{} != {}", err1, err2),
                    (Ok(e), Err(err)) => panic!("{:?} != {}", e, err),
                    (Err(err), Ok(e)) => panic!("{} != {:?}", err, e),
                }
            }
            Op::GetVersions(key) => {
                counts[14] += 1;
                match (index.get_versions(&key), mdb.get(&key)) {
                    (Ok(e1), Ok(mut e2)) if !config.delta_ok => {
                        e2.deltas = vec![];
                        assert_eq!(e1, e2);
                    }
                    (Ok(e1), Ok(e2)) => assert_eq!(e1, e2),
                    (Err(KeyNotFound(_, _)), Err(Error::KeyNotFound(_, _))) => (),
                    (Err(err1), Err(err2)) => panic!("{} != {}", err1, err2),
                    (Ok(e), Err(err)) => panic!("{:?} != {}", e, err),
                    (Err(err), Ok(e)) => panic!("{} != {:?}", err, e),
                }
            }
            Op::Iter(iter_op) => {
                use IterOp::*;

                match iter_op {
                    Iter((l, h)) => {
                        counts[15] += 1;
                        let r = (Bound::from(l), Bound::from(h));
                        let mut iter1 = mdb.range(r.clone()).unwrap();
                        let mut iter2 = index.iter(r).unwrap();
                        while let Some(mut e1) = iter1.next() {
                            e1.deltas = vec![];
                            assert_eq!(e1, iter2.next().unwrap().unwrap())
                        }
                        assert!(iter1.next().is_none());
                        assert!(iter2.next().is_none());
                    }
                    Reverse((l, h)) => {
                        counts[16] += 1;
                        let r = (Bound::from(l), Bound::from(h));
                        let mut iter1 = mdb.reverse(r.clone()).unwrap();
                        let mut iter2 = index.reverse(r).unwrap();
                        while let Some(mut e1) = iter1.next() {
                            e1.deltas = vec![];
                            assert_eq!(e1, iter2.next().unwrap().unwrap())
                        }
                        assert!(iter1.next().is_none());
                        assert!(iter2.next().is_none());
                    }
                    IterVersions((l, h)) => {
                        counts[17] += 1;
                        let r = (Bound::from(l), Bound::from(h));
                        let mut iter1 = mdb.range(r.clone()).unwrap();
                        let mut iter2 = index.iter_versions(r).unwrap();
                        while let Some(mut e1) = iter1.next() {
                            if !config.delta_ok {
                                e1.deltas = vec![];
                            }
                            assert_eq!(e1, iter2.next().unwrap().unwrap())
                        }
                        assert!(iter1.next().is_none());
                        assert!(iter2.next().is_none());
                    }
                    ReverseVersions((l, h)) => {
                        counts[18] += 1;
                        let r = (Bound::from(l), Bound::from(h));
                        let mut iter1 = mdb.reverse(r.clone()).unwrap();
                        let mut iter2 = index.reverse_versions(r).unwrap();
                        while let Some(mut e1) = iter1.next() {
                            if !config.delta_ok {
                                e1.deltas = vec![];
                            }
                            assert_eq!(e1, iter2.next().unwrap().unwrap())
                        }
                        assert!(iter1.next().is_none());
                        assert!(iter2.next().is_none());
                    }
                }
            }
        };
    }
    println!("{} {}-counts {:?}", prefix, id, counts);

    index.close().unwrap();
}

fn validate_stats<K>(
    stats: &Stats,
    config: &Config,
    mdb: &llrb::Index<K, u64>,
    n_abytes: u64,
) {
    assert_eq!(stats.name, config.name);
    assert_eq!(stats.z_blocksize, config.z_blocksize);
    assert_eq!(stats.m_blocksize, config.m_blocksize);
    assert_eq!(stats.v_blocksize, config.v_blocksize);
    assert_eq!(stats.delta_ok, config.delta_ok);
    assert_eq!(stats.value_in_vlog, config.value_in_vlog);

    if config.value_in_vlog || config.delta_ok {
        assert_eq!(stats.vlog_location.clone(), config.to_vlog_location());
    }

    assert_eq!(stats.n_count, mdb.len() as u64);
    assert_eq!(stats.n_deleted, mdb.deleted_count());
    assert_eq!(stats.seqno, mdb.to_seqno());
    assert_eq!(stats.n_abytes, n_abytes);
}

fn open_index<K, B>(
    dir: &ffi::OsStr,
    name: &str,
    file: &ffi::OsStr,
    seed: u128,
) -> Index<K, u64, B>
where
    K: Clone + FromCbor,
    B: db::Bloom,
{
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let index = match rng.gen::<u8>() % 2 {
        0 => Index::open(dir, name).unwrap(),
        1 => Index::open_file(file).unwrap(),
        _ => unreachable!(),
    };

    match rng.gen::<bool>() {
        true => index.try_clone().unwrap(),
        false => index,
    }
}

fn validate_compact<K, B>(
    index: &mut Index<K, u64, B>,
    cutoff: db::Cutoff,
    mdb: &llrb::Index<K, u64>,
) where
    K: Clone + PartialEq + Ord + FromCbor + fmt::Debug,
    B: db::Bloom,
{
    let ref_entries: Vec<db::Entry<K, u64>> = mdb
        .iter()
        .unwrap()
        .filter_map(|e| e.compact(cutoff))
        .collect();
    let entries: Vec<db::Entry<K, u64>> = index
        .iter_versions(..)
        .unwrap()
        .map(|e| e.unwrap())
        .collect();
    assert_eq!(
        ref_entries.len(),
        entries.len(),
        "{} {}",
        ref_entries.len(),
        entries.len()
    );
    for (i, (re, ee)) in ref_entries.into_iter().zip(entries.into_iter()).enumerate() {
        assert_eq!(re, ee, "i:{}", i);
    }
}

#[derive(Clone, Debug, Arbitrary)]
enum MetaOp {
    IsCompacted,
    IsEmpty,
    AsBitmap,
    ToName,
    ToAppMetadata,
    ToBitmap,
    ToIndexLocation,
    ToVlogLocation,
    ToRoot,
    ToSeqno,
    ToStats,
    Len,
}

#[derive(Clone, Debug, Arbitrary)]
enum IterOp<K> {
    Iter((Limit<K>, Limit<K>)),
    IterVersions((Limit<K>, Limit<K>)),
    Reverse((Limit<K>, Limit<K>)),
    ReverseVersions((Limit<K>, Limit<K>)),
}

#[derive(Clone, Debug, Arbitrary)]
enum Op<K> {
    M(MetaOp),
    Get(K),
    GetVersions(K),
    Iter(IterOp<K>),
}

#[derive(Clone, Debug, Arbitrary, Eq, PartialEq)]
enum Limit<T> {
    Unbounded,
    Included(T),
    Excluded(T),
}

impl<T> From<Limit<T>> for Bound<T> {
    fn from(limit: Limit<T>) -> Self {
        match limit {
            Limit::Unbounded => Bound::Unbounded,
            Limit::Included(v) => Bound::Included(v),
            Limit::Excluded(v) => Bound::Excluded(v),
        }
    }
}
