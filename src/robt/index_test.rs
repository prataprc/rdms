use arbitrary::{self, unstructured::Unstructured, Arbitrary};
use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};
use xorfilter::{BuildHasherDefault, Xor8};

use std::thread;

use super::*;
use crate::llrb;

#[test]
fn test_robt_read() {
    let seed: u128 = [
        random(),
        315408295460649044406651951935429140111,
        315408295460649044406651951935429140111,
        254380117901283245685140957742548176144,
    ][random::<usize>() % 2];
    // let seed: u128 = 254380117901283245685140957742548176144;
    println!("test_robt_read {}", seed);

    do_robt_read(seed, NoBitmap);
    do_robt_read(seed, Xor8::<BuildHasherDefault>::new());
}

fn do_robt_read<B>(seed: u128, bitmap: B) {
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    // initial build
    let dir = std::env::temp_dir().join("test_robt_read");
    let name = "do-robt-read";
    let config = Config {
        dir: dir.as_os_str().to_os_string(),
        name: name.to_string(),
        z_blocksize: [1024, 4096, 8192, 16384, 1048576][rng.gen::<usize>() % 5],
        m_blocksize: [1024, 4096, 8192, 16384, 1048576][rng.gen::<usize>() % 5],
        v_blocksize: [1024, 4096, 8192, 16384, 1048576][rng.gen::<usize>() % 5],
        delta_ok: rng.gen(),
        value_in_vlog: rng.gen(),
        flush_queue_size: [32, 64, 1024][rng.gen::<usize>() % 3],
    };
    println!(
        "do-robt-read index file {:?}",
        config.to_index_file_location()
    );

    let n_sets = 100_000;
    let n_inserts = 100_000;
    let n_dels = 10_000;
    let n_rems = 10_000;
    let n_threads = 8;

    let appmd = "test_robt_read-metadata".as_bytes().to_vec();

    let mut config = config.clone();
    config.value_in_vlog = rng.gen();
    config.delta_ok = rng.gen();
    println!("do-robt-read config:{:?}", config);

    let mdb = llrb::load_index(seed, n_sets, n_inserts, n_rems, n_dels, None);
    let seqno = Some(mdb.to_seqno());

    let handles = do_initial(seed, bitmap, &mdb, &config, &appmd, seqno, n_threads),
    for handle in handles.into_iter() {
        handle.join().unwrap();
    }

    let snap = util::load_index(seed, s, i, r, d, Some(mdb.to_seqno()));
    let seqno = Some(snap.to_seqno());

    let appmd = "test_robt_read-metadata-snap".as_bytes().to_vec();
    let handles = {
        mdb.commit(snap.iter().unwrap()).unwrap();
        mdb.set_seqno(snap.to_seqno());
        do_incremental(seed, bitmap, &mdb, &config, &appmd, seqno, n_threads)
    };
    for handle in handles.into_iter() {
        handle.join().unwrap();
    }
}

fn do_initial<B>(
    seed: u128,
    bitmap: B,
    mdb: &llrb::Index<u16, u64>,
    config: &Config,
    appmd: &[u8],
    seqno: Option<u64>,
    n_threads: usize,
) -> Vec<thread::JoinHandle<()>>
where
    B: Bloom,
{
    let mut build = Builder::initial(config.clone(), appmd.to_vec()).unwrap();
    build
        .build_index(mdb.iter().unwrap(), bitmap, seqno)
        .unwrap();

    let mut handles = vec![];
    for i in 0..n_threads {
        let (cnf, mdb, appmd) = (config.clone(), mdb.clone(), appmd.to_vec());
        let seed = seed + ((i as u128) * 10);
        //handles.push(thread::spawn(move || {
        //    read_thread::<B>(i, seed, cnf, mdb, appmd)
        //}));
    }

    handles
}

//fn do_incremental<B>(
//    seed: u128,
//    bitmap: B,
//    mdb: &Index<u16, u64>,
//    config: &Config,
//    appmd: &[u8],
//    seqno: Option<u64>,
//    n_threads: usize,
//) -> Vec<thread::JoinHandle<()>>
//where
//    B: Bloom,
//{
//    let vlog = {
//        let dir = config.dir.as_os_str();
//        let file = config.to_index_file_location();
//        let index = open_index::<B>(dir, &config.name, &file, seed);
//        index.to_vlog_file_location()
//    };
//
//    let mut build = Builder::incremental(config.clone(), vlog, appmd.to_vec()).unwrap();
//    build
//        .build_index(mdb.iter().unwrap(), bitmap, seqno)
//        .unwrap();
//
//    let mut handles = vec![];
//    for i in 0..n_threads {
//        let (cnf, mdb, appmd) = (config.clone(), mdb.clone(), appmd.to_vec());
//        let seed = seed + ((i as u128) * 10);
//        handles.push(thread::spawn(move || {
//            read_thread::<B>(i, seed, cnf, mdb, appmd)
//        }));
//    }
//
//    handles
//}
//
//fn read_thread<B>(
//    id: usize,
//    seed: u128,
//    config: Config,
//    mdb: Index<u16, u64>,
//    app_meta_data: Vec<u8>,
//) where
//    B: Bloom,
//{
//    use Error::KeyNotFound;
//
//    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
//
//    let mut index = {
//        let dir = config.dir.as_os_str();
//        let file = config.to_index_file_location();
//        open_index::<B>(dir, &config.name, &file, seed)
//    };
//
//    let mut counts = [0_usize; 8];
//    let n = 1000;
//
//    for _i in 0..n {
//        let bytes = rng.gen::<[u8; 32]>();
//        let mut uns = Unstructured::new(&bytes);
//
//        let op: Op<u16> = uns.arbitrary().unwrap();
//        // println!("{}-op {} -- {:?}", id, _i, op);
//        match op.clone() {
//            Op::Mo(meta_op) => {
//                use MetaOp::*;
//
//                counts[0] += 1;
//                match meta_op {
//                    Name => assert_eq!(index.to_name(), config.name.clone()),
//                    Stats => {
//                        let stats = index.to_stats();
//                        validate_stats(&stats, &config, &mdb, 0);
//                    }
//                    AppMetadata => assert_eq!(index.to_app_metadata(), app_meta_data),
//                    Seqno => assert_eq!(index.to_seqno(), mdb.to_seqno()),
//                    IsCompacted => assert_eq!(index.is_compacted(), true),
//                    Len => assert_eq!(index.len(), mdb.len()),
//                    IsEmpty => assert_eq!(index.is_empty(), false),
//                    _ => (),
//                }
//            }
//            Op::Get(key) => {
//                counts[1] += 1;
//                match (index.get(&key), mdb.get(&key)) {
//                    (Ok(e1), Ok(mut e2)) => {
//                        e2.deltas = vec![];
//                        assert_eq!(e1, e2);
//                    }
//                    (Err(KeyNotFound(_, _)), Err(ppom::Error::KeyNotFound(_, _))) => (),
//                    (Err(err1), Err(err2)) => panic!("{} != {}", err1, err2),
//                    (Ok(e), Err(err)) => panic!("{:?} != {}", e, err),
//                    (Err(err), Ok(e)) => panic!("{} != {:?}", err, e),
//                }
//            }
//            Op::GetVersions(key) => {
//                counts[2] += 1;
//                match (index.get_versions(&key), mdb.get(&key)) {
//                    (Ok(e1), Ok(mut e2)) if !config.delta_ok => {
//                        e2.deltas = vec![];
//                        assert_eq!(e1, e2);
//                    }
//                    (Ok(e1), Ok(e2)) => assert_eq!(e1, e2),
//                    (Err(KeyNotFound(_, _)), Err(ppom::Error::KeyNotFound(_, _))) => (),
//                    (Err(err1), Err(err2)) => panic!("{} != {}", err1, err2),
//                    (Ok(e), Err(err)) => panic!("{:?} != {}", e, err),
//                    (Err(err), Ok(e)) => panic!("{} != {:?}", err, e),
//                }
//            }
//            Op::Iter(iter_op) => {
//                use IterOp::*;
//
//                match iter_op {
//                    Iter((l, h)) => {
//                        counts[3] += 1;
//                        let r = (Bound::from(l), Bound::from(h));
//                        let mut iter1 = mdb.range(r).unwrap();
//                        let mut iter2 = index.iter(r).unwrap();
//                        while let Some(mut e1) = iter1.next() {
//                            e1.deltas = vec![];
//                            assert_eq!(e1, iter2.next().unwrap().unwrap())
//                        }
//                        assert!(iter1.next().is_none());
//                        assert!(iter2.next().is_none());
//                    }
//                    Reverse((l, h)) => {
//                        counts[4] += 1;
//                        let r = (Bound::from(l), Bound::from(h));
//                        let mut iter1 = mdb.reverse(r).unwrap();
//                        let mut iter2 = index.reverse(r).unwrap();
//                        while let Some(mut e1) = iter1.next() {
//                            e1.deltas = vec![];
//                            assert_eq!(e1, iter2.next().unwrap().unwrap())
//                        }
//                        assert!(iter1.next().is_none());
//                        assert!(iter2.next().is_none());
//                    }
//                    IterVersions((l, h)) => {
//                        counts[5] += 1;
//                        let r = (Bound::from(l), Bound::from(h));
//                        let mut iter1 = mdb.range(r).unwrap();
//                        let mut iter2 = index.iter_versions(r).unwrap();
//                        while let Some(mut e1) = iter1.next() {
//                            if !config.delta_ok {
//                                e1.deltas = vec![];
//                            }
//                            assert_eq!(e1, iter2.next().unwrap().unwrap())
//                        }
//                        assert!(iter1.next().is_none());
//                        assert!(iter2.next().is_none());
//                    }
//                    ReverseVersions((l, h)) => {
//                        counts[6] += 1;
//                        let r = (Bound::from(l), Bound::from(h));
//                        let mut iter1 = mdb.reverse(r).unwrap();
//                        let mut iter2 = index.reverse_versions(r).unwrap();
//                        while let Some(mut e1) = iter1.next() {
//                            if !config.delta_ok {
//                                e1.deltas = vec![];
//                            }
//                            assert_eq!(e1, iter2.next().unwrap().unwrap())
//                        }
//                        assert!(iter1.next().is_none());
//                        assert!(iter2.next().is_none());
//                    }
//                }
//            }
//            Op::Validate => {
//                counts[7] += 1;
//                index.validate().unwrap();
//            }
//        };
//    }
//    println!("{}-counts {:?}", id, counts);
//}
//
//#[test]
//fn test_compact_mono() {
//    let seed: u128 = random();
//    println!("test_compact_mono {}", seed);
//}
//
//#[test]
//fn test_compact_lsm() {
//    let seed: u128 = random();
//    println!("test_compact {}", seed);
//}
//
//#[test]
//fn test_compact_tombstone() {
//    let seed: u128 = random();
//    println!("test_compact {}", seed);
//}
//
//fn validate_stats(
//    stats: &Stats,
//    config: &Config,
//    mdb: &Index<u16, u64>,
//    n_abytes: u64,
//) {
//    assert_eq!(stats.name, config.name);
//    assert_eq!(stats.z_blocksize, config.z_blocksize);
//    assert_eq!(stats.m_blocksize, config.m_blocksize);
//    assert_eq!(stats.v_blocksize, config.v_blocksize);
//    assert_eq!(stats.delta_ok, config.delta_ok);
//    assert_eq!(stats.value_in_vlog, config.value_in_vlog);
//
//    if config.value_in_vlog || config.delta_ok {
//        assert_eq!(
//            config.to_vlog_file_location(),
//            stats.vlog_file.clone().unwrap()
//        );
//    }
//
//    assert_eq!(stats.n_count, mdb.len() as u64);
//    assert_eq!(stats.n_deleted, mdb.deleted_count());
//    assert_eq!(stats.seqno, mdb.to_seqno());
//    assert_eq!(stats.n_abytes, n_abytes);
//}
//
//fn open_index<B>(
//    dir: &ffi::OsStr,
//    name: &str,
//    file: &ffi::OsStr,
//    seed: u128,
//) -> Index<u16, u64, u64, B>
//where
//    B: Bloom,
//{
//    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
//
//    let index = match rng.gen::<u8>() % 2 {
//        0 => Index::open(dir, name).unwrap(),
//        1 => Index::open_file(file).unwrap(),
//        _ => unreachable!(),
//    };
//
//    match rng.gen::<bool>() {
//        true => index.try_clone().unwrap(),
//        false => index,
//    }
//}
//
//#[derive(Clone, Debug, Arbitrary)]
//enum MetaOp {
//    Name,
//    Bitmap,
//    Stats,
//    AppMetadata,
//    Root,
//    Seqno,
//    IsCompacted,
//    Len,
//    IsEmpty,
//}
//
//#[derive(Clone, Debug, Arbitrary)]
//enum IterOp<K> {
//    Iter((Limit<K>, Limit<K>)),
//    IterVersions((Limit<K>, Limit<K>)),
//    Reverse((Limit<K>, Limit<K>)),
//    ReverseVersions((Limit<K>, Limit<K>)),
//}
//
//#[derive(Clone, Debug, Arbitrary)]
//enum Op<K> {
//    Mo(MetaOp),
//    Get(K),
//    GetVersions(K),
//    Iter(IterOp<K>),
//    Validate,
//}
//
//#[derive(Clone, Debug, Arbitrary, Eq, PartialEq)]
//enum Limit<T> {
//    Unbounded,
//    Included(T),
//    Excluded(T),
//}
//
//impl<T> From<Limit<T>> for Bound<T> {
//    fn from(limit: Limit<T>) -> Self {
//        match limit {
//            Limit::Unbounded => Bound::Unbounded,
//            Limit::Included(v) => Bound::Included(v),
//            Limit::Excluded(v) => Bound::Excluded(v),
//        }
//    }
//}
