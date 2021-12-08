use arbitrary::{self, unstructured::Unstructured, Arbitrary};
use rand::{self, prelude::random, rngs::SmallRng, Rng, SeedableRng};
use xorfilter::{BuildHasherDefault, Xor8};

use std::{fs, mem, thread};

use super::*;
use crate::{bitmaps::NoBitmap, dbs, llrb};

trait Key:
    Sync
    + Send
    + Clone
    + Ord
    + PartialEq
    + Hash
    + dbs::Footprint
    + IntoCbor
    + FromCbor
    + ToString
    + fmt::Debug
{
}
trait Value:
    Sync
    + Send
    + Clone
    + PartialEq
    + dbs::Diff
    + dbs::Footprint
    + IntoCbor
    + FromCbor
    + ToString
    + fmt::Debug
{
}
trait Delta:
    Sync
    + Send
    + Clone
    + PartialEq
    + dbs::Footprint
    + IntoCbor
    + FromCbor
    + ToString
    + fmt::Debug
{
}

impl Key for u16 {}
impl Value for u16 {}
impl Delta for u16 {}
impl Key for u64 {}
impl Value for u64 {}
impl Delta for u64 {}
impl Key for dbs::Binary {}
impl Value for dbs::Binary {}
impl Delta for dbs::Binary {}

#[test]
fn test_robt_build_read() {
    let seed: u64 = [419111650579980006, random()][random::<usize>() % 2];
    println!("test_robt_read {}", seed);

    do_robt_build_read::<u16, u64, _>("u16,nobitmap", seed, NoBitmap);
    do_robt_build_read::<dbs::Binary, dbs::Binary, _>("binary,nobitmap", seed, NoBitmap);
    do_robt_build_read::<u16, u64, _>(
        "u16,xor8",
        seed,
        Xor8::<BuildHasherDefault>::new(),
    );
    do_robt_build_read::<dbs::Binary, dbs::Binary, _>(
        "binary,xor8",
        seed,
        Xor8::<BuildHasherDefault>::new(),
    );
}

fn do_robt_build_read<K, V, B>(prefix: &str, seed: u64, bitmap: B)
where
    for<'a> K: 'static + Key + Arbitrary<'a>,
    V: 'static + Value,
    <V as dbs::Diff>::Delta: 'static + Delta,
    B: 'static + Sync + Send + Clone + dbs::Bloom,
    rand::distributions::Standard: rand::distributions::Distribution<K>,
    rand::distributions::Standard: rand::distributions::Distribution<V>,
{
    let mut rng = SmallRng::seed_from_u64(seed);

    // initial build
    let dir = std::env::temp_dir().join("test_robt_read");
    fs::remove_dir(&dir).ok();
    let name = "do-robt-read";
    let mut config = Config {
        dir: dir.as_os_str().to_os_string(),
        name: name.to_string(),
        z_blocksize: [4096, 8192, 16384, 1048576][rng.gen::<usize>() % 4],
        m_blocksize: [4096, 8192, 16384, 1048576][rng.gen::<usize>() % 4],
        v_blocksize: [4096, 8192, 16384, 1048576][rng.gen::<usize>() % 4],
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

    let mut mdb = do_initial::<K, V, B>(prefix, seed, bitmap.clone(), &config, None);

    println!("do_robt_build_read-{} done initial build ...", prefix);

    let mut index = {
        let file = config.to_index_location();
        open_index::<K, V, B>(&config.dir, &config.name, &file, seed)
    };
    validate_bitmap(&mut index);

    config.name = name.to_owned() + "-incr";
    config.set_vlog_location(None);
    mdb = do_incremental::<K, V, B>(prefix, seed, bitmap.clone(), mdb, index, &config);

    println!("do_robt_build_read-{} done incremental build ...", prefix);

    let mut index = {
        let file = config.to_index_location();
        open_index::<K, V, B>(&config.dir, &config.name, &file, seed)
    };
    validate_bitmap(&mut index);

    config.name = name.to_owned() + "-compact1";
    config.set_vlog_location(None);
    let cutoff: dbs::Cutoff = {
        let bytes = rng.gen::<[u8; 32]>();
        let mut uns = Unstructured::new(&bytes);
        uns.arbitrary().unwrap()
    };
    println!("do_robt_build_read-{}, compact cutoff:{:?}", prefix, cutoff);
    index = index.compact(config.clone(), bitmap, cutoff).unwrap();
    validate_compact(&mut index, cutoff, &mdb);
    validate_bitmap(&mut index);

    let file = config.to_index_location();
    index = open_index::<K, V, B>(&config.dir, &config.name, &file, seed);
    index.validate().unwrap();
    index.purge().unwrap();
}

fn do_initial<K, V, B>(
    prefix: &str,
    seed: u64,
    bitmap: B,
    config: &Config,
    seqno: Option<u64>,
) -> llrb::Index<K, V>
where
    for<'a> K: 'static + Key + Arbitrary<'a> + fmt::Debug,
    V: 'static + Value,
    <V as dbs::Diff>::Delta: 'static + Delta,
    B: 'static + Sync + Send + Clone + dbs::Bloom,
    rand::distributions::Standard: rand::distributions::Distribution<K>,
    rand::distributions::Standard: rand::distributions::Distribution<V>,
{
    let mut n_sets = 100_000;
    let mut n_inserts = 100_000;
    let mut n_rems = 10_000;
    let mut n_dels = 10_000;
    let n_readers = 1;

    if !config.delta_ok && !config.value_in_vlog {
        n_sets += n_inserts;
        n_rems += n_dels;
        n_inserts = 0;
        n_dels = 0;
    }

    let appmd = "test_robt_read-metadata".as_bytes().to_vec();
    let mdb = llrb::load_index(seed, n_sets, n_inserts, n_rems, n_dels, seqno);
    let seqno = Some(mdb.to_seqno());

    let mut build = Builder::initial(config.clone(), appmd.to_vec()).unwrap();
    build
        .build_index(mdb.iter_versions().unwrap().map(Ok), bitmap, seqno)
        .unwrap();
    mem::drop(build);

    let mut handles = vec![];
    for i in 0..n_readers {
        let prefix = prefix.to_string();
        let (cnf, mdb, appmd) = (config.clone(), mdb.clone(), appmd.to_vec());
        let seed = seed + ((i as u64) * 10);
        handles.push(thread::spawn(move || {
            read_thread::<K, V, B>(prefix, i, seed, cnf, mdb, appmd, false /*incr*/)
        }));
    }

    for handle in handles.into_iter() {
        handle.join().unwrap();
    }

    mdb
}

fn do_incremental<K, V, B>(
    prefix: &str,
    seed: u64,
    bitmap: B,
    mdb: llrb::Index<K, V>,
    mut index: Index<K, V, B>,
    config: &Config,
) -> llrb::Index<K, V>
where
    for<'a> K: 'static + Key + Arbitrary<'a> + fmt::Debug,
    V: 'static + Value,
    <V as dbs::Diff>::Delta: 'static + Delta,
    B: 'static + Sync + Send + Clone + dbs::Bloom,
    rand::distributions::Standard: rand::distributions::Distribution<K>,
    rand::distributions::Standard: rand::distributions::Distribution<V>,
{
    let mut n_sets = 100_000;
    let mut n_inserts = 100_000;
    let mut n_dels = 10_000;
    let mut n_rems = 10_000;
    let n_readers = 1;

    let delta = config.delta_ok || config.value_in_vlog;
    if !delta {
        n_sets += n_inserts;
        n_rems += n_dels;
        n_inserts = 0;
        n_dels = 0;
    }

    let appmd = "test_robt_read-metadata-snap".as_bytes().to_vec();
    let snap = {
        let seqno = Some(mdb.to_seqno());
        llrb::load_index::<K, V>(seed, n_sets, n_inserts, n_rems, n_dels, seqno)
    };

    let mut build = index
        .try_clone()
        .unwrap()
        .incremental(&config.dir, &config.name, appmd.to_vec())
        .unwrap();
    build
        .build_index(
            index
                .lsm_merge(
                    snap.iter_versions().unwrap().map(Ok),
                    true, /*versions*/
                )
                .unwrap(),
            bitmap,
            Some(snap.to_seqno()),
        )
        .unwrap();
    mem::drop(build);

    mdb.commit(snap.iter().unwrap(), delta).unwrap();
    mdb.set_seqno(snap.to_seqno());

    let mut handles = vec![];
    for i in 0..n_readers {
        let prefix = prefix.to_string();
        let (config, mdb, appmd) = (config.clone(), mdb.clone(), appmd.to_vec());
        let seed = seed + ((i as u64) * 100);
        handles.push(thread::spawn(move || {
            read_thread::<K, V, B>(
                prefix, i, seed, config, mdb, appmd, true, /*incr*/
            )
        }));
    }

    for handle in handles.into_iter() {
        handle.join().unwrap();
    }

    mdb
}

fn read_thread<K, V, B>(
    prefix: String,
    id: usize,
    seed: u64,
    config: Config,
    mdb: llrb::Index<K, V>,
    app_meta_data: Vec<u8>,
    incr: bool,
) where
    for<'a> K: 'static + Key + Arbitrary<'a> + fmt::Debug,
    V: 'static + Value,
    <V as dbs::Diff>::Delta: 'static + Delta,
    B: 'static + Sync + Send + Clone + dbs::Bloom,
    rand::distributions::Standard: rand::distributions::Distribution<K>,
    rand::distributions::Standard: rand::distributions::Distribution<V>,
{
    use Error::NotFound;

    let mut rng = SmallRng::seed_from_u64(seed);

    let mut index = {
        let dir = config.dir.as_os_str();
        let file = config.to_index_location();
        open_index::<K, V, B>(dir, &config.name, &file, seed)
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
                        assert!(index.is_compacted());
                    }
                    IsEmpty => {
                        counts[2] += 1;
                        assert!(!index.is_empty());
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
                        assert!(index.to_vlog_location().is_some())
                    }
                    ToVlogLocation => {
                        counts[7] += 1;
                        assert!(index.to_vlog_location().is_none())
                    }
                    ToRoot => {
                        counts[8] += 1;
                        assert!(index.to_root().is_some(), "{:?}", index.to_root());
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
                    (Err(NotFound(_, _)), Err(Error::NotFound(_, _))) => (),
                    (Err(err1), Err(err2)) => panic!("{} != {}", err1, err2),
                    (Ok(e), Err(err)) => panic!("{:?} != {}", e, err),
                    (Err(err), Ok(e)) => panic!("{} != {:?}", err, e),
                }
            }
            Op::GetVersions(key) => {
                counts[14] += 1;
                match (index.get_versions(&key), mdb.get_versions(&key)) {
                    (Ok(e1), Ok(mut e2)) if !config.delta_ok && incr => {
                        e2.deltas = vec![];
                        assert!(e1.contains(&e2), "index:{:?} mdb:{:?}", e1, e2);
                    }
                    (Ok(e1), Ok(mut e2)) if !config.delta_ok => {
                        e2.deltas = vec![];
                        assert_eq!(e1, e2);
                    }
                    (Ok(e1), Ok(e2)) if incr => {
                        assert!(e1.contains(&e2), "index:{:?} mdb:{:?}", e1, e2);
                    }
                    (Ok(e1), Ok(e2)) => assert_eq!(e1, e2),
                    (Err(NotFound(_, _)), Err(Error::NotFound(_, _))) => (),
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
                        for mut e1 in &mut iter1 {
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
                        for mut e1 in &mut iter1 {
                            e1.deltas = vec![];
                            assert_eq!(e1, iter2.next().unwrap().unwrap())
                        }
                        assert!(iter1.next().is_none());
                        assert!(iter2.next().is_none());
                    }
                    IterVersions((l, h)) => {
                        counts[17] += 1;
                        let r = (Bound::from(l), Bound::from(h));
                        let mut iter1 = mdb.range_versions(r.clone()).unwrap();
                        let mut iter2 = index.iter_versions(r).unwrap();
                        for mut e1 in &mut iter1 {
                            if !config.delta_ok {
                                e1.deltas = vec![];
                            }
                            match iter2.next() {
                                Some(Ok(e2)) if incr => {
                                    assert!(
                                        e2.contains(&e1),
                                        "index:{:?} mdb:{:?}",
                                        e2,
                                        e1,
                                    )
                                }
                                Some(Ok(e2)) => {
                                    assert_eq!(
                                        e1,
                                        e2,
                                        "mdb:{} index:{}",
                                        e1.as_key().to_string(),
                                        e2.as_key().to_string()
                                    )
                                }
                                Some(Err(e)) => panic!("err for e2 key:{}", e),
                                None => panic!(
                                    "missing for e2 key:{}",
                                    e1.as_key().to_string()
                                ),
                            }
                        }
                        assert!(iter1.next().is_none());
                        assert!(iter2.next().is_none());
                    }
                    ReverseVersions((l, h)) => {
                        counts[18] += 1;
                        let r = (Bound::from(l), Bound::from(h));
                        let mut iter1 = mdb.reverse_versions(r.clone()).unwrap();
                        let mut iter2 = index.reverse_versions(r).unwrap();
                        for mut e1 in &mut iter1 {
                            if !config.delta_ok {
                                e1.deltas = vec![];
                            }
                            let e2 = iter2.next().unwrap().unwrap();
                            if incr {
                                assert!(e2.contains(&e1), "mdb:{:?} index:{:?}", e1, e2);
                            } else {
                                assert_eq!(e1, e2);
                            }
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

fn validate_stats<K, V>(
    stats: &Stats,
    config: &Config,
    mdb: &llrb::Index<K, V>,
    n_abytes: u64,
) where
    V: dbs::Diff,
{
    assert_eq!(stats.name, config.name);
    assert_eq!(stats.z_blocksize, config.z_blocksize);
    assert_eq!(stats.m_blocksize, config.m_blocksize);
    assert_eq!(stats.v_blocksize, config.v_blocksize);
    assert_eq!(stats.delta_ok, config.delta_ok);
    assert_eq!(stats.value_in_vlog, config.value_in_vlog);

    if config.value_in_vlog || config.delta_ok {
        assert!(stats.vlog_location.is_some())
    } else {
        assert!(stats.vlog_location.is_none())
    }

    assert_eq!(stats.n_count, mdb.len() as u64);
    assert_eq!(stats.n_deleted, mdb.deleted_count());
    assert_eq!(stats.seqno, mdb.to_seqno());
    assert_eq!(stats.n_abytes, n_abytes);
}

fn open_index<K, V, B>(
    dir: &ffi::OsStr,
    name: &str,
    file: &ffi::OsStr,
    seed: u64,
) -> Index<K, V, B>
where
    K: Clone + FromCbor,
    V: dbs::Diff + FromCbor,
    <V as dbs::Diff>::Delta: FromCbor,
    B: dbs::Bloom,
{
    let mut rng = SmallRng::seed_from_u64(seed);

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

fn validate_compact<K, V, B>(
    index: &mut Index<K, V, B>,
    cutoff: dbs::Cutoff,
    mdb: &llrb::Index<K, V>,
) where
    for<'a> K: 'static + Key + Arbitrary<'a>,
    V: 'static + Value,
    <V as dbs::Diff>::Delta: 'static + Delta,
    B: 'static + Sync + Send + Clone + dbs::Bloom,
    rand::distributions::Standard: rand::distributions::Distribution<K>,
    rand::distributions::Standard: rand::distributions::Distribution<V>,
{
    let ref_entries: Vec<dbs::Entry<K, V>> = mdb
        .iter_versions()
        .unwrap()
        .filter_map(|e| e.compact(cutoff))
        .collect();
    let entries: Vec<dbs::Entry<K, V>> = index
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
        assert!(ee.contains(&re), "i:{} mdb:{:?} index:{:?}", i, re, ee);
    }
}

fn validate_bitmap<K, V, B>(index: &mut Index<K, V, B>)
where
    for<'a> K: 'static + Key + Arbitrary<'a>,
    V: 'static + Value,
    <V as dbs::Diff>::Delta: 'static + Delta,
    B: 'static + Sync + Send + Clone + dbs::Bloom,
    rand::distributions::Standard: rand::distributions::Distribution<K>,
    rand::distributions::Standard: rand::distributions::Distribution<V>,
{
    let bitmap = index.to_bitmap();
    for entry in index.iter(..).unwrap() {
        let entry = entry.unwrap();
        assert!(bitmap.contains(entry.as_key()), "{:?}", entry.as_key());
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

impl<T> ToString for Limit<T>
where
    T: ToString,
{
    fn to_string(&self) -> String {
        match self {
            Limit::Unbounded => "limit::unbounded".to_string(),
            Limit::Included(v) => format!("limit::included({})", v.to_string()),
            Limit::Excluded(v) => format!("limit::excluded({})", v.to_string()),
        }
    }
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
