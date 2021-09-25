use arbitrary::{self, unstructured::Unstructured, Arbitrary};
use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use std::{
    collections::BTreeMap,
    convert::TryFrom,
    ops::{Add, Bound, Div, Mul, Rem, Sub},
    thread,
};

use super::*;

// TODO
// +new +close +purge
// +set +set_cas +insert +insert_cas +delete +delete_cas +remove +remove_cas
// +commit +set_seqno write
// +get +iter +range +reverse +validate
// footprint +deleted_count is_empty +is_spin +len to_name +to_seqno to_stats

#[test]
fn test_llrb() {
    let seed: u128 = random();
    // let seed: u128 = 306171699234476756746827099155462650145;
    println!("test_llrb seed:{}", seed);
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let n_init = 100_000;
    let n_incr = 2000;
    let n_threads = 8;
    let spin = rng.gen::<bool>();

    // with u8 keys
    let index: Index<u64, u64> = Index::new("test_diff", spin);
    let mut btmap: BTreeMap<u64, db::Entry<u64, u64>> = BTreeMap::new();
    test_with_key(
        "test_llrb_u8",
        seed,
        &mut rng,
        n_init,
        n_incr,
        n_threads,
        u64::MAX,
        &index,
        &mut btmap,
    );
    index.close().unwrap();

    // with u16 keys
    let index: Index<u64, u64> = Index::new("test_diff", spin);
    let mut btmap: BTreeMap<u64, db::Entry<u64, u64>> = BTreeMap::new();
    test_with_key(
        "test_llrb_u16",
        seed,
        &mut rng,
        n_init,
        n_incr,
        n_threads,
        u64::MAX,
        &index,
        &mut btmap,
    );
    index.close().unwrap();

    // with u64 keys
    let index: Index<u64, u64> = Index::new("test_diff", spin);
    let mut btmap: BTreeMap<u64, db::Entry<u64, u64>> = BTreeMap::new();
    test_with_key(
        "test_llrb_u64",
        seed,
        &mut rng,
        n_init,
        n_incr,
        n_threads,
        u64::MAX,
        &index,
        &mut btmap,
    );
    index.close().unwrap();
}

#[test]
fn test_llrb_commit() {
    let seed: u128 = random();
    // let seed: u128 = 101184856431704577826207314398605498816;
    println!("test_llrb_commit seed:{}", seed);

    test_commit_with_key::<u8>("test_commit_u8", seed, u8::MAX);
    test_commit_with_key::<u16>("test_commit_u16", seed, u16::MAX);
    test_commit_with_key::<u64>("test_commit_u64", seed, 1_000_000);
}

fn test_commit_with_key<K>(prefix: &str, seed: u128, key_max: K)
where
    K: Ord + Copy + Clone + Rem<Output = K> + fmt::Debug + fmt::Display + db::Footprint,
    rand::distributions::Standard: rand::distributions::Distribution<K>,
{
    let mut btmap = BTreeMap::<K, db::Entry<K, u64>>::new();

    let index1 = {
        // (sets, ins, rems, dels)
        let load_ops = (10_000, 10_000, 1000, 1000);
        random_load_index(prefix, seed, 0 /*seqno*/, key_max, load_ops)
    };
    for e in index1.iter().unwrap() {
        btmap.insert(e.to_key(), e);
    }
    let index2 = {
        // (sets, ins, rems, dels)
        let load_ops = (10_000, 10_000, 1000, 1000);
        random_load_index(prefix, seed + 100, index1.to_seqno(), key_max, load_ops)
    };
    for e in index2.iter().unwrap() {
        let e = match btmap.get(e.as_key()) {
            Some(entry) => entry.commit(&e).unwrap(),
            None => e,
        };
        btmap.insert(e.to_key(), e);
    }
    println!("{} {} {}", prefix, index1.len(), index2.len());
    let n = index1.commit(index2.iter().unwrap()).unwrap();

    assert_eq!(n, index2.len());
    assert_eq!(index1.len(), btmap.len());

    let mut iter1 = index1.iter().unwrap();
    let iter2 = btmap.iter();

    let mut n_deleted = 0;
    let mut seqno = 0;
    for (_, e2) in iter2 {
        if e2.is_deleted() {
            n_deleted += 1;
        }
        seqno = cmp::max(seqno, e2.to_seqno());
        let e1 = iter1.next().unwrap();
        assert_eq!(&e1, e2)
    }
    assert_eq!(iter1.next(), None);
    assert_eq!(n_deleted, index1.deleted_count());
    assert_eq!(seqno, index1.to_seqno());

    index1.purge().unwrap();
    index2.purge().unwrap();
}

fn test_with_key<K>(
    prefix: &'static str,
    seed: u128,
    rng: &mut SmallRng,
    n_init: usize,
    n_incr: usize,
    n_threads: usize,
    key_max: K,
    index: &Index<K, u64>,
    btmap: &mut BTreeMap<K, db::Entry<K, u64>>,
) where
    for<'a> K: 'static
        + Send
        + Sync
        + Clone
        + Default
        + Ord
        + Arbitrary<'a>
        + Rem<Output = K>
        + Sub<K, Output = K>
        + Add<Output = K>
        + Mul<Output = K>
        + TryFrom<usize>
        + Div<Output = K>
        + fmt::Debug
        + fmt::Display
        + db::Footprint,
    <K as TryFrom<usize>>::Error: fmt::Debug,
    rand::distributions::Standard: rand::distributions::Distribution<K>,
{
    for _i in 0..n_init {
        let (key, val): (K, u64) = (rng.gen::<K>(), rng.gen::<u64>());
        let Wr { seqno, .. } = index.set(key.clone(), val).unwrap();
        btmap.insert(key.clone(), db::Entry::new(key, val, seqno));
    }
    println!(
        "{} initial load len:{}/{}",
        prefix,
        index.len(),
        btmap.len()
    );

    let mut handles = vec![];
    for id in 0..n_threads {
        let (index, bt, key_max) = (index.clone(), btmap.clone(), key_max.clone());
        let seed = seed + ((id as u128) * 100);
        let h = thread::spawn(move || {
            do_test_with_key(prefix, id, seed, n_incr, n_threads, key_max, index, bt)
        });
        handles.push(h);
    }

    let mut btmap = BTreeMap::new();
    for handle in handles.into_iter() {
        btmap = merge_btmap([btmap, handle.join().unwrap()]);
    }

    assert_eq!(index.len(), btmap.len());

    for (key, val) in btmap.iter() {
        let entry = index.get(&key).unwrap();
        assert_eq!(entry, val.clone());
    }
}

fn do_test_with_key<K>(
    _prefix: &str,
    id: usize,
    seed: u128,
    ops: usize,
    n_threads: usize,
    key_max: K,
    index: Index<K, u64>,
    mut btmap: BTreeMap<K, db::Entry<K, u64>>,
) -> BTreeMap<K, db::Entry<K, u64>>
where
    for<'a> K: Clone
        + Default
        + Ord
        + Arbitrary<'a>
        + Rem<Output = K>
        + Sub<K, Output = K>
        + Add<Output = K>
        + Mul<Output = K>
        + TryFrom<usize>
        + Div<Output = K>
        + fmt::Debug
        + fmt::Display
        + db::Footprint,
    <K as TryFrom<usize>>::Error: fmt::Debug,
{
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
    let mut counts = [0_usize; 13];
    let mut cas_fails = [0_usize; 4];
    let mut skip_rr = 0;

    for _i in 0..ops {
        let bytes = rng.gen::<[u8; 32]>();
        let mut uns = Unstructured::new(&bytes);

        let key_max = key_max.clone();
        let op: Op<K, u64> = uns.arbitrary().unwrap();
        // println!("{} {}-op -- {:?}", _prefix, id, op);
        let (_seqno, _cas) = match op {
            Op::Set(key, val) => {
                let key = key_for_thread(key, key_max, n_threads, id);
                let Wr { seqno, old_entry } = index.set(key.clone(), val).unwrap();
                let e = db::Entry::new(key.clone(), val, seqno);
                compare_old_entry(old_entry, btmap.insert(key, e));
                counts[0] += 1;
                (seqno, 0)
            }
            Op::SetCas(key, val) => {
                let key = key_for_thread(key, key_max, n_threads, id);
                let cas = index.get(&key).map(|e| e.to_seqno()).unwrap_or(0);
                let (seqno, cas) = match index.set_cas(key.clone(), val, cas) {
                    Ok(Wr { seqno, old_entry }) => {
                        let e = db::Entry::new(key.clone(), val, seqno);
                        compare_old_entry(old_entry, btmap.insert(key, e));
                        (seqno, cas)
                    }
                    Err(_) => {
                        cas_fails[0] += 1;
                        (0, cas)
                    }
                };
                counts[1] += 1;
                (seqno, cas)
            }
            Op::Insert(key, val) => {
                let key = key_for_thread(key, key_max, n_threads, id);
                let Wr { seqno, old_entry } = index.insert(key.clone(), val).unwrap();
                let e = btmap
                    .get(&key)
                    .cloned()
                    .map(|e| e.insert(val, seqno))
                    .unwrap_or(db::Entry::new(key.clone(), val, seqno));
                compare_old_entry(old_entry, btmap.insert(key.clone(), e));
                counts[2] += 1;
                (seqno, 0)
            }
            Op::InsertCas(key, val) => {
                let key = key_for_thread(key, key_max, n_threads, id);
                let cas = index.get(&key).map(|e| e.to_seqno()).unwrap_or(0);
                match index.insert_cas(key.clone(), val, cas) {
                    Ok(Wr { seqno, old_entry }) => {
                        let e = btmap
                            .get(&key.clone())
                            .cloned()
                            .map(|e| e.insert(val, seqno))
                            .unwrap_or(db::Entry::new(key.clone(), val, seqno));
                        compare_old_entry(old_entry, btmap.insert(key.clone(), e));
                    }
                    Err(_) => {
                        cas_fails[1] += 1;
                    }
                };
                counts[3] += 1;
                (0, cas)
            }
            Op::Remove(key) => {
                let key = key_for_thread(key, key_max, n_threads, id);
                let Wr { seqno, old_entry } = index.remove(&key).unwrap();
                compare_old_entry(old_entry, btmap.remove(&key));
                counts[4] += 1;
                (seqno, 0)
            }
            Op::RemoveCas(key) => {
                counts[5] += 1;
                let key = key_for_thread(key, key_max, n_threads, id);
                let cas = index.get(&key).map(|e| e.to_seqno()).unwrap_or(0);
                match index.remove_cas(&key, cas) {
                    Ok(Wr { seqno, old_entry }) => {
                        compare_old_entry(old_entry, btmap.remove(&key));
                        (seqno, cas)
                    }
                    Err(Error::InvalidCAS(_, _)) => {
                        cas_fails[2] += 1;
                        (0, cas)
                    }
                    Err(err) => panic!("{}", err),
                }
            }
            Op::Delete(key) => {
                let key = key_for_thread(key, key_max, n_threads, id);
                let Wr { seqno, old_entry } = index.delete(&key).unwrap();
                let e = btmap
                    .get(&key)
                    .cloned()
                    .map(|e| e.delete(seqno))
                    .unwrap_or(db::Entry::new_delete(key.clone(), seqno));
                compare_old_entry(old_entry, btmap.insert(key.clone(), e));
                counts[6] += 1;
                (seqno, 0)
            }
            Op::DeleteCas(key) => {
                let key = key_for_thread(key, key_max, n_threads, id);
                let cas = index.get(&key).map(|e| e.to_seqno()).unwrap_or(0);
                let (seqno, cas) = match index.delete_cas(&key, cas) {
                    Ok(Wr { seqno, old_entry }) => {
                        let e = btmap
                            .get(&key)
                            .cloned()
                            .map(|e| e.delete(seqno))
                            .unwrap_or(db::Entry::new_delete(key.clone(), seqno));
                        compare_old_entry(old_entry, btmap.insert(key.clone(), e));
                        (seqno, cas)
                    }
                    Err(Error::InvalidCAS(_, _)) => {
                        cas_fails[3] += 1;
                        (0, cas)
                    }
                    Err(err) => panic!("{}", err),
                };
                counts[7] += 1;
                (seqno, cas)
            }
            Op::Get(key) => {
                match (index.get(&key), btmap.get(&key)) {
                    (Err(Error::KeyNotFound(_, _)), None) => (),
                    (Err(err), _) => panic!("{}", err),
                    (Ok(e), Some(x)) => assert!(e.contains(x)),
                    (Ok(_), None) => (),
                }
                counts[8] += 1;
                (0, 0)
            }
            Op::Iter => {
                for (key, val) in btmap.iter() {
                    assert!(index.get(key).unwrap().contains(val))
                }
                counts[9] += 1;
                (0, 0)
            }
            Op::Range((l, h)) if asc_range(&l, &h) => {
                let r = (Bound::from(l), Bound::from(h));
                compare_iter(id, index.range(r.clone()).unwrap(), btmap.range(r), true);
                counts[10] += 1;
                (0, 0)
            }
            Op::Reverse((l, h)) if asc_range(&l, &h) => {
                let r = (Bound::from(l), Bound::from(h));
                compare_iter(
                    id,
                    index.reverse(r.clone()).unwrap(),
                    btmap.range(r).rev(),
                    false,
                );
                counts[11] += 1;
                (0, 0)
            }
            Op::Range((_, _)) | Op::Reverse((_, _)) => {
                skip_rr += 1;
                (0, 0)
            }
            Op::Validate => {
                index.validate().unwrap();
                counts[12] += 1;
                (0, 0)
            }
        };
    }

    println!(
        "{} len:{:09}/{:09}, skip_rr:{:05} cas_fails:{:?} counts:{:?}",
        id,
        index.len(),
        btmap.len(),
        skip_rr,
        cas_fails,
        counts,
    );

    btmap
}

#[derive(Clone, Debug, Arbitrary)]
enum Op<K, V> {
    Set(K, V),
    SetCas(K, V),
    Insert(K, V),
    InsertCas(K, V),
    Remove(K),
    RemoveCas(K),
    Delete(K),
    DeleteCas(K),
    Get(K),
    Iter,
    Range((Limit<K>, Limit<K>)),
    Reverse((Limit<K>, Limit<K>)),
    Validate,
}

#[derive(Clone, Debug, Arbitrary, Eq, PartialEq)]
enum Limit<T> {
    Unbounded,
    Included(T),
    Excluded(T),
}

fn asc_range<T: PartialOrd>(from: &Limit<T>, to: &Limit<T>) -> bool {
    match (from, to) {
        (Limit::Unbounded, _) => true,
        (_, Limit::Unbounded) => true,
        (Limit::Included(a), Limit::Included(b)) => a <= b,
        (Limit::Included(a), Limit::Excluded(b)) => a <= b,
        (Limit::Excluded(a), Limit::Included(b)) => a <= b,
        (Limit::Excluded(a), Limit::Excluded(b)) => b > a,
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

fn key_for_thread<K>(key: K, max: K, n_threads: usize, thread: usize) -> K
where
    K: Clone
        + Default
        + Rem<Output = K>
        + Sub<K, Output = K>
        + Add<Output = K>
        + Mul<Output = K>
        + TryFrom<usize>
        + Div<Output = K>,
    <K as TryFrom<usize>>::Error: fmt::Debug,
{
    let w = max / K::try_from(n_threads).unwrap();
    (K::try_from(thread).unwrap() * w.clone()) + (key % w)
}

fn merge_btmap<K>(
    items: [BTreeMap<K, db::Entry<K, u64>>; 2],
) -> BTreeMap<K, db::Entry<K, u64>>
where
    K: Clone + Ord,
{
    let [one, mut two] = items;
    let mut thr = BTreeMap::new();

    for (key, oe) in one.iter() {
        let e = match two.get(key) {
            Some(te) => oe.commit(te).unwrap(),
            None => oe.clone(),
        };
        let e = {
            let mut values = e.to_values();
            values.dedup();
            db::Entry::from_values(key.clone(), values).unwrap()
        };
        two.remove(key);
        thr.insert(key.clone(), e);
    }
    for (key, val) in two.iter() {
        let val = match thr.get(key) {
            Some(v) => val.commit(v).unwrap(),
            None => val.clone(),
        };
        thr.insert(key.clone(), val);
    }

    thr
}

fn compare_iter<'a, K>(
    id: usize,
    mut index: impl Iterator<Item = db::Entry<K, u64>>,
    btmap: impl Iterator<Item = (&'a K, &'a db::Entry<K, u64>)>,
    frwrd: bool,
) where
    K: 'a + Clone + PartialEq + Ord + fmt::Display,
{
    for (_key, val) in btmap {
        loop {
            let e = index.next();
            match e {
                Some(e) => match e.as_key().cmp(val.as_key()) {
                    Ordering::Equal => {
                        assert!(e.contains(&val));
                        break;
                    }
                    Ordering::Less if frwrd => (),
                    Ordering::Greater if !frwrd => (),
                    Ordering::Less | Ordering::Greater => {
                        panic!("{} error miss entry {} {}", id, e.as_key(), val.as_key())
                    }
                },
                None => panic!("{} error missing entry", id),
            }
        }
    }
}

fn compare_old_entry<K>(
    index: Option<db::Entry<K, u64>>,
    btmap: Option<db::Entry<K, u64>>,
) where
    K: PartialEq + fmt::Debug,
{
    match (index, btmap) {
        (None, None) => (),
        (index @ Some(_), btmap @ None) => panic!("{:?} {:?}", index, btmap),
        (index @ None, btmap @ Some(_)) => panic!("{:?} {:?}", index, btmap),
        (Some(e), Some(x)) => assert!(e.contains(&x)),
    }
}

fn random_load_index<K>(
    _prefix: &str,
    seed: u128,
    seqno: u64,
    key_max: K,
    (mut sets, mut ins, mut rems, mut dels): (u64, u64, u64, u64),
) -> Index<K, u64>
where
    K: Copy + Clone + Ord + Rem<Output = K> + db::Footprint + fmt::Display,
    rand::distributions::Standard: rand::distributions::Distribution<K>,
{
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
    let spin = rng.gen::<bool>();
    let index = Index::new("testing", spin);
    index.set_seqno(seqno);
    assert_eq!(index.is_spin(), spin);
    assert_eq!(index.to_seqno(), seqno);

    while (sets + ins + dels + rems) > 0 {
        let (key, value) = (rng.gen::<K>(), rng.gen::<u64>());
        let key = key % key_max;
        // println!("{} {} {}", _prefix, (sets + ins + dels + rems), key);
        match rng.gen::<u64>() % (sets + ins + dels + rems) {
            i if i < sets => {
                index.set(key, value).ok();
                sets -= 1;
            }
            i if i < (sets + ins) => {
                index.insert(key, value).ok();
                ins -= 1;
            }
            i => match index.get(&key) {
                Ok(entry) if !entry.is_deleted() && (i < (sets + ins + dels)) => {
                    index.delete(&key).unwrap();
                    dels -= 1;
                }
                Ok(entry) if !entry.is_deleted() => {
                    index.remove(&key).unwrap();
                    rems -= 1;
                }
                _ => (),
            },
        }
    }

    index
}
