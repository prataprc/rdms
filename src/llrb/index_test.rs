use arbitrary::{self, unstructured::Unstructured, Arbitrary};
use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use super::*;

use std::{collections::BTreeMap, ops::Bound, thread};

type Ky = u8;
type Entry = db::Entry<Ky, u64, u64>;

#[test]
fn test_mdb_nodiff() {
    let seed: u128 = random();
    // let seed: u128 = 306171699234476756746827099155462650145;
    println!("test_mdb_nodiff seed {}", seed);
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let n_init = 1_000;
    let n_incr = 1_000;
    let n_threads = 8;

    let index: Mdb<Ky, u64, u64> = Mdb::new("test_diff");
    let mut btmap: BTreeMap<Ky, Entry> = BTreeMap::new();

    for _i in 0..n_init {
        let (key, val): (Ky, u64) = (rng.gen(), rng.gen());
        let Wr { seqno, .. } = index.set(key, val).unwrap();
        btmap.insert(key, Entry::new(key, val, seqno));
    }

    println!("initial load len:{}/{}", index.len(), btmap.len());

    let mut handles = vec![];
    for id in 0..n_threads {
        let (index, bt) = (index.clone(), btmap.clone());
        let seed = seed + ((id as u128) * 100);
        let h = thread::spawn(move || do_nodiff_test(id, seed, n_incr, index, bt));
        handles.push(h);
    }

    let mut btmap = BTreeMap::new();
    for handle in handles.into_iter() {
        btmap = merge_btmap([btmap, handle.join().unwrap()]);
    }

    let mut n_count = 0;
    for (key, val) in btmap.iter() {
        match val.value {
            db::Value::U { value, seqno } => {
                // println!("verify {} {} {}", key, value, seqno);
                let entry = index.get(key).unwrap();
                assert_eq!(value, entry.to_value().unwrap(), "{}", key);
                assert_eq!(seqno, entry.to_seqno());
                n_count += 1;
            }
            db::Value::D { .. } => assert!(index.get(key).is_err()),
        }
    }

    assert_eq!(index.len(), n_count);
}

#[test]
fn test_mdb_diff() {
    let seed: u128 = random();
    // let seed: u128 = 231762160918118338780311754609780190356;
    println!("test_mdb_diff seed {}", seed);
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let n_init = 1_000;
    let n_incr = 1_000;
    let n_threads = 8;

    let index: Mdb<Ky, u64, u64> = Mdb::new("test_nodiff");
    let mut btmap: BTreeMap<Ky, Entry> = BTreeMap::new();

    for _i in 0..n_init {
        let (key, val): (Ky, u64) = (rng.gen(), rng.gen());
        let Wr { seqno, .. } = index.insert(key, val).unwrap();
        match btmap.get_mut(&key) {
            Some(entry) => entry.insert(val, seqno),
            None => {
                btmap.insert(key, Entry::new(key, val, seqno));
            }
        };
    }

    println!("initial load len:{}/{}", index.len(), btmap.len());

    let mut handles = vec![];
    for id in 0..n_threads {
        let (index, bt) = (index.clone(), btmap.clone());
        let seed = seed + ((id as u128) * 100);
        let h = thread::spawn(move || do_diff_test(id, seed, n_incr, index, bt));
        handles.push(h);
    }

    let mut btmap = BTreeMap::new();
    for handle in handles.into_iter() {
        btmap = merge_btmap([btmap, handle.join().unwrap()]);
    }
    for (key, val) in btmap.iter_mut() {
        let mut values = val.to_values();
        values.dedup();
        *val = Entry::from((*key, values));
    }

    assert_eq!(index.len(), btmap.len());

    for (val, (x, y)) in index.iter().unwrap().zip(btmap.iter()) {
        assert_eq!(val.as_key(), x);
        assert_eq!(val, *y, "for key {}", val.as_key());
    }
}

#[test]
fn test_commit() {
    let seed: u128 = random();
    // let seed: u128 = 101184856431704577826207314398605498816;
    println!("test_commit seed {}", seed);

    let mut btmap = BTreeMap::<u16, db::Entry<u16, u64, u64>>::new();

    let mdb1 = load_index(seed, 0 /*seqno*/, 10_000, 10_000, 1000, 1000);
    for e in mdb1.iter().unwrap() {
        btmap.insert(e.to_key(), e);
    }
    let mdb2 = load_index(seed + 100, mdb1.to_seqno(), 10_000, 10_000, 1000, 1000);
    for e in mdb2.iter().unwrap() {
        let e = match btmap.get(e.as_key()) {
            Some(entry) => entry.merge(&e),
            None => e,
        };
        btmap.insert(e.to_key(), e);
    }

    let n = mdb1.commit(mdb2.iter().unwrap()).unwrap();

    assert_eq!(n, mdb2.len());
    assert_eq!(mdb1.len(), btmap.len());

    let mut iter1 = mdb1.iter().unwrap();
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
    assert_eq!(n_deleted, mdb1.deleted_count());
    assert_eq!(seqno, mdb1.to_seqno());
}

fn do_nodiff_test(
    id: usize,
    seed: u128,
    n: usize,
    index: Mdb<Ky, u64, u64>,
    mut btmap: BTreeMap<Ky, Entry>,
) -> BTreeMap<Ky, Entry> {
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let mut counts = [0_usize; 14];

    for _i in 0..n {
        let bytes = rng.gen::<[u8; 32]>();
        let mut uns = Unstructured::new(&bytes);

        let op: NodiffOp<Ky, u64> = uns.arbitrary().unwrap();
        let (_seqno, _cas) = match op.clone() {
            NodiffOp::Set(key, val) => {
                counts[0] += 1;
                let Wr { seqno, .. } = index.set(key, val).unwrap();
                btmap.insert(key, Entry::new(key, val, seqno));
                (seqno, 0)
            }
            NodiffOp::SetCas(key, val) => {
                counts[1] += 1;
                let cas = index.get(&key).map(|e| e.to_seqno()).unwrap_or(0);
                match index.set_cas(key, val, cas) {
                    Ok(Wr { seqno, .. }) => {
                        btmap.insert(key, Entry::new(key, val, seqno));
                        (seqno, cas)
                    }
                    Err(_) => {
                        counts[2] += 1;
                        (0, cas)
                    }
                }
            }
            NodiffOp::Remove(key) => {
                counts[3] += 1;
                match index.remove(&key) {
                    Ok(Wr {
                        seqno,
                        old_entry: Some(_),
                    }) => {
                        btmap.insert(key, Entry::new_deleted(key, seqno));
                        (seqno, 0)
                    }
                    _ => (0, 0),
                }
            }
            NodiffOp::RemoveCas(key) => {
                counts[4] += 1;
                let cas = index.get(&key).map(|e| e.to_seqno()).unwrap_or(0);
                match index.remove_cas(&key, cas) {
                    Err(Error::InvalidCAS(_, _)) => {
                        counts[5] += 1;
                        (0, cas)
                    }
                    Err(err) => panic!("{}", err),
                    Ok(Wr {
                        seqno,
                        old_entry: Some(_),
                    }) => {
                        btmap.insert(key, Entry::new_deleted(key, seqno));
                        (seqno, cas)
                    }
                    Ok(_) => (0, cas),
                }
            }
            NodiffOp::Get(key) => {
                counts[8] += 1;
                index.get(&key).ok();
                (0, 0)
            }
            NodiffOp::Iter => {
                counts[9] += 1;
                let _: Vec<Entry> = index.iter().unwrap().collect();
                (0, 0)
            }
            NodiffOp::Range((l, h)) if asc_range(&l, &h) => {
                counts[10] += 1;
                let r = (Bound::from(l), Bound::from(h));
                let _: Vec<Entry> = index.range(r).unwrap().collect();
                (0, 0)
            }
            NodiffOp::Reverse((l, h)) if asc_range(&l, &h) => {
                counts[11] += 1;
                let r = (Bound::from(l), Bound::from(h));
                let _: Vec<Entry> = index.reverse(r).unwrap().collect();
                (0, 0)
            }
            NodiffOp::Range((_, _)) | NodiffOp::Reverse((_, _)) => {
                counts[12] += 1;
                (0, 0)
            }
            NodiffOp::Validate => {
                counts[13] += 1;
                index.validate().unwrap();
                (0, 0)
            }
        };
        // println!("{}-op -- {:?} seqno:{} cas:{}", id, op, _seqno, _cas);
    }

    println!(
        "{} counts {:?} len:{}/{}",
        id,
        counts,
        index.len(),
        btmap.len()
    );
    btmap
}

fn do_diff_test(
    id: usize,
    seed: u128,
    n: usize,
    index: Mdb<Ky, u64, u64>,
    mut btmap: BTreeMap<Ky, Entry>,
) -> BTreeMap<Ky, Entry> {
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let mut counts = [0_usize; 12];

    for _i in 0..n {
        let bytes = rng.gen::<[u8; 32]>();
        let mut uns = Unstructured::new(&bytes);

        let op: DiffOp<Ky, u64> = uns.arbitrary().unwrap();
        // println!("{}-op -- {:?}", id, op);
        match op {
            DiffOp::Insert(key, val) => {
                counts[0] += 1;
                let Wr { seqno, old_entry } = index.insert(key, val).unwrap();
                let val = match btmap.get(&key).cloned() {
                    Some(mut e) => {
                        e.insert(val, seqno);
                        e
                    }
                    None => Entry::new(key, val, seqno),
                };
                compare_old_entry(old_entry, btmap.insert(key, val));
            }
            DiffOp::InsertCas(key, val) => {
                counts[1] += 1;
                let cas = index.get(&key).map(|e| e.to_seqno()).unwrap_or(0);
                match index.insert_cas(key, val, cas) {
                    Ok(Wr { seqno, old_entry }) => {
                        let val = match btmap.get(&key).cloned() {
                            Some(mut e) => {
                                e.insert(val, seqno);
                                e
                            }
                            None => Entry::new(key, val, seqno),
                        };
                        compare_old_entry(old_entry, btmap.insert(key, val));
                    }
                    Err(_) => {
                        counts[2] += 1;
                    }
                };
            }
            DiffOp::Delete(key) => {
                counts[3] += 1;
                let Wr { seqno, old_entry } = index.delete(&key).unwrap();
                let val = match btmap.get(&key).cloned() {
                    Some(mut e) => {
                        e.delete(seqno);
                        e
                    }
                    None => Entry::new_deleted(key, seqno),
                };
                compare_old_entry(old_entry, btmap.insert(key, val));
            }
            DiffOp::DeleteCas(key) => {
                counts[4] += 1;
                let cas = index.get(&key).map(|e| e.to_seqno()).unwrap_or(0);
                match index.delete_cas(&key, cas) {
                    Ok(Wr { seqno, old_entry }) => {
                        let val = match btmap.get(&key).cloned() {
                            Some(mut e) => {
                                e.delete(seqno);
                                e
                            }
                            None => Entry::new_deleted(key, seqno),
                        };
                        compare_old_entry(old_entry, btmap.insert(key, val));
                    }
                    Err(Error::InvalidCAS(_, _)) => {
                        counts[5] += 1;
                    }
                    Err(err) => panic!("{}", err),
                };
            }
            DiffOp::Get(key) => {
                counts[6] += 1;
                match (index.get(&key), btmap.get(&key)) {
                    (Err(Error::KeyNotFound(_, _)), None) => (),
                    (Err(err), _) => panic!("{}", err),
                    (Ok(e), Some(x)) => assert!(e.contains(x)),
                    (Ok(_), None) => (),
                }
            }
            DiffOp::Iter => {
                counts[7] += 1;
                for (key, val) in btmap.iter() {
                    assert!(index.get(key).unwrap().contains(val))
                }
            }
            DiffOp::Range((l, h)) if asc_range(&l, &h) => {
                counts[8] += 1;
                let r = (Bound::from(l), Bound::from(h));
                compare_iter(id, index.range(r).unwrap(), btmap.range(r), true);
            }
            DiffOp::Reverse((l, h)) if asc_range(&l, &h) => {
                counts[9] += 1;
                let r = (Bound::from(l), Bound::from(h));
                compare_iter(id, index.reverse(r).unwrap(), btmap.range(r).rev(), false);
            }
            DiffOp::Range((_, _)) | DiffOp::Reverse((_, _)) => {
                counts[10] += 1;
            }
            DiffOp::Validate => {
                counts[11] += 1;
                index.validate().unwrap();
            }
        }
    }

    println!(
        "{} counts {:?} len:{}/{}",
        id,
        counts,
        index.len(),
        btmap.len()
    );

    btmap
}

#[derive(Clone, Debug, Arbitrary)]
enum NodiffOp<K, V> {
    Set(K, V),
    SetCas(K, V),
    Remove(K),
    RemoveCas(K),
    Get(K),
    Iter,
    Range((Limit<K>, Limit<K>)),
    Reverse((Limit<K>, Limit<K>)),
    Validate,
}

#[derive(Clone, Debug, Arbitrary)]
enum DiffOp<K, V> {
    Insert(K, V),
    InsertCas(K, V),
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

fn merge_btmap(items: [BTreeMap<Ky, Entry>; 2]) -> BTreeMap<Ky, Entry> {
    let [one, mut two] = items;

    let mut thr = BTreeMap::new();
    for (key, oe) in one.iter() {
        let val = match two.get(key) {
            Some(te) => oe.merge(te),
            None => oe.clone(),
        };
        two.remove(key);
        thr.insert(*key, val);
    }
    for (key, val) in two.iter() {
        let val = match thr.get(key) {
            Some(v) => val.merge(v),
            None => val.clone(),
        };
        thr.insert(*key, val);
    }

    thr
}

fn compare_iter<'a>(
    id: usize,
    mut index: impl Iterator<Item = Entry>,
    btmap: impl Iterator<Item = (&'a Ky, &'a Entry)>,
    frwrd: bool,
) {
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

fn compare_old_entry(index: Option<Entry>, btmap: Option<Entry>) {
    match (index, btmap) {
        (None, None) | (Some(_), None) => (),
        (None, Some(btmap)) => panic!("{:?}", btmap),
        (Some(e), Some(x)) => assert!(e.contains(&x)),
    }
}

fn load_index(
    seed: u128,
    seqno: u64,
    sets: u64,
    inserts: u64,
    rems: u64,
    dels: u64,
) -> Mdb<u16, u64, u64> {
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
    let index = Mdb::new("testing");
    index.set_seqno(seqno);

    let (mut se, mut it, mut de, mut rm) = (sets, inserts, dels, rems);
    while (se + it + de + rm) > 0 {
        let key: u16 = rng.gen();
        let value: u64 = rng.gen();
        // println!("{} {}", (se + it + de + rm), key);
        match rng.gen::<u64>() % (se + it + de + rm) {
            k if k < se => {
                index.set(key, value).ok();
                se -= 1;
            }
            k if k < (se + it) => {
                index.insert(key, value).ok();
                it -= 1;
            }
            k => match index.get(&key) {
                Ok(entry) if !entry.is_deleted() && (k < (se + it + de)) => {
                    index.delete(&key).unwrap();
                    de -= 1;
                }
                Ok(entry) if !entry.is_deleted() => {
                    index.remove(&key).unwrap();
                    rm -= 1;
                }
                _ => (),
            },
        }
    }

    index
}
