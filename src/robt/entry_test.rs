use super::*;
use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

#[test]
fn test_robt_entry() {
    let seed: u64 = random();
    println!("test_entry {}", seed);
    let mut rng = SmallRng::seed_from_u64(seed);
    let key = 10;

    let dbnt = match rng.gen::<u8>() % 2 {
        0 => dbs::Entry::<u64, u64>::new(key, rng.gen(), 1),
        1 => dbs::Entry::<u64, u64>::new_delete(key, 1),
        _ => unreachable!(),
    };
    for seqno in 2..10 {
        match rng.gen::<u8>() % 2 {
            0 => {
                dbnt.insert(rng.gen(), seqno);
            }
            1 => {
                dbnt.delete(seqno);
            }
            _ => unreachable!(),
        }
    }
    let zz = Entry::<u64, u64, u64>::from(dbnt.clone());
    let mm = Entry::<u64, u64, u64>::new_mm(key, 100);
    let mz = Entry::<u64, u64, u64>::new_mz(key, 200);

    assert_eq!(
        dbnt,
        dbs::Entry::try_from(Entry::from(dbnt.clone())).unwrap()
    );
    assert_eq!(zz.as_key(), &key);
    assert_eq!(mz.as_key(), &key);
    assert_eq!(mm.as_key(), &key);
    assert_eq!(zz.borrow_key(), &key);
    assert_eq!(mz.borrow_key(), &key);
    assert_eq!(mm.borrow_key(), &key);
    assert_eq!(zz.to_key(), key);
    assert_eq!(mz.to_key(), key);
    assert_eq!(mm.to_key(), key);
    assert!(zz.is_zblock());
    assert!(!mz.is_zblock());
    assert!(!mm.is_zblock());

    let res = mm.clone().into_reference(0, true).unwrap();
    assert_eq!(mm, res.0);
    assert!(res.1.is_empty());
    let res = mz.clone().into_reference(0, true).unwrap();
    assert_eq!(mz, res.0);
    assert!(res.1.is_empty());

    let (zz_ref, data) = zz.clone().into_reference(0, true).unwrap();
    assert_eq!(zz_ref.to_key(), key);

    let mut data = io::Cursor::new(data);
    assert_eq!(zz_ref.into_native(&mut data, true).unwrap(), zz);
}
