use super::*;

#[test]
fn test_delta_new() {
    let delta = Delta::new_upsert(0x1234_u64, 1);
    assert_eq!(delta.to_seqno(), 1);
    assert_eq!(delta.to_delta(), Some(0x1234));
    assert_eq!(delta.unpack(), (1, Some(0x1234)));

    let delta: Delta<u64> = Delta::new_delete(2);
    assert_eq!(delta.to_seqno(), 2);
    assert_eq!(delta.to_delta(), None);
    assert_eq!(delta.unpack(), (2, None));
}

#[test]
fn test_delta_footprint() {
    let delta = Delta::new_upsert(0x1234_u64, 2);
    assert_eq!(delta.footprint().unwrap(), 32);
    let delta = Delta::new_upsert(vec![0x1234_u64], 2);
    assert!(
        delta.footprint().unwrap() > 64,
        "{} < 64",
        delta.footprint().unwrap()
    );
    let delta = Delta::new_upsert(vec!["hello world".to_string()], 2);
    assert!(
        delta.footprint().unwrap() > 64,
        "{} < 64",
        delta.footprint().unwrap()
    );

    let delta: Delta<u64> = Delta::new_delete(2);
    assert_eq!(delta.footprint().unwrap(), 24);
}
