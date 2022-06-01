use super::*;

#[test]
fn test_value_new() {
    let value = Value::new_upsert(0x1234_u64, 1);
    assert_eq!(value.to_seqno(), 1);
    assert_eq!(value.to_value(), Some(0x1234));
    assert_eq!(value.unpack(), (1, Some(0x1234)));

    let value: Value<u64> = Value::new_delete(2);
    assert_eq!(value.to_seqno(), 2);
    assert_eq!(value.to_value(), None);
    assert_eq!(value.unpack(), (2, None));
}

#[test]
fn test_value_footprint() {
    let value = Value::new_upsert(0x1234_u64, 2);
    assert_eq!(value.footprint().unwrap(), 24);
    let value = Value::new_upsert(vec![0x1234_u64], 2);
    assert_eq!(value.footprint().unwrap(), 49, "{}", value.footprint().unwrap());
    let value = Value::new_upsert(vec!["hello world".to_string()], 2);
    assert_eq!(value.footprint().unwrap(), 76, "{}", value.footprint().unwrap());

    let value: Value<u64> = Value::new_delete(2);
    assert_eq!(value.footprint().unwrap(), 16);
}
