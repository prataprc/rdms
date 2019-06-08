use crate::core::{Delta, Value};
use crate::vlog;

#[test]
fn test_delta_new_upsert() {
    let delta: Delta<i32> = Delta::new_upsert(vlog::Delta::new_native(100), 200);
    assert_eq!(delta.clone().into_diff(), Some(100));
    assert_eq!(delta.to_seqno(), 200);
    assert_eq!(delta.to_seqno_state(), (true, 200));

    match delta.clone().into_upserted() {
        Some((d, seqno)) => {
            assert_eq!(d.into_native(), Some(100));
            assert_eq!(seqno, 200);
        }
        None => assert!(false),
    }
    assert_eq!(delta.into_deleted(), None);
}

#[test]
fn test_delta_new_delete() {
    let delta: Delta<i32> = Delta::new_delete(300);
    assert_eq!(delta.clone().into_diff(), None);
    assert_eq!(delta.to_seqno(), 300);
    assert_eq!(delta.to_seqno_state(), (false, 300));

    match delta.clone().into_upserted() {
        Some(_) => assert!(false),
        _ => (),
    }
    assert_eq!(delta.into_deleted(), Some(300));
}

#[test]
fn test_value_new_upsert() {
    let value: Value<i32> = Value::new_upsert(vlog::Value::new_native(100), 200);
    assert_eq!(value.to_native_value(), Some(100));
    assert_eq!(value.is_deleted(), false);
}

#[test]
fn test_value_new_delete() {
    let value: Value<i32> = Value::new_delete(300);
    assert_eq!(value.to_native_value(), None);
    assert_eq!(value.is_deleted(), true);
}
