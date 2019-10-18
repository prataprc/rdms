use crate::{core::Footprint, vlog};

#[test]
fn test_value() {
    let value = vlog::Value::new_native(10);
    assert_eq!(value.footprint().unwrap(), 0);
    // encode
    let mut out = vec![];
    assert_eq!(value.encode(&mut out).unwrap(), 12);
    assert_eq!(out, vec![16, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 10]);
    // to_native_value
    assert_eq!(value.to_native_value(), Some(10));

    let value = vlog::Value::<i32>::new_reference(10, 100, 20);
    assert_eq!(value.footprint().unwrap(), 0);
    assert_eq!(value.to_native_value(), None);

    let value = vlog::Value::new_native(vec![10_u8, 20, 30]);
    assert_eq!(value.footprint().unwrap(), 3);
    // encode
    let mut out = vec![];
    assert_eq!(value.encode(&mut out).unwrap(), 15);
    assert_eq!(out, vec![16, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 3, 10, 20, 30]);
    // to_native_value
    assert_eq!(value.to_native_value(), Some(vec![10_u8, 20, 30]));
}

#[test]
fn test_fetch_value() {
    let mut path = std::env::temp_dir();
    path.push("test_fetch_value.data");

    let value = vlog::Value::new_native(vec![10_u8, 20, 30]);
    let mut refb = vec![];
    value.encode(&mut refb).unwrap();

    std::fs::write(path.clone(), &refb).expect("io failure");
    let out = std::fs::read(path).unwrap();
    assert_eq!(refb, out);
}

#[test]
fn test_delta() {
    let delta = vlog::Delta::<i32>::new_native(10);
    assert_eq!(delta.footprint().unwrap(), 0);
    // encode
    let mut out = vec![];
    assert_eq!(delta.encode(&mut out).unwrap(), 12);
    assert_eq!(out, vec![0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 10]);
    // into_native_delta
    assert_eq!(delta.into_native_delta(), Some(10));

    let delta = vlog::Delta::<i32>::new_reference(10, 100, 20);
    assert_eq!(delta.footprint().unwrap(), 0);
    assert_eq!(delta.into_native_delta(), None);

    let delta = vlog::Delta::<Vec<u8>>::new_native(vec![10_u8, 20, 30]);
    assert_eq!(delta.footprint().unwrap(), 3);
    // encode
    let mut out = vec![];
    assert_eq!(delta.encode(&mut out).unwrap(), 15);
    assert_eq!(out, vec![0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 3, 10, 20, 30]);
    // into_native_delta
    assert_eq!(delta.into_native_delta(), Some(vec![10_u8, 20, 30]));
}

#[test]
fn test_fetch_delta() {
    let mut path = std::env::temp_dir();
    path.push("test_fetch_delta.data");

    let delta = vlog::Delta::<Vec<u8>>::new_native(vec![10_u8, 20, 30]);
    let mut refb = vec![];
    delta.encode(&mut refb).unwrap();

    std::fs::write(path.clone(), &refb).expect("io failure");
    let out = std::fs::read(path).unwrap();
    assert_eq!(refb, out);
}
