use crate::{
    core::{Diff, Footprint, Serialize},
    types::Empty,
};

#[test]
fn test_bytes_diff() {
    let old = "hello world".as_bytes().to_vec();
    let new = "welcome".as_bytes().to_vec();
    let diff = new.diff(&old);
    assert_eq!(diff.as_slice(), "hello world".as_bytes());

    assert_eq!(old, new.merge(&diff));
}

#[test]
fn test_bytes_serialize() {
    let value = "hello world".as_bytes().to_vec();
    let mut buf = vec![];
    value.encode(&mut buf).unwrap();
    let value_ref = [
        0, 0, 0, 11, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100,
    ];
    assert_eq!(&value_ref[..], buf.as_slice());

    let mut out = vec![];
    out.decode(&buf).expect("failed decode");
    assert_eq!(value, out);
}

#[test]
fn test_bytes_footprint() {
    let mut value: Vec<u8> = vec![];
    assert_eq!(value.footprint().unwrap(), 0);

    value.extend_from_slice(&[10, 20, 30]);
    assert_eq!(value.footprint().unwrap(), 3);
}

#[test]
fn test_empty_diff() {
    let old = Empty;
    let new = Empty;
    let diff = new.diff(&old);
    assert_eq!(diff, Empty);

    assert_eq!(old, new.merge(&diff));
}

#[test]
fn test_empty_serialize() {
    let value = Empty;
    let mut buf = vec![];
    value.encode(&mut buf).unwrap();

    let mut out = Empty;
    out.decode(&buf).expect("failed decode");
    assert_eq!(value, out);
}

#[test]
fn test_empty_footprint() {
    let value = Empty;
    assert_eq!(value.footprint().unwrap(), 0);
}

#[test]
fn test_i32_diff() {
    let old = 10_i32;
    let new = -20_i32;
    let diff = new.diff(&old);
    assert_eq!(diff, 10);

    assert_eq!(old, new.merge(&diff));
}

#[test]
fn test_i32_serialize() {
    let value = 10_i32;
    let mut buf = vec![];
    value.encode(&mut buf).unwrap();

    let mut out: i32 = Default::default();
    out.decode(&buf).expect("failed decode");
    assert_eq!(value, out);
}

#[test]
fn test_i32_footprint() {
    let value = 0_i32;
    assert_eq!(value.footprint().unwrap(), 0);
}

#[test]
fn test_i64_diff() {
    let old = 10_i64;
    let new = -20_i64;
    let diff = new.diff(&old);
    assert_eq!(diff, 10);

    assert_eq!(old, new.merge(&diff));
}

#[test]
fn test_i64_serialize() {
    let value = 10_i64;
    let mut buf = vec![];
    value.encode(&mut buf).unwrap();

    let mut out: i64 = Default::default();
    out.decode(&buf).expect("failed decode");
    assert_eq!(value, out);
}

#[test]
fn test_i64_footprint() {
    let value = 0_i64;
    assert_eq!(value.footprint().unwrap(), 0);
}
