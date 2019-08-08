use crate::core::{Diff, Footprint, Serialize};

#[test]
fn test_diff() {
    let old = "hello world".as_bytes().to_vec();
    let new = "welcome".as_bytes().to_vec();
    let diff = new.diff(&old);
    assert_eq!(diff.as_slice(), "hello world".as_bytes());

    assert_eq!(old, new.merge(&diff));
}

#[test]
fn test_serialize() {
    let value = "hello world".as_bytes().to_vec();
    let mut buf = vec![];
    value.encode(&mut buf);
    let value_ref = [
        0, 0, 0, 11, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100,
    ];
    assert_eq!(&value_ref[..], buf.as_slice());

    let mut out = vec![];
    out.decode(&buf).expect("failed decode");
    assert_eq!(value, out);
}

#[test]
fn test_footprint() {
    let mut value: Vec<u8> = vec![];
    assert_eq!(value.footprint(), 0);

    value.extend_from_slice(&[10, 20, 30]);
    assert_eq!(value.footprint(), 3);
}
