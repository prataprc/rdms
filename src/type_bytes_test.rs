use crate::core::{Diff, Serialize};

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
    assert_eq!(value, buf);

    let mut out = vec![];
    out.decode(&buf).expect("failed decode");
    assert_eq!(value, out);
}
