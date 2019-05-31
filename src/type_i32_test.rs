use crate::core::{Diff, Serialize};

#[test]
fn test_diff() {
    let old = 10_i32;
    let new = -20_i32;
    let diff = new.diff(&old);
    assert_eq!(diff, 10);

    assert_eq!(old, new.merge(&diff));
}

#[test]
fn test_serialize() {
    let value = 10_i32;
    let mut buf = vec![];
    value.encode(&mut buf);

    let mut out: i32 = Default::default();
    out.decode(&buf).expect("failed decode");
    assert_eq!(value, out);
}
