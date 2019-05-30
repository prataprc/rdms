use crate::core::{Diff, Serialize};
use crate::type_empty::Empty;

#[test]
fn test_diff() {
    let old = Empty;
    let new = Empty;
    let diff = new.diff(&old);
    assert_eq!(diff, Empty);

    assert_eq!(old, new.merge(&diff));
}

#[test]
fn test_serialize() {
    let value = Empty;
    let mut buf = vec![];
    value.encode(&mut buf);

    let mut out = Empty;
    out.decode(&buf).expect("failed decode");
    assert_eq!(value, out);
}
