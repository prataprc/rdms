use cbordata::{FromCbor, IntoCbor};

use super::*;

#[test]
fn test_nodiff() {
    let no_diff = NoDiff;

    let val = no_diff.clone().into_cbor().unwrap();
    assert_eq!(NoDiff::from_cbor(val).unwrap(), no_diff)
}
