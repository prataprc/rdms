use crate::llrb::Llrb;
use crate::empty::Empty;
use crate::traits::{AsNode, AsValue};

#[test]
fn test_id() {
    let llrb: Llrb<i32,Empty> = Llrb::new("test-llrb", false);
    assert_eq!(llrb.id(), "test-llrb".to_string());
}

#[test]
fn test_seqno() {
    let mut llrb: Llrb<i32,Empty> = Llrb::new("test-llrb", false);
    assert_eq!(llrb.get_seqno(), 0);
    llrb.set_seqno(1234);
    assert_eq!(llrb.get_seqno(), 1234);
}

#[test]
fn test_count() {
    let llrb: Llrb<i32,Empty> = Llrb::new("test-llrb", false);
    assert_eq!(llrb.count(), 0);
}

#[test]
fn test_crud_set() {
    let mut llrb: Llrb<i32,Empty> = Llrb::new("test-llrb", false);
    assert!(llrb.set(2, Empty).is_none());
    assert!(llrb.set(1, Empty).is_none());
    assert!(llrb.set(3, Empty).is_none());
    assert!(llrb.set(6, Empty).is_none());
    assert!(llrb.set(5, Empty).is_none());
    assert!(llrb.set(4, Empty).is_none());
    assert!(llrb.set(8, Empty).is_none());
    assert!(llrb.set(0, Empty).is_none());
    assert!(llrb.set(9, Empty).is_none());
    assert!(llrb.set(7, Empty).is_none());

    assert_eq!(llrb.count(), 10);
    assert!(llrb.validate().is_ok());

    let refvals = [
        (0, Empty, 8, false, 8, false),
        (1, Empty, 2, false, 2, false),
        (2, Empty, 1, false, 1, false),
        (3, Empty, 3, false, 3, false),
        (4, Empty, 6, false, 6, false),
        (5, Empty, 5, false, 5, false),
        (6, Empty, 4, false, 4, false),
        (7, Empty, 10, false, 10, false),
        (8, Empty, 7, false, 7, false),
        (9, Empty, 9, false, 9, false),
    ];
    for i in 0..10 {
        let node = llrb.get(&i).unwrap();
        let i = i as usize;
        assert_eq!(node.key(), refvals[i].0);
        assert_eq!(node.value().value(), refvals[i].1);
        assert_eq!(node.value().seqno(), refvals[i].2);
        assert_eq!(node.value().is_deleted(), refvals[i].3);
        assert_eq!(node.seqno(), refvals[i].4);
        assert_eq!(node.is_deleted(), refvals[i].5);
    }
}
