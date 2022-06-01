use super::*;

#[test]
fn test_llrb_node() {
    let entry = dbs::Entry::new(10, 200, 1);
    let mut node: Node<u32, u32> = entry.into();
    assert_eq!(node.footprint().unwrap(), 80);
    assert!(node.as_left_ref().is_none());
    assert!(node.as_right_ref().is_none());
    assert!(!node.is_black());
    assert_eq!(*node.as_key(), 10);
    assert_eq!(node.to_seqno(), 1);
    assert!(!node.is_deleted());

    node.set_red();
    assert!(!node.is_black());
    node.set_black();
    assert!(node.is_black());
    node.toggle_link();
    assert!(!node.is_black());

    node.set(300, 2);
    assert_eq!(dbs::Entry::new(10, 300, 2), node.entry.as_ref().clone());

    node.insert(400, 3);
    let mut entry = dbs::Entry::new(10, 400, 3);
    entry.deltas = vec![crate::dbs::Delta::U { delta: 300, seqno: 2 }];
    assert_eq!(entry, node.entry.as_ref().clone());

    node.delete(4);
    entry = entry.delete(4);
    assert_eq!(entry, node.entry.as_ref().clone());

    node.delete(5);
    entry = entry.delete(5);
    assert_eq!(entry, node.entry.as_ref().clone());

    node.insert(500, 6);
    entry = entry.insert(500, 6);
    assert_eq!(entry, node.entry.as_ref().clone());
}
