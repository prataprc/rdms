use super::*;

#[test]
fn test_llrb_node() {
    let entry = Entry::new(10, 200, 1);
    let mut node: Node<u32, u32> = entry.into();
    assert_eq!(node.footprint().unwrap(), 80);
    assert_eq!(node.as_left_ref().is_none(), true);
    assert_eq!(node.as_right_ref().is_none(), true);
    assert_eq!(node.is_black(), false);
    assert_eq!(*node.as_key(), 10);
    assert_eq!(node.to_seqno(), 1);
    assert_eq!(node.is_deleted(), false);

    node.set_red();
    assert_eq!(node.is_black(), false);
    node.set_black();
    assert_eq!(node.is_black(), true);
    node.toggle_link();
    assert_eq!(node.is_black(), false);

    node.set(300, 2);
    assert_eq!(Entry::new(10, 300, 2), node.entry.as_ref().clone());

    node.insert(400, 3);
    let mut entry = Entry::new(10, 400, 3);
    entry.deltas = vec![crate::db::Delta::U {
        delta: 300,
        seqno: 2,
    }];
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
