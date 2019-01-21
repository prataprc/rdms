use std::fmt::Debug;

use rand::prelude::random;

use crate::llrb::{Llrb};
use crate::empty::Empty;
use crate::error::BognError;
use crate::traits::{AsKey, AsEntry, AsValue};

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
fn test_set() {
    let mut llrb: Llrb<i32,Empty> = Llrb::new("test-llrb", false /*lsm*/);
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

    let refns = [
        RefNode{
            key: 0, value: Empty, seqno: 8, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: Empty, seqno: 8, deleted: false}]
        },
        RefNode{
            key: 1, value: Empty, seqno: 2, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: Empty, seqno: 2, deleted: false}]
        },
        RefNode{
            key: 2, value: Empty, seqno: 1, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: Empty, seqno: 1, deleted: false}]
        },
        RefNode{
            key: 3, value: Empty, seqno: 3, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: Empty, seqno: 3, deleted: false}]
        },
        RefNode{
            key: 4, value: Empty, seqno: 6, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: Empty, seqno: 6, deleted: false}]
        },
        RefNode{
            key: 5, value: Empty, seqno: 5, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: Empty, seqno: 5, deleted: false}]
        },
        RefNode{
            key: 6, value: Empty, seqno: 4, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: Empty, seqno: 4, deleted: false}]
        },
        RefNode{
            key: 7, value: Empty, seqno: 10, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: Empty, seqno: 10, deleted: false}]
        },
        RefNode{
            key: 8, value: Empty, seqno: 7, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: Empty, seqno: 7, deleted: false}]
        },
        RefNode{
            key: 9, value: Empty, seqno: 9, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: Empty, seqno: 9, deleted: false}]
        },
    ];

    // test get
    for i in 0..10 {
        let node = llrb.get(&i);
        check_node(node, &refns[i as usize], true);
    }
    // test iter
    for (i, node) in llrb.iter().enumerate() {
        check_node(Some(node), &refns[i], false)
    }
}

#[test]
fn test_cas_lsm() {
    let mut llrb: Llrb<i32,i32> = Llrb::new("test-llrb", true /*lsm*/);
    assert!(llrb.set(2, 100).is_none());
    assert!(llrb.set(1, 100).is_none());
    assert!(llrb.set(3, 100).is_none());
    assert!(llrb.set(6, 100).is_none());
    assert!(llrb.set(5, 100).is_none());
    assert!(llrb.set(4, 100).is_none());
    assert!(llrb.set(8, 100).is_none());
    assert!(llrb.set(0, 100).is_none());
    assert!(llrb.set(9, 100).is_none());
    assert!(llrb.set(7, 100).is_none());
    // repeated mutations on same key
    let node = llrb.set_cas(0, 200, 8).unwrap();
    check_node(
        node,
        &RefNode{
            key: 0, value: 100, seqno: 8, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: 100, seqno: 8, deleted: false}]
        },
        true,
    );
    let node = llrb.set_cas(5, 200, 5).unwrap();
    check_node(
        node,
        &RefNode{
            key: 5, value: 100, seqno: 5, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: 100, seqno: 5, deleted: false}]
        },
        true
    );
    let node = llrb.set_cas(6, 200, 4).unwrap();
    check_node(
        node,
        &RefNode{
            key: 6, value: 100, seqno: 4, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: 100, seqno: 4, deleted: false}]
        },
        true
    );
    let node = llrb.set_cas(9, 200, 9).unwrap();
    check_node(
        node,
        &RefNode{
            key: 9, value: 100, seqno: 9, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: 100, seqno: 9, deleted: false}]
        },
        true
    );
    let node = llrb.set_cas(0, 300, 11).unwrap();
    check_node(
        node,
        &RefNode{
            key: 0, value: 200, seqno: 11, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: 200, seqno: 11, deleted: false}]
        },
        true
    );
    let node = llrb.set_cas(5, 300, 12).unwrap();
    check_node(
        node,
        &RefNode{
            key: 5, value: 200, seqno: 12, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: 200, seqno: 12, deleted: false}]
        },
        true
    );
    let node = llrb.set_cas(9, 300, 14).unwrap();
    check_node(
        node,
        &RefNode{
            key: 9, value: 200, seqno: 14, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: 200, seqno: 14, deleted: false}]
        },
        true
    );
    // create
    assert!(llrb.set_cas(10, 100, 0).unwrap().is_none());
    // error create
    assert_eq!(llrb.set_cas(10, 100, 0).err().unwrap(), BognError::InvalidCAS);
    // error insert
    assert_eq!(llrb.set_cas(9, 400, 14).err().unwrap(), BognError::InvalidCAS);

    assert_eq!(llrb.count(), 11);
    assert!(llrb.validate().is_ok());

    let refns = [
        RefNode{
            key: 0, value: 300, seqno: 15, deleted: false, num_versions: 3,
            versions: vec![
                RefValue{value: 300, seqno: 15, deleted: false},
                RefValue{value: 200, seqno: 11, deleted: false},
                RefValue{value: 100, seqno: 8, deleted: false},
            ]
        },
        RefNode{key: 1, value: 100, seqno: 2, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: 100, seqno: 2, deleted: false}]},
        RefNode{key: 2, value: 100, seqno: 1, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: 100, seqno: 1, deleted: false}]},
        RefNode{key: 3, value: 100, seqno: 3, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: 100, seqno: 3, deleted: false}]},
        RefNode{key: 4, value: 100, seqno: 6, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: 100, seqno: 6, deleted: false}]},
        RefNode{
            key: 5, value: 300, seqno: 16, deleted: false, num_versions: 3,
            versions: vec![
                RefValue{value: 300, seqno: 16, deleted: false},
                RefValue{value: 200, seqno: 12, deleted: false},
                RefValue{value: 100, seqno: 5, deleted: false},
            ]
        },
        RefNode{
            key: 6, value: 200, seqno: 13, deleted: false, num_versions: 2,
            versions: vec![
                RefValue{value: 200, seqno: 13, deleted: false},
                RefValue{value: 100, seqno: 4, deleted: false},
            ]
        },
        RefNode{key: 7, value: 100, seqno: 10, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: 100, seqno: 10, deleted: false}]},
        RefNode{key: 8, value: 100, seqno: 7, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: 100, seqno: 7, deleted: false}]},
        RefNode{
            key: 9, value: 300, seqno: 17, deleted: false, num_versions: 3,
            versions: vec![
                RefValue{value: 300, seqno: 17, deleted: false},
                RefValue{value: 200, seqno: 14, deleted: false},
                RefValue{value: 100, seqno: 9, deleted: false},
            ]
        },
        RefNode{key:10, value: 100, seqno: 18, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: 100, seqno: 18, deleted: false}]},
    ];
    // test get
    for i in 0..11 {
        let node = llrb.get(&i);
        check_node(node, &refns[i as usize], false);
    }
    // test iter
    for (i, node) in llrb.iter().enumerate() {
        check_node(Some(node), &refns[i], false)
    }
}

#[test]
fn test_delete() {
    let mut llrb: Llrb<i32,i32> = Llrb::new("test-llrb", false);
    assert!(llrb.set(2, 100).is_none());
    assert!(llrb.set(1, 100).is_none());
    assert!(llrb.set(3, 100).is_none());
    assert!(llrb.set(6, 100).is_none());
    assert!(llrb.set(5, 100).is_none());
    assert!(llrb.set(4, 100).is_none());
    assert!(llrb.set(8, 100).is_none());
    assert!(llrb.set(0, 100).is_none());
    assert!(llrb.set(9, 100).is_none());
    assert!(llrb.set(7, 100).is_none());

    // delete a missing node.
    assert!(llrb.delete(&10).is_none());
    let refns = [
        RefNode{
            key: 0, value: 100, seqno: 8, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: 100, seqno: 8, deleted: false}]
        },
        RefNode{
            key: 1, value: 100, seqno: 2, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: 100, seqno: 2, deleted: false}]
        },
        RefNode{
            key: 2, value: 100, seqno: 1, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: 100, seqno: 1, deleted: false}]
        },
        RefNode{
            key: 3, value: 100, seqno: 3, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: 100, seqno: 3, deleted: false}]
        },
        RefNode{
            key: 4, value: 100, seqno: 6, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: 100, seqno: 6, deleted: false}]
        },
        RefNode{
            key: 5, value: 100, seqno: 5, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: 100, seqno: 5, deleted: false}]
        },
        RefNode{
            key: 6, value: 100, seqno: 4, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: 100, seqno: 4, deleted: false}]
        },
        RefNode{
            key: 7, value: 100, seqno: 10, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: 100, seqno: 10, deleted: false}]
        },
        RefNode{
            key: 8, value: 100, seqno: 7, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: 100, seqno: 7, deleted: false}]
        },
        RefNode{
            key: 9, value: 100, seqno: 9, deleted: false, num_versions: 1,
            versions: vec![RefValue{value: 100, seqno: 9, deleted: false}]
        },
    ];
    assert_eq!(llrb.count(), 10);
    assert!(llrb.validate().is_ok());
    for (i, node) in llrb.iter().enumerate() {
        check_node(Some(node), &refns[i], false);
    }

    // delete all entry. and set new entries
    for i in 0..10 {
        let node = llrb.delete(&i);
        check_node(node, &refns[i as usize], true);
    }
    assert_eq!(llrb.count(), 0);
    assert!(llrb.validate().is_ok());
    // test iter
    assert!(llrb.iter().next().is_none());
}

#[test]
fn test_crud() {
    let mut llrb: Llrb<i32,i32> = Llrb::new("test-llrb", false /*lsm*/);

    let mut refns: Vec<RefNode<i32,i32>> = vec![];
    let size = 1000;
    for i in 0..1000 {
        let mut refn: RefNode<i32,i32> = Default::default();
        refn.key = i;
        refn.versions = vec![Default::default()];
        refns.push(refn);
    }

    for i in 0..100000 {
        let key: i32 = (random::<i32>() % size).abs();
        let value: i32 = random();
        let op: i32 = (random::<i32>() % 3).abs();
        let deleted: bool = match op {
            0 => {
                let node = llrb.set(key, value);
                check_node(node, &refns[key as usize], true);
                false
            },
            1 => {
                let seqno = refns[key as usize].seqno;
                let node = llrb.set_cas(key, value, seqno).ok().unwrap();
                check_node(node, &refns[key as usize], true);
                false
            },
            2 => {
                let node = llrb.delete(&key);
                check_node(node, &refns[key as usize], true);
                true
            },
            op => panic!("unreachable {}", op),
        };

        // update reference
        let refn = &mut refns[key as usize];
        if deleted {
            *refn = Default::default();
            refn.key = key;
            refn.versions = vec![Default::default()];

        } else {
            refn.value = value;
            refn.seqno = i+1;
            refn.deleted = deleted;
            refn.num_versions = 1;
            refn.versions[0] = RefValue{value, seqno: i+1, deleted};
        }
        assert!(llrb.validate().is_ok(), "validate failed");
    }

    let mut llrb_iter = llrb.iter();
    for i in 0..size {
        let refn = &refns[i as usize];
        if refn.seqno == 0 { continue }

        let node = llrb_iter.next();
        check_node(node, refn, false);
    }
}

#[test]
fn test_crud_lsm() {
    let mut llrb: Llrb<i32,i32> = Llrb::new("test-llrb", true /*lsm*/);

    let mut refns: Vec<RefNode<i32,i32>> = vec![];
    let size = 1000;
    for i in 0..1000 {
        let mut refn: RefNode<i32,i32> = Default::default();
        refn.key = i;
        refns.push(refn);
    }

    for i in 0..100000 {
        let key: i32 = (random::<i32>() % size).abs();
        let value: i32 = random();
        let op: i32 = (random::<i32>() % 2).abs();
        //println!("op {} on {}", op, key);
        let deleted: bool = match op {
            0 => {
                let node = llrb.set(key, value);
                check_node(node, &refns[key as usize], true);
                false
            },
            1 => {
                let seqno = refns[key as usize].seqno;
                //println!("set_cas {} {}", key, seqno);
                let node = llrb.set_cas(key, value, seqno).ok().unwrap();
                check_node(node, &refns[key as usize], true);
                false
            },
            2 => {
                let node = llrb.delete(&key);
                check_node(node, &refns[key as usize], true);
                true
            },
            op => panic!("unreachable {}", op),
        };

        // update reference
        let refn = &mut refns[key as usize];
        refn.value = value;
        refn.seqno = i+1;
        refn.deleted = deleted;
        refn.num_versions += 1;
        refn.versions.insert(0, RefValue{value, seqno: i+1, deleted});
        assert!(llrb.validate().is_ok(), "validate failed");
    }

    let mut llrb_iter = llrb.iter();
    for i in 0..size {
        let refn = &refns[i as usize];
        if refn.seqno == 0 { continue }

        let node = llrb_iter.next();
        check_node(node, refn, false);
    }
}


#[derive(Default)]
struct RefValue<V>{
    value: V,
    seqno: u64,
    deleted: bool,
}

#[derive(Default)]
struct RefNode<K,V>{
    key: K,
    value: V,
    seqno: u64,
    deleted: bool,
    num_versions: usize,
    versions: Vec<RefValue<V>>,
}

fn check_node<K,V>(
    node: Option<impl AsEntry<K,V>>, refn: &RefNode<K,V>, ret: bool)
where
    K: AsKey,
    V: Default + Clone + PartialEq + Debug,
{
    if node.is_none() {
        assert_eq!(refn.seqno, 0);
        return
    }

    let node = node.unwrap();
    assert_eq!(node.key(), refn.key, "key");

    if refn.seqno == 0 { return }

    assert_eq!(node.value().value(), refn.versions[0].value, "val_value");
    assert_eq!(node.value().seqno(), refn.versions[0].seqno, "val_seqno");
    assert_eq!(node.value().is_deleted(), refn.versions[0].deleted, "val_del");
    assert_eq!(node.seqno(), refn.seqno, "node_seqno");
    assert_eq!(node.is_deleted(), refn.deleted, "node_del");
    let num_versions = if ret { 1 } else { refn.num_versions };
    for (i, value) in node.versions().iter().enumerate() {
        assert_eq!(value.value(), refn.versions[i].value, "version.value {}", i);
        assert_eq!(value.seqno(), refn.versions[i].seqno, "version.seqno {}", i);
        assert_eq!(value.is_deleted(), refn.versions[i].deleted, "version {}", i);
        if ret { break }
    }
    assert_eq!(node.versions().len(), num_versions, "num_versions");
}
