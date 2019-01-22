use rand::prelude::random;

use crate::llrb::{Llrb};
use crate::empty::Empty;
use crate::error::BognError;
use crate::traits::{AsEntry, AsValue};

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
    let mut llrb: Llrb<i64,i64> = Llrb::new("test-llrb", false /*lsm*/);
    let mut refns = RefNodes::new(false /*lsm*/, 10);

    assert!(llrb.set(2, 10).is_none()); refns.set(2, 10);
    assert!(llrb.set(1, 10).is_none()); refns.set(1, 10);
    assert!(llrb.set(3, 10).is_none()); refns.set(3, 10);
    assert!(llrb.set(6, 10).is_none()); refns.set(6, 10);
    assert!(llrb.set(5, 10).is_none()); refns.set(5, 10);
    assert!(llrb.set(4, 10).is_none()); refns.set(4, 10);
    assert!(llrb.set(8, 10).is_none()); refns.set(8, 10);
    assert!(llrb.set(0, 10).is_none()); refns.set(0, 10);
    assert!(llrb.set(9, 10).is_none()); refns.set(9, 10);
    assert!(llrb.set(7, 10).is_none()); refns.set(7, 10);

    assert_eq!(llrb.count(), 10);
    assert!(llrb.validate().is_ok());

    // test get
    for i in 0..10 {
        let node = llrb.get(&i);
        let refn = refns.get(i);
        check_node(node, refn);
    }
    // test iter
    for (_, node) in llrb.iter().enumerate() {
        let refn = refns.get(node.key());
        check_node(Some(node), refn);
    }
}

#[test]
fn test_cas_lsm() {
    let mut llrb: Llrb<i64,i64> = Llrb::new("test-llrb", true /*lsm*/);
    let mut refns = RefNodes::new(true /*lsm*/, 11);

    assert!(llrb.set(2, 100).is_none()); refns.set(2, 100);
    assert!(llrb.set(1, 100).is_none()); refns.set(1, 100);
    assert!(llrb.set(3, 100).is_none()); refns.set(3, 100);
    assert!(llrb.set(6, 100).is_none()); refns.set(6, 100);
    assert!(llrb.set(5, 100).is_none()); refns.set(5, 100);
    assert!(llrb.set(4, 100).is_none()); refns.set(4, 100);
    assert!(llrb.set(8, 100).is_none()); refns.set(8, 100);
    assert!(llrb.set(0, 100).is_none()); refns.set(0, 100);
    assert!(llrb.set(9, 100).is_none()); refns.set(9, 100);
    assert!(llrb.set(7, 100).is_none()); refns.set(7, 100);

    // repeated mutations on same key

    let node = llrb.set_cas(0, 200, 8).unwrap();
    let refn = refns.set_cas(0, 200, 8);
    check_node(node, refn);

    let node = llrb.set_cas(5, 200, 5).unwrap();
    let refn = refns.set_cas(5, 200, 5);
    check_node(node, refn);

    let node = llrb.set_cas(6, 200, 4).unwrap();
    let refn = refns.set_cas(6, 200, 4);
    check_node(node, refn);

    let node = llrb.set_cas(9, 200, 9).unwrap();
    let refn = refns.set_cas(9, 200, 9);
    check_node(node, refn);

    let node = llrb.set_cas(0, 300, 11).unwrap();
    let refn = refns.set_cas(0, 300, 11);
    check_node(node, refn);

    let node = llrb.set_cas(5, 300, 12).unwrap();
    let refn = refns.set_cas(5, 300, 12);
    check_node(node, refn);

    let node = llrb.set_cas(9, 300, 14).unwrap();
    let refn = refns.set_cas(9, 300, 14);
    check_node(node, refn);

    // create
    assert!(llrb.set_cas(10, 100, 0).unwrap().is_none());
    assert!(refns.set_cas(10, 100, 0).is_none());
    // error create
    assert_eq!(llrb.set_cas(10, 100, 0).err().unwrap(), BognError::InvalidCAS);
    // error insert
    assert_eq!(llrb.set_cas(9, 400, 14).err().unwrap(), BognError::InvalidCAS);

    assert_eq!(llrb.count(), 11);
    assert!(llrb.validate().is_ok());

    // test get
    for i in 0..11 {
        let node = llrb.get(&i);
        let refn = refns.get(i);
        check_node(node, refn);
    }
    // test iter
    for (_, node) in llrb.iter().enumerate() {
        let refn = refns.get(node.key());
        check_node(Some(node), refn);
    }
}

#[test]
fn test_delete() {
    let mut llrb: Llrb<i64,i64> = Llrb::new("test-llrb", false);
    let mut refns = RefNodes::new(false /*lsm*/, 11);

    assert!(llrb.set(2, 100).is_none());  refns.set(2, 100);
    assert!(llrb.set(1, 100).is_none());  refns.set(1, 100);
    assert!(llrb.set(3, 100).is_none());  refns.set(3, 100);
    assert!(llrb.set(6, 100).is_none());  refns.set(6, 100);
    assert!(llrb.set(5, 100).is_none());  refns.set(5, 100);
    assert!(llrb.set(4, 100).is_none());  refns.set(4, 100);
    assert!(llrb.set(8, 100).is_none());  refns.set(8, 100);
    assert!(llrb.set(0, 100).is_none());  refns.set(0, 100);
    assert!(llrb.set(9, 100).is_none());  refns.set(9, 100);
    assert!(llrb.set(7, 100).is_none());  refns.set(7, 100);

    // delete a missing node.
    assert!(llrb.delete(&10).is_none());
    assert!(refns.delete(10).is_none());

    assert_eq!(llrb.count(), 10);
    assert!(llrb.validate().is_ok());

    for (i, node) in llrb.iter().enumerate() {
        let refn = refns.get(i as i64);
        check_node(Some(node), refn);
    }

    // delete all entry. and set new entries
    for i in 0..10 {
        let node = llrb.delete(&i);
        let refn = refns.delete(i);
        check_node(node, refn);
    }
    assert_eq!(llrb.count(), 0);
    assert!(llrb.validate().is_ok());
    // test iter
    assert!(llrb.iter().next().is_none());
}

#[test]
fn test_crud() {
    let size = 1000;
    let mut llrb: Llrb<i64,i64> = Llrb::new("test-llrb", false /*lsm*/);
    let mut refns = RefNodes::new(false /*lsm*/, size);

    for _ in 0..100000 {
        let key: i64 = (random::<i64>() % (size as i64)).abs();
        let value: i64 = random();
        let op: i64 = (random::<i64>() % 3).abs();
        //println!("key {} value {} op {}", key, value, op);
        match op {
            0 => {
                let node = llrb.set(key, value);
                let refn = refns.set(key, value);
                check_node(node, refn);
                false
            },
            1 => {
                let refn = &refns.entries[key as usize];
                let cas = if refn.versions.len() > 0 {refn.get_seqno()} else {0};

                let node = llrb.set_cas(key, value, cas).ok().unwrap();
                let refn = refns.set_cas(key, value, cas);
                check_node(node, refn);
                false
            },
            2 => {
                let node = llrb.delete(&key);
                let refn = refns.delete(key);
                check_node(node, refn);
                true
            },
            op => panic!("unreachable {}", op),
        };

        assert!(llrb.validate().is_ok(), "validate failed");
    }

    let mut llrb_iter = llrb.iter();
    for i in 0..size {
        let refn = refns.get(i as i64);
        if refn.is_some() {
            let node = llrb_iter.next();
            check_node(node, refn);
        }
    }
}

#[test]
fn test_crud_lsm() {
    let size = 1000;
    let mut llrb: Llrb<i64,i64> = Llrb::new("test-llrb", true /*lsm*/);
    let mut refns = RefNodes::new(true /*lsm*/, size as usize);

    for _i in 0..100000 {
        let key: i64 = (random::<i64>() % size).abs();
        let value: i64 = random();
        let op: i64 = (random::<i64>() % 2).abs();
        //println!("op {} on {}", op, key);
        match op {
            0 => {
                let node = llrb.set(key, value);
                let refn = refns.set(key, value);
                check_node(node, refn);
                false
            },
            1 => {
                let refn = &refns.entries[key as usize];
                let cas = if refn.versions.len() > 0 {refn.get_seqno()} else {0};

                //println!("set_cas {} {}", key, seqno);
                let node = llrb.set_cas(key, value, cas).ok().unwrap();
                let refn = refns.set_cas(key, value, cas);
                check_node(node, refn);
                false
            },
            2 => {
                let node = llrb.delete(&key);
                let refn = refns.delete(key);
                check_node(node, refn);
                true
            },
            op => panic!("unreachable {}", op),
        };

        assert!(llrb.validate().is_ok(), "validate failed");
    }

    let mut llrb_iter = llrb.iter();
    for i in 0..size {
        let refn = refns.get(i as i64);
        if refn.is_some() {
            let node = llrb_iter.next();
            check_node(node, refn);
        }
    }
}


#[derive(Clone, Default)]
struct RefValue {
    value: i64,
    seqno: u64,
    deleted: Option<u64>
}

impl RefValue {
    fn get_seqno(&self) -> u64 {
        match self.deleted {
            None => self.seqno,
            Some(seqno) => {
                if seqno < self.seqno {
                    panic!("{} < {}", seqno, self.seqno);
                }
                seqno
            },
        }
    }
}

#[derive(Clone, Default)]
struct RefNode {
    key: i64,
    versions: Vec<RefValue>,
}

impl RefNode {
    fn get_seqno(&self) -> u64 {
        self.versions[0].get_seqno()
    }

    fn is_deleted(&self) -> bool {
        self.versions[0].deleted.is_some()
    }

    fn is_present(&self) -> bool {
        self.versions.len() > 0
    }
}

struct RefNodes{
    lsm: bool,
    seqno: u64,
    entries: Vec<RefNode>,
}

impl RefNodes {
    fn new(lsm: bool, capacity: usize) -> RefNodes {
        let mut entries: Vec<RefNode> = Vec::with_capacity(capacity);
        (0..capacity).for_each(|_| entries.push(Default::default()));
        RefNodes{lsm, seqno: 0, entries}
    }

    fn get(&self, key: i64) -> Option<RefNode> {
        let entry = self.entries[key as usize].clone();
        if entry.versions.len() == 0 { None } else { Some(entry) }
    }

    fn set(&mut self, key: i64, value: i64) -> Option<RefNode> {
        let refval = RefValue{value, seqno: self.seqno+1, deleted: None};
        let entry = &mut self.entries[key as usize];
        let refn = if entry.versions.len() > 0 {Some(entry.clone())} else {None};
        entry.key = key;
        if self.lsm || entry.versions.len() == 0 {
            entry.versions.insert(0, refval);
        } else {
            entry.versions[0] = refval;
        };
        self.seqno += 1;
        refn
    }

    fn set_cas(&mut self, key: i64, value: i64, cas: u64) -> Option<RefNode> {
        let refval = RefValue{value, seqno: self.seqno+1, deleted: None};
        let entry = &mut self.entries[key as usize];
        let ok = entry.versions.len() == 0 && cas == 0;
        if ok || (cas == entry.versions[0].seqno) {
            let refn = if entry.versions.len() > 0 {
                Some(entry.clone())
            } else {
                None
            };
            entry.key = key;
            if self.lsm || entry.versions.len() == 0 {
                entry.versions.insert(0, refval);
            } else {
                entry.versions[0] = refval;
            };
            self.seqno += 1;
            refn
        } else {
            None
        }
    }

    fn delete(&mut self, key: i64) -> Option<RefNode> {
        let entry = &mut self.entries[key as usize];

        if entry.is_present() {
            if self.lsm && entry.versions[0].deleted.is_none() {
                let refn = entry.clone();
                entry.versions[0].deleted = Some(self.seqno+1);
                self.seqno += 1;
                Some(refn)
            } else if self.lsm { // noop
                Some(entry.clone())
            } else {
                let refn = entry.clone();
                entry.versions = vec![];
                self.seqno += 1;
                Some(refn)
            }

        } else {
            if self.lsm {
                let refval = RefValue{
                    value: 0, seqno: 0, deleted: Some(self.seqno+1)
                };
                entry.versions.insert(0, refval);
                entry.key = key;
                self.seqno += 1;
            }
            None
        }
    }
}

fn check_node(node: Option<impl AsEntry<i64,i64>>, refn: Option<RefNode>) {
    if node.is_none() && refn.is_none() {
        return
    } else if node.is_none() {
        panic!("node is none but not refn {:?}", refn.unwrap().key);
    } else if refn.is_none() {
        panic!("refn is none but not node {:?}", node.unwrap().key());
    }

    let node = node.unwrap();
    let refn = refn.unwrap();
    assert_eq!(node.key(), refn.key, "key");

    assert_eq!(
        node.value().value(), refn.versions[0].value, "key {}", refn.key
    );
    assert_eq!(
        node.value().seqno(), refn.versions[0].seqno, "key {}", refn.key
    );
    assert_eq!(
        node.value().is_deleted(), refn.versions[0].deleted.is_some(),
        "key {}", refn.key
    );

    assert_eq!(node.seqno(), refn.get_seqno(), "key {}", refn.key);
    assert_eq!(node.is_deleted(), refn.is_deleted(), "key {}", refn.key);
    assert_eq!(node.versions().len(), refn.versions.len(), "key {}", refn.key);
    for (i, value) in node.versions().iter().enumerate() {
        assert_eq!(
            value.value(), refn.versions[i].value, "key {} i {}", refn.key, i,
        );
        assert_eq!(
            value.seqno(), refn.versions[i].seqno, "key {} i {}", refn.key, i
        );
        assert_eq!(
            value.is_deleted(), refn.versions[i].deleted.is_some(),
            "key {} i {}", refn.key, i
        );
    }
}
