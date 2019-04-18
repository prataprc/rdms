use crate::traits::Diff;

#[derive(Clone, Default)]
struct RefValue {
    value: i64,
    seqno: u64,
    deleted: Option<u64>,
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
            }
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

struct RefNodes {
    lsm: bool,
    seqno: u64,
    entries: Vec<RefNode>,
}

impl RefNodes {
    fn new(lsm: bool, capacity: usize) -> RefNodes {
        let mut entries: Vec<RefNode> = Vec::with_capacity(capacity);
        (0..capacity).for_each(|_| entries.push(Default::default()));
        RefNodes {
            lsm,
            seqno: 0,
            entries,
        }
    }

    fn get(&self, key: i64) -> Option<RefNode> {
        let entry = self.entries[key as usize].clone();
        if entry.versions.len() == 0 {
            None
        } else {
            Some(entry)
        }
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = &RefNode> {
        self.entries.iter().filter(|item| item.versions.len() > 0)
    }

    fn range<'a>(
        &'a self,
        low: Bound<i64>,
        high: Bound<i64>,
    ) -> Box<dyn Iterator<Item = &'a RefNode> + 'a> {
        let low = match low {
            Bound::Included(low) => low as usize,
            Bound::Excluded(low) => (low + 1) as usize,
            Bound::Unbounded => 0,
        };
        let high = match high {
            Bound::Included(high) => (high + 1) as usize,
            Bound::Excluded(high) => high as usize,
            Bound::Unbounded => self.entries.len(),
        };
        //println!("range ref compute low high {} {}", low, high);
        let ok = low < self.entries.len();
        let ok = ok && (high >= low && high <= self.entries.len());
        let entries = if ok {
            &self.entries[low..high]
        } else {
            &self.entries[..0]
        };

        //println!("range len {}", entries.len());
        let iter = entries.iter().filter(|item| item.versions.len() > 0);
        Box::new(iter)
    }

    fn reverse<'a>(
        &'a self,
        low: Bound<i64>,
        high: Bound<i64>,
    ) -> Box<dyn Iterator<Item = &'a RefNode> + 'a> {
        let low = match low {
            Bound::Included(low) => low as usize,
            Bound::Excluded(low) => (low + 1) as usize,
            Bound::Unbounded => 0,
        };
        let high = match high {
            Bound::Included(high) => (high + 1) as usize,
            Bound::Excluded(high) => high as usize,
            Bound::Unbounded => self.entries.len(),
        };
        //println!("reverse ref compute low high {} {}", low, high);
        let ok = low < self.entries.len();
        let ok = ok && (high >= low && high <= self.entries.len());
        let entries = if ok {
            &self.entries[low..high]
        } else {
            &self.entries[..0]
        };

        //println!("reverse len {}", entries.len());
        let iter = entries.iter().rev().filter(|item| item.versions.len() > 0);
        Box::new(iter)
    }

    fn set(&mut self, key: i64, value: i64) -> Option<RefNode> {
        let refval = RefValue {
            value,
            seqno: self.seqno + 1,
            deleted: None,
        };
        let entry = &mut self.entries[key as usize];
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
    }

    fn set_cas(&mut self, key: i64, value: i64, cas: u64) -> Option<RefNode> {
        let refval = RefValue {
            value,
            seqno: self.seqno + 1,
            deleted: None,
        };
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
                entry.versions[0].deleted = Some(self.seqno + 1);
                self.seqno += 1;
                Some(refn)
            } else if self.lsm {
                entry.versions[0].deleted = Some(self.seqno + 1);
                self.seqno += 1;
                Some(entry.clone())
            } else {
                let refn = entry.clone();
                entry.versions = vec![];
                self.seqno += 1;
                Some(refn)
            }
        } else {
            if self.lsm {
                let refval = RefValue {
                    value: 0,
                    seqno: 0,
                    deleted: Some(self.seqno + 1),
                };
                entry.versions.insert(0, refval);
                entry.key = key;
            }
            self.seqno += 1;
            None
        }
    }
}

fn check_node(node: Option<impl AsEntry<i64, i64>>, refn: Option<RefNode>) -> bool {
    if node.is_none() && refn.is_none() {
        return false;
    } else if node.is_none() {
        panic!("node is none but not refn {:?}", refn.unwrap().key);
    } else if refn.is_none() {
        let node = node.as_ref().unwrap();
        println!("node num_versions {}", node.deltas().len());
        panic!("refn is none but not node {:?}", node.key());
    }

    let node = node.unwrap();
    let refn = refn.unwrap();
    //println!("check_node {} {}", node.key(), refn.key);
    assert_eq!(node.key(), refn.key, "key");

    let ver = &refn.versions[0];
    assert_eq!(node.value(), ver.value, "key {}", refn.key);
    assert_eq!(node.seqno(), ver.seqno, "key {}", refn.key);
    assert_eq!(node.is_deleted(), ver.deleted.is_some(), "key {}", refn.key);
    assert_eq!(node.seqno(), refn.get_seqno(), "key {}", refn.key);
    assert_eq!(node.is_deleted(), refn.is_deleted(), "key {}", refn.key);

    let (n_vers, refn_vers) = (node.deltas().len() + 1, refn.versions.len());
    assert_eq!(n_vers, refn_vers, "key {}", refn.key);

    let mut curr_value = node.value();
    //println!("{} {}", n_vers, refn_vers);
    for (i, delta) in node.deltas().iter().rev().enumerate() {
        let ver = &refn.versions[i + 1];
        let prev_value = curr_value.merge(&delta.delta());
        assert_eq!(prev_value, ver.value, "key {} i {}", refn.key, i,);
        assert_eq!(delta.seqno(), ver.seqno, "key {} i {}", refn.key, i);
        assert_eq!(
            delta.is_deleted(),
            ver.deleted.is_some(),
            "key {} i {}",
            refn.key,
            i
        );
        curr_value = prev_value;
    }

    return true;
}

fn random_low_high(size: usize) -> (Bound<i64>, Bound<i64>) {
    let size = size as u64;
    let low = (random::<u64>() % size) as i64;
    let high = (random::<u64>() % size) as i64;
    let low = match random::<u8>() % 3 {
        0 => Bound::Included(low),
        1 => Bound::Excluded(low),
        2 => Bound::Unbounded,
        _ => unreachable!(),
    };
    let high = match random::<u8>() % 3 {
        0 => Bound::Included(high),
        1 => Bound::Excluded(high),
        2 => Bound::Unbounded,
        _ => unreachable!(),
    };
    //println!("low_high {:?} {:?}", low, high);
    (low, high)
}
