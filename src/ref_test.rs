use std::convert::TryInto;

use crate::core::Entry;

#[derive(Clone, Debug)]
enum RefValue {
    U { value: i64, seqno: u64 },
    D { deleted: u64 },
}

impl RefValue {
    fn to_seqno(&self) -> u64 {
        match self {
            RefValue::U { seqno, .. } => *seqno,
            RefValue::D { deleted } => *deleted,
        }
    }

    fn to_value(&self) -> Option<i64> {
        match self {
            RefValue::U { value, .. } => Some(*value),
            RefValue::D { .. } => None,
        }
    }

    fn is_deleted(&self) -> bool {
        match self {
            RefValue::U { .. } => false,
            RefValue::D { .. } => true,
        }
    }
}

#[derive(Clone, Default, Debug)]
struct RefNode {
    key: i64,
    versions: Vec<RefValue>,
}

impl RefNode {
    fn to_seqno(&self) -> u64 {
        self.versions[0].to_seqno()
    }

    fn is_deleted(&self) -> bool {
        match self.versions[0] {
            RefValue::D { .. } => true,
            RefValue::U { .. } => false,
        }
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

    fn to_seqno(&self) -> u64 {
        self.seqno
    }

    fn get(&self, key: i64) -> Option<RefNode> {
        let off: usize = key.try_into().unwrap();
        let entry = self.entries[off].clone();
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
            Bound::Included(low) => low.try_into().unwrap(),
            Bound::Excluded(low) => (low + 1).try_into().unwrap(),
            Bound::Unbounded => 0,
        };
        let high = match high {
            Bound::Included(high) => (high + 1).try_into().unwrap(),
            Bound::Excluded(high) => high.try_into().unwrap(),
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
            Bound::Included(low) => low.try_into().unwrap(),
            Bound::Excluded(low) => (low + 1).try_into().unwrap(),
            Bound::Unbounded => 0,
        };
        let high = match high {
            Bound::Included(high) => (high + 1).try_into().unwrap(),
            Bound::Excluded(high) => high.try_into().unwrap(),
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
        let refval = RefValue::U {
            value,
            seqno: self.seqno + 1,
        };
        let off: usize = key.try_into().unwrap();
        let entry = &mut self.entries[off];
        let old = if entry.versions.len() > 0 {
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
        old
    }

    fn set_cas(&mut self, key: i64, value: i64, cas: u64) -> Option<RefNode> {
        let refval = RefValue::U {
            value,
            seqno: self.seqno + 1,
        };
        let off: usize = key.try_into().unwrap();
        let entry = &mut self.entries[off];
        let ok = entry.versions.len() == 0 && cas == 0;
        if ok || (cas == entry.versions[0].to_seqno()) {
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
            // println!("{:?} {}", entry, self.lsm);
            self.seqno += 1;
            refn
        } else {
            None
        }
    }

    fn delete(&mut self, key: i64) -> Option<RefNode> {
        let newver = RefValue::D {
            deleted: self.seqno + 1,
        };
        let off: usize = key.try_into().unwrap();
        let entry = &mut self.entries[off];
        let old = if entry.versions.len() > 0 {
            Some(entry.clone())
        } else {
            None
        };

        entry.key = key;
        if self.lsm && entry.is_present() {
            match entry.versions[0] {
                RefValue::U { .. } => {
                    entry.versions.insert(0, newver);
                    self.seqno += 1;
                    old
                }
                RefValue::D { .. } => old,
            }
        } else if self.lsm {
            entry.versions.insert(0, newver);
            self.seqno += 1;
            old
        } else if entry.is_present() {
            entry.versions.truncate(0);
            self.seqno += 1;
            old
        } else {
            None
        }
    }
}

fn check_node(entry: Option<Entry<i64, i64>>, refn: Option<RefNode>) -> bool {
    if entry.is_none() && refn.is_none() {
        return false;
    } else if entry.is_none() {
        panic!("entry is none but not refn {:?}", refn.unwrap().key);
    } else if refn.is_none() {
        let entry = entry.as_ref().unwrap();
        //println!("entry num_versions {}", entry.to_deltas().len());
        panic!("refn is none but not entry {:?}", entry.as_key());
    }

    let entry = entry.unwrap();
    let refn = refn.unwrap();
    //println!("check_node {} {}", entry.key(), refn.key);
    assert_eq!(entry.as_key().clone(), refn.key, "key");

    let ver = &refn.versions[0];
    //println!("check-node value {:?}", entry.to_native_value());
    assert_eq!(entry.to_native_value(), ver.to_value(), "key {}", refn.key);
    assert_eq!(entry.to_seqno(), ver.to_seqno(), "key {}", refn.key);
    assert_eq!(entry.is_deleted(), ver.is_deleted(), "key {}", refn.key);
    assert_eq!(entry.to_seqno(), refn.to_seqno(), "key {}", refn.key);
    assert_eq!(entry.is_deleted(), refn.is_deleted(), "key {}", refn.key);

    let (n_vers, refn_vers) = (entry.to_deltas().len() + 1, refn.versions.len());
    assert_eq!(n_vers, refn_vers, "key {}", refn.key);

    //println!("versions {} {}", n_vers, refn_vers);
    for (i, core_ver) in entry.versions().enumerate() {
        let ver = &refn.versions[i];
        let value = core_ver.to_native_value();
        assert_eq!(value, ver.to_value(), "key {} i {}", refn.key, i);
        let (seqno1, seqno2) = (core_ver.to_seqno(), ver.to_seqno());
        assert_eq!(seqno1, seqno2, "key {} i {}", refn.key, i);
        let (del1, del2) = (core_ver.is_deleted(), ver.is_deleted());
        assert_eq!(del1, del2, "key {} i {}", refn.key, i);
    }

    return true;
}

fn random_low_high(size: usize) -> (Bound<i64>, Bound<i64>) {
    let size: u64 = size.try_into().unwrap();
    let low: i64 = (random::<u64>() % size) as i64;
    let high: i64 = (random::<u64>() % size) as i64;
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
