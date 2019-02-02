use std::borrow::Borrow;
use std::cmp::{Ord, Ordering};
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ops::{Bound, Deref, DerefMut};
use std::sync::{atomic::AtomicPtr, Arc, Mutex, RwLock};

use crate::error::BognError;
use crate::traits::{AsEntry, AsKey, AsValue};

include!("llrb_single.rs");
include!("llrb_mvcc.rs");
include!("llrb_node.rs");

// TODO: Sizing.
// TODO: Implement and document primitive types, std-types that can be used
// as key (K) / value (V) for Llrb.
// TODO: optimize comparison
// TODO: llrb_depth_histogram, as feature, to measure the depth of LLRB tree.

/// Llrb manage a single instance of in-memory sorted index using
/// [left-leaning-red-black][llrb] tree.
///
/// **[LSM mode]**: Llrb instance can support what is called as
/// log-structured-merge while mutating the tree. In simple terms, this
/// means that nothing shall be over-written in the tree and all the
/// mutations for the same key shall be preserved until they are undone or
/// purged. Although there is one exception to it, back-to-back deletes
/// will collapse into a no-op, only the first delete shall be ingested.
///
/// IMPORTANT: This tree is not thread safe.
///
/// [llrb]: https://en.wikipedia.org/wiki/Left-leaning_red-black_tree
/// [LSM mode]: https://en.wikipedia.org/wiki/Log-structured_merge-tree
pub struct Llrb<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    name: String,
    lsm: bool,
    is_mvcc: bool,
    // TODO: can we handle this as union single_root | mvcc_root ?
    single_root: SingleRoot<K, V>,
    snapshot: Snapshot<K, V>,
    mutex: Mutex<i32>,
    rw: RwLock<i32>,
}

impl<K, V> Drop for Llrb<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    fn drop(&mut self) {
        use std::sync::atomic::Ordering::Relaxed;

        let _arc = unsafe { Arc::from_raw(self.snapshot.value.load(Relaxed)) };
    }
}

/// Different ways to construct a new Llrb instance.
impl<K, V> Llrb<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    /// Create an empty instance of Llrb, identified by `name`.
    /// Applications can choose unique names. When `lsm` is true, mutations
    /// are added as log for each key, instead of over-writing previous
    /// mutation.
    pub fn new<S>(name: S, lsm: bool) -> Llrb<K, V>
    where
        S: AsRef<str>,
    {
        let store = Llrb {
            name: name.as_ref().to_string(),
            lsm,
            is_mvcc: false,
            single_root: SingleRoot::new(),
            snapshot: Snapshot::new(),
            mutex: Mutex::new(0),
            rw: RwLock::new(0),
        };
        store
    }

    /// Create an empty instance of Llrb in MVCC mode, identified by `name`.
    /// Applications can choose unique names. When `lsm` is true, mutations
    /// are added as log for each key, instead of over-writing previous
    /// mutation.
    pub fn new_mvcc<S>(name: S, lsm: bool) -> Llrb<K, V>
    where
        S: AsRef<str>,
    {
        let store = Llrb {
            name: name.as_ref().to_string(),
            lsm,
            is_mvcc: true,
            single_root: SingleRoot::new(),
            snapshot: Snapshot::new(),
            mutex: Mutex::new(0),
            rw: RwLock::new(0),
        };
        store
    }

    /// Create a new instance of Llrb tree and load it with entries from
    /// `iter`. Note that iterator shall return items that implement [`AsEntry`].
    pub fn load_from<E>(
        name: String,
        iter: impl Iterator<Item = E>,
        lsm: bool,
        mvcc: bool,
    ) -> Result<Llrb<K, V>, BognError>
    where
        E: AsEntry<K, V>,
        <E as AsEntry<K, V>>::Value: Default + Clone,
    {
        let mut store = Llrb::new(name, lsm);

        let (mut n_count, mut seqno) = (0_u64, 0_64);
        let mut root = store.single_root.root.take();
        for entry in iter {
            let e_seqno = entry.seqno();
            root = match Llrb::load_entry(root, entry.key(), entry)? {
                Some(mut root) => {
                    if e_seqno > seqno {
                        seqno = e_seqno;
                    }
                    root.set_black();
                    Some(root)
                }
                None => unreachable!(),
            };
            n_count += 1;
        }
        store.single_root.root = root;
        store.single_root.n_count = n_count;
        store.single_root.seqno = seqno;

        if mvcc {
            store.is_mvcc = true;

            let root = store.single_root.root.take();
            let seqno = store.single_root.seqno;
            let n_count = store.single_root.n_count;
            let reclaim = vec![];
            store
                .snapshot
                .move_next_snapshot(root, seqno, n_count, reclaim, &store.rw);
        }

        Ok(store)
    }

    fn load_entry<E>(
        node: Option<Box<Node<K, V>>>,
        key: K,
        entry: E,
    ) -> Result<Option<Box<Node<K, V>>>, BognError>
    where
        E: AsEntry<K, V>,
        <E as AsEntry<K, V>>::Value: Default + Clone,
    {
        if node.is_none() {
            let node: Node<K, V> = Node::from_entry(entry);
            Ok(Some(Box::new(node)))
        } else {
            let mut node = node.unwrap();
            node = Single::walkdown_rot23(node);
            match node.key.cmp(&key) {
                Ordering::Greater => {
                    node.left = Llrb::load_entry(node.left, key, entry)?;
                    Ok(Some(Single::walkuprot_23(node)))
                }
                Ordering::Less => {
                    node.right = Llrb::load_entry(node.right, key, entry)?;
                    Ok(Some(Single::walkuprot_23(node)))
                }
                Ordering::Equal => {
                    let err = format!("load_entry: {:?}", key);
                    Err(BognError::DuplicateKey(err))
                }
            }
        }
    }
}

/// Maintanence API.
impl<K, V> Llrb<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    /// Identify this instance. Applications can choose unique names while
    /// creating Llrb instances.
    pub fn id(&self) -> String {
        self.name.clone()
    }

    /// Return number of entries in this instance.
    pub fn count(&self) -> u64 {
        if self.is_mvcc {
            self.snapshot.clone(&self.rw).n_count
        } else {
            self.single_root.n_count
        }
    }

    /// Set current seqno.
    pub fn set_seqno(&mut self, seqno: u64) {
        let _lock = self.mutex.lock();

        if self.is_mvcc {
            let mut arc = self.snapshot.clone(&self.rw);
            let root = Arc::get_mut(&mut arc).unwrap().owned_root();
            let seqno = arc.seqno;
            let n_count = arc.n_count;
            self.snapshot
                .move_next_snapshot(root, seqno, n_count, vec![], &self.rw);
        } else {
            self.single_root.seqno = seqno
        }
    }

    /// Return current seqno.
    pub fn get_seqno(&self) -> u64 {
        if self.is_mvcc {
            self.snapshot.clone(&self.rw).seqno
        } else {
            self.single_root.seqno
        }
    }
}

/// CRUD operations on Llrb instance.
impl<K, V> Llrb<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    /// Get the latest version for key.
    pub fn get<Q>(&self, key: &Q) -> Option<impl AsEntry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        if self.is_mvcc {
            let arc = self.snapshot.clone(&self.rw);
            let mut node = arc.as_ref().as_ref();
            while node.is_some() {
                let nref = node.unwrap();
                node = match nref.key.borrow().cmp(key) {
                    Ordering::Less => nref.right_deref(),
                    Ordering::Greater => nref.left_deref(),
                    Ordering::Equal => return Some(nref.clone_detach()),
                };
            }
        } else {
            let mut node = self.single_root.as_ref();
            while node.is_some() {
                let nref = node.unwrap();
                node = match nref.key.borrow().cmp(key) {
                    Ordering::Less => nref.right_deref(),
                    Ordering::Greater => nref.left_deref(),
                    Ordering::Equal => return Some(nref.clone_detach()),
                };
            }
        };
        None
    }

    /// Return an iterator over all entries in this instance.
    pub fn iter(&self) -> Iter<K, V> {
        if self.is_mvcc {
            Iter {
                arc: self.snapshot.clone(&self.rw),
                llrb: self,
                node_iter: vec![].into_iter(),
                after_key: Bound::Unbounded,
                limit: 100,
                fin: false,
            }
        } else {
            Iter {
                arc: Default::default(),
                llrb: self,
                node_iter: vec![].into_iter(),
                after_key: Bound::Unbounded,
                limit: 100,
                fin: false,
            }
        }
    }

    /// Range over all entries from low to high.
    pub fn range(&self, low: Bound<K>, high: Bound<K>) -> Range<K, V> {
        if self.is_mvcc {
            Range {
                arc: self.snapshot.clone(&self.rw),
                llrb: self,
                node_iter: vec![].into_iter(),
                low,
                high,
                limit: 100, // TODO: no magic number.
                fin: false,
            }
        } else {
            Range {
                arc: Default::default(),
                llrb: self,
                node_iter: vec![].into_iter(),
                low,
                high,
                limit: 100, // TODO: no magic number.
                fin: false,
            }
        }
    }

    /// Set operation for non-mvcc instance. If key is already
    /// present, return the previous entry. In LSM mode, this will
    /// add a new version for the key.
    ///
    /// If an entry already exist for the, return the old-entry will all its
    /// versions.
    pub fn set(&mut self, key: K, value: V) -> Option<impl AsEntry<K, V>> {
        if self.is_mvcc {
            panic!("use shared reference in mvcc mode !!");
        }
        Single::set(self, key, value)
    }

    /// Set operation for mvcc instance. If key is already present, return
    /// the previous entry. In LSM mode, this will add a new version for the
    /// key.
    ///
    /// If an entry already exist for the, return the old-entry will all its
    /// versions.
    pub fn set_mvcc(&self, key: K, value: V) -> Option<impl AsEntry<K, V>> {
        if !self.is_mvcc {
            panic!("use mutable reference in non-mvcc mode !!");
        }

        let _lock = self.mutex.lock();
        Mvcc::set(self, key, value)
    }

    /// Set a new entry into a non-mvcc instance, only if entry's seqno matches
    /// the supplied CAS. Use CAS == 0 to enforce a create operation. If key is
    /// already present, return the previous entry. In LSM mode, this will add
    /// a new version for the key.
    pub fn set_cas(
        &mut self,
        key: K,
        value: V,
        cas: u64,
    ) -> Result<Option<impl AsEntry<K, V>>, BognError> {
        if self.is_mvcc {
            panic!("use shared reference in mvcc mode !!");
        }
        Single::set_cas(self, key, value, cas)
    }

    /// Set a new entry into a mvcc instance, only if entry's seqno matches the
    /// supplied CAS. Use CAS == 0 to enforce a create operation. If key is
    /// already present, return the previous entry. In LSM mode, this will add
    /// a new version for the key.
    pub fn set_cas_mvcc(
        &self,
        key: K,
        value: V,
        cas: u64,
    ) -> Result<Option<impl AsEntry<K, V>>, BognError> {
        if !self.is_mvcc {
            panic!("use mutable reference in non-mvcc mode !!");
        }

        let _lock = self.mutex.lock();
        Mvcc::set_cas(self, key, value, cas)
    }

    /// Delete the given key from non-mvcc intance, in LSM mode it simply marks
    /// the version as deleted. Note that back-to-back delete for the same
    /// key shall collapse into a single delete.
    pub fn delete<Q>(&mut self, key: &Q) -> Option<impl AsEntry<K, V>>
    where
        K: Borrow<Q> + From<Q>,
        Q: Clone + Ord + ?Sized,
    {
        if self.is_mvcc {
            panic!("use shared reference in mvcc mode !!");
        }
        Single::delete(self, key)
    }

    /// Delete the given key from mvcc intance, in LSM mode it simply marks
    /// the version as deleted. Note that back-to-back delete for the same
    /// key shall collapse into a single delete.
    pub fn delete_mvcc<Q>(&self, key: &Q) -> Option<impl AsEntry<K, V>>
    where
        K: Borrow<Q> + From<Q>,
        Q: Clone + Ord + ?Sized,
    {
        if !self.is_mvcc {
            panic!("use mutable reference in non-mvcc mode !!");
        }

        let _lock = self.mutex.lock();
        Mvcc::delete(self, key)
    }

    /// validate llrb rules:
    /// a. No consecutive reds should be found in the tree.
    /// b. number of blacks should be same on both sides.
    pub fn validate(&self) -> Result<(), BognError> {
        if self.is_mvcc {
            let arc = self.snapshot.clone(&self.rw);
            let root = arc.as_ref().as_ref();
            let (fromred, nblacks) = (is_red(root), 0);
            Llrb::validate_tree(root, fromred, nblacks)?
        } else {
            let root = self.single_root.as_ref();
            let (fromred, nblacks) = (is_red(root), 0);
            Llrb::validate_tree(root, fromred, nblacks)?
        };
        Ok(())
    }

    fn validate_tree(
        node: Option<&Node<K, V>>,
        fromred: bool,
        mut nblacks: u64,
    ) -> Result<u64, BognError> {
        if node.is_none() {
            return Ok(nblacks);
        }

        let red = is_red(node.as_ref().map(|item| item.deref()));
        if fromred && red {
            return Err(BognError::ConsecutiveReds);
        }
        if !red {
            nblacks += 1;
        }
        let node = &node.as_ref().unwrap();
        let left = node.left_deref();
        let right = node.right_deref();
        let lblacks = Llrb::validate_tree(left, red, nblacks)?;
        let rblacks = Llrb::validate_tree(right, red, nblacks)?;
        if lblacks != rblacks {
            let err = format!(
                "llrb_store: unbalanced blacks left: {} and right: {}",
                lblacks, rblacks,
            );
            return Err(BognError::UnbalancedBlacks(err));
        }
        if node.left.is_some() {
            let left = node.left.as_ref().unwrap();
            if left.key.ge(&node.key) {
                let [a, b] = [&left.key, &node.key];
                let err = format!("left key {:?} >= parent {:?}", a, b);
                return Err(BognError::SortError(err));
            }
        }
        if node.right.is_some() {
            let right = node.right.as_ref().unwrap();
            if right.key.le(&node.key) {
                let [a, b] = [&right.key, &node.key];
                let err = format!("right {:?} <= parent {:?}", a, b);
                return Err(BognError::SortError(err));
            }
        }
        Ok(lblacks)
    }
}

fn is_red<K, V>(node: Option<&Node<K, V>>) -> bool
where
    K: AsKey,
    V: Default + Clone,
{
    match node {
        None => false,
        node @ Some(_) => !is_black(node),
    }
}

fn is_black<K, V>(node: Option<&Node<K, V>>) -> bool
where
    K: AsKey,
    V: Default + Clone,
{
    match node {
        None => true,
        Some(node) => node.is_black(),
    }
}

// TODO: refactor this for mvcc.
impl<K, V> Clone for Llrb<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    fn clone(&self) -> Llrb<K, V> {
        let _lock = self.mutex.lock();

        let mut arc = self.snapshot.clone(&self.rw);
        Llrb {
            name: self.name.clone(),
            lsm: self.lsm,
            is_mvcc: self.is_mvcc,
            single_root: self.single_root.clone(),
            snapshot: Snapshot {
                value: AtomicPtr::new(&mut arc),
            },
            mutex: Mutex::new(0),
            rw: RwLock::new(0),
        }
    }
}

//----------------------------------------------------------------------------

pub struct Iter<'a, K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    arc: Arc<MvccRoot<K, V>>,
    llrb: &'a Llrb<K, V>,
    node_iter: std::vec::IntoIter<Node<K, V>>,
    after_key: Bound<K>,
    limit: usize,
    fin: bool,
}

impl<'a, K, V> Iter<'a, K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    fn get_root(&self) -> Option<&Node<K, V>> {
        if self.llrb.is_mvcc {
            self.arc.as_ref().as_ref()
        } else {
            self.llrb.single_root.as_ref()
        }
    }

    fn scan_iter(
        &self,
        node: Option<&Node<K, V>>,
        acc: &mut Vec<Node<K, V>>, // accumulator for batch of nodes
    ) -> bool {
        if node.is_none() {
            return true;
        }

        let node = node.unwrap();
        //println!("scan_iter {:?} {:?}", node.key, self.after_key);
        let left = node.left_deref();
        let right = node.right_deref();
        match &self.after_key {
            Bound::Included(akey) | Bound::Excluded(akey) => {
                if node.key.borrow().le(akey) {
                    return self.scan_iter(right, acc);
                }
            }
            Bound::Unbounded => (),
        }

        //println!("left {:?} {:?}", node.key, self.after_key);
        if !self.scan_iter(left, acc) {
            return false;
        }

        acc.push(node.clone_detach());
        //println!("push {:?} {}", self.after_key, acc.len());
        if acc.len() >= self.limit {
            return false;
        }

        return self.scan_iter(right, acc);
    }
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    type Item = Node<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        //println!("yyy");
        if self.fin {
            return None;
        }

        let node = self.node_iter.next();
        if node.is_some() {
            return node;
        }

        let mut acc: Vec<Node<K, V>> = Vec::with_capacity(self.limit);
        self.scan_iter(self.get_root(), &mut acc);

        if acc.len() == 0 {
            self.fin = true;
            None
        } else {
            //println!("iter-next {}", acc.len());
            self.after_key = Bound::Excluded(acc.last().unwrap().key());
            self.node_iter = acc.into_iter();
            self.node_iter.next()
        }
    }
}

pub struct Range<'a, K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    arc: Arc<MvccRoot<K, V>>,
    llrb: &'a Llrb<K, V>,
    node_iter: std::vec::IntoIter<Node<K, V>>,
    low: Bound<K>,
    high: Bound<K>,
    limit: usize,
    fin: bool,
}

impl<'a, K, V> Range<'a, K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    pub fn rev(self) -> Reverse<'a, K, V> {
        Reverse {
            arc: self.arc,
            llrb: self.llrb,
            node_iter: vec![].into_iter(),
            low: self.low,
            high: self.high,
            limit: self.limit,
            fin: false,
        }
    }

    fn get_root(&self) -> Option<&Node<K, V>> {
        if self.llrb.is_mvcc {
            self.arc.as_ref().as_ref()
        } else {
            self.llrb.single_root.as_ref()
        }
    }

    fn range_iter(
        &self,
        node: Option<&Node<K, V>>,
        acc: &mut Vec<Node<K, V>>, // accumulator for batch of nodes
    ) -> bool {
        if node.is_none() {
            return true;
        }

        let node = node.unwrap();
        //println!("range_iter {:?} {:?}", node.key, self.low);
        let left = node.left_deref();
        let right = node.right_deref();
        match &self.low {
            Bound::Included(qow) if node.key.lt(qow) => {
                return self.range_iter(right, acc);
            }
            Bound::Excluded(qow) if node.key.le(qow) => {
                return self.range_iter(right, acc);
            }
            _ => (),
        }

        //println!("left {:?} {:?}", node.key, self.low);
        if !self.range_iter(left, acc) {
            return false;
        }

        acc.push(node.clone_detach());
        //println!("push {:?} {}", self.low, acc.len());
        if acc.len() >= self.limit {
            return false;
        }

        return self.range_iter(right, acc);
    }
}

impl<'a, K, V> Iterator for Range<'a, K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    type Item = Node<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        //println!("yyy");
        if self.fin {
            return None;
        }

        let node = self.node_iter.next();
        let node = if node.is_none() {
            let mut acc: Vec<Node<K, V>> = Vec::with_capacity(self.limit);
            self.range_iter(self.get_root(), &mut acc);
            if acc.len() > 0 {
                //println!("iter-next {}", acc.len());
                self.low = Bound::Excluded(acc.last().unwrap().key());
                self.node_iter = acc.into_iter();
                self.node_iter.next()
            } else {
                None
            }
        } else {
            node
        };

        if node.is_none() {
            self.fin = true;
            return None;
        }

        // handle upper limit
        let node = node.unwrap();
        //println!("llrb next {:?}", node.key);
        match &self.high {
            Bound::Unbounded => Some(node),
            Bound::Included(qigh) if node.key.le(qigh) => Some(node),
            Bound::Excluded(qigh) if node.key.lt(qigh) => Some(node),
            _ => {
                self.fin = true;
                None
            }
        }
    }
}

pub struct Reverse<'a, K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    arc: Arc<MvccRoot<K, V>>,
    llrb: &'a Llrb<K, V>,
    node_iter: std::vec::IntoIter<Node<K, V>>,
    high: Bound<K>,
    low: Bound<K>,
    limit: usize,
    fin: bool,
}

impl<'a, K, V> Reverse<'a, K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    fn get_root(&self) -> Option<&Node<K, V>> {
        if self.llrb.is_mvcc {
            self.arc.as_ref().as_ref()
        } else {
            self.llrb.single_root.as_ref()
        }
    }

    fn reverse_iter(
        &self,
        node: Option<&Node<K, V>>,
        acc: &mut Vec<Node<K, V>>, // accumulator for batch of nodes
    ) -> bool {
        if node.is_none() {
            return true;
        }

        let node = node.unwrap();
        //println!("reverse_iter {:?} {:?}", node.key, self.high);
        let left = node.left_deref();
        let right = node.right_deref();
        match &self.high {
            Bound::Included(qigh) if node.key.gt(qigh) => {
                return self.reverse_iter(left, acc);
            }
            Bound::Excluded(qigh) if node.key.ge(qigh) => {
                return self.reverse_iter(left, acc);
            }
            _ => (),
        }

        //println!("left {:?} {:?}", node.key, self.high);
        if !self.reverse_iter(right, acc) {
            return false;
        }

        acc.push(node.clone_detach());
        //println!("push {:?} {}", self.high, acc.len());
        if acc.len() >= self.limit {
            return false;
        }

        return self.reverse_iter(left, acc);
    }
}

impl<'a, K, V> Iterator for Reverse<'a, K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    type Item = Node<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        //println!("yyy");
        if self.fin {
            return None;
        }

        let node = self.node_iter.next();
        let node = if node.is_none() {
            let mut acc: Vec<Node<K, V>> = Vec::with_capacity(self.limit);
            self.reverse_iter(self.get_root(), &mut acc);
            if acc.len() > 0 {
                //println!("iter-next {}", acc.len());
                self.high = Bound::Excluded(acc.last().unwrap().key());
                self.node_iter = acc.into_iter();
                self.node_iter.next()
            } else {
                None
            }
        } else {
            node
        };

        if node.is_none() {
            self.fin = true;
            return None;
        }

        // handle lower limit
        let node = node.unwrap();
        //println!("llrb next {:?}", node.key);
        match &self.low {
            Bound::Unbounded => Some(node),
            Bound::Included(qow) if node.key.ge(qow) => Some(node),
            Bound::Excluded(qow) if node.key.gt(qow) => Some(node),
            _ => {
                //println!("llrb reverse over {:?}", &self.low);
                self.fin = true;
                None
            }
        }
    }
}
