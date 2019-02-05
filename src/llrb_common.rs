use std::borrow::Borrow;
use std::cmp::{Ord, Ordering};
use std::ops::{Bound, Deref};
use std::sync::Arc;

use crate::error::BognError;
use crate::llrb_node::Node;
use crate::mvcc::MvccRoot;
use crate::traits::{AsEntry, AsKey};

pub(crate) fn get<K, V, Q>(
    mut node: Option<&Node<K, V>>, // root node
    key: &Q,
) -> Option<impl AsEntry<K, V>>
where
    K: AsKey + Borrow<Q>,
    V: Default + Clone,
    Q: Ord + ?Sized,
{
    while node.is_some() {
        let nref = node.unwrap();
        node = match nref.key.borrow().cmp(key) {
            Ordering::Less => nref.right_deref(),
            Ordering::Greater => nref.left_deref(),
            Ordering::Equal => return Some(nref.clone_detach()),
        };
    }
    None
}

pub(crate) fn validate_tree<K, V>(
    node: Option<&Node<K, V>>,
    fromred: bool,
    mut nblacks: u64,
) -> Result<u64, BognError>
where
    K: AsKey,
    V: Default + Clone,
{
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
    let lblacks = validate_tree(left, red, nblacks)?;
    let rblacks = validate_tree(right, red, nblacks)?;
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

pub struct Iter<'a, K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    pub(crate) arc: Arc<Box<MvccRoot<K, V>>>,
    pub(crate) root: Option<&'a Node<K, V>>,
    pub(crate) node_iter: std::vec::IntoIter<Node<K, V>>,
    pub(crate) after_key: Bound<K>,
    pub(crate) limit: usize,
    pub(crate) fin: bool,
}

impl<'a, K, V> Iter<'a, K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    fn get_root(&self) -> Option<&Node<K, V>> {
        match self.root {
            root @ Some(_) => root,
            None => self.arc.root.as_ref().map(|item| item.deref()),
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
    pub(crate) arc: Arc<Box<MvccRoot<K, V>>>,
    pub(crate) root: Option<&'a Node<K, V>>,
    pub(crate) node_iter: std::vec::IntoIter<Node<K, V>>,
    pub(crate) low: Bound<K>,
    pub(crate) high: Bound<K>,
    pub(crate) limit: usize,
    pub(crate) fin: bool,
}

impl<'a, K, V> Range<'a, K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    fn get_root(&self) -> Option<&Node<K, V>> {
        match self.root {
            root @ Some(_) => root,
            None => self.arc.root.as_ref().map(|item| item.deref()),
        }
    }

    pub fn rev(self) -> Reverse<'a, K, V> {
        Reverse {
            arc: self.arc,
            root: self.root,
            node_iter: vec![].into_iter(),
            low: self.low,
            high: self.high,
            limit: self.limit,
            fin: false,
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
        //println!("root next {:?}", node.key);
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
    arc: Arc<Box<MvccRoot<K, V>>>,
    root: Option<&'a Node<K, V>>,
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
        match self.root {
            root @ Some(_) => root,
            None => self.arc.root.as_ref().map(|item| item.deref()),
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

pub fn is_red<K, V>(node: Option<&Node<K, V>>) -> bool
where
    K: AsKey,
    V: Default + Clone,
{
    match node {
        None => false,
        node @ Some(_) => !is_black(node),
    }
}

pub fn is_black<K, V>(node: Option<&Node<K, V>>) -> bool
where
    K: AsKey,
    V: Default + Clone,
{
    match node {
        None => true,
        Some(node) => node.is_black(),
    }
}
