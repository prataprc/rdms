use std::mem;
use std::ops::Deref;

use crate::core::{Diff, Entry, Footprint, Value};
#[allow(unused_imports)]
use crate::llrb::Llrb;

/// Node corresponds to a single entry in Llrb instance.
#[derive(Clone)]
pub(crate) struct Node<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    pub(crate) entry: Entry<K, V>,
    pub(crate) black: bool,                    // store: black or red
    pub(crate) dirty: bool,                    // new node in mvcc path
    pub(crate) left: Option<Box<Node<K, V>>>,  // store: left child
    pub(crate) right: Option<Box<Node<K, V>>>, // store: right child
}

// construct node values.
impl<K, V> Node<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    // lsm delete.
    pub(crate) fn new_deleted(key: K, deleted: u64) -> Box<Node<K, V>> {
        let node = Box::new(Node {
            entry: Entry::new(key, Box::new(Value::new_delete(deleted))),
            black: false,
            dirty: true,
            left: None,
            right: None,
        });
        //println!("new node {:p}", node);
        node
    }

    // unsafe clone for MVCC CoW
    pub(crate) fn mvcc_clone(
        &self,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> Box<Node<K, V>> {
        let new_node = Box::new(Node {
            entry: self.entry.clone(),
            black: self.black,
            dirty: self.dirty,
            left: self.left.as_ref().map(|n| n.duplicate()),
            right: self.right.as_ref().map(|n| n.duplicate()),
        });
        //println!("new node (mvcc) {:p} {:p}", self, new_node);
        reclaim.push(self.duplicate());
        new_node
    }

    // remove this node from the tree without dropping the children.
    pub(crate) fn mvcc_detach(&mut self) {
        self.left.take().map(Box::leak);
        self.right.take().map(Box::leak);
    }

    // clone this node without cloning its children. dirty is set to true.
    pub(crate) fn clone_detach(&self) -> Node<K, V> {
        Node {
            entry: self.entry.clone(),
            black: self.black,
            dirty: true,
            left: None,
            right: None,
        }
    }

    #[inline]
    pub(crate) fn duplicate(&self) -> Box<Node<K, V>> {
        unsafe { Box::from_raw(self as *const Node<K, V> as *mut Node<K, V>) }
    }
}

// write/update methods
impl<K, V> Node<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    // prepend operation, equivalent to SET / INSERT / UPDATE
    // return the different in footprint for this node
    pub(crate) fn prepend_version(
        &mut self,
        entry: Entry<K, V>,
        lsm: bool, /* will preseve old mutations*/
    ) -> isize {
        self.entry.prepend_version(entry, lsm)
    }

    // DELETE operation, back to back delete shall collapse
    #[inline]
    pub(crate) fn delete(&mut self, seqno: u64) -> isize {
        self.entry.delete(seqno)
    }

    #[inline]
    pub(crate) fn set_red(&mut self) {
        self.black = false
    }

    #[inline]
    pub(crate) fn set_black(&mut self) {
        self.black = true
    }

    #[inline]
    pub(crate) fn toggle_link(&mut self) {
        self.black = !self.black
    }

    pub(crate) fn overhead() -> usize {
        mem::size_of::<Node<K, V>>() - mem::size_of::<Entry<K, V>>()
    }

    pub(crate) fn footprint(&self) -> isize {
        (Node::<K, V>::overhead() as isize) + self.entry.footprint()
    }
}

// read methods
impl<K, V> Node<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    #[inline]
    pub(crate) fn as_left_deref(&self) -> Option<&Node<K, V>> {
        self.left.as_ref().map(Deref::deref)
    }

    #[inline]
    pub(crate) fn as_right_deref(&self) -> Option<&Node<K, V>> {
        self.right.as_ref().map(Deref::deref)
    }

    #[inline]
    pub(crate) fn is_black(&self) -> bool {
        self.black
    }

    pub(crate) fn as_key(&self) -> &K {
        self.entry.as_key()
    }

    pub(crate) fn to_seqno(&self) -> u64 {
        self.entry.to_seqno()
    }

    pub(crate) fn is_deleted(&self) -> bool {
        self.entry.is_deleted()
    }
}

// fence recursive drops
impl<K, V> Drop for Node<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn drop(&mut self) {
        self.left.take().map(Box::leak);
        self.right.take().map(Box::leak);
    }
}

impl<K, V> From<Entry<K, V>> for Node<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn from(entry: Entry<K, V>) -> Node<K, V> {
        Node {
            entry,
            black: false,
            dirty: true,
            left: None,
            right: None,
        }
    }
}

/// Statistics for [`Llrb`] and [`Mvcc`] tree.
pub enum Stats {
    /// full statisics via [`Llrb::validate`] method.
    Full {
        entries: usize,
        node_size: usize,
        blacks: usize,
        depths: LlrbDepth,
    },
    /// partial but quick statistics via [`Llrb::stats`] method.
    Partial { entries: usize, node_size: usize },
}

impl Stats {
    pub(crate) fn new_partial(entries: usize, node_size: usize) -> Stats {
        Stats::Partial { entries, node_size }
    }

    pub(crate) fn new_full(
        entries: usize,
        node_size: usize,
        blacks: usize,
        depths: LlrbDepth,
    ) -> Stats {
        Stats::Full {
            entries,
            node_size,
            blacks,
            depths,
        }
    }

    #[inline]
    /// Return number entries in [`Llrb`] / [`Mvcc`] instance.
    pub fn to_entries(&self) -> usize {
        match self {
            Stats::Partial { entries, .. } => *entries,
            Stats::Full { entries, .. } => *entries,
        }
    }

    #[inline]
    /// Return node-size, including over-head for `Llrb<k,V>` / `Mvcc<K,V>.
    /// Although the node overhead is constant, the node size varies based
    /// on key and value types. EG:
    ///
    /// ```
    /// use bogn::llrb::Llrb;
    /// let mut llrb: Llrb<i64,i64> = Llrb::new("myinstance");
    ///
    /// assert_eq!(llrb.stats().to_node_size(), 64);
    /// ```
    pub fn to_node_size(&self) -> usize {
        match self {
            Stats::Partial { node_size, .. } => *node_size,
            Stats::Full { node_size, .. } => *node_size,
        }
    }

    #[inline]
    /// Return number of black nodes from root to leaf, on both left
    /// and right child.
    pub fn to_blacks(&self) -> Option<usize> {
        match self {
            Stats::Partial { .. } => None,
            Stats::Full { blacks, .. } => Some(*blacks),
        }
    }

    /// Return [`LlrbDepth`] statistics.
    pub fn to_depths(&self) -> Option<LlrbDepth> {
        match self {
            Stats::Partial { .. } => None,
            Stats::Full { depths, .. } => Some(depths.clone()),
        }
    }
}

// TODO: test cases for Depth.

/// LlrbDepth calculates minimum, maximum, average and percentile of
/// leaf-node depth in the LLRB tree.
#[derive(Clone)]
pub struct LlrbDepth {
    samples: usize,
    min: usize,
    max: usize,
    total: usize,
    depths: [u64; 256],
}

impl LlrbDepth {
    pub(crate) fn sample(&mut self, depth: usize) {
        self.samples += 1;
        self.total += depth;
        if self.min == 0 || self.min > depth {
            self.min = depth
        }
        if self.max == 0 || self.max < depth {
            self.max = depth
        }
        self.depths[depth] += 1;
    }

    /// Return number of leaf-nodes sample for depth in LLRB tree.
    pub fn to_samples(&self) -> usize {
        self.samples
    }

    /// Return minimum depth of leaf-node in LLRB tree.
    pub fn to_min(&self) -> usize {
        self.min
    }

    /// Return the average depth of leaf-nodes in LLRB tree.
    pub fn to_mean(&self) -> usize {
        self.total / self.samples
    }

    /// Return maximum depth of leaf-node in LLRB tree.
    pub fn to_max(&self) -> usize {
        self.max
    }

    /// Return depth as tuple of percentiles, each tuple provides
    /// (percentile, depth). Returned percentiles from 90, 91 .. 99
    pub fn to_percentiles(&self) -> Vec<(u8, usize)> {
        let mut percentiles: Vec<(u8, usize)> = vec![];
        let (mut acc, mut prev_perc) = (0_u64, 90_u8);
        let iter = self.depths.iter().enumerate().filter(|(_, &item)| item > 0);
        for (depth, samples) in iter {
            acc += *samples;
            let perc = ((acc as f64 / (self.samples as f64)) * 100_f64) as u8;
            if perc >= prev_perc {
                percentiles.push((perc, depth));
                prev_perc = perc;
            }
        }
        percentiles
    }

    pub fn pretty_print(&self, prefix: &str) {
        let mean = self.to_mean();
        println!(
            "{}depth (min, max, avg): {:?}",
            prefix,
            (self.min, mean, self.max)
        );
        for (depth, n) in self.to_percentiles().into_iter() {
            if n > 0 {
                println!("{}  {} percentile = {}", prefix, depth, n);
            }
        }
    }

    // TODO: start using jsondata package. Can be a single line implementation
    // From::from::<jsondata::Json>(self).to_string()
    pub fn to_json_text(&self) -> String {
        let ps: Vec<String> = self
            .to_percentiles()
            .into_iter()
            .map(|(d, n)| format!("{}: {}", d, n))
            .collect();
        let strs = [
            format!("min: {}", self.to_min()),
            format!("mean: {}", self.to_mean()),
            format!("max: {}", self.to_max()),
            format!("percentiles: {}", ps.join(", ")),
        ];
        ("{ ".to_string() + strs.join(", ").as_str() + " }").to_string()
    }
}

impl Default for LlrbDepth {
    fn default() -> Self {
        LlrbDepth {
            samples: 0,
            min: 0,
            max: 0,
            total: 0,
            depths: [0; 256],
        }
    }
}
