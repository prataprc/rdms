use std::ops::Deref;

use crate::core::{Diff, Entry};
#[allow(unused_imports)]
use crate::{Llrb, Mvcc};

/// Node corresponds to a single entry in Llrb instance.
#[derive(Clone)]
pub struct Node<K, V>
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
    // CREATE operation
    pub(crate) fn new(
        k: K,
        v: Option<V>, // in case of delete None
        seqno: u64,
        black: bool,
    ) -> Box<Node<K, V>> {
        let node = Box::new(Node {
            entry: Entry::new_entry(k, v, seqno),
            black,
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
    K: Clone + Ord,
    V: Clone + Diff,
{
    // prepend operation, equivalent to SET / INSERT / UPDATE
    pub(crate) fn prepend_version(&mut self, value: V, seqno: u64, lsm: bool) {
        self.entry.prepend_version(value, seqno, lsm)
    }

    // DELETE operation, back to back delete shall collapse
    #[inline]
    pub(crate) fn delete(&mut self, seqno: u64) {
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
}

// read methods
impl<K, V> Node<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    #[inline]
    pub(crate) fn left_deref(&self) -> Option<&Node<K, V>> {
        self.left.as_ref().map(Deref::deref)
    }

    #[inline]
    pub(crate) fn right_deref(&self) -> Option<&Node<K, V>> {
        self.right.as_ref().map(Deref::deref)
    }

    #[inline]
    pub(crate) fn is_black(&self) -> bool {
        self.black
    }

    pub(crate) fn key_ref(&self) -> &K {
        &self.entry.key_ref()
    }

    pub(crate) fn seqno(&self) -> u64 {
        self.entry.seqno()
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

/// Statistics for [`Llrb`] and [`Mvcc`] tree. Serves two purpose:
///
/// * To get partial but quick statistics via [`Llrb::stats`] method.
/// * To get full statisics via [`Llrb::validate`] method.
#[derive(Default)]
pub struct LlrbStats {
    entries: usize, // number of entries in the tree.
    node_size: usize,
    blacks: Option<usize>,
    depths: Option<LlrbDepth>,
}

impl LlrbStats {
    pub(crate) fn new(entries: usize, node_size: usize) -> LlrbStats {
        LlrbStats {
            entries,
            node_size,
            blacks: Default::default(),
            depths: Default::default(),
        }
    }

    #[inline]
    pub(crate) fn set_blacks(&mut self, blacks: usize) {
        self.blacks = Some(blacks)
    }

    #[inline]
    pub(crate) fn set_depths(&mut self, depths: LlrbDepth) {
        self.depths = Some(depths)
    }

    #[inline]
    pub(crate) fn sample_depth(&mut self, depth: usize) {
        self.depths.as_mut().unwrap().sample(depth)
    }

    #[inline]
    /// Return number entries in [`Llrb`] / [`Mvcc`] instance.
    pub fn entries(&self) -> usize {
        self.entries
    }

    #[inline]
    /// Return node-size, including over-head for `Llrb<k,V>` / `Mvcc<K,V>.
    /// Although the node overhead is constant, the node size varies based
    /// on key and value types. EG:
    ///
    /// ```
    /// use bogn::Llrb;
    /// let mut llrb: Llrb<i64,i64> = Llrb::new("myinstance");
    ///
    /// // size of key: 8 bytes
    /// // size of value: 16 bytes
    /// // overhead is 24 bytes
    /// assert_eq!(llrb.stats().node_size(), 128);
    /// ```
    pub fn node_size(&self) -> usize {
        self.node_size
    }

    #[inline]
    /// Return number of black nodes from root to leaf, on both left
    /// and right child.
    pub fn blacks(&self) -> Option<usize> {
        self.blacks
    }

    /// Return [`LlrbDepth`] statistics.
    pub fn depths(&self) -> Option<LlrbDepth> {
        if self.depths.as_ref().unwrap().samples() == 0 {
            None
        } else {
            self.depths.clone()
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
    pub fn samples(&self) -> usize {
        self.samples
    }

    /// Return minimum depth of leaf-node in LLRB tree.
    pub fn min(&self) -> usize {
        self.min
    }

    /// Return the average depth of leaf-nodes in LLRB tree.
    pub fn mean(&self) -> usize {
        self.total / self.samples
    }

    /// Return maximum depth of leaf-node in LLRB tree.
    pub fn max(&self) -> usize {
        self.max
    }

    /// Return depth as tuple of percentiles, each tuple provides
    /// (percentile, depth). Returned percentiles from 90, 91 .. 99
    pub fn percentiles(&self) -> Vec<(u8, usize)> {
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
        let mean = self.mean();
        println!(
            "{}depth (min, max, avg): {:?}",
            prefix,
            (self.min, mean, self.max)
        );
        for (depth, n) in self.percentiles().into_iter() {
            if n > 0 {
                println!("{}  {} percentile = {}", prefix, depth, n);
            }
        }
    }

    pub fn json(&self) -> String {
        let ps: Vec<String> = self
            .percentiles()
            .into_iter()
            .map(|(d, n)| format!("{}: {}", d, n))
            .collect();
        let strs = [
            format!("min: {}", self.min),
            format!("mean: {}", self.mean()),
            format!("max: {}", self.max),
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
